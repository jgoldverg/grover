package gclient

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/pool"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

var errSessionClosed = errors.New("udp session closed")

// udpConn / streamState: core data-plane per session
type udpConn struct {
	// identity
	sessionID  string
	sessionKey uint32
	token      string
	udpHost    string
	udpPort    uint32

	// data-plane
	streams   map[uint32]*streamState // stream id -> state
	txWindows map[uint32]*txWindow
	conn      *net.UDPConn
	mtu       int

	// lifecycle
	mu      sync.RWMutex
	ref     int32
	lastUse time.Time
	closed  atomic.Bool

	rb      *RingBuffer
	rxDone  chan struct{}
	txDone  chan error
	bufPool *pool.BufferPool
}

type streamState struct {
	id      uint32
	pw      *io.PipeWriter
	pr      *io.PipeReader
	tracker *udpwire.SackTracker
}

type txPacket struct {
	payload  []byte
	seq      uint32
	offset   uint64
	streamID uint32
}

// newSession constructs a udpConn and starts the recv loop.
// It assumes conn is already a dialed *net.UDPConn.
func newSession(
	sessionID string,
	sessionIDRaw []byte,
	token string,
	udpHost string,
	udpPort uint32,
	conn *net.UDPConn,
	mtu int,
	bufferSize uint64,
	streamIDs []uint32,
	maxInFlightBytes int,
	linkBandwidthMbps int,
	targetLossPercent int,
) *udpConn {
	streams := make(map[uint32]*streamState, len(streamIDs))
	txWins := make(map[uint32]*txWindow, len(streamIDs))

	windowBase := maxInFlightBytes
	if windowBase <= 0 {
		windowBase = 64 * 1024
	}
	bdpBytes := estimateBDPBytes(linkBandwidthMbps)
	windowCeiling := windowBase
	if bdpBytes > windowCeiling {
		windowCeiling = bdpBytes
	}
	lossRatio := targetLossRatio(targetLossPercent)

	for _, id := range streamIDs {
		pr, pw := io.Pipe()
		streams[id] = &streamState{
			id:      id,
			pr:      pr,
			pw:      pw,
			tracker: udpwire.NewSackTracker(),
		}
		txWins[id] = newTxWindow(windowBase, windowCeiling, lossRatio)
	}

	var sessionKey uint32
	if len(sessionIDRaw) >= 4 {
		sessionKey = binary.BigEndian.Uint32(sessionIDRaw[:4])
	}

	u := &udpConn{
		sessionID:  sessionID,
		sessionKey: sessionKey,
		token:      token,
		udpHost:    udpHost,
		udpPort:    udpPort,

		streams:   streams,
		txWindows: txWins,

		conn:    conn,
		mtu:     mtu,
		ref:     1,
		lastUse: time.Now(),
		rxDone:  make(chan struct{}),
		txDone:  make(chan error, 1),
		bufPool: pool.NewBufferPool(bufferSize),
		rb:      NewRingBuffer(64),
	}

	go u.recvLoop()
	go u.txLoop(context.Background())
	return u
}

func (u *udpConn) Close() {
	// Make Close() idempotent.
	if !u.closed.CompareAndSwap(false, true) {
		return
	}

	_ = u.conn.Close()
	u.failTxWindows(errSessionClosed)

	u.mu.Lock()
	for _, s := range u.streams {
		_ = s.pw.Close()
	}
	u.mu.Unlock()
	u.rb.Close()
	<-u.rxDone
}

func (u *udpConn) payloadCapacity() int {
	payload := u.mtu - udpwire.DataHeaderLen - 4
	if payload <= 0 {
		return 1024
	}
	return payload
}

func (u *udpConn) writePacket(ctx context.Context, packet []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = u.conn.SetWriteDeadline(deadline)
		defer u.conn.SetWriteDeadline(time.Time{})
	}
	_, err := u.conn.Write(packet)
	return err
}

func (u *udpConn) touch() {
	u.mu.Lock()
	u.lastUse = time.Now()
	u.mu.Unlock()
}

func (u *udpConn) Write(ctx context.Context, r io.Reader, size int64, overwrite backend.OverwritePolicy) error {
	streamID, err := u.singleStreamID()
	if err != nil {
		return err
	}
	return u.writeStream(ctx, streamID, r, size, overwrite)
}

// Read currently only supports sessions with a single stream. For multi-stream
// sessions we need a proper merge/reassembler, so we fail fast instead of
// picking a default.
func (u *udpConn) Read(ctx context.Context) (io.ReadCloser, error) {
	s, err := u.singleStream()
	if err != nil {
		return nil, err
	}
	if s.pr == nil {
		return nil, fmt.Errorf("nil reader for stream %d", s.id)
	}
	return s.pr, nil
}

func (u *udpConn) ReadAt(ctx context.Context, offset int64, p []byte) (int, error) {
	// Random access isn't a transport concern. Session layer does this.
	return 0, fmt.Errorf("udpConn.ReadAt not implemented at transport level")
}

func (u *udpConn) txLoop(ctx context.Context) {
	var errOut error
	defer func() {
		u.txDone <- errOut
		close(u.txDone)
	}()
	scratch := make([]byte, udpwire.DataHeaderLen+u.payloadCapacity()+4)
	for {
		pkt, err := u.rb.Dequeue(ctx)
		if err != nil {
			internal.Info("error inside of txLoop", internal.Fields{
				"error": err.Error(),
			})
			if errors.Is(err, io.EOF) {
				return
			}
			_ = u.conn.Close()
			u.failTxWindows(err)
			errOut = err
			return
		}
		txPayload := pkt.payload
		if len(txPayload) == 0 {
			u.cancelWindow(pkt.streamID, pkt.seq)
			continue
		}

		dp := udpwire.DataPacket{
			SessionID: u.sessionKey,
			StreamID:  pkt.streamID,
			Seq:       pkt.seq,
			Offset:    pkt.offset,
			Payload:   txPayload,
		}
		n, err := dp.Encode(scratch)
		if err != nil {
			internal.Error("failed to encode udp data packet", internal.Fields{
				internal.FieldError: err.Error(),
				"session":           u.sessionID,
				"stream":            pkt.streamID,
				"seq":               pkt.seq,
			})
			u.cancelWindow(pkt.streamID, pkt.seq)
			u.failTxWindows(err)
			errOut = err
			_ = u.conn.Close()
			return
		}
		if err := u.writePacket(ctx, scratch[:n]); err != nil {
			u.cancelWindow(pkt.streamID, pkt.seq)
			_ = u.conn.Close()
			u.failTxWindows(err)
			errOut = err
			return
		}
		internal.Debug("client udp data tx", internal.Fields{
			"session": u.sessionID,
			"stream":  pkt.streamID,
			"seq":     pkt.seq,
			"bytes":   len(pkt.payload),
		})
		u.touch()
	}
}

func (u *udpConn) recvLoop() {
	defer close(u.rxDone)

	buf := u.bufPool.GetBuffer()
	defer u.bufPool.PutBuffer(buf)

	var dp udpwire.DataPacket
	var sp udpwire.StatusPacket
	statusBuf := make([]byte, udpwire.StatusHeaderLen+udpwire.MaxSackRanges*udpwire.SackBlockLen)
	var sackScratch []udpwire.SackRange

	for {
		n, _, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			// propagate error to stream readers
			u.mu.RLock()
			for _, s := range u.streams {
				_ = s.pw.CloseWithError(err)
			}
			u.mu.RUnlock()
			return
		}

		if n == 0 {
			continue
		}

		packet := buf[:n]
		kind, ok := udpwire.PeekKind(packet)
		if !ok {
			continue
		}

		switch kind {
		case udpwire.KindData:
			if _, err := dp.Decode(packet); err != nil {
				continue
			}

			// Core demux: route by StreamID to the right streamState.
			u.mu.RLock()
			s := u.streams[dp.StreamID]
			u.mu.RUnlock()

			if s == nil {
				// unknown stream for this session; drop packet
				continue
			}

			if len(dp.Payload) > 0 {
				if _, err := s.pw.Write(dp.Payload); err != nil {
					// reader closed; ignore further data for this stream
					continue
				}
			}
			internal.Debug("client udp data rx", internal.Fields{
				"session": u.sessionID,
				"stream":  dp.StreamID,
				"seq":     dp.Seq,
				"bytes":   len(dp.Payload),
			})
			if st := s.tracker; st != nil && st.OnPacket(dp.Seq) {
				sackScratch = u.sendStatusPacket(dp.StreamID, st, sackScratch, statusBuf)
			}

		case udpwire.KindStatus:
			if _, err := sp.Decode(packet); err != nil {
				continue
			}
			fields := internal.Fields{
				"session": u.sessionID,
				"stream":  sp.StreamID,
				"ack":     sp.AckSeq,
			}
			if desc := formatSackRanges(sp.Sacks); desc != "" {
				fields["sacks"] = desc
			}
			internal.Debug("client udp status rx", fields)
			u.handleStatusPacket(sp)

		default:
			// ignore unknown kinds
		}

		u.mu.Lock()
		u.lastUse = time.Now()
		u.mu.Unlock()
	}
}

func (u *udpConn) getTxWindow(streamID uint32) *txWindow {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.txWindows == nil {
		return nil
	}
	return u.txWindows[streamID]
}

func (u *udpConn) reserveWindow(ctx context.Context, streamID uint32, seq uint32, bytes int) error {
	if bytes <= 0 {
		return nil
	}
	win := u.getTxWindow(streamID)
	if win == nil {
		return fmt.Errorf("tx window missing for stream %d", streamID)
	}
	return win.reserve(ctx, seq, bytes)
}

func (u *udpConn) cancelWindow(streamID uint32, seq uint32) {
	win := u.getTxWindow(streamID)
	if win == nil {
		return
	}
	win.cancel(seq)
}

func (u *udpConn) handleStatusPacket(sp udpwire.StatusPacket) {
	win := u.getTxWindow(sp.StreamID)
	if win == nil {
		return
	}
	win.releaseThrough(sp.AckSeq)
	if len(sp.Sacks) > 0 {
		win.releaseRanges(sp.Sacks)
	}
	// TODO: interpret sp.Sacks for selective retransmission.
}

func (u *udpConn) failTxWindows(err error) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	for _, win := range u.txWindows {
		win.close(err)
	}
}

func (u *udpConn) sendStatusPacket(
	streamID uint32,
	tracker *udpwire.SackTracker,
	scratch []udpwire.SackRange,
	statusBuf []byte,
) []udpwire.SackRange {
	if tracker == nil {
		if scratch == nil {
			return nil
		}
		return scratch[:0]
	}
	if len(statusBuf) < udpwire.StatusHeaderLen {
		return scratch[:0]
	}

	ackSeq, sacks := tracker.Snapshot(udpwire.MaxSackRanges, scratch)
	sp := udpwire.StatusPacket{
		SessionID: u.sessionKey,
		StreamID:  streamID,
		AckSeq:    ackSeq,
		Sacks:     sacks,
	}

	n, err := sp.Encode(statusBuf)
	if err != nil {
		internal.Debug("client failed to encode status packet", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        u.sessionID,
			"stream_id":         streamID,
		})
		return sacks[:0]
	}

	if _, err := u.conn.Write(statusBuf[:n]); err != nil {
		internal.Debug("client failed to send udp status packet", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        u.sessionID,
			"stream_id":         streamID,
		})
		return sacks[:0]
	}

	fields := internal.Fields{
		"session": u.sessionID,
		"stream":  streamID,
		"ack":     ackSeq,
	}
	if desc := formatSackRanges(sacks); desc != "" {
		fields["sacks"] = desc
	}
	internal.Debug("client udp status tx", fields)
	return sacks[:0]
}

func formatSackRanges(r []udpwire.SackRange) string {
	if len(r) == 0 {
		return ""
	}
	var b strings.Builder
	for i, rng := range r {
		if i > 0 {
			b.WriteByte(',')
		}
		if rng.Start == rng.End {
			fmt.Fprintf(&b, "%d", rng.Start)
			continue
		}
		fmt.Fprintf(&b, "%d-%d", rng.Start, rng.End)
	}
	return b.String()
}

func (u *udpConn) singleStreamID() (uint32, error) {
	s, err := u.singleStream()
	if err != nil {
		return 0, err
	}
	return s.id, nil
}

func (u *udpConn) singleStream() (*streamState, error) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if u.closed.Load() {
		return nil, errSessionClosed
	}
	if u.conn == nil {
		return nil, fmt.Errorf("nil UDP conn")
	}
	if u.bufPool == nil {
		return nil, fmt.Errorf("nil buffer pool")
	}

	var s *streamState
	for _, st := range u.streams {
		if s != nil {
			return nil, fmt.Errorf("operation not supported for multi-stream sessions yet")
		}
		s = st
	}
	if s == nil {
		return nil, fmt.Errorf("no streams configured for session")
	}
	return s, nil
}

func (u *udpConn) writeStream(ctx context.Context, streamID uint32, r io.Reader, size int64, overwrite backend.OverwritePolicy) error {
	if u.closed.Load() {
		return errSessionClosed
	}
	if u.rb == nil {
		return fmt.Errorf("nil tx ring buffer")
	}

	internal.Info("starting udp stream write", internal.Fields{
		"session":           u.sessionID,
		"stream":            streamID,
		"size_bytes":        size,
		"overwrite_policy":  describeOverwrite(overwrite),
		"payload_capacity":  u.payloadCapacity(),
		"tx_window_streams": len(u.txWindows),
	})

	payload := make([]byte, u.payloadCapacity())
	var seq uint32
	var offset uint64

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		n, readErr := r.Read(payload)
		if n > 0 {
			seq++
			if err := u.reserveWindow(ctx, streamID, seq, n); err != nil {
				return err
			}
			chunk := append([]byte(nil), payload[:n]...)
			pkt := txPacket{
				streamID: streamID,
				seq:      seq,
				offset:   offset,
				payload:  chunk,
			}
			internal.Debug("udpConn.writeStream enqueue", internal.Fields{
				"session": u.sessionID,
				"stream":  streamID,
				"seq":     seq,
				"offset":  offset,
				"bytes":   n,
			})
			if err := u.rb.Enqueue(ctx, pkt); err != nil {
				u.cancelWindow(streamID, seq)
				return err
			}
			offset += uint64(n)
			u.touch()
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return readErr
		}
	}

	u.rb.Close()
	if err := <-u.txDone; err != nil {
		return err
	}
	internal.Info("udp stream write complete", internal.Fields{
		"session":  u.sessionID,
		"stream":   streamID,
		"bytes_tx": offset,
		"seq":      seq,
	})
	return nil
}
