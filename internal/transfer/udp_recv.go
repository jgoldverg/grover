package transfer

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/tuning"
	"github.com/jgoldverg/grover/internal/udpdataplane"
	"github.com/jgoldverg/grover/internal/udpwire"
	"github.com/jgoldverg/grover/internal/wire"
)

const (
	udpSessionQueue   = 8192 // packets buffered per file session before drops
	udpFinishedRetain = 5 * time.Minute
)

// udpEndpoint is a destination UDP socket. One goroutine reads the socket and
// routes datagrams by session key to per-file receive sessions, so many files
// can be in flight at once. START packets create sessions; repeated STARTs are
// answered with the session's current state (READY, DONE, or ERROR).
type udpEndpoint struct {
	conn     *net.UDPConn
	token    []byte
	recv     *receiver
	tuning   tuning.Tuning
	ctx      context.Context
	dropped  atomic.Uint64
	mu       sync.Mutex
	sessions map[uint32]*udpSession
	finished map[uint32]udpFinished
}

type udpSession struct {
	ch chan udpPacket
}

type udpFinished struct {
	err string
	at  time.Time
}

type udpPacket struct {
	data []byte
	addr *net.UDPAddr
}

func newUDPEndpoint(ctx context.Context, conn *net.UDPConn, token []byte, recv *receiver, t tuning.Tuning) *udpEndpoint {
	setUDPBuffers(conn, t.UDPSocketRead, t.UDPSocketWrite)
	return &udpEndpoint{
		conn: conn, token: token, recv: recv, tuning: t, ctx: ctx,
		sessions: map[uint32]*udpSession{}, finished: map[uint32]udpFinished{},
	}
}

// serve reads until the socket is closed.
func (e *udpEndpoint) serve() {
	defer e.closeSessions()
	transport := udpdataplane.NewUDPConnTransport(e.conn)
	const bufSize = 64 << 10
	if runtime.GOOS == "linux" {
		batch := make([]udpdataplane.PacketBuffer, 64)
		for i := range batch {
			batch[i].Bytes = make([]byte, bufSize)
		}
		for {
			for i := range batch {
				batch[i].N, batch[i].Addr = 0, nil
			}
			n, err := transport.ReadBatch(batch)
			for i := 0; i < n; i++ {
				if batch[i].N > 0 {
					e.dispatch(batch[i].Bytes[:batch[i].N], batch[i].Addr)
				}
			}
			if err != nil && n == 0 && !isTimeoutErr(err) {
				return
			}
		}
	}
	buf := make([]byte, bufSize)
	for {
		n, addr, err := e.conn.ReadFromUDP(buf)
		if err != nil {
			if isTimeoutErr(err) {
				continue
			}
			return
		}
		e.dispatch(buf[:n], addr)
	}
}

func (e *udpEndpoint) dispatch(p []byte, addr *net.UDPAddr) {
	if udpwire.IsDataPacket(p) && len(p) >= 6 {
		key := binary.BigEndian.Uint32(p[2:6])
		e.mu.Lock()
		s := e.sessions[key]
		e.mu.Unlock()
		if s == nil {
			return
		}
		pkt := udpPacket{data: append([]byte(nil), p...), addr: addr}
		select {
		case s.ch <- pkt:
		default:
			e.dropped.Add(1) // the sender will retransmit
		}
		return
	}
	start, ok, err := wire.DecodeUDPStart(p)
	if !ok {
		return
	}
	if err != nil {
		e.reply(wire.UDPError, start.SessionKey, err.Error(), addr)
		return
	}
	e.handleStart(start, addr)
}

func (e *udpEndpoint) handleStart(s wire.UDPStartPacket, addr *net.UDPAddr) {
	if !wire.TokenEqual(s.Token, e.token) {
		e.reply(wire.UDPError, s.SessionKey, "bad data token", addr)
		return
	}
	e.mu.Lock()
	if _, active := e.sessions[s.SessionKey]; active {
		e.mu.Unlock()
		e.reply(wire.UDPReady, s.SessionKey, "", addr)
		return
	}
	if f, ok := e.finished[s.SessionKey]; ok {
		e.mu.Unlock()
		if f.err != "" {
			e.reply(wire.UDPError, s.SessionKey, f.err, addr)
		} else {
			e.reply(wire.UDPDone, s.SessionKey, "", addr)
		}
		return
	}
	for k, f := range e.finished {
		if time.Since(f.at) > udpFinishedRetain {
			delete(e.finished, k)
		}
	}
	sess := &udpSession{ch: make(chan udpPacket, udpSessionQueue)}
	e.sessions[s.SessionKey] = sess
	e.mu.Unlock()
	go e.receiveFile(s, addr, sess)
}

func (e *udpEndpoint) receiveFile(s wire.UDPStartPacket, ctrl *net.UDPAddr, sess *udpSession) {
	handle := e.recv.begin(receiveInfo{
		JobID: s.JobID, RouteID: s.RouteID, Destination: s.Destination, RelativePath: s.RelativePath,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_UDP, FileSize: s.FileSize, StreamID: 1, Length: s.FileSize,
	})
	err := e.receiveInto(s, ctrl, sess, handle)
	handle.Done(err)
	e.mu.Lock()
	delete(e.sessions, s.SessionKey)
	f := udpFinished{at: time.Now()}
	if err != nil {
		f.err = err.Error()
	}
	e.finished[s.SessionKey] = f
	e.mu.Unlock()
	if err != nil {
		slog.Warn("udp receive failed", "job_id", s.JobID, "path", s.RelativePath, "remote", ctrl.String(), "error", err)
		e.reply(wire.UDPError, s.SessionKey, err.Error(), ctrl)
	}
}

func (e *udpEndpoint) receiveInto(s wire.UDPStartPacket, ctrl *net.UDPAddr, sess *udpSession, handle *receiveHandle) error {
	f, err := openDestination(s.Destination, s.RelativePath, s.FileSize)
	if err != nil {
		return err
	}
	defer f.Close()
	e.reply(wire.UDPReady, s.SessionKey, "", ctrl)
	complete := func() error {
		if err := f.Sync(); err != nil {
			return err
		}
		e.mu.Lock()
		e.finished[s.SessionKey] = udpFinished{at: time.Now()}
		e.mu.Unlock()
		e.reply(wire.UDPDone, s.SessionKey, "", ctrl)
		return nil
	}
	if s.FileSize == 0 {
		return complete()
	}
	dst := &countingWriterAt{w: f, onWrite: handle.Add}
	t := e.tuning.Merge(tuning.Tuning{UDPAckEveryPackets: s.AckEveryPackets, UDPAckEveryMs: s.AckEveryMs, UDPWriteFlushBytes: s.WriteFlushBytes})
	_, err = udpdataplane.Receive(e.ctx, udpdataplane.ReceiveConfig{
		Transport:       &chanTransport{ch: sess.ch, conn: e.conn},
		SessionKey:      s.SessionKey,
		StreamIDs:       s.StreamIDs,
		Collector:       handle.j.counters,
		ExpectedSize:    s.FileSize,
		AckEveryPackets: int(t.UDPAckEveryPackets),
		AckEvery:        t.UDPAckEvery(),
		BatchPackets:    int(t.UDPBatchPackets),
		WriteFlushBytes: t.UDPWriteFlushBytes,
		OnComplete:      complete,
	}, dst)
	return err
}

func (e *udpEndpoint) reply(kind byte, key uint32, msg string, addr *net.UDPAddr) {
	if addr != nil {
		_, _ = e.conn.WriteToUDP(wire.EncodeUDPControl(kind, key, msg), addr)
	}
}

func (e *udpEndpoint) closeSessions() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for k, s := range e.sessions {
		close(s.ch)
		delete(e.sessions, k)
	}
}

// chanTransport feeds one session's datagrams to udpdataplane.Receive and
// writes replies through the shared socket.
type chanTransport struct {
	ch       chan udpPacket
	conn     *net.UDPConn
	deadline time.Time
}

func (t *chanTransport) ReadPacket(buf []byte) (int, *net.UDPAddr, error) {
	var timeout <-chan time.Time
	if !t.deadline.IsZero() {
		d := time.Until(t.deadline)
		if d <= 0 {
			return 0, nil, os.ErrDeadlineExceeded
		}
		timer := time.NewTimer(d)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case p, ok := <-t.ch:
		if !ok {
			return 0, nil, net.ErrClosed
		}
		return copy(buf, p.data), p.addr, nil
	case <-timeout:
		return 0, nil, os.ErrDeadlineExceeded
	}
}

func (t *chanTransport) WritePacket(p []byte, to *net.UDPAddr) (int, error) {
	return t.conn.WriteToUDP(p, to)
}
func (t *chanTransport) SetReadDeadline(d time.Time) error { t.deadline = d; return nil }
func (t *chanTransport) SetWriteDeadline(time.Time) error  { return nil }
func (t *chanTransport) RemoteAddr() *net.UDPAddr          { return nil }

type countingWriterAt struct {
	w       io.WriterAt
	onWrite func(int)
}

func (c *countingWriterAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := c.w.WriteAt(p, off)
	if n > 0 {
		c.onWrite(n)
	}
	return n, err
}

func isTimeoutErr(err error) bool {
	var ne net.Error
	return errors.Is(err, os.ErrDeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}
