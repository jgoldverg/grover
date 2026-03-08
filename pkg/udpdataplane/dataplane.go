package udpdataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/metrics"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

const (
	defaultReadTimeout   = 2 * time.Second
	defaultWriteTimeout  = 2 * time.Second
	maxAckRetries        = 5
	enobufsRetryInterval = 5 * time.Millisecond
)

// SendConfig captures the parameters needed to transmit data over UDP.
type SendConfig struct {
	Transport  Transport
	RemoteAddr *net.UDPAddr
	SessionID  string
	SessionKey uint32
	StreamID   uint32
	MTU        int
	Collector  *metrics.TransferCollector
}

// ReceiveConfig controls how UDP payloads are ingested.
type ReceiveConfig struct {
	Transport    Transport
	RemoteAddr   *net.UDPAddr
	SessionID    string
	SessionKey   uint32
	StreamID     uint32
	BufferSize   int
	Collector    *metrics.TransferCollector
	ExpectedSize uint64
	// OnRemoteAddr is called the first time we learn the peer address (for unconnected sockets).
	OnRemoteAddr func(*net.UDPAddr)
}

type pendingPacket struct {
	seq        uint32
	payloadLen int
	data       []byte
	sentAt     time.Time
}

type interval struct {
	start uint64
	end   uint64
}

type rangeTracker struct {
	expected   uint64
	contiguous uint64
	pending    []interval
}

func newRangeTracker(expected uint64) *rangeTracker {
	if expected == 0 {
		return nil
	}
	return &rangeTracker{
		expected: expected,
		pending:  make([]interval, 0, 16),
	}
}

func (rt *rangeTracker) add(start, end uint64) bool {
	if rt == nil {
		return false
	}
	if end <= start || end <= rt.contiguous {
		return rt.contiguous >= rt.expected
	}
	if start <= rt.contiguous {
		rt.contiguous = end
		rt.absorbPending()
	} else {
		rt.insertPending(interval{start: start, end: end})
	}
	return rt.contiguous >= rt.expected
}

func (rt *rangeTracker) absorbPending() {
	if len(rt.pending) == 0 {
		return
	}
	idx := 0
	for idx < len(rt.pending) && rt.pending[idx].start <= rt.contiguous {
		if rt.pending[idx].end > rt.contiguous {
			rt.contiguous = rt.pending[idx].end
		}
		idx++
	}
	if idx > 0 {
		copy(rt.pending[0:], rt.pending[idx:])
		rt.pending = rt.pending[:len(rt.pending)-idx]
	}
}

func (rt *rangeTracker) insertPending(seg interval) {
	if seg.end <= seg.start {
		return
	}
	pos := 0
	for pos < len(rt.pending) && rt.pending[pos].start < seg.start {
		pos++
	}
	rt.pending = append(rt.pending, interval{})
	copy(rt.pending[pos+1:], rt.pending[pos:])
	rt.pending[pos] = seg
	rt.mergeFrom(pos)
}

func (rt *rangeTracker) mergeFrom(idx int) {
	if idx > 0 && rt.pending[idx-1].end >= rt.pending[idx].start {
		if rt.pending[idx].end > rt.pending[idx-1].end {
			rt.pending[idx-1].end = rt.pending[idx].end
		}
		copy(rt.pending[idx:], rt.pending[idx+1:])
		rt.pending = rt.pending[:len(rt.pending)-1]
		idx--
	}
	for idx+1 < len(rt.pending) && rt.pending[idx].end >= rt.pending[idx+1].start {
		if rt.pending[idx+1].end > rt.pending[idx].end {
			rt.pending[idx].end = rt.pending[idx+1].end
		}
		copy(rt.pending[idx+1:], rt.pending[idx+2:])
		rt.pending = rt.pending[:len(rt.pending)-1]
	}
}

// Sender defines the behavior for transmitting data over the UDP data plane.
type Sender interface {
	Send(ctx context.Context, cfg SendConfig, src io.Reader) (uint64, error)
}

// DefaultSender implements a SACK-aware congestion-controlled sender.
var DefaultSender Sender = NewBBRSender()

// Send reads from src and sends UDP data packets using the configured sender.
func Send(ctx context.Context, cfg SendConfig, src io.Reader) (uint64, error) {
	if DefaultSender == nil {
		return 0, errors.New("no UDP sender configured")
	}
	return DefaultSender.Send(ctx, cfg, src)
}

// Receive streams UDP payloads into dst and emits ACK/SACK status packets.
func Receive(ctx context.Context, cfg ReceiveConfig, dst io.WriterAt) (uint64, error) {
	if cfg.Transport == nil {
		return 0, errors.New("transport is required")
	}
	if dst == nil {
		return 0, errors.New("nil destination writer")
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 64 * 1024
	}
	buf := make([]byte, cfg.BufferSize)
	statusBuf := make([]byte, udpwire.StatusHeaderLen+udpwire.MaxSackRanges*udpwire.SackBlockLen)
	tracker := udpwire.NewSackTracker()
	var sackScratch []udpwire.SackRange
	var packet udpwire.DataPacket
	var total uint64
	progress := newRangeTracker(cfg.ExpectedSize)

	defer cfg.Transport.SetReadDeadline(time.Time{})
	remote := cfg.RemoteAddr

	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if err := setReadDeadline(ctx, cfg.Transport); err != nil {
			return total, err
		}
		n, addr, err := cfg.Transport.ReadPacket(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if isClosedNetworkError(err) || errors.Is(err, io.EOF) {
				return total, nil
			}
			return total, err
		}
		if n == 0 {
			continue
		}
		if remote == nil {
			remote = addr
			if cfg.OnRemoteAddr != nil {
				cfg.OnRemoteAddr(remote)
			}
		} else if addr != nil && !udpAddrEqual(addr, remote) {
			continue
		}
		packetBytes := buf[:n]
		if !udpwire.IsDataPacket(packetBytes) {
			continue
		}
		if _, err := packet.Decode(packetBytes); err != nil {
			continue
		}
		if packet.StreamID != cfg.StreamID {
			continue
		}
		if len(packet.Payload) > 0 {
			if _, err := dst.WriteAt(packet.Payload, int64(packet.Offset)); err != nil {
				return total, fmt.Errorf("write payload: %w", err)
			}
			end := packet.Offset + uint64(len(packet.Payload))
			if end > total {
				total = end
			}
			recordPacketReceive(cfg.Collector)
			recordReceiveMetric(cfg.Collector, len(packet.Payload))
			internal.Debug("udp data rx", internal.Fields{
				"session": cfg.SessionID,
				"stream":  cfg.StreamID,
				"seq":     packet.Seq,
				"bytes":   len(packet.Payload),
			})
			finished := false
			if progress != nil {
				finished = progress.add(packet.Offset, end)
			}
			if tracker.OnPacket(packet.Seq) {
				emitStatusPacket(cfg.Transport, remote, cfg.SessionID, cfg.SessionKey, cfg.StreamID, tracker, statusBuf, &sackScratch)
			}
			if finished {
				return total, nil
			}
			continue
		}
		if tracker.OnPacket(packet.Seq) {
			emitStatusPacket(cfg.Transport, remote, cfg.SessionID, cfg.SessionKey, cfg.StreamID, tracker, statusBuf, &sackScratch)
		}
	}
}

// NewSequentialWriter wraps an io.Writer to provide io.WriterAt semantics with buffering.
func NewSequentialWriter(w io.Writer) io.WriterAt {
	return &sequentialWriter{
		w:       w,
		pending: make(map[uint64][]byte),
	}
}

type sequentialWriter struct {
	w       io.Writer
	offset  uint64
	pending map[uint64][]byte
}

func (s *sequentialWriter) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset %d", off)
	}
	uoff := uint64(off)
	if uoff < s.offset {
		// already committed
		return len(p), nil
	}
	if uoff == s.offset {
		n, err := s.w.Write(p)
		if err != nil {
			return n, err
		}
		s.offset += uint64(n)
		s.flushPending()
		return n, nil
	}
	buf := make([]byte, len(p))
	copy(buf, p)
	s.pending[uoff] = buf
	return len(p), nil
}

func (s *sequentialWriter) flushPending() {
	for {
		buf, ok := s.pending[s.offset]
		if !ok {
			return
		}
		delete(s.pending, s.offset)
		n, err := s.w.Write(buf)
		if err != nil {
			internal.Error("sequential writer flush failed", internal.Fields{
				internal.FieldError: err.Error(),
			})
			return
		}
		s.offset += uint64(n)
	}
}

func emitStatusPacket(
	transport Transport,
	remote *net.UDPAddr,
	sessionID string,
	sessionKey uint32,
	streamID uint32,
	tracker *udpwire.SackTracker,
	statusBuf []byte,
	scratch *[]udpwire.SackRange,
) {
	if transport == nil || tracker == nil || len(statusBuf) < udpwire.StatusHeaderLen {
		return
	}
	ackSeq, sacks := tracker.Snapshot(udpwire.MaxSackRanges, *scratch)
	sp := udpwire.StatusPacket{
		SessionID: sessionKey,
		StreamID:  streamID,
		AckSeq:    ackSeq,
		Sacks:     sacks,
	}
	n, err := sp.Encode(statusBuf)
	if err != nil {
		return
	}
	if _, err := transport.WritePacket(statusBuf[:n], remote); err != nil {
		internal.Debug("failed to send udp status", internal.Fields{
			internal.FieldError: err.Error(),
			"session":           sessionID,
			"stream":            streamID,
		})
	}
	fields := internal.Fields{
		"session": sessionID,
		"stream":  streamID,
		"ack":     ackSeq,
	}
	if desc := describeSackRanges(sacks); desc != "" {
		fields["sacks"] = desc
	}
	internal.Debug("udp status tx", fields)
	if scratch != nil {
		*scratch = sacks[:0]
	}
}

func setReadDeadline(ctx context.Context, transport Transport) error {
	if transport == nil {
		return nil
	}
	if deadline, ok := ctx.Deadline(); ok {
		return transport.SetReadDeadline(deadline)
	}
	return transport.SetReadDeadline(time.Now().Add(defaultReadTimeout))
}

func setWriteDeadline(ctx context.Context, transport Transport) error {
	if transport == nil {
		return nil
	}
	if deadline, ok := ctx.Deadline(); ok {
		return transport.SetWriteDeadline(deadline)
	}
	return transport.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
}

func payloadSizeFromMTU(mtu int) int {
	if mtu <= 0 {
		mtu = 1500
	}
	payload := mtu - udpwire.DataHeaderLen - 4
	if payload < 256 {
		payload = 256
	}
	return payload
}

func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrClosed) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

func writePacketWithRetry(ctx context.Context, transport Transport, remote *net.UDPAddr, packet []byte) error {
	for {
		if err := setWriteDeadline(ctx, transport); err != nil {
			return err
		}
		if _, err := transport.WritePacket(packet, remote); err != nil {
			if isNoBufferSpaceErr(err) {
				internal.Debug("udp write hit ENOBUFS, backing off", internal.Fields{
					internal.FieldError: err.Error(),
				})
				if err := waitForRetry(ctx, enobufsRetryInterval); err != nil {
					return err
				}
				continue
			}
			return err
		}
		return nil
	}
}

func waitForRetry(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		d = time.Millisecond
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isNoBufferSpaceErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ENOBUFS) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.ENOBUFS) {
			return true
		}
		var sysErr *os.SyscallError
		if errors.As(opErr.Err, &sysErr) {
			return errors.Is(sysErr.Err, syscall.ENOBUFS)
		}
	}
	return strings.Contains(strings.ToLower(err.Error()), "no buffer space")
}

func drainStatusPackets(
	ctx context.Context,
	transport Transport,
	remote **net.UDPAddr,
	streamID uint32,
	ackBuf []byte,
	ackPkt *udpwire.StatusPacket,
	pending *[]pendingPacket,
	collector *metrics.TransferCollector,
	timeout time.Duration,
	nonBlocking bool,
	onAck func(time.Duration, int),
) (int, error) {
	if len(*pending) == 0 {
		return 0, nil
	}
	attempts := 0
	totalAcked := 0
	for len(*pending) > 0 {
		if nonBlocking {
			if timeout > 0 {
				if err := transport.SetReadDeadline(time.Now().Add(timeout)); err != nil {
					return totalAcked, err
				}
			} else {
				if err := transport.SetReadDeadline(time.Now()); err != nil {
					return totalAcked, err
				}
			}
		} else {
			if timeout > 0 {
				if err := transport.SetReadDeadline(time.Now().Add(timeout)); err != nil {
					return totalAcked, err
				}
			} else if err := setReadDeadline(ctx, transport); err != nil {
				return totalAcked, err
			}
		}
		readStart := time.Now()
		n, addr, err := transport.ReadPacket(ackBuf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if nonBlocking {
					return totalAcked, nil
				}
				attempts++
				if attempts >= maxAckRetries {
					lastSeq := (*pending)[len(*pending)-1].seq
					return totalAcked, fmt.Errorf("failed to receive ack for seq %d after %d attempts", lastSeq, maxAckRetries)
				}
				if err := retransmitPending(ctx, transport, *remote, pending, collector); err != nil {
					return totalAcked, err
				}
				continue
			}
			return totalAcked, err
		}
		if *remote == nil {
			*remote = addr
		} else if addr != nil && !udpAddrEqual(addr, *remote) {
			continue
		}
		if n == 0 || !udpwire.IsStatusPacket(ackBuf[:n]) {
			continue
		}
		if _, err := ackPkt.Decode(ackBuf[:n]); err != nil {
			continue
		}
		if ackPkt.StreamID != streamID {
			continue
		}
		attempts = 0
		acked := advancePendingWithAck(ackPkt, pending, collector, readStart, onAck)
		totalAcked += acked
		if nonBlocking && len(*pending) == 0 {
			return totalAcked, nil
		}
	}
	return totalAcked, nil
}

func advancePendingWithAck(
	pkt *udpwire.StatusPacket,
	pending *[]pendingPacket,
	collector *metrics.TransferCollector,
	ackReceived time.Time,
	onAck func(time.Duration, int),
) int {
	if len(*pending) == 0 {
		return 0
	}
	p := *pending
	idx := 0
	ackedBytes := 0
	for idx < len(p) && p[idx].seq <= pkt.AckSeq {
		ackedBytes += p[idx].payloadLen
		if onAck != nil && !p[idx].sentAt.IsZero() {
			if sample := ackReceived.Sub(p[idx].sentAt); sample > 0 {
				recordAckMetric(collector, sample)
				onAck(sample, p[idx].payloadLen)
			}
		}
		idx++
	}
	p = p[idx:]
	if len(pkt.Sacks) > 0 && len(p) > 0 {
		keep := p[:0]
		for _, cur := range p {
			acked := false
			for _, sack := range pkt.Sacks {
				if cur.seq >= sack.Start && cur.seq <= sack.End {
					acked = true
					break
				}
			}
			if !acked {
				keep = append(keep, cur)
			} else if !cur.sentAt.IsZero() {
				ackedBytes += cur.payloadLen
				if sample := ackReceived.Sub(cur.sentAt); sample > 0 {
					recordAckMetric(collector, sample)
					if onAck != nil {
						onAck(sample, cur.payloadLen)
					}
				}
			} else {
				ackedBytes += cur.payloadLen
			}
		}
		p = keep
	}
	*pending = p
	return ackedBytes
}

func retransmitPending(ctx context.Context, transport Transport, remote *net.UDPAddr, pending *[]pendingPacket, collector *metrics.TransferCollector) error {
	for i := range *pending {
		pkt := &(*pending)[i]
		if err := writePacketWithRetry(ctx, transport, remote, pkt.data); err != nil {
			return err
		}
		recordSendMetric(collector, pkt.payloadLen, true)
		recordPacketSend(collector)
		pkt.sentAt = time.Now()
	}
	return nil
}

func waitForAck(
	ctx context.Context,
	transport Transport,
	remote *net.UDPAddr,
	streamID uint32,
	seq uint32,
	buf []byte,
	pkt *udpwire.StatusPacket,
) error {
	if transport == nil || len(buf) == 0 || pkt == nil {
		return fmt.Errorf("invalid ack buffer")
	}
	if err := setReadDeadline(ctx, transport); err != nil {
		return err
	}
	n, addr, err := transport.ReadPacket(buf)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return ne
		}
		return err
	}
	if remote != nil && addr != nil {
		if !addr.IP.Equal(remote.IP) || addr.Port != remote.Port {
			return fmt.Errorf("ack from unexpected peer")
		}
	}
	if n == 0 {
		return fmt.Errorf("empty ack packet")
	}
	if !udpwire.IsStatusPacket(buf[:n]) {
		return fmt.Errorf("unexpected packet while waiting for ack")
	}
	if _, err := pkt.Decode(buf[:n]); err != nil {
		return err
	}
	if pkt.StreamID != streamID {
		return fmt.Errorf("ack for wrong stream %d (want %d)", pkt.StreamID, streamID)
	}
	if pkt.AckSeq < seq {
		return fmt.Errorf("stale ack seq %d (want >= %d)", pkt.AckSeq, seq)
	}
	return nil
}

func retrySendPacket(
	ctx context.Context,
	transport Transport,
	remote *net.UDPAddr,
	packet []byte,
	streamID uint32,
	seq uint32,
	buf []byte,
	pkt *udpwire.StatusPacket,
	payloadBytes int,
	collector *metrics.TransferCollector,
) error {
	for attempt := 0; attempt < maxAckRetries; attempt++ {
		if err := writePacketWithRetry(ctx, transport, remote, packet); err != nil {
			return err
		}
		recordPacketSend(collector)
		recordSendMetric(collector, payloadBytes, true)
		ackStart := time.Now()
		if err := waitForAck(ctx, transport, remote, streamID, seq, buf, pkt); err == nil {
			recordAckMetric(collector, time.Since(ackStart))
			return nil
		}
		if err := waitForRetry(ctx, 5*time.Millisecond*(1<<attempt)); err != nil {
			return err
		}
	}
	return fmt.Errorf("failed to receive ack for seq %d after %d attempts", seq, maxAckRetries)
}

func recordSendMetric(col *metrics.TransferCollector, bytes int, retrans bool) {
	if col == nil || bytes <= 0 {
		return
	}
	col.ObserveSend(bytes, retrans)
}

func recordReceiveMetric(col *metrics.TransferCollector, bytes int) {
	if col == nil || bytes <= 0 {
		return
	}
	col.ObserveReceive(bytes)
}

func recordPacketSend(col *metrics.TransferCollector) {
	if col == nil {
		return
	}
	col.ObservePacketSend()
}

func recordPacketReceive(col *metrics.TransferCollector) {
	if col == nil {
		return
	}
	col.ObservePacketReceive()
}

func recordAckMetric(col *metrics.TransferCollector, d time.Duration) {
	if col == nil || d <= 0 {
		return
	}
	col.ObserveAck(d)
}

func describeSackRanges(r []udpwire.SackRange) string {
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

func udpAddrEqual(a, b *net.UDPAddr) bool {
	if a == nil || b == nil {
		return false
	}
	return a.IP.Equal(b.IP) && a.Port == b.Port
}
