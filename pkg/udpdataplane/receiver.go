package udpdataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/metrics"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

type rangeTracker struct {
	expected   uint64
	contiguous uint64
	pending    []interval
}

type interval struct {
	start uint64
	end   uint64
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
