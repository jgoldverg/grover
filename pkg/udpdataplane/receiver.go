package udpdataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

type rangeTracker struct {
	expected   uint64
	contiguous uint64
	pending    []interval
	covered    uint64
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
	_ = rt.addCovered(start, end)
	return rt.complete()
}

func (rt *rangeTracker) addCovered(start, end uint64) uint64 {
	if rt == nil {
		return 0
	}
	if end <= start {
		return 0
	}
	if rt.expected > 0 && end > rt.expected {
		end = rt.expected
	}
	if end <= start {
		return 0
	}
	added := end - start
	if start < rt.contiguous {
		overlapEnd := minUint64(end, rt.contiguous)
		added -= overlapEnd - start
	}
	if end <= rt.contiguous {
		return 0
	}
	for _, cur := range rt.pending {
		if cur.end <= start || cur.start >= end {
			continue
		}
		overlapStart := maxUint64(start, cur.start)
		overlapEnd := minUint64(end, cur.end)
		if overlapEnd > overlapStart {
			added -= overlapEnd - overlapStart
		}
	}
	if start <= rt.contiguous {
		rt.contiguous = end
		rt.absorbPending()
	} else {
		rt.insertPending(interval{start: start, end: end})
	}
	rt.covered += added
	return added
}

func (rt *rangeTracker) complete() bool {
	if rt == nil {
		return false
	}
	return rt.expected > 0 && rt.contiguous >= rt.expected
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

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
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
	acks := newAckCoalescer(cfg.AckEveryPackets, cfg.AckEvery)
	batchTransport, useBatch := cfg.Transport.(BatchTransport)
	useBatch = useBatch && runtime.GOOS == "linux"
	batchMax := batchPacketCount(cfg.BatchPackets)
	batch := make([]PacketBuffer, 0)
	if useBatch {
		batch = make([]PacketBuffer, batchMax)
		for i := range batch {
			batch[i].Bytes = make([]byte, cfg.BufferSize)
		}
	}

	defer cfg.Transport.SetReadDeadline(time.Time{})
	remote := cfg.RemoteAddr

	processPacket := func(packetBytes []byte, addr *net.UDPAddr) (bool, error) {
		if len(packetBytes) == 0 || !udpwire.IsDataPacket(packetBytes) {
			return false, nil
		}
		if remote == nil {
			remote = addr
			if cfg.OnRemoteAddr != nil {
				cfg.OnRemoteAddr(remote)
			}
		} else if addr != nil && !udpAddrEqual(addr, remote) {
			return false, nil
		}
		if _, err := packet.Decode(packetBytes); err != nil {
			return false, nil
		}
		if packet.StreamID != cfg.StreamID {
			return false, nil
		}
		if len(packet.Payload) > 0 {
			recordNetworkReceiveMetric(cfg.Collector, len(packet.Payload))
			payloadLen := len(packet.Payload)
			if cfg.ExpectedSize > 0 {
				if packet.Offset >= cfg.ExpectedSize {
					payloadLen = 0
				} else if packet.Offset+uint64(payloadLen) > cfg.ExpectedSize {
					payloadLen = int(cfg.ExpectedSize - packet.Offset)
				}
			}
			if payloadLen > 0 {
				n, err := dst.WriteAt(packet.Payload[:payloadLen], int64(packet.Offset))
				if err != nil {
					return false, fmt.Errorf("write payload: %w", err)
				}
				if n != payloadLen {
					return false, io.ErrShortWrite
				}
			}
			end := packet.Offset + uint64(payloadLen)
			added := uint64(0)
			finished := false
			if progress != nil {
				added = progress.addCovered(packet.Offset, end)
				finished = progress.complete()
				total = progress.covered
			} else if end > total {
				total = end
				added = uint64(payloadLen)
			}
			if added > 0 {
				recordPacketReceive(cfg.Collector)
				recordReceiveMetric(cfg.Collector, int(added))
			}
			if shouldLogUDPPacket(packet.Seq) {
				internal.Debug("udp data rx sample", internal.Fields{
					"session": cfg.SessionID,
					"stream":  cfg.StreamID,
					"seq":     packet.Seq,
					"bytes":   payloadLen,
				})
			}
			beforeAck, _ := tracker.Snapshot(0, nil)
			changed := tracker.OnPacket(packet.Seq)
			gap := packet.Seq > beforeAck+1
			if changed && acks.OnPacket(gap) {
				emitStatusPacket(cfg.Transport, remote, cfg.SessionID, cfg.SessionKey, cfg.StreamID, tracker, statusBuf, &sackScratch)
				acks.MarkSent()
			}
			return finished, nil
		}
		beforeAck, _ := tracker.Snapshot(0, nil)
		changed := tracker.OnPacket(packet.Seq)
		gap := packet.Seq > beforeAck+1
		if changed && acks.OnPacket(gap) {
			emitStatusPacket(cfg.Transport, remote, cfg.SessionID, cfg.SessionKey, cfg.StreamID, tracker, statusBuf, &sackScratch)
			acks.MarkSent()
		}
		return false, nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if err := setReceiveDeadline(ctx, cfg.Transport, acks); err != nil {
			return total, err
		}
		if useBatch {
			for i := range batch {
				batch[i].N = 0
				batch[i].Addr = nil
			}
			n, err := batchTransport.ReadBatch(batch)
			if err != nil && n == 0 {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					if remote != nil && acks.ShouldFlush() {
						emitStatusPacket(cfg.Transport, remote, cfg.SessionID, cfg.SessionKey, cfg.StreamID, tracker, statusBuf, &sackScratch)
						acks.MarkSent()
					}
					continue
				}
				if isClosedNetworkError(err) || errors.Is(err, io.EOF) {
					return total, nil
				}
				return total, err
			}
			for i := 0; i < n; i++ {
				if batch[i].N <= 0 {
					continue
				}
				finished, err := processPacket(batch[i].Bytes[:batch[i].N], batch[i].Addr)
				if err != nil {
					return total, err
				}
				if finished {
					emitStatusPacket(cfg.Transport, remote, cfg.SessionID, cfg.SessionKey, cfg.StreamID, tracker, statusBuf, &sackScratch)
					lingerStatus(ctx, cfg, remote, tracker, statusBuf, &sackScratch, buf, &packet)
					return total, nil
				}
			}
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					if remote != nil && acks.ShouldFlush() {
						emitStatusPacket(cfg.Transport, remote, cfg.SessionID, cfg.SessionKey, cfg.StreamID, tracker, statusBuf, &sackScratch)
						acks.MarkSent()
					}
					continue
				}
				if isClosedNetworkError(err) || errors.Is(err, io.EOF) {
					return total, nil
				}
				return total, err
			}
			continue
		}

		n, addr, err := cfg.Transport.ReadPacket(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if remote != nil && acks.ShouldFlush() {
					emitStatusPacket(cfg.Transport, remote, cfg.SessionID, cfg.SessionKey, cfg.StreamID, tracker, statusBuf, &sackScratch)
					acks.MarkSent()
				}
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
		finished, err := processPacket(buf[:n], addr)
		if err != nil {
			return total, err
		}
		if finished {
			emitStatusPacket(cfg.Transport, remote, cfg.SessionID, cfg.SessionKey, cfg.StreamID, tracker, statusBuf, &sackScratch)
			lingerStatus(ctx, cfg, remote, tracker, statusBuf, &sackScratch, buf, &packet)
			return total, nil
		}
	}
}

type receiveStreamState struct {
	tracker     *udpwire.SackTracker
	acks        *ackCoalescer
	remote      *net.UDPAddr
	sackScratch []udpwire.SackRange
}

func ReceiveMany(ctx context.Context, cfg ReceiveConfig, dst io.WriterAt) (uint64, error) {
	if cfg.Transport == nil {
		return 0, errors.New("transport is required")
	}
	if dst == nil {
		return 0, errors.New("nil destination writer")
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 64 * 1024
	}
	streamIDs := cfg.StreamIDs
	if len(streamIDs) == 0 && cfg.StreamID != 0 {
		streamIDs = []uint32{cfg.StreamID}
	}
	if len(streamIDs) == 0 {
		return 0, errors.New("at least one stream id is required")
	}

	states := make(map[uint32]*receiveStreamState, len(streamIDs))
	for _, id := range streamIDs {
		if id == 0 {
			continue
		}
		states[id] = &receiveStreamState{
			tracker: udpwire.NewSackTracker(),
			acks:    newAckCoalescer(cfg.AckEveryPackets, cfg.AckEvery),
		}
	}
	if len(states) == 0 {
		return 0, errors.New("at least one non-zero stream id is required")
	}

	buf := make([]byte, cfg.BufferSize)
	statusBuf := make([]byte, udpwire.StatusHeaderLen+udpwire.MaxSackRanges*udpwire.SackBlockLen)
	var packet udpwire.DataPacket
	var total uint64
	progress := newRangeTracker(cfg.ExpectedSize)
	batchTransport, useBatch := cfg.Transport.(BatchTransport)
	useBatch = useBatch && runtime.GOOS == "linux"
	batchMax := batchPacketCount(cfg.BatchPackets)
	batch := make([]PacketBuffer, 0)
	if useBatch {
		batch = make([]PacketBuffer, batchMax)
		for i := range batch {
			batch[i].Bytes = make([]byte, cfg.BufferSize)
		}
	}

	processPacket := func(packetBytes []byte, addr *net.UDPAddr) (bool, error) {
		if len(packetBytes) == 0 || !udpwire.IsDataPacket(packetBytes) {
			return false, nil
		}
		if _, err := packet.Decode(packetBytes); err != nil {
			return false, nil
		}
		state := states[packet.StreamID]
		if state == nil {
			return false, nil
		}
		if state.remote == nil {
			state.remote = addr
		} else if addr != nil && !udpAddrEqual(addr, state.remote) {
			return false, nil
		}
		if len(packet.Payload) == 0 {
			beforeAck, _ := state.tracker.Snapshot(0, nil)
			changed := state.tracker.OnPacket(packet.Seq)
			gap := packet.Seq > beforeAck+1
			if changed && state.acks.OnPacket(gap) {
				emitStatusPacket(cfg.Transport, state.remote, cfg.SessionID, cfg.SessionKey, packet.StreamID, state.tracker, statusBuf, &state.sackScratch)
				state.acks.MarkSent()
			}
			return false, nil
		}

		recordNetworkReceiveMetric(cfg.Collector, len(packet.Payload))
		payloadLen := len(packet.Payload)
		if cfg.ExpectedSize > 0 {
			if packet.Offset >= cfg.ExpectedSize {
				payloadLen = 0
			} else if packet.Offset+uint64(payloadLen) > cfg.ExpectedSize {
				payloadLen = int(cfg.ExpectedSize - packet.Offset)
			}
		}
		if payloadLen > 0 {
			n, err := dst.WriteAt(packet.Payload[:payloadLen], int64(packet.Offset))
			if err != nil {
				return false, fmt.Errorf("write payload: %w", err)
			}
			if n != payloadLen {
				return false, io.ErrShortWrite
			}
		}
		end := packet.Offset + uint64(payloadLen)
		added := uint64(0)
		finished := false
		if progress != nil {
			added = progress.addCovered(packet.Offset, end)
			finished = progress.complete()
			total = progress.covered
		} else if end > total {
			total = end
			added = uint64(payloadLen)
		}
		if added > 0 {
			recordPacketReceive(cfg.Collector)
			recordReceiveMetric(cfg.Collector, int(added))
		}
		if shouldLogUDPPacket(packet.Seq) {
			internal.Debug("udp data rx sample", internal.Fields{
				"session": cfg.SessionID,
				"stream":  packet.StreamID,
				"seq":     packet.Seq,
				"bytes":   payloadLen,
			})
		}
		beforeAck, _ := state.tracker.Snapshot(0, nil)
		changed := state.tracker.OnPacket(packet.Seq)
		gap := packet.Seq > beforeAck+1
		if changed && state.acks.OnPacket(gap) {
			emitStatusPacket(cfg.Transport, state.remote, cfg.SessionID, cfg.SessionKey, packet.StreamID, state.tracker, statusBuf, &state.sackScratch)
			state.acks.MarkSent()
		}
		return finished, nil
	}

	defer cfg.Transport.SetReadDeadline(time.Time{})
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if err := setReceiveManyDeadline(ctx, cfg.Transport, states); err != nil {
			return total, err
		}
		if useBatch {
			for i := range batch {
				batch[i].N = 0
				batch[i].Addr = nil
			}
			n, err := batchTransport.ReadBatch(batch)
			if err != nil && n == 0 {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					flushDueManyStatus(cfg, states, statusBuf)
					continue
				}
				if isClosedNetworkError(err) || errors.Is(err, io.EOF) {
					return total, nil
				}
				return total, err
			}
			for i := 0; i < n; i++ {
				if batch[i].N <= 0 {
					continue
				}
				finished, err := processPacket(batch[i].Bytes[:batch[i].N], batch[i].Addr)
				if err != nil {
					return total, err
				}
				if finished {
					for streamID, st := range states {
						if st.remote != nil {
							emitStatusPacket(cfg.Transport, st.remote, cfg.SessionID, cfg.SessionKey, streamID, st.tracker, statusBuf, &st.sackScratch)
						}
					}
					lingerManyStatus(ctx, cfg, states, statusBuf, buf, &packet)
					return total, nil
				}
			}
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					flushDueManyStatus(cfg, states, statusBuf)
					continue
				}
				if isClosedNetworkError(err) || errors.Is(err, io.EOF) {
					return total, nil
				}
				return total, err
			}
			continue
		}

		n, addr, err := cfg.Transport.ReadPacket(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				flushDueManyStatus(cfg, states, statusBuf)
				continue
			}
			if isClosedNetworkError(err) || errors.Is(err, io.EOF) {
				return total, nil
			}
			return total, err
		}
		finished, err := processPacket(buf[:n], addr)
		if err != nil {
			return total, err
		}
		if finished {
			for streamID, st := range states {
				if st.remote != nil {
					emitStatusPacket(cfg.Transport, st.remote, cfg.SessionID, cfg.SessionKey, streamID, st.tracker, statusBuf, &st.sackScratch)
				}
			}
			lingerManyStatus(ctx, cfg, states, statusBuf, buf, &packet)
			return total, nil
		}
	}
}

func setReceiveDeadline(ctx context.Context, transport Transport, acks *ackCoalescer) error {
	if transport == nil {
		return nil
	}
	deadline := time.Now().Add(defaultReadTimeout)
	if acks != nil && acks.Pending() {
		if next := acks.NextDeadline(); !next.IsZero() && next.Before(deadline) {
			deadline = next
		}
	}
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	return transport.SetReadDeadline(deadline)
}

func setReceiveManyDeadline(ctx context.Context, transport Transport, states map[uint32]*receiveStreamState) error {
	if transport == nil {
		return nil
	}
	deadline := time.Now().Add(defaultReadTimeout)
	for _, st := range states {
		if st == nil || st.acks == nil || !st.acks.Pending() {
			continue
		}
		if next := st.acks.NextDeadline(); !next.IsZero() && next.Before(deadline) {
			deadline = next
		}
	}
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	return transport.SetReadDeadline(deadline)
}

func flushDueManyStatus(cfg ReceiveConfig, states map[uint32]*receiveStreamState, statusBuf []byte) {
	for streamID, st := range states {
		if st == nil || st.remote == nil || !st.acks.ShouldFlush() {
			continue
		}
		emitStatusPacket(cfg.Transport, st.remote, cfg.SessionID, cfg.SessionKey, streamID, st.tracker, statusBuf, &st.sackScratch)
		st.acks.MarkSent()
	}
}

func lingerStatus(
	ctx context.Context,
	cfg ReceiveConfig,
	remote *net.UDPAddr,
	tracker *udpwire.SackTracker,
	statusBuf []byte,
	sackScratch *[]udpwire.SackRange,
	buf []byte,
	packet *udpwire.DataPacket,
) {
	if cfg.Transport == nil || tracker == nil || remote == nil || packet == nil {
		return
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return
		}
		_ = cfg.Transport.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
		n, addr, err := cfg.Transport.ReadPacket(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		if addr != nil && !udpAddrEqual(addr, remote) {
			continue
		}
		if n == 0 || !udpwire.IsDataPacket(buf[:n]) {
			continue
		}
		if _, err := packet.Decode(buf[:n]); err != nil {
			continue
		}
		if packet.StreamID != cfg.StreamID {
			continue
		}
		_ = tracker.OnPacket(packet.Seq)
		emitStatusPacket(cfg.Transport, remote, cfg.SessionID, cfg.SessionKey, cfg.StreamID, tracker, statusBuf, sackScratch)
	}
}

func lingerManyStatus(
	ctx context.Context,
	cfg ReceiveConfig,
	states map[uint32]*receiveStreamState,
	statusBuf []byte,
	buf []byte,
	packet *udpwire.DataPacket,
) {
	if cfg.Transport == nil || packet == nil {
		return
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return
		}
		_ = cfg.Transport.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
		n, addr, err := cfg.Transport.ReadPacket(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		if n == 0 || !udpwire.IsDataPacket(buf[:n]) {
			continue
		}
		if _, err := packet.Decode(buf[:n]); err != nil {
			continue
		}
		state := states[packet.StreamID]
		if state == nil || state.remote == nil {
			continue
		}
		if addr != nil && !udpAddrEqual(addr, state.remote) {
			continue
		}
		_ = state.tracker.OnPacket(packet.Seq)
		emitStatusPacket(cfg.Transport, state.remote, cfg.SessionID, cfg.SessionKey, packet.StreamID, state.tracker, statusBuf, &state.sackScratch)
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
		fields["sack_count"] = len(sacks)
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
	const maxLoggedSackRanges = 16
	limit := len(r)
	if limit > maxLoggedSackRanges {
		limit = maxLoggedSackRanges
	}
	var b strings.Builder
	for i, rng := range r[:limit] {
		if i > 0 {
			b.WriteByte(',')
		}
		if rng.Start == rng.End {
			fmt.Fprintf(&b, "%d", rng.Start)
			continue
		}
		fmt.Fprintf(&b, "%d-%d", rng.Start, rng.End)
	}
	if len(r) > limit {
		fmt.Fprintf(&b, ",...(+%d more)", len(r)-limit)
	}
	return b.String()
}

func shouldLogUDPPacket(seq uint32) bool {
	const sampleEvery = 4096
	return seq == 0 || seq%sampleEvery == 0
}
