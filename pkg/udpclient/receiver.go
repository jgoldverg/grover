package udpclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

type Receiver struct {
	pc      net.PacketConn
	session SessionParams

	mu      sync.Mutex
	streams map[uint32]*sinkState
}

type sinkState struct {
	startOffset uint64
	chunkSize   int
	sizeBytes   int64
	callbacks   StreamCallbacks

	expectedSeq uint32
	highestSeq  uint32
	received    map[uint32]struct{}
	totalChunks uint32
	done        chan struct{}
	completed   bool
}

func NewReceiver(pc net.PacketConn, session SessionParams) *Receiver {
	return &Receiver{
		pc:      pc,
		session: session,
		streams: make(map[uint32]*sinkState),
	}
}

func (r *Receiver) RegisterStream(streamID uint32, startOffset uint64, sizeBytes int64, chunkSize int, cb StreamCallbacks) error {
	if cb.OnChunk == nil {
		return errors.New("stream callback OnChunk required")
	}
	if chunkSize <= 0 {
		return errors.New("chunk size must be positive")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.streams[streamID]; exists {
		return fmt.Errorf("stream %d already registered", streamID)
	}
	total := computeChunkCount(sizeBytes, chunkSize)
	r.streams[streamID] = &sinkState{
		startOffset: startOffset,
		chunkSize:   chunkSize,
		sizeBytes:   sizeBytes,
		callbacks:   cb,
		received:    make(map[uint32]struct{}),
		totalChunks: total,
		done:        make(chan struct{}),
	}
	return nil
}

func (r *Receiver) Run(ctx context.Context) error {
	if r.session.RemoteAddr == nil {
		return errors.New("remote addr required for receiver")
	}
	buf := make([]byte, 64*1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		timeout := r.session.AckInterval
		if timeout <= 0 {
			timeout = 200 * time.Millisecond
		}
		_ = r.pc.SetReadDeadline(time.Now().Add(timeout))

		n, addr, err := r.pc.ReadFrom(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}

		if !matchAddr(addr, r.session.RemoteAddr) {
			continue
		}

		payload := buf[:n]
		if !udpwire.IsDataPacket(payload) {
			continue
		}
		if err := r.handleData(payload); err != nil {
			internal.Warn("receiver data error", internal.Fields{
				internal.FieldError: err.Error(),
			})
		}
	}
}

func (r *Receiver) handleData(packet []byte) error {
	var pkt udpwire.DataPacket
	if _, err := pkt.Decode(packet); err != nil {
		return err
	}

	r.mu.Lock()
	state, ok := r.streams[pkt.StreamID]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("unknown stream %d", pkt.StreamID)
	}

	offset := state.startOffset + uint64(pkt.Seq)*uint64(state.chunkSize)
	payloadCopy := append([]byte(nil), pkt.Payload...)

	if err := state.callbacks.OnChunk(offset, payloadCopy); err != nil {
		internal.Warn("receiver chunk handler error", internal.Fields{
			internal.FieldError: err.Error(),
		})
	}

	ack, sacks, completedNow := state.record(pkt.Seq)
	cb := state.callbacks
	r.mu.Unlock()

	r.sendStatus(pkt.SessionID, pkt.StreamID, ack, sacks)

	if completedNow && cb.OnComplete != nil {
		cb.OnComplete()
	}
	return nil
}

func (r *Receiver) sendStatus(sessionID, streamID, ack uint32, sacks []udpwire.SackRange) {
	buf := make([]byte, udpwire.StatusHeaderLen+len(sacks)*udpwire.SackBlockLen)
	packet := udpwire.StatusPacket{
		SessionID: sessionID,
		StreamID:  streamID,
		AckSeq:    ack,
		Sacks:     sacks,
	}
	n, err := packet.Encode(buf)
	if err != nil {
		internal.Warn("receiver status encode failed", internal.Fields{
			internal.FieldError: err.Error(),
		})
		return
	}
	if _, err := r.pc.WriteTo(buf[:n], r.session.RemoteAddr); err != nil {
		internal.Warn("receiver status send failed", internal.Fields{
			internal.FieldError: err.Error(),
		})
	}
}

func (state *sinkState) record(seq uint32) (uint32, []udpwire.SackRange, bool) {
	if seq < state.expectedSeq {
		return state.expectedSeq, nil, state.isComplete()
	}
	if _, ok := state.received[seq]; !ok {
		state.received[seq] = struct{}{}
	}
	if seq > state.highestSeq {
		state.highestSeq = seq
	}

	for {
		if _, ok := state.received[state.expectedSeq]; !ok {
			break
		}
		delete(state.received, state.expectedSeq)
		state.expectedSeq++
	}

	if state.expectedSeq > 0 && state.highestSeq+1 < state.expectedSeq {
		state.highestSeq = state.expectedSeq - 1
	}
	ack := state.expectedSeq
	sacks := buildMissingRanges(ack, state.highestSeq, state.received)

	done := state.isComplete()
	transition := false
	if done && !state.completed {
		state.completed = true
		close(state.done)
		transition = true
	}
	return ack, sacks, transition
}

func (state *sinkState) isComplete() bool {
	if state.totalChunks == 0 {
		return false
	}
	return state.expectedSeq >= state.totalChunks
}

func (r *Receiver) Wait(ctx context.Context, streamID uint32) error {
	r.mu.Lock()
	state, ok := r.streams[streamID]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("stream %d not registered", streamID)
	}
	done := state.done
	if state.completed {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Receiver) CompleteStream(streamID uint32) {
	r.mu.Lock()
	state, ok := r.streams[streamID]
	if !ok || state.completed {
		r.mu.Unlock()
		return
	}
	state.completed = true
	close(state.done)
	cb := state.callbacks
	r.mu.Unlock()

	if cb.OnComplete != nil {
		cb.OnComplete()
	}
}

func computeChunkCount(size int64, chunk int) uint32 {
	if size <= 0 || chunk <= 0 {
		return 0
	}
	total := (size + int64(chunk) - 1) / int64(chunk)
	if total > int64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(total)
}

func buildMissingRanges(start, end uint32, received map[uint32]struct{}) []udpwire.SackRange {
	if end < start {
		return nil
	}
	var ranges []udpwire.SackRange
	var current *udpwire.SackRange
	seq := start
	for {
		if _, ok := received[seq]; ok {
			if current != nil {
				ranges = append(ranges, *current)
				current = nil
			}
		} else {
			if current == nil {
				current = &udpwire.SackRange{Start: seq, End: seq}
			} else {
				current.End = seq
			}
		}
		if seq == end {
			break
		}
		seq++
	}
	if current != nil {
		ranges = append(ranges, *current)
	}
	return ranges
}
