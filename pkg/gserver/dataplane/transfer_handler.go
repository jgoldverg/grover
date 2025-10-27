package dataplane

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

type SessionStore interface {
	Put(meta *SessionMetadata) error
	Get(sessionID uint32) (*SessionMetadata, bool)
	Delete(sessionID uint32)
}

type SessionMetadata struct {
	SessionID uint32
	Token     []byte
	Streams   map[uint32]*StreamMetadata
}

type StreamMetadata struct {
	StreamID    uint32
	DestPath    string
	SizeBytes   int64
	ChunkSize   int
	StartOffset uint64
	Writer      Writable
}

type TransferHandler struct {
	store SessionStore
	mu    sync.Mutex
	state map[uint32]*sessionState
}

type sessionState struct {
	meta       *SessionMetadata
	streams    map[uint32]*streamState
	lastAck    time.Time
	pendingAck bool
	remote     *net.UDPAddr
}

type streamState struct {
	meta        *StreamMetadata
	startOffset uint64
	chunkSize   int
	expectedSeq uint32
	highestSeq  uint32
	received    map[uint32]struct{}
	writer      Writable
	finalSeq    *uint16
}

type Writable interface {
	WriteAt(p []byte, off int64) (int, error)
	Close() error
}

func NewTransferHandler(store SessionStore) *TransferHandler {
	return &TransferHandler{
		store: store,
		state: make(map[uint32]*sessionState),
	}
}

func (h *TransferHandler) OnStart(ctx context.Context, pc net.PacketConn) error {
	return nil
}

func (h *TransferHandler) OnStop(ctx context.Context, pc net.PacketConn) error {
	return nil
}

func (h *TransferHandler) HandlePacket(ctx context.Context, pc net.PacketConn, src net.Addr, buf []byte, n int) {
	packet := buf[:n]
	kind, ok := udpwire.PeekKind(packet)
	if !ok {
		return
	}

	switch kind {
	case udpwire.KindData:
		if err := h.handleData(ctx, pc, src, packet); err != nil {
			internal.Warn("udp data handling failed", internal.Fields{
				internal.FieldError: err.Error(),
			})
		}
	case udpwire.KindStatus:
	default:
	}
}

func (h *TransferHandler) handleData(ctx context.Context, pc net.PacketConn, src net.Addr, packet []byte) error {
	var pkt udpwire.DataPacket
	if _, err := pkt.Decode(packet); err != nil {
		return err
	}

	udpAddr, ok := src.(*net.UDPAddr)
	if !ok {
		return fmt.Errorf("unexpected addr type %T", src)
	}

	h.mu.Lock()
	sess, err := h.ensureSession(pkt.SessionID)
	if err != nil {
		h.mu.Unlock()
		return err
	}
	if err := sess.ensureRemote(udpAddr); err != nil {
		h.mu.Unlock()
		return err
	}
	stream, err := h.ensureStream(sess, pkt.StreamID)
	if err != nil {
		h.mu.Unlock()
		return err
	}

	if stream.chunkSize <= 0 {
		stream.chunkSize = len(pkt.Payload)
	}

	chunkSize := stream.chunkSize
	if chunkSize <= 0 {
		chunkSize = len(pkt.Payload)
	}
	offset := stream.startOffset + uint64(pkt.Seq)*uint64(chunkSize)

	if stream.writer != nil && len(pkt.Payload) > 0 {
		if _, err := stream.writer.WriteAt(pkt.Payload, int64(offset)); err != nil {
			internal.Warn("udp write failed", internal.Fields{
				internal.FieldError: err.Error(),
			})
		}
	}

	ackSeq, sacks := stream.record(pkt.Seq)
	remote := cloneUDPAddr(sess.remote)
	h.mu.Unlock()

	h.sendStatus(pc, remote, pkt.SessionID, pkt.StreamID, ackSeq, sacks)
	return nil
}

func (h *TransferHandler) ensureSession(sessionID uint32) (*sessionState, error) {
	if sess, ok := h.state[sessionID]; ok {
		return sess, nil
	}

	meta, ok := h.store.Get(sessionID)
	if !ok {
		return nil, errors.New("unknown session")
	}

	sess := &sessionState{
		meta:    meta,
		streams: make(map[uint32]*streamState),
	}
	h.state[sessionID] = sess
	return sess, nil
}

func (h *TransferHandler) ensureStream(sess *sessionState, streamID uint32) (*streamState, error) {
	if stream, ok := sess.streams[streamID]; ok {
		return stream, nil
	}

	streamMeta, ok := sess.meta.Streams[streamID]
	if !ok {
		return nil, errors.New("unexpected stream")
	}

	stream := &streamState{
		meta:        streamMeta,
		startOffset: streamMeta.StartOffset,
		chunkSize:   streamMeta.ChunkSize,
		received:    make(map[uint32]struct{}),
		writer:      streamMeta.Writer,
	}
	sess.streams[streamID] = stream
	return stream, nil
}

func (sess *sessionState) ensureRemote(addr *net.UDPAddr) error {
	if addr == nil {
		return errors.New("nil remote address")
	}
	if sess.remote == nil {
		sess.remote = cloneUDPAddr(addr)
		return nil
	}
	if !sess.remote.IP.Equal(addr.IP) || sess.remote.Port != addr.Port {
		return fmt.Errorf("remote address mismatch: got %s expected %s", addr.String(), sess.remote.String())
	}
	return nil
}

func (s *streamState) record(seq uint32) (uint32, []udpwire.SackRange) {
	if seq < s.expectedSeq {
		return s.expectedSeq, nil
	}
	if _, ok := s.received[seq]; !ok {
		s.received[seq] = struct{}{}
	}
	if seq > s.highestSeq {
		s.highestSeq = seq
	}

	for {
		if _, ok := s.received[s.expectedSeq]; !ok {
			break
		}
		delete(s.received, s.expectedSeq)
		s.expectedSeq++
	}

	if s.expectedSeq > 0 && s.highestSeq+1 < s.expectedSeq {
		s.highestSeq = s.expectedSeq - 1
	}

	ack := s.expectedSeq
	sacks := buildMissingRanges(ack, s.highestSeq, s.received)
	return ack, sacks
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

func (h *TransferHandler) sendStatus(pc net.PacketConn, addr *net.UDPAddr, sessionID, streamID uint32, ack uint32, sacks []udpwire.SackRange) {
	if pc == nil || addr == nil {
		return
	}
	payload := make([]byte, udpwire.StatusHeaderLen+len(sacks)*udpwire.SackBlockLen)
	packet := udpwire.StatusPacket{
		SessionID: sessionID,
		StreamID:  streamID,
		AckSeq:    ack,
		Sacks:     sacks,
	}
	n, err := packet.Encode(payload)
	if err != nil {
		internal.Warn("status encode failed", internal.Fields{
			internal.FieldError: err.Error(),
		})
		return
	}
	if _, err := pc.WriteTo(payload[:n], addr); err != nil {
		internal.Warn("status send failed", internal.Fields{
			internal.FieldError: err.Error(),
		})
	}
}

func cloneUDPAddr(src *net.UDPAddr) *net.UDPAddr {
	if src == nil {
		return nil
	}
	out := *src
	if src.IP != nil {
		out.IP = append(net.IP(nil), src.IP...)
	}
	return &out
}
