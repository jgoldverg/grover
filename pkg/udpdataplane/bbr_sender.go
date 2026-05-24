package udpdataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/metrics"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

// AckSample is the feedback unit consumed by congestion controllers.
type AckSample struct {
	RTT   time.Duration
	Bytes int
}

// SendState is the sender loop's current transmit accounting.
type SendState struct {
	InflightBytes   int64
	InflightPackets int
	BatchBytes      int64
	BatchPackets    int
	PayloadSize     int
}

// ReliabilityController owns ACK/SACK interpretation and retransmission policy.
type ReliabilityController interface {
	TrackSent(pendingPacket)
	PendingPackets() int
	PendingBytes() int64
	DrainFeedback(ctx context.Context, timeout time.Duration, mode FeedbackMode) (int, error)
}

// FlowController is a hard sender-side safety gate.
type FlowController interface {
	CanSend(SendState) bool
}

// CongestionController chooses the current network in-flight target.
type CongestionController interface {
	TargetInflightBytes(SendState) int64
	AckTimeout() time.Duration
	OnAck(AckSample)
}

// Pacer controls packet send timing. NoopPacer preserves the current bursty path.
type Pacer interface {
	Wait(ctx context.Context, bytes int) error
	OnPacketSent(bytes int)
}

type FeedbackMode int

const (
	FeedbackNonBlocking FeedbackMode = iota
	FeedbackUntilProgress
	FeedbackUntilDrained
)

// BBRSender is the UDP sender implementation. It now composes reliability,
// flow, congestion, and pacing controllers behind one transmit loop.
type BBRSender struct {
	minWindowBytes int64
}

// NewBBRSender constructs the default sender. Despite the legacy name, fixed
// flow control remains the default unless SendConfig.FlowControl asks for bbr.
func NewBBRSender() *BBRSender {
	return &BBRSender{
		minWindowBytes: 10 * 1024 * 1024,
	}
}

// Send implements the Sender interface.
func (s *BBRSender) Send(ctx context.Context, cfg SendConfig, src io.Reader) (uint64, error) {
	if cfg.Transport == nil {
		return 0, errors.New("transport is required")
	}
	if cfg.StreamID == 0 {
		return 0, errors.New("stream id is required")
	}

	payloadSize := payloadSizeFromMTU(cfg.MTU)
	windowPackets := cfg.WindowPackets
	if windowPackets <= 0 {
		windowPackets = 4096
	}

	batchMax := batchPacketCount(cfg.BatchPackets)
	batch := make([]pendingPacket, 0, batchMax)
	var batchBytes int64
	var seq uint32
	var offset uint64
	var sentSincePoll int

	congestion := s.newCongestionController(cfg, payloadSize, windowPackets)
	flow := s.newFlowController(cfg, payloadSize, windowPackets)
	pacer := NoopPacer{}
	reliability := newSACKReliabilityController(cfg, congestion.OnAck)
	sendRemote := cfg.RemoteAddr

	defer cfg.Transport.SetWriteDeadline(time.Time{})

	state := func() SendState {
		return SendState{
			InflightBytes:   reliability.PendingBytes(),
			InflightPackets: reliability.PendingPackets(),
			BatchBytes:      batchBytes,
			BatchPackets:    len(batch),
			PayloadSize:     payloadSize,
		}
	}

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := writePendingBatch(ctx, cfg.Transport, sendRemote, batch, cfg.Collector, pacer, cfg.SessionID, cfg.StreamID); err != nil {
			return err
		}
		sentAt := time.Now()
		for i := range batch {
			batch[i].sentAt = sentAt
			reliability.TrackSent(batch[i])
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}

	drainFeedback := func(timeout time.Duration, mode FeedbackMode) (int, error) {
		acked, err := reliability.DrainFeedback(ctx, timeout, mode)
		if remote := reliability.RemoteAddr(); remote != nil {
			sendRemote = remote
		}
		return acked, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return offset, err
		}
		cur := state()
		if !flow.CanSend(cur) || cur.InflightBytes+cur.BatchBytes >= congestion.TargetInflightBytes(cur) {
			if err := flushBatch(); err != nil {
				return offset, err
			}
			if _, err := drainFeedback(congestion.AckTimeout(), FeedbackUntilProgress); err != nil {
				return offset, err
			}
			continue
		}

		packetBuf := make([]byte, udpwire.DataHeaderLen+payloadSize+4)
		n, readErr := src.Read(packetBuf[udpwire.DataHeaderLen : udpwire.DataHeaderLen+payloadSize])
		if n > 0 {
			seq++
			pktLen, err := udpwire.EncodeDataPacketInPlace(packetBuf, cfg.SessionKey, cfg.StreamID, seq, cfg.BaseOffset+offset, n)
			if err != nil {
				return offset, fmt.Errorf("encode data packet: %w", err)
			}
			packetBuf = packetBuf[:pktLen]
			offset += uint64(n)
			batch = append(batch, pendingPacket{
				seq:        seq,
				payloadLen: n,
				data:       packetBuf,
			})
			batchBytes += int64(n)
			sentSincePoll++

			if shouldPollAcks(sentSincePoll, reliability.PendingPackets(), windowPackets) {
				sentSincePoll = 0
				if err := flushBatch(); err != nil {
					return offset, err
				}
				if _, err := drainFeedback(0, FeedbackNonBlocking); err != nil {
					return offset, err
				}
			}
			if len(batch) >= batchMax {
				if err := flushBatch(); err != nil {
					return offset, err
				}
			}
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return offset, readErr
		}
	}

	if err := flushBatch(); err != nil {
		return offset, err
	}
	for reliability.PendingPackets() > 0 {
		if _, err := drainFeedback(congestion.AckTimeout(), FeedbackUntilDrained); err != nil {
			if !cfg.RequireFinalAck {
				return offset, nil
			}
			return offset, err
		}
	}
	return offset, nil
}

func (s *BBRSender) newFlowController(cfg SendConfig, payloadSize int, windowPackets int) FlowController {
	if strings.EqualFold(strings.TrimSpace(cfg.FlowControl), "bbr") {
		return newBBRFlowController(cfg)
	}
	return newFixedWindowFlowController(cfg, payloadSize, windowPackets)
}

func (s *BBRSender) newCongestionController(cfg SendConfig, payloadSize int, windowPackets int) CongestionController {
	if strings.EqualFold(strings.TrimSpace(cfg.FlowControl), "bbr") {
		minWindow := s.minWindowBytes
		if cfg.WindowBytes > 0 {
			minWindow = int64(cfg.WindowBytes)
		}
		return newBBRLikeCongestionController(minWindow)
	}
	return newFixedCongestionController(s.flowControlWindowBytes(cfg, payloadSize, windowPackets))
}

func (s *BBRSender) flowControlWindowBytes(cfg SendConfig, payloadSize int, windowPackets int) int64 {
	if cfg.WindowBytes > 0 {
		return int64(cfg.WindowBytes)
	}
	if strings.EqualFold(strings.TrimSpace(cfg.FlowControl), "bbr") {
		return s.minWindowBytes
	}
	if windowPackets > 0 && payloadSize > 0 {
		return int64(windowPackets * payloadSize)
	}
	return s.minWindowBytes
}

type SACKReliabilityController struct {
	transport               Transport
	remote                  *net.UDPAddr
	streamID                uint32
	ackBuf                  []byte
	ackPkt                  udpwire.StatusPacket
	pending                 []pendingPacket
	collector               *metrics.TransferCollector
	fastRetransmitLimit     int
	onAck                   func(AckSample)
	samples                 []AckSample
	lastDrainAckSampleCount int
}

func newSACKReliabilityController(cfg SendConfig, onAck func(AckSample)) *SACKReliabilityController {
	return &SACKReliabilityController{
		transport:           cfg.Transport,
		remote:              cfg.RemoteAddr,
		streamID:            cfg.StreamID,
		ackBuf:              make([]byte, udpwire.StatusHeaderLen+udpwire.MaxSackRanges*udpwire.SackBlockLen),
		pending:             make([]pendingPacket, 0, 1024),
		collector:           cfg.Collector,
		fastRetransmitLimit: fastRetransmitPacketCount(cfg.FastRetransmitPackets),
		onAck:               onAck,
	}
}

func (r *SACKReliabilityController) TrackSent(pkt pendingPacket) {
	r.pending = append(r.pending, pkt)
}

func (r *SACKReliabilityController) PendingPackets() int {
	return len(r.pending)
}

func (r *SACKReliabilityController) PendingBytes() int64 {
	var n int64
	for i := range r.pending {
		n += int64(r.pending[i].payloadLen)
	}
	return n
}

func (r *SACKReliabilityController) RemoteAddr() *net.UDPAddr {
	return r.remote
}

func (r *SACKReliabilityController) DrainFeedback(ctx context.Context, timeout time.Duration, mode FeedbackMode) (int, error) {
	r.lastDrainAckSampleCount = 0
	nonBlocking := mode == FeedbackNonBlocking
	stopAfterProgress := mode == FeedbackUntilProgress
	acked, err := drainStatusPackets(
		ctx,
		r.transport,
		&r.remote,
		r.streamID,
		r.ackBuf,
		&r.ackPkt,
		&r.pending,
		r.collector,
		timeout,
		nonBlocking,
		stopAfterProgress,
		r.fastRetransmitLimit,
		r.observeAck,
	)
	return acked, err
}

func (r *SACKReliabilityController) observeAck(sample time.Duration, ackBytes int) {
	if ackBytes <= 0 {
		return
	}
	ack := AckSample{RTT: sample, Bytes: ackBytes}
	r.samples = append(r.samples, ack)
	r.lastDrainAckSampleCount++
	if r.onAck != nil {
		r.onAck(ack)
	}
}

type FixedWindowFlowController struct {
	maxPackets int
	maxBytes   int64
}

func newFixedWindowFlowController(cfg SendConfig, payloadSize int, windowPackets int) FixedWindowFlowController {
	maxBytes := int64(0)
	if cfg.WindowBytes > 0 {
		maxBytes = int64(cfg.WindowBytes)
	} else if payloadSize > 0 && windowPackets > 0 {
		maxBytes = int64(payloadSize * windowPackets)
	}
	return FixedWindowFlowController{maxPackets: windowPackets, maxBytes: maxBytes}
}

func (c FixedWindowFlowController) CanSend(state SendState) bool {
	if c.maxPackets > 0 && state.InflightPackets+state.BatchPackets >= c.maxPackets {
		return false
	}
	if c.maxBytes > 0 && state.InflightBytes+state.BatchBytes >= c.maxBytes {
		return false
	}
	return true
}

type BBRFlowController struct {
	maxBytes int64
}

func newBBRFlowController(cfg SendConfig) BBRFlowController {
	return BBRFlowController{maxBytes: int64(cfg.WindowBytes)}
}

func (c BBRFlowController) CanSend(state SendState) bool {
	if c.maxBytes > 0 && state.InflightBytes+state.BatchBytes >= c.maxBytes {
		return false
	}
	return true
}

type FixedCongestionController struct {
	targetBytes int64
}

func newFixedCongestionController(targetBytes int64) FixedCongestionController {
	return FixedCongestionController{targetBytes: targetBytes}
}

func (c FixedCongestionController) TargetInflightBytes(state SendState) int64 {
	if c.targetBytes > 0 {
		return c.targetBytes
	}
	if state.PayloadSize > 0 {
		return int64(4096 * state.PayloadSize)
	}
	return 10 * 1024 * 1024
}

func (c FixedCongestionController) AckTimeout() time.Duration {
	return 200 * time.Millisecond
}

func (c FixedCongestionController) OnAck(AckSample) {}

type BBRLikeCongestionController struct {
	minWindowBytes int64

	mu         sync.Mutex
	minRTT     time.Duration
	srtt       time.Duration
	rttVar     time.Duration
	bwEstimate float64
	lastAck    time.Time
}

func newBBRLikeCongestionController(minWindowBytes int64) *BBRLikeCongestionController {
	if minWindowBytes <= 0 {
		minWindowBytes = 10 * 1024 * 1024
	}
	return &BBRLikeCongestionController{minWindowBytes: minWindowBytes}
}

func (c *BBRLikeCongestionController) TargetInflightBytes(SendState) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	window := float64(c.minWindowBytes)
	if c.bwEstimate > 0 && c.minRTT > 0 {
		window = math.Max(window, c.bwEstimate*c.minRTT.Seconds())
	}
	if c.srtt > 0 && c.minRTT > 0 {
		ratio := float64(c.minRTT) / float64(c.srtt)
		if ratio < 0.5 {
			ratio = 0.5
		} else if ratio > 1.5 {
			ratio = 1.5
		}
		window *= ratio
	}
	if window < float64(c.minWindowBytes) {
		window = float64(c.minWindowBytes)
	}
	return int64(window)
}

func (c *BBRLikeCongestionController) AckTimeout() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	timeout := 200 * time.Millisecond
	if c.srtt > 0 {
		timeout = c.srtt * 5
	}
	if timeout < 20*time.Millisecond {
		timeout = 20 * time.Millisecond
	}
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	return timeout
}

func (c *BBRLikeCongestionController) OnAck(sample AckSample) {
	if sample.Bytes <= 0 {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if sample.RTT > 0 {
		if c.minRTT == 0 || sample.RTT < c.minRTT {
			c.minRTT = sample.RTT
		}
		if c.srtt == 0 {
			c.srtt = sample.RTT
			c.rttVar = sample.RTT / 2
		} else {
			delta := sample.RTT - c.srtt
			c.srtt += delta / 8
			if delta < 0 {
				delta = -delta
			}
			c.rttVar += (delta - c.rttVar) / 4
		}
	}
	if !c.lastAck.IsZero() {
		dt := now.Sub(c.lastAck)
		if dt > 0 {
			rate := float64(sample.Bytes) / dt.Seconds()
			if rate > c.bwEstimate {
				c.bwEstimate = rate
			} else {
				c.bwEstimate = c.bwEstimate*0.95 + rate*0.05
			}
		}
	} else if sample.RTT > 0 {
		c.bwEstimate = float64(sample.Bytes) / sample.RTT.Seconds()
	}
	c.lastAck = now
}

type NoopPacer struct{}

func (NoopPacer) Wait(context.Context, int) error { return nil }
func (NoopPacer) OnPacketSent(int)                {}

func writePendingBatch(
	ctx context.Context,
	transport Transport,
	remote *net.UDPAddr,
	batch []pendingPacket,
	collector *metrics.TransferCollector,
	pacer Pacer,
	sessionID string,
	streamID uint32,
) error {
	if len(batch) == 0 {
		return nil
	}
	if _, ok := pacer.(NoopPacer); ok && len(batch) > 1 {
		buffers := make([]PacketBuffer, len(batch))
		for i := range batch {
			buffers[i] = PacketBuffer{Bytes: batch[i].data}
		}
		if err := writeBatchWithRetry(ctx, transport, remote, buffers); err != nil {
			return err
		}
		for i := range batch {
			recordPacketSent(batch[i], collector, pacer, sessionID, streamID)
		}
		return nil
	}
	for i := range batch {
		if err := pacer.Wait(ctx, batch[i].payloadLen); err != nil {
			return err
		}
		if err := writePacketWithRetry(ctx, transport, remote, batch[i].data); err != nil {
			return err
		}
		recordPacketSent(batch[i], collector, pacer, sessionID, streamID)
	}
	return nil
}

func recordPacketSent(pkt pendingPacket, collector *metrics.TransferCollector, pacer Pacer, sessionID string, streamID uint32) {
	recordPacketSend(collector)
	recordSendMetric(collector, pkt.payloadLen, false)
	pacer.OnPacketSent(pkt.payloadLen)
	internal.Debug("udp data tx", internal.Fields{
		"session": sessionID,
		"stream":  streamID,
		"seq":     pkt.seq,
		"bytes":   pkt.payloadLen,
	})
}

func shouldPollAcks(sentSincePoll int, pendingPackets int, windowPackets int) bool {
	if pendingPackets <= 0 {
		return false
	}
	if sentSincePoll >= defaultAckPollEvery {
		return true
	}
	if windowPackets <= 0 {
		return false
	}
	return pendingPackets >= windowPackets-(defaultAckPollEvery*2)
}
