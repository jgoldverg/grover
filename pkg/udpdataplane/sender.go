package udpdataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jgoldverg/grover/pkg/udpwire"
)

// PacketSender is the UDP sender implementation. It composes reliability,
// flow, congestion, and pacing controllers behind one transmit loop.
type PacketSender struct {
	minWindowBytes int64
	Factories      SenderFactories
}

// BBRSender is retained for source compatibility. Both fixed and BBR-like modes
// use PacketSender; congestion control is a separate component.
type BBRSender = PacketSender

func NewBBRSender() *PacketSender { return NewPacketSender() }

// NewPacketSender constructs a sender with fixed flow control by default.
func NewPacketSender() *PacketSender {
	return &PacketSender{
		minWindowBytes: 10 * 1024 * 1024,
	}
}

// Send implements the Sender interface.
func (s *PacketSender) Send(ctx context.Context, cfg SendConfig, src io.Reader) (uint64, error) {
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
	batch := make([]SentPacket, 0, batchMax)
	var batchBytes int64
	var seq uint32
	var offset uint64
	var sentSincePoll int

	congestion := s.newCongestionController(cfg, payloadSize, windowPackets)
	if s.Factories.Congestion != nil {
		congestion = s.Factories.Congestion(cfg)
	}
	flow := s.newFlowController(cfg, payloadSize, windowPackets)
	if s.Factories.Flow != nil {
		flow = s.Factories.Flow(cfg)
	}
	var pacer Pacer = NoopPacer{}
	if s.Factories.Pacer != nil {
		pacer = s.Factories.Pacer(cfg)
	}
	var reliability ReliabilityController = newSACKReliabilityController(cfg, congestion.OnAck)
	if s.Factories.Reliability != nil {
		reliability = s.Factories.Reliability(cfg, congestion.OnAck)
	}
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
		send := packetSendConfig{Transport: cfg.Transport, Remote: sendRemote,
			Collector: cfg.Collector, Pacer: pacer, SessionID: cfg.SessionID, StreamID: cfg.StreamID}
		if err := writePendingBatch(ctx, send, batch); err != nil {
			return err
		}
		sentAt := time.Now()
		for i := range batch {
			batch[i].SentAt = sentAt
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
			batch = append(batch, SentPacket{
				Sequence:     seq,
				PayloadBytes: n,
				Data:         packetBuf,
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

func (s *PacketSender) newFlowController(cfg SendConfig, payloadSize int, windowPackets int) FlowController {
	if strings.EqualFold(strings.TrimSpace(cfg.FlowControl), "bbr") {
		return newBBRFlowController(cfg)
	}
	return newFixedWindowFlowController(cfg, payloadSize, windowPackets)
}

func (s *PacketSender) newCongestionController(cfg SendConfig, payloadSize int, windowPackets int) CongestionController {
	if strings.EqualFold(strings.TrimSpace(cfg.FlowControl), "bbr") {
		minWindow := s.minWindowBytes
		if cfg.WindowBytes > 0 {
			minWindow = int64(cfg.WindowBytes)
		}
		return newBBRLikeCongestionController(minWindow)
	}
	return newFixedCongestionController(s.flowControlWindowBytes(cfg, payloadSize, windowPackets))
}

func (s *PacketSender) flowControlWindowBytes(cfg SendConfig, payloadSize int, windowPackets int) int64 {
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
