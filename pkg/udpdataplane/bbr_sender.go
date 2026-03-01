package udpdataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

// BBRSender attempts to approximate BBR-style congestion control using ACK rate and RTT.
type BBRSender struct {
	minWindowBytes int64
	maxWindowBytes int64

	mu         sync.Mutex
	minRTT     time.Duration
	srtt       time.Duration
	rttVar     time.Duration
	bwEstimate float64 // bytes per second
	lastAck    time.Time
}

// NewBBRSender constructs a sender tuned for WAN transfers.
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
	payloadBuf := make([]byte, payloadSize)
	packetBuf := make([]byte, udpwire.DataHeaderLen+payloadSize+4)
	ackBuf := make([]byte, udpwire.StatusHeaderLen+udpwire.MaxSackRanges*udpwire.SackBlockLen)
	var ackPkt udpwire.StatusPacket

	var (
		seq           uint32
		offset        uint64
		pending       = make([]pendingPacket, 0, 1024)
		bytesInflight int64
	)

	defer cfg.Transport.SetWriteDeadline(time.Time{})

	sendRemote := cfg.RemoteAddr

	for {
		if err := ctx.Err(); err != nil {
			return offset, err
		}
		target := s.inflightLimitBytes()
		if target <= 0 {
			target = s.minWindowBytes
		}
		if bytesInflight >= target {
			acked, err := drainStatusPackets(ctx, cfg.Transport, &sendRemote, cfg.StreamID, ackBuf, &ackPkt, &pending, cfg.Collector, s.ackTimeout(), false, s.observeAck)
			if err != nil {
				return offset, err
			}
			if acked > 0 {
				bytesInflight -= int64(acked)
				if bytesInflight < 0 {
					bytesInflight = 0
				}
			}
			continue
		}

		n, readErr := src.Read(payloadBuf)
		if n > 0 {
			seq++
			dp := udpwire.DataPacket{
				SessionID: cfg.SessionKey,
				StreamID:  cfg.StreamID,
				Seq:       seq,
				Offset:    offset,
				Payload:   payloadBuf[:n],
			}
			pktLen, err := dp.Encode(packetBuf)
			if err != nil {
				return offset, fmt.Errorf("encode data packet: %w", err)
			}
			packetCopy := make([]byte, pktLen)
			copy(packetCopy, packetBuf[:pktLen])
			if err := writePacketWithRetry(ctx, cfg.Transport, sendRemote, packetCopy); err != nil {
				return offset, err
			}
			recordPacketSend(cfg.Collector)
			recordSendMetric(cfg.Collector, n, false)
			internal.Debug("udp data tx", internal.Fields{
				"session": cfg.SessionID,
				"stream":  cfg.StreamID,
				"seq":     seq,
				"bytes":   n,
			})
			offset += uint64(n)
			bytesInflight += int64(n)
			pending = append(pending, pendingPacket{
				seq:        seq,
				payloadLen: n,
				data:       packetCopy,
				sentAt:     time.Now(),
			})
			acked, err := drainStatusPackets(ctx, cfg.Transport, &sendRemote, cfg.StreamID, ackBuf, &ackPkt, &pending, cfg.Collector, s.pollTimeout(), true, s.observeAck)
			if err != nil {
				return offset, err
			}
			if acked > 0 {
				bytesInflight -= int64(acked)
				if bytesInflight < 0 {
					bytesInflight = 0
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
	for len(pending) > 0 {
		acked, err := drainStatusPackets(ctx, cfg.Transport, &sendRemote, cfg.StreamID, ackBuf, &ackPkt, &pending, cfg.Collector, s.ackTimeout(), false, s.observeAck)
		if err != nil {
			return offset, err
		}
		if acked > 0 {
			bytesInflight -= int64(acked)
			if bytesInflight < 0 {
				bytesInflight = 0
			}
		}
	}
	return offset, nil
}

func (s *BBRSender) observeAck(sample time.Duration, ackBytes int) {
	if ackBytes <= 0 {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if sample > 0 {
		if s.minRTT == 0 || sample < s.minRTT {
			s.minRTT = sample
		}
		if s.srtt == 0 {
			s.srtt = sample
			s.rttVar = sample / 2
		} else {
			delta := sample - s.srtt
			s.srtt += delta / 8
			if delta < 0 {
				delta = -delta
			}
			s.rttVar += (delta - s.rttVar) / 4
		}
	}
	if !s.lastAck.IsZero() {
		dt := now.Sub(s.lastAck)
		if dt > 0 {
			rate := float64(ackBytes) / dt.Seconds()
			if rate > s.bwEstimate {
				s.bwEstimate = rate
			} else {
				s.bwEstimate = s.bwEstimate*0.95 + rate*0.05
			}
		}
	} else if sample > 0 {
		s.bwEstimate = float64(ackBytes) / sample.Seconds()
	}
	s.lastAck = now
}

func (s *BBRSender) inflightLimitBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	window := float64(s.minWindowBytes)
	if s.bwEstimate > 0 && s.minRTT > 0 {
		window = math.Max(window, s.bwEstimate*s.minRTT.Seconds())
	}
	if s.srtt > 0 && s.minRTT > 0 {
		ratio := float64(s.minRTT) / float64(s.srtt)
		if ratio < 0.5 {
			ratio = 0.5
		} else if ratio > 1.5 {
			ratio = 1.5
		}
		window *= ratio
	}
	if window < float64(s.minWindowBytes) {
		window = float64(s.minWindowBytes)
	}
	return int64(window)
}

func (s *BBRSender) ackTimeout() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	timeout := 200 * time.Millisecond
	if s.srtt > 0 {
		timeout = s.srtt * 5
	}
	if timeout < 20*time.Millisecond {
		timeout = 20 * time.Millisecond
	}
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	return timeout
}

func (s *BBRSender) pollTimeout() time.Duration {
	t := s.ackTimeout() / 20
	if t < 2*time.Millisecond {
		t = 2 * time.Millisecond
	}
	return t
}

func (s *BBRSender) onAck(sample time.Duration, ackBytes int) {
	s.observeAck(sample, ackBytes)
}
