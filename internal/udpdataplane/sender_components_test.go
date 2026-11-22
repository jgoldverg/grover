package udpdataplane

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

type immediateReliability struct {
	packets []SentPacket
	onAck   func(AckSample)
}

func (r *immediateReliability) TrackSent(p SentPacket) { r.packets = append(r.packets, p) }
func (*immediateReliability) RemoteAddr() *net.UDPAddr { return nil }
func (r *immediateReliability) PendingPackets() int    { return len(r.packets) }
func (r *immediateReliability) PendingBytes() int64 {
	var n int64
	for _, p := range r.packets {
		n += int64(p.PayloadBytes)
	}
	return n
}
func (r *immediateReliability) DrainFeedback(context.Context, time.Duration, FeedbackMode) (int, error) {
	n := int(r.PendingBytes())
	r.packets = nil
	r.onAck(AckSample{Bytes: n, RTT: time.Millisecond})
	return n, nil
}

type observingCongestion struct{ acked int }

func (*observingCongestion) TargetInflightBytes(SendState) int64 { return 100000 }
func (*observingCongestion) AckTimeout() time.Duration           { return time.Second }
func (c *observingCongestion) OnAck(s AckSample)                 { c.acked += s.Bytes }

type observingPacer struct{ sent int }

func (*observingPacer) Wait(context.Context, int) error { return nil }
func (p *observingPacer) OnPacketSent(n int)            { p.sent += n }

func TestSenderFactoriesArePerStreamAndReceiveFeedback(t *testing.T) {
	sender := NewPacketSender()
	var congestions []*observingCongestion
	var pacers []*observingPacer
	sender.Factories = SenderFactories{
		Congestion: func(SendConfig) CongestionController {
			c := &observingCongestion{}
			congestions = append(congestions, c)
			return c
		},
		Reliability: func(_ SendConfig, ack func(AckSample)) ReliabilityController {
			return &immediateReliability{onAck: ack}
		},
		Pacer: func(SendConfig) Pacer { p := &observingPacer{}; pacers = append(pacers, p); return p },
	}
	for stream := uint32(1); stream <= 2; stream++ {
		transport := &recordingTransport{}
		n, err := sender.Send(context.Background(), SendConfig{Transport: transport, StreamID: stream, RequireFinalAck: true}, strings.NewReader("payload"))
		if err != nil || n != 7 || len(transport.writes) != 1 {
			t.Fatalf("send: bytes=%d writes=%d err=%v", n, len(transport.writes), err)
		}
	}
	if len(congestions) != 2 || len(pacers) != 2 {
		t.Fatal("controllers were not constructed per stream")
	}
	for i := range congestions {
		if congestions[i].acked != 7 || pacers[i].sent != 7 {
			t.Fatalf("stream %d: feedback=%d paced=%d", i, congestions[i].acked, pacers[i].sent)
		}
	}
}
