package udpdataplane

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/jgoldverg/grover/pkg/udpwire"
)

func TestOutstandingPacketsAcknowledgesEachByteOnce(t *testing.T) {
	at := time.Unix(100, 0)
	var pending outstandingPackets
	for seq := uint32(1); seq <= 5; seq++ {
		pending.track(SentPacket{Sequence: seq, PayloadBytes: 100, SentAt: at.Add(-time.Second)})
	}
	status := &udpwire.StatusPacket{AckSeq: 1, Sacks: []udpwire.SackRange{{Start: 3, End: 4}, {Start: 4, End: 4}}}
	var samples []AckSample
	observe := func(s AckSample) { samples = append(samples, s) }
	if got := pending.acknowledge(status, at, observe); got != 300 {
		t.Fatalf("acked %d, want 300", got)
	}
	if pending.bytes() != 200 || len(pending.packets) != 2 || pending.packets[0].Sequence != 2 || pending.packets[1].Sequence != 5 {
		t.Fatalf("remaining packets: %+v", pending.packets)
	}
	if len(samples) != 3 || samples[0].RTT != time.Second {
		t.Fatalf("samples: %+v", samples)
	}
	if got := pending.acknowledge(status, at, observe); got != 0 {
		t.Fatalf("duplicate feedback acked %d", got)
	}
	if len(samples) != 3 {
		t.Fatal("duplicate feedback emitted samples")
	}
	if got := pending.acknowledge(&udpwire.StatusPacket{AckSeq: 5}, at, observe); got != 200 {
		t.Fatalf("final ack: %d", got)
	}
	if len(pending.packets) != 0 || pending.bytes() != 0 {
		t.Fatal("queue did not drain")
	}
}

func TestOutstandingPacketsWithoutTimingStillAccountsBytes(t *testing.T) {
	p := outstandingPackets{packets: []SentPacket{{Sequence: 1, PayloadBytes: 10}, {Sequence: 2, PayloadBytes: 20}}}
	samples := 0
	got := p.acknowledge(&udpwire.StatusPacket{AckSeq: 1, Sacks: []udpwire.SackRange{{Start: 2, End: 2}}}, time.Now(), func(AckSample) { samples++ })
	if got != 30 || samples != 0 {
		t.Fatalf("bytes=%d samples=%d", got, samples)
	}
}

func TestRecoveryPolicySelectsWithoutMutating(t *testing.T) {
	packets := []SentPacket{{Sequence: 1}, {Sequence: 2}, {Sequence: 3}, {Sequence: 4}}
	before := append([]SentPacket(nil), packets...)
	policy := sackRecovery{limit: 1}
	status := &udpwire.StatusPacket{Sacks: []udpwire.SackRange{{Start: 3, End: 3}}}
	if got := policy.OnFeedback(packets, status); !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("feedback indices: %v", got)
	}
	if got := policy.OnTimeout(packets); !reflect.DeepEqual(got, []int{0, 1, 2, 3}) {
		t.Fatalf("timeout indices: %v", got)
	}
	if !reflect.DeepEqual(packets, before) {
		t.Fatal("policy mutated outstanding packets")
	}
}

type timeoutRecordingTransport struct{ recordingTransport }

func (*timeoutRecordingTransport) ReadPacket([]byte) (int, *net.UDPAddr, error) {
	return 0, nil, timeoutErr{}
}

func TestFeedbackTimeoutPollAndCancellation(t *testing.T) {
	for _, mode := range []FeedbackMode{FeedbackNonBlocking, FeedbackUntilDrained} {
		transport := &timeoutRecordingTransport{}
		r := newSACKReliabilityController(SendConfig{Transport: transport, StreamID: 1}, nil)
		r.TrackSent(SentPacket{Sequence: 1, Data: []byte("packet"), PayloadBytes: 6})
		_, err := r.DrainFeedback(context.Background(), time.Millisecond, mode)
		if mode == FeedbackNonBlocking {
			if err != nil || len(transport.writes) != 0 {
				t.Fatalf("poll retransmitted or failed: %v", err)
			}
		} else if err == nil || len(transport.writes) != maxAckRetries-1 {
			t.Fatalf("retry count=%d error=%v", len(transport.writes), err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	transport := &timeoutRecordingTransport{}
	r := newSACKReliabilityController(SendConfig{Transport: transport}, nil)
	r.TrackSent(SentPacket{Sequence: 1})
	if _, err := r.DrainFeedback(ctx, time.Second, FeedbackUntilDrained); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if len(transport.writes) != 0 {
		t.Fatal("sent after cancellation")
	}
}
