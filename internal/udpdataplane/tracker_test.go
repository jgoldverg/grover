package udpdataplane

import (
	"reflect"
	"testing"
	"time"

	"github.com/jgoldverg/grover/internal/udpwire"
)

func TestOutstandingPacketsAcknowledgesEachByteOnce(t *testing.T) {
	at := time.Unix(100, 0)
	var pending Tracker
	for seq := uint32(1); seq <= 5; seq++ {
		pending.Track(SentPacket{Sequence: seq, PayloadBytes: 100, SentAt: at.Add(-time.Second)})
	}
	status := &udpwire.StatusPacket{AckSeq: 1, Sacks: []udpwire.SackRange{{Start: 3, End: 4}, {Start: 4, End: 4}}}
	var samples []AckSample
	observe := func(s AckSample) { samples = append(samples, s) }
	if got := pending.Acknowledge(status, at, observe); got != 300 {
		t.Fatalf("acked %d, want 300", got)
	}
	if pending.Bytes() != 200 || len(pending.packets) != 2 || pending.packets[0].Sequence != 2 || pending.packets[1].Sequence != 5 {
		t.Fatalf("remaining packets: %+v", pending.packets)
	}
	withRTT := 0
	for _, s := range samples {
		if s.RTT > 0 {
			withRTT++
		}
	}
	if len(samples) != 3 || withRTT != 1 {
		t.Fatalf("want 3 samples with exactly one RTT, got %+v", samples)
	}
	if got := pending.Acknowledge(status, at, observe); got != 0 {
		t.Fatalf("duplicate feedback acked %d", got)
	}
	if len(samples) != 3 {
		t.Fatal("duplicate feedback emitted samples")
	}
	if got := pending.Acknowledge(&udpwire.StatusPacket{AckSeq: 5}, at, observe); got != 200 {
		t.Fatalf("final ack: %d", got)
	}
	if len(pending.packets) != 0 || pending.Bytes() != 0 {
		t.Fatal("queue did not drain")
	}
}

func TestUntimedPacketsAccountBytesWithoutRTT(t *testing.T) {
	var p Tracker
	p.Track(SentPacket{Sequence: 1, PayloadBytes: 10})
	p.Track(SentPacket{Sequence: 2, PayloadBytes: 20})
	rtts := 0
	got := p.Acknowledge(&udpwire.StatusPacket{AckSeq: 1, Sacks: []udpwire.SackRange{{Start: 2, End: 2}}}, time.Now(), func(s AckSample) {
		if s.RTT > 0 {
			rtts++
		}
	})
	if got != 30 || rtts != 0 {
		t.Fatalf("Bytes=%d rtt samples=%d (untimed packets must not produce RTTs)", got, rtts)
	}
}

func TestRecoveryPolicySelectsWithoutMutating(t *testing.T) {
	packets := []SentPacket{{Sequence: 1}, {Sequence: 2}, {Sequence: 3}, {Sequence: 4}}
	before := append([]SentPacket(nil), packets...)
	policy := SACKRecovery{Limit: 1}
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
