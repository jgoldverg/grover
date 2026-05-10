package udpdataplane

import (
	"testing"
	"time"
)

func TestAckCoalescerPacketThreshold(t *testing.T) {
	c := newAckCoalescer(3, time.Hour)
	if c.OnPacket(false) {
		t.Fatal("first packet should not emit ack")
	}
	if c.OnPacket(false) {
		t.Fatal("second packet should not emit ack")
	}
	if !c.OnPacket(false) {
		t.Fatal("third packet should emit ack")
	}
	c.MarkSent()
	if c.OnPacket(false) {
		t.Fatal("packet count should reset after ack")
	}
}

func TestAckCoalescerTimerThreshold(t *testing.T) {
	c := newAckCoalescer(100, time.Millisecond)
	c.last = time.Now().Add(-2 * time.Millisecond)
	if !c.OnPacket(false) {
		t.Fatal("expired timer should emit ack")
	}
}

func TestAckCoalescerGapImmediate(t *testing.T) {
	c := newAckCoalescer(100, time.Hour)
	if !c.OnPacket(true) {
		t.Fatal("gap should emit ack immediately")
	}
}
