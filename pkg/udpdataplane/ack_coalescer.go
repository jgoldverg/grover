package udpdataplane

import "time"

type ackCoalescer struct {
	everyPackets int
	every        time.Duration
	last         time.Time
	pending      int
}

func newAckCoalescer(everyPackets int, every time.Duration) *ackCoalescer {
	if everyPackets <= 0 {
		everyPackets = 32
	}
	if every <= 0 {
		every = 5 * time.Millisecond
	}
	return &ackCoalescer{
		everyPackets: everyPackets,
		every:        every,
		last:         time.Now(),
	}
}

func (a *ackCoalescer) OnPacket(gap bool) bool {
	if a == nil {
		return true
	}
	a.pending++
	if gap {
		return true
	}
	if a.pending >= a.everyPackets {
		return true
	}
	return time.Since(a.last) >= a.every
}

func (a *ackCoalescer) MarkSent() {
	if a == nil {
		return
	}
	a.pending = 0
	a.last = time.Now()
}
