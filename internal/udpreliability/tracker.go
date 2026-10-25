package udpreliability

import (
	"time"

	"github.com/jgoldverg/grover/pkg/udpwire"
)

// Packet describes an encoded packet retained until acknowledged. Data is
// immutable while outstanding; retransmission refreshes SentAt.
type Packet struct {
	Sequence     uint32
	PayloadBytes int
	Data         []byte
	SentAt       time.Time
}

// Tracker is per-stream accounting. It knows no sockets, metrics,
// congestion policy, or retry timing. Packets are tracked in sequence order.
type Tracker struct{ packets []Packet }

// Sample is timing and byte feedback for a newly acknowledged packet.
type Sample struct {
	RTT   time.Duration
	Bytes int
}

// Pending returns a borrowed view valid until Track or Acknowledge is called.
// Callers may refresh SentAt after retransmission, but must not reorder packets
// or change their sequence, payload, or encoded data.
func (p *Tracker) Pending() []Packet { return p.packets }

// Len returns the number of packets still awaiting acknowledgment.
func (p *Tracker) Len() int { return len(p.packets) }

// Track retains a packet after a successful send, in sequence order.
func (p *Tracker) Track(packet Packet) { p.packets = append(p.packets, packet) }

// Bytes returns the payload bytes still awaiting acknowledgment.
func (p *Tracker) Bytes() int64 {
	var n int64
	for _, packet := range p.packets {
		n += int64(packet.PayloadBytes)
	}
	return n
}

// Acknowledge removes cumulative and selectively acknowledged packets exactly
// once. The observer receives timing samples synchronously, without retaining
// an ever-growing history of samples in the sender.
func (p *Tracker) Acknowledge(status *udpwire.StatusPacket, at time.Time, observe func(Sample)) int {
	ackedBytes := 0
	keep := p.packets[:0]
	for _, packet := range p.packets {
		if packet.Sequence > status.AckSeq && !sackCovers(status.Sacks, packet.Sequence) {
			keep = append(keep, packet)
			continue
		}
		ackedBytes += packet.PayloadBytes
		if observe != nil && !packet.SentAt.IsZero() {
			if sample := at.Sub(packet.SentAt); sample > 0 {
				observe(Sample{RTT: sample, Bytes: packet.PayloadBytes})
			}
		}
	}
	clear(p.packets[len(keep):])
	p.packets = keep
	return ackedBytes
}
