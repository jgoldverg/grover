package udpdataplane

import (
	"time"

	"github.com/jgoldverg/grover/pkg/udpwire"
)

// SentPacket describes an encoded packet retained until acknowledged. Data is
// immutable while outstanding; retransmission refreshes SentAt.
type SentPacket struct {
	Sequence     uint32
	PayloadBytes int
	Data         []byte
	SentAt       time.Time
}

// outstandingPackets is per-stream accounting. It knows no sockets, metrics,
// congestion policy, or retry timing. Packets are tracked in sequence order.
type outstandingPackets struct{ packets []SentPacket }

func (p *outstandingPackets) track(packet SentPacket) { p.packets = append(p.packets, packet) }
func (p *outstandingPackets) bytes() int64 {
	var n int64
	for _, packet := range p.packets {
		n += int64(packet.PayloadBytes)
	}
	return n
}

// acknowledge removes cumulative and selectively acknowledged packets exactly
// once. The observer receives timing samples synchronously, without retaining
// an ever-growing history of samples in the sender.
func (p *outstandingPackets) acknowledge(status *udpwire.StatusPacket, at time.Time, observe func(AckSample)) int {
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
				observe(AckSample{RTT: sample, Bytes: packet.PayloadBytes})
			}
		}
	}
	clear(p.packets[len(keep):])
	p.packets = keep
	return ackedBytes
}
