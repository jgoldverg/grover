package udpdataplane

import (
	"slices"
	"time"

	"github.com/jgoldverg/grover/internal/udpwire"
)

// SentPacket describes an encoded packet retained until acknowledged. Data is
// immutable while outstanding; retransmission refreshes SentAt.
type SentPacket struct {
	Sequence     uint32
	PayloadBytes int
	Data         []byte
	SentAt       time.Time
	// Retransmitted packets never produce RTT samples (Karn's rule).
	Retransmitted bool
}

// Tracker is per-stream accounting. It knows no sockets, metrics,
// congestion policy, or retry timing. Packets are tracked in sequence order.
type Tracker struct {
	packets []SentPacket
	bytes   int64 // running sum of PayloadBytes in packets
}

// AckSample is timing and byte feedback for a newly acknowledged packet.
type AckSample struct {
	RTT   time.Duration
	Bytes int
}

// Pending returns a borrowed view valid until Track or Acknowledge is called.
// Callers may refresh SentAt after retransmission, but must not reorder packets
// or change their sequence, payload, or encoded data.
func (p *Tracker) Pending() []SentPacket { return p.packets }

// Len returns the number of packets still awaiting acknowledgment.
func (p *Tracker) Len() int { return len(p.packets) }

// Track retains a packet after a successful send, in sequence order.
func (p *Tracker) Track(packet SentPacket) {
	p.packets = append(p.packets, packet)
	p.bytes += int64(packet.PayloadBytes)
}

// Bytes returns the payload bytes still awaiting acknowledgment.
func (p *Tracker) Bytes() int64 { return p.bytes }

// Acknowledge removes cumulatively and selectively acknowledged packets
// exactly once and reports one AckSample per removed packet.
//
// Only one sample per call carries an RTT: the most recently sent packet that
// was never retransmitted (Karn's rule). Older packets in the same ACK may have
// been received long ago but only now become visible (a status packet lists a
// bounded number of SACK ranges), so their age is not a round-trip time.
func (p *Tracker) Acknowledge(status *udpwire.StatusPacket, at time.Time, observe func(AckSample)) int {
	sacks := status.Sacks
	if !slices.IsSortedFunc(sacks, func(a, b udpwire.SackRange) int { return int(int64(a.Start) - int64(b.Start)) }) {
		sacks = slices.Clone(sacks)
		slices.SortFunc(sacks, func(a, b udpwire.SackRange) int { return int(int64(a.Start) - int64(b.Start)) })
	}
	ackedBytes := 0
	rttIdx := -1
	var acked []SentPacket
	keep := p.packets[:0]
	j := 0 // sacks and packets are both in ascending sequence order
	for _, packet := range p.packets {
		seq := packet.Sequence
		covered := seq <= status.AckSeq
		for !covered && j < len(sacks) && sacks[j].End < seq {
			j++
		}
		if !covered && j < len(sacks) && sacks[j].Start <= seq {
			covered = true
		}
		if !covered {
			keep = append(keep, packet)
			continue
		}
		ackedBytes += packet.PayloadBytes
		if observe != nil {
			if !packet.Retransmitted && !packet.SentAt.IsZero() && (rttIdx < 0 || packet.SentAt.After(acked[rttIdx].SentAt)) {
				rttIdx = len(acked)
			}
			acked = append(acked, packet)
		}
	}
	clear(p.packets[len(keep):])
	p.packets = keep
	p.bytes -= int64(ackedBytes)
	for i, packet := range acked {
		sample := AckSample{Bytes: packet.PayloadBytes}
		if i == rttIdx {
			if rtt := at.Sub(packet.SentAt); rtt > 0 {
				sample.RTT = rtt
			}
		}
		observe(sample)
	}
	return ackedBytes
}
