package udpreliability

import "github.com/jgoldverg/grover/pkg/udpwire"

// SACKRecovery preserves the existing policy: bounded SACK-hole retransmits,
// and all outstanding packets on timeout. A tail beyond the last SACK is not
// evidence of loss and is left to timeout recovery.
type SACKRecovery struct{ Limit int }

func (r SACKRecovery) OnFeedback(pending []Packet, status *udpwire.StatusPacket) []int {
	if status == nil || r.Limit <= 0 {
		return nil
	}
	var indices []int
	for i, packet := range pending {
		if !sackCovers(status.Sacks, packet.Sequence) && sackHasHigher(status.Sacks, packet.Sequence) {
			indices = append(indices, i)
			if len(indices) >= r.Limit {
				break
			}
		}
	}
	return indices
}

func (SACKRecovery) OnTimeout(pending []Packet) []int {
	indices := make([]int, len(pending))
	for i := range pending {
		indices[i] = i
	}
	return indices
}

func sackCovers(sacks []udpwire.SackRange, seq uint32) bool {
	for _, sack := range sacks {
		if seq >= sack.Start && seq <= sack.End {
			return true
		}
	}
	return false
}

func sackHasHigher(sacks []udpwire.SackRange, seq uint32) bool {
	for _, sack := range sacks {
		if sack.End > seq {
			return true
		}
	}
	return false
}
