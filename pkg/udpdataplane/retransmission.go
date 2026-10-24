package udpdataplane

import "github.com/jgoldverg/grover/pkg/udpwire"

// recoveryPolicy selects indices in the outstanding queue. The caller sends
// them; policy implementations never perform I/O or change packet accounting.
type recoveryPolicy interface {
	OnFeedback([]SentPacket, *udpwire.StatusPacket) []int
	OnTimeout([]SentPacket) []int
}

// sackRecovery preserves the existing policy: bounded SACK-hole retransmits,
// and all outstanding packets on timeout. A tail beyond the last SACK is not
// evidence of loss and is left to timeout recovery.
type sackRecovery struct{ limit int }

func (r sackRecovery) OnFeedback(pending []SentPacket, status *udpwire.StatusPacket) []int {
	if status == nil || r.limit <= 0 {
		return nil
	}
	var indices []int
	for i, packet := range pending {
		if !sackCovers(status.Sacks, packet.Sequence) && sackHasHigher(status.Sacks, packet.Sequence) {
			indices = append(indices, i)
			if len(indices) >= r.limit {
				break
			}
		}
	}
	return indices
}

func (sackRecovery) OnTimeout(pending []SentPacket) []int {
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
