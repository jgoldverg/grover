package udpdataplane

import "github.com/jgoldverg/grover/internal/udpwire"

// SACKRecovery preserves the existing policy: bounded SACK-hole retransmits,
// and the oldest timeoutRetransmitPackets packets on timeout. A tail beyond the last SACK is not
// evidence of loss and is left to timeout recovery.
type SACKRecovery struct{ Limit int }

func (r SACKRecovery) OnFeedback(pending []SentPacket, status *udpwire.StatusPacket) []int {
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

func (SACKRecovery) OnTimeout(pending []SentPacket) []int {
	n := len(pending)
	if n > timeoutRetransmitPackets {
		n = timeoutRetransmitPackets
	}
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	return indices
}

// sackCovers is used by the recovery policy only; Tracker.Acknowledge walks
// SACK ranges linearly instead.
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
