package udpwire

// SackTracker is a simple helper that tracks the highest contiguous ACKed
// sequence number along with out-of-order gaps that should be advertised as
// SACK ranges. It is not thread safe and is meant to be owned by a single
// goroutine per stream.
type SackTracker struct {
	ackSeq uint32
	gaps   []SackRange
}

// NewSackTracker constructs a tracker initialized to zero.
func NewSackTracker() *SackTracker {
	return &SackTracker{
		ackSeq: 0,
		gaps:   make([]SackRange, 0),
	}
}

// OnPacket records seq and returns true if the ACK/SACK state changed.
func (st *SackTracker) OnPacket(seq uint32) bool {
	if st == nil {
		return false
	}
	if st.ackSeq >= seq {
		// already acked
		return false
	}
	if seq == st.ackSeq+1 {
		changed := true
		st.ackSeq++
		for len(st.gaps) > 0 && st.gaps[0].Start <= st.ackSeq+1 {
			gap := st.gaps[0]
			if gap.Start > st.ackSeq+1 {
				break
			}
			if gap.Start == st.ackSeq+1 {
				st.ackSeq = gap.End
				st.gaps = st.gaps[1:]
				continue
			}
			st.gaps[0].Start = st.ackSeq + 1
			break
		}
		return changed
	}

	gaps := st.gaps
	for i := 0; i < len(gaps); i++ {
		g := gaps[i]
		switch {
		case seq < g.Start-1:
			gaps = append(gaps[:i], append([]SackRange{{seq, seq}}, gaps[i:]...)...)
			st.gaps = gaps
			return true
		case seq == g.Start-1:
			gaps[i].Start--
			st.gaps = gaps
			return true
		case seq >= g.Start && seq <= g.End:
			return false
		case seq == g.End+1:
			gaps[i].End++
			if i+1 < len(gaps) {
				next := gaps[i+1]
				if gaps[i].End+1 >= next.Start {
					if next.End > gaps[i].End {
						gaps[i].End = next.End
					}
					gaps = append(gaps[:i+1], gaps[i+2:]...)
				}
			}
			st.gaps = gaps
			return true
		}
	}
	st.gaps = append(st.gaps, SackRange{Start: seq, End: seq})
	return true
}

// Snapshot copies up to limit SACK ranges into dst and returns the current
// ackSeq plus the slice containing the copied ranges. The returned slice shares
// backing storage with dst and will be re-used on the next call, so callers
// should treat it as read-only.
func (st *SackTracker) Snapshot(limit int, dst []SackRange) (uint32, []SackRange) {
	if st == nil {
		if dst == nil {
			return 0, nil
		}
		return 0, dst[:0]
	}
	if limit <= 0 || limit > len(st.gaps) {
		limit = len(st.gaps)
	}
	dst = dst[:0]
	if limit == 0 {
		return st.ackSeq, dst
	}
	if cap(dst) < limit {
		dst = make([]SackRange, 0, limit)
	}
	dst = append(dst, st.gaps[:limit]...)
	return st.ackSeq, dst
}
