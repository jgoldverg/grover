package udpdataplane

// rangeTracker counts distinct bytes received in [0, expected) and reports
// completion. Out-of-order intervals are kept sorted and merged.
type rangeTracker struct {
	expected   uint64
	contiguous uint64
	pending    []interval
	covered    uint64
}

type interval struct {
	start uint64
	end   uint64
}

func newRangeTracker(expected uint64) *rangeTracker {
	if expected == 0 {
		return nil
	}
	return &rangeTracker{
		expected: expected,
		pending:  make([]interval, 0, 16),
	}
}

func (rt *rangeTracker) addCovered(start, end uint64) uint64 {
	if rt == nil {
		return 0
	}
	if end <= start {
		return 0
	}
	if rt.expected > 0 && end > rt.expected {
		end = rt.expected
	}
	if end <= start {
		return 0
	}
	added := end - start
	if start < rt.contiguous {
		overlapEnd := minUint64(end, rt.contiguous)
		added -= overlapEnd - start
	}
	if end <= rt.contiguous {
		return 0
	}
	for _, cur := range rt.pending {
		if cur.end <= start || cur.start >= end {
			continue
		}
		overlapStart := maxUint64(start, cur.start)
		overlapEnd := minUint64(end, cur.end)
		if overlapEnd > overlapStart {
			added -= overlapEnd - overlapStart
		}
	}
	if start <= rt.contiguous {
		rt.contiguous = end
		rt.absorbPending()
	} else {
		rt.insertPending(interval{start: start, end: end})
	}
	rt.covered += added
	return added
}

func (rt *rangeTracker) complete() bool {
	if rt == nil {
		return false
	}
	return rt.expected > 0 && rt.contiguous >= rt.expected
}

func (rt *rangeTracker) absorbPending() {
	if len(rt.pending) == 0 {
		return
	}
	idx := 0
	for idx < len(rt.pending) && rt.pending[idx].start <= rt.contiguous {
		if rt.pending[idx].end > rt.contiguous {
			rt.contiguous = rt.pending[idx].end
		}
		idx++
	}
	if idx > 0 {
		copy(rt.pending[0:], rt.pending[idx:])
		rt.pending = rt.pending[:len(rt.pending)-idx]
	}
}

func (rt *rangeTracker) insertPending(seg interval) {
	if seg.end <= seg.start {
		return
	}
	pos := 0
	for pos < len(rt.pending) && rt.pending[pos].start < seg.start {
		pos++
	}
	rt.pending = append(rt.pending, interval{})
	copy(rt.pending[pos+1:], rt.pending[pos:])
	rt.pending[pos] = seg
	rt.mergeFrom(pos)
}

func (rt *rangeTracker) mergeFrom(idx int) {
	if idx > 0 && rt.pending[idx-1].end >= rt.pending[idx].start {
		if rt.pending[idx].end > rt.pending[idx-1].end {
			rt.pending[idx-1].end = rt.pending[idx].end
		}
		copy(rt.pending[idx:], rt.pending[idx+1:])
		rt.pending = rt.pending[:len(rt.pending)-1]
		idx--
	}
	for idx+1 < len(rt.pending) && rt.pending[idx].end >= rt.pending[idx+1].start {
		if rt.pending[idx+1].end > rt.pending[idx].end {
			rt.pending[idx].end = rt.pending[idx+1].end
		}
		copy(rt.pending[idx+1:], rt.pending[idx+2:])
		rt.pending = rt.pending[:len(rt.pending)-1]
	}
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
