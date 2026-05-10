package udpdataplane

import "testing"

func TestRangeTrackerCountsUniqueCoverage(t *testing.T) {
	rt := newRangeTracker(100)
	if got := rt.addCovered(50, 100); got != 50 {
		t.Fatalf("first out-of-order coverage = %d, want 50", got)
	}
	if got := rt.addCovered(50, 100); got != 0 {
		t.Fatalf("duplicate coverage = %d, want 0", got)
	}
	if got := rt.addCovered(0, 60); got != 50 {
		t.Fatalf("overlap coverage = %d, want 50", got)
	}
	if !rt.complete() {
		t.Fatal("expected complete coverage")
	}
	if rt.covered != 100 {
		t.Fatalf("covered = %d, want 100", rt.covered)
	}
}

func TestRangeTrackerClampsToExpectedSize(t *testing.T) {
	rt := newRangeTracker(100)
	if got := rt.addCovered(90, 120); got != 10 {
		t.Fatalf("clamped coverage = %d, want 10", got)
	}
	if got := rt.addCovered(120, 140); got != 0 {
		t.Fatalf("out-of-bounds coverage = %d, want 0", got)
	}
}
