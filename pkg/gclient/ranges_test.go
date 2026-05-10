package gclient

import "testing"

func TestPlanByteRangesCoversWithoutGaps(t *testing.T) {
	for _, tc := range []struct {
		name    string
		size    int64
		streams int
	}{
		{name: "single", size: 10, streams: 1},
		{name: "even", size: 12, streams: 4},
		{name: "remainder", size: 10, streams: 3},
		{name: "more_streams_than_bytes", size: 3, streams: 10},
		{name: "zero_streams", size: 8, streams: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ranges, err := planByteRanges(tc.size, tc.streams)
			if err != nil {
				t.Fatal(err)
			}
			var next int64
			for _, r := range ranges {
				if r.offset != next {
					t.Fatalf("gap or overlap: got offset %d want %d", r.offset, next)
				}
				if r.length <= 0 {
					t.Fatalf("invalid length: %d", r.length)
				}
				next += r.length
			}
			if next != tc.size {
				t.Fatalf("covered %d bytes, want %d", next, tc.size)
			}
		})
	}
}

func TestPlanByteRangesZeroSize(t *testing.T) {
	ranges, err := planByteRanges(0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) != 0 {
		t.Fatalf("expected no ranges for zero-size file, got %d", len(ranges))
	}
}

func TestPlanByteRangesRejectsNegativeSize(t *testing.T) {
	if _, err := planByteRanges(-1, 1); err == nil {
		t.Fatal("expected error for negative size")
	}
}
