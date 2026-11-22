package tuning

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestMergeOnlyOverridesSetFields(t *testing.T) {
	base := Defaults()
	got := base.Merge(Tuning{Parallelism: 8, UDPFlowControl: " BBR "})
	if got.Parallelism != 8 || got.UDPFlowControl != FlowBBR {
		t.Fatalf("override not applied: %+v", got)
	}
	if got.Concurrency != base.Concurrency || got.UDPMTU != base.UDPMTU {
		t.Fatalf("unset fields changed: %+v", got)
	}
}

func TestValidate(t *testing.T) {
	if err := (Tuning{}).Validate(); err != nil {
		t.Fatalf("empty tuning should be valid: %v", err)
	}
	if err := Defaults().Validate(); err != nil {
		t.Fatalf("defaults should be valid: %v", err)
	}
	for name, bad := range map[string]Tuning{
		"small chunk": {ChunkSizeBytes: 10},
		"big mtu":     {UDPMTU: 70000},
		"flow":        {UDPFlowControl: "cubic"},
	} {
		if err := bad.Validate(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestPBRoundTrip(t *testing.T) {
	in := Defaults()
	if got := FromPB(in.PB()); got != in {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, in)
	}
	if !proto.Equal(FromPB(nil).PB(), Tuning{}.PB()) {
		t.Fatal("nil should convert to zero tuning")
	}
}
