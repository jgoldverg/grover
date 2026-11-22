package dataport

import (
	"net"
	"strconv"
	"testing"
)

func TestEphemeralAllocation(t *testing.T) {
	a, err := New(Options{BindHost: "127.0.0.1", AdvertiseHost: "example.test"})
	if err != nil {
		t.Fatal(err)
	}
	u, err := a.AllocateUDP()
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if u.Port == 0 || u.UDP == nil || u.AdvertiseHost != "example.test" {
		t.Fatalf("bad udp lease: %+v", u)
	}
	tl, err := a.AllocateTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer tl.Close()
	if tl.Port == 0 || tl.TCP == nil {
		t.Fatalf("bad tcp lease: %+v", tl)
	}
}

func TestRangeAllocationReusesReleasedPorts(t *testing.T) {
	// Find two adjacent free ports.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := probe.Addr().(*net.TCPAddr).Port
	probe.Close()
	a, err := New(Options{BindHost: "127.0.0.1", MinPort: base, MaxPort: base})
	if err != nil {
		t.Fatal(err)
	}
	first, err := a.AllocateTCP()
	if err != nil {
		t.Skipf("port %d not available: %v", base, err)
	}
	if _, err := a.AllocateTCP(); err == nil {
		t.Fatal("range of one should be exhausted")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	_ = first.Close() // idempotent
	again, err := a.AllocateTCP()
	if err != nil {
		t.Fatalf("released port should be reusable: %v", err)
	}
	defer again.Close()
	if strconv.Itoa(again.Port) != strconv.Itoa(base) {
		t.Fatalf("got port %d want %d", again.Port, base)
	}
}

func TestInvalidRange(t *testing.T) {
	if _, err := New(Options{MinPort: 10}); err == nil {
		t.Fatal("expected error")
	}
}
