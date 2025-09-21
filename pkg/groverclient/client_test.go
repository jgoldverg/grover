package groverclient

import (
	"net"
	"testing"

	"github.com/jgoldverg/grover/pkg/groverserver"
)

func TestSplitHostPortDefaultUsesDefaultMtuPort(t *testing.T) {
	host, port, err := splitHostPortDefault("localhost", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if port != int(groverserver.DefaultMtuPort) {
		t.Fatalf("expected default MTU port %d, got %d for host %q", groverserver.DefaultMtuPort, port, host)
	}
}

func TestSplitHostPortDefaultRespectsFallbackOverride(t *testing.T) {
	const custom = 42424
	host, port, err := splitHostPortDefault("example.com:54321", custom)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "example.com" {
		t.Fatalf("expected host to be example.com, got %q", host)
	}
	if port != custom {
		t.Fatalf("expected fallback port %d, got %d", custom, port)
	}
}

func TestSplitHostPortDefaultReadsPortWhenFallbackMissing(t *testing.T) {
	host, port, err := splitHostPortDefault("example.com:12345", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "example.com" {
		t.Fatalf("expected host to be example.com, got %q", host)
	}
	if port != 12345 {
		t.Fatalf("expected parsed port 12345, got %d", port)
	}
}

func TestSameEndpointRecognisesEquivalentAddrs(t *testing.T) {
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9000}
	from := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9000}
	if !sameEndpoint(from, addr) {
		t.Fatalf("expected sameEndpoint to match identical IPv4 addrs")
	}

	ip6 := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1000}
	from6 := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1000}
	if !sameEndpoint(from6, ip6) {
		t.Fatalf("expected sameEndpoint to match identical IPv6 addrs")
	}

	wrongPort := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9999}
	if sameEndpoint(wrongPort, addr) {
		t.Fatalf("expected port mismatch to return false")
	}
}
