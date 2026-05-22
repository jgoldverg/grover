package gserver

import (
	"net"
	"testing"

	"github.com/jgoldverg/grover/internal"
)

func TestDataPortAllocatorEphemeralAllocation(t *testing.T) {
	allocator, err := NewDataPortAllocator(&internal.ServerConfig{
		DataBindHost:      "127.0.0.1",
		DataAdvertiseHost: "example.test",
	})
	if err != nil {
		t.Fatal(err)
	}

	udpLease, err := allocator.AllocateUDP()
	if err != nil {
		t.Fatal(err)
	}
	defer udpLease.Close()
	if udpLease.Port == 0 || udpLease.UDPConn == nil {
		t.Fatalf("unexpected udp lease: %+v", udpLease)
	}
	if udpLease.AdvertiseHost != "example.test" {
		t.Fatalf("advertise host = %q, want example.test", udpLease.AdvertiseHost)
	}

	tcpLease, err := allocator.AllocateTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer tcpLease.Close()
	if tcpLease.Port == 0 || tcpLease.TCPListener == nil {
		t.Fatalf("unexpected tcp lease: %+v", tcpLease)
	}
}

func TestDataPortAllocatorRangedAllocationAndExhaustion(t *testing.T) {
	port := freeTCPPort(t)
	allocator, err := NewDataPortAllocator(&internal.ServerConfig{
		DataBindHost: "127.0.0.1",
		DataPortMin:  port,
		DataPortMax:  port,
	})
	if err != nil {
		t.Fatal(err)
	}

	lease, err := allocator.AllocateTCP()
	if err != nil {
		t.Fatal(err)
	}
	if lease.Port != port {
		t.Fatalf("port = %d, want %d", lease.Port, port)
	}
	if _, err := allocator.AllocateTCP(); err == nil {
		t.Fatal("expected ranged tcp allocation exhaustion")
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}

	lease, err = allocator.AllocateTCP()
	if err != nil {
		t.Fatalf("expected released port to be reusable: %v", err)
	}
	_ = lease.Close()
}

func TestDataPortAllocatorRangedUDPAllocation(t *testing.T) {
	port := freeUDPPort(t)
	allocator, err := NewDataPortAllocator(&internal.ServerConfig{
		DataBindHost: "127.0.0.1",
		DataPortMin:  port,
		DataPortMax:  port,
	})
	if err != nil {
		t.Fatal(err)
	}

	lease, err := allocator.AllocateUDP()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if lease.Port != port {
		t.Fatalf("port = %d, want %d", lease.Port, port)
	}
}

func TestDataPortAllocatorRejectsInvalidRange(t *testing.T) {
	if _, err := NewDataPortAllocator(&internal.ServerConfig{DataPortMin: 5000}); err == nil {
		t.Fatal("expected error for half-configured range")
	}
	if _, err := NewDataPortAllocator(&internal.ServerConfig{DataPortMin: 6000, DataPortMax: 5000}); err == nil {
		t.Fatal("expected error for inverted range")
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).Port
}
