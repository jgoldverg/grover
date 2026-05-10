package udpdataplane

import (
	"net"
	"testing"
	"time"
)

func TestUDPConnTransportWriteBatchReadBatch(t *testing.T) {
	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Skipf("local udp sockets unavailable: %v", err)
	}
	defer serverConn.Close()

	clientConn, err := net.DialUDP("udp", nil, serverConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	client := NewUDPConnTransport(clientConn)
	server := NewUDPConnTransport(serverConn)
	if err := server.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	sent, err := client.WriteBatch([]PacketBuffer{
		{Bytes: []byte("one")},
		{Bytes: []byte("two")},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sent != 2 {
		t.Fatalf("sent %d packets, want 2", sent)
	}

	bufs := []PacketBuffer{
		{Bytes: make([]byte, 16)},
		{Bytes: make([]byte, 16)},
	}
	got, err := server.ReadBatch(bufs)
	if err != nil {
		t.Fatal(err)
	}
	if got != 2 {
		t.Fatalf("read %d packets, want 2", got)
	}
	if string(bufs[0].Bytes[:bufs[0].N]) != "one" {
		t.Fatalf("first packet = %q", string(bufs[0].Bytes[:bufs[0].N]))
	}
	if string(bufs[1].Bytes[:bufs[1].N]) != "two" {
		t.Fatalf("second packet = %q", string(bufs[1].Bytes[:bufs[1].N]))
	}
}
