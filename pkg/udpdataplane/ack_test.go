package udpdataplane

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jgoldverg/grover/pkg/udpwire"
)

type recordingTransport struct {
	writes [][]byte
}

func (t *recordingTransport) WritePacket(packet []byte, _ *net.UDPAddr) (int, error) {
	t.writes = append(t.writes, append([]byte(nil), packet...))
	return len(packet), nil
}

func (t *recordingTransport) ReadPacket([]byte) (int, *net.UDPAddr, error) {
	return 0, nil, nil
}

func (t *recordingTransport) SetReadDeadline(time.Time) error {
	return nil
}

func (t *recordingTransport) SetWriteDeadline(time.Time) error {
	return nil
}

func (t *recordingTransport) RemoteAddr() *net.UDPAddr {
	return nil
}

func TestFastRetransmitMissingRespectsSacksAndLimit(t *testing.T) {
	pending := []pendingPacket{
		{seq: 10, payloadLen: 1, data: []byte("10")},
		{seq: 11, payloadLen: 1, data: []byte("11")},
		{seq: 12, payloadLen: 1, data: []byte("12")},
		{seq: 13, payloadLen: 1, data: []byte("13")},
		{seq: 14, payloadLen: 1, data: []byte("14")},
		{seq: 15, payloadLen: 1, data: []byte("15")},
	}
	status := &udpwire.StatusPacket{
		AckSeq: 9,
		Sacks: []udpwire.SackRange{
			{Start: 12, End: 13},
			{Start: 15, End: 15},
		},
	}
	transport := &recordingTransport{}

	if err := fastRetransmitMissing(context.Background(), transport, nil, status, &pending, nil, 2); err != nil {
		t.Fatal(err)
	}
	if got, want := len(transport.writes), 2; got != want {
		t.Fatalf("retransmits = %d, want %d", got, want)
	}
	if string(transport.writes[0]) != "10" || string(transport.writes[1]) != "11" {
		t.Fatalf("retransmitted %q, %q; want 10, 11", transport.writes[0], transport.writes[1])
	}
}

func TestFastRetransmitMissingIgnoresUnprovenTail(t *testing.T) {
	pending := []pendingPacket{
		{seq: 10, payloadLen: 1, data: []byte("10")},
		{seq: 11, payloadLen: 1, data: []byte("11")},
		{seq: 12, payloadLen: 1, data: []byte("12")},
		{seq: 13, payloadLen: 1, data: []byte("13")},
	}
	status := &udpwire.StatusPacket{
		AckSeq: 9,
		Sacks:  []udpwire.SackRange{{Start: 12, End: 12}},
	}
	transport := &recordingTransport{}

	if err := fastRetransmitMissing(context.Background(), transport, nil, status, &pending, nil, 10); err != nil {
		t.Fatal(err)
	}
	if got, want := len(transport.writes), 2; got != want {
		t.Fatalf("retransmits = %d, want %d", got, want)
	}
	if string(transport.writes[0]) != "10" || string(transport.writes[1]) != "11" {
		t.Fatalf("retransmitted %q, %q; want 10, 11", transport.writes[0], transport.writes[1])
	}
}
