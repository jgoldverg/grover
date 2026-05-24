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

type queuedStatusTransport struct {
	packets [][]byte
	reads   int
}

func (t *queuedStatusTransport) WritePacket([]byte, *net.UDPAddr) (int, error) {
	return 0, nil
}

func (t *queuedStatusTransport) ReadPacket(buf []byte) (int, *net.UDPAddr, error) {
	if len(t.packets) == 0 {
		return 0, nil, timeoutErr{}
	}
	packet := t.packets[0]
	t.packets = t.packets[1:]
	t.reads++
	return copy(buf, packet), nil, nil
}

func (t *queuedStatusTransport) SetReadDeadline(time.Time) error {
	return nil
}

func (t *queuedStatusTransport) SetWriteDeadline(time.Time) error {
	return nil
}

func (t *queuedStatusTransport) RemoteAddr() *net.UDPAddr {
	return nil
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

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

func TestDrainStatusPacketsCanStopAfterProgress(t *testing.T) {
	status := udpwire.StatusPacket{SessionID: 1, StreamID: 7, AckSeq: 1}
	buf := make([]byte, udpwire.StatusHeaderLen)
	n, err := status.Encode(buf)
	if err != nil {
		t.Fatal(err)
	}
	transport := &queuedStatusTransport{packets: [][]byte{append([]byte(nil), buf[:n]...)}}
	pending := []pendingPacket{
		{seq: 1, payloadLen: 100, data: []byte("1"), sentAt: time.Now().Add(-time.Millisecond)},
		{seq: 2, payloadLen: 100, data: []byte("2"), sentAt: time.Now().Add(-time.Millisecond)},
	}
	var remote *net.UDPAddr
	var ackPkt udpwire.StatusPacket
	acked, err := drainStatusPackets(
		context.Background(),
		transport,
		&remote,
		7,
		make([]byte, udpwire.StatusHeaderLen+udpwire.MaxSackRanges*udpwire.SackBlockLen),
		&ackPkt,
		&pending,
		nil,
		time.Millisecond,
		false,
		true,
		0,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if acked != 100 {
		t.Fatalf("acked = %d, want 100", acked)
	}
	if len(pending) != 1 || pending[0].seq != 2 {
		t.Fatalf("pending = %+v, want only seq 2", pending)
	}
	if transport.reads != 1 {
		t.Fatalf("reads = %d, want 1", transport.reads)
	}

}
