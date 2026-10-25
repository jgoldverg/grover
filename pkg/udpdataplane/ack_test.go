package udpdataplane

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jgoldverg/grover/internal/udpreliability"

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
	pending := []SentPacket{
		{Sequence: 10, PayloadBytes: 1, Data: []byte("10")},
		{Sequence: 11, PayloadBytes: 1, Data: []byte("11")},
		{Sequence: 12, PayloadBytes: 1, Data: []byte("12")},
		{Sequence: 13, PayloadBytes: 1, Data: []byte("13")},
		{Sequence: 14, PayloadBytes: 1, Data: []byte("14")},
		{Sequence: 15, PayloadBytes: 1, Data: []byte("15")},
	}
	status := &udpwire.StatusPacket{
		AckSeq: 9,
		Sacks: []udpwire.SackRange{
			{Start: 12, End: 13},
			{Start: 15, End: 15},
		},
	}
	transport := &recordingTransport{}

	r := newSACKReliabilityController(SendConfig{Transport: transport}, nil)
	for _, p := range pending {
		r.TrackSent(p)
	}
	if err := r.retransmit(context.Background(), (udpreliability.SACKRecovery{Limit: 2}).OnFeedback(pending, status)); err != nil {
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
	pending := []SentPacket{
		{Sequence: 10, PayloadBytes: 1, Data: []byte("10")},
		{Sequence: 11, PayloadBytes: 1, Data: []byte("11")},
		{Sequence: 12, PayloadBytes: 1, Data: []byte("12")},
		{Sequence: 13, PayloadBytes: 1, Data: []byte("13")},
	}
	status := &udpwire.StatusPacket{
		AckSeq: 9,
		Sacks:  []udpwire.SackRange{{Start: 12, End: 12}},
	}
	transport := &recordingTransport{}

	r := newSACKReliabilityController(SendConfig{Transport: transport}, nil)
	for _, p := range pending {
		r.TrackSent(p)
	}
	if err := r.retransmit(context.Background(), (udpreliability.SACKRecovery{Limit: 10}).OnFeedback(pending, status)); err != nil {
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
	pending := []SentPacket{
		{Sequence: 1, PayloadBytes: 100, Data: []byte("1"), SentAt: time.Now().Add(-time.Millisecond)},
		{Sequence: 2, PayloadBytes: 100, Data: []byte("2"), SentAt: time.Now().Add(-time.Millisecond)},
	}
	r := newSACKReliabilityController(SendConfig{Transport: transport, StreamID: 7}, nil)
	for _, p := range pending {
		r.TrackSent(p)
	}
	acked, err := r.DrainFeedback(context.Background(), time.Millisecond, FeedbackUntilProgress)
	pending = r.outstanding.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if acked != 100 {
		t.Fatalf("acked = %d, want 100", acked)
	}
	if len(pending) != 1 || pending[0].Sequence != 2 {
		t.Fatalf("pending = %+v, want only seq 2", pending)
	}
	if transport.reads != 1 {
		t.Fatalf("reads = %d, want 1", transport.reads)
	}

}
