package udpdataplane

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jgoldverg/grover/pkg/udpwire"
)

type queuedDatagramTransport struct {
	packets [][]byte
	addr    *net.UDPAddr
	writes  [][]byte
}

func (t *queuedDatagramTransport) WritePacket(packet []byte, _ *net.UDPAddr) (int, error) {
	t.writes = append(t.writes, append([]byte(nil), packet...))
	return len(packet), nil
}

func (t *queuedDatagramTransport) ReadPacket(buf []byte) (int, *net.UDPAddr, error) {
	if len(t.packets) == 0 {
		return 0, nil, io.EOF
	}
	packet := t.packets[0]
	t.packets = t.packets[1:]
	return copy(buf, packet), t.addr, nil
}

func (t *queuedDatagramTransport) SetReadDeadline(time.Time) error {
	return nil
}

func (t *queuedDatagramTransport) SetWriteDeadline(time.Time) error {
	return nil
}

func (t *queuedDatagramTransport) RemoteAddr() *net.UDPAddr {
	return nil
}

func TestReceiveReturnsIncompleteErrorOnEarlyClose(t *testing.T) {
	packet := encodeTestDataPacket(t, 77, 1, 1, 0, []byte("hello"))
	transport := &queuedDatagramTransport{
		packets: [][]byte{packet},
		addr:    &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9000},
	}
	dst := newRecordingWriterAt()

	n, err := Receive(context.Background(), ReceiveConfig{
		Transport:       transport,
		SessionID:       "test",
		SessionKey:      77,
		StreamID:        1,
		BufferSize:      1024,
		ExpectedSize:    10,
		AckEveryPackets: 1,
	}, dst)
	if err == nil || !strings.Contains(err.Error(), "udp receive incomplete") {
		t.Fatalf("error = %v, want incomplete receive", err)
	}
	if n != 5 {
		t.Fatalf("received bytes = %d, want 5", n)
	}
}

func TestReceiveCompletesWhenExpectedBytesAreDurable(t *testing.T) {
	packet := encodeTestDataPacket(t, 77, 1, 1, 0, []byte("hello"))
	transport := &queuedDatagramTransport{
		packets: [][]byte{packet},
		addr:    &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9000},
	}
	dst := newRecordingWriterAt()

	n, err := Receive(context.Background(), ReceiveConfig{
		Transport:       transport,
		SessionID:       "test",
		SessionKey:      77,
		StreamID:        1,
		BufferSize:      1024,
		ExpectedSize:    5,
		AckEveryPackets: 1,
	}, dst)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("received bytes = %d, want 5", n)
	}
	if len(transport.writes) == 0 {
		t.Fatal("expected final status ack")
	}
}

func TestEnsureReceiveCompleteAllowsUnknownExpectedSize(t *testing.T) {
	if err := ensureReceiveComplete(nil, 0); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureReceiveCompleteRejectsShortDurableBytes(t *testing.T) {
	writer, err := newDirectPayloadWriter(newRecordingWriterAt(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := writer.WritePayload(0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	err = ensureReceiveComplete(writer, 10)
	if err == nil {
		t.Fatal("expected incomplete error")
	}
}

func encodeTestDataPacket(t *testing.T, sessionKey, streamID, seq uint32, offset uint64, payload []byte) []byte {
	t.Helper()
	buf := make([]byte, udpwire.DataHeaderLen+len(payload)+4)
	copy(buf[udpwire.DataHeaderLen:], payload)
	n, err := udpwire.EncodeDataPacketInPlace(buf, sessionKey, streamID, seq, offset, len(payload))
	if err != nil {
		t.Fatal(err)
	}
	return buf[:n]
}
