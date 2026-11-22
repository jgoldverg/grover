package wire

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestHelloRoundTrip(t *testing.T) {
	tok := NewToken()
	var buf bytes.Buffer
	if err := WriteHello(&buf, tok); err != nil {
		t.Fatal(err)
	}
	got, err := ReadHello(&buf)
	if err != nil || !TokenEqual(got, tok) {
		t.Fatalf("hello: %v %x", err, got)
	}
	if TokenEqual(got, NewToken()) {
		t.Fatal("different tokens compared equal")
	}
	buf.Reset()
	_ = WriteHelloStatus(&buf, false)
	if err := ReadHelloStatus(&buf); err == nil {
		t.Fatal("rejected hello should error")
	}
}

func TestRangeHeaderRoundTrip(t *testing.T) {
	h := RangeHeader{JobID: "j", RouteID: "r", Destination: "/dst", RelativePath: "a/b.bin", StreamID: 3, FileSize: 100, Offset: 40, Length: 60}
	enc, err := h.Encode()
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(bytes.NewReader(enc))
	got, err := ReadRangeHeader(r)
	if err != nil || got != h {
		t.Fatalf("got %+v err %v", got, err)
	}
	if _, err := ReadRangeHeader(r); err != io.EOF {
		t.Fatalf("clean end should be io.EOF, got %v", err)
	}
	if _, err := (RangeHeader{FileSize: 10, Offset: 5, Length: 6}).Encode(); err == nil {
		t.Fatal("range past end of file should fail")
	}
}

func TestRangeReply(t *testing.T) {
	var buf bytes.Buffer
	_ = WriteRangeReply(&buf, nil)
	_ = WriteRangeReply(&buf, errors.New("disk full"))
	if err := ReadRangeReply(&buf); err != nil {
		t.Fatal(err)
	}
	err := ReadRangeReply(&buf)
	if !errors.Is(err, ErrRemote) || err.Error() != "destination error: disk full" {
		t.Fatalf("got %v", err)
	}
}

func TestUDPStartRoundTrip(t *testing.T) {
	s := UDPStartPacket{SessionKey: 42, Token: NewToken(), FileSize: 9, StreamIDs: []uint32{1, 2}, AckEveryPackets: 64, AckEveryMs: 2, WriteFlushBytes: 1 << 20, JobID: "j", RouteID: "r", Destination: "/d", RelativePath: "x"}
	enc, err := s.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := DecodeUDPStart(enc)
	if !ok || err != nil || !reflect.DeepEqual(got, s) {
		t.Fatalf("got %+v ok=%v err=%v", got, ok, err)
	}
	if key, ok := RoutingKey(enc, true); !ok || key != 42 {
		t.Fatal("relay ingress routing key")
	}
	if _, ok := RoutingKey(enc, false); ok {
		t.Fatal("start is not an egress packet")
	}
	done := EncodeUDPControl(UDPDone, 42, "")
	if key, ok := RoutingKey(done, false); !ok || key != 42 {
		t.Fatal("relay egress routing key")
	}
	e := EncodeUDPControl(UDPError, 7, "nope")
	if UDPErrorMessage(e) != "nope" {
		t.Fatal("error message")
	}
	if _, ok, _ := DecodeUDPStart([]byte{1, 1, 0}); ok {
		t.Fatal("data packet misdetected as start")
	}
}
