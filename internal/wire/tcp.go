// Package wire defines Grover's data-plane framing. It is shared by the
// transfer layer (which understands every field) and relays (which only use
// RoutingKey to route UDP replies).
//
// TCP connection:
//
//	dialer -> listener  Hello{token}
//	listener -> dialer  status byte (0 = ok)
//	then, repeated over the same connection, always source -> destination:
//	  source -> dest  RangeHeader + <length> payload bytes
//	  dest -> source  RangeReply{ok, message}
//
// The side that dials depends on the session's connect_from; the byte flow
// does not. A connection carries ranges until the source closes it.
package wire

import (
	"bufio"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	TokenLen     = 16
	helloMagic   = "GRVR"
	helloVersion = 2
	rangeMagic   = "RNG2"
	maxString    = 1<<16 - 1
)

// NewToken returns a random data-plane token.
func NewToken() []byte {
	t := make([]byte, TokenLen)
	if _, err := rand.Read(t); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	return t
}

// TokenEqual compares tokens in constant time.
func TokenEqual(a, b []byte) bool {
	return len(a) == TokenLen && subtle.ConstantTimeCompare(a, b) == 1
}

// WriteHello sends the connection hello.
func WriteHello(w io.Writer, token []byte) error {
	if len(token) != TokenLen {
		return fmt.Errorf("token must be %d bytes, got %d", TokenLen, len(token))
	}
	buf := make([]byte, 0, len(helloMagic)+1+TokenLen)
	buf = append(buf, helloMagic...)
	buf = append(buf, helloVersion)
	buf = append(buf, token...)
	_, err := w.Write(buf)
	return err
}

// ReadHello reads a hello and returns the presented token.
func ReadHello(r io.Reader) ([]byte, error) {
	buf := make([]byte, len(helloMagic)+1+TokenLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	if string(buf[:len(helloMagic)]) != helloMagic {
		return nil, errors.New("not a grover data connection")
	}
	if v := buf[len(helloMagic)]; v != helloVersion {
		return nil, fmt.Errorf("unsupported data protocol version %d (want %d); upgrade both nodes", v, helloVersion)
	}
	return buf[len(helloMagic)+1:], nil
}

// WriteHelloStatus answers a hello. ok=false means the token was rejected.
func WriteHelloStatus(w io.Writer, ok bool) error {
	b := []byte{1}
	if ok {
		b[0] = 0
	}
	_, err := w.Write(b)
	return err
}

// ReadHelloStatus reads the answer to a hello.
func ReadHelloStatus(r io.Reader) error {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return fmt.Errorf("read hello status: %w", err)
	}
	if b[0] != 0 {
		return errors.New("data connection rejected: bad token")
	}
	return nil
}

// RangeHeader describes one byte range of one file, followed by Length bytes.
type RangeHeader struct {
	JobID        string
	RouteID      string
	Destination  string // absolute destination directory
	RelativePath string // slash-separated path under Destination
	StreamID     uint32
	FileSize     uint64
	Offset       uint64
	Length       uint64
}

func (h RangeHeader) Encode() ([]byte, error) {
	strs := []string{h.JobID, h.RouteID, h.Destination, h.RelativePath}
	size := len(rangeMagic) + 4 + 8 + 8 + 8 + 2*len(strs)
	for _, s := range strs {
		if len(s) > maxString {
			return nil, fmt.Errorf("range header field too long (%d bytes)", len(s))
		}
		size += len(s)
	}
	if h.Offset > h.FileSize || h.Length > h.FileSize-h.Offset {
		return nil, fmt.Errorf("range %d+%d exceeds file size %d", h.Offset, h.Length, h.FileSize)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, rangeMagic...)
	buf = binary.BigEndian.AppendUint32(buf, h.StreamID)
	buf = binary.BigEndian.AppendUint64(buf, h.FileSize)
	buf = binary.BigEndian.AppendUint64(buf, h.Offset)
	buf = binary.BigEndian.AppendUint64(buf, h.Length)
	for _, s := range strs {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(s)))
	}
	for _, s := range strs {
		buf = append(buf, s...)
	}
	return buf, nil
}

// ReadRangeHeader reads the next range header. io.EOF means the source closed
// the connection cleanly between ranges.
func ReadRangeHeader(r *bufio.Reader) (RangeHeader, error) {
	var h RangeHeader
	fixed := make([]byte, len(rangeMagic)+4+8+8+8+2*4)
	if _, err := io.ReadFull(r, fixed[:1]); err != nil {
		return h, err // io.EOF passes through untouched
	}
	if _, err := io.ReadFull(r, fixed[1:]); err != nil {
		return h, fmt.Errorf("read range header: %w", err)
	}
	if string(fixed[:len(rangeMagic)]) != rangeMagic {
		return h, errors.New("bad range header magic")
	}
	p := fixed[len(rangeMagic):]
	h.StreamID = binary.BigEndian.Uint32(p[0:4])
	h.FileSize = binary.BigEndian.Uint64(p[4:12])
	h.Offset = binary.BigEndian.Uint64(p[12:20])
	h.Length = binary.BigEndian.Uint64(p[20:28])
	lens := p[28:36]
	strs := make([]string, 4)
	for i := range strs {
		n := binary.BigEndian.Uint16(lens[i*2:])
		b := make([]byte, n)
		if _, err := io.ReadFull(r, b); err != nil {
			return h, fmt.Errorf("read range header: %w", err)
		}
		strs[i] = string(b)
	}
	h.JobID, h.RouteID, h.Destination, h.RelativePath = strs[0], strs[1], strs[2], strs[3]
	if h.Offset > h.FileSize || h.Length > h.FileSize-h.Offset {
		return h, fmt.Errorf("range %d+%d exceeds file size %d", h.Offset, h.Length, h.FileSize)
	}
	return h, nil
}

// WriteRangeReply acknowledges a range. A nil err means the range is durable.
func WriteRangeReply(w io.Writer, err error) error {
	msg := ""
	status := byte(0)
	if err != nil {
		status = 1
		msg = err.Error()
		if len(msg) > maxString {
			msg = msg[:maxString]
		}
	}
	buf := []byte{status}
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(msg)))
	buf = append(buf, msg...)
	_, werr := w.Write(buf)
	return werr
}

// ReadRangeReply returns the destination's error for the range, if any.
// The returned error wraps ErrRemote when the destination reported a failure.
func ReadRangeReply(r io.Reader) error {
	var hdr [3]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return fmt.Errorf("read range reply: %w", err)
	}
	n := binary.BigEndian.Uint16(hdr[1:])
	msg := make([]byte, n)
	if _, err := io.ReadFull(r, msg); err != nil {
		return fmt.Errorf("read range reply: %w", err)
	}
	if hdr[0] != 0 {
		return fmt.Errorf("%w: %s", ErrRemote, msg)
	}
	return nil
}

// ErrRemote marks errors reported by the destination node.
var ErrRemote = errors.New("destination error")
