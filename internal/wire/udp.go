package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// UDP control packets share the destination's data socket with udpwire data
// and status packets. Every control packet starts with
//
//	"GRVRUDP2" | kind u8 | session key u32
//
// Relays use only that prefix (see RoutingKey). Data/status packets start with
// udpwire.Version (1), so the two never collide.
const (
	udpMagic           = "GRVRUDP2"
	udpPrefixLen       = len(udpMagic) + 1 + 4
	maxUDPPayload      = 65507
	UDPStart      byte = 1 // sender -> receiver: open a file session (resent until READY/DONE)
	UDPReady      byte = 2 // receiver -> sender: session accepted, send data
	UDPDone       byte = 3 // receiver -> sender: file complete and synced
	UDPError      byte = 4 // receiver -> sender: session rejected or failed; body is a message
)

// UDPStart describes one file. The receiver writes into
// Destination/RelativePath and expects data on the listed stream ids.
type UDPStartPacket struct {
	SessionKey uint32
	Token      []byte
	FileSize   uint64
	StreamIDs  []uint32
	// Receiver-side tuning for this file; zero means the receiver's default.
	AckEveryPackets uint32
	AckEveryMs      uint32
	WriteFlushBytes uint64
	JobID           string
	RouteID         string
	Destination     string
	RelativePath    string
}

func (s UDPStartPacket) Encode() ([]byte, error) {
	if len(s.Token) != TokenLen {
		return nil, fmt.Errorf("token must be %d bytes", TokenLen)
	}
	if len(s.StreamIDs) == 0 || len(s.StreamIDs) > 1024 {
		return nil, fmt.Errorf("stream count must be 1-1024, got %d", len(s.StreamIDs))
	}
	strs := []string{s.JobID, s.RouteID, s.Destination, s.RelativePath}
	buf := appendUDPPrefix(nil, UDPStart, s.SessionKey)
	buf = append(buf, s.Token...)
	buf = binary.BigEndian.AppendUint64(buf, s.FileSize)
	buf = binary.BigEndian.AppendUint32(buf, s.AckEveryPackets)
	buf = binary.BigEndian.AppendUint32(buf, s.AckEveryMs)
	buf = binary.BigEndian.AppendUint64(buf, s.WriteFlushBytes)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(s.StreamIDs)))
	for _, id := range s.StreamIDs {
		if id == 0 {
			return nil, errors.New("stream id must be non-zero")
		}
		buf = binary.BigEndian.AppendUint32(buf, id)
	}
	for _, str := range strs {
		if len(str) > maxString {
			return nil, errors.New("udp start field too long")
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(str)))
	}
	for _, str := range strs {
		buf = append(buf, str...)
	}
	if len(buf) > maxUDPPayload {
		return nil, fmt.Errorf("udp start packet is %d bytes, exceeds datagram limit", len(buf))
	}
	return buf, nil
}

// DecodeUDPStart parses a START packet. ok=false means p is not a START packet.
func DecodeUDPStart(p []byte) (s UDPStartPacket, ok bool, err error) {
	kind, key, ok := PeekUDPControl(p)
	if !ok || kind != UDPStart {
		return s, false, nil
	}
	s.SessionKey = key
	b := p[udpPrefixLen:]
	need := func(n int) error {
		if len(b) < n {
			return errors.New("truncated udp start packet")
		}
		return nil
	}
	if err := need(TokenLen + 8 + 4 + 4 + 8 + 2); err != nil {
		return s, true, err
	}
	s.Token = append([]byte(nil), b[:TokenLen]...)
	b = b[TokenLen:]
	s.FileSize = binary.BigEndian.Uint64(b)
	s.AckEveryPackets = binary.BigEndian.Uint32(b[8:])
	s.AckEveryMs = binary.BigEndian.Uint32(b[12:])
	s.WriteFlushBytes = binary.BigEndian.Uint64(b[16:])
	n := int(binary.BigEndian.Uint16(b[24:]))
	b = b[26:]
	if n == 0 {
		return s, true, errors.New("udp start packet has no streams")
	}
	if err := need(n*4 + 8); err != nil {
		return s, true, err
	}
	s.StreamIDs = make([]uint32, n)
	for i := range s.StreamIDs {
		s.StreamIDs[i] = binary.BigEndian.Uint32(b[i*4:])
		if s.StreamIDs[i] == 0 {
			return s, true, errors.New("udp start packet has stream id 0")
		}
	}
	b = b[n*4:]
	lens := [4]int{}
	total := 0
	for i := range lens {
		lens[i] = int(binary.BigEndian.Uint16(b[i*2:]))
		total += lens[i]
	}
	b = b[8:]
	if len(b) != total {
		return s, true, errors.New("udp start packet string lengths do not match")
	}
	strs := make([]string, 4)
	for i, l := range lens {
		strs[i] = string(b[:l])
		b = b[l:]
	}
	s.JobID, s.RouteID, s.Destination, s.RelativePath = strs[0], strs[1], strs[2], strs[3]
	return s, true, nil
}

// EncodeUDPControl builds READY/DONE packets, or ERROR with a message.
func EncodeUDPControl(kind byte, key uint32, msg string) []byte {
	buf := appendUDPPrefix(nil, kind, key)
	if kind == UDPError {
		if len(msg) > 1024 {
			msg = msg[:1024]
		}
		buf = append(buf, msg...)
	}
	return buf
}

// PeekUDPControl returns the kind and session key of a control packet.
func PeekUDPControl(p []byte) (kind byte, key uint32, ok bool) {
	if len(p) < udpPrefixLen || string(p[:len(udpMagic)]) != udpMagic {
		return 0, 0, false
	}
	return p[len(udpMagic)], binary.BigEndian.Uint32(p[len(udpMagic)+1:]), true
}

// UDPErrorMessage returns the message carried by an ERROR packet.
func UDPErrorMessage(p []byte) string {
	if len(p) <= udpPrefixLen {
		return ""
	}
	return string(p[udpPrefixLen:])
}

// RoutingKey is what relays use: the session key of a control packet travelling
// toward the receiver (ingress: START) or back toward the sender (egress: the rest).
func RoutingKey(p []byte, ingress bool) (uint32, bool) {
	kind, key, ok := PeekUDPControl(p)
	if !ok {
		return 0, false
	}
	if ingress != (kind == UDPStart) {
		return 0, false
	}
	return key, true
}

func appendUDPPrefix(buf []byte, kind byte, key uint32) []byte {
	buf = append(buf, udpMagic...)
	buf = append(buf, kind)
	return binary.BigEndian.AppendUint32(buf, key)
}
