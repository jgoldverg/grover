package udpwire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	Version         byte = 1
	KindData        byte = 1
	KindStatus      byte = 2
	DataHeaderLen        = 26
	StatusHeaderLen      = 15
	SackBlockLen         = 8 // two uint32 values
)

type DataPacket struct {
	SessionID uint32
	StreamID  uint32
	Seq       uint32
	Offset    uint64
	Payload   []byte
	Checksum  uint32 // CRC32 over payload
}

func (p *DataPacket) Encode(dst []byte) (int, error) {
	need := DataHeaderLen + len(p.Payload) + 4 // 4 bytes for checksum
	if len(dst) < need {
		return 0, errors.New("buffer too small")
	}
	dst[0] = Version
	dst[1] = KindData
	binary.BigEndian.PutUint32(dst[2:6], p.SessionID)
	binary.BigEndian.PutUint32(dst[6:10], p.StreamID)
	binary.BigEndian.PutUint32(dst[10:14], p.Seq)
	binary.BigEndian.PutUint64(dst[14:22], p.Offset)
	binary.BigEndian.PutUint32(dst[22:26], uint32(len(p.Payload)))

	copy(dst[26:], p.Payload)
	sum := crc32.ChecksumIEEE(p.Payload)
	binary.BigEndian.PutUint32(dst[26+len(p.Payload):need], sum)
	return need, nil
}

func (p *DataPacket) Decode(src []byte) (int, error) {
	if len(src) < DataHeaderLen+4 {
		return 0, errors.New("packet length too short")
	}
	if src[0] != Version || src[1] != KindData {
		return 0, errors.New("wrong version/kind of packet")
	}
	p.SessionID = binary.BigEndian.Uint32(src[2:6])
	p.StreamID = binary.BigEndian.Uint32(src[6:10])
	p.Seq = binary.BigEndian.Uint32(src[10:14])
	p.Offset = binary.BigEndian.Uint64(src[14:22])
	payloadLen := int(binary.BigEndian.Uint32(src[22:26]))
	needed := DataHeaderLen + payloadLen + 4
	if len(src) < DataHeaderLen+payloadLen+4 {
		return 0, errors.New("payload truncated")
	}
	payload := src[26 : 26+payloadLen]
	checksum := binary.BigEndian.Uint32(src[26+payloadLen : needed])
	if crc32.ChecksumIEEE(payload) != checksum {
		return 0, errors.New("checksum mismatch")
	}
	p.Payload = payload
	p.Checksum = checksum
	return needed, nil
}

type SackRange struct {
	Start uint32
	End   uint32
}

func PeekKind(src []byte) (byte, bool) {
	if len(src) < 2 {
		return 0, false
	}
	if src[0] != Version {
		return 0, false
	}
	return src[1], true
}

func IsDataPacket(src []byte) bool {
	kind, ok := PeekKind(src)
	return ok && kind == KindData
}

func IsStatusPacket(src []byte) bool {
	kind, ok := PeekKind(src)
	return ok && kind == KindStatus
}

type StatusPacket struct {
	SessionID uint32
	StreamID  uint32
	AckSeq    uint32
	Sacks     []SackRange
}

func (sp *StatusPacket) Encode(dst []byte) (int, error) {
	if len(sp.Sacks) > 255 {
		return 0, errors.New("too many SACK ranges")
	}
	need := StatusHeaderLen + len(sp.Sacks)*SackBlockLen
	if len(dst) < need {
		return 0, errors.New("buffer too small")
	}

	dst[0] = Version
	dst[1] = KindStatus
	binary.BigEndian.PutUint32(dst[2:6], sp.SessionID)
	binary.BigEndian.PutUint32(dst[6:10], sp.StreamID)
	binary.BigEndian.PutUint32(dst[10:14], sp.AckSeq)
	dst[14] = byte(len(sp.Sacks))

	for i, rng := range sp.Sacks {
		offset := StatusHeaderLen + i*SackBlockLen
		binary.BigEndian.PutUint32(dst[offset:offset+4], rng.Start)
		binary.BigEndian.PutUint32(dst[offset+4:offset+8], rng.End)
	}
	return need, nil
}

func (sp *StatusPacket) Decode(src []byte) (int, error) {
	if len(src) < StatusHeaderLen {
		return 0, errors.New("packet too short")
	}
	if src[0] != Version || src[1] != KindStatus {
		return 0, errors.New("wrong version/kind")
	}

	sp.SessionID = binary.BigEndian.Uint32(src[2:6])
	sp.StreamID = binary.BigEndian.Uint32(src[6:10])
	sp.AckSeq = binary.BigEndian.Uint32(src[10:14])

	count := int(src[14])
	need := StatusHeaderLen + count*SackBlockLen
	if len(src) < need {
		return 0, errors.New("packet truncated")
	}

	if count == 0 {
		sp.Sacks = nil
		return need, nil
	}

	if cap(sp.Sacks) < count {
		sp.Sacks = make([]SackRange, count)
	} else {
		sp.Sacks = sp.Sacks[:count]
	}

	for i := 0; i < count; i++ {
		offset := StatusHeaderLen + i*SackBlockLen
		sp.Sacks[i] = SackRange{
			Start: binary.BigEndian.Uint32(src[offset : offset+4]),
			End:   binary.BigEndian.Uint32(src[offset+4 : offset+8]),
		}
	}

	return need, nil
}

type HelloPacket struct {
	SessionID string
	Token     string
}

func (hp *HelloPacket) Encode(buf []byte) (int, error) {
	sid := []byte(hp.SessionID)
	tok := []byte(hp.Token)
	sessionLen := len(sid)
	tokLen := len(tok)

	if sessionLen > 0xffff || tokLen > 0xffff {
		return 0, fmt.Errorf("session or token too long")
	}

	totalLen := 2 + 2 + sessionLen + tokLen

	if len(buf) < totalLen {
		return 0, fmt.Errorf("buffer too small: need %d, got %d", totalLen, len(buf))
	}
	binary.BigEndian.PutUint16(buf[0:2], uint16(sessionLen))
	binary.BigEndian.PutUint16(buf[2:4], uint16(tokLen))
	offset := 4
	copy(buf[offset:offset+sessionLen], sid)
	offset += sessionLen
	copy(buf[offset:offset+tokLen], tok)
	offset += tokLen
	return offset, nil
}

func (hp *HelloPacket) Decode(buf []byte) (int, error) {
	if len(buf) < 4 {
		return 0, fmt.Errorf("buffer too small for header: %d", len(buf))
	}

	sessionLen := int(binary.BigEndian.Uint16(buf[0:2]))
	tokenLen := int(binary.BigEndian.Uint16(buf[2:4]))

	totalLen := 4 + sessionLen + tokenLen
	if len(buf) < totalLen {
		return 0, fmt.Errorf("buffer too small for payload: need %d, got %d", totalLen, len(buf))
	}

	offset := 4
	hp.SessionID = string(buf[offset : offset+sessionLen])
	offset += sessionLen
	hp.Token = string(buf[offset : offset+tokenLen])
	offset += tokenLen

	return offset, nil
}
