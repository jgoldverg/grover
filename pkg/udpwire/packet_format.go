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

	HelloMagic   = "GRVR"
	HelloVersion = 1
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
	SessionID []byte
	Token     []byte
}

func (hp *HelloPacket) Encode(buf []byte) (int, error) {
	if len(hp.SessionID) > 0xff {
		return 0, fmt.Errorf("sessionID too long for hello packet: %d", len(hp.SessionID))
	}
	if len(hp.Token) > 0xffff {
		return 0, fmt.Errorf("token too long for hello packet: %d", len(hp.Token))
	}

	totalLen := len(HelloMagic) + 1 + 1 + len(hp.SessionID) + 2 + len(hp.Token)
	if len(buf) < totalLen {
		return 0, fmt.Errorf("buffer too small: need %d, got %d", totalLen, len(buf))
	}

	offset := 0
	copy(buf[offset:], []byte(HelloMagic))
	offset += len(HelloMagic)

	buf[offset] = HelloVersion
	offset++

	buf[offset] = byte(len(hp.SessionID))
	offset++
	copy(buf[offset:offset+len(hp.SessionID)], hp.SessionID)
	offset += len(hp.SessionID)

	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(hp.Token)))
	offset += 2
	copy(buf[offset:offset+len(hp.Token)], hp.Token)
	offset += len(hp.Token)

	return offset, nil
}

func (hp *HelloPacket) Decode(buf []byte) (int, error) {
	if len(buf) < len(HelloMagic)+2 {
		return 0, fmt.Errorf("buffer too small for hello packet: %d", len(buf))
	}
	if string(buf[:len(HelloMagic)]) != HelloMagic {
		return 0, fmt.Errorf("unexpected hello magic %q", string(buf[:len(HelloMagic)]))
	}
	if buf[len(HelloMagic)] != HelloVersion {
		return 0, fmt.Errorf("unexpected hello version %d", buf[len(HelloMagic)])
	}

	offset := len(HelloMagic) + 1
	sessionLen := int(buf[offset])
	offset++
	if len(buf) < offset+sessionLen+2 {
		return 0, fmt.Errorf("hello packet truncated before session id bytes")
	}

	hp.SessionID = append(hp.SessionID[:0], buf[offset:offset+sessionLen]...)
	offset += sessionLen

	tokenLen := int(binary.BigEndian.Uint16(buf[offset : offset+2]))
	offset += 2
	if len(buf) < offset+tokenLen {
		return 0, fmt.Errorf("hello packet truncated before token bytes")
	}
	hp.Token = append(hp.Token[:0], buf[offset:offset+tokenLen]...)
	offset += tokenLen

	return offset, nil
}
