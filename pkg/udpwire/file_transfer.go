package udpwire

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	Version         byte = 1
	KindData        byte = 1
	KindStatus      byte = 2
	DataHeaderLen        = 18
	StatusHeaderLen      = 15
	SackBlockLen         = 8 // two uint32 values
)

type DataPacket struct {
	SessionID uint32
	StreamID  uint32
	Seq       uint32
	Payload   []byte
	Checksum  uint32 //CRC32 over payload
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
	binary.BigEndian.PutUint32(dst[14:18], uint32(len(p.Payload)))

	copy(dst[18:], p.Payload)
	sum := crc32.ChecksumIEEE(p.Payload)
	binary.BigEndian.PutUint32(dst[18+len(p.Payload):need], sum)
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
	payloadLen := int(binary.BigEndian.Uint32(src[14:18]))
	needed := DataHeaderLen + payloadLen + 4
	if len(src) < DataHeaderLen+payloadLen+4 {
		return 0, errors.New("payload truncated")
	}
	payload := src[18 : 18+payloadLen]
	checksum := binary.BigEndian.Uint32(src[18+payloadLen : needed])
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
