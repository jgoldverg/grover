// pkg/gserver/udpwire/mtu_probe.go
package udpwire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	// 4-byte magic "GWMT"
	magic   uint32 = 0x47574D54
	version uint8  = 1

	// Kinds (no global protocol needed; these are local to MTU probing)
	KindMtuProbe    uint8 = 0xA1
	KindMtuProbeAck uint8 = 0xA2

	// Fixed header: magic(4) + version(1) + kind(1) + flags(1) + rsv(1) = 8 bytes
	fixedHdrLen = 8

	// Common fields after header:
	// token(8) + probe_id(4) + size(2) + time(8) = 22 bytes
	commonBodyLen = 22

	// Minimum total lengths (header + body); probe may add padding.
	minProbeLen = fixedHdrLen + commonBodyLen // 30 bytes (without padding)
	ackLen      = fixedHdrLen + commonBodyLen // exactly 30 bytes

	// Max UDP payload (application-visible) per RFC
	maxUDPPayload = 65507
)

// BuildMtuProbe creates a datagram whose TOTAL UDP payload length equals targetUDPPayload.
// The packet starts with the small MTU header inside this file; no other protocol code is needed.
func BuildMtuProbe(token uint64, probeID uint32, targetUDPPayload int) ([]byte, error) {
	if targetUDPPayload < minProbeLen {
		return nil, fmt.Errorf("targetUDPPayload too small: need >= %d, got %d", minProbeLen, targetUDPPayload)
	}
	if targetUDPPayload > maxUDPPayload {
		return nil, fmt.Errorf("targetUDPPayload too big: %d > %d", targetUDPPayload, maxUDPPayload)
	}
	b := make([]byte, targetUDPPayload)

	// Header
	binary.BigEndian.PutUint32(b[0:4], magic)
	b[4] = version
	b[5] = KindMtuProbe
	b[6] = 0 // flags
	b[7] = 0 // reserved

	// Body (common)
	off := fixedHdrLen
	binary.BigEndian.PutUint64(b[off+0:off+8], token)
	binary.BigEndian.PutUint32(b[off+8:off+12], probeID)
	binary.BigEndian.PutUint16(b[off+12:off+14], uint16(targetUDPPayload))
	binary.BigEndian.PutUint64(b[off+14:off+22], uint64(time.Now().UnixNano()))

	// The rest (if any) is padding (left as zeros) to reach targetUDPPayload.
	return b, nil
}

// ParseMtuProbe validates and unpacks a probe.
// Returns: token, probeID, target UDP payload, tx time, padding bytes, error
func ParseMtuProbe(dgram []byte) (uint64, uint32, int, time.Time, int, error) {
	if len(dgram) < minProbeLen {
		return 0, 0, 0, time.Time{}, 0, errors.New("mtu_probe too short")
	}
	if binary.BigEndian.Uint32(dgram[0:4]) != magic {
		return 0, 0, 0, time.Time{}, 0, errors.New("bad magic")
	}
	if dgram[4] != version {
		return 0, 0, 0, time.Time{}, 0, errors.New("bad version")
	}
	if dgram[5] != KindMtuProbe {
		return 0, 0, 0, time.Time{}, 0, errors.New("not an MTU_PROBE")
	}
	off := fixedHdrLen
	token := binary.BigEndian.Uint64(dgram[off+0 : off+8])
	probeID := binary.BigEndian.Uint32(dgram[off+8 : off+12])
	target := int(binary.BigEndian.Uint16(dgram[off+12 : off+14]))
	tx := time.Unix(0, int64(binary.BigEndian.Uint64(dgram[off+14:off+22])))
	pad := len(dgram) - minProbeLen
	return token, probeID, target, tx, pad, nil
}

// BuildMtuProbeAck creates a tiny ack (always 30 bytes).
// observedUDPPayload should be len(receivedDatagram).
func BuildMtuProbeAck(token uint64, probeID uint32, observedUDPPayload int) ([]byte, error) {
	if observedUDPPayload <= 0 || observedUDPPayload > maxUDPPayload {
		return nil, fmt.Errorf("observedUDPPayload out of range: %d", observedUDPPayload)
	}
	b := make([]byte, ackLen)

	binary.BigEndian.PutUint32(b[0:4], magic)
	b[4] = version
	b[5] = KindMtuProbeAck
	b[6] = 0 // flags
	b[7] = 0 // reserved

	off := fixedHdrLen
	binary.BigEndian.PutUint64(b[off+0:off+8], token)
	binary.BigEndian.PutUint32(b[off+8:off+12], probeID)
	binary.BigEndian.PutUint16(b[off+12:off+14], uint16(observedUDPPayload))
	binary.BigEndian.PutUint64(b[off+14:off+22], uint64(time.Now().UnixNano()))
	return b, nil
}

// ParseMtuProbeAck validates and unpacks an ack.
// Returns: token, probeID, observed UDP payload, rx time, error
func ParseMtuProbeAck(dgram []byte) (uint64, uint32, int, time.Time, error) {
	if len(dgram) != ackLen {
		return 0, 0, 0, time.Time{}, errors.New("mtu_probe_ack wrong length")
	}
	if binary.BigEndian.Uint32(dgram[0:4]) != magic {
		return 0, 0, 0, time.Time{}, errors.New("bad magic")
	}
	if dgram[4] != version {
		return 0, 0, 0, time.Time{}, errors.New("bad version")
	}
	if dgram[5] != KindMtuProbeAck {
		return 0, 0, 0, time.Time{}, errors.New("not an MTU_PROBE_ACK")
	}
	off := fixedHdrLen
	token := binary.BigEndian.Uint64(dgram[off+0 : off+8])
	probeID := binary.BigEndian.Uint32(dgram[off+8 : off+12])
	observed := int(binary.BigEndian.Uint16(dgram[off+12 : off+14]))
	rx := time.Unix(0, int64(binary.BigEndian.Uint64(dgram[off+14:off+22])))
	return token, probeID, observed, rx, nil
}

// IsMtuProbe/IsMtuProbeAck let your handler cheaply detect these without a registry.
func IsMtuProbe(dgram []byte) bool {
	return len(dgram) >= minProbeLen &&
		binary.BigEndian.Uint32(dgram[0:4]) == magic &&
		dgram[4] == version &&
		dgram[5] == KindMtuProbe
}

func IsMtuProbeAck(dgram []byte) bool {
	return len(dgram) == ackLen &&
		binary.BigEndian.Uint32(dgram[0:4]) == magic &&
		dgram[4] == version &&
		dgram[5] == KindMtuProbeAck
}

// ObservedUDPPayloadLen is simply the length of the datagram you read.
func ObservedUDPPayloadLen(dgram []byte) int { return len(dgram) }
