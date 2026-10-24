package gserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const (
	maxUDPDatagramPayloadSize      = 65507
	minUDPJobPayloadSize           = 512
	udpJobMagicV2                  = "GROVERJOBUDP2"
	currentUDPJobMagic             = udpJobMagicV2
	udpJobPacketStart         byte = 1
	udpJobPacketReady         byte = 2
	udpJobPacketDone          byte = 3
)

var udpJobMagic = []byte(currentUDPJobMagic)

type udpJobStartPacket struct {
	sessionKey uint32
	size       uint64
	streamIDs  []uint32
	relPath    string
	jobID      string
	routeID    string
}

func encodeUDPJobStartPacket(start udpJobStartPacket) ([]byte, error) {
	pathBytes := []byte(start.relPath)
	jobBytes := []byte(strings.TrimSpace(start.jobID))
	routeBytes := []byte(strings.TrimSpace(start.routeID))
	if len(pathBytes) == 0 {
		return nil, errors.New("udp job path is required")
	}
	if len(start.streamIDs) == 0 {
		return nil, errors.New("at least one udp stream id is required")
	}
	if len(start.streamIDs) > 1<<16-1 {
		return nil, fmt.Errorf("too many udp streams: %d", len(start.streamIDs))
	}
	if len(jobBytes) > 1<<16-1 || len(routeBytes) > 1<<16-1 {
		return nil, fmt.Errorf("udp job metadata exceeds packet limit: job_id=%d route_id=%d", len(jobBytes), len(routeBytes))
	}
	headerLen := len(udpJobMagic) + 1 + 4 + 8 + 2 + len(start.streamIDs)*4 + 4 + 2 + 2
	if headerLen+len(pathBytes)+len(jobBytes)+len(routeBytes) > maxUDPDatagramPayloadSize {
		return nil, fmt.Errorf("udp start packet exceeds datagram payload limit: path=%d job_id=%d route_id=%d streams=%d", len(pathBytes), len(jobBytes), len(routeBytes), len(start.streamIDs))
	}
	packet := make([]byte, headerLen+len(pathBytes)+len(jobBytes)+len(routeBytes))
	copy(packet, udpJobMagic)
	pos := len(udpJobMagic)
	packet[pos] = udpJobPacketStart
	pos++
	binary.BigEndian.PutUint32(packet[pos:pos+4], start.sessionKey)
	pos += 4
	binary.BigEndian.PutUint64(packet[pos:pos+8], start.size)
	pos += 8
	binary.BigEndian.PutUint16(packet[pos:pos+2], uint16(len(start.streamIDs)))
	pos += 2
	for _, streamID := range start.streamIDs {
		if streamID == 0 {
			return nil, errors.New("udp stream id must be non-zero")
		}
		binary.BigEndian.PutUint32(packet[pos:pos+4], streamID)
		pos += 4
	}
	binary.BigEndian.PutUint32(packet[pos:pos+4], uint32(len(pathBytes)))
	pos += 4
	binary.BigEndian.PutUint16(packet[pos:pos+2], uint16(len(jobBytes)))
	pos += 2
	binary.BigEndian.PutUint16(packet[pos:pos+2], uint16(len(routeBytes)))
	pos += 2
	copy(packet[pos:], pathBytes)
	pos += len(pathBytes)
	copy(packet[pos:], jobBytes)
	pos += len(jobBytes)
	copy(packet[pos:], routeBytes)
	return packet, nil
}

func decodeUDPJobStartPacket(packet []byte) (udpJobStartPacket, bool, error) {
	minLen := len(udpJobMagic) + 1 + 4 + 8 + 2 + 4 + 2 + 2
	if len(packet) < minLen || string(packet[:len(udpJobMagic)]) != string(udpJobMagic) {
		return udpJobStartPacket{}, false, nil
	}
	pos := len(udpJobMagic)
	if packet[pos] != udpJobPacketStart {
		return udpJobStartPacket{}, false, nil
	}
	pos++
	start := udpJobStartPacket{}
	start.sessionKey = binary.BigEndian.Uint32(packet[pos : pos+4])
	pos += 4
	start.size = binary.BigEndian.Uint64(packet[pos : pos+8])
	pos += 8
	streamCount := int(binary.BigEndian.Uint16(packet[pos : pos+2]))
	pos += 2
	if streamCount == 0 || pos+streamCount*4+4 > len(packet) {
		return udpJobStartPacket{}, true, errors.New("invalid udp start packet stream ids")
	}
	start.streamIDs = make([]uint32, streamCount)
	for i := 0; i < streamCount; i++ {
		start.streamIDs[i] = binary.BigEndian.Uint32(packet[pos : pos+4])
		if start.streamIDs[i] == 0 {
			return udpJobStartPacket{}, true, errors.New("invalid zero udp stream id")
		}
		pos += 4
	}
	pathLen := int(binary.BigEndian.Uint32(packet[pos : pos+4]))
	pos += 4
	jobLen := int(binary.BigEndian.Uint16(packet[pos : pos+2]))
	pos += 2
	routeLen := int(binary.BigEndian.Uint16(packet[pos : pos+2]))
	pos += 2
	if pathLen == 0 || pos+pathLen+jobLen+routeLen != len(packet) {
		return udpJobStartPacket{}, true, errors.New("invalid udp start packet path")
	}
	start.relPath = string(packet[pos : pos+pathLen])
	pos += pathLen
	start.jobID = string(packet[pos : pos+jobLen])
	pos += jobLen
	start.routeID = string(packet[pos : pos+routeLen])
	return start, true, nil
}

func encodeUDPJobReadyPacket(sessionKey uint32) []byte {
	return encodeUDPJobControlPacket(udpJobPacketReady, sessionKey)
}

func encodeUDPJobDonePacket(sessionKey uint32) []byte {
	return encodeUDPJobControlPacket(udpJobPacketDone, sessionKey)
}

func encodeUDPJobControlPacket(kind byte, sessionKey uint32) []byte {
	packet := make([]byte, len(udpJobMagic)+1+4)
	copy(packet, udpJobMagic)
	pos := len(udpJobMagic)
	packet[pos] = kind
	pos++
	binary.BigEndian.PutUint32(packet[pos:pos+4], sessionKey)
	return packet
}

func isUDPJobReadyPacket(packet []byte, sessionKey uint32) bool {
	return isUDPJobControlPacket(packet, udpJobPacketReady, sessionKey)
}

func isUDPJobDonePacket(packet []byte, sessionKey uint32) bool {
	return isUDPJobControlPacket(packet, udpJobPacketDone, sessionKey)
}

func isUDPJobControlPacket(packet []byte, kind byte, sessionKey uint32) bool {
	if len(packet) != len(udpJobMagic)+1+4 || string(packet[:len(udpJobMagic)]) != string(udpJobMagic) {
		return false
	}
	pos := len(udpJobMagic)
	if packet[pos] != kind {
		return false
	}
	pos++
	return binary.BigEndian.Uint32(packet[pos:pos+4]) == sessionKey
}
