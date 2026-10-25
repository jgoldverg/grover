// Package transferwire defines the existing UDP transfer control envelope.
// Relays use only RoutingKey to route opaque control datagrams; they never
// decode file paths, sizes, or job metadata.
package transferwire

import "encoding/binary"

const Magic = "GROVERJOBUDP2"
const (
	Start byte = 1
	Ready byte = 2
	Done  byte = 3
)

// RoutingKey reads only the session key from a transfer control envelope.
// ingress selects START; egress selects READY/DONE. Length checks preserve the
// legacy relay framing rules, including rejecting extra bytes in responses.
func RoutingKey(packet []byte, ingress bool) (uint32, bool) {
	const headerLen = len(Magic) + 1 + 4
	if len(packet) < headerLen || string(packet[:len(Magic)]) != Magic {
		return 0, false
	}
	kind := packet[len(Magic)]
	if ingress {
		if kind != Start {
			return 0, false
		}
	} else if len(packet) != headerLen || (kind != Ready && kind != Done) {
		return 0, false
	}
	return binary.BigEndian.Uint32(packet[len(Magic)+1 : headerLen]), true
}
