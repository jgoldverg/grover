package transferwire

import (
	"encoding/binary"
	"testing"
)

func TestRoutingKey(t *testing.T) {
	packet := func(kind byte, payload bool) []byte {
		p := make([]byte, len(Magic)+5)
		copy(p, Magic)
		p[len(Magic)] = kind
		binary.BigEndian.PutUint32(p[len(Magic)+1:], 42)
		if payload {
			p = append(p, []byte("opaque body")...)
		}
		return p
	}
	for _, tc := range []struct {
		name           string
		packet         []byte
		ingress, valid bool
	}{
		{"start", packet(Start, true), true, true},
		{"ready", packet(Ready, false), false, true},
		{"done", packet(Done, false), false, true},
		{"wrong direction", packet(Start, true), false, false},
		{"response body", packet(Ready, true), false, false},
		{"truncated", packet(Start, false)[:len(Magic)+4], true, false},
		{"unknown kind", packet(99, false), false, false},
		{"empty", nil, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := RoutingKey(tc.packet, tc.ingress)
			if ok != tc.valid || (ok && key != 42) {
				t.Fatalf("key=%d ok=%t", key, ok)
			}
		})
	}
}
