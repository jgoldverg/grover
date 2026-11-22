// Package tuning is the single definition of transfer tuning: defaults,
// validation, layering (server config -> job request -> live updates), and
// conversion to/from the protobuf message.
//
// Zero values always mean "unset, inherit from the layer below".
package tuning

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jgoldverg/grover/internal/pb"
)

const (
	MinChunkSize = 4 << 10
	MaxChunkSize = 1 << 30
	// UDP payload limits for one datagram.
	MinUDPMTU = 512
	MaxUDPMTU = 65507

	FlowFixed = "fixed"
	FlowBBR   = "bbr"
)

// Tuning holds every per-job knob. TOML tags make it usable directly in the
// groverd config file under a [tuning] table.
type Tuning struct {
	Concurrency        uint32 `toml:"concurrency" json:"concurrency,omitempty"`
	Parallelism        uint32 `toml:"parallelism" json:"parallelism,omitempty"`
	ChunkSizeBytes     uint64 `toml:"chunk_size_bytes" json:"chunk_size_bytes,omitempty"`
	UDPMTU             uint32 `toml:"udp_mtu" json:"udp_mtu,omitempty"`
	UDPWindowPackets   uint32 `toml:"udp_window_packets" json:"udp_window_packets,omitempty"`
	UDPBatchPackets    uint32 `toml:"udp_batch_packets" json:"udp_batch_packets,omitempty"`
	UDPAckEveryPackets uint32 `toml:"udp_ack_every_packets" json:"udp_ack_every_packets,omitempty"`
	UDPAckEveryMs      uint32 `toml:"udp_ack_every_ms" json:"udp_ack_every_ms,omitempty"`
	UDPSocketRead      uint64 `toml:"udp_socket_read_bytes" json:"udp_socket_read_bytes,omitempty"`
	UDPSocketWrite     uint64 `toml:"udp_socket_write_bytes" json:"udp_socket_write_bytes,omitempty"`
	UDPWriteFlushBytes uint64 `toml:"udp_write_flush_bytes" json:"udp_write_flush_bytes,omitempty"`
	UDPFlowControl     string `toml:"udp_flow_control" json:"udp_flow_control,omitempty"`
}

// Defaults are the built-in values used when nothing else is configured.
func Defaults() Tuning {
	return Tuning{
		Concurrency:        4,
		Parallelism:        1,
		ChunkSizeBytes:     1 << 20,
		UDPMTU:             1472,
		UDPWindowPackets:   4096,
		UDPBatchPackets:    64,
		UDPAckEveryPackets: 32,
		UDPAckEveryMs:      5,
		UDPSocketRead:      8 << 20,
		UDPSocketWrite:     8 << 20,
		UDPWriteFlushBytes: 1 << 20,
		UDPFlowControl:     FlowFixed,
	}
}

// Merge returns base with every non-zero field of override applied.
func (base Tuning) Merge(override Tuning) Tuning {
	out := base
	set32 := func(dst *uint32, v uint32) {
		if v != 0 {
			*dst = v
		}
	}
	set64 := func(dst *uint64, v uint64) {
		if v != 0 {
			*dst = v
		}
	}
	set32(&out.Concurrency, override.Concurrency)
	set32(&out.Parallelism, override.Parallelism)
	set64(&out.ChunkSizeBytes, override.ChunkSizeBytes)
	set32(&out.UDPMTU, override.UDPMTU)
	set32(&out.UDPWindowPackets, override.UDPWindowPackets)
	set32(&out.UDPBatchPackets, override.UDPBatchPackets)
	set32(&out.UDPAckEveryPackets, override.UDPAckEveryPackets)
	set32(&out.UDPAckEveryMs, override.UDPAckEveryMs)
	set64(&out.UDPSocketRead, override.UDPSocketRead)
	set64(&out.UDPSocketWrite, override.UDPSocketWrite)
	set64(&out.UDPWriteFlushBytes, override.UDPWriteFlushBytes)
	if s := strings.ToLower(strings.TrimSpace(override.UDPFlowControl)); s != "" {
		out.UDPFlowControl = s
	}
	return out
}

// Validate checks the fields that are set. It accepts partial tunings, so it
// is safe on requests as well as fully merged values.
func (t Tuning) Validate() error {
	var errs []error
	if t.ChunkSizeBytes != 0 && (t.ChunkSizeBytes < MinChunkSize || t.ChunkSizeBytes > MaxChunkSize) {
		errs = append(errs, fmt.Errorf("chunk size must be between %d and %d bytes, got %d", MinChunkSize, MaxChunkSize, t.ChunkSizeBytes))
	}
	if t.UDPMTU != 0 && (t.UDPMTU < MinUDPMTU || t.UDPMTU > MaxUDPMTU) {
		errs = append(errs, fmt.Errorf("udp mtu must be between %d and %d, got %d", MinUDPMTU, MaxUDPMTU, t.UDPMTU))
	}
	if t.Concurrency > 1024 {
		errs = append(errs, fmt.Errorf("concurrency must be <= 1024, got %d", t.Concurrency))
	}
	if t.Parallelism > 1024 {
		errs = append(errs, fmt.Errorf("parallelism must be <= 1024, got %d", t.Parallelism))
	}
	switch strings.ToLower(strings.TrimSpace(t.UDPFlowControl)) {
	case "", FlowFixed, FlowBBR:
	default:
		errs = append(errs, fmt.Errorf("udp flow control must be fixed or bbr, got %q", t.UDPFlowControl))
	}
	return errors.Join(errs...)
}

// UDPAckEvery returns the ACK interval as a duration.
func (t Tuning) UDPAckEvery() time.Duration {
	return time.Duration(t.UDPAckEveryMs) * time.Millisecond
}

// FromPB converts a protobuf tuning (nil is all zeros).
func FromPB(p *pb.Tuning) Tuning {
	if p == nil {
		return Tuning{}
	}
	return Tuning{
		Concurrency:        p.GetConcurrency(),
		Parallelism:        p.GetParallelism(),
		ChunkSizeBytes:     p.GetChunkSizeBytes(),
		UDPMTU:             p.GetUdpMtu(),
		UDPWindowPackets:   p.GetUdpWindowPackets(),
		UDPBatchPackets:    p.GetUdpBatchPackets(),
		UDPAckEveryPackets: p.GetUdpAckEveryPackets(),
		UDPAckEveryMs:      p.GetUdpAckEveryMs(),
		UDPSocketRead:      p.GetUdpSocketReadBytes(),
		UDPSocketWrite:     p.GetUdpSocketWriteBytes(),
		UDPWriteFlushBytes: p.GetUdpWriteFlushBytes(),
		UDPFlowControl:     p.GetUdpFlowControl(),
	}
}

// PB converts to the protobuf message.
func (t Tuning) PB() *pb.Tuning {
	return &pb.Tuning{
		Concurrency:         t.Concurrency,
		Parallelism:         t.Parallelism,
		ChunkSizeBytes:      t.ChunkSizeBytes,
		UdpMtu:              t.UDPMTU,
		UdpWindowPackets:    t.UDPWindowPackets,
		UdpBatchPackets:     t.UDPBatchPackets,
		UdpAckEveryPackets:  t.UDPAckEveryPackets,
		UdpAckEveryMs:       t.UDPAckEveryMs,
		UdpSocketReadBytes:  t.UDPSocketRead,
		UdpSocketWriteBytes: t.UDPSocketWrite,
		UdpWriteFlushBytes:  t.UDPWriteFlushBytes,
		UdpFlowControl:      t.UDPFlowControl,
	}
}
