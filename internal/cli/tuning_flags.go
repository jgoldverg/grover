package cli

import (
	"github.com/spf13/pflag"

	"github.com/jgoldverg/grover/internal/tuning"
)

// tuningFlags is the shared set of tuning flags for transfer, job tune, and schedule.
type tuningFlags struct {
	concurrency, parallelism       uint32
	chunk, socketRead, socketWrite string
	writeFlush                     string
	mtu, window, batch             uint32
	ackPackets, ackMs              uint32
	flow                           string
}

func (f *tuningFlags) register(fs *pflag.FlagSet) {
	fs.Uint32VarP(&f.concurrency, "concurrency", "c", 0, "files in flight (0 = node default)")
	fs.Uint32VarP(&f.parallelism, "parallelism", "P", 0, "streams per file (0 = node default)")
	fs.StringVar(&f.chunk, "chunk-size", "", "read/write buffer size, e.g. 1MiB")
	fs.Uint32Var(&f.mtu, "udp-mtu", 0, "UDP datagram size in bytes")
	fs.Uint32Var(&f.window, "udp-window", 0, "UDP max in-flight packets per stream")
	fs.Uint32Var(&f.batch, "udp-batch", 0, "UDP packets per sendmmsg/recvmmsg")
	fs.Uint32Var(&f.ackPackets, "udp-ack-packets", 0, "UDP receiver ACKs every N packets")
	fs.Uint32Var(&f.ackMs, "udp-ack-ms", 0, "UDP receiver ACK interval in ms")
	fs.StringVar(&f.socketRead, "udp-socket-read", "", "UDP sender socket receive buffer, e.g. 8MiB")
	fs.StringVar(&f.socketWrite, "udp-socket-write", "", "UDP sender socket send buffer, e.g. 8MiB")
	fs.StringVar(&f.writeFlush, "udp-write-flush", "", "UDP receiver write coalescing size, e.g. 1MiB")
	fs.StringVar(&f.flow, "udp-flow", "", "UDP flow control: fixed|bbr")
}

// tuning builds a partial Tuning (zero = unset) and validates it.
func (f *tuningFlags) tuning() (tuning.Tuning, error) {
	t := tuning.Tuning{
		Concurrency: f.concurrency, Parallelism: f.parallelism,
		UDPMTU: f.mtu, UDPWindowPackets: f.window, UDPBatchPackets: f.batch,
		UDPAckEveryPackets: f.ackPackets, UDPAckEveryMs: f.ackMs, UDPFlowControl: f.flow,
	}
	var err error
	for _, s := range []struct {
		raw string
		dst *uint64
	}{{f.chunk, &t.ChunkSizeBytes}, {f.socketRead, &t.UDPSocketRead}, {f.socketWrite, &t.UDPSocketWrite}, {f.writeFlush, &t.UDPWriteFlushBytes}} {
		if *s.dst, err = parseSize(s.raw); err != nil {
			return t, err
		}
	}
	return t, t.Validate()
}
