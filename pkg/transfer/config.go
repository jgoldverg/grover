package transfer

import (
	"context"
	"net"
	"time"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/metrics"
)

// Execution groups per-file configuration, runtime hooks, and progress reporting.
// Configuration values are snapshots; hooks may read live scheduling state.
type Execution struct {
	Job       JobIdentity
	Paths     PathConfig
	Target    TargetConfig
	IO        IOConfig
	TCP       TCPConfig
	UDP       UDPConfig
	Hooks     ExecutionHooks
	Progress  ProgressCallbacks
	Collector *metrics.TransferCollector
}

// JobIdentity associates execution and observability with a job and route.
type JobIdentity struct {
	ID      string
	RouteID string
}

type PathConfig struct {
	SourceRoot string
	DestRoot   string
}

type TargetConfig struct {
	Data     *pb.DataEndpoint
	Protocol pb.DataProtocol
}

type IOConfig struct{ ChunkSizeBytes uint64 }
type TCPConfig struct{ BufferBytes uint64 }

// UDPConfig is a per-file snapshot of transport tuning, not mutable job state.
type UDPConfig struct {
	MTU              int
	FlowControl      string
	WindowPackets    int
	BatchPackets     int
	AckPackets       int
	AckEvery         time.Duration
	SocketReadBytes  uint64
	SocketWriteBytes uint64
}

// ExecutionHooks are runtime dependencies, kept separate from configuration.
// StreamsPerFile reads live tuning so future stream scheduling can see updates.
type ExecutionHooks struct {
	TCPConnProvider func(context.Context) (net.Conn, error)
	StreamsPerFile  func() uint32
}

type ProgressCallbacks struct {
	OnProgress       func(filePath string, bytesRead int)
	OnStreamStart    func(filePath string, stream StreamPlan)
	OnStreamProgress func(filePath string, streamID uint32, bytesRead int)
	OnStreamDone     func(filePath string, streamID uint32, state pb.RuntimeState, errText string)
}
