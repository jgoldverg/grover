package gserver

import (
	"context"
	"net"
	"time"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/metrics"
)

type TransferEndpointRegistry interface {
	PrepareEndpoint(context.Context, *pb.PrepareTransferEndpointRequest) (*pb.TransferEndpoint, error)
	GetEndpoint(id string) (*pb.TransferEndpoint, bool)
}

// TransferSourcePlanner discovers files without opening network connections.
type TransferSourcePlanner interface {
	PlanFiles(context.Context, *pb.TransferEndpoint, []string) ([]TransferFilePlan, error)
}

// TransferFileExecutor moves one planned file. The manager owns scheduling.
type TransferFileExecutor interface {
	TransferFile(context.Context, *TransferExecutionContext, TransferFilePlan) error
}

// TransferJobExecutor preserves the original combined extension point.
// New implementations can use NewTransferJobManagerWithComponents instead.
type TransferJobExecutor interface {
	TransferSourcePlanner
	TransferFileExecutor
}

type TransferExecutionContext struct {
	JobID            string
	RouteID          string
	SourceRoot       string
	DestRoot         string
	DestData         *pb.DataEndpoint
	Protocol         pb.DataProtocol
	ChunkSizeBytes   uint64
	TCPBufferBytes   uint64
	UDPPayload       int
	UDPFlow          string
	UDPWindow        int
	UDPBatch         int
	UDPAckPackets    int
	UDPAckEvery      time.Duration
	UDPSocketRead    uint64
	UDPSocketWrite   uint64
	Collector        *metrics.TransferCollector
	TCPConnProvider  func(context.Context) (net.Conn, error)
	StreamsFunc      func() uint32
	OnProgress       func(filePath string, bytesRead int)
	OnStreamStart    func(filePath string, stream TransferStreamPlan)
	OnStreamProgress func(filePath string, streamID uint32, bytesRead int)
	OnStreamDone     func(filePath string, streamID uint32, state pb.RuntimeState, errText string)
}

type TransferFilePlan struct {
	SourcePath   string
	RelativePath string
	Size         uint64
}

type syntheticTransferSource struct {
	RelativePath string
	Size         uint64
}

type zeroReader struct{}

type TransferStreamPlan struct {
	StreamID uint32
	Offset   uint64
	Size     uint64
}
