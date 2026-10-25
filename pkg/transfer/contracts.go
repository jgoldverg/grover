package transfer

import (
	"context"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

type EndpointRegistry interface {
	PrepareEndpoint(context.Context, *pb.PrepareTransferEndpointRequest) (*pb.TransferEndpoint, error)
	GetEndpoint(id string) (*pb.TransferEndpoint, bool)
}

// SourcePlanner discovers files without opening network connections.
type SourcePlanner interface {
	PlanFiles(context.Context, *pb.TransferEndpoint, []string) ([]FilePlan, error)
}

// FileExecutor moves one planned file. The manager owns scheduling.
type FileExecutor interface {
	TransferFile(context.Context, *Execution, FilePlan) error
}

// JobExecutor preserves the original combined extension point.
// New implementations can use NewManagerWithComponents instead.
type JobExecutor interface {
	SourcePlanner
	FileExecutor
}

type FilePlan struct {
	SourcePath   string
	RelativePath string
	Size         uint64
}

type syntheticTransferSource struct {
	RelativePath string
	Size         uint64
}

type zeroReader struct{}

type StreamPlan struct {
	StreamID uint32
	Offset   uint64
	Size     uint64
}
