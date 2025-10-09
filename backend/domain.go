package backend

import (
	"github.com/jgoldverg/grover/backend/chunker"
	"github.com/jgoldverg/grover/backend/filesystem"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

type BackendType string

const (
	LOCALFSBackend BackendType = "localfs"
	HTTPBackend    BackendType = "http"
	GROVERBackend  BackendType = "grover"
	UnknownBackend BackendType = "unknown"
)

type CheckSumType int

const (
	NONE = iota
	MD5
	SHA256SUM
)

type OverwritePolicy int

const (
	UNSPECIFIED = iota
	ALWAYS
	IF_NEWER
	NEVER
	IF_DIFFERENT
)

type Stream interface {
	Open() error
	Close() error
}

type Reader interface {
	Read() (chunker.Chunk, error)
	Stream
}

type Writer interface {
	Write([]chunker.Chunk) error
	Stream
}

var backends = map[BackendType]struct{}{
	LOCALFSBackend: {},
	HTTPBackend:    {},
	GROVERBackend:  {},
}

type TransferRequest struct {
	SourceCredId   string
	DestCredId     string
	fileEntries    []*filesystem.FileInfo
	transferParams *TransferParams
	IdempotencyKey string
}

type TransferParams struct {
	Concurrency    uint
	Parallelism    uint
	Pipelining     uint
	ChunkSize      uint
	RateLimitMbps  uint32
	owPolicy       OverwritePolicy
	checkSumType   CheckSumType
	verifyChecksum bool
	MaxRetires     uint
	RetryBackOffMs uint
}

func PbTypeToBackendType(pbBackendType pb.EndpointType) BackendType {
	switch pbBackendType {
	case pb.EndpointType_LOCAL_FS:
		return LOCALFSBackend
	case pb.EndpointType_HTTP:
		return HTTPBackend
	default:
		return UnknownBackend
	}
}

func IsBackendTypeValid(bt BackendType) bool {
	_, ok := backends[bt]
	return ok
}
