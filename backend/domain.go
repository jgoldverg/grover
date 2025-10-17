package backend

import (
	"github.com/jgoldverg/grover/backend/chunker"
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
	XXH3
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

type Endpoint struct {
	Raw            string
	Scheme         string
	Paths          []string
	CredentialHint string
	CredentialID   string
}

type TransferEdge struct {
	SourceIndex int
	DestIndex   int
	SourcePath  string
	DestPath    string
	Options     map[string]string
}

type TransferParams struct {
	Concurrency    uint32
	Parallelism    uint32
	Pipelining     uint32
	ChunkSize      uint64
	RateLimitMbps  uint32
	Overwrite      OverwritePolicy
	Checksum       CheckSumType
	VerifyChecksum bool
	MaxRetries     uint32
	RetryBackoffMs uint32
	BatchSize      uint32
}

type TransferRequest struct {
	Sources        []Endpoint
	Destinations   []Endpoint
	Edges          []TransferEdge
	Params         TransferParams
	IdempotencyKey string
	DeleteSource   bool
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
