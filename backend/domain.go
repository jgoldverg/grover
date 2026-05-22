package backend

import pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"

type BackendType string

const (
	LOCALFSBackend BackendType = "localfs"
	HTTPBackend    BackendType = "http"
	GROVERBackend  BackendType = "grover"
	UnknownBackend BackendType = "unknown"
)

type OverwritePolicy int

const (
	UNSPECIFIED = iota
	ALWAYS
	IF_NEWER
	NEVER
	IF_DIFFERENT
)

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
