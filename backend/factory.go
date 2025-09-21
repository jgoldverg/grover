package backend

import (
	"fmt"

	"github.com/jgoldverg/grover/backend/filesystem"
	"github.com/jgoldverg/grover/backend/localfs"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

type BackendType string

const (
	LOCALFSBackend BackendType = "localfs"
	HTTPBackend    BackendType = "http"
	GROVERBackend  BackendType = "grover"
	UnknownBackend BackendType = "unknown"
)

var backends = map[BackendType]struct{}{
	LOCALFSBackend: {},
	HTTPBackend:    {},
	GROVERBackend:  {},
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

func OpsFactory(t BackendType, cred Credential) (filesystem.FileOps, error) {
	switch t {
	case LOCALFSBackend:
		return localfs.NewFileSystemOperations(), nil
	case HTTPBackend:
		return nil, fmt.Errorf("http backend not yet implemented")
	case GROVERBackend:
		return nil, fmt.Errorf("grover backend not yet implemented")
	default:
		return nil, fmt.Errorf("unknown backend type: %s", t)
	}
}

func RmFactory(kind BackendType, cred Credential) filesystem.FileOps {
	if kind == LOCALFSBackend {
		return localfs.NewFileSystemOperations()
	}
	return nil
}
