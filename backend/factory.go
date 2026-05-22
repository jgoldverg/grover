package backend

import (
	"fmt"

	"github.com/jgoldverg/grover/backend/filesystem"
	"github.com/jgoldverg/grover/backend/localfs"
)

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
