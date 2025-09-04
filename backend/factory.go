package backend

import (
	"github.com/jgoldverg/grover/backend/fs"
	"github.com/jgoldverg/grover/backend/localfs"
)

func ListerFactory(kind fs.BackendType, cred Credential) fs.ListOperation {
	if kind == fs.LOCALFSBackend {
		return localfs.NewFileSystemOperations()
	}
	return nil
}

func RmFactory(kind fs.BackendType, cred Credential) fs.RmOperation {
	if kind == fs.LOCALFSBackend {
		return localfs.NewFileSystemOperations()
	}
	return nil
}
