package backend

import (
	"fmt"

	"github.com/jgoldverg/grover/backend/chunker"
	"github.com/jgoldverg/grover/backend/filesystem"
	"github.com/jgoldverg/grover/backend/ghttp"
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

func RmFactory(kind BackendType, cred Credential) filesystem.FileOps {
	if kind == LOCALFSBackend {
		return localfs.NewFileSystemOperations()
	}
	return nil
}

func ReaderFactory(kind BackendType, cred Credential, ch *chunker.Chunker, file *filesystem.FileInfo) (Reader, error) {
	switch kind {
	case LOCALFSBackend:
		if ch == nil || file == nil {
			return nil, fmt.Errorf("chunker and file required for localfs reader")
		}
		return localfs.NewFileSystemIo(ch, file), nil
	case HTTPBackend:
		if file == nil {
			return nil, fmt.Errorf("file metadata required for http reader")
		}
		basicCred, ok := cred.(ghttp.BasicCredential)
		if !ok {
			return nil, fmt.Errorf("credential type %T does not support http reader", cred)
		}
		pool, err := ghttp.NewHttpClientPool(1, basicCred)
		if err != nil {
			return nil, err
		}
		return ghttp.NewHttpIo(pool, file), nil
	default:
		return nil, fmt.Errorf("reader factory not implemented for backend type: %s", kind)
	}
}

func WriterFactory(kind BackendType, cred Credential, ch *chunker.Chunker, file *filesystem.FileInfo) (Writer, error) {
	switch kind {
	case LOCALFSBackend:
		if ch == nil || file == nil {
			return nil, fmt.Errorf("chunker and file required for localfs reader")
		}
		return localfs.NewFileSystemIo(ch, file), nil
	case HTTPBackend:
		if file == nil {
			return nil, fmt.Errorf("file metadata required for http reader")
		}
		basicCred, ok := cred.(ghttp.BasicCredential)
		if !ok {
			return nil, fmt.Errorf("credential type %T does not support http reader", cred)
		}
		pool, err := ghttp.NewHttpClientPool(1, basicCred)
		if err != nil {
			return nil, err
		}
		return ghttp.NewHttpIo(pool, file), nil
	default:
		return nil, fmt.Errorf("reader factory not implemented for backend type: %s", kind)
	}
}
