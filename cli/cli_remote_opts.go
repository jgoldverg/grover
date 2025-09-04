package cli

import (
	"github.com/jgoldverg/grover/backend/fs"
	"github.com/jgoldverg/grover/pb"
)

func ListOnRemote(fs fs.BackendType, serverURL, path string) ([]fs.FileInfo, error) {

	return nil, nil
}

func RmOnRemote(fs fs.BackendType, serverURL, path string) (bool, error) {
	return false, nil
}

func BackendTypeToPbType(lt fs.BackendType) pb.EndpointType {
	switch lt {
	case fs.HTTPBackend:
		return pb.EndpointType_HTTP
	case fs.LOCALFSBackend:
		return pb.EndpointType_LOCAL_FS
	default:
		return pb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED
	}
}
