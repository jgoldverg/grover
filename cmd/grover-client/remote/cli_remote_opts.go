package remote

import (
	"context"
	"fmt"

	"github.com/jgoldverg/grover/backend/fs"
	"github.com/jgoldverg/grover/pb"
	"google.golang.org/grpc"
)

func ListOnRemote(fl fs.ListerType, serverURL, path string) ([]fs.FileInfo, error) {
	conn, err := grpc.NewClient(serverURL)
	if err != nil {
		return nil, err
	}

	et := EndpointTypeFromListerType(fl)

	if et == pb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED {
		return nil, fmt.Errorf("endpoint type is incorrect got: %s", fl)
	}

	req := pb.ListFilesRequest{
		Type: et,
		Path: path,
	}

	rpcClient := pb.NewFileServiceClient(conn)
	resp, err := rpcClient.List(context.Background(), &req)
	if err != nil {
		return nil, err
	}
	output := make([]fs.FileInfo, len(resp.GetFiles()))

	for i, val := range resp.GetFiles() {
		output[i] = fs.FileInfo{
			ID:      val.GetId(),
			Size:    val.GetSize(),
			AbsPath: val.GetPath(),
		}
	}
	return output, nil

}

func ListerTypeFromEndpointType(et pb.EndpointType) fs.ListerType {
	switch et {
	case pb.EndpointType_HTTP:
		return fs.ListerHTTP
	case pb.EndpointType_LOCAL_FS:
		return fs.ListerFilesystem
	default:
		return "" // or some default / error value
	}
}

func EndpointTypeFromListerType(lt fs.ListerType) pb.EndpointType {
	switch lt {
	case fs.ListerHTTP:
		return pb.EndpointType_HTTP
	case fs.ListerFilesystem:
		return pb.EndpointType_LOCAL_FS
	default:
		return pb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED
	}
}
