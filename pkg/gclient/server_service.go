package gclient

import (
	"context"
	"errors"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"google.golang.org/grpc"
)

type ServerService struct {
	cfg *internal.AppConfig
	gs  pb.GroverServerClient
	pb.UnimplementedGroverServerServer
}

var ErrFileTransferNotImplemented = errors.New("file transfer client not implemented yet")

func NewServerService(cfg *internal.AppConfig, conn *grpc.ClientConn) *ServerService {
	return &ServerService{
		cfg: cfg,
		gs:  pb.NewGroverServerClient(conn),
	}
}

func (g *ServerService) CreatePorts(ctx context.Context, portCount uint32) ([]uint32, error) {
	req := pb.CreateUdpPortsRequest{PortCount: portCount}
	resp, err := g.gs.CreateUdpPorts(ctx, &req)
	if err != nil {
		return nil, err
	}
	return resp.GetPorts(), nil
}

func (g *ServerService) ListPorts(ctx context.Context) ([]uint32, error) {
	req := pb.ListPortRequest{}
	resp, err := g.gs.ListPorts(ctx, &req)
	return resp.GetPort(), err
}

func (g *ServerService) StartServer(ctx context.Context) (uint32, error) {
	req := pb.StartServerRequest{
		UdpPort:        0,
		WorkersPerFile: 0,
		MaxTransfers:   0,
	}
	resp, err := g.gs.StartServer(ctx, &req)
	if err != nil {
		return 0, err
	}

	if resp.GetOk() {
		return resp.GetPort(), nil
	}
	return 0, errors.New("start server failed: " + resp.GetMessage())
}

func (g *ServerService) StopServer(ctx context.Context) (string, error) {
	req := pb.StopServerRequest{}

	resp, err := g.gs.StopServer(ctx, &req)
	if err != nil {
		return "", err
	}

	if !resp.GetOk() {
		return "", errors.New("stop server failed: " + resp.GetMessage())
	} else {
		return resp.GetMessage(), nil
	}
}

func (g *ServerService) DeletePorts(ctx context.Context, ports []uint32) (bool, error) {
	req := pb.DeleteUdpPortsRequest{
		PortNum: ports,
	}
	resp, err := g.gs.DeleteUdpPorts(ctx, &req)

	if err != nil {
		return false, err
	}
	return resp.Ok, nil
}
