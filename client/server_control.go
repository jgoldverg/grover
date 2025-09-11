package client

import (
	"context"
	"errors"

	"github.com/jgoldverg/grover/config"
	"github.com/jgoldverg/grover/pb"
	"google.golang.org/grpc"
)

type GroverServerCommands struct {
	cfg *config.AppConfig
	gs  pb.GroverServerClient
	pb.UnimplementedGroverServerServer
}

func NewGroverServerCommands(cfg *config.AppConfig, conn *grpc.ClientConn) *GroverServerCommands {
	return &GroverServerCommands{
		cfg: cfg,
		gs:  pb.NewGroverServerClient(conn),
	}
}

func (g GroverServerCommands) StartServer(ctx context.Context) (uint32, error) {
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

func (g GroverServerCommands) StopServer(ctx context.Context) (string, error) {
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
