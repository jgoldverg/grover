package control

import (
	"context"

	"github.com/jgoldverg/grover/config"
	"github.com/jgoldverg/grover/pb"
)

type GroverUdpServer struct {
	pb.UnimplementedGroverServerServer
	cfg *config.ServerConfig
}

func NewGroverUdpServer(cfg *config.ServerConfig) *GroverUdpServer {
	return &GroverUdpServer{
		cfg: cfg,
	}
}

func StartServer(cmd context.Context, in *pb.StartServerRequest) (*pb.StartServerResponse, error) {

	return nil, nil
}

func StopServer(cmd context.Context, in *pb.StopServerRequest) (*pb.StopServerResponse, error) {
	return nil, nil
}
