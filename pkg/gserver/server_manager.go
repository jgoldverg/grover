package gserver

import (
	"context"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"github.com/jgoldverg/grover/pkg/gserver/dataplane"
)

const DefaultMtuPort = 59001

type GroverUdpServer struct {
	pb.UnimplementedGroverServerServer
	lm      *pkg.ListenerManager
	handler dataplane.Handler // your normal app handler
	mtuPort uint32            // pinned MTU listener port (0 == not started)
}

func NewGroverUdpServer(lm *pkg.ListenerManager, appHandler dataplane.Handler) *GroverUdpServer {
	return &GroverUdpServer{
		lm:      lm,
		handler: appHandler,
	}
}

// Optionally override default port or handler before starting anything.
func (s *GroverUdpServer) SetMtuPort(port uint32) { s.mtuPort = port }

// EnableMtuListener brings up the MTU listener if not already up.
// It prefers the pinned port and falls back to ephemeral if that port is busy.
func (s *GroverUdpServer) EnableMtuListener(ctx context.Context) error {
	if s.mtuPort != 0 {
		if _, pc := s.lm.GetListener(s.mtuPort); pc != nil {
			internal.Info("mtu listener already active", internal.Fields{
				internal.FieldPort: s.mtuPort,
			})
			return nil
		}
		internal.Warn("mtu listener stale reference", internal.Fields{
			internal.FieldPort: s.mtuPort,
		})
		s.mtuPort = 0
	}
	pinned := DefaultMtuPort

	if s.mtuPort != 0 {
		pinned = int(s.mtuPort)
	}

	internal.Info("mtu listener launch attempt", internal.Fields{
		internal.FieldPort: pinned,
	})
	if _, port, err := s.lm.LaunchNewListener(ctx, pinned, &dataplane.MtuOnlyHandler{}); err == nil {
		s.mtuPort = uint32(port)
		internal.Info("mtu listener started", internal.Fields{
			internal.FieldPort:        port,
			internal.FieldKey("mode"): "pinned",
		})
		return nil
	}

	internal.Warn("mtu listener pinned failed", internal.Fields{
		internal.FieldPort: pinned,
	})
	return nil
}

func (s *GroverUdpServer) CreateUdpPorts(ctx context.Context, req *pb.CreateUdpPortsRequest) (*pb.CreateUdpPortsResponse, error) {
	internal.Info("udp port creation requested", internal.Fields{
		internal.FieldKey("count"): req.GetPortCount(),
	})
	ports := make([]uint32, req.GetPortCount())
	for i := uint32(0); i < req.GetPortCount(); i++ {
		listenerCtx := context.Background()
		_, port, err := s.lm.LaunchNewListener(listenerCtx, 0, s.handler)
		if err != nil {
			internal.Error("udp port create failed", internal.Fields{
				internal.FieldKey("index"): i,
				internal.FieldError:        err.Error(),
			})
			return &pb.CreateUdpPortsResponse{Ports: ports}, err
		}
		ports[i] = uint32(port)
	}
	internal.Info("udp ports created", internal.Fields{
		internal.FieldKey("ports"): ports,
	})
	return &pb.CreateUdpPortsResponse{Ports: ports}, nil
}

func (s *GroverUdpServer) DeleteUdpPorts(ctx context.Context, in *pb.DeleteUdpPortsRequest) (*pb.DeleteUdpPortsResponse, error) {
	for _, port := range in.GetPortNum() {
		if s.mtuPort != 0 && port == s.mtuPort {
			continue
		}
		s.lm.DeleteListener(port)
	}
	return &pb.DeleteUdpPortsResponse{Ok: true}, nil
}

func (s *GroverUdpServer) ListPorts(ctx context.Context, _ *pb.ListPortRequest) (*pb.ListPortResponse, error) {
	ports, err := s.lm.GetListeners(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.ListPortResponse{Port: ports}, nil
}

// StopServer now closes all NON-MTU ports, leaving the pinned MTU listener running.
func (s *GroverUdpServer) StopServer(ctx context.Context, _ *pb.StopServerRequest) (*pb.StopServerResponse, error) {
	ports, err := s.lm.GetListeners(ctx)
	if err != nil {
		return &pb.StopServerResponse{Ok: false, Message: err.Error()}, nil
	}
	internal.Info("udp server stop requested", internal.Fields{
		internal.FieldKey("ports"): ports,
	})
	for _, p := range ports {
		s.lm.DeleteListener(p)
	}
	internal.Info("udp server ports closed", internal.Fields{
		internal.FieldKey("remaining_mtu"): s.mtuPort,
	})
	return &pb.StopServerResponse{Ok: true}, nil
}
