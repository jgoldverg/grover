package gclient

import (
	"context"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc"
)

type RelayControlService struct {
	rc pb.RelayControlClient
}

func NewRelayControlService(conn *grpc.ClientConn) *RelayControlService {
	return &RelayControlService{rc: pb.NewRelayControlClient(conn)}
}

func (s *RelayControlService) CreateForward(ctx context.Context, req *pb.CreateForwardRequest) (*pb.ForwardSession, error) {
	resp, err := s.rc.CreateForward(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetForward(), nil
}

func (s *RelayControlService) GetForward(ctx context.Context, forwardID string) (*pb.ForwardSession, error) {
	resp, err := s.rc.GetForward(ctx, &pb.GetForwardRequest{ForwardId: forwardID})
	if err != nil {
		return nil, err
	}
	return resp.GetForward(), nil
}

func (s *RelayControlService) ListForwards(ctx context.Context, routeID, jobID string) ([]*pb.ForwardSession, error) {
	resp, err := s.rc.ListForwards(ctx, &pb.ListForwardsRequest{RouteId: routeID, JobId: jobID})
	if err != nil {
		return nil, err
	}
	return resp.GetForwards(), nil
}

func (s *RelayControlService) DeleteForward(ctx context.Context, forwardID string) (bool, error) {
	resp, err := s.rc.DeleteForward(ctx, &pb.DeleteForwardRequest{ForwardId: forwardID})
	if err != nil {
		return false, err
	}
	return resp.GetOk(), nil
}

func (s *RelayControlService) StreamForwardStats(ctx context.Context, forwardID, routeID, jobID string) (pb.RelayControl_StreamForwardStatsClient, error) {
	return s.rc.StreamForwardStats(ctx, &pb.StreamForwardStatsRequest{ForwardId: forwardID, RouteId: routeID, JobId: jobID})
}
