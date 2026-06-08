package gclient

import (
	"context"
	"time"

	"github.com/jgoldverg/grover/internal"
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
	started := time.Now()
	internal.Info("grpc RelayControl.CreateForward start", internal.Fields{
		"route_id":    req.GetRouteId(),
		"job_id":      req.GetJobId(),
		"hop_index":   req.GetHopIndex(),
		"protocol":    req.GetProtocol().String(),
		"egress":      dataEndpointLabel(req.GetEgress()),
		"ttl_seconds": req.GetTtlSeconds(),
	})
	resp, err := s.rc.CreateForward(ctx, req)
	if err != nil {
		internal.Warn("grpc RelayControl.CreateForward failed", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          req.GetRouteId(),
			"job_id":            req.GetJobId(),
			"hop_index":         req.GetHopIndex(),
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	forward := resp.GetForward()
	internal.Info("grpc RelayControl.CreateForward done", internal.Fields{
		"route_id":   forward.GetRouteId(),
		"job_id":     forward.GetJobId(),
		"forward_id": forward.GetForwardId(),
		"hop_index":  forward.GetHopIndex(),
		"ingress":    dataEndpointLabel(forward.GetIngress()),
		"egress":     dataEndpointLabel(forward.GetEgress()),
		"state":      forward.GetState().String(),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return forward, nil
}

func (s *RelayControlService) GetForward(ctx context.Context, forwardID string) (*pb.ForwardSession, error) {
	started := time.Now()
	internal.Info("grpc RelayControl.GetForward start", internal.Fields{"forward_id": forwardID})
	resp, err := s.rc.GetForward(ctx, &pb.GetForwardRequest{ForwardId: forwardID})
	if err != nil {
		internal.Warn("grpc RelayControl.GetForward failed", internal.Fields{
			internal.FieldError: err.Error(),
			"forward_id":        forwardID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	forward := resp.GetForward()
	internal.Info("grpc RelayControl.GetForward done", internal.Fields{
		"route_id":   forward.GetRouteId(),
		"job_id":     forward.GetJobId(),
		"forward_id": forward.GetForwardId(),
		"state":      forward.GetState().String(),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return forward, nil
}

func (s *RelayControlService) ListForwards(ctx context.Context, routeID, jobID string) ([]*pb.ForwardSession, error) {
	started := time.Now()
	internal.Info("grpc RelayControl.ListForwards start", internal.Fields{
		"route_id": routeID,
		"job_id":   jobID,
	})
	resp, err := s.rc.ListForwards(ctx, &pb.ListForwardsRequest{RouteId: routeID, JobId: jobID})
	if err != nil {
		internal.Warn("grpc RelayControl.ListForwards failed", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          routeID,
			"job_id":            jobID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	forwards := resp.GetForwards()
	internal.Info("grpc RelayControl.ListForwards done", internal.Fields{
		"route_id":   routeID,
		"job_id":     jobID,
		"forwards":   len(forwards),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return forwards, nil
}

func (s *RelayControlService) DeleteForward(ctx context.Context, forwardID string) (bool, error) {
	started := time.Now()
	internal.Info("grpc RelayControl.DeleteForward start", internal.Fields{"forward_id": forwardID})
	resp, err := s.rc.DeleteForward(ctx, &pb.DeleteForwardRequest{ForwardId: forwardID})
	if err != nil {
		internal.Warn("grpc RelayControl.DeleteForward failed", internal.Fields{
			internal.FieldError: err.Error(),
			"forward_id":        forwardID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return false, err
	}
	ok := resp.GetOk()
	internal.Info("grpc RelayControl.DeleteForward done", internal.Fields{
		"forward_id": forwardID,
		"ok":         ok,
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return ok, nil
}

func (s *RelayControlService) StreamForwardStats(ctx context.Context, forwardID, routeID, jobID string) (pb.RelayControl_StreamForwardStatsClient, error) {
	return s.rc.StreamForwardStats(ctx, &pb.StreamForwardStatsRequest{ForwardId: forwardID, RouteId: routeID, JobId: jobID})
}
