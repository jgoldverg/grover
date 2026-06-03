package gclient

import (
	"context"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc"
)

type RouteSessionService struct {
	rc pb.RouteSessionControlClient
}

func NewRouteSessionService(conn *grpc.ClientConn) *RouteSessionService {
	return &RouteSessionService{rc: pb.NewRouteSessionControlClient(conn)}
}

func (s *RouteSessionService) CreateRouteSession(ctx context.Context, req *pb.CreateRouteSessionRequest) (*pb.RouteSession, error) {
	resp, err := s.rc.CreateRouteSession(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetSession(), nil
}

func (s *RouteSessionService) GetRouteSession(ctx context.Context, sessionID string) (*pb.RouteSession, error) {
	resp, err := s.rc.GetRouteSession(ctx, &pb.GetRouteSessionRequest{SessionId: sessionID})
	if err != nil {
		return nil, err
	}
	return resp.GetSession(), nil
}

func (s *RouteSessionService) ListRouteSessions(ctx context.Context, routeID, jobID string) ([]*pb.RouteSession, error) {
	resp, err := s.rc.ListRouteSessions(ctx, &pb.ListRouteSessionsRequest{RouteId: routeID, JobId: jobID})
	if err != nil {
		return nil, err
	}
	return resp.GetSessions(), nil
}

func (s *RouteSessionService) DeleteRouteSession(ctx context.Context, sessionID string) (bool, error) {
	resp, err := s.rc.DeleteRouteSession(ctx, &pb.DeleteRouteSessionRequest{SessionId: sessionID})
	if err != nil {
		return false, err
	}
	return resp.GetOk(), nil
}

func (s *RouteSessionService) AbortRouteSession(ctx context.Context, sessionID string) (*pb.RouteSession, error) {
	resp, err := s.rc.AbortRouteSession(ctx, &pb.AbortRouteSessionRequest{SessionId: sessionID})
	if err != nil {
		return nil, err
	}
	return resp.GetSession(), nil
}

func (s *RouteSessionService) UpdateRouteSessionState(ctx context.Context, sessionID string, state pb.RuntimeState, errText string) (*pb.RouteSession, error) {
	resp, err := s.rc.UpdateRouteSessionState(ctx, &pb.UpdateRouteSessionStateRequest{
		SessionId:    sessionID,
		State:        state,
		ErrorMessage: errText,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetSession(), nil
}

func (s *RouteSessionService) StreamRouteSessionStats(ctx context.Context, sessionID, routeID, jobID string) (pb.RouteSessionControl_StreamRouteSessionStatsClient, error) {
	return s.rc.StreamRouteSessionStats(ctx, &pb.StreamRouteSessionStatsRequest{SessionId: sessionID, RouteId: routeID, JobId: jobID})
}
