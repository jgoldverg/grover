package gclient

import (
	"context"
	"time"

	"github.com/jgoldverg/grover/internal"
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
	started := time.Now()
	internal.Debug("grpc RouteSessionControl.CreateRouteSession start", internal.Fields{
		"route_id":          req.GetRouteId(),
		"session_id":        req.GetSessionId(),
		"job_id":            req.GetJobId(),
		"protocol":          req.GetProtocol().String(),
		"connection_origin": req.GetConnectionOrigin().String(),
		"data_direction":    req.GetDataDirection().String(),
		"hops":              len(req.GetHops()),
		"reverse_hops":      len(req.GetReverseHops()),
	})
	resp, err := s.rc.CreateRouteSession(ctx, req)
	if err != nil {
		internal.Warn("grpc RouteSessionControl.CreateRouteSession failed", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          req.GetRouteId(),
			"session_id":        req.GetSessionId(),
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	session := resp.GetSession()
	internal.Debug("grpc RouteSessionControl.CreateRouteSession done", internal.Fields{
		"route_id":         session.GetRouteId(),
		"session_id":       session.GetSessionId(),
		"state":            session.GetState().String(),
		"source_data":      dataEndpointLabel(session.GetSource().GetDataEndpoint()),
		"destination_data": dataEndpointLabel(session.GetDestination().GetDataEndpoint()),
		"reverse_source":   dataEndpointLabel(session.GetReverseSource().GetDataEndpoint()),
		"reverse_dest":     dataEndpointLabel(session.GetReverseDestination().GetDataEndpoint()),
		"elapsed_ms":       time.Since(started).Milliseconds(),
	})
	return session, nil
}

func (s *RouteSessionService) GetRouteSession(ctx context.Context, sessionID string) (*pb.RouteSession, error) {
	started := time.Now()
	internal.Debug("grpc RouteSessionControl.GetRouteSession start", internal.Fields{"session_id": sessionID})
	resp, err := s.rc.GetRouteSession(ctx, &pb.GetRouteSessionRequest{SessionId: sessionID})
	if err != nil {
		internal.Warn("grpc RouteSessionControl.GetRouteSession failed", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	session := resp.GetSession()
	internal.Debug("grpc RouteSessionControl.GetRouteSession done", internal.Fields{
		"route_id":   session.GetRouteId(),
		"session_id": session.GetSessionId(),
		"state":      session.GetState().String(),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return session, nil
}

func (s *RouteSessionService) ListRouteSessions(ctx context.Context, routeID, jobID string) ([]*pb.RouteSession, error) {
	started := time.Now()
	internal.Debug("grpc RouteSessionControl.ListRouteSessions start", internal.Fields{
		"route_id": routeID,
		"job_id":   jobID,
	})
	resp, err := s.rc.ListRouteSessions(ctx, &pb.ListRouteSessionsRequest{RouteId: routeID, JobId: jobID})
	if err != nil {
		internal.Warn("grpc RouteSessionControl.ListRouteSessions failed", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          routeID,
			"job_id":            jobID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	sessions := resp.GetSessions()
	internal.Debug("grpc RouteSessionControl.ListRouteSessions done", internal.Fields{
		"route_id":   routeID,
		"job_id":     jobID,
		"sessions":   len(sessions),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return sessions, nil
}

func (s *RouteSessionService) DeleteRouteSession(ctx context.Context, sessionID string) (bool, error) {
	started := time.Now()
	internal.Debug("grpc RouteSessionControl.DeleteRouteSession start", internal.Fields{"session_id": sessionID})
	resp, err := s.rc.DeleteRouteSession(ctx, &pb.DeleteRouteSessionRequest{SessionId: sessionID})
	if err != nil {
		internal.Warn("grpc RouteSessionControl.DeleteRouteSession failed", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return false, err
	}
	ok := resp.GetOk()
	internal.Debug("grpc RouteSessionControl.DeleteRouteSession done", internal.Fields{
		"session_id": sessionID,
		"ok":         ok,
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return ok, nil
}

func (s *RouteSessionService) AbortRouteSession(ctx context.Context, sessionID string) (*pb.RouteSession, error) {
	started := time.Now()
	internal.Debug("grpc RouteSessionControl.AbortRouteSession start", internal.Fields{"session_id": sessionID})
	resp, err := s.rc.AbortRouteSession(ctx, &pb.AbortRouteSessionRequest{SessionId: sessionID})
	if err != nil {
		internal.Warn("grpc RouteSessionControl.AbortRouteSession failed", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	session := resp.GetSession()
	internal.Debug("grpc RouteSessionControl.AbortRouteSession done", internal.Fields{
		"route_id":   session.GetRouteId(),
		"session_id": session.GetSessionId(),
		"state":      session.GetState().String(),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return session, nil
}

func (s *RouteSessionService) UpdateRouteSessionState(ctx context.Context, sessionID string, state pb.RuntimeState, errText string) (*pb.RouteSession, error) {
	started := time.Now()
	internal.Debug("grpc RouteSessionControl.UpdateRouteSessionState start", internal.Fields{
		"session_id": sessionID,
		"state":      state.String(),
		"error_text": errText,
	})
	resp, err := s.rc.UpdateRouteSessionState(ctx, &pb.UpdateRouteSessionStateRequest{
		SessionId:    sessionID,
		State:        state,
		ErrorMessage: errText,
	})
	if err != nil {
		internal.Warn("grpc RouteSessionControl.UpdateRouteSessionState failed", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
			"state":             state.String(),
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	session := resp.GetSession()
	internal.Debug("grpc RouteSessionControl.UpdateRouteSessionState done", internal.Fields{
		"route_id":   session.GetRouteId(),
		"session_id": session.GetSessionId(),
		"state":      session.GetState().String(),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return session, nil
}

func (s *RouteSessionService) StreamRouteSessionStats(ctx context.Context, sessionID, routeID, jobID string) (pb.RouteSessionControl_StreamRouteSessionStatsClient, error) {
	return s.rc.StreamRouteSessionStats(ctx, &pb.StreamRouteSessionStatsRequest{SessionId: sessionID, RouteId: routeID, JobId: jobID})
}
