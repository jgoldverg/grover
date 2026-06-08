package gserver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jgoldverg/grover/internal"
	groverPb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RouteSessionControlService struct {
	groverPb.UnimplementedRouteSessionControlServer

	manager *RouteSessionManager
}

func NewRouteSessionControlService() *RouteSessionControlService {
	return &RouteSessionControlService{manager: NewRouteSessionManager()}
}

func (s *RouteSessionControlService) CreateRouteSession(ctx context.Context, req *groverPb.CreateRouteSessionRequest) (*groverPb.CreateRouteSessionResponse, error) {
	internal.Info("rpc RouteSessionControl.CreateRouteSession received", internal.Fields{
		"route_id":          req.GetRouteId(),
		"session_id":        req.GetSessionId(),
		"job_id":            req.GetJobId(),
		"protocol":          req.GetProtocol().String(),
		"connection_origin": req.GetConnectionOrigin().String(),
		"data_direction":    req.GetDataDirection().String(),
		"hops":              len(req.GetHops()),
		"reverse_hops":      len(req.GetReverseHops()),
	})
	createReq, err := createRouteSessionRequestFromPB(req)
	if err != nil {
		internal.Warn("rpc RouteSessionControl.CreateRouteSession rejected", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          req.GetRouteId(),
			"session_id":        req.GetSessionId(),
		})
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	session, err := s.manager.Create(createReq)
	if err != nil {
		internal.Warn("rpc RouteSessionControl.CreateRouteSession failed", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          req.GetRouteId(),
			"session_id":        req.GetSessionId(),
		})
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	internal.Info("rpc RouteSessionControl.CreateRouteSession completed", internal.Fields{
		"route_id":            session.RouteID,
		"session_id":          session.SessionID,
		"state":               session.State.String(),
		"source_data":         endpointLabel(session.Source.GetDataEndpoint()),
		"destination_data":    endpointLabel(session.Destination.GetDataEndpoint()),
		"reverse_source":      endpointLabel(session.ReverseSource.GetDataEndpoint()),
		"reverse_destination": endpointLabel(session.ReverseDest.GetDataEndpoint()),
		"hops":                len(session.Hops),
		"reverse_hops":        len(session.ReverseHops),
	})
	return &groverPb.CreateRouteSessionResponse{Session: routeSessionToPB(session)}, nil
}

func (s *RouteSessionControlService) GetRouteSession(ctx context.Context, req *groverPb.GetRouteSessionRequest) (*groverPb.GetRouteSessionResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	internal.Info("rpc RouteSessionControl.GetRouteSession received", internal.Fields{"session_id": sessionID})
	session, ok := s.manager.Get(sessionID)
	if !ok {
		internal.Warn("rpc RouteSessionControl.GetRouteSession not found", internal.Fields{"session_id": sessionID})
		return nil, status.Errorf(codes.NotFound, "route session %q not found", sessionID)
	}
	internal.Info("rpc RouteSessionControl.GetRouteSession completed", internal.Fields{
		"route_id":   session.RouteID,
		"session_id": session.SessionID,
		"state":      session.State.String(),
	})
	return &groverPb.GetRouteSessionResponse{Session: routeSessionToPB(session)}, nil
}

func (s *RouteSessionControlService) ListRouteSessions(ctx context.Context, req *groverPb.ListRouteSessionsRequest) (*groverPb.ListRouteSessionsResponse, error) {
	sessions := s.manager.List(req.GetRouteId(), req.GetJobId())
	internal.Info("rpc RouteSessionControl.ListRouteSessions completed", internal.Fields{
		"route_id": req.GetRouteId(),
		"job_id":   req.GetJobId(),
		"sessions": len(sessions),
	})
	return &groverPb.ListRouteSessionsResponse{Sessions: routeSessionsToPB(sessions)}, nil
}

func (s *RouteSessionControlService) DeleteRouteSession(ctx context.Context, req *groverPb.DeleteRouteSessionRequest) (*groverPb.DeleteRouteSessionResponse, error) {
	return &groverPb.DeleteRouteSessionResponse{Ok: s.manager.Delete(req.GetSessionId())}, nil
}

func (s *RouteSessionControlService) AbortRouteSession(ctx context.Context, req *groverPb.AbortRouteSessionRequest) (*groverPb.AbortRouteSessionResponse, error) {
	session, err := s.manager.Abort(req.GetSessionId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &groverPb.AbortRouteSessionResponse{Session: routeSessionToPB(session)}, nil
}

func (s *RouteSessionControlService) UpdateRouteSessionState(ctx context.Context, req *groverPb.UpdateRouteSessionStateRequest) (*groverPb.UpdateRouteSessionStateResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "update route session state request is required")
	}
	internal.Info("rpc RouteSessionControl.UpdateRouteSessionState received", internal.Fields{
		"session_id": req.GetSessionId(),
		"state":      req.GetState().String(),
		"error_text": strings.TrimSpace(req.GetErrorMessage()),
	})
	state := req.GetState()
	if !validRouteSessionState(state) {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported route session state %s", state.String())
	}
	session, err := s.manager.updateState(req.GetSessionId(), state, strings.TrimSpace(req.GetErrorMessage()))
	if err != nil {
		internal.Warn("rpc RouteSessionControl.UpdateRouteSessionState failed", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        req.GetSessionId(),
			"state":             state.String(),
		})
		return nil, status.Error(codes.NotFound, err.Error())
	}
	internal.Info("rpc RouteSessionControl.UpdateRouteSessionState completed", internal.Fields{
		"route_id":   session.RouteID,
		"session_id": session.SessionID,
		"state":      session.State.String(),
	})
	return &groverPb.UpdateRouteSessionStateResponse{Session: routeSessionToPB(session)}, nil
}

func (s *RouteSessionControlService) StreamRouteSessionStats(req *groverPb.StreamRouteSessionStatsRequest, stream groverPb.RouteSessionControl_StreamRouteSessionStatsServer) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			if req.GetSessionId() != "" {
				session, ok := s.manager.Get(req.GetSessionId())
				if !ok {
					return status.Errorf(codes.NotFound, "route session %q not found", req.GetSessionId())
				}
				if err := stream.Send(routeSessionToPB(session)); err != nil {
					return err
				}
				continue
			}
			for _, session := range s.manager.List(req.GetRouteId(), req.GetJobId()) {
				if err := stream.Send(routeSessionToPB(session)); err != nil {
					return err
				}
			}
		}
	}
}

func createRouteSessionRequestFromPB(req *groverPb.CreateRouteSessionRequest) (CreateRouteSessionRequest, error) {
	if req == nil {
		return CreateRouteSessionRequest{}, fmt.Errorf("create route session request is required")
	}
	origin, err := routeConnectionOriginFromPB(req.GetConnectionOrigin())
	if err != nil {
		return CreateRouteSessionRequest{}, err
	}
	direction, err := routeDataDirectionFromPB(req.GetDataDirection())
	if err != nil {
		return CreateRouteSessionRequest{}, err
	}
	return CreateRouteSessionRequest{
		SessionID:        req.GetSessionId(),
		RouteID:          req.GetRouteId(),
		JobID:            req.GetJobId(),
		Protocol:         req.GetProtocol(),
		ConnectionOrigin: origin,
		DataDirection:    direction,
		Source:           req.GetSource(),
		Destination:      req.GetDestination(),
		Hops:             routeSessionHopsFromPB(req.GetHops()),
		ReverseSource:    req.GetReverseSource(),
		ReverseDest:      req.GetReverseDestination(),
		ReverseHops:      routeSessionHopsFromPB(req.GetReverseHops()),
	}, nil
}

func routeConnectionOriginFromPB(origin groverPb.ConnectionOrigin) (ConnectionOrigin, error) {
	switch origin {
	case groverPb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE:
		return ConnectionOriginSource, nil
	case groverPb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION:
		return ConnectionOriginDestination, nil
	default:
		return "", fmt.Errorf("connection_origin is required")
	}
}

func routeConnectionOriginToPB(origin ConnectionOrigin) groverPb.ConnectionOrigin {
	switch origin {
	case ConnectionOriginSource:
		return groverPb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
	case ConnectionOriginDestination:
		return groverPb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION
	default:
		return groverPb.ConnectionOrigin_CONNECTION_ORIGIN_UNSPECIFIED
	}
}

func routeDataDirectionFromPB(direction groverPb.DataDirection) (DataDirection, error) {
	switch direction {
	case groverPb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION:
		return DataDirectionSourceToDestination, nil
	case groverPb.DataDirection_DATA_DIRECTION_DESTINATION_TO_SOURCE:
		return DataDirectionDestinationToSource, nil
	default:
		return "", fmt.Errorf("data_direction is required")
	}
}

func routeDataDirectionToPB(direction DataDirection) groverPb.DataDirection {
	switch direction {
	case DataDirectionSourceToDestination:
		return groverPb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION
	case DataDirectionDestinationToSource:
		return groverPb.DataDirection_DATA_DIRECTION_DESTINATION_TO_SOURCE
	default:
		return groverPb.DataDirection_DATA_DIRECTION_UNSPECIFIED
	}
}

func routeSessionHopsFromPB(hops []*groverPb.RouteSessionHop) []RouteSessionHop {
	if len(hops) == 0 {
		return nil
	}
	out := make([]RouteSessionHop, 0, len(hops))
	for _, hop := range hops {
		if hop == nil {
			continue
		}
		out = append(out, RouteSessionHop{
			HopIndex: hop.GetHopIndex(),
			NodeID:   hop.GetNodeId(),
			Control:  hop.GetControlEndpoint(),
			Ingress:  hop.GetIngress(),
			Egress:   hop.GetEgress(),
			State:    hop.GetState(),
			Stats:    hop.GetStats(),
			Error:    hop.GetErrorMessage(),
		})
	}
	return out
}

func routeSessionToPB(session *RouteSession) *groverPb.RouteSession {
	if session == nil {
		return nil
	}
	return &groverPb.RouteSession{
		SessionId:          session.SessionID,
		RouteId:            session.RouteID,
		JobId:              session.JobID,
		Protocol:           session.Protocol,
		ConnectionOrigin:   routeConnectionOriginToPB(session.ConnectionOrigin),
		DataDirection:      routeDataDirectionToPB(session.DataDirection),
		Source:             cloneTransferEndpoint(session.Source),
		Destination:        cloneTransferEndpoint(session.Destination),
		Hops:               routeSessionHopsToPB(session.Hops),
		ReverseSource:      cloneTransferEndpoint(session.ReverseSource),
		ReverseDestination: cloneTransferEndpoint(session.ReverseDest),
		ReverseHops:        routeSessionHopsToPB(session.ReverseHops),
		State:              session.State,
		Stats:              cloneStatsSnapshot(session.Stats),
		ErrorMessage:       session.Error,
		CreatedAtUnixNano:  session.CreatedAt.UnixNano(),
		UpdatedAtUnixNano:  session.UpdatedAt.UnixNano(),
	}
}

func routeSessionsToPB(sessions []*RouteSession) []*groverPb.RouteSession {
	if len(sessions) == 0 {
		return nil
	}
	out := make([]*groverPb.RouteSession, 0, len(sessions))
	for _, session := range sessions {
		out = append(out, routeSessionToPB(session))
	}
	return out
}

func routeSessionHopsToPB(hops []RouteSessionHop) []*groverPb.RouteSessionHop {
	if len(hops) == 0 {
		return nil
	}
	out := make([]*groverPb.RouteSessionHop, 0, len(hops))
	for _, hop := range hops {
		out = append(out, &groverPb.RouteSessionHop{
			HopIndex:        hop.HopIndex,
			NodeId:          hop.NodeID,
			ControlEndpoint: hop.Control,
			Ingress:         cloneEndpoint(hop.Ingress),
			Egress:          cloneEndpoint(hop.Egress),
			State:           hop.State,
			Stats:           cloneStatsSnapshot(hop.Stats),
			ErrorMessage:    hop.Error,
		})
	}
	return out
}

func validRouteSessionState(state groverPb.RuntimeState) bool {
	switch state {
	case groverPb.RuntimeState_RUNTIME_STATE_PREPARING,
		groverPb.RuntimeState_RUNTIME_STATE_READY,
		groverPb.RuntimeState_RUNTIME_STATE_RUNNING,
		groverPb.RuntimeState_RUNTIME_STATE_DONE,
		groverPb.RuntimeState_RUNTIME_STATE_ABORTED,
		groverPb.RuntimeState_RUNTIME_STATE_FAILED,
		groverPb.RuntimeState_RUNTIME_STATE_EXPIRED:
		return true
	default:
		return false
	}
}
