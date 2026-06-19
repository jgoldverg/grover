package gserver

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RouteConfigControlService struct {
	pb.UnimplementedRouteConfigControlServer

	store RouteStore
}

func NewRouteConfigControlService(cfg *internal.ServerConfig) (*RouteConfigControlService, error) {
	store, err := NewJSONRouteStore(cfg)
	if err != nil {
		return nil, err
	}
	return &RouteConfigControlService{store: store}, nil
}

func (s *RouteConfigControlService) PutRoute(ctx context.Context, req *pb.PutRouteRequest) (*pb.PutRouteResponse, error) {
	const rpcName = "RouteConfigControl.PutRoute"
	started := time.Now()
	if s == nil || s.store == nil {
		err := status.Error(codes.Unavailable, "route config service unavailable")
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, err
	}
	internal.RPCReceived(rpcName, internal.Fields{
		"route_id":          req.GetRoute().GetName(),
		"source":            req.GetRoute().GetSource(),
		"destination":       req.GetRoute().GetDestination(),
		"relays":            len(req.GetRoute().GetVia()),
		"protocol":          req.GetRoute().GetProtocol().String(),
		"connection_origin": req.GetRoute().GetConnectionOrigin().String(),
		"data_direction":    req.GetRoute().GetDataDirection().String(),
	})
	route, err := routeConfigFromPB(req.GetRoute())
	if err != nil {
		internal.RPCRejected(rpcName, err, internal.Fields{
			"route_id": req.GetRoute().GetName(),
		}, time.Since(started))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	route, err = s.store.Put(ctx, route)
	if err != nil {
		internal.RPCFailed(rpcName, err, internal.Fields{
			"route_id": route.Name,
		}, time.Since(started))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{
		"route_id":    route.Name,
		"source":      route.Source,
		"destination": route.Destination,
		"relays":      len(route.Via),
	}, time.Since(started))
	return &pb.PutRouteResponse{Route: routeConfigToPB(route)}, nil
}

func (s *RouteConfigControlService) GetRoute(ctx context.Context, req *pb.GetRouteRequest) (*pb.GetRouteResponse, error) {
	const rpcName = "RouteConfigControl.GetRoute"
	started := time.Now()
	if s == nil || s.store == nil {
		err := status.Error(codes.Unavailable, "route config service unavailable")
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, err
	}
	name := strings.TrimSpace(req.GetName())
	internal.RPCReceived(rpcName, internal.Fields{"route_id": name})
	route, ok, err := s.store.Get(ctx, name)
	if err != nil {
		internal.RPCFailed(rpcName, err, internal.Fields{
			"route_id": name,
		}, time.Since(started))
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !ok {
		err := status.Errorf(codes.NotFound, "route %q not found", name)
		internal.RPCFailed(rpcName, err, internal.Fields{"route_id": name}, time.Since(started))
		return nil, err
	}
	internal.RPCCompleted(rpcName, internal.Fields{
		"route_id":    route.Name,
		"source":      route.Source,
		"destination": route.Destination,
		"relays":      len(route.Via),
	}, time.Since(started))
	return &pb.GetRouteResponse{Route: routeConfigToPB(route)}, nil
}

func (s *RouteConfigControlService) ListRoutes(ctx context.Context, req *pb.ListRoutesRequest) (*pb.ListRoutesResponse, error) {
	const rpcName = "RouteConfigControl.ListRoutes"
	started := time.Now()
	if s == nil || s.store == nil {
		err := status.Error(codes.Unavailable, "route config service unavailable")
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, err
	}
	internal.RPCReceived(rpcName, nil)
	routes, err := s.store.List(ctx)
	if err != nil {
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := make([]*pb.RouteConfig, 0, len(routes))
	for _, route := range routes {
		out = append(out, routeConfigToPB(route))
	}
	internal.RPCCompleted(rpcName, internal.Fields{"routes": len(out)}, time.Since(started))
	return &pb.ListRoutesResponse{Routes: out}, nil
}

func (s *RouteConfigControlService) DeleteRoute(ctx context.Context, req *pb.DeleteRouteRequest) (*pb.DeleteRouteResponse, error) {
	const rpcName = "RouteConfigControl.DeleteRoute"
	started := time.Now()
	if s == nil || s.store == nil {
		err := status.Error(codes.Unavailable, "route config service unavailable")
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, err
	}
	internal.RPCReceived(rpcName, internal.Fields{"route_id": req.GetName()})
	ok, err := s.store.Delete(ctx, req.GetName())
	if err != nil {
		internal.RPCFailed(rpcName, err, internal.Fields{"route_id": req.GetName()}, time.Since(started))
		return nil, status.Error(codes.Internal, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{"route_id": req.GetName(), "deleted": ok}, time.Since(started))
	return &pb.DeleteRouteResponse{Ok: ok}, nil
}

func routeConfigFromPB(route *pb.RouteConfig) (RouteConfig, error) {
	if route == nil {
		return RouteConfig{}, errors.New("route is required")
	}
	out := RouteConfig{
		Name:             route.GetName(),
		Source:           route.GetSource(),
		Destination:      route.GetDestination(),
		Via:              append([]string(nil), route.GetVia()...),
		Protocol:         route.GetProtocol(),
		ConnectionOrigin: route.GetConnectionOrigin(),
		DataDirection:    route.GetDataDirection(),
	}
	if route.GetCreatedAtUnixNano() > 0 {
		out.CreatedAt = time.Unix(0, route.GetCreatedAtUnixNano()).UTC()
	}
	if route.GetUpdatedAtUnixNano() > 0 {
		out.UpdatedAt = time.Unix(0, route.GetUpdatedAtUnixNano()).UTC()
	}
	return normalizeRouteConfig(out)
}

func routeConfigToPB(route RouteConfig) *pb.RouteConfig {
	return &pb.RouteConfig{
		Name:              route.Name,
		Source:            route.Source,
		Destination:       route.Destination,
		Via:               append([]string(nil), route.Via...),
		Protocol:          route.Protocol,
		ConnectionOrigin:  route.ConnectionOrigin,
		DataDirection:     route.DataDirection,
		CreatedAtUnixNano: route.CreatedAt.UnixNano(),
		UpdatedAtUnixNano: route.UpdatedAt.UnixNano(),
	}
}
