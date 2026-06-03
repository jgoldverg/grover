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
	if s == nil || s.store == nil {
		return nil, status.Error(codes.Unavailable, "route config service unavailable")
	}
	route, err := routeConfigFromPB(req.GetRoute())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	route, err = s.store.Put(ctx, route)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.PutRouteResponse{Route: routeConfigToPB(route)}, nil
}

func (s *RouteConfigControlService) GetRoute(ctx context.Context, req *pb.GetRouteRequest) (*pb.GetRouteResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.Unavailable, "route config service unavailable")
	}
	name := strings.TrimSpace(req.GetName())
	route, ok, err := s.store.Get(ctx, name)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "route %q not found", name)
	}
	return &pb.GetRouteResponse{Route: routeConfigToPB(route)}, nil
}

func (s *RouteConfigControlService) ListRoutes(ctx context.Context, req *pb.ListRoutesRequest) (*pb.ListRoutesResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.Unavailable, "route config service unavailable")
	}
	routes, err := s.store.List(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := make([]*pb.RouteConfig, 0, len(routes))
	for _, route := range routes {
		out = append(out, routeConfigToPB(route))
	}
	return &pb.ListRoutesResponse{Routes: out}, nil
}

func (s *RouteConfigControlService) DeleteRoute(ctx context.Context, req *pb.DeleteRouteRequest) (*pb.DeleteRouteResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.Unavailable, "route config service unavailable")
	}
	ok, err := s.store.Delete(ctx, req.GetName())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
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
