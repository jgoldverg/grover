package gclient

import (
	"context"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc"
)

type RouteConfigService struct {
	rc pb.RouteConfigControlClient
}

func NewRouteConfigService(conn *grpc.ClientConn) *RouteConfigService {
	return &RouteConfigService{rc: pb.NewRouteConfigControlClient(conn)}
}

func (s *RouteConfigService) PutRoute(ctx context.Context, route *pb.RouteConfig) (*pb.RouteConfig, error) {
	resp, err := s.rc.PutRoute(ctx, &pb.PutRouteRequest{Route: route})
	if err != nil {
		return nil, err
	}
	return resp.GetRoute(), nil
}

func (s *RouteConfigService) GetRoute(ctx context.Context, name string) (*pb.RouteConfig, error) {
	resp, err := s.rc.GetRoute(ctx, &pb.GetRouteRequest{Name: name})
	if err != nil {
		return nil, err
	}
	return resp.GetRoute(), nil
}

func (s *RouteConfigService) ListRoutes(ctx context.Context) ([]*pb.RouteConfig, error) {
	resp, err := s.rc.ListRoutes(ctx, &pb.ListRoutesRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetRoutes(), nil
}

func (s *RouteConfigService) DeleteRoute(ctx context.Context, name string) (bool, error) {
	resp, err := s.rc.DeleteRoute(ctx, &pb.DeleteRouteRequest{Name: name})
	if err != nil {
		return false, err
	}
	return resp.GetOk(), nil
}
