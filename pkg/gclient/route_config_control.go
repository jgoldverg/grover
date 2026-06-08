package gclient

import (
	"context"
	"time"

	"github.com/jgoldverg/grover/internal"
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
	started := time.Now()
	internal.Info("grpc RouteConfigControl.PutRoute start", internal.Fields{
		"route_id":          route.GetName(),
		"source":            route.GetSource(),
		"destination":       route.GetDestination(),
		"relays":            len(route.GetVia()),
		"protocol":          route.GetProtocol().String(),
		"connection_origin": route.GetConnectionOrigin().String(),
		"data_direction":    route.GetDataDirection().String(),
	})
	resp, err := s.rc.PutRoute(ctx, &pb.PutRouteRequest{Route: route})
	if err != nil {
		internal.Warn("grpc RouteConfigControl.PutRoute failed", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          route.GetName(),
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	out := resp.GetRoute()
	internal.Info("grpc RouteConfigControl.PutRoute done", internal.Fields{
		"route_id":     out.GetName(),
		"elapsed_ms":   time.Since(started).Milliseconds(),
		"updated_unix": out.GetUpdatedAtUnixNano(),
	})
	return out, nil
}

func (s *RouteConfigService) GetRoute(ctx context.Context, name string) (*pb.RouteConfig, error) {
	started := time.Now()
	internal.Info("grpc RouteConfigControl.GetRoute start", internal.Fields{"route_id": name})
	resp, err := s.rc.GetRoute(ctx, &pb.GetRouteRequest{Name: name})
	if err != nil {
		internal.Warn("grpc RouteConfigControl.GetRoute failed", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          name,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	route := resp.GetRoute()
	internal.Info("grpc RouteConfigControl.GetRoute done", internal.Fields{
		"route_id":          route.GetName(),
		"source":            route.GetSource(),
		"destination":       route.GetDestination(),
		"relays":            len(route.GetVia()),
		"protocol":          route.GetProtocol().String(),
		"connection_origin": route.GetConnectionOrigin().String(),
		"data_direction":    route.GetDataDirection().String(),
		"elapsed_ms":        time.Since(started).Milliseconds(),
	})
	return route, nil
}

func (s *RouteConfigService) ListRoutes(ctx context.Context) ([]*pb.RouteConfig, error) {
	started := time.Now()
	internal.Info("grpc RouteConfigControl.ListRoutes start", nil)
	resp, err := s.rc.ListRoutes(ctx, &pb.ListRoutesRequest{})
	if err != nil {
		internal.Warn("grpc RouteConfigControl.ListRoutes failed", internal.Fields{
			internal.FieldError: err.Error(),
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	routes := resp.GetRoutes()
	internal.Info("grpc RouteConfigControl.ListRoutes done", internal.Fields{
		"routes":     len(routes),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return routes, nil
}

func (s *RouteConfigService) DeleteRoute(ctx context.Context, name string) (bool, error) {
	started := time.Now()
	internal.Info("grpc RouteConfigControl.DeleteRoute start", internal.Fields{"route_id": name})
	resp, err := s.rc.DeleteRoute(ctx, &pb.DeleteRouteRequest{Name: name})
	if err != nil {
		internal.Warn("grpc RouteConfigControl.DeleteRoute failed", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          name,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return false, err
	}
	ok := resp.GetOk()
	internal.Info("grpc RouteConfigControl.DeleteRoute done", internal.Fields{
		"route_id":   name,
		"ok":         ok,
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return ok, nil
}
