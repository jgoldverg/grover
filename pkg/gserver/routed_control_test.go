package gserver

import (
	"context"
	"testing"

	"github.com/jgoldverg/grover/internal"
	groverPb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRoutedControlServicesRegister(t *testing.T) {
	cfg := &internal.ServerConfig{
		InsecureControl: true,
		CredentialsFile: t.TempDir() + "/credentials.toml",
		RouteStoreFile:  t.TempDir() + "/routes.json",
	}
	server := NewGroverServer(context.Background(), cfg)

	services := server.grpcServer.GetServiceInfo()
	for _, name := range []string{
		groverPb.RouteConfigControl_ServiceDesc.ServiceName,
		groverPb.RelayControl_ServiceDesc.ServiceName,
		groverPb.RouteSessionControl_ServiceDesc.ServiceName,
		groverPb.TransferJobControl_ServiceDesc.ServiceName,
	} {
		if _, ok := services[name]; !ok {
			t.Fatalf("service %s was not registered; got %v", name, serviceNames(services))
		}
	}
}

func TestRelayControlCreateForwardValidation(t *testing.T) {
	relay, err := NewRelayControlService(&internal.ServerConfig{DataBindHost: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.manager.Close()

	if _, err := relay.CreateForward(context.Background(), &groverPb.CreateForwardRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateForward error = %v, want invalid argument", err)
	}
}

func TestTransferJobControlValidation(t *testing.T) {
	jobs := NewTransferJobControlService(nil)
	if _, err := jobs.PrepareTransferEndpoint(context.Background(), &groverPb.PrepareTransferEndpointRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("PrepareTransferEndpoint error = %v, want invalid argument", err)
	}
}

func TestRouteSessionControlLifecycle(t *testing.T) {
	routes := NewRouteSessionControlService()

	created, err := routes.CreateRouteSession(context.Background(), &groverPb.CreateRouteSessionRequest{
		SessionId:        "session-rpc",
		RouteId:          "route-rpc",
		JobId:            "job-rpc",
		Protocol:         groverPb.DataProtocol_DATA_PROTOCOL_TCP,
		ConnectionOrigin: groverPb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION,
		DataDirection:    groverPb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION,
		Source:           routeSessionControlEndpoint("src", groverPb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE),
		Destination:      routeSessionControlEndpoint("dst", groverPb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION),
		Hops: []*groverPb.RouteSessionHop{{
			HopIndex:        1,
			NodeId:          "relay-a",
			ControlEndpoint: "127.0.0.1:22446",
			Ingress:         &groverPb.DataEndpoint{Host: "127.0.0.1", Port: 30000},
			Egress:          &groverPb.DataEndpoint{Host: "127.0.0.1", Port: 30100},
			State:           groverPb.RuntimeState_RUNTIME_STATE_READY,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetSession().GetConnectionOrigin() != groverPb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
		t.Fatalf("origin = %s, want destination", created.GetSession().GetConnectionOrigin())
	}

	updated, err := routes.UpdateRouteSessionState(context.Background(), &groverPb.UpdateRouteSessionStateRequest{
		SessionId: "session-rpc",
		State:     groverPb.RuntimeState_RUNTIME_STATE_RUNNING,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.GetSession().GetState() != groverPb.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("state = %s, want running", updated.GetSession().GetState())
	}

	listed, err := routes.ListRouteSessions(context.Background(), &groverPb.ListRouteSessionsRequest{RouteId: "route-rpc", JobId: "job-rpc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetSessions()) != 1 {
		t.Fatalf("sessions = %d, want 1", len(listed.GetSessions()))
	}

	aborted, err := routes.AbortRouteSession(context.Background(), &groverPb.AbortRouteSessionRequest{SessionId: "session-rpc"})
	if err != nil {
		t.Fatal(err)
	}
	if aborted.GetSession().GetState() != groverPb.RuntimeState_RUNTIME_STATE_ABORTED {
		t.Fatalf("state = %s, want aborted", aborted.GetSession().GetState())
	}

	deleted, err := routes.DeleteRouteSession(context.Background(), &groverPb.DeleteRouteSessionRequest{SessionId: "session-rpc"})
	if err != nil {
		t.Fatal(err)
	}
	if !deleted.GetOk() {
		t.Fatal("expected delete ok")
	}
}

func TestRouteSessionControlValidation(t *testing.T) {
	routes := NewRouteSessionControlService()
	if _, err := routes.CreateRouteSession(context.Background(), &groverPb.CreateRouteSessionRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateRouteSession error = %v, want invalid argument", err)
	}
	if _, err := routes.GetRouteSession(context.Background(), &groverPb.GetRouteSessionRequest{SessionId: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetRouteSession error = %v, want not found", err)
	}
	if _, err := routes.UpdateRouteSessionState(context.Background(), &groverPb.UpdateRouteSessionStateRequest{SessionId: "missing"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateRouteSessionState error = %v, want invalid argument for unspecified state", err)
	}
}

func routeSessionControlEndpoint(root string, role groverPb.TransferEndpointRole) *groverPb.TransferEndpoint {
	return &groverPb.TransferEndpoint{
		EndpointId:   root + "-endpoint",
		RouteId:      "route-rpc",
		JobId:        "job-rpc",
		Role:         role,
		Protocol:     groverPb.DataProtocol_DATA_PROTOCOL_TCP,
		DataEndpoint: &groverPb.DataEndpoint{Host: "127.0.0.1", Port: 30000},
		RootPath:     "/" + root,
	}
}

func serviceNames(services map[string]grpc.ServiceInfo) []string {
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	return names
}
