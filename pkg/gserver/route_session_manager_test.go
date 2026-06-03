package gserver

import (
	"testing"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func TestRouteSessionManagerLifecycle(t *testing.T) {
	manager := NewRouteSessionManager()

	session, err := manager.Create(CreateRouteSessionRequest{
		SessionID:        "session-a",
		RouteID:          "route-a",
		JobID:            "job-a",
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_TCP,
		ConnectionOrigin: ConnectionOriginSource,
		DataDirection:    DataDirectionSourceToDestination,
		Source:           routeSessionEndpoint("src", pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE),
		Destination:      routeSessionEndpoint("dst", pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION),
		Hops: []RouteSessionHop{{
			HopIndex: 1,
			NodeID:   "relay-a",
			Control:  "10.0.0.3:22444",
			Ingress:  &pb.DataEndpoint{Host: "10.0.0.3", Port: 30000},
			Egress:   &pb.DataEndpoint{Host: "10.0.0.2", Port: 30100},
			State:    pb.RuntimeState_RUNTIME_STATE_READY,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if session.State != pb.RuntimeState_RUNTIME_STATE_READY {
		t.Fatalf("state = %s, want READY", session.State)
	}
	if session.ConnectionOrigin != ConnectionOriginSource {
		t.Fatalf("origin = %q, want source", session.ConnectionOrigin)
	}
	if len(session.Hops) != 1 || session.Hops[0].NodeID != "relay-a" {
		t.Fatalf("unexpected hops: %+v", session.Hops)
	}

	got, ok := manager.Get("session-a")
	if !ok {
		t.Fatal("expected session lookup")
	}
	got.Source.RootPath = "/mutated"
	got.Hops[0].Ingress.Host = "mutated"
	again, ok := manager.Get("session-a")
	if !ok {
		t.Fatal("expected session lookup after mutation")
	}
	if again.Source.GetRootPath() == "/mutated" || again.Hops[0].Ingress.GetHost() == "mutated" {
		t.Fatal("session snapshots must be defensive clones")
	}

	running, err := manager.MarkRunning("session-a")
	if err != nil {
		t.Fatal(err)
	}
	if running.State != pb.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("state = %s, want RUNNING", running.State)
	}
	failed, err := manager.MarkFailed("session-a", "dial failed")
	if err != nil {
		t.Fatal(err)
	}
	if failed.State != pb.RuntimeState_RUNTIME_STATE_FAILED || failed.Error != "dial failed" {
		t.Fatalf("unexpected failed session: %+v", failed)
	}
	if !manager.Delete("session-a") {
		t.Fatal("expected delete to return true")
	}
	if _, ok := manager.Get("session-a"); ok {
		t.Fatal("expected deleted session to be absent")
	}
}

func TestRouteSessionManagerDestinationOriginDownloadModel(t *testing.T) {
	manager := NewRouteSessionManager()

	session, err := manager.Create(CreateRouteSessionRequest{
		RouteID:          "edu-download",
		JobID:            "schedule-job",
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_UDP,
		ConnectionOrigin: ConnectionOriginDestination,
		DataDirection:    DataDirectionSourceToDestination,
		Source:           routeSessionEndpoint("uc", pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE),
		Destination:      routeSessionEndpoint("edu", pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION),
	})
	if err != nil {
		t.Fatal(err)
	}
	if session.SessionID == "" {
		t.Fatal("expected generated session id")
	}
	if session.ConnectionOrigin != ConnectionOriginDestination {
		t.Fatalf("origin = %q, want destination", session.ConnectionOrigin)
	}
	if session.DataDirection != DataDirectionSourceToDestination {
		t.Fatalf("direction = %q, want source_to_destination", session.DataDirection)
	}

	listed := manager.List("edu-download", "schedule-job")
	if len(listed) != 1 || listed[0].SessionID != session.SessionID {
		t.Fatalf("unexpected list result: %+v", listed)
	}
}

func TestRouteSessionManagerValidation(t *testing.T) {
	manager := NewRouteSessionManager()
	valid := CreateRouteSessionRequest{
		RouteID:          "route",
		ConnectionOrigin: ConnectionOriginSource,
		DataDirection:    DataDirectionSourceToDestination,
		Source:           routeSessionEndpoint("src", pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE),
		Destination:      routeSessionEndpoint("dst", pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION),
	}
	cases := []struct {
		name string
		mut  func(*CreateRouteSessionRequest)
	}{
		{name: "nil source", mut: func(req *CreateRouteSessionRequest) { req.Source = nil }},
		{name: "nil destination", mut: func(req *CreateRouteSessionRequest) { req.Destination = nil }},
		{name: "missing route", mut: func(req *CreateRouteSessionRequest) { req.RouteID = "" }},
		{name: "bad origin", mut: func(req *CreateRouteSessionRequest) { req.ConnectionOrigin = "relay" }},
		{name: "bad direction", mut: func(req *CreateRouteSessionRequest) { req.DataDirection = "sideways" }},
		{name: "bad protocol", mut: func(req *CreateRouteSessionRequest) { req.Protocol = pb.DataProtocol(99) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			tc.mut(&req)
			if _, err := manager.Create(req); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func routeSessionEndpoint(root string, role pb.TransferEndpointRole) *pb.TransferEndpoint {
	return &pb.TransferEndpoint{
		EndpointId:   root + "-endpoint",
		RouteId:      "route",
		JobId:        "job",
		Role:         role,
		Protocol:     pb.DataProtocol_DATA_PROTOCOL_TCP,
		DataEndpoint: &pb.DataEndpoint{Host: "127.0.0.1", Port: 30000},
		RootPath:     "/" + root,
	}
}
