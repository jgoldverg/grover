package gserver

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestJSONRouteStoreLifecycleUsesReadableJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.json")
	store, err := NewJSONRouteStore(&internal.ServerConfig{RouteStoreFile: path})
	if err != nil {
		t.Fatal(err)
	}

	route, err := store.Put(context.Background(), RouteConfig{
		Name:             "uc-to-edu",
		Source:           "10.0.0.10:22444",
		Destination:      "10.0.0.20:22444",
		Via:              []string{"relay-a:22444"},
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_UDP,
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION,
		DataDirection:    pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION,
	})
	if err != nil {
		t.Fatal(err)
	}
	if route.CreatedAt.IsZero() || route.UpdatedAt.IsZero() {
		t.Fatalf("timestamps were not set: %+v", route)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"protocol": "udp"`,
		`"connection_origin": "destination"`,
		`"data_direction": "source_to_destination"`,
	} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("route json missing %s:\n%s", want, string(data))
		}
	}

	var raw struct {
		Routes []RouteConfig `json:"routes"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if len(raw.Routes) != 1 || raw.Routes[0].Protocol != pb.DataProtocol_DATA_PROTOCOL_UDP {
		t.Fatalf("decoded routes = %+v", raw.Routes)
	}

	reloaded, err := NewJSONRouteStore(&internal.ServerConfig{RouteStoreFile: path})
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := reloaded.Get(context.Background(), "uc-to-edu")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got.ConnectionOrigin != pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
		t.Fatalf("reloaded route = %+v ok=%v", got, ok)
	}

	deleted, err := reloaded.Delete(context.Background(), "uc-to-edu")
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("expected route delete to report true")
	}
}

func TestJSONRouteStoreValidation(t *testing.T) {
	store, err := NewJSONRouteStore(&internal.ServerConfig{RouteStoreFile: filepath.Join(t.TempDir(), "routes.json")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Put(context.Background(), RouteConfig{
		Name:        "bad route",
		Source:      "src:22444",
		Destination: "dst:22444",
	})
	if err == nil || !strings.Contains(err.Error(), "must not contain whitespace") {
		t.Fatalf("Put error = %v, want route name validation", err)
	}
	_, err = store.Put(context.Background(), RouteConfig{
		Name:        "bad-hop",
		Source:      "src:22444",
		Destination: "dst:22444",
		Via:         []string{"relay:/tmp"},
	})
	if err == nil || !strings.Contains(err.Error(), "not paths") {
		t.Fatalf("Put error = %v, want relay validation", err)
	}
}

func TestRouteConfigControlLifecycle(t *testing.T) {
	service, err := NewRouteConfigControlService(&internal.ServerConfig{RouteStoreFile: filepath.Join(t.TempDir(), "routes.json")})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	created, err := service.PutRoute(ctx, &pb.PutRouteRequest{Route: &pb.RouteConfig{
		Name:             "direct",
		Source:           "src:22444",
		Destination:      "dst:22444",
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_TCP,
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE,
		DataDirection:    pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetRoute().GetName() != "direct" || created.GetRoute().GetCreatedAtUnixNano() == 0 {
		t.Fatalf("created route = %+v", created.GetRoute())
	}

	fetched, err := service.GetRoute(ctx, &pb.GetRouteRequest{Name: "direct"})
	if err != nil {
		t.Fatal(err)
	}
	if fetched.GetRoute().GetSource() != "src:22444" {
		t.Fatalf("fetched route = %+v", fetched.GetRoute())
	}
	listed, err := service.ListRoutes(ctx, &pb.ListRoutesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetRoutes()) != 1 {
		t.Fatalf("listed routes = %+v", listed.GetRoutes())
	}
	deleted, err := service.DeleteRoute(ctx, &pb.DeleteRouteRequest{Name: "direct"})
	if err != nil {
		t.Fatal(err)
	}
	if !deleted.GetOk() {
		t.Fatal("DeleteRoute ok=false")
	}
	if _, err := service.GetRoute(ctx, &pb.GetRouteRequest{Name: "direct"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetRoute error = %v, want not found", err)
	}
}

func TestRouteConfigControlValidation(t *testing.T) {
	service, err := NewRouteConfigControlService(&internal.ServerConfig{RouteStoreFile: filepath.Join(t.TempDir(), "routes.json")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.PutRoute(context.Background(), &pb.PutRouteRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("PutRoute error = %v, want invalid argument", err)
	}
}
