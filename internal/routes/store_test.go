package routes

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jgoldverg/grover/internal/pb"
)

func TestPutGetDeletePersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Put(&pb.Route{Name: "uc-tacc", Source: "uc:1", Destination: "tacc:1", Via: []string{" relay:1 ", ""}, Protocol: pb.DataProtocol_DATA_PROTOCOL_UDP})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Via) != 1 || r.Via[0] != "relay:1" || r.ConnectFrom != pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE {
		t.Fatalf("normalized: %v", r)
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), `"protocol": "udp"`) {
		t.Fatalf("json should use readable enum names:\n%s", data)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Get("uc-tacc")
	if err != nil || got.Protocol != pb.DataProtocol_DATA_PROTOCOL_UDP || got.Destination != "tacc:1" {
		t.Fatalf("reloaded: %v %v", got, err)
	}
	if err := reopened.Delete("uc-tacc"); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Get("uc-tacc"); !errors.Is(err, ErrNotFound) {
		t.Fatal("expected not found")
	}
}

func TestLoadsLegacyFileWithDataDirection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.json")
	legacy := `{"routes":[{"name":"edu","source":"a:1","destination":"b:1","protocol":"tcp","connection_origin":"destination","data_direction":"source-to-destination","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}]}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Get("edu")
	if err != nil || r.ConnectFrom != pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
		t.Fatalf("legacy route: %v %v", r, err)
	}
}

func TestNormalizeRejects(t *testing.T) {
	for name, r := range map[string]*pb.Route{
		"no name":        {Source: "a", Destination: "b"},
		"slash in name":  {Name: "a/b", Source: "a", Destination: "b"},
		"udp dest dials": {Name: "x", Source: "a", Destination: "b", Protocol: pb.DataProtocol_DATA_PROTOCOL_UDP, ConnectFrom: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION},
		"path relay":     {Name: "x", Source: "a", Destination: "b", Via: []string{"/tmp"}},
	} {
		if _, err := Normalize(r); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
