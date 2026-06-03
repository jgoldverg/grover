package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
)

func TestParseTransferRelaysSupportsRepeatedAndCommaSeparatedValues(t *testing.T) {
	relays, err := parseTransferRelays([]string{"relay-a, relay-b", "10.0.0.5:22444"})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(relays))
	for _, relay := range relays {
		got = append(got, relay.Raw)
	}
	want := []string{"relay-a", "relay-b", "10.0.0.5:22444"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("relays = %v, want %v", got, want)
	}
	for _, relay := range relays {
		if relay.ControlEndpoint != relay.Raw {
			t.Fatalf("relay control endpoint = %q, want %q", relay.ControlEndpoint, relay.Raw)
		}
		if relay.DataEndpoint != "allocated-by-relay" {
			t.Fatalf("relay data endpoint = %q, want allocated-by-relay", relay.DataEndpoint)
		}
	}
}

func TestParseTransferRelaysRejectsPathLikeRelay(t *testing.T) {
	if _, err := parseTransferRelays([]string{"relay-a/data"}); err == nil {
		t.Fatal("expected error for path-like relay hop")
	}
}

func TestBuildTransferRoutePlanDirect(t *testing.T) {
	src, err := parseLocation("10.0.0.1:22444:/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	dst, err := parseLocation("10.0.0.2:22444:/data/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := buildTransferRoutePlan(src, dst, CopyOptions{Protocol: "tcp", ParallelStreams: 4, Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != "direct" {
		t.Fatalf("mode = %q, want direct", plan.Mode)
	}
	if plan.Direction != "remote-to-remote" {
		t.Fatalf("direction = %q, want remote-to-remote", plan.Direction)
	}
	if len(plan.Relays) != 0 {
		t.Fatalf("relays = %v, want none", plan.Relays)
	}
	if plan.Protocol != "tcp" || plan.ParallelStreams != 4 || plan.Concurrency != 2 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	if plan.ConnectionOrigin != "source" || plan.DataDirection != "source-to-destination" {
		t.Fatalf("unexpected session metadata: %+v", plan)
	}
}

func TestBuildTransferRoutePlanSessionMetadata(t *testing.T) {
	src, err := parseLocation("10.0.0.1:22444:/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	dst, err := parseLocation("10.0.0.2:22444:/data/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := buildTransferRoutePlan(src, dst, CopyOptions{
		ConnectionOrigin: "destination",
		DataDirection:    "destination_to_source",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.ConnectionOrigin != "destination" || plan.DataDirection != "destination-to-source" {
		t.Fatalf("unexpected plan metadata: %+v", plan)
	}
}

func TestBuildTransferRoutePlanBridge(t *testing.T) {
	src, err := parseLocation("10.0.0.1:22444:/data/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	dst, err := parseLocation("10.0.0.2:22444:/data/")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := buildTransferRoutePlan(src, dst, CopyOptions{Via: []string{"relay-a,relay-b"}})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != "bridge" {
		t.Fatalf("mode = %q, want bridge", plan.Mode)
	}
	if plan.Direction != "remote-to-remote" {
		t.Fatalf("direction = %q, want remote-to-remote", plan.Direction)
	}
	if got := len(plan.Relays); got != 2 {
		t.Fatalf("relay count = %d, want 2", got)
	}
	if got := len(plan.Hops); got != 4 {
		t.Fatalf("hop count = %d, want 4", got)
	}
	if plan.Hops[0].Role != "source" || plan.Hops[1].Role != "relay" || plan.Hops[3].Role != "destination" {
		t.Fatalf("unexpected hop roles: %+v", plan.Hops)
	}
	if plan.Hops[1].ControlEndpoint != "relay-a" || plan.Hops[1].DataEndpoint != "allocated-by-relay" {
		t.Fatalf("unexpected relay endpoints: %+v", plan.Hops[1])
	}
	if plan.Hops[3].DataEndpoint != "allocated-by-destination" {
		t.Fatalf("unexpected destination endpoint allocation: %+v", plan.Hops[3])
	}
	if plan.Protocol != "config" {
		t.Fatalf("protocol = %q, want config", plan.Protocol)
	}
}

func TestPrintTransferRoutePlan(t *testing.T) {
	src, err := parseLocation("10.0.0.1:22444:/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	dst, err := parseLocation("10.0.0.2:22444:/data/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := buildTransferRoutePlan(src, dst, CopyOptions{Protocol: "udp", ParallelStreams: 4, Via: []string{"relay-a", "relay-b"}})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	printTransferRoutePlan(&out, plan)
	got := out.String()
	for _, want := range []string{
		"route: 10.0.0.1:22444 -> relay-a -> relay-b -> 10.0.0.2:22444",
		"mode: bridge",
		"direction: remote-to-remote",
		"connection_origin: source",
		"data_direction: source-to-destination",
		"protocol: udp",
		"parallel_streams: 4",
		"concurrency: 1",
		"hop[0]: role=source endpoint=10.0.0.1:22444 control_endpoint=10.0.0.1:22444 data_endpoint=source",
		"hop[1]: role=relay endpoint=relay-a control_endpoint=relay-a data_endpoint=allocated-by-relay",
		"hop[3]: role=destination endpoint=10.0.0.2:22444 control_endpoint=10.0.0.2:22444 data_endpoint=allocated-by-destination",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestParseLocationSupportsGroverdEndpointPaths(t *testing.T) {
	ref, err := parseLocation("10.0.0.1:22444:/mnt/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	if !ref.isRemote || ref.ControlEndpoint != "10.0.0.1:22444" || ref.Path != "/mnt/file.bin" {
		t.Fatalf("unexpected endpoint ref: %+v", ref)
	}
	ref, err = parseLocation("[::1]:22444:/mnt/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	if ref.ControlEndpoint != "[::1]:22444" || ref.Path != "/mnt/file.bin" {
		t.Fatalf("unexpected ipv6 endpoint ref: %+v", ref)
	}
}

func TestParseLocationRejectsEmptyAndMalformedGroverdEndpoint(t *testing.T) {
	if _, err := parseLocation(" "); err == nil {
		t.Fatal("expected error for empty location")
	}
	if _, err := parseLocation("10.0.0.1:/mnt/file.bin"); err == nil {
		t.Fatal("expected error for missing endpoint port")
	}
	if _, err := parseLocation("[::1:/mnt/file.bin"); err == nil {
		t.Fatal("expected error for malformed ipv6 endpoint")
	}
}

func TestTransferDryRunMergesPreparedRoute(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "routes.toml")
	prepare := RouteCommand()
	prepare.SetArgs([]string{"--route-store", storePath, "prepare", "relay-a-b", "--via", "relay-a", "--via", "relay-b", "--protocol", "tcp"})
	if err := prepare.Execute(); err != nil {
		t.Fatal(err)
	}

	cmd := SimpleCopy()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--route-store", storePath,
		"--route", "relay-a-b",
		"--dry-run",
		"10.0.0.1:22444:/src.bin",
		"10.0.0.2:22444:/dst.bin",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"route: 10.0.0.1:22444 -> relay-a -> relay-b -> 10.0.0.2:22444",
		"protocol: tcp",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestTransferRouteUsesStoredDefaultEndpoints(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "routes.toml")
	prepare := RouteCommand()
	prepare.SetArgs([]string{
		"--route-store", storePath,
		"prepare", "defaults",
		"10.0.0.1:22444:/src.bin",
		"10.0.0.2:22444:/dst.bin",
		"--via", "relay-a",
		"--protocol", "tcp",
	})
	if err := prepare.Execute(); err != nil {
		t.Fatal(err)
	}

	cmd := SimpleCopy()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--route-store", storePath, "--route", "defaults", "--dry-run"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "route: 10.0.0.1:22444 -> relay-a -> 10.0.0.2:22444") {
		t.Fatalf("output missing stored default route:\n%s", got)
	}
}

func TestTransferRouteWithoutArgsRequiresDefaults(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "routes.toml")
	prepare := RouteCommand()
	prepare.SetArgs([]string{"--route-store", storePath, "prepare", "relay-only", "--via", "relay-a"})
	if err := prepare.Execute(); err != nil {
		t.Fatal(err)
	}

	cmd := SimpleCopy()
	cmd.SetArgs([]string{"--route-store", storePath, "--route", "relay-only", "--dry-run"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "has no source/destination defaults") {
		t.Fatalf("transfer --route error = %v, want defaults guidance", err)
	}
}

func TestRouteEndpointLocationMapsAbsolutePathToConfiguredControlEndpoint(t *testing.T) {
	got, err := routeEndpointLocation("10.0.0.10:22444", "/data/src", "source")
	if err != nil {
		t.Fatal(err)
	}
	if got != "10.0.0.10:22444:/data/src" {
		t.Fatalf("endpoint location = %q", got)
	}

	remote, err := routeEndpointLocation("10.0.0.10:22444", "10.0.0.20:22444:/already/remote", "source")
	if err != nil {
		t.Fatal(err)
	}
	if remote != "10.0.0.20:22444:/already/remote" {
		t.Fatalf("remote endpoint should pass through, got %q", remote)
	}

	if _, err := routeEndpointLocation("10.0.0.10:22444", "relative/path", "source"); err == nil {
		t.Fatal("expected relative route path to be rejected")
	}
}

func TestTransferDryRunResolvesCredentialEndpoint(t *testing.T) {
	credPath := filepath.Join(t.TempDir(), "credentials.toml")
	store, err := backend.NewTomlCredentialStorage(credPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddCredential(&backend.BasicAuthCredential{
		Name: "dst",
		URL:  "10.0.0.20:22444",
		UUID: uuid.New(),
	}); err != nil {
		t.Fatal(err)
	}
	cmd := SimpleCopy()
	cmd.SetContext(context.WithValue(context.Background(), appCtxKey, &internal.AppConfig{CredentialsFile: credPath}))
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"10.0.0.10:22444:/src.bin", "dst:/dst.bin", "--protocol", "tcp", "--dry-run"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "route: 10.0.0.10:22444 -> 10.0.0.20:22444") {
		t.Fatalf("credential endpoint not resolved in route:\n%s", got)
	}
}
