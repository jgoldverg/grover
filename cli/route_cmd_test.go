package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func TestRoutePrepareStoresTemplateAndStatusPrintsPlan(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "routes.toml")

	prepare := RouteCommand()
	var prepareOut bytes.Buffer
	prepare.SetOut(&prepareOut)
	prepare.SetErr(&prepareOut)
	prepare.SetArgs([]string{
		"--route-store", storePath,
		"prepare", "daily-upload",
		"--via", "relay-a,relay-b",
		"--protocol", "tcp",
	})
	if err := prepare.Execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(storePath); err != nil {
		t.Fatal(err)
	}
	if got := prepareOut.String(); !strings.Contains(got, "prepared route daily-upload") {
		t.Fatalf("prepare output missing route name:\n%s", got)
	}

	status := RouteCommand()
	var statusOut bytes.Buffer
	status.SetOut(&statusOut)
	status.SetErr(&statusOut)
	status.SetArgs([]string{"--route-store", storePath, "status", "daily-upload"})
	if err := status.Execute(); err != nil {
		t.Fatal(err)
	}
	got := statusOut.String()
	for _, want := range []string{
		"route_id: daily-upload",
		"state: prepared",
		"relays: relay-a -> relay-b",
		"protocol: tcp",
		"connection_origin: source",
		"data_direction: source-to-destination",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("status output missing %q:\n%s", want, got)
		}
	}
}

func TestRouteListAndAbort(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "routes.toml")

	cmd := RouteCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--route-store", storePath, "prepare", "download", "--via", "relay-a"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}

	abort := RouteCommand()
	var abortOut bytes.Buffer
	abort.SetOut(&abortOut)
	abort.SetErr(&abortOut)
	abort.SetArgs([]string{"--route-store", storePath, "abort", "download"})
	if err := abort.Execute(); err != nil {
		t.Fatal(err)
	}

	list := RouteCommand()
	var listOut bytes.Buffer
	list.SetOut(&listOut)
	list.SetErr(&listOut)
	list.SetArgs([]string{"--route-store", storePath, "list"})
	if err := list.Execute(); err != nil {
		t.Fatal(err)
	}
	got := listOut.String()
	for _, want := range []string{"ROUTE", "STATE", "RELAYS", "download", "aborted", "relay-a"} {
		if !strings.Contains(got, want) {
			t.Fatalf("list output missing %q:\n%s", want, got)
		}
	}
}

func TestRoutePrepareStoresFriendlyDirectionFlags(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "routes.toml")
	cmd := RouteCommand()
	cmd.SetArgs([]string{
		"--route-store", storePath,
		"prepare", "friendly",
		"--connect-from", "dst",
		"--flow", "reverse",
		"--protocol", "tcp",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	store, err := newRouteTemplateStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	route, err := store.get("friendly")
	if err != nil {
		t.Fatal(err)
	}
	if route.ConnectionOrigin != "destination" || route.DataDirection != "destination-to-source" {
		t.Fatalf("friendly session metadata not stored: %+v", route)
	}
}

func TestPrintServerRouteTable(t *testing.T) {
	var out bytes.Buffer
	err := printServerRouteTable(&out, []*pb.RouteConfig{{
		Name:             "tacc-uc",
		Source:           "129.114.108.86:22444",
		Destination:      "192.5.86.187:22444",
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_TCP,
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE,
		DataDirection:    pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION,
	}})
	if err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"ROUTE", "SOURCE", "RELAYS", "DESTINATION", "tacc-uc", "129.114.108.86:22444", "(direct)", "192.5.86.187:22444"} {
		if !strings.Contains(got, want) {
			t.Fatalf("server route table missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "\ttacc-uc\t") {
		t.Fatalf("server route table should not be raw tab-separated output:\n%s", got)
	}
}

func TestRoutePrepareCanStoreOptionalDefaultEndpoints(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "routes.toml")
	cmd := RouteCommand()
	cmd.SetArgs([]string{"--route-store", storePath, "prepare", "defaults", "10.0.0.1:22444:/src", "10.0.0.2:22444:/dst", "--via", "10.0.0.3:22444"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	store, err := newRouteTemplateStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	route, err := store.get("defaults")
	if err != nil {
		t.Fatal(err)
	}
	if route.Source != "10.0.0.1:22444:/src" || route.Destination != "10.0.0.2:22444:/dst" {
		t.Fatalf("defaults not stored: %+v", route)
	}
}

func TestStoredRouteTemplateFromServerRouteUsesEndpointPaths(t *testing.T) {
	route := &pb.RouteConfig{
		Name:             "uc-to-edu",
		Source:           "10.137.1.2:22444",
		Destination:      "10.137.132.2:22444",
		Via:              []string{"10.133.3.2:22444"},
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_TCP,
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE,
		DataDirection:    pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION,
	}
	tmpl, err := storedRouteTemplateFromServerRoute(route, "/src", "/dst")
	if err != nil {
		t.Fatal(err)
	}
	if tmpl.Source != "10.137.1.2:22444:/src" || tmpl.Destination != "10.137.132.2:22444:/dst" {
		t.Fatalf("template endpoints = %+v", tmpl)
	}
	if len(tmpl.Via) != 1 || tmpl.Via[0] != "10.133.3.2:22444" {
		t.Fatalf("template relays = %+v", tmpl.Via)
	}
}

func TestRoutePrepareStoresSessionDirectionMetadata(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "routes.toml")
	cmd := RouteCommand()
	cmd.SetArgs([]string{
		"--route-store", storePath,
		"prepare", "edu-download",
		"--connection-origin", "destination",
		"--data-direction", "source-to-destination",
		"--protocol", "tcp",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	store, err := newRouteTemplateStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	route, err := store.get("edu-download")
	if err != nil {
		t.Fatal(err)
	}
	if route.ConnectionOrigin != "destination" || route.DataDirection != "source-to-destination" {
		t.Fatalf("session metadata not stored: %+v", route)
	}
}

func TestRoutePrepareRejectsInvalidSessionDirectionMetadata(t *testing.T) {
	cmd := RouteCommand()
	cmd.SetArgs([]string{"prepare", "bad", "--connection-origin", "relay"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "invalid --connect-from") {
		t.Fatalf("route prepare error = %v, want connection origin validation", err)
	}
}

func TestRoutePrepareRejectsPartialDefaultEndpoints(t *testing.T) {
	cmd := RouteCommand()
	cmd.SetArgs([]string{"prepare", "broken", "/src-only"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "accepts <name> or <name> <source> <destination>") {
		t.Fatalf("route prepare error = %v, want arity guidance", err)
	}
}

func TestRouteStartDirectsUsersToTransfer(t *testing.T) {
	cmd := RouteCommand()
	cmd.SetArgs([]string{"start", "anything"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "use transfer --route") {
		t.Fatalf("route start error = %v, want transfer guidance", err)
	}
}

func TestPrintRouteJobsAndForwards(t *testing.T) {
	var out bytes.Buffer
	printRouteJobs(&out, []*pb.TransferJob{{
		JobId:        "job-1",
		State:        pb.RuntimeState_RUNTIME_STATE_RUNNING,
		Protocol:     pb.DataProtocol_DATA_PROTOCOL_TCP,
		GoodBytes:    10,
		NetworkBytes: 12,
		FilesDone:    1,
		FilesActive:  2,
		Stats:        &pb.StatsSnapshot{CurrentThroughputBps: 99, Errors: 1},
	}})
	printRouteForwards(&out, "relay-a:22444", []*pb.ForwardSession{{
		ForwardId: "forward-1",
		HopIndex:  1,
		State:     pb.RuntimeState_RUNTIME_STATE_RUNNING,
		Protocol:  pb.DataProtocol_DATA_PROTOCOL_UDP,
		Stats:     &pb.StatsSnapshot{IngressBytes: 100, EgressBytes: 80, Packets: 3, CurrentThroughputBps: 77, Errors: 2},
	}})
	printRouteSessions(&out, []*pb.RouteSession{{
		SessionId:        "session-1",
		State:            pb.RuntimeState_RUNTIME_STATE_READY,
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_TCP,
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION,
		DataDirection:    pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION,
		Source: &pb.TransferEndpoint{
			DataEndpoint: &pb.DataEndpoint{Host: "10.0.0.1", Port: 30000},
			RootPath:     "/src",
		},
		Destination: &pb.TransferEndpoint{
			DataEndpoint: &pb.DataEndpoint{Host: "10.0.0.2", Port: 30100},
			RootPath:     "/dst",
		},
		Hops: []*pb.RouteSessionHop{{
			HopIndex:        1,
			ControlEndpoint: "relay-a:22444",
			Ingress:         &pb.DataEndpoint{Host: "10.0.0.3", Port: 30200},
			Egress:          &pb.DataEndpoint{Host: "10.0.0.2", Port: 30100},
			State:           pb.RuntimeState_RUNTIME_STATE_RUNNING,
			Stats:           &pb.StatsSnapshot{CurrentThroughputBps: 44, Errors: 5, Drops: 6},
		}},
		Stats: &pb.StatsSnapshot{CurrentThroughputBps: 11, Errors: 3},
	}})
	got := out.String()
	for _, want := range []string{
		"job[job-1]: state=RUNTIME_STATE_RUNNING protocol=DATA_PROTOCOL_TCP good_bytes=10 network_bytes=12 files_done=1 files_active=2 throughput_bps=99 errors=1",
		"relay[relay-a:22444] forward[forward-1]: hop=1 state=RUNTIME_STATE_RUNNING protocol=DATA_PROTOCOL_UDP ingress_bytes=100 egress_bytes=80 packets=3 throughput_bps=77 errors=2",
		"session[session-1]: state=RUNTIME_STATE_READY protocol=DATA_PROTOCOL_TCP origin=CONNECTION_ORIGIN_DESTINATION direction=DATA_DIRECTION_SOURCE_TO_DESTINATION hops=1 throughput_bps=11 errors=3",
		"  source: 10.0.0.1:30000 root=/src",
		"  hop[1] relay-a:22444: 10.0.0.3:30200 -> 10.0.0.2:30100 state=RUNTIME_STATE_RUNNING throughput_bps=44 errors=5 drops=6",
		"  destination: 10.0.0.2:30100 root=/dst",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestRuntimeStateActive(t *testing.T) {
	for _, state := range []pb.RuntimeState{
		pb.RuntimeState_RUNTIME_STATE_PREPARING,
		pb.RuntimeState_RUNTIME_STATE_READY,
		pb.RuntimeState_RUNTIME_STATE_RUNNING,
	} {
		if !runtimeStateActive(state) {
			t.Fatalf("%s should be active", state)
		}
	}
	for _, state := range []pb.RuntimeState{
		pb.RuntimeState_RUNTIME_STATE_DONE,
		pb.RuntimeState_RUNTIME_STATE_ABORTED,
		pb.RuntimeState_RUNTIME_STATE_FAILED,
		pb.RuntimeState_RUNTIME_STATE_EXPIRED,
	} {
		if runtimeStateActive(state) {
			t.Fatalf("%s should not be active", state)
		}
	}
}

func TestDataEndpointLabel(t *testing.T) {
	cases := []struct {
		name     string
		endpoint *pb.DataEndpoint
		want     string
	}{
		{name: "nil", want: "(none)"},
		{name: "empty host", endpoint: &pb.DataEndpoint{Port: 30000}, want: "(none)"},
		{name: "zero port", endpoint: &pb.DataEndpoint{Host: "10.0.0.1"}, want: "(none)"},
		{name: "ipv4", endpoint: &pb.DataEndpoint{Host: "10.0.0.1", Port: 30000}, want: "10.0.0.1:30000"},
		{name: "ipv6", endpoint: &pb.DataEndpoint{Host: "2001:db8::1", Port: 30000}, want: "[2001:db8::1]:30000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dataEndpointLabel(tc.endpoint); got != tc.want {
				t.Fatalf("label = %q, want %q", got, tc.want)
			}
		})
	}
}
