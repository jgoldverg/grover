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
	if !strings.Contains(got, "download\taborted\trelay-a") {
		t.Fatalf("list output missing aborted route:\n%s", got)
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
	got := out.String()
	for _, want := range []string{
		"job[job-1]: state=RUNTIME_STATE_RUNNING protocol=DATA_PROTOCOL_TCP good_bytes=10 network_bytes=12 files_done=1 files_active=2 throughput_bps=99 errors=1",
		"relay[relay-a:22444] forward[forward-1]: hop=1 state=RUNTIME_STATE_RUNNING protocol=DATA_PROTOCOL_UDP ingress_bytes=100 egress_bytes=80 packets=3 throughput_bps=77 errors=2",
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
