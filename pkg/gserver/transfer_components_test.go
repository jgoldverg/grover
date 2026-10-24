package gserver

import (
	"context"
	"testing"
	"time"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

type plannerFunc func(context.Context, *pb.TransferEndpoint, []string) ([]TransferFilePlan, error)

func (f plannerFunc) PlanFiles(ctx context.Context, source *pb.TransferEndpoint, paths []string) ([]TransferFilePlan, error) {
	return f(ctx, source, paths)
}

type executorFunc func(context.Context, *TransferExecutionContext, TransferFilePlan) error

func (f executorFunc) TransferFile(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan) error {
	return f(ctx, exec, plan)
}

func TestManagerComposesIndependentPlannerAndExecutor(t *testing.T) {
	plan := TransferFilePlan{SourcePath: "custom://source", RelativePath: "payload", Size: 123}
	seen := make(chan TransferFilePlan, 1)
	planner := plannerFunc(func(context.Context, *pb.TransferEndpoint, []string) ([]TransferFilePlan, error) {
		return []TransferFilePlan{plan}, nil
	})
	executor := executorFunc(func(_ context.Context, exec *TransferExecutionContext, p TransferFilePlan) error {
		seen <- p
		exec.OnProgress(p.RelativePath, int(p.Size))
		return nil
	})
	m := NewTransferJobManagerWithComponents(nil, planner, executor)
	defer m.Close()
	_, err := m.StartJob(context.Background(), &pb.StartTransferJobRequest{JobId: "components",
		Source: &pb.TransferEndpoint{RootPath: "/virtual/source"}, Destination: &pb.TransferEndpoint{RootPath: "/virtual/destination"}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-seen:
		if got != plan {
			t.Fatalf("plan changed: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("executor not called")
	}
	job := waitForTransferJobState(t, m, "components", pb.RuntimeState_RUNTIME_STATE_DONE)
	if job.GetFilesDone() != 1 || job.GetGoodBytes() != 123 {
		t.Fatalf("unexpected progress: %+v", job)
	}
}

func TestProtocolExecutorDispatch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		protocol pb.DataProtocol
		data     *pb.DataEndpoint
	}{
		{"local", pb.DataProtocol_DATA_PROTOCOL_TCP, nil},
		{"tcp", pb.DataProtocol_DATA_PROTOCOL_TCP, &pb.DataEndpoint{Host: "host", Port: 123}},
		{"udp", pb.DataProtocol_DATA_PROTOCOL_UDP, &pb.DataEndpoint{Host: "host", Port: 123}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := ""
			makeExecutor := func(name string) executorFunc {
				return func(context.Context, *TransferExecutionContext, TransferFilePlan) error { called = name; return nil }
			}
			e := ProtocolTransferExecutor{Local: makeExecutor("local"), TCP: makeExecutor("tcp"), UDP: makeExecutor("udp")}
			if err := e.TransferFile(context.Background(), &TransferExecutionContext{Protocol: tc.protocol, DestData: tc.data}, TransferFilePlan{}); err != nil {
				t.Fatal(err)
			}
			if called != tc.name {
				t.Fatalf("called %s, want %s", called, tc.name)
			}
		})
	}
}
