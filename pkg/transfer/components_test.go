package transfer_test

import (
	"context"
	"testing"
	"time"

	"github.com/jgoldverg/grover/pkg/transfer"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func waitForTransferJobState(t *testing.T, manager *transfer.Manager, jobID string, state pb.RuntimeState) *pb.TransferJob {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		job, err := manager.GetJob(jobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.GetState() == state {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s state = %s, want %s", jobID, job.GetState(), state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type plannerFunc func(context.Context, *pb.TransferEndpoint, []string) ([]transfer.FilePlan, error)

func (f plannerFunc) PlanFiles(ctx context.Context, source *pb.TransferEndpoint, paths []string) ([]transfer.FilePlan, error) {
	return f(ctx, source, paths)
}

type executorFunc func(context.Context, *transfer.Execution, transfer.FilePlan) error

func (f executorFunc) TransferFile(ctx context.Context, exec *transfer.Execution, plan transfer.FilePlan) error {
	return f(ctx, exec, plan)
}

func TestManagerComposesIndependentPlannerAndExecutor(t *testing.T) {
	plan := transfer.FilePlan{SourcePath: "custom://source", RelativePath: "payload", Size: 123}
	seen := make(chan transfer.FilePlan, 1)
	planner := plannerFunc(func(context.Context, *pb.TransferEndpoint, []string) ([]transfer.FilePlan, error) {
		return []transfer.FilePlan{plan}, nil
	})
	executor := executorFunc(func(_ context.Context, exec *transfer.Execution, p transfer.FilePlan) error {
		seen <- p
		exec.Progress.OnProgress(p.RelativePath, int(p.Size))
		return nil
	})
	m := transfer.NewManagerWithComponents(nil, planner, executor)
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
				return func(context.Context, *transfer.Execution, transfer.FilePlan) error { called = name; return nil }
			}
			e := transfer.ProtocolExecutor{Local: makeExecutor("local"), TCP: makeExecutor("tcp"), UDP: makeExecutor("udp")}
			if err := e.TransferFile(context.Background(), &transfer.Execution{Target: transfer.TargetConfig{Protocol: tc.protocol,
				Data: tc.data}}, transfer.FilePlan{}); err != nil {
				t.Fatal(err)
			}
			if called != tc.name {
				t.Fatalf("called %s, want %s", called, tc.name)
			}
		})
	}
}
