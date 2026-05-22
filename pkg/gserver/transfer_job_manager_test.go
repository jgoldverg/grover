package gserver

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func TestTransferJobManagerLocalFilesystemLifecycle(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("alpha"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(src, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "nested", "b.txt"), []byte("beta"), 0o600); err != nil {
		t.Fatal(err)
	}

	manager := NewTransferJobManager(nil, nil)
	source, err := manager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "route-1",
		JobId:    "job-1",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: src,
	})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := manager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "route-1",
		JobId:    "job-1",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: dst,
	})
	if err != nil {
		t.Fatal(err)
	}

	job, err := manager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "route-1",
		JobId:          "job-1",
		Source:         source,
		Destination:    dest,
		FilesInFlight:  2,
		StreamsPerFile: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.GetState() != pb.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("initial job state = %s, want running", job.GetState())
	}

	job = waitForTransferJobState(t, manager, "job-1", pb.RuntimeState_RUNTIME_STATE_DONE)
	if job.GetFilesDone() != 2 {
		t.Fatalf("files done = %d, want 2", job.GetFilesDone())
	}
	if job.GetStreamsPerFile() != 3 {
		t.Fatalf("streams per file = %d, want 3", job.GetStreamsPerFile())
	}
	assertFileBytes(t, filepath.Join(dst, "a.txt"), []byte("alpha"))
	assertFileBytes(t, filepath.Join(dst, "nested", "b.txt"), []byte("beta"))

	list := manager.ListJobs("route-1")
	if len(list) != 1 || list[0].GetJobId() != "job-1" {
		t.Fatalf("unexpected job list: %+v", list)
	}
}

func TestTransferJobManagerDirectTCPBetweenManagers(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "file.txt"), []byte("over the wire"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1"}
	sourceManager := NewTransferJobManager(cfg, nil)
	destManager := NewTransferJobManager(cfg, nil)
	source, err := sourceManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "direct",
		JobId:    "job-direct",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: src,
	})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := destManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "direct",
		JobId:    "job-direct",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: dst,
	})
	if err != nil {
		t.Fatal(err)
	}
	if dest.GetDataEndpoint().GetPort() == 0 {
		t.Fatalf("destination data endpoint not allocated: %+v", dest)
	}
	if _, err := sourceManager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "direct",
		JobId:          "job-direct",
		Source:         source,
		Destination:    dest,
		FilesInFlight:  1,
		StreamsPerFile: 1,
	}); err != nil {
		t.Fatal(err)
	}
	waitForTransferJobState(t, sourceManager, "job-direct", pb.RuntimeState_RUNTIME_STATE_DONE)
	assertFileBytes(t, filepath.Join(dst, "file.txt"), []byte("over the wire"))
}

func TestTransferJobManagerDirectUDPBetweenManagers(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "file.txt"), []byte("udp over the wire"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1"}
	sourceManager := NewTransferJobManager(cfg, nil)
	destManager := NewTransferJobManager(cfg, nil)
	source, err := sourceManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "direct-udp",
		JobId:    "job-direct-udp",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_UDP,
		RootPath: src,
	})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := destManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "direct-udp",
		JobId:    "job-direct-udp",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_UDP,
		RootPath: dst,
	})
	if err != nil {
		t.Fatal(err)
	}
	if dest.GetDataEndpoint().GetPort() == 0 {
		t.Fatalf("destination data endpoint not allocated: %+v", dest)
	}
	if _, err := sourceManager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "direct-udp",
		JobId:          "job-direct-udp",
		Source:         source,
		Destination:    dest,
		FilesInFlight:  1,
		StreamsPerFile: 3,
	}); err != nil {
		t.Fatal(err)
	}
	job := waitForTransferJobState(t, sourceManager, "job-direct-udp", pb.RuntimeState_RUNTIME_STATE_DONE)
	if job.GetStreamsPerFile() != 3 {
		t.Fatalf("streams per file = %d, want 3", job.GetStreamsPerFile())
	}
	if job.GetStats().GetEgressBytes() == 0 || job.GetStats().GetSampledAtUnixNano() == 0 {
		t.Fatalf("job stats not populated: %+v", job.GetStats())
	}
	assertEventuallyFileBytes(t, filepath.Join(dst, "file.txt"), []byte("udp over the wire"))
}

func TestTransferJobManagerEndpointExpiresAndReleasesLease(t *testing.T) {
	cfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1"}
	manager := NewTransferJobManager(cfg, nil)
	defer manager.Close()

	endpoint, err := manager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		Role:       pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol:   pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath:   t.TempDir(),
		TtlSeconds: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.GetDataEndpoint().GetPort() == 0 {
		t.Fatalf("destination endpoint did not allocate data port: %+v", endpoint)
	}
	if _, ok := manager.GetEndpoint(endpoint.GetEndpointId()); !ok {
		t.Fatal("endpoint expired before ttl")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := manager.GetEndpoint(endpoint.GetEndpointId()); !ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("endpoint still present after ttl")
}

func TestUDPJobPayloadSizeRespectsMTUAndPacketLimit(t *testing.T) {
	exec := &TransferExecutionContext{UDPPayload: 1200}
	if got := udpJobPayloadSize(exec, "file.bin"); got >= 1200 {
		t.Fatalf("payload size = %d, want below mtu after header", got)
	}

	longPath := strings.Repeat("a", 2000)
	if got := udpJobPayloadSize(&TransferExecutionContext{UDPPayload: 1500}, longPath); got < 1 {
		t.Fatalf("payload size = %d, want positive size for long path", got)
	}

	tooManyStreams := make([]uint32, 1<<16)
	if _, err := encodeUDPJobStartPacket(udpJobStartPacket{sessionKey: 1, size: 1, streamIDs: tooManyStreams, relPath: "file.bin"}); err == nil {
		t.Fatal("expected oversized udp start packet error")
	}
}

func TestUDPJobStartPacketRoundTrip(t *testing.T) {
	packet, err := encodeUDPJobStartPacket(udpJobStartPacket{
		sessionKey: 42,
		size:       1234,
		streamIDs:  []uint32{1, 2, 3},
		relPath:    "nested/file.bin",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := decodeUDPJobStartPacket(packet)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("packet was not recognized as udp job start")
	}
	if got.sessionKey != 42 || got.size != 1234 || got.relPath != "nested/file.bin" ||
		len(got.streamIDs) != 3 || got.streamIDs[0] != 1 || got.streamIDs[1] != 2 || got.streamIDs[2] != 3 {
		t.Fatalf("decoded packet mismatch: %+v", got)
	}
	if !isUDPJobReadyPacket(encodeUDPJobReadyPacket(42), 42) {
		t.Fatal("ready packet did not validate")
	}
}

func TestTransferJobManagerTCPViaRelayForward(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "file.txt"), []byte("through relay"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1"}
	sourceManager := NewTransferJobManager(cfg, nil)
	destManager := NewTransferJobManager(cfg, nil)
	relayManager, err := NewForwardSessionManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer relayManager.Close()

	source, err := sourceManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "relay",
		JobId:    "job-relay",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: src,
	})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := destManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "relay",
		JobId:    "job-relay",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: dst,
	})
	if err != nil {
		t.Fatal(err)
	}
	forward, err := relayManager.Create(context.Background(), &pb.CreateForwardRequest{
		RouteId:  "relay",
		JobId:    "job-relay",
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		Egress:   dest.GetDataEndpoint(),
	})
	if err != nil {
		t.Fatal(err)
	}
	dest.DataEndpoint = forward.GetIngress()
	if _, err := sourceManager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "relay",
		JobId:          "job-relay",
		Source:         source,
		Destination:    dest,
		FilesInFlight:  1,
		StreamsPerFile: 1,
	}); err != nil {
		t.Fatal(err)
	}
	waitForTransferJobState(t, sourceManager, "job-relay", pb.RuntimeState_RUNTIME_STATE_DONE)
	assertFileBytes(t, filepath.Join(dst, "file.txt"), []byte("through relay"))
	snapshot, err := relayManager.Get(forward.GetForwardId())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.GetStats().GetIngressBytes() == 0 {
		t.Fatalf("relay stats did not record traffic: %+v", snapshot.GetStats())
	}
}

func TestTransferJobManagerUDPViaRelayForward(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "file.txt"), []byte("udp through relay"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1"}
	sourceManager := NewTransferJobManager(cfg, nil)
	destManager := NewTransferJobManager(cfg, nil)
	relayManager, err := NewForwardSessionManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer relayManager.Close()

	source, err := sourceManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "relay-udp",
		JobId:    "job-relay-udp",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_UDP,
		RootPath: src,
	})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := destManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "relay-udp",
		JobId:    "job-relay-udp",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_UDP,
		RootPath: dst,
	})
	if err != nil {
		t.Fatal(err)
	}
	forward, err := relayManager.Create(context.Background(), &pb.CreateForwardRequest{
		RouteId:  "relay-udp",
		JobId:    "job-relay-udp",
		Protocol: pb.DataProtocol_DATA_PROTOCOL_UDP,
		Egress:   dest.GetDataEndpoint(),
	})
	if err != nil {
		t.Fatal(err)
	}
	dest.DataEndpoint = forward.GetIngress()
	if _, err := sourceManager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "relay-udp",
		JobId:          "job-relay-udp",
		Source:         source,
		Destination:    dest,
		FilesInFlight:  1,
		StreamsPerFile: 1,
	}); err != nil {
		t.Fatal(err)
	}
	waitForTransferJobState(t, sourceManager, "job-relay-udp", pb.RuntimeState_RUNTIME_STATE_DONE)
	assertEventuallyFileBytes(t, filepath.Join(dst, "file.txt"), []byte("udp through relay"))
	var snapshot *pb.ForwardSession
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err = relayManager.Get(forward.GetForwardId())
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.GetStats().GetIngressBytes() > 0 && snapshot.GetStats().GetPackets() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("relay stats did not record udp traffic: %+v", snapshot.GetStats())
}

func TestTransferJobManagerConcurrencyUpdateAffectsScheduling(t *testing.T) {
	exec := newBlockingTransferExecutor(3)
	manager := NewTransferJobManager(nil, exec)
	source := &pb.TransferEndpoint{RootPath: "/src", Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP}
	dest := &pb.TransferEndpoint{RootPath: "/dst", Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP}

	job, err := manager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "route-2",
		JobId:          "job-2",
		Source:         source,
		Destination:    dest,
		FilesInFlight:  1,
		StreamsPerFile: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.GetFilesInFlight() != 1 {
		t.Fatalf("files in flight = %d, want 1", job.GetFilesInFlight())
	}
	exec.waitStarted(t, 1)
	if got := exec.startedCount(); got != 1 {
		t.Fatalf("started count = %d, want 1 before update", got)
	}

	updated, err := manager.UpdateConcurrency("job-2", 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if updated.GetFilesInFlight() != 2 || updated.GetStreamsPerFile() != 4 {
		t.Fatalf("unexpected updated concurrency: %+v", updated)
	}
	exec.waitStarted(t, 2)
	if got := exec.startedCount(); got != 2 {
		t.Fatalf("started count = %d, want 2 after update", got)
	}

	exec.releaseAll()
	waitForTransferJobState(t, manager, "job-2", pb.RuntimeState_RUNTIME_STATE_DONE)
}

func TestTransferJobManagerAbort(t *testing.T) {
	exec := newBlockingTransferExecutor(1)
	manager := NewTransferJobManager(nil, exec)
	_, err := manager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		JobId:         "job-abort",
		Source:        &pb.TransferEndpoint{RootPath: "/src", Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP},
		Destination:   &pb.TransferEndpoint{RootPath: "/dst", Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP},
		FilesInFlight: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	exec.waitStarted(t, 1)
	job, err := manager.AbortJob("job-abort")
	if err != nil {
		t.Fatal(err)
	}
	if job.GetState() != pb.RuntimeState_RUNTIME_STATE_ABORTED {
		t.Fatalf("state = %s, want aborted", job.GetState())
	}
	exec.releaseAll()
}

func waitForTransferJobState(t *testing.T, manager *TransferJobManager, jobID string, state pb.RuntimeState) *pb.TransferJob {
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

func assertFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s = %q, want %q", path, got, want)
	}
}

func assertEventuallyFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := os.ReadFile(path)
		if err == nil && bytes.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			if err != nil {
				t.Fatal(err)
			}
			t.Fatalf("%s = %q, want %q", path, got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type blockingTransferExecutor struct {
	plans   []TransferFilePlan
	started chan struct{}
	release chan struct{}

	mu    sync.Mutex
	count int
}

func newBlockingTransferExecutor(n int) *blockingTransferExecutor {
	plans := make([]TransferFilePlan, 0, n)
	for i := 0; i < n; i++ {
		plans = append(plans, TransferFilePlan{
			SourcePath:   filepath.Join("/src", "file"),
			RelativePath: filepath.Join("file"),
			Size:         1,
		})
	}
	return &blockingTransferExecutor{
		plans:   plans,
		started: make(chan struct{}, n),
		release: make(chan struct{}),
	}
}

func (e *blockingTransferExecutor) PlanFiles(context.Context, *pb.TransferEndpoint, []string) ([]TransferFilePlan, error) {
	return append([]TransferFilePlan(nil), e.plans...), nil
}

func (e *blockingTransferExecutor) TransferFile(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan) error {
	e.mu.Lock()
	e.count++
	e.mu.Unlock()
	e.started <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.release:
		if exec.OnProgress != nil {
			exec.OnProgress(plan.SourcePath, int(plan.Size))
		}
		return nil
	}
}

func (e *blockingTransferExecutor) waitStarted(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for e.startedCount() < n {
		select {
		case <-e.started:
		case <-deadline:
			t.Fatalf("started count = %d, want at least %d", e.startedCount(), n)
		}
	}
}

func (e *blockingTransferExecutor) startedCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.count
}

func (e *blockingTransferExecutor) releaseAll() {
	close(e.release)
}
