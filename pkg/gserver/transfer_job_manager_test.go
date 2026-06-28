package gserver

import (
	"bytes"
	"context"
	"encoding/json"
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
	for _, file := range job.GetFiles() {
		if len(file.GetStreams()) != 3 {
			t.Fatalf("file %s streams = %d, want 3", file.GetRelativePath(), len(file.GetStreams()))
		}
		var streamBytes uint64
		for _, stream := range file.GetStreams() {
			if stream.GetState() != pb.RuntimeState_RUNTIME_STATE_DONE {
				t.Fatalf("file %s stream %d state = %s, want done", file.GetRelativePath(), stream.GetStreamId(), stream.GetState())
			}
			streamBytes += stream.GetBytesDone()
		}
		if streamBytes != file.GetSize() {
			t.Fatalf("file %s stream bytes = %d, want %d", file.GetRelativePath(), streamBytes, file.GetSize())
		}
	}
	assertFileBytes(t, filepath.Join(dst, "a.txt"), []byte("alpha"))
	assertFileBytes(t, filepath.Join(dst, "nested", "b.txt"), []byte("beta"))

	list := manager.ListJobs("route-1")
	if len(list) != 1 || list[0].GetJobId() != "job-1" {
		t.Fatalf("unexpected job list: %+v", list)
	}
}

func TestTransferJobManagerWritesHistoricalJobLog(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("alpha"), 0o600); err != nil {
		t.Fatal(err)
	}
	logRoot := t.TempDir()
	manager := NewTransferJobManager(&internal.ServerConfig{JobLogDir: logRoot}, nil)
	jobID := "job/history:one"
	if _, err := manager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "route-history",
		JobId:          jobID,
		Source:         &pb.TransferEndpoint{RootPath: src, Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP},
		Destination:    &pb.TransferEndpoint{RootPath: dst, Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP},
		FilesInFlight:  1,
		StreamsPerFile: 1,
	}); err != nil {
		t.Fatal(err)
	}
	waitForTransferJobState(t, manager, jobID, pb.RuntimeState_RUNTIME_STATE_DONE)

	jobLogDir := filepath.Join(logRoot, "job_history_one")
	manifestBytes, err := os.ReadFile(filepath.Join(jobLogDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest transferJobManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.JobID != jobID || manifest.RouteID != "route-history" || manifest.TotalFiles != 1 {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}
	if _, err := os.Stat(filepath.Join(jobLogDir, "final.json")); err != nil {
		t.Fatal(err)
	}
	snapshots, err := os.ReadFile(filepath.Join(jobLogDir, "snapshots.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(strings.TrimSpace(string(snapshots)), "\n") + 1; lines < 2 {
		t.Fatalf("snapshot lines = %d, want at least initial and final", lines)
	}
}

func TestTransferJobManagerWritesEnergyCSVWhenEnabled(t *testing.T) {
	raplRoot := t.TempDir()
	writeFakeRAPLDomain(t, raplRoot, "intel-rapl:0", "package-0", "1000")
	writeFakeRAPLDomain(t, filepath.Join(raplRoot, "intel-rapl:0"), "intel-rapl:0:0", "dram", "250")
	t.Setenv("GROVER_RAPL_ROOT", raplRoot)

	src := t.TempDir()
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("alpha"), 0o600); err != nil {
		t.Fatal(err)
	}
	logRoot := t.TempDir()
	manager := NewTransferJobManager(&internal.ServerConfig{
		JobLogDir:      logRoot,
		EnergyMonitor:  true,
		EnergySampleMs: 10,
	}, nil)
	defer manager.Close()
	jobID := "job-energy"
	if _, err := manager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "route-energy",
		JobId:          jobID,
		Source:         &pb.TransferEndpoint{RootPath: src, Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP},
		Destination:    &pb.TransferEndpoint{RootPath: dst, Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP},
		FilesInFlight:  1,
		StreamsPerFile: 1,
	}); err != nil {
		t.Fatal(err)
	}
	waitForTransferJobState(t, manager, jobID, pb.RuntimeState_RUNTIME_STATE_DONE)

	energyCSV, err := os.ReadFile(filepath.Join(logRoot, jobID, "energy.csv"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(energyCSV)
	if !strings.Contains(got, "energy_uj_pkg,energy_uj_dram,energy_uj_sum_all,energy_uj_total") {
		t.Fatalf("energy csv missing header: %s", got)
	}
	if !strings.Contains(got, "1000,250,1250,1250") {
		t.Fatalf("energy csv missing sample: %s", got)
	}

	nodeEnergyCSV, err := os.ReadFile(filepath.Join(logRoot, "energy.csv"))
	if err != nil {
		t.Fatal(err)
	}
	got = string(nodeEnergyCSV)
	if !strings.Contains(got, "timestamp_ns,tick,active_job_count,job_id,route_id,energy_uj_pkg,energy_uj_dram,energy_uj_sum_all,energy_uj_total") {
		t.Fatalf("node energy csv missing header: %s", got)
	}
	if !strings.Contains(got, ",0,0,,,1000,250,1250,1250") {
		t.Fatalf("node energy csv missing baseline row: %s", got)
	}
	if !strings.Contains(got, ",1,1,job-energy,route-energy,1000,250,1250,1250") {
		t.Fatalf("node energy csv missing active job row: %s", got)
	}
}

func TestTransferJobManagerSyntheticSourceLifecycle(t *testing.T) {
	dst := t.TempDir()
	manager := NewTransferJobManager(nil, nil)
	job, err := manager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "route-synthetic",
		JobId:          "job-synthetic",
		Source:         &pb.TransferEndpoint{RootPath: "/unused", Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP},
		Destination:    &pb.TransferEndpoint{RootPath: dst, Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP},
		Paths:          []string{"synthetic://4096/schedule/tacc_buff/job-1.bin"},
		FilesInFlight:  1,
		StreamsPerFile: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.GetState() != pb.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("initial job state = %s, want running", job.GetState())
	}
	job = waitForTransferJobState(t, manager, "job-synthetic", pb.RuntimeState_RUNTIME_STATE_DONE)
	if job.GetGoodBytes() != 4096 {
		t.Fatalf("good bytes = %d, want 4096", job.GetGoodBytes())
	}
	out := filepath.Join(dst, "schedule", "tacc_buff", "job-1.bin")
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4096 {
		t.Fatalf("synthetic output length = %d, want 4096", len(got))
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("synthetic output byte[%d] = %d, want 0", i, b)
		}
	}
}

func TestLocalFilesystemTransferExecutorPlansRootFileAsBaseName(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "file-1g.bin")
	if err := os.WriteFile(src, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	plans, err := (localFilesystemTransferExecutor{}).PlanFiles(context.Background(), &pb.TransferEndpoint{
		RootPath: src,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(plans))
	}
	if plans[0].RelativePath != "file-1g.bin" {
		t.Fatalf("relative path = %q, want file-1g.bin", plans[0].RelativePath)
	}
}

func TestResolveDestinationFilePathUsesExplicitFileTarget(t *testing.T) {
	dir := t.TempDir()
	got, err := resolveDestinationFilePath(filepath.Join(dir, "file-1g.bin"), "file-1g.bin")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(dir, "file-1g.bin"); got != want {
		t.Fatalf("destination = %q, want %q", got, want)
	}

	got, err = resolveDestinationFilePath(dir, "nested/file-1g.bin")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(dir, "nested", "file-1g.bin"); got != want {
		t.Fatalf("destination = %q, want %q", got, want)
	}
}

func TestTransferJobManagerDirectTCPBetweenManagers(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	payload := []byte("over the wire across multiple tcp ranges")
	if err := os.WriteFile(filepath.Join(src, "file.txt"), payload, 0o600); err != nil {
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
		StreamsPerFile: 3,
	}); err != nil {
		t.Fatal(err)
	}
	job := waitForTransferJobState(t, sourceManager, "job-direct", pb.RuntimeState_RUNTIME_STATE_DONE)
	if len(job.GetFiles()) != 1 || len(job.GetFiles()[0].GetStreams()) != 3 {
		t.Fatalf("tcp streams = %+v, want 3 streams on one file", job.GetFiles())
	}
	for _, stream := range job.GetFiles()[0].GetStreams() {
		if stream.GetState() != pb.RuntimeState_RUNTIME_STATE_DONE {
			t.Fatalf("stream %d state = %s, want done", stream.GetStreamId(), stream.GetState())
		}
		if stream.GetBytesDone() == 0 {
			t.Fatalf("stream %d did not record progress: %+v", stream.GetStreamId(), stream)
		}
	}
	assertFileBytes(t, filepath.Join(dst, "file.txt"), payload)
}

func TestTransferJobManagerDestinationWritesReceiveHistory(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	payload := bytes.Repeat([]byte("history over destination tcp streams\n"), 1024)
	if err := os.WriteFile(filepath.Join(src, "file.txt"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	logRoot := t.TempDir()
	sourceCfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1"}
	destCfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1", JobLogDir: logRoot}
	sourceManager := NewTransferJobManager(sourceCfg, nil)
	destManager := NewTransferJobManager(destCfg, nil)
	defer sourceManager.Close()
	defer destManager.Close()

	source, err := sourceManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "direct-history",
		JobId:    "job-direct-history",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: src,
	})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := destManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "direct-history",
		JobId:    "job-direct-history",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: dst,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceManager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "direct-history",
		JobId:          "job-direct-history",
		Source:         source,
		Destination:    dest,
		FilesInFlight:  1,
		StreamsPerFile: 4,
	}); err != nil {
		t.Fatal(err)
	}
	waitForTransferJobState(t, sourceManager, "job-direct-history", pb.RuntimeState_RUNTIME_STATE_DONE)
	assertFileBytes(t, filepath.Join(dst, "file.txt"), payload)

	destManager.finalizeDestinationReceives()
	jobLogDir := filepath.Join(logRoot, "job-direct-history")
	manifestBytes, err := os.ReadFile(filepath.Join(jobLogDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest transferJobManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.JobID != "job-direct-history" || manifest.RouteID != "direct-history" {
		t.Fatalf("unexpected destination manifest identity: %+v", manifest)
	}
	if manifest.DestinationRoot != dst || manifest.TotalFiles != 1 || manifest.TotalBytes != uint64(len(payload)) {
		t.Fatalf("unexpected destination manifest totals: %+v", manifest)
	}
	snapshots, err := os.ReadFile(filepath.Join(jobLogDir, "snapshots.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(snapshots), `"jobId":"job-direct-history"`) {
		t.Fatalf("destination snapshots are not transfer-job snapshots: %s", snapshots)
	}
	final, err := os.ReadFile(filepath.Join(jobLogDir, "final.json"))
	if err != nil {
		t.Fatal(err)
	}
	finalText := string(final)
	if !strings.Contains(finalText, `"jobId":  "job-direct-history"`) || !strings.Contains(finalText, `"diskWriteBytes":`) {
		t.Fatalf("unexpected destination final log: %s", finalText)
	}
}

func TestTransferJobManagerReverseTCPBetweenManagers(t *testing.T) {
	routeSourceDst := t.TempDir()
	routeDestinationSrc := t.TempDir()
	payload := []byte("reverse direction over prepared tcp route")
	if err := os.WriteFile(filepath.Join(routeDestinationSrc, "file.txt"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1"}
	routeSourceManager := NewTransferJobManager(cfg, nil)
	routeDestinationManager := NewTransferJobManager(cfg, nil)
	reverseSource, err := routeDestinationManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "bidirectional",
		JobId:    "session-bidi",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: routeDestinationSrc,
	})
	if err != nil {
		t.Fatal(err)
	}
	reverseDest, err := routeSourceManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "bidirectional",
		JobId:    "session-bidi",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: routeSourceDst,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := routeDestinationManager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "bidirectional",
		JobId:          "job-reverse",
		SessionId:      "session-bidi",
		Source:         reverseSource,
		Destination:    reverseDest,
		FilesInFlight:  1,
		StreamsPerFile: 2,
	}); err != nil {
		t.Fatal(err)
	}
	job := waitForTransferJobState(t, routeDestinationManager, "job-reverse", pb.RuntimeState_RUNTIME_STATE_DONE)
	if len(job.GetFiles()) != 1 || len(job.GetFiles()[0].GetStreams()) != 2 {
		t.Fatalf("tcp streams = %+v, want 2 streams on one file", job.GetFiles())
	}
	assertFileBytes(t, filepath.Join(routeSourceDst, "file.txt"), payload)
}

func TestTransferJobManagerDestinationOriginDirectTCPBetweenManagers(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	payload := []byte("destination originated the tcp connection")
	if err := os.WriteFile(filepath.Join(src, "file.txt"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1"}
	sourceManager := NewTransferJobManager(cfg, nil)
	defer sourceManager.Close()
	destManager := NewTransferJobManager(cfg, nil)
	defer destManager.Close()

	source, err := sourceManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:          "edu-direct",
		JobId:            "job-edu-direct",
		Role:             pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath:         src,
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION,
	})
	if err != nil {
		t.Fatal(err)
	}
	if source.GetDataEndpoint().GetPort() == 0 {
		t.Fatalf("source reverse data endpoint not allocated: %+v", source)
	}
	dest, err := destManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:          "edu-direct",
		JobId:            "job-edu-direct",
		Role:             pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath:         dst,
		Bind:             source.GetDataEndpoint(),
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceManager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:          "edu-direct",
		JobId:            "job-edu-direct",
		Source:           source,
		Destination:      dest,
		FilesInFlight:    1,
		StreamsPerFile:   2,
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION,
	}); err != nil {
		t.Fatal(err)
	}
	job := waitForTransferJobState(t, sourceManager, "job-edu-direct", pb.RuntimeState_RUNTIME_STATE_DONE)
	if len(job.GetFiles()) != 1 || len(job.GetFiles()[0].GetStreams()) != 2 {
		t.Fatalf("tcp streams = %+v, want 2 streams on one file", job.GetFiles())
	}
	assertEventuallyFileBytes(t, filepath.Join(dst, "file.txt"), payload)
}

func TestTransferJobManagerDestinationOriginTCPViaRelayForward(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	payload := []byte("destination originated tcp through relay")
	if err := os.WriteFile(filepath.Join(src, "file.txt"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1"}
	sourceManager := NewTransferJobManager(cfg, nil)
	defer sourceManager.Close()
	destManager := NewTransferJobManager(cfg, nil)
	defer destManager.Close()
	relayManager, err := NewForwardSessionManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer relayManager.Close()

	source, err := sourceManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:          "edu-relay",
		JobId:            "job-edu-relay",
		Role:             pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath:         src,
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION,
	})
	if err != nil {
		t.Fatal(err)
	}
	forward, err := relayManager.Create(context.Background(), &pb.CreateForwardRequest{
		RouteId:    "edu-relay",
		JobId:      "job-edu-relay",
		Protocol:   pb.DataProtocol_DATA_PROTOCOL_TCP,
		Egress:     source.GetDataEndpoint(),
		TtlSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := destManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:          "edu-relay",
		JobId:            "job-edu-relay",
		Role:             pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath:         dst,
		Bind:             forward.GetIngress(),
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceManager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:          "edu-relay",
		JobId:            "job-edu-relay",
		Source:           source,
		Destination:      dest,
		FilesInFlight:    1,
		StreamsPerFile:   2,
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION,
	}); err != nil {
		t.Fatal(err)
	}
	waitForTransferJobState(t, sourceManager, "job-edu-relay", pb.RuntimeState_RUNTIME_STATE_DONE)
	assertEventuallyFileBytes(t, filepath.Join(dst, "file.txt"), payload)
	snapshot, err := relayManager.Get(forward.GetForwardId())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.GetStats().GetIngressBytes() == 0 {
		t.Fatalf("relay stats did not record destination-origin traffic: %+v", snapshot.GetStats())
	}
}

func TestTransferJobManagerPreparedTCPRouteSessionCanRunSerialTransfers(t *testing.T) {
	srcOne := t.TempDir()
	dstOne := t.TempDir()
	srcTwo := t.TempDir()
	dstTwo := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcOne, "one.txt"), []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcTwo, "two.txt"), []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1"}
	sourceManager := NewTransferJobManager(cfg, nil)
	defer sourceManager.Close()
	destManager := NewTransferJobManager(cfg, nil)
	defer destManager.Close()

	sourceSession, err := sourceManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:   "prepared-direct",
		JobId:     "session-direct",
		SessionId: "session-direct",
		Role:      pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol:  pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath:  string(os.PathSeparator),
	})
	if err != nil {
		t.Fatal(err)
	}
	destSession, err := destManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:   "prepared-direct",
		JobId:     "session-direct",
		SessionId: "session-direct",
		Role:      pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol:  pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath:  string(os.PathSeparator),
	})
	if err != nil {
		t.Fatal(err)
	}

	run := func(jobID, srcRoot, dstRoot, fileName string, want []byte) {
		t.Helper()
		source := cloneTransferEndpoint(sourceSession)
		dest := cloneTransferEndpoint(destSession)
		source.RootPath = srcRoot
		dest.RootPath = dstRoot
		if _, err := sourceManager.StartJob(context.Background(), &pb.StartTransferJobRequest{
			RouteId:        "prepared-direct",
			JobId:          jobID,
			SessionId:      "session-direct",
			Source:         source,
			Destination:    dest,
			FilesInFlight:  1,
			StreamsPerFile: 1,
		}); err != nil {
			t.Fatal(err)
		}
		waitForTransferJobState(t, sourceManager, jobID, pb.RuntimeState_RUNTIME_STATE_DONE)
		assertEventuallyFileBytes(t, filepath.Join(dstRoot, fileName), want)
	}

	run("job-one", srcOne, dstOne, "one.txt", []byte("one"))
	run("job-two", srcTwo, dstTwo, "two.txt", []byte("two"))
}

func TestTransferJobManagerDirectTCPRootFileBetweenManagers(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "file-1g.bin")
	if err := os.WriteFile(srcFile, []byte("over the wire"), 0o600); err != nil {
		t.Fatal(err)
	}
	dstFile := filepath.Join(dstDir, "file-1g.bin")
	cfg := &internal.ServerConfig{DataBindHost: "127.0.0.1", DataAdvertiseHost: "127.0.0.1"}
	sourceManager := NewTransferJobManager(cfg, nil)
	destManager := NewTransferJobManager(cfg, nil)
	source, err := sourceManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "direct-file",
		JobId:    "job-direct-file",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: srcFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := destManager.PrepareEndpoint(context.Background(), &pb.PrepareTransferEndpointRequest{
		RouteId:  "direct-file",
		JobId:    "job-direct-file",
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		RootPath: dstFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceManager.StartJob(context.Background(), &pb.StartTransferJobRequest{
		RouteId:        "direct-file",
		JobId:          "job-direct-file",
		Source:         source,
		Destination:    dest,
		FilesInFlight:  1,
		StreamsPerFile: 1,
	}); err != nil {
		t.Fatal(err)
	}
	job := waitForTransferJobState(t, sourceManager, "job-direct-file", pb.RuntimeState_RUNTIME_STATE_DONE)
	if job.GetFilesDone() != 1 {
		t.Fatalf("files done = %d, want 1", job.GetFilesDone())
	}
	assertEventuallyFileBytes(t, dstFile, []byte("over the wire"))
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
	if len(job.GetFiles()) != 1 || len(job.GetFiles()[0].GetStreams()) != 3 {
		t.Fatalf("udp streams = %+v, want 3 streams on one file", job.GetFiles())
	}
	for _, stream := range job.GetFiles()[0].GetStreams() {
		if stream.GetBytesDone() == 0 {
			t.Fatalf("stream %d did not record progress: %+v", stream.GetStreamId(), stream)
		}
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

func TestNormalizedUDPTransferTuningIncludesFlowControl(t *testing.T) {
	tuning := normalizedUDPTransferTuning(&internal.ServerConfig{UDPFlowControl: "bbr"})
	if tuning.flowControl != "bbr" {
		t.Fatalf("flow control = %q, want bbr", tuning.flowControl)
	}
	if got := udpFlowControl(&TransferExecutionContext{UDPFlow: tuning.flowControl}); got != "bbr" {
		t.Fatalf("exec flow control = %q, want bbr", got)
	}
	if got := udpFlowControl(&TransferExecutionContext{UDPFlow: "unknown"}); got != "fixed" {
		t.Fatalf("unknown flow control = %q, want fixed", got)
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
	if !isUDPJobDonePacket(encodeUDPJobDonePacket(42), 42) {
		t.Fatal("done packet did not validate")
	}
	if isUDPJobDonePacket(encodeUDPJobReadyPacket(42), 42) {
		t.Fatal("ready packet validated as done")
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
		StreamsPerFile: 3,
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

func writeFakeRAPLDomain(t *testing.T, root string, name string, domainName string, energy string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "name"), []byte(domainName), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "energy_uj"), []byte(energy), 0o644); err != nil {
		t.Fatal(err)
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
