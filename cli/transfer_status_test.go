package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func TestTransferRateSamplerReportsIntervalTrend(t *testing.T) {
	sampler := &transferRateSampler{}
	first := &pb.TransferJob{
		GoodBytes: 100,
		Stats:     &pb.StatsSnapshot{AverageThroughputBps: 10},
	}
	if sample := sampler.Observe(first, time.Unix(0, 0)); sample.Valid {
		t.Fatal("first sample should be warmup")
	}

	second := &pb.TransferJob{
		GoodBytes: 1100,
		Stats:     &pb.StatsSnapshot{AverageThroughputBps: 100},
	}
	sample := sampler.Observe(second, time.Unix(1, 0))
	if !sample.Valid {
		t.Fatal("second sample should be valid")
	}
	if sample.NowBps != 1000 {
		t.Fatalf("now bps = %f, want 1000", sample.NowBps)
	}
	if sample.Trend != "flat" {
		t.Fatalf("first valid trend = %q, want flat", sample.Trend)
	}

	third := &pb.TransferJob{
		GoodBytes: 3100,
		Stats:     &pb.StatsSnapshot{AverageThroughputBps: 200},
	}
	sample = sampler.Observe(third, time.Unix(2, 0))
	if sample.NowBps != 2000 {
		t.Fatalf("now bps = %f, want 2000", sample.NowBps)
	}
	if sample.Trend != "up" {
		t.Fatalf("trend = %q, want up", sample.Trend)
	}
}

func TestPrintTransferJobStatusShowsNowAverageAndTrend(t *testing.T) {
	job := &pb.TransferJob{
		JobId:        "job-1",
		RouteId:      "route-1",
		State:        pb.RuntimeState_RUNTIME_STATE_RUNNING,
		Protocol:     pb.DataProtocol_DATA_PROTOCOL_UDP,
		GoodBytes:    2048,
		NetworkBytes: 2048,
		Stats:        &pb.StatsSnapshot{AverageThroughputBps: 1024, LatencyMs: 12.3},
		Files: []*pb.TransferFileState{{
			Path:         "/src/file.bin",
			RelativePath: "file.bin",
			Size:         4096,
			BytesDone:    2048,
			State:        pb.RuntimeState_RUNTIME_STATE_RUNNING,
			Streams: []*pb.TransferStreamState{{
				StreamId:             1,
				Size:                 4096,
				BytesDone:            2048,
				State:                pb.RuntimeState_RUNTIME_STATE_RUNNING,
				CurrentThroughputBps: 1024,
				AverageThroughputBps: 512,
			}},
		}},
	}
	var out bytes.Buffer
	printTransferJobStatus(&out, job, transferRateSample{NowBps: 2048, AvgBps: 1024, Trend: "up", Valid: true})

	got := out.String()
	for _, want := range []string{
		"Transferred: 2.00 KiB / 4.00 KiB, 50.0%, now 2.00 KiB/s, avg 1.00 KiB/s",
		"trend up",
		"Transferring:",
		"* file.bin: 2.00 KiB / 4.00 KiB, 50.0%",
		"stream 1: 2.00 KiB / 4.00 KiB, now 1.00 KiB/s, avg 512 B/s",
		"Grover network",
		"efficiency=100.00%",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestTransferProgressHelpers(t *testing.T) {
	if got := percentComplete(25, 100); got != 25 {
		t.Fatalf("percent = %f, want 25", got)
	}
	if got := remainingBytes(120, 100); got != 0 {
		t.Fatalf("remaining = %d, want 0", got)
	}
	if got := formatETA(2048, 1024); got != "2s" {
		t.Fatalf("eta = %q, want 2s", got)
	}
}
