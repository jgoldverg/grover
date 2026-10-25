package transfer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func TestLocalFilesystemTransferExecutorPlansRootFileAsBaseName(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "file-1g.bin")
	if err := os.WriteFile(src, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	plans, err := (FilesystemSourcePlanner{}).PlanFiles(context.Background(), &pb.TransferEndpoint{
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

func TestUDPJobPayloadSizeRespectsMTUAndPacketLimit(t *testing.T) {
	exec := &Execution{UDP: UDPConfig{MTU: 1200}}
	if got := udpJobPayloadSize(exec, "file.bin"); got >= 1200 {
		t.Fatalf("payload size = %d, want below mtu after header", got)
	}

	longPath := strings.Repeat("a", 2000)
	if got := udpJobPayloadSize(&Execution{UDP: UDPConfig{MTU: 1500}}, longPath); got < 1 {
		t.Fatalf("payload size = %d, want positive size for long path", got)
	}

	tooManyStreams := make([]uint32, 1<<16)
	if _, err := encodeUDPJobStartPacket(udpJobStartPacket{sessionKey: 1, size: 1, streamIDs: tooManyStreams, relPath: "file.bin"}); err == nil {
		t.Fatal("expected oversized udp start packet error")
	}
}

func TestNormalizedUDPTransferTuningIncludesFlowControl(t *testing.T) {
	tuning := normalizeTransferRuntimeConfig((&internal.ServerConfig{UDPFlowControl: "bbr"}).TransferRuntimeConfig())
	if tuning.UDP.FlowControl != "bbr" {
		t.Fatalf("flow control = %q, want bbr", tuning.UDP.FlowControl)
	}
	if got := udpFlowControl(&Execution{UDP: UDPConfig{FlowControl: tuning.UDP.FlowControl}}); got != "bbr" {
		t.Fatalf("exec flow control = %q, want bbr", got)
	}
	if got := udpFlowControl(&Execution{UDP: UDPConfig{FlowControl: "unknown"}}); got != "fixed" {
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
