package transfer

import (
	"reflect"
	"testing"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func TestExecutionConfigurationSnapshotAndLiveParallelism(t *testing.T) {
	m := &Manager{runtimeCfg: normalizeTransferRuntimeConfig(internal.DefaultTransferRuntimeConfig())}
	r := &transferJobRuntime{
		jobID: "job", routeID: "route", protocol: pb.DataProtocol_DATA_PROTOCOL_UDP,
		source:         &pb.TransferEndpoint{RootPath: "/source"},
		dest:           &pb.TransferEndpoint{RootPath: "/dest", DataEndpoint: &pb.DataEndpoint{Host: "host", Port: 123}},
		chunkSizeBytes: 8192, tcpBufferBytes: 16384, streamsPerFile: 2,
		udpMTUBytes: 1400, udpWindow: 40, udpBatch: 8, udpAckPackets: 4, udpAckMs: 12,
		udpSocketRead: 100000, udpSocketWrite: 200000, udpFlowControl: "bbr",
	}
	first := m.executionForFile(r, 0)
	if first.Job != (JobIdentity{ID: "job", RouteID: "route"}) || first.Paths != (PathConfig{SourceRoot: "/source", DestRoot: "/dest"}) {
		t.Fatalf("identity/paths lost: %+v", first)
	}
	wantUDP := UDPConfig{MTU: 1400, FlowControl: "bbr", WindowPackets: 40, BatchPackets: 8, AckPackets: 4,
		AckEvery: 12 * time.Millisecond, SocketReadBytes: 100000, SocketWriteBytes: 200000}
	if first.UDP != wantUDP || first.IO.ChunkSizeBytes != 8192 || first.TCP.BufferBytes != 16384 {
		t.Fatalf("tuning lost: %+v", first)
	}
	if first.Target.Data == r.dest.DataEndpoint || !reflect.DeepEqual(first.Target.Data, r.dest.DataEndpoint) {
		t.Fatal("destination endpoint was not cloned")
	}
	r.mu.Lock()
	r.chunkSizeBytes = 32768
	r.udpMTUBytes = 1500
	r.streamsPerFile = 5
	r.mu.Unlock()
	second := m.executionForFile(r, 0)
	if first.IO.ChunkSizeBytes != 8192 || first.UDP.MTU != 1400 || second.IO.ChunkSizeBytes != 32768 || second.UDP.MTU != 1500 {
		t.Fatal("configuration is not a per-file snapshot")
	}
	if first.Hooks.StreamsPerFile() != 5 || second.Hooks.StreamsPerFile() != 5 {
		t.Fatal("parallelism hook did not retain live tuning")
	}
}

func TestExecutionConfigurationDefaults(t *testing.T) {
	defaults := normalizeTransferRuntimeConfig(internal.DefaultTransferRuntimeConfig())
	m := &Manager{runtimeCfg: defaults}
	exec := m.executionForFile(&transferJobRuntime{}, 0)
	if exec.IO.ChunkSizeBytes != uint64(defaults.ChunkSizeBytes) || exec.UDP.MTU != defaults.UDP.MTU ||
		exec.UDP.WindowPackets != defaults.UDP.WindowPackets || exec.UDP.BatchPackets != defaults.UDP.BatchPackets ||
		exec.UDP.AckPackets != defaults.UDP.AckEveryPackets || exec.UDP.AckEvery != defaults.UDP.AckEvery ||
		exec.UDP.FlowControl != defaults.UDP.FlowControl {
		t.Fatalf("defaults changed: %+v", exec)
	}
}
