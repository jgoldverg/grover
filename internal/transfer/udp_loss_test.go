package transfer

import (
	"context"
	"testing"

	"github.com/jgoldverg/grover/internal/pb"
)

// Regression: jumbo datagrams with a large window overflow loopback socket
// buffers, so most of the first window is lost. Recovery used to stall once
// late SACKs inflated the RTT estimate and blocked retransmission.
func TestUDPRecoversFromHeavyLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("moves 64 MiB")
	}
	src, _, _ := pairOverNetwork(t, pb.DataProtocol_DATA_PROTOCOL_UDP)
	_, err := src.StartJob(context.Background(), &pb.StartJobRequest{JobId: "lossy", SessionId: "s1", Destination: t.TempDir(),
		Sources: []string{SyntheticSource(64<<20, "big.bin")}, Tuning: &pb.Tuning{Parallelism: 4, UdpMtu: 8972, UdpWindowPackets: 16384}})
	if err != nil {
		t.Fatal(err)
	}
	f := waitJob(t, src, "lossy")
	if f.State != pb.RuntimeState_RUNTIME_STATE_DONE {
		t.Fatalf("state=%s err=%s retransmits=%d", f.State, f.Error, f.Retransmits)
	}
	t.Logf("retransmits=%d rtt=%.1fms avg=%.0f MB/s", f.Retransmits, f.RttMs, f.AverageBps/1e6)
}
