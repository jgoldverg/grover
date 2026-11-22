// Package integration runs real groverd servers in-process on loopback and
// drives them through the same orchestrate/client code the CLI uses.
package integration

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jgoldverg/grover/internal/client"
	"github.com/jgoldverg/grover/internal/config"
	"github.com/jgoldverg/grover/internal/orchestrate"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/server"
	"github.com/jgoldverg/grover/internal/transfer"
)

type node struct {
	addr string
	srv  *server.Server
	logs string
}

func startNode(t *testing.T) *node {
	t.Helper()
	cfg := config.DefaultServer()
	cfg.InsecureControl = true
	cfg.DataBindHost, cfg.DataAdvertiseHost = "127.0.0.1", "127.0.0.1"
	cfg.JobLogDir = t.TempDir()
	cfg.RouteStoreFile = filepath.Join(t.TempDir(), "routes.json")
	srv, err := server.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx, ln); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return &node{addr: ln.Addr().String(), srv: srv, logs: cfg.JobLogDir}
}

func newPool(t *testing.T) *client.Pool {
	p := client.NewPool(config.Client{ServerURL: "127.0.0.1:1", InsecureControl: true})
	t.Cleanup(p.Close)
	return p
}

func makeFiles(t *testing.T, n, size int) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	var rels []string
	for i := 0; i < n; i++ {
		rel := filepath.Join(fmt.Sprintf("sub%d", i%2), fmt.Sprintf("file-%02d.bin", i))
		b := make([]byte, size+i*1013)
		_, _ = rand.Read(b)
		_ = os.MkdirAll(filepath.Join(dir, filepath.Dir(rel)), 0o755)
		if err := os.WriteFile(filepath.Join(dir, rel), b, 0o644); err != nil {
			t.Fatal(err)
		}
		rels = append(rels, rel)
	}
	return dir, rels
}

func assertSame(t *testing.T, srcDir, dstDir string, rels []string) {
	t.Helper()
	for _, rel := range rels {
		a, _ := os.ReadFile(filepath.Join(srcDir, rel))
		b, err := os.ReadFile(filepath.Join(dstDir, rel))
		if err != nil || sha256.Sum256(a) != sha256.Sum256(b) {
			t.Fatalf("%s differs or missing (err=%v, %d vs %d bytes)", rel, err, len(a), len(b))
		}
	}
}

// runJob starts a job on src over the session and waits for it.
func runJob(t *testing.T, pool *client.Pool, src *node, sessionID string, sources []string, dest string, tun *pb.Tuning) *pb.Job {
	t.Helper()
	ctx := context.Background()
	c, err := pool.Get(ctx, src.addr)
	if err != nil {
		t.Fatal(err)
	}
	job, err := c.Transfers.StartJob(ctx, &pb.StartJobRequest{SessionId: sessionID, Sources: sources, Destination: dest, Tuning: tun})
	if err != nil {
		t.Fatal(err)
	}
	final, err := orchestrate.Watch(ctx, c, job.JobId, 100*time.Millisecond, pb.FileView_FILE_VIEW_ACTIVE, nil)
	if err != nil {
		t.Fatal(err)
	}
	return final
}

func TestRoutes(t *testing.T) {
	cases := []struct {
		name   string
		proto  pb.DataProtocol
		origin pb.ConnectionOrigin
		relays int
	}{
		{"tcp-direct", pb.DataProtocol_DATA_PROTOCOL_TCP, pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE, 0},
		{"udp-direct", pb.DataProtocol_DATA_PROTOCOL_UDP, pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE, 0},
		{"tcp-1-relay", pb.DataProtocol_DATA_PROTOCOL_TCP, pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE, 1},
		{"udp-1-relay", pb.DataProtocol_DATA_PROTOCOL_UDP, pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE, 1},
		{"tcp-2-relays", pb.DataProtocol_DATA_PROTOCOL_TCP, pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE, 2},
		{"udp-2-relays", pb.DataProtocol_DATA_PROTOCOL_UDP, pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE, 2},
		{"tcp-dest-origin-direct", pb.DataProtocol_DATA_PROTOCOL_TCP, pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION, 0},
		{"tcp-dest-origin-1-relay", pb.DataProtocol_DATA_PROTOCOL_TCP, pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			src, dst := startNode(t), startNode(t)
			var via []string
			for i := 0; i < tc.relays; i++ {
				via = append(via, startNode(t).addr)
			}
			pool := newPool(t)
			sess, err := orchestrate.Prepare(ctx, pool, orchestrate.Spec{
				RouteID: tc.name, Source: src.addr, Destination: dst.addr, Via: via,
				Protocol: tc.proto, ConnectFrom: tc.origin, Connections: 4,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(sess.Hops) != tc.relays {
				t.Fatalf("hops = %d, want %d", len(sess.Hops), tc.relays)
			}
			srcDir, rels := makeFiles(t, 24, 64<<10)
			dstDir := t.TempDir()
			final := runJob(t, pool, src, sess.SessionId,
				[]string{srcDir, transfer.SyntheticSource(3<<20, "synthetic.bin")}, dstDir,
				&pb.Tuning{Concurrency: 4, Parallelism: 3})
			if final.State != pb.RuntimeState_RUNTIME_STATE_DONE {
				t.Fatalf("job %s: %s", final.State, final.Error)
			}
			assertSame(t, srcDir, dstDir, rels)
			if info, err := os.Stat(filepath.Join(dstDir, "synthetic.bin")); err != nil || info.Size() != 3<<20 {
				t.Fatalf("synthetic output: %v", err)
			}

			// The destination reports its side of the same job.
			dc, _ := pool.Get(ctx, dst.addr)
			recv, err := dc.Transfers.GetJob(ctx, &pb.GetJobRequest{JobId: final.JobId})
			if err != nil || recv.Role != pb.EndpointRole_ENDPOINT_ROLE_DESTINATION || recv.BytesDone != final.BytesTotal {
				t.Fatalf("destination view: %v err=%v", recv, err)
			}
			// Relays saw the bytes.
			for _, hop := range sess.Hops {
				rc, _ := pool.Get(ctx, hop.Control)
				fw, err := rc.Relays.GetForward(ctx, &pb.ForwardRef{ForwardId: hop.ForwardId})
				if err != nil || fw.Stats.BytesIn+fw.Stats.BytesOut < final.BytesTotal {
					t.Fatalf("relay hop %d stats: %v err=%v", hop.Index, fw.GetStats(), err)
				}
			}
			// The session is reusable, then closes cleanly.
			if again := runJob(t, pool, src, sess.SessionId, []string{transfer.SyntheticSource(1000, "again.bin")}, dstDir, nil); again.State != pb.RuntimeState_RUNTIME_STATE_DONE {
				t.Fatalf("second job: %s", again.Error)
			}
			if err := orchestrate.Close(ctx, pool, src.addr, sess.SessionId); err != nil {
				t.Fatal(err)
			}
			sc, _ := pool.Get(ctx, src.addr)
			if _, err := sc.Sessions.GetSession(ctx, &pb.SessionRef{SessionId: sess.SessionId}); status.Code(err) != codes.NotFound {
				t.Fatalf("session should be gone, got %v", err)
			}
			if _, err := dc.Transfers.CloseEndpoint(ctx, &pb.EndpointRef{EndpointId: sess.Destination.EndpointId}); status.Code(err) != codes.NotFound {
				t.Fatalf("destination endpoint should be closed, got %v", err)
			}
		})
	}
}

func TestCloseRefusesBusySessionAndAbortFrees(t *testing.T) {
	ctx := context.Background()
	src, dst := startNode(t), startNode(t)
	pool := newPool(t)
	sess, err := orchestrate.Prepare(ctx, pool, orchestrate.Spec{Source: src.addr, Destination: dst.addr})
	if err != nil {
		t.Fatal(err)
	}
	c, _ := pool.Get(ctx, src.addr)
	job, err := c.Transfers.StartJob(ctx, &pb.StartJobRequest{SessionId: sess.SessionId, Sources: []string{transfer.SyntheticSource(50<<30, "huge.bin")}, Destination: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := orchestrate.Close(ctx, pool, src.addr, sess.SessionId); err == nil || !strings.Contains(err.Error(), "abort it first") {
		t.Fatalf("close of busy session: %v", err)
	}
	if _, err := c.Transfers.AbortJob(ctx, &pb.JobRef{JobId: job.JobId}); err != nil {
		t.Fatal(err)
	}
	final, err := orchestrate.Watch(ctx, c, job.JobId, 100*time.Millisecond, pb.FileView_FILE_VIEW_NONE, nil)
	if err != nil || final.State != pb.RuntimeState_RUNTIME_STATE_ABORTED {
		t.Fatalf("abort: %v %v", final, err)
	}
	if err := orchestrate.Close(ctx, pool, src.addr, sess.SessionId); err != nil {
		t.Fatalf("close after abort: %v", err)
	}
}

func TestPrepareRollsBackOnFailure(t *testing.T) {
	ctx := context.Background()
	src, dst := startNode(t), startNode(t)
	pool := newPool(t)
	dead := "127.0.0.1:1" // nothing listens here
	_, err := orchestrate.Prepare(ctx, pool, orchestrate.Spec{Source: src.addr, Destination: dst.addr, Via: []string{dead}})
	if err == nil {
		t.Fatal("prepare through an unreachable relay should fail")
	}
	sc, _ := pool.Get(ctx, src.addr)
	list, _ := sc.Sessions.ListSessions(ctx, &pb.ListSessionsRequest{})
	if len(list.GetSessions()) != 0 {
		t.Fatal("failed prepare left a session behind")
	}
}

func TestRouteStoreAndHistoryOverRPC(t *testing.T) {
	ctx := context.Background()
	n := startNode(t)
	pool := newPool(t)
	c, _ := pool.Get(ctx, n.addr)
	if _, err := c.Routes.PutRoute(ctx, &pb.Route{Name: "loop", Source: n.addr, Destination: n.addr}); err != nil {
		t.Fatal(err)
	}
	r, err := c.Routes.GetRoute(ctx, &pb.RouteRef{Name: "loop"})
	if err != nil || r.Protocol != pb.DataProtocol_DATA_PROTOCOL_TCP {
		t.Fatalf("route: %v %v", r, err)
	}
	if _, err := c.Routes.GetRoute(ctx, &pb.RouteRef{Name: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing route should be NotFound, got %v", err)
	}
	sess, err := orchestrate.Prepare(ctx, pool, orchestrate.SpecFromRoute(r, ""))
	if err != nil {
		t.Fatal(err)
	}
	final := runJob(t, pool, n, sess.SessionId, []string{transfer.SyntheticSource(4096, "h.bin")}, t.TempDir(), nil)
	if final.State != pb.RuntimeState_RUNTIME_STATE_DONE {
		t.Fatal(final.Error)
	}
	// Wait for the destination record (same node) to finalize, then read history.
	deadline := time.Now().Add(15 * time.Second)
	for {
		list, err := c.History.ListHistory(ctx, &pb.ListHistoryRequest{})
		if err != nil {
			t.Fatal(err)
		}
		roles := map[pb.EndpointRole]bool{}
		for _, e := range list.Jobs {
			if e.JobId == final.JobId && e.State == pb.RuntimeState_RUNTIME_STATE_DONE {
				roles[e.Role] = true
			}
		}
		if roles[pb.EndpointRole_ENDPOINT_ROLE_SOURCE] && roles[pb.EndpointRole_ENDPOINT_ROLE_DESTINATION] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("history entries for both roles not found: %v", list.Jobs)
		}
		time.Sleep(200 * time.Millisecond)
	}
	rec, err := c.History.GetHistory(ctx, &pb.HistoryRef{JobId: final.JobId})
	if err != nil || rec.Final.GetBytesDone() != 4096 || len(rec.Manifest.GetFiles()) != 1 {
		t.Fatalf("history record: %v %v", rec, err)
	}
	snaps, err := c.History.ListSnapshots(ctx, &pb.ListSnapshotsRequest{JobId: final.JobId})
	if err != nil || len(snaps.Jobs) == 0 {
		t.Fatalf("snapshots: %v", err)
	}
}

func TestLocalCopyWithoutSession(t *testing.T) {
	n := startNode(t)
	pool := newPool(t)
	srcDir, rels := makeFiles(t, 3, 1000)
	dstDir := t.TempDir()
	final := runJob(t, pool, n, "", []string{srcDir}, dstDir, nil)
	if final.State != pb.RuntimeState_RUNTIME_STATE_DONE {
		t.Fatal(final.Error)
	}
	assertSame(t, srcDir, dstDir, rels)
}
