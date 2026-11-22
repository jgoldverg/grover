package transfer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jgoldverg/grover/internal/dataport"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/tuning"
	"github.com/jgoldverg/grover/internal/wire"
)

// fakeSessions is a minimal in-memory Sessions implementation.
type fakeSessions struct {
	mu     sync.Mutex
	s      map[string]*pb.RouteSession
	active map[string]string
}

func newFakeSessions() *fakeSessions {
	return &fakeSessions{s: map[string]*pb.RouteSession{}, active: map[string]string{}}
}

func (f *fakeSessions) put(s *pb.RouteSession) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.s[s.SessionId] = s
}

func (f *fakeSessions) Claim(id, job string) (*pb.RouteSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.s[id]
	if s == nil {
		return nil, ErrNotFound
	}
	if f.active[id] != "" {
		return nil, fmt.Errorf("session busy with %s", f.active[id])
	}
	f.active[id] = job
	return s, nil
}

func (f *fakeSessions) Release(id, job string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active[id] == job {
		delete(f.active, id)
	}
}

func newTestManager(t *testing.T, sessions Sessions) *Manager {
	t.Helper()
	ports, err := dataport.New(dataport.Options{BindHost: "127.0.0.1", AdvertiseHost: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(Options{Ports: ports, Sessions: sessions, JobLogDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}

// makeTree writes n random files under dir and returns their relative paths.
func makeTree(t *testing.T, dir string, n int, size int) []string {
	t.Helper()
	var rels []string
	for i := 0; i < n; i++ {
		rel := filepath.Join(fmt.Sprintf("d%d", i%3), fmt.Sprintf("f%03d.bin", i))
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		b := make([]byte, size+i*17)
		_, _ = rand.Read(b)
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		rels = append(rels, rel)
	}
	return rels
}

func sameFile(t *testing.T, a, b string) {
	t.Helper()
	x, err := os.ReadFile(a)
	if err != nil {
		t.Fatal(err)
	}
	y, err := os.ReadFile(b)
	if err != nil {
		t.Fatalf("missing destination file: %v", err)
	}
	if sha256.Sum256(x) != sha256.Sum256(y) {
		t.Fatalf("%s and %s differ (%d vs %d bytes)", a, b, len(x), len(y))
	}
}

func waitJob(t *testing.T, m *Manager, id string) *pb.Job {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		j, err := m.GetJob(id, pb.FileView_FILE_VIEW_ACTIVE)
		if err != nil {
			t.Fatal(err)
		}
		if isTerminal(j.GetState()) {
			return j
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish", id)
	return nil
}

func TestLocalCopy(t *testing.T) {
	m := newTestManager(t, nil)
	src, dst := t.TempDir(), t.TempDir()
	rels := makeTree(t, src, 5, 1000)
	job, err := m.StartJob(context.Background(), &pb.StartJobRequest{JobId: "local", Sources: []string{src}, Destination: dst})
	if err != nil {
		t.Fatal(err)
	}
	if job.GetFilesTotal() != 5 {
		t.Fatalf("planned %d files", job.GetFilesTotal())
	}
	final := waitJob(t, m, "local")
	if final.GetState() != pb.RuntimeState_RUNTIME_STATE_DONE || final.GetBytesDone() != final.GetBytesTotal() {
		t.Fatalf("final: %v", final)
	}
	for _, rel := range rels {
		sameFile(t, filepath.Join(src, rel), filepath.Join(dst, rel))
	}
	for _, f := range []string{ManifestFile, FinalFile, SnapshotsFile} {
		if _, err := os.Stat(filepath.Join(m.opts.JobLogDir, "local", f)); err != nil {
			t.Errorf("job log %s missing: %v", f, err)
		}
	}
}

// pairOverNetwork prepares a destination endpoint on dst and a session on src.
func pairOverNetwork(t *testing.T, proto pb.DataProtocol) (src, dst *Manager, sessions *fakeSessions) {
	sessions = newFakeSessions()
	src = newTestManager(t, sessions)
	dst = newTestManager(t, nil)
	ep, err := dst.PrepareEndpoint(&pb.PrepareEndpointRequest{SessionId: "s1", Role: pb.EndpointRole_ENDPOINT_ROLE_DESTINATION, Protocol: proto})
	if err != nil {
		t.Fatal(err)
	}
	sessions.put(&pb.RouteSession{SessionId: "s1", RouteId: "r1", Protocol: proto,
		ConnectFrom: pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE, Destination: ep, Data: ep.Data})
	return src, dst, sessions
}

func TestNetworkTransfer(t *testing.T) {
	for _, proto := range []pb.DataProtocol{pb.DataProtocol_DATA_PROTOCOL_TCP, pb.DataProtocol_DATA_PROTOCOL_UDP} {
		for _, par := range []uint32{1, 4} {
			t.Run(fmt.Sprintf("%s/parallelism=%d", proto, par), func(t *testing.T) {
				src, dst, _ := pairOverNetwork(t, proto)
				srcDir, dstDir := t.TempDir(), t.TempDir()
				rels := makeTree(t, srcDir, 6, 300_000)
				id := "net-job"
				_, err := src.StartJob(context.Background(), &pb.StartJobRequest{
					JobId: id, SessionId: "s1", Destination: dstDir,
					Sources: []string{srcDir, SyntheticSource(1<<20, "synthetic/zeros.bin"), SyntheticSource(0, "empty.bin")},
					Tuning:  &pb.Tuning{Concurrency: 3, Parallelism: par},
				})
				if err != nil {
					t.Fatal(err)
				}
				final := waitJob(t, src, id)
				if final.GetState() != pb.RuntimeState_RUNTIME_STATE_DONE {
					t.Fatalf("job ended %s: %s", final.GetState(), final.GetError())
				}
				for _, rel := range rels {
					sameFile(t, filepath.Join(srcDir, rel), filepath.Join(dstDir, rel))
				}
				zeros, err := os.ReadFile(filepath.Join(dstDir, "synthetic", "zeros.bin"))
				if err != nil || len(zeros) != 1<<20 || !bytes.Equal(zeros, make([]byte, 1<<20)) {
					t.Fatalf("synthetic file wrong: len=%d err=%v", len(zeros), err)
				}
				if info, err := os.Stat(filepath.Join(dstDir, "empty.bin")); err != nil || info.Size() != 0 {
					t.Fatalf("empty file: %v", err)
				}
				recv, err := dst.GetJob(id, pb.FileView_FILE_VIEW_NONE)
				if err != nil {
					t.Fatalf("destination has no receive record: %v", err)
				}
				if recv.GetRole() != pb.EndpointRole_ENDPOINT_ROLE_DESTINATION || recv.GetBytesDone() != final.GetBytesTotal() {
					t.Fatalf("receive record bytes=%d want %d", recv.GetBytesDone(), final.GetBytesTotal())
				}
			})
		}
	}
}

func TestDestinationOriginTCPReusesConnections(t *testing.T) {
	sessions := newFakeSessions()
	src := newTestManager(t, sessions)
	dst := newTestManager(t, nil)
	srcEP, err := src.PrepareEndpoint(&pb.PrepareEndpointRequest{SessionId: "s2", Role: pb.EndpointRole_ENDPOINT_ROLE_SOURCE,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP, ConnectFrom: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION})
	if err != nil {
		t.Fatal(err)
	}
	dstEP, err := dst.PrepareEndpoint(&pb.PrepareEndpointRequest{SessionId: "s2", Role: pb.EndpointRole_ENDPOINT_ROLE_DESTINATION,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP, ConnectFrom: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION,
		Dial: srcEP.Data, Token: srcEP.Token, Connections: 3})
	if err != nil {
		t.Fatal(err)
	}
	sessions.put(&pb.RouteSession{SessionId: "s2", Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		ConnectFrom: pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION, Source: srcEP, Destination: dstEP})

	srcDir, dstDir := t.TempDir(), t.TempDir()
	rels := makeTree(t, srcDir, 40, 2000)           // far more ranges than connections
	for _, id := range []string{"rev-1", "rev-2"} { // the session is reusable
		if _, err := src.StartJob(context.Background(), &pb.StartJobRequest{JobId: id, SessionId: "s2", Sources: []string{srcDir}, Destination: dstDir,
			Tuning: &pb.Tuning{Concurrency: 8, Parallelism: 2}}); err != nil {
			t.Fatal(err)
		}
		if final := waitJob(t, src, id); final.GetState() != pb.RuntimeState_RUNTIME_STATE_DONE {
			t.Fatalf("%s ended %s: %s", id, final.GetState(), final.GetError())
		}
	}
	for _, rel := range rels {
		sameFile(t, filepath.Join(srcDir, rel), filepath.Join(dstDir, rel))
	}
}

func TestSessionIsExclusive(t *testing.T) {
	src, _, _ := pairOverNetwork(t, pb.DataProtocol_DATA_PROTOCOL_TCP)
	req := &pb.StartJobRequest{SessionId: "s1", Sources: []string{SyntheticSource(200<<20, "big.bin")}, Destination: t.TempDir()}
	first, err := src.StartJob(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.StartJob(context.Background(), req); err == nil || !strings.Contains(err.Error(), "busy") {
		t.Fatalf("second job on a busy session should fail, got %v", err)
	}
	if _, err := src.AbortJob(first.GetJobId()); err != nil {
		t.Fatal(err)
	}
	if final := waitJob(t, src, first.GetJobId()); final.GetState() != pb.RuntimeState_RUNTIME_STATE_ABORTED {
		t.Fatalf("abort: %s", final.GetState())
	}
	if _, err := src.StartJob(context.Background(), &pb.StartJobRequest{SessionId: "s1", Sources: []string{SyntheticSource(10, "x")}, Destination: t.TempDir()}); err != nil {
		t.Fatalf("session should be free after abort: %v", err)
	}
}

func TestBadTokenIsRejected(t *testing.T) {
	src, _, sessions := pairOverNetwork(t, pb.DataProtocol_DATA_PROTOCOL_TCP)
	s, _ := sessions.Claim("s1", "x")
	sessions.Release("s1", "x")
	s.Destination.Token = wire.NewToken()
	if _, err := src.StartJob(context.Background(), &pb.StartJobRequest{JobId: "bad", SessionId: "s1", Sources: []string{SyntheticSource(10, "x")}, Destination: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	final := waitJob(t, src, "bad")
	if final.GetState() != pb.RuntimeState_RUNTIME_STATE_FAILED || !strings.Contains(final.GetError(), "bad token") {
		t.Fatalf("expected token rejection, got %s: %s", final.GetState(), final.GetError())
	}
}

func TestDestinationErrorsAreReported(t *testing.T) {
	src, _, _ := pairOverNetwork(t, pb.DataProtocol_DATA_PROTOCOL_TCP)
	blocker := filepath.Join(t.TempDir(), "file")
	_ = os.WriteFile(blocker, nil, 0o644)
	// Destination "directory" is a regular file, so mkdir fails on the receiver.
	if _, err := src.StartJob(context.Background(), &pb.StartJobRequest{JobId: "deny", SessionId: "s1", Sources: []string{SyntheticSource(10, "a/b")}, Destination: blocker}); err != nil {
		t.Fatal(err)
	}
	final := waitJob(t, src, "deny")
	if final.GetState() != pb.RuntimeState_RUNTIME_STATE_FAILED || !strings.Contains(final.GetError(), "destination error") {
		t.Fatalf("got %s: %s", final.GetState(), final.GetError())
	}
}

func TestTuneJobValidates(t *testing.T) {
	m := newTestManager(t, nil)
	j, err := m.StartJob(context.Background(), &pb.StartJobRequest{Sources: []string{SyntheticSource(1<<30, "x")}, Destination: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.TuneJob(j.GetJobId(), tuning.Tuning{ChunkSizeBytes: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid tuning error, got %v", err)
	}
	got, err := m.TuneJob(j.GetJobId(), tuning.Tuning{Concurrency: 9})
	if err != nil || got.GetTuning().GetConcurrency() != 9 {
		t.Fatalf("tune: %v %v", got.GetTuning(), err)
	}
	_, _ = m.AbortJob(j.GetJobId())
}

func TestPlanAndPaths(t *testing.T) {
	if _, err := PlanSources(context.Background(), []string{"relative/path"}); err == nil {
		t.Fatal("relative source should fail")
	}
	if _, err := PlanSources(context.Background(), []string{"synthetic://10/../escape"}); err == nil {
		t.Fatal("escaping synthetic path should fail")
	}
	for _, rel := range []string{"../x", "a/../../x", "/abs", ""} {
		if _, err := DestinationPath("/dst", rel); err == nil {
			t.Errorf("DestinationPath accepted %q", rel)
		}
	}
	if _, err := DestinationPath("relative", "x"); err == nil {
		t.Fatal("relative destination should fail")
	}
	if p, err := DestinationPath("/dst", "a/b.bin"); err != nil || p != filepath.FromSlash("/dst/a/b.bin") {
		t.Fatalf("got %s %v", p, err)
	}
	if r := splitRanges(10, 3); len(r) != 3 || r[0].length != 4 || r[2].offset != 7 {
		t.Fatalf("ranges %+v", r)
	}
	if r := splitRanges(2, 8); len(r) != 2 {
		t.Fatalf("ranges %+v", r)
	}
}
