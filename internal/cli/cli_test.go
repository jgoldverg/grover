package cli

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jgoldverg/grover/internal/config"
	"github.com/jgoldverg/grover/internal/server"
)

func TestParseLocation(t *testing.T) {
	cases := map[string]location{
		"/data/x":                   {Path: "/data/x"},
		"uc:/data/x":                {Node: "uc", Path: "/data/x"},
		"10.0.0.1:22444:/data/x/":   {Node: "10.0.0.1:22444", Path: "/data/x"},
		"[::1]:22444:/data":         {Node: "[::1]:22444", Path: "/data"},
		"synthetic://100/a.bin":     {Path: "synthetic://100/a.bin"},
		"uc:synthetic://100/a.bin":  {Node: "uc", Path: "synthetic://100/a.bin"},
		"host:1:synthetic://5/name": {Node: "host:1", Path: "synthetic://5/name"},
	}
	for in, want := range cases {
		got, err := parseLocation(in)
		if err != nil || got != want {
			t.Errorf("parseLocation(%q) = %+v, %v; want %+v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "relative/path", "uc:relative", "[::1:22:/x"} {
		if _, err := parseLocation(bad); err == nil {
			t.Errorf("parseLocation(%q) should fail", bad)
		}
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]uint64{"": 0, "1048576": 1 << 20, "1MiB": 1 << 20, "8MB": 8 << 20, "512k": 512 << 10, "1.5G": 3 << 29} {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	if _, err := parseSize("lots"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseScheduleRows(t *testing.T) {
	csv := "route,allocated_bytes,job_id,forecast_id,slot_duration_seconds,flow_count,carbon\n" +
		"tacc,1.5e3,a,1,60,4,10\n" +
		",,,,,,\n" +
		"tacc,100,b,3,60,,11\n" +
		"uc,1,c,1,60,,12\n"
	rows, err := parseScheduleRows(strings.NewReader(csv), "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].AllocatedBytes != 1500 || rows[0].FlowCount != 4 || rows[0].Raw["carbon"] != "10" {
		t.Fatalf("rows: %+v", rows)
	}
	sel, matched := selectScheduleRows(rows, "tacc", 0, 0)
	slots := scheduleSlots(sel)
	if matched != 2 || len(slots) != 2 || slots[1].offset.Minutes() != 2 {
		t.Fatalf("slots: %+v", slots)
	}
	if _, err := parseScheduleRows(strings.NewReader("route,job_id\nx,y\n"), "t"); err == nil {
		t.Fatal("missing allocated_bytes should fail")
	}
	if _, err := parseScheduleRows(strings.NewReader("route,job_id,allocated_bytes,flow_count\nx,y,1,1.5\n"), "t"); err == nil {
		t.Fatal("fractional flow_count should fail")
	}
}

// --- end to end through the real command tree ---

type cliEnv struct {
	t       *testing.T
	cfgPath string
	nodes   []string
}

func startNodes(t *testing.T, n int) *cliEnv {
	t.Helper()
	env := &cliEnv{t: t, cfgPath: filepath.Join(t.TempDir(), "cli.toml")}
	for i := 0; i < n; i++ {
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
		env.nodes = append(env.nodes, ln.Addr().String())
	}
	return env
}

func (e *cliEnv) run(args ...string) (string, error) {
	cmd := NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"--config", e.cfgPath, "--log-level", "error"}, args...))
	err := cmd.ExecuteContext(context.Background())
	return out.String(), err
}

func (e *cliEnv) must(args ...string) string {
	e.t.Helper()
	out, err := e.run(args...)
	if err != nil {
		e.t.Fatalf("grover %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func TestCLIEndToEnd(t *testing.T) {
	env := startNodes(t, 3)
	src, dst, relay := env.nodes[0], env.nodes[1], env.nodes[2]
	env.must("profile", "set", "src", "--server", src, "--insecure-control")
	env.must("profile", "set", "dst", "--server", dst, "--insecure-control")
	env.must("profile", "set", "relay", "--server", relay, "--insecure-control")
	if out := env.must("profile", "list"); !strings.Contains(out, "*  src") {
		t.Fatalf("first profile should become the default:\n%s", out)
	}

	srcDir, dstDir := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "a.bin"), bytes.Repeat([]byte("x"), 100_000), 0o644); err != nil {
		t.Fatal(err)
	}

	// Direct transfer between two nodes (temporary session).
	out := env.must("transfer", "src:"+srcDir+"/a.bin", "dst:"+dstDir+"/direct/")
	if !strings.Contains(out, "state") || !strings.Contains(out, "done") {
		t.Fatalf("direct transfer output:\n%s", out)
	}
	if b, err := os.ReadFile(filepath.Join(dstDir, "direct", "a.bin")); err != nil || len(b) != 100_000 {
		t.Fatalf("direct transfer result: %v", err)
	}

	// Route through a relay, prepared once, used twice.
	env.must("route", "put", "r1", "--source", "src", "--destination", "dst", "--via", "relay", "--protocol", "udp")
	if out := env.must("route", "list"); !strings.Contains(out, relay) || !strings.Contains(out, "udp") {
		t.Fatalf("route list:\n%s", out)
	}
	env.must("route", "prepare", "r1")
	env.must("transfer", "--route", "r1", "-P", "2", srcDir, dstDir+"/routed")
	env.must("transfer", "--route", "r1", "synthetic://5000/gen/zeros.bin", dstDir+"/routed")
	if b, err := os.ReadFile(filepath.Join(dstDir, "routed", "gen", "zeros.bin")); err != nil || len(b) != 5000 {
		t.Fatalf("routed synthetic: %v", err)
	}
	status := env.must("route", "status", "r1")
	if !strings.Contains(status, "hop 1") || !strings.Contains(status, "ready") {
		t.Fatalf("route status:\n%s", status)
	}

	// Conflicting flags are errors, not silently ignored.
	if _, err := env.run("transfer", "--route", "r1", "--protocol", "tcp", srcDir, dstDir); err == nil {
		t.Fatal("--protocol with --route should be rejected")
	}

	// Jobs and history are visible from the CLI.
	jobs := env.must("job", "list", "--route", "r1")
	if strings.Count(jobs, "done") < 2 {
		t.Fatalf("job list:\n%s", jobs)
	}
	if out := env.must("job", "history", "list", "--node", "src"); !strings.Contains(out, "r1") {
		t.Fatalf("history list:\n%s", out)
	}

	env.must("route", "close", "r1")
	if out := env.must("route", "status", "r1"); !strings.Contains(out, "no sessions") {
		t.Fatalf("after close:\n%s", out)
	}
	if _, err := env.run("transfer", "--route", "r1", srcDir, dstDir); err == nil || !strings.Contains(err.Error(), "route prepare") {
		t.Fatalf("transfer without a session should point at route prepare, got %v", err)
	}
}
