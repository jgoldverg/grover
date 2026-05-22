package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTransferRouteFileDryRun(t *testing.T) {
	dir := t.TempDir()
	specPath := filepath.Join(dir, "route.toml")
	spec := `
source = "10.0.0.1:22444:/file.bin"
destination = "10.0.0.2:22444:/data/file.bin"

[transfer]
protocol = "tcp"
parallel_streams = 4
concurrency = 2

[route]
via = ["relay-a", "relay-b"]
`
	if err := os.WriteFile(specPath, []byte(spec), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := SimpleCopy()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--route-file", specPath, "--dry-run"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"route: 10.0.0.1:22444 -> relay-a -> relay-b -> 10.0.0.2:22444",
		"mode: bridge",
		"protocol: tcp",
		"parallel_streams: 4",
		"concurrency: 2",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestTransferRouteFileCLIFlagsOverrideSpec(t *testing.T) {
	dir := t.TempDir()
	specPath := filepath.Join(dir, "route.toml")
	spec := `
source = "10.0.0.1:22444:/file.bin"
destination = "10.0.0.2:22444:/data/file.bin"

[transfer]
protocol = "udp"
parallel_streams = 2
concurrency = 2

[route]
via = ["relay-a"]
`
	if err := os.WriteFile(specPath, []byte(spec), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := SimpleCopy()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--route-file", specPath,
		"--protocol", "tcp",
		"--parallel-streams", "8",
		"--concurrency", "3",
		"--via", "relay-x,relay-y",
		"--dry-run",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"route: 10.0.0.1:22444 -> relay-x -> relay-y -> 10.0.0.2:22444",
		"protocol: tcp",
		"parallel_streams: 8",
		"concurrency: 3",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}
