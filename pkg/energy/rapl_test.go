package energy

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRAPLMonitorWritesCSV(t *testing.T) {
	root := t.TempDir()
	writeRAPLDomain(t, root, "intel-rapl:0", "package-0", "1000")
	writeRAPLDomain(t, filepath.Join(root, "intel-rapl:0"), "intel-rapl:0:0", "dram", "250")

	monitor, err := NewRAPLMonitor(root)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := monitor.WriteCSVHeader(writer); err != nil {
		t.Fatal(err)
	}
	if err := monitor.WriteCSVRecord(writer, 7, "job-1", "route-1", time.Unix(0, 123)); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	for _, want := range []string{
		"timestamp_ns,tick,job_id,route_id,energy_uj_pkg,energy_uj_dram,energy_uj_sum_all,energy_uj_total",
		"123,7,job-1,route-1,1000,250,1250,1250",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("csv output missing %q:\n%s", want, got)
		}
	}
}

func TestRAPLMonitorFailsWithoutReadableDomains(t *testing.T) {
	if _, err := NewRAPLMonitor(t.TempDir()); err == nil {
		t.Fatal("expected missing RAPL domains error")
	}
}

func writeRAPLDomain(t *testing.T, root string, name string, domainName string, energy string) {
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
