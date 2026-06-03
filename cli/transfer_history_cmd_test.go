package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTransferHistoryReadsManifestAndFinal(t *testing.T) {
	root := t.TempDir()
	jobDir := filepath.Join(root, "job-1")
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, "manifest.json"), []byte(`{
  "job_id": "job-1",
  "route_id": "route-a",
  "protocol": "DATA_PROTOCOL_TCP",
  "source_root": "/src",
  "destination_root": "/dst",
  "total_files": 1,
  "total_bytes": 42,
  "created_at": "2026-06-01T12:00:00Z"
}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, "final.json"), []byte(`{
  "jobId": "job-1",
  "routeId": "route-a",
  "state": "RUNTIME_STATE_DONE",
  "goodBytes": "42",
  "networkBytes": "42"
}`), 0o644); err != nil {
		t.Fatal(err)
	}
	entries, err := listTransferHistory(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d", len(entries))
	}
	if entries[0].Manifest.JobID != "job-1" || entries[0].Final == nil || entries[0].Final.State != "RUNTIME_STATE_DONE" {
		t.Fatalf("entry = %+v", entries[0])
	}
}
