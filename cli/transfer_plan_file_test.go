package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadTransferPlanDocumentYAML(t *testing.T) {
	dir := t.TempDir()
	content := `
version: 1
via: client
idempotency_key: plan-123
params:
  concurrency: 8
  overwrite: always
endpoints:
  from:
    - name: build
      uri: file:///builds/report.pdf
  to:
    - name: ops
      uri: sftp://ops@reports/incoming/
      credential_id: ops-prod
    - name: archive
      uri: grover://cold-storage/reports/
maps:
  - from: build
    to: [ops, archive]
    dest_path: /reports/report.pdf
    options:
      overwrite: if-different
`
	planPath := filepath.Join(dir, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write plan file: %v", err)
	}

	doc, err := loadTransferPlanDocument(planPath)
	if err != nil {
		t.Fatalf("load plan: %v", err)
	}
	if doc.Via != "client" {
		t.Fatalf("expected via=client, got %q", doc.Via)
	}
	if doc.IdempotencyKey != "plan-123" {
		t.Fatalf("unexpected idempotency key %q", doc.IdempotencyKey)
	}
	fromInputs, toInputs, mapSpecs, err := doc.toInputs()
	if err != nil {
		t.Fatalf("toSpecSlices: %v", err)
	}
	if len(fromInputs) != 1 || fromInputs[0].Label != "build" || fromInputs[0].URI != "file:///builds/report.pdf" {
		t.Fatalf("unexpected from inputs: %#v", fromInputs)
	}
	if len(toInputs) != 2 {
		t.Fatalf("expected 2 to inputs, got %d", len(toInputs))
	}
	if toInputs[0].CredentialID != "ops-prod" {
		t.Fatalf("expected credential id ops-prod, got %q", toInputs[0].CredentialID)
	}
	if len(mapSpecs) != 2 {
		t.Fatalf("expected 2 map specs, got %d", len(mapSpecs))
	}
	expected := "build ops:/reports/report.pdf overwrite=if-different"
	found := false
	for _, m := range mapSpecs {
		if m == expected {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected map spec %q in %v", expected, mapSpecs)
	}
}

func TestLoadTransferPlanDocumentJSON(t *testing.T) {
	dir := t.TempDir()
	content := `{
  "version": 1,
  "endpoints": {
    "from": [{"uri": "file:///tmp/src.txt"}],
    "to":   [{"uri": "file:///tmp/dst.txt"}]
  }
}`
	planPath := filepath.Join(dir, "plan.json")
	if err := os.WriteFile(planPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write plan file: %v", err)
	}
	doc, err := loadTransferPlanDocument(planPath)
	if err != nil {
		t.Fatalf("load plan: %v", err)
	}
	fromInputs, toInputs, mapSpecs, err := doc.toInputs()
	if err != nil {
		t.Fatalf("toSpecSlices: %v", err)
	}
	if len(fromInputs) != 1 || fromInputs[0].URI != "file:///tmp/src.txt" {
		t.Fatalf("unexpected from inputs: %#v", fromInputs)
	}
	if len(toInputs) != 1 || toInputs[0].URI != "file:///tmp/dst.txt" {
		t.Fatalf("unexpected to inputs: %#v", toInputs)
	}
	if len(mapSpecs) != 0 {
		t.Fatalf("expected no map specs, got %v", mapSpecs)
	}
}

func TestApplyPlanParams(t *testing.T) {
	params := &planParams{
		Concurrency:    uintPtr(5),
		VerifyChecksum: boolPtr(true),
		Overwrite:      stringPtr("always"),
	}
	opts := FileTransferParamsOpts{}
	applyPlanParams(&opts, params)
	if opts.Concurrency != 5 {
		t.Fatalf("expected concurrency 5, got %d", opts.Concurrency)
	}
	if !opts.VerifyChecksum {
		t.Fatalf("expected verifyChecksum true")
	}
	if opts.Overwrite != "always" {
		t.Fatalf("expected overwrite always, got %q", opts.Overwrite)
	}
}

func uintPtr(v uint) *uint       { return &v }
func boolPtr(v bool) *bool       { return &v }
func stringPtr(v string) *string { return &v }
