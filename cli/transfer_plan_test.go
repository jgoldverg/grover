package cli

import "testing"

func TestNewTransferPlan_DefaultEdges(t *testing.T) {
	opts := TransferCommandOpts{TransferParams: defaultTransferOptions().TransferParams}
	plan, err := newTransferPlan(
		&opts,
		[]endpointInput{{Label: "src", URI: "file:///data/input.csv"}},
		[]endpointInput{{Label: "dst", URI: "file:///data/output.csv"}},
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan.Request.Edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(plan.Request.Edges))
	}
	if !plan.legacyCompatible() {
		t.Fatalf("expected plan to be legacy compatible")
	}
	if !plan.usedDefaultEdges() {
		t.Fatalf("expected default mapping to be used")
	}
	edge := plan.Request.Edges[0]
	src, err := plan.sourcePath(edge)
	if err != nil {
		t.Fatalf("source path error: %v", err)
	}
	dst, err := plan.destPath(edge)
	if err != nil {
		t.Fatalf("dest path error: %v", err)
	}
	if src != "/data/input.csv" {
		t.Fatalf("unexpected source path %q", src)
	}
	if dst != "/data/output.csv" {
		t.Fatalf("unexpected dest path %q", dst)
	}
}

func TestNewTransferPlan_WithMappings(t *testing.T) {
	opts := TransferCommandOpts{TransferParams: defaultTransferOptions().TransferParams}
	plan, err := newTransferPlan(
		&opts,
		[]endpointInput{{Label: "a", URI: "file:///var/a.csv"}, {Label: "b", URI: "file:///var/b.csv"}},
		[]endpointInput{{Label: "remote", URI: "sftp://host/incoming/"}},
		[]string{"a remote:/data/a.csv", "b remote:/data/b.csv"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan.Request.Edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(plan.Request.Edges))
	}
	if plan.legacyCompatible() {
		t.Fatalf("plan with multiple edges should not be legacy compatible")
	}
	if plan.usedDefaultEdges() {
		t.Fatalf("explicit mappings should disable default edges")
	}
	src0, err := plan.sourcePath(plan.Request.Edges[0])
	if err != nil {
		t.Fatalf("source path error: %v", err)
	}
	if src0 != "/var/a.csv" {
		t.Fatalf("unexpected source path: %q", src0)
	}
	dst0, err := plan.destPath(plan.Request.Edges[0])
	if err != nil {
		t.Fatalf("dest path error: %v", err)
	}
	if dst0 != "/data/a.csv" {
		t.Fatalf("unexpected dest path: %q", dst0)
	}
}

func TestNewTransferPlan_InvalidReference(t *testing.T) {
	opts := TransferCommandOpts{TransferParams: defaultTransferOptions().TransferParams}
	_, err := newTransferPlan(
		&opts,
		[]endpointInput{{Label: "src", URI: "file:///tmp/src.txt"}},
		[]endpointInput{{Label: "dst", URI: "file:///tmp/dst.txt"}},
		[]string{"unknown dst"},
	)
	if err == nil {
		t.Fatalf("expected error for unknown source reference")
	}
}

func TestSplitLabelAndURI(t *testing.T) {
	label, uri := splitLabelAndURI("foo=file:///tmp/foo.txt")
	if label != "foo" || uri != "file:///tmp/foo.txt" {
		t.Fatalf("unexpected split results %q, %q", label, uri)
	}
	label, uri = splitLabelAndURI("file:///tmp/foo.txt")
	if label != "" || uri != "file:///tmp/foo.txt" {
		t.Fatalf("unexpected split results without label: %q, %q", label, uri)
	}
}

func TestSplitEndpointRef(t *testing.T) {
	base, override := splitEndpointRef("source:/custom")
	if base != "source" || override != "/custom" {
		t.Fatalf("unexpected ref split: %q, %q", base, override)
	}
	base, override = splitEndpointRef("file:///tmp/foo.txt")
	if base != "file:///tmp/foo.txt" || override != "" {
		t.Fatalf("unexpected ref split for URI: %q, %q", base, override)
	}
}
