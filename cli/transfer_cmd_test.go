package cli

import "testing"

func TestBuildPlanTransferRequestSingleEdge(t *testing.T) {
	opts := TransferCommandOpts{
		SourceCredID:   "source-cred",
		DestCredID:     "dest-cred",
		TransferParams: defaultTransferOptions().TransferParams,
	}
	plan, err := newTransferPlan(
		&opts,
		[]endpointInput{{Label: "src", URI: "file:///tmp/src.txt"}},
		[]endpointInput{{Label: "dst", URI: "file:///tmp/dst.txt"}},
		[]string{"src dst"},
	)
	if err != nil {
		t.Fatalf("plan build: %v", err)
	}
	if len(plan.Request.Edges) != 1 {
		t.Fatalf("expected single edge, got %d", len(plan.Request.Edges))
	}
	edge := plan.Request.Edges[0]
	if edge.SourceIndex != 0 || edge.DestIndex != 0 {
		t.Fatalf("unexpected edge indexes: %+v", edge)
	}
}

func TestBuildPlanTransferRequestMultipleEdges(t *testing.T) {
	opts := TransferCommandOpts{TransferParams: defaultTransferOptions().TransferParams}
	plan, err := newTransferPlan(
		&opts,
		[]endpointInput{{Label: "a", URI: "file:///tmp/a"}, {Label: "b", URI: "file:///tmp/b"}},
		[]endpointInput{{Label: "dst", URI: "file:///tmp/dst"}},
		[]string{"a dst:/out/a", "b dst:/out/b"},
	)
	if err != nil {
		t.Fatalf("plan build: %v", err)
	}
	if len(plan.Request.Edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(plan.Request.Edges))
	}
}
