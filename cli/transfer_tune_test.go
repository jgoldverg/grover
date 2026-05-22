package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestTransferTuneRequiresAppConfigWhenExecuting(t *testing.T) {
	cmd := SimpleCopy()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"tune", "transfer-123", "--concurrency", "8", "--parallel-streams", "4"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected transfer tune to require app config")
	}
	if !strings.Contains(err.Error(), "app config unavailable") {
		t.Fatalf("unexpected tune error: %v", err)
	}
}

func TestTransferTuneRequiresAtLeastOneTuningValue(t *testing.T) {
	cmd := TransferTuneCommand()
	cmd.SetArgs([]string{"transfer-123"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected missing tuning values error")
	}
	if !strings.Contains(err.Error(), "set at least one") {
		t.Fatalf("unexpected tune error: %v", err)
	}
}

func TestTransferStatusRequiresAppConfigWhenExecuting(t *testing.T) {
	cmd := TransferStatusCommand()
	cmd.SetArgs([]string{"transfer-123"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected transfer status to require app config")
	}
	if !strings.Contains(err.Error(), "app config unavailable") {
		t.Fatalf("unexpected status error: %v", err)
	}
}
