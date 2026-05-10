package gclient

import (
	"context"
	"testing"
	"time"
)

func TestDiscoverPMTURejectsInvalidRange(t *testing.T) {
	_, err := NewPMTUService().DiscoverPMTU(context.Background(), "127.0.0.1", 1, 2000, 1200, time.Millisecond)
	if err == nil {
		t.Fatal("expected invalid range error")
	}
}

func TestDiscoverPMTURequiresPort(t *testing.T) {
	_, err := NewPMTUService().DiscoverPMTU(context.Background(), "127.0.0.1", 0, 1200, 1500, time.Millisecond)
	if err == nil {
		t.Fatal("expected missing port error")
	}
}
