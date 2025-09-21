package udpwire

import (
	"testing"
	"time"
)

func TestBuildParseMtuProbe(t *testing.T) {
	token := uint64(0xDEADBEEFCAFEBABE)
	probeID := uint32(42)
	size := 1200

	pkt, err := BuildMtuProbe(token, probeID, size)
	if err != nil {
		t.Fatalf("BuildMtuProbe failed: %v", err)
	}
	if got := len(pkt); got != size {
		t.Fatalf("expected payload size %d, got %d", size, got)
	}

	gotToken, gotProbeID, gotTarget, txTime, pad, err := ParseMtuProbe(pkt)
	if err != nil {
		t.Fatalf("ParseMtuProbe failed: %v", err)
	}
	if gotToken != token || gotProbeID != probeID {
		t.Fatalf("unexpected token/probeID: got (%d,%d) want (%d,%d)", gotToken, gotProbeID, token, probeID)
	}
	if gotTarget != size {
		t.Fatalf("unexpected target size: got %d want %d", gotTarget, size)
	}
	if pad != size-minProbeLen {
		t.Fatalf("unexpected padding: got %d want %d", pad, size-minProbeLen)
	}
	if time.Since(txTime) > time.Second {
		t.Fatalf("txTime too old: %v", txTime)
	}
}

func TestBuildParseMtuProbeAck(t *testing.T) {
	token := uint64(1)
	probeID := uint32(2)
	observed := 1500

	ack, err := BuildMtuProbeAck(token, probeID, observed)
	if err != nil {
		t.Fatalf("BuildMtuProbeAck failed: %v", err)
	}
	if len(ack) != ackLen {
		t.Fatalf("expected ack len %d, got %d", ackLen, len(ack))
	}

	gotToken, gotProbeID, gotObserved, rxTime, err := ParseMtuProbeAck(ack)
	if err != nil {
		t.Fatalf("ParseMtuProbeAck failed: %v", err)
	}
	if gotToken != token || gotProbeID != probeID || gotObserved != observed {
		t.Fatalf("unexpected ack fields: got (%d,%d,%d) want (%d,%d,%d)", gotToken, gotProbeID, gotObserved, token, probeID, observed)
	}
	if time.Since(rxTime) > time.Second {
		t.Fatalf("rxTime too old: %v", rxTime)
	}
}

func TestProbeGuards(t *testing.T) {
	if IsMtuProbeAck(make([]byte, ackLen-1)) {
		t.Fatalf("short ack should not be recognised")
	}
	if IsMtuProbe(make([]byte, minProbeLen-1)) {
		t.Fatalf("short probe should not be recognised")
	}
}
