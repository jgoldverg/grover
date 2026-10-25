package udpdataplane

import "testing"

func TestFlowControlWindowBytes(t *testing.T) {
	s := NewBBRSender()

	if got := s.flowControlWindowBytes(SendConfig{WindowBytes: 1234}, 100, 10); got != 1234 {
		t.Fatalf("window bytes override = %d, want 1234", got)
	}
	if got := s.flowControlWindowBytes(SendConfig{FlowControl: "fixed"}, 100, 10); got != 1000 {
		t.Fatalf("fixed packet window = %d, want 1000", got)
	}
	if got := s.flowControlWindowBytes(SendConfig{FlowControl: "bbr"}, 100, 10); got != s.minWindowBytes {
		t.Fatalf("bbr initial window = %d, want %d", got, s.minWindowBytes)
	}
}

func TestShouldPollAcks(t *testing.T) {
	if shouldPollAcks(1, 1, 4096) {
		t.Fatal("should not poll immediately")
	}
	if !shouldPollAcks(defaultAckPollEvery, defaultAckPollEvery, 4096) {
		t.Fatal("should poll after default poll interval")
	}
	if !shouldPollAcks(1, 4096, 4096) {
		t.Fatal("should poll near full packet window")
	}
}
