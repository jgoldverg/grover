package udpdataplane

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type timeoutRecordingTransport struct{ recordingTransport }

func (*timeoutRecordingTransport) ReadPacket([]byte) (int, *net.UDPAddr, error) {
	return 0, nil, timeoutErr{}
}

func TestFeedbackTimeoutPollAndCancellation(t *testing.T) {
	for _, mode := range []FeedbackMode{FeedbackNonBlocking, FeedbackUntilDrained} {
		transport := &timeoutRecordingTransport{}
		r := newSACKReliabilityController(SendConfig{Transport: transport, StreamID: 1}, nil)
		r.TrackSent(SentPacket{Sequence: 1, Data: []byte("packet"), PayloadBytes: 6})
		_, err := r.DrainFeedback(context.Background(), time.Millisecond, mode)
		if mode == FeedbackNonBlocking {
			if err != nil || len(transport.writes) != 0 {
				t.Fatalf("poll retransmitted or failed: %v", err)
			}
		} else if err == nil || len(transport.writes) != maxAckRetries-1 {
			t.Fatalf("retry count=%d error=%v", len(transport.writes), err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	transport := &timeoutRecordingTransport{}
	r := newSACKReliabilityController(SendConfig{Transport: transport}, nil)
	r.TrackSent(SentPacket{Sequence: 1})
	if _, err := r.DrainFeedback(ctx, time.Second, FeedbackUntilDrained); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if len(transport.writes) != 0 {
		t.Fatal("sent after cancellation")
	}
}
