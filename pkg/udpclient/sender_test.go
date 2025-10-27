package udpclient

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestSenderUploadBytesRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	receiverConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen receiver: %v", err)
	}
	defer receiverConn.Close()

	senderConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen sender: %v", err)
	}
	defer senderConn.Close()

	data := []byte("hello over udp")
	chunkSize := 4
	sessionID := uint32(1)
	streamID := uint32(1)

	receiver := NewReceiver(receiverConn, SessionParams{
		SessionID:   sessionID,
		RemoteAddr:  senderConn.LocalAddr().(*net.UDPAddr),
		AckInterval: 50 * time.Millisecond,
	})

	bufSink := NewBufferSink(int64(len(data)))
	if err := receiver.RegisterStream(streamID, 0, int64(len(data)), chunkSize, bufSink.Callbacks()); err != nil {
		t.Fatalf("register stream: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		if runErr := receiver.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
			errCh <- runErr
		}
	}()

	sender := NewSender(senderConn, SessionParams{
		SessionID:   sessionID,
		RemoteAddr:  receiverConn.LocalAddr().(*net.UDPAddr),
		ChunkSize:   chunkSize,
		WindowSize:  4,
		RetryAfter:  50 * time.Millisecond,
		AckInterval: 50 * time.Millisecond,
	})

	if err := sender.UploadBytes(ctx, StreamPlan{
		StreamID:    streamID,
		SizeBytes:   int64(len(data)),
		StartOffset: 0,
	}, data); err != nil {
		t.Fatalf("UploadBytes: %v", err)
	}

	if err := receiver.Wait(ctx, streamID); err != nil {
		t.Fatalf("receiver wait: %v", err)
	}

	cancel()
	select {
	case runErr := <-errCh:
		t.Fatalf("receiver run error: %v", runErr)
	case <-time.After(50 * time.Millisecond):
	}

	got := bufSink.Bytes()
	if !bytes.Equal(got, data) {
		t.Fatalf("received data mismatch: got %q want %q", got, data)
	}
}
