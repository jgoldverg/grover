package gclient

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jgoldverg/grover/pkg/udpwire"
)

func TestNewClientSessions(t *testing.T) {
	ttl := 5 * time.Second
	scan := 500 * time.Millisecond
	sm := newClientSessions(ttl, scan)

	t.Cleanup(sm.Stop)

	if sm == nil {
		t.Fatalf("NewClientSessions returned nil")
	}

	if sm.sessions == nil {
		t.Fatalf("sessions map is nil")
	}

	if sm.ttl != ttl {
		t.Fatalf("ttl mismatch: got %v want %v", sm.ttl, ttl)
	}
	if sm.scanEvery != scan {
		t.Fatalf("scanEvery mismatch: got %v want %v", sm.scanEvery, scan)
	}
	if sm.stopCh == nil {
		t.Fatalf("stopCh is nil")
	}
}

func TestCreateNewSession(t *testing.T) {
	ttl := 5 * time.Second
	scan := 500 * time.Millisecond
	sm := newClientSessions(ttl, scan)

	t.Cleanup(sm.Stop)
	sessionID := "s1"
	token := "token"
	udpHost := "127.0.0.1"
	udpPort := 7777
	mtu := 1500
	bufferSize := uint64(64 * 1024)
	streamIds := make([]uint32, 0)
	streamIds = append(streamIds, 1)
	sp := SessionParams{
		SessionID:         sessionID,
		Token:             token,
		UDPHost:           udpHost,
		UDPPort:           uint32(udpPort),
		MTU:               mtu,
		BufferSize:        bufferSize,
		StreamIDs:         streamIds,
		MaxInFlightBytes:  int(bufferSize),
		LinkBandwidthMbps: 0,
		TargetLossPercent: 1,
	}

	udpSession, err := sm.Open(t.Context(), sp)
	if err != nil {
		t.Fatalf("Create returned error %v:", err)
	}

	if udpSession == nil {
		t.Fatal("Create returned a nil udp session")
	}
	sm.mu.RLock()
	sess, ok := sm.sessions[sessionID]
	sm.mu.RUnlock()
	if !ok {
		t.Fatalf("create new session never stored the proposed session in the map")
	}

	if sess != udpSession {
		t.Fatalf("the session created in session manager does not match what we read from the session map directly")
	}

	const N = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(N)

	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			sp := SessionParams{
				SessionID:         "sess-" + strconv.Itoa(i),
				Token:             "tok",
				UDPHost:           "127.0.0.1",
				UDPPort:           9000,
				MTU:               1400,
				BufferSize:        32 * 1024,
				MaxInFlightBytes:  32 * 1024,
				LinkBandwidthMbps: 0,
				TargetLossPercent: 1,
			}
			_, err := sm.Open(t.Context(), sp)
			if err != nil {
				t.Errorf("Create error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	sm.mu.RLock()
	got := len(sm.sessions)
	sm.mu.RUnlock()
	if got != N+1 {
		t.Fatalf("expected %d sessions, got %d", N, got)
	}
}

func TestReleaseSessions(t *testing.T) {
	ttl := 5 * time.Second
	scan := 500 * time.Millisecond
	sm := newClientSessions(ttl, scan)
	t.Cleanup(sm.Stop)

	sp := SessionParams{
		SessionID:         "sess-" + strconv.Itoa(0),
		Token:             "tok",
		UDPHost:           "127.0.0.1",
		UDPPort:           9000,
		MTU:               1400,
		BufferSize:        32 * 1024,
		MaxInFlightBytes:  32 * 1024,
		LinkBandwidthMbps: 0,
		TargetLossPercent: 1,
	}
	udpSession, err := sm.Open(t.Context(), sp)
	if err != nil {
		t.Fatalf("failed to create a sessionin session manager %v", err)
	}
	sm.mu.RLock()
	if len(sm.sessions) != 1 {
		t.Fatalf("added one session, but the length of the map is not one")
	}
	sm.mu.RUnlock()
	sm.Release(udpSession)
	sm.mu.RLock()
	if len(sm.sessions) != 0 {
		t.Fatalf("releasing a session did not reduce the sessions size")
	}
	sm.mu.RUnlock()

	const N = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(N)

	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			sp := SessionParams{
				SessionID:         "sess-" + strconv.Itoa(i),
				Token:             "tok",
				UDPHost:           "127.0.0.1",
				UDPPort:           9000,
				MTU:               1400,
				BufferSize:        32 * 1024,
				MaxInFlightBytes:  32 * 1024,
				LinkBandwidthMbps: 0,
				TargetLossPercent: 1,
			}
			sess, err := sm.Open(t.Context(), sp)
			if err != nil {
				t.Errorf("failed to create parallel udp sessions:%v", err)
			}
			sm.Release(sess)
		}()
	}
	sm.mu.RLock()
	if len(sm.sessions) != 0 {
		t.Fatalf("should have released all udp session, length is not 0")
	}
	sm.mu.RUnlock()
}

func TestStopSm(t *testing.T) {
	ttl := 5 * time.Second
	scan := 500 * time.Millisecond
	sm := newClientSessions(ttl, scan)

	sp := SessionParams{
		SessionID:         "sess-" + strconv.Itoa(0),
		Token:             "tok",
		UDPHost:           "127.0.0.1",
		UDPPort:           9000,
		MTU:               1400,
		BufferSize:        32 * 1024,
		MaxInFlightBytes:  32 * 1024,
		LinkBandwidthMbps: 0,
		TargetLossPercent: 1,
	}
	_, err := sm.Open(t.Context(), sp)
	if err != nil {
		t.Fatalf("failed to create a sessionin session manager %v", err)
	}
	sm.Stop()
	sm.mu.RLock()
	if len(sm.sessions) != 0 {
		t.Fatalf("failed to clear our the udp sessions")
	}
	sm.mu.RUnlock()
}

func TestTxWindowReserveRelease(t *testing.T) {
	tw := newTxWindow(8, 32, defaultLossRatio)
	if err := tw.reserve(context.Background(), 1, 8); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- tw.reserve(context.Background(), 2, 4)
	}()

	select {
	case err := <-done:
		t.Fatalf("reserve should have blocked, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	tw.releaseThrough(1)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reserve returned error after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("reserve did not unblock after release")
	}
}

func TestTxWindowClose(t *testing.T) {
	tw := newTxWindow(4, 16, defaultLossRatio)
	if err := tw.reserve(context.Background(), 1, 4); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		done <- tw.reserve(ctx, 2, 4)
	}()

	select {
	case err := <-done:
		t.Fatalf("reserve should have blocked, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	tw.close(errSessionClosed)

	select {
	case err := <-done:
		if err == nil || (!errors.Is(err, errTxWindowClosed) && !errors.Is(err, errSessionClosed)) {
			t.Fatalf("expected tx window closed error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("reserve did not unblock after close")
	}
}

func TestTxWindowSackRelease(t *testing.T) {
	tw := newTxWindow(100, 200, defaultLossRatio)
	for i := 1; i <= 4; i++ {
		if err := tw.reserve(context.Background(), uint32(i), 10); err != nil {
			t.Fatalf("reserve %d failed: %v", i, err)
		}
	}

	tw.releaseRanges([]udpwire.SackRange{
		{Start: 3, End: 4},
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tw.reserve(ctx, 5, 40); err != nil {
		t.Fatalf("reserve after SACK release failed: %v", err)
	}
}
