package gclient

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestNewSessionManager(t *testing.T) {
	ttl := 5 * time.Second
	scan := 500 * time.Millisecond
	sm := newSessionManager(ttl, scan)

	t.Cleanup(sm.Stop)

	if sm == nil {
		t.Fatalf("NewSessionManager returned nil")
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
	sm := newSessionManager(ttl, scan)

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
		SessionID:  sessionID,
		Token:      token,
		UDPHost:    udpHost,
		UDPPort:    uint32(udpPort),
		MTU:        mtu,
		BufferSize: bufferSize,
		StreamIDs:  streamIds,
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
				SessionID:  "sess-" + strconv.Itoa(i),
				Token:      "tok",
				UDPHost:    "127.0.0.1",
				UDPPort:    9000,
				MTU:        1400,
				BufferSize: 32 * 1024,
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
	sm := newSessionManager(ttl, scan)
	t.Cleanup(sm.Stop)

	sp := SessionParams{
		SessionID:  "sess-" + strconv.Itoa(0),
		Token:      "tok",
		UDPHost:    "127.0.0.1",
		UDPPort:    9000,
		MTU:        1400,
		BufferSize: 32 * 1024,
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
				SessionID:  "sess-" + strconv.Itoa(i),
				Token:      "tok",
				UDPHost:    "127.0.0.1",
				UDPPort:    9000,
				MTU:        1400,
				BufferSize: 32 * 1024,
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
	sm := newSessionManager(ttl, scan)

	sp := SessionParams{
		SessionID:  "sess-" + strconv.Itoa(0),
		Token:      "tok",
		UDPHost:    "127.0.0.1",
		UDPPort:    9000,
		MTU:        1400,
		BufferSize: 32 * 1024,
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
