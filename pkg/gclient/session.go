package gclient

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

// SessionParams + HELLO

type SessionParams struct {
	// SessionID is a printable key for the ClientSessions map.
	SessionID string

	// Raw session/token as returned by gRPC.
	SessionIDRaw []byte
	Token        string

	UDPHost           string
	UDPPort           uint32
	MTU               int
	BufferSize        uint64
	StreamIDs         []uint32
	MaxInFlightBytes  int
	LinkBandwidthMbps int
	TargetLossPercent int
}

// sendHello sends a simple HELLO packet over conn to bind this UDP 5-tuple to
// (sessionID, token) on the server side.
func sendHello(ctx context.Context, conn *net.UDPConn, sessionID []byte, token []byte) error {
	totalLen := len(udpwire.HelloMagic) + 1 + 1 + len(sessionID) + 2 + len(token)
	tmp := make([]byte, totalLen)
	hp := udpwire.HelloPacket{
		SessionID: sessionID,
		Token:     token,
	}
	n, err := hp.Encode(tmp)
	if err != nil {
		return err
	}

	remoteAddr := ""
	if conn != nil && conn.RemoteAddr() != nil {
		remoteAddr = conn.RemoteAddr().String()
	}
	fields := internal.Fields{
		"session_id":  fmt.Sprintf("%x", sessionID),
		"token_len":   len(token),
		"hello_bytes": n,
		"remote_addr": remoteAddr,
	}
	internal.Info("sending udp hello", fields)

	// Respect ctx deadline if present
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
		defer conn.SetWriteDeadline(time.Time{})
	}

	if _, err := conn.Write(tmp[:n]); err != nil {
		errFields := internal.Fields{
			"session_id":        fields["session_id"],
			"token_len":         len(token),
			"hello_bytes":       n,
			"remote_addr":       remoteAddr,
			internal.FieldError: err.Error(),
		}
		internal.Error("failed to send udp hello", errFields)
		return err
	}
	internal.Info("udp hello sent", fields)
	return nil
}

// udpSession: per-file logical Session implementation

type udpSession struct {
	conn      *udpConn
	streamID  uint32
	onClose   func()
	closeOnce sync.Once

	sessionID []byte
	leaseID   []byte
	control   pb.TransferControlClient

	releaseOnce sync.Once
	releaseErr  error
}

// Read returns an io.ReadCloser for this session's stream.
// This is single-stream only for now; multi-stream reassembly would
// live here later.
func (s *udpSession) Read(ctx context.Context) (io.ReadCloser, error) {
	s.conn.mu.RLock()
	st := s.conn.streams[s.streamID]
	s.conn.mu.RUnlock()

	if st == nil {
		return nil, fmt.Errorf("stream %d not found for session %s", s.streamID, s.conn.sessionID)
	}
	return st.pr, nil
}

func (s *udpSession) Write(ctx context.Context, r io.Reader, size int64, overwrite backend.OverwritePolicy) error {
	if s.conn == nil {
		return fmt.Errorf("udp session closed")
	}

	return s.conn.writeStream(ctx, s.streamID, r, size, overwrite)
}

func (s *udpSession) ReadAt(ctx context.Context, offset int64, p []byte) (int, error) {
	// TODO: random access via range requests / seekable protocol.
	return 0, fmt.Errorf("udpSession.ReadAt not implemented yet")
}

func (s *udpSession) WriteAt(ctx context.Context, offset int64, p []byte) (int, error) {
	// TODO: random access write via protocol extensions.
	return 0, fmt.Errorf("udpSession.WriteAt not implemented yet")
}

func (s *udpSession) Close() {
	s.closeOnce.Do(func() {
		if s.control != nil {
			_ = s.release(context.Background(), false, 0)
		}
		if s.onClose != nil {
			s.onClose()
		}
		if s.conn != nil {
			s.conn.Close()
		}
	})
}

func (s *udpSession) release(ctx context.Context, commit bool, bytes uint64) error {
	if s.control == nil || len(s.sessionID) == 0 || len(s.leaseID) == 0 {
		return nil
	}
	var err error
	s.releaseOnce.Do(func() {
		req := &pb.ReleaseStreamRequest{
			SessionId:        append([]byte(nil), s.sessionID...),
			StreamId:         s.streamID,
			LeaseId:          append([]byte(nil), s.leaseID...),
			Commit:           commit,
			BytesTransferred: bytes,
		}
		_, err = s.control.ReleaseStream(ctx, req)
		if err != nil {
			s.releaseErr = err
		}
	})
	if err == nil {
		err = s.releaseErr
	}
	return err
}

// Ensure udpSession satisfies the high-level Session interface.
var _ Session = (*udpSession)(nil)

// ClientSessions

type ClientSessions struct {
	mu        sync.RWMutex
	sessions  map[string]*udpConn
	ttl       time.Duration
	scanEvery time.Duration
	stopCh    chan struct{}
	wg        sync.WaitGroup

	stopOnce sync.Once
}

func newClientSessions(ttl, scanEvery time.Duration) *ClientSessions {
	sm := &ClientSessions{
		sessions:  make(map[string]*udpConn),
		ttl:       ttl,
		scanEvery: scanEvery,
		stopCh:    make(chan struct{}),
	}
	sm.wg.Add(1)
	go sm.janitor()
	return sm
}

// Open either returns an existing session for p.SessionID or dials a new UDP
// conn, sends HELLO, and constructs a udpConn.
func (m *ClientSessions) Open(ctx context.Context, p SessionParams) (*udpConn, error) {
	// Fast path: already have this session
	m.mu.RLock()
	if sess, ok := m.sessions[p.SessionID]; ok {
		m.mu.RUnlock()
		return sess, nil
	}
	m.mu.RUnlock()

	// Resolve and dial UDP
	addrStr := net.JoinHostPort(p.UDPHost, fmt.Sprint(p.UDPPort))
	udpAddr, err := net.ResolveUDPAddr("udp", addrStr)
	if err != nil {
		return nil, err
	}

	udpConn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}

	// HELLO: bind this UDP 5-tuple on the server to (sessionID, token)
	if err := sendHello(ctx, udpConn, p.SessionIDRaw, []byte(p.Token)); err != nil {
		udpConn.Close()
		return nil, fmt.Errorf("sending hello: %w", err)
	}

	sess := newSession(
		p.SessionID,
		p.SessionIDRaw,
		p.Token,
		p.UDPHost,
		p.UDPPort,
		udpConn,
		p.MTU,
		p.BufferSize,
		p.StreamIDs,
		p.MaxInFlightBytes,
		p.LinkBandwidthMbps,
		p.TargetLossPercent,
	)

	m.mu.Lock()
	m.sessions[p.SessionID] = sess
	m.mu.Unlock()

	return sess, nil
}

// Get returns an existing session by ID, if present.
func (m *ClientSessions) Get(sessionID string) (*udpConn, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sess, ok := m.sessions[sessionID]
	return sess, ok
}

// Release closes and removes the given session from the manager.
func (m *ClientSessions) Release(s *udpConn) {
	m.mu.Lock()
	sess, ok := m.sessions[s.sessionID]
	if !ok {
		m.mu.Unlock()
		return
	}
	sess.Close()
	delete(m.sessions, s.sessionID)
	m.mu.Unlock()
}

// janitor periodically evicts idle / closed sessions based on ttl.
func (m *ClientSessions) janitor() {
	defer m.wg.Done()

	if m.scanEvery <= 0 {
		return
	}

	ticker := time.NewTicker(m.scanEvery)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			now := time.Now()

			m.mu.Lock()
			for id, sess := range m.sessions {
				sess.mu.RLock()
				lastUse := sess.lastUse
				closed := sess.closed.Load()
				ref := atomic.LoadInt32(&sess.ref)
				sess.mu.RUnlock()

				if closed || (m.ttl > 0 && now.Sub(lastUse) > m.ttl && ref == 0) {
					sess.Close()
					delete(m.sessions, id)
				}
			}
			m.mu.Unlock()
		}
	}
}

// Stop stops the janitor and closes all remaining sessions.
func (m *ClientSessions) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		m.wg.Wait()

		m.mu.Lock()
		for _, sess := range m.sessions {
			sess.Close()
		}
		m.sessions = make(map[string]*udpConn)
		m.mu.Unlock()
	})
}
