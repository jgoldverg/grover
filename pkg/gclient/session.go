package gclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/pool"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

// =======================================================
// udpConn / streamState: core data-plane per session
// =======================================================

type udpConn struct {
	// identity
	sessionID string
	token     string
	udpHost   string
	udpPort   uint32

	// data-plane
	streams map[uint32]*streamState // stream id -> state
	conn    *net.UDPConn
	mtu     int

	// lifecycle
	mu      sync.RWMutex
	ref     int32
	lastUse time.Time
	closed  atomic.Bool

	rxDone  chan struct{}
	txDone  chan struct{}
	bufPool *pool.BufferPool
}

type streamState struct {
	id uint32
	pw *io.PipeWriter
	pr *io.PipeReader

	mu sync.Mutex
}

// newSession constructs a udpConn and starts the recv loop.
// It assumes conn is already a dialed *net.UDPConn.
func newSession(
	sessionID, token, udpHost string,
	udpPort uint32,
	conn *net.UDPConn,
	mtu int,
	bufferSize uint64,
	streamIds []uint32,
) *udpConn {
	streams := make(map[uint32]*streamState, len(streamIds))

	for _, id := range streamIds {
		pr, pw := io.Pipe()
		streams[id] = &streamState{
			id: id,
			pr: pr,
			pw: pw,
		}
	}

	u := &udpConn{
		sessionID: sessionID,
		token:     token,
		udpHost:   udpHost,
		udpPort:   udpPort,

		streams: streams,

		conn:    conn,
		mtu:     mtu,
		ref:     1,
		lastUse: time.Now(),
		rxDone:  make(chan struct{}),
		txDone:  make(chan struct{}),
		bufPool: pool.NewBufferPool(bufferSize),
	}

	go u.recvLoop()

	return u
}

func (u *udpConn) Close() {
	// Make Close() idempotent.
	if !u.closed.CompareAndSwap(false, true) {
		return
	}

	_ = u.conn.Close()

	u.mu.Lock()
	for _, s := range u.streams {
		_ = s.pw.Close()
	}
	u.mu.Unlock()

	<-u.rxDone
}

func (u *udpConn) Write(ctx context.Context, r io.Reader, size int64, overwrite backend.OverwritePolicy) error {
	// Transport-level TX will live here later (packetization, SACK, etc.).
	return fmt.Errorf("udpConn.Write not implemented (transport TX path TODO)")
}

// Read currently only supports sessions with a single stream. For multi-stream
// sessions we need a proper merge/reassembler, so we fail fast instead of
// picking a default.
func (u *udpConn) Read(ctx context.Context) (io.ReadCloser, error) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if u.closed.Load() {
		return nil, fmt.Errorf("udp session closed")
	}
	if u.conn == nil {
		return nil, fmt.Errorf("nil UDP conn")
	}
	if u.bufPool == nil {
		return nil, fmt.Errorf("nil buffer pool")
	}

	var s *streamState
	for _, st := range u.streams {
		if s != nil {
			return nil, fmt.Errorf("Read() not supported for multi-stream sessions yet")
		}
		s = st
	}
	if s == nil {
		return nil, fmt.Errorf("no streams configured for session")
	}

	return s.pr, nil
}

func (u *udpConn) ReadAt(ctx context.Context, offset int64, p []byte) (int, error) {
	// Random access isn't a transport concern. Session layer does this.
	return 0, fmt.Errorf("udpConn.ReadAt not implemented at transport level")
}

func (u *udpConn) recvLoop() {
	defer close(u.rxDone)

	buf := u.bufPool.GetBuffer()
	defer u.bufPool.PutBuffer(buf)

	var dp udpwire.DataPacket
	var sp udpwire.StatusPacket

	for {
		n, _, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			// propagate error to stream readers
			u.mu.RLock()
			for _, s := range u.streams {
				_ = s.pw.CloseWithError(err)
			}
			u.mu.RUnlock()
			return
		}

		if n == 0 {
			continue
		}

		packet := buf[:n]
		kind, ok := udpwire.PeekKind(packet)
		if !ok {
			continue
		}

		switch kind {
		case udpwire.KindData:
			if _, err := dp.Decode(packet); err != nil {
				continue
			}

			// Core demux: route by StreamID to the right streamState.
			u.mu.RLock()
			s := u.streams[dp.StreamID]
			u.mu.RUnlock()

			if s == nil {
				// unknown stream for this session; drop packet
				continue
			}

			if len(dp.Payload) > 0 {
				if _, err := s.pw.Write(dp.Payload); err != nil {
					// reader closed; ignore further data for this stream
					continue
				}
			}

		case udpwire.KindStatus:
			if _, err := sp.Decode(packet); err != nil {
				continue
			}
			// TODO: hook into tx-side SACK/ACK logic.

		default:
			// ignore unknown kinds
		}

		u.mu.Lock()
		u.lastUse = time.Now()
		u.mu.Unlock()
	}
}

// =======================================================
// SessionParams + HELLO
// =======================================================

type SessionParams struct {
	// SessionID is a printable key for the sessionManager map.
	SessionID string

	// Raw session/token as returned by gRPC.
	SessionIDRaw []byte
	Token        string

	UDPHost    string
	UDPPort    uint32
	MTU        int
	BufferSize uint64
	StreamIDs  []uint32
}

const (
	helloMagic   = "GRVR" // 4-byte magic
	helloVersion = 1      // protocol version for HELLO
)

// sendHello sends a simple HELLO packet over conn to bind this UDP 5-tuple to
// (sessionID, token) on the server side.
func sendHello(ctx context.Context, conn *net.UDPConn, sessionID []byte, token []byte) error {
	var buf bytes.Buffer

	// Magic + version
	_, _ = buf.WriteString(helloMagic)
	_ = buf.WriteByte(helloVersion)

	// Session ID length + bytes (uint8)
	if len(sessionID) > 255 {
		return fmt.Errorf("sessionID too long for hello packet: %d", len(sessionID))
	}
	_ = buf.WriteByte(byte(len(sessionID)))
	_, _ = buf.Write(sessionID)

	// Token length + bytes (uint16)
	if len(token) > 0xFFFF {
		return fmt.Errorf("token too long for hello packet: %d", len(token))
	}
	var tokLen [2]byte
	binary.BigEndian.PutUint16(tokLen[:], uint16(len(token)))
	_, _ = buf.Write(tokLen[:])
	_, _ = buf.Write(token)

	// Respect ctx deadline if present
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
		defer conn.SetWriteDeadline(time.Time{})
	}

	_, err := conn.Write(buf.Bytes())
	return err
}

// =======================================================
// udpSession: per-file logical Session implementation
// =======================================================

type udpSession struct {
	conn     *udpConn
	streamID uint32
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
	// TODO: implement packetization + send via s.conn.
	return fmt.Errorf("udpSession.Write not implemented yet")
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
	if s.conn != nil {
		s.conn.Close()
	}
}

// Ensure udpSession satisfies the high-level Session interface.
var _ Session = (*udpSession)(nil)

// =======================================================
// sessionManager
// =======================================================

type sessionManager struct {
	mu        sync.RWMutex
	sessions  map[string]*udpConn
	ttl       time.Duration
	scanEvery time.Duration
	stopCh    chan struct{}
	wg        sync.WaitGroup

	stopOnce sync.Once
}

func newSessionManager(ttl, scanEvery time.Duration) *sessionManager {
	sm := &sessionManager{
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
func (m *sessionManager) Open(ctx context.Context, p SessionParams) (*udpConn, error) {
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
		p.Token,
		p.UDPHost,
		p.UDPPort,
		udpConn,
		p.MTU,
		p.BufferSize,
		p.StreamIDs,
	)

	m.mu.Lock()
	m.sessions[p.SessionID] = sess
	m.mu.Unlock()

	return sess, nil
}

// Get returns an existing session by ID, if present.
func (m *sessionManager) Get(sessionID string) (*udpConn, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sess, ok := m.sessions[sessionID]
	return sess, ok
}

// Release closes and removes the given session from the manager.
func (m *sessionManager) Release(s *udpConn) {
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
func (m *sessionManager) janitor() {
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
func (m *sessionManager) Stop() {
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
