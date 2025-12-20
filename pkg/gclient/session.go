package gclient

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/pool"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

// =======================================================
// udpConn / streamState: core data-plane per session
// =======================================================

type udpConn struct {
	// identity
	sessionID  string
	sessionKey uint32
	token      string
	udpHost    string
	udpPort    uint32

	// data-plane
	streams map[uint32]*streamState // stream id -> state
	conn    *net.UDPConn
	mtu     int

	// lifecycle
	mu      sync.RWMutex
	ref     int32
	lastUse time.Time
	closed  atomic.Bool

	rb      *RingBuffer
	rxDone  chan struct{}
	txDone  chan struct{}
	bufPool *pool.BufferPool
}

type streamState struct {
	id uint32
	pw *io.PipeWriter
	pr *io.PipeReader
}

// newSession constructs a udpConn and starts the recv loop.
// It assumes conn is already a dialed *net.UDPConn.
func newSession(
	sessionID string,
	sessionIDRaw []byte,
	token string,
	udpHost string,
	udpPort uint32,
	conn *net.UDPConn,
	mtu int,
	bufferSize uint64,
	streamIDs []uint32,
) *udpConn {
	streams := make(map[uint32]*streamState, len(streamIDs))

	for _, id := range streamIDs {
		pr, pw := io.Pipe()
		streams[id] = &streamState{
			id: id,
			pr: pr,
			pw: pw,
		}
	}

	var sessionKey uint32
	if len(sessionIDRaw) >= 4 {
		sessionKey = binary.BigEndian.Uint32(sessionIDRaw[:4])
	}

	u := &udpConn{
		sessionID:  sessionID,
		sessionKey: sessionKey,
		token:      token,
		udpHost:    udpHost,
		udpPort:    udpPort,

		streams: streams,

		conn:    conn,
		mtu:     mtu,
		ref:     1,
		lastUse: time.Now(),
		rxDone:  make(chan struct{}),
		txDone:  make(chan struct{}),
		bufPool: pool.NewBufferPool(bufferSize),
		rb:      NewRingBuffer(64),
	}

	go u.recvLoop()
	go u.txLoop(context.Background())
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
	u.rb.Close()
	<-u.rxDone
}

func (u *udpConn) payloadCapacity() int {
	payload := u.mtu - udpwire.DataHeaderLen - 4
	if payload <= 0 {
		return 1024
	}
	return payload
}

func (u *udpConn) writePacket(ctx context.Context, packet []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = u.conn.SetWriteDeadline(deadline)
		defer u.conn.SetWriteDeadline(time.Time{})
	}
	_, err := u.conn.Write(packet)
	return err
}

func (u *udpConn) touch() {
	u.mu.Lock()
	u.lastUse = time.Now()
	u.mu.Unlock()
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

func (u *udpConn) txLoop(ctx context.Context) {
	defer close(u.txDone)
	scratch := make([]byte, udpwire.DataHeaderLen+u.payloadCapacity()+4)
	for {
		pkt, err := u.rb.Dequeue(ctx)
		if err != nil {
			internal.Info("error inside of txLoop", internal.Fields{
				"error": err.Error(),
			})
			if errors.Is(err, io.EOF) {
				return
			}
			_ = u.conn.Close()
			return
		}
		dp := udpwire.DataPacket{
			SessionID: u.sessionKey,
			StreamID:  pkt.streamID,
			Seq:       pkt.seq,
			Offset:    pkt.offset,
			Payload:   pkt.payload,
		}
		internal.Info("created packet with properties", internal.Fields{
			"data_packet": dp,
		})
		n, err := dp.Encode(scratch)
		if err != nil {
			continue
		}
		if err := u.writePacket(ctx, scratch[:n]); err != nil {
			_ = u.conn.Close()
			return
		}
		u.touch()
	}
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
	// SessionID is a printable key for the ClientSessions map.
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

// =======================================================
// udpSession: per-file logical Session implementation
// =======================================================

type txPacket struct {
	payload  []byte
	seq      uint32
	offset   uint64
	streamID uint32
}

type udpSession struct {
	conn      *udpConn
	streamID  uint32
	onClose   func()
	closeOnce sync.Once
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

	payload := make([]byte, s.conn.payloadCapacity())
	var seq uint32
	var offset uint64

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// io.Reader here is a file. We may consider a network socket as well but that is strictly sequential io, which kinda sucks.
		n, readErr := r.Read(payload)
		if n > 0 {
			seq++
			chunk := append([]byte(nil), payload[:n]...)
			pkt := txPacket{
				streamID: s.streamID,
				seq:      seq,
				offset:   offset,
				payload:  chunk,
			}
			internal.Info("UdpSession.Write called with fields: ", internal.Fields{
				"pkt.streamID":       s.streamID,
				"pkt.seq":            seq,
				"pkt.offset":         offset,
				"pkt.payload.length": len(payload[:n]),
			})
			if err := s.conn.rb.Enqueue(ctx, pkt); err != nil {
				internal.Error("failed to enqueue pkt", nil)
				return err
			}
			offset += uint64(n)
			s.conn.touch()
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return readErr
		}
	}

	s.conn.rb.Close()
	<-s.conn.txDone
	return nil
}

func (s *udpSession) ReadAt(ctx context.Context, offset int64, p []byte) (int, error) {
	// TODO: random access via range requests / seekable protocol.
	return 0, fmt.Errorf("udpSession.ReadAt not implemented yet")
}

func (s *udpSession) WriteAt(ctx context.Context, offset int64, p []byte) (int, error) {
	// TODO: random access write via protocol extensions.
	//
	return 0, fmt.Errorf("udpSession.WriteAt not implemented yet")
}

func (s *udpSession) Close() {
	s.closeOnce.Do(func() {
		if s.onClose != nil {
			s.onClose()
		}
		if s.conn != nil {
			s.conn.Close()
		}
	})
}

// Ensure udpSession satisfies the high-level Session interface.
var _ Session = (*udpSession)(nil)

// =======================================================
// ClientSessions
// =======================================================

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
