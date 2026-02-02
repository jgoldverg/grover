package gserver

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

type ServerSessions struct {
	mu        sync.RWMutex
	sessions  map[string]*ServerSession
	cfg       *internal.ServerConfig
	host      string
	streamSeq atomic.Uint32
}

type ServerSession struct {
	ID         uuid.UUID
	Token      []byte
	Mode       pb.OpenSessionRequest_Mode
	Path       string
	Size       int64
	StreamID   uint32
	LeaseID    uuid.UUID
	MTU        uint32
	TTLSeconds uint32
	TotalSize  uint64
	CreatedAt  time.Time

	conn      *net.UDPConn
	localAddr *net.UDPAddr

	remoteMu   sync.RWMutex
	remoteAddr *net.UDPAddr

	file        *os.File
	tracker     *udpwire.SackTracker
	writeOffset uint64
}

func NewServerSessions(cfg *internal.ServerConfig) *ServerSessions {
	host := os.Getenv("GROVER_UDP_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	return &ServerSessions{
		sessions: make(map[string]*ServerSession),
		cfg:      cfg,
		host:     host,
	}
}

func (s *ServerSession) Conn() *net.UDPConn {
	return s.conn
}

func (s *ServerSession) LocalAddr() *net.UDPAddr {
	return s.localAddr
}

func (s *ServerSession) RemoteAddr() *net.UDPAddr {
	s.remoteMu.RLock()
	defer s.remoteMu.RUnlock()
	return s.remoteAddr
}

func (s *ServerSession) SetRemoteAddr(addr *net.UDPAddr) {
	s.remoteMu.Lock()
	s.remoteAddr = addr
	s.remoteMu.Unlock()
}

func (s *ServerSession) Close() {
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.file != nil {
		_ = s.file.Close()
	}
}

func (sm *ServerSessions) CreateSession(req *pb.OpenSessionRequest) (*ServerSession, error) {
	if req == nil {
		return nil, errors.New("request cannot be nil")
	}
	if req.GetMode() == pb.OpenSessionRequest_MODE_UNSPECIFIED {
		return nil, errors.New("session mode is required")
	}
	if req.GetPath() == "" {
		return nil, errors.New("path is required")
	}

	conn, laddr, err := sm.allocateUDPConn()
	if err != nil {
		return nil, fmt.Errorf("allocate udp socket: %w", err)
	}

	token, err := sm.generateSessionToken()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("generate session token: %w", err)
	}

	var (
		file *os.File
		size int64
	)
	switch req.GetMode() {
	case pb.OpenSessionRequest_READ:
		file, size, err = sm.openFileForRead(req.GetPath())
	case pb.OpenSessionRequest_WRITE:
		file, err = sm.openFileForWrite(req.GetPath())
	default:
		err = fmt.Errorf("unsupported mode %s", req.GetMode())
	}
	if err != nil {
		internal.Error("failed to create file", internal.Fields{
			"error": err.Error(),
		})
		conn.Close()
		return nil, err
	}

	sessionID := uuid.New()
	streamID := sm.nextStreamID()
	totalSize := func() uint64 {
		switch req.GetMode() {
		case pb.OpenSessionRequest_READ:
			if size > 0 {
				return uint64(size)
			}
		case pb.OpenSessionRequest_WRITE:
			if req.GetSize() > 0 {
				return uint64(req.GetSize())
			}
		}
		return 0
	}()
	session := &ServerSession{
		ID:          sessionID,
		Token:       token,
		Mode:        req.GetMode(),
		Path:        req.GetPath(),
		Size:        req.GetSize(),
		StreamID:    streamID,
		LeaseID:     uuid.New(),
		MTU:         sm.mtuHint(),
		TTLSeconds:  sm.ttlSeconds(),
		TotalSize:   totalSize,
		CreatedAt:   time.Now(),
		conn:        conn,
		localAddr:   laddr,
		file:        file,
		tracker:     udpwire.NewSackTracker(),
		writeOffset: 0,
	}

	sm.mu.Lock()
	sm.sessions[sessionID.String()] = session
	sm.mu.Unlock()
	internal.Info("created session for file transfer", internal.Fields{
		"session_id": sessionID.String(),
		"path":       session.Path,
		"mode":       session.Mode.String(),
		"stream_id":  session.StreamID,
	})
	go newUDPSessionRunner(sm, session).run()

	return session, nil
}

func (sm *ServerSessions) LeaseStream(sessionID string, req *pb.LeaseStreamRequest) (*ServerSession, uuid.UUID, error) {
	if req == nil {
		return nil, uuid.Nil, errors.New("lease request is required")
	}
	sm.mu.RLock()
	session := sm.sessions[sessionID]
	sm.mu.RUnlock()
	if session == nil {
		return nil, uuid.Nil, fmt.Errorf("session %s not found", sessionID)
	}
	if req.GetMode() != session.Mode {
		return nil, uuid.Nil, fmt.Errorf("mode mismatch: %s vs %s", req.GetMode(), session.Mode)
	}

	lease := uuid.New()
	session.LeaseID = lease
	return session, lease, nil
}

func (sm *ServerSessions) ReleaseStream(sessionID string, streamID uint32, leaseID uuid.UUID, commit bool) error {
	sm.mu.RLock()
	session := sm.sessions[sessionID]
	sm.mu.RUnlock()
	if session == nil {
		return fmt.Errorf("session %s not found", sessionID)
	}
	if streamID != 0 && streamID != session.StreamID {
		return fmt.Errorf("unknown stream %d for session %s", streamID, sessionID)
	}
	if session.LeaseID != leaseID {
		return fmt.Errorf("lease mismatch for session %s", sessionID)
	}

	if session.Mode == pb.OpenSessionRequest_WRITE && session.file != nil {
		if commit {
			_ = session.file.Sync()
		} else {
			path := session.Path
			_ = session.file.Close()
			session.file = nil
			_ = os.Remove(path)
			return nil
		}
	}
	return nil
}

func (sm *ServerSessions) CloseSession(sessionID string) (*ServerSession, bool) {
	sm.mu.Lock()
	session, ok := sm.sessions[sessionID]
	if ok {
		delete(sm.sessions, sessionID)
	}
	sm.mu.Unlock()
	if !ok {
		return nil, false
	}
	session.Close()
	return session, true
}

func (sm *ServerSessions) UDPHost(*ServerSession) string {
	return sm.host
}

func (sm *ServerSessions) generateSessionToken() ([]byte, error) {
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	return token, nil
}

func (sm *ServerSessions) allocateUDPConn() (*net.UDPConn, *net.UDPAddr, error) {
	conn, err := sm.newUDPConn()
	if err != nil {
		return nil, nil, err
	}
	laddr, _ := conn.LocalAddr().(*net.UDPAddr)
	if laddr == nil {
		conn.Close()
		return nil, nil, errors.New("udp listener missing local address")
	}
	return conn, laddr, nil
}

func (sm *ServerSessions) newUDPConn() (*net.UDPConn, error) {
	addr := &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	if sm.cfg != nil {
		if sm.cfg.UDPReadBufferSize > 0 {
			_ = conn.SetReadBuffer(sm.cfg.UDPReadBufferSize)
		}
		if sm.cfg.UDPWriteBufferSize > 0 {
			_ = conn.SetWriteBuffer(sm.cfg.UDPWriteBufferSize)
		}
	}
	return conn, nil
}

func (sm *ServerSessions) nextStreamID() uint32 {
	id := sm.streamSeq.Add(1)
	if id == 0 {
		id = sm.streamSeq.Add(1)
	}
	return id
}

func (sm *ServerSessions) ttlSeconds() uint32 {
	return 10
}

func (sm *ServerSessions) mtuHint() uint32 {
	return defaultMTU
}
