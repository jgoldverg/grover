package gserver

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
)

type ServerSessions struct {
	mu        sync.RWMutex
	sessions  map[string]*ServerSession
	cfg       *internal.ServerConfig
	host      string
	transport string
	streamSeq atomic.Uint32
}

type ServerSession struct {
	ID         uuid.UUID
	Token      []byte
	Mode       pb.OpenSessionRequest_Mode
	Path       string
	Size       int64
	StreamID   uint32
	StreamIDs  []uint32
	LeaseID    uuid.UUID
	MTU        uint32
	TTLSeconds uint32
	TotalSize  uint64
	CreatedAt  time.Time

	conn      *net.UDPConn
	localAddr *net.UDPAddr

	remoteMu   sync.RWMutex
	remoteAddr *net.UDPAddr

	file *os.File

	tcpListener *net.TCPListener

	dataDone chan struct{}
	dataMu   sync.RWMutex
	dataErr  error
}

func NewServerSessions(cfg *internal.ServerConfig) *ServerSessions {
	host := os.Getenv("GROVER_UDP_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	transport := "udp"
	if cfg != nil && strings.EqualFold(strings.TrimSpace(cfg.TransferProtocol), "tcp") {
		transport = "tcp"
	}
	if v := os.Getenv("GROVER_TRANSFER_PROTOCOL"); strings.EqualFold(strings.TrimSpace(v), "tcp") {
		transport = "tcp"
	}
	return &ServerSessions{
		sessions:  make(map[string]*ServerSession),
		cfg:       cfg,
		host:      host,
		transport: transport,
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
	if s.tcpListener != nil {
		_ = s.tcpListener.Close()
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

	token, err := sm.generateSessionToken()
	if err != nil {
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
		return nil, err
	}

	var (
		conn        *net.UDPConn
		laddr       *net.UDPAddr
		tcpListener *net.TCPListener
	)
	switch sm.transport {
	case "tcp":
		tcpListener, err = sm.allocateTCPListener()
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("allocate tcp listener: %w", err)
		}
	case "udp":
		conn, laddr, err = sm.allocateUDPConn()
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("allocate udp socket: %w", err)
		}
	default:
		_ = file.Close()
		return nil, fmt.Errorf("unsupported transfer transport %q", sm.transport)
	}

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
	sessionID := uuid.New()
	parallelStreams := req.GetParallelStreams()
	if parallelStreams == 0 {
		parallelStreams = 1
	}
	if totalSize > 0 && uint64(parallelStreams) > totalSize {
		parallelStreams = uint32(totalSize)
	}
	streamIDs := make([]uint32, 0, parallelStreams)
	for i := uint32(0); i < parallelStreams; i++ {
		streamIDs = append(streamIDs, sm.nextStreamID())
	}
	streamID := streamIDs[0]
	session := &ServerSession{
		ID:          sessionID,
		Token:       token,
		Mode:        req.GetMode(),
		Path:        req.GetPath(),
		Size:        req.GetSize(),
		StreamID:    streamID,
		StreamIDs:   streamIDs,
		LeaseID:     uuid.New(),
		MTU:         sm.mtuHint(),
		TTLSeconds:  sm.ttlSeconds(),
		TotalSize:   totalSize,
		CreatedAt:   time.Now(),
		conn:        conn,
		localAddr:   laddr,
		file:        file,
		tcpListener: tcpListener,
		dataDone:    make(chan struct{}),
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
	switch sm.transport {
	case "tcp":
		go newTCPSessionRunner(sm, session).run()
	default:
		go newUDPSessionRunner(sm, session).run()
	}

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
		internal.Debug("release stream ignored for missing session", internal.Fields{
			"session_id": sessionID,
		})
		return nil
	}
	if streamID != 0 && streamID != session.StreamID {
		return fmt.Errorf("unknown stream %d for session %s", streamID, sessionID)
	}
	if session.LeaseID != leaseID {
		return fmt.Errorf("lease mismatch for session %s", sessionID)
	}

	if session.Mode == pb.OpenSessionRequest_WRITE && session.file != nil {
		if commit {
			if session.tcpListener != nil {
				if err := session.WaitDataDone(sm.ttlSeconds()); err != nil {
					return err
				}
			}
			_ = session.file.Sync()
			sm.CloseSession(sessionID)
		} else {
			path := session.Path
			sm.CloseSession(sessionID)
			_ = os.Remove(path)
			return nil
		}
	}
	if session.Mode == pb.OpenSessionRequest_READ && session.tcpListener != nil {
		if err := session.WaitDataDone(sm.ttlSeconds()); err != nil {
			return err
		}
		sm.CloseSession(sessionID)
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

func (s *ServerSession) FinishData(err error) {
	s.dataMu.Lock()
	s.dataErr = err
	s.dataMu.Unlock()
	close(s.dataDone)
}

func (s *ServerSession) WaitDataDone(ttlSeconds uint32) error {
	if s.dataDone == nil {
		return nil
	}
	timeout := time.Duration(ttlSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	select {
	case <-s.dataDone:
	case <-time.After(timeout):
		return fmt.Errorf("timed out waiting for session %s data plane to finish", s.ID.String())
	}
	s.dataMu.RLock()
	defer s.dataMu.RUnlock()
	return s.dataErr
}

func (sm *ServerSessions) UDPHost(*ServerSession) string {
	return sm.host
}

func (sm *ServerSessions) Transport() string {
	return sm.transport
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

func (sm *ServerSessions) allocateTCPListener() (*net.TCPListener, error) {
	addr := &net.TCPAddr{IP: net.IPv4zero, Port: 0}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	return l, nil
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
