package gserver

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

// ServerSessions tracks every live UDP transfer session along with the
// stream identifiers that belong to that session so that the UDP data plane
// can route packets without needing to hit the control channel again.
type ServerSessions struct {
	mu           sync.RWMutex
	sessions     map[string]*ServerSession
	streamLookup map[uint32]string

	cfg          *internal.ServerConfig
	announceHost string

	streamSeq atomic.Uint32
}

// SessionDescriptor captures the immutable information about a transfer that
// we share with clients over the control plane.
type SessionDescriptor struct {
	ID        uuid.UUID
	Token     []byte
	Mode      pb.OpenSessionRequest_Mode
	Path      string
	Size      int64
	StreamIDs []uint32

	MTU        uint32
	TTLSeconds uint32
	TotalSize  uint64
	CreatedAt  time.Time
}

type sessionRuntime struct {
	conn      *net.UDPConn
	localAddr *net.UDPAddr

	file     *os.File
	fileSize int64

	remoteMu   sync.RWMutex
	remoteAddr *net.UDPAddr
}

// ServerSession bundles together a descriptor with the runtime resources that
// only exist while the transfer is alive.
type ServerSession struct {
	SessionDescriptor
	runtime sessionRuntime
}

func (s *ServerSession) Conn() *net.UDPConn {
	return s.runtime.conn
}

func (s *ServerSession) LocalAddr() *net.UDPAddr {
	return s.runtime.localAddr
}

func (s *ServerSession) File() *os.File {
	return s.runtime.file
}

func (s *ServerSession) FileSize() int64 {
	return s.runtime.fileSize
}

func (s *ServerSession) SetFileSize(size int64) {
	s.runtime.fileSize = size
}

func (s *ServerSession) RemoteAddr() *net.UDPAddr {
	s.runtime.remoteMu.RLock()
	defer s.runtime.remoteMu.RUnlock()
	return s.runtime.remoteAddr
}

func (s *ServerSession) SetRemoteAddr(addr *net.UDPAddr) {
	s.runtime.remoteMu.Lock()
	s.runtime.remoteAddr = addr
	s.runtime.remoteMu.Unlock()
}

func (s *ServerSession) CloseRuntime() {
	if s.runtime.conn != nil {
		_ = s.runtime.conn.Close()
	}
	if s.runtime.file != nil {
		_ = s.runtime.file.Close()
	}
}

const (
	helloTimeout = 30 * time.Second
	defaultMTU   = 1500
)

var errNotRegularFile = errors.New("path is not a regular file")

// NewServerSessions builds an instance that knows how to size the sockets and
// what host should be advertised back to clients. The host can be overridden
// via GROVER_UDP_HOST; otherwise we default to localhost.
func NewServerSessions(cfg *internal.ServerConfig) *ServerSessions {
	host := os.Getenv("GROVER_UDP_HOST")
	if host == "" {
		host = "127.0.0.1"
	}

	return &ServerSessions{
		sessions:     make(map[string]*ServerSession),
		streamLookup: make(map[uint32]string),
		cfg:          cfg,
		announceHost: host,
	}
}

// CreateSession allocates a UDP socket, generates stream IDs, and stores the
// metadata so the control plane can reply with everything the client needs to
// dial the data plane.
func (sm *ServerSessions) CreateSession(req *pb.OpenSessionRequest) (*ServerSession, error) {
	if req == nil {
		return nil, errors.New("request cannot be nil")
	}
	if err := sm.validateOpenRequest(req); err != nil {
		return nil, err
	}

	conn, laddr, err := sm.allocateUDPConn()
	if err != nil {
		return nil, fmt.Errorf("allocate udp socket: %w", err)
	}

	dataFile, fileSize, err := sm.prepareDataFile(req)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	token, err := sm.generateSessionToken()
	if err != nil {
		_ = conn.Close()
		if dataFile != nil {
			_ = dataFile.Close()
		}
		return nil, fmt.Errorf("generate session token: %w", err)
	}

	streamIDs := sm.allocateStreams(req.GetParallelStreams())
	sessionID := uuid.New()

	session := &ServerSession{
		SessionDescriptor: sm.buildDescriptor(req, sessionID, token, streamIDs, fileSize),
		runtime: sessionRuntime{
			conn:      conn,
			localAddr: laddr,
			file:      dataFile,
			fileSize:  fileSize,
		},
	}

	sm.storeSession(session)
	sm.launchSession(session)

	return session, nil
}

func (sm *ServerSessions) validateOpenRequest(req *pb.OpenSessionRequest) error {
	if req.GetMode() == pb.OpenSessionRequest_MODE_UNSPECIFIED {
		return errors.New("session mode is required")
	}
	if req.GetPath() == "" {
		return errors.New("path is required")
	}
	return nil
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

func (sm *ServerSessions) prepareDataFile(req *pb.OpenSessionRequest) (*os.File, int64, error) {
	switch req.GetMode() {
	case pb.OpenSessionRequest_READ:
		return sm.openFileForRead(req.GetPath())
	case pb.OpenSessionRequest_WRITE:
		f, err := sm.openFileForWrite(req.GetPath())
		return f, 0, err
	default:
		return nil, 0, fmt.Errorf("unsupported session mode %s", req.GetMode().String())
	}
}

func (sm *ServerSessions) generateSessionToken() ([]byte, error) {
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	return token, nil
}

func (sm *ServerSessions) allocateStreams(streamCount uint32) []uint32 {
	if streamCount == 0 {
		streamCount = 1
	}
	streamIDs := make([]uint32, 0, streamCount)
	for i := uint32(0); i < streamCount; i++ {
		streamIDs = append(streamIDs, sm.nextStreamID())
	}
	return streamIDs
}

func (sm *ServerSessions) buildDescriptor(
	req *pb.OpenSessionRequest,
	sessionID uuid.UUID,
	token []byte,
	streamIDs []uint32,
	fileSize int64,
) SessionDescriptor {
	return SessionDescriptor{
		ID:    sessionID,
		Token: append([]byte(nil), token...),
		Mode:  req.GetMode(),
		Path:  req.GetPath(),
		Size: func() int64 {
			if fileSize > 0 {
				return fileSize
			}
			return req.GetSize()
		}(),
		StreamIDs:  append([]uint32(nil), streamIDs...),
		MTU:        sm.mtuHint(),
		TTLSeconds: sm.ttlSeconds(),
		TotalSize: func() uint64 {
			switch {
			case fileSize > 0:
				return uint64(fileSize)
			case req.GetSize() > 0:
				return uint64(req.GetSize())
			default:
				return 0
			}
		}(),
		CreatedAt: time.Now(),
	}
}

func (sm *ServerSessions) storeSession(session *ServerSession) {
	sessionKey := session.ID.String()
	sm.mu.Lock()
	sm.sessions[sessionKey] = session
	for _, sid := range session.StreamIDs {
		sm.streamLookup[sid] = sessionKey
	}
	sm.mu.Unlock()
}

func (sm *ServerSessions) launchSession(meta *ServerSession) {
	if meta == nil {
		return
	}

	switch meta.Mode {
	case pb.OpenSessionRequest_READ:
		go sm.runDownload(meta)
	case pb.OpenSessionRequest_WRITE:
		go sm.runUpload(meta)
	default:
		internal.Debug("session mode not implemented yet", internal.Fields{
			internal.FieldMsg: "mode not supported for udp transfer",
			"action":          "noop",
			"mode":            meta.Mode.String(),
			"session_id":      meta.ID.String(),
		})
	}
}

func (sm *ServerSessions) runDownload(session *ServerSession) {
	if session.File() == nil {
		internal.Error("download session missing source file", internal.Fields{
			"session_id": session.ID.String(),
			"path":       session.Path,
		})
		sm.CloseSession(session.ID.String())
		return
	}

	sm.runSession(
		session,
		"completed udp file stream",
		"failed to stream file over udp",
		func(sess *ServerSession, addr *net.UDPAddr) error {
			go sm.recvLoop(sess)
			return sm.sendFile(sess, addr)
		},
		func(sess *ServerSession) internal.Fields {
			return internal.Fields{"bytes": sess.FileSize()}
		},
	)
}

func (sm *ServerSessions) runUpload(session *ServerSession) {
	if session.File() == nil {
		internal.Error("upload session missing destination file", internal.Fields{
			"session_id": session.ID.String(),
			"path":       session.Path,
		})
		sm.CloseSession(session.ID.String())
		return
	}

	sm.runSession(
		session,
		"completed udp file upload",
		"failed to receive file over udp",
		func(sess *ServerSession, _ *net.UDPAddr) error {
			if err := sm.receiveFile(sess); err != nil {
				return err
			}
			if f := sess.File(); f != nil {
				if err := f.Sync(); err != nil {
					return fmt.Errorf("sync file: %w", err)
				}
			}
			return nil
		},
		func(sess *ServerSession) internal.Fields {
			return internal.Fields{"bytes": sess.FileSize()}
		},
	)
}

func (sm *ServerSessions) runSession(
	session *ServerSession,
	successMsg string,
	failureMsg string,
	handler func(*ServerSession, *net.UDPAddr) error,
	successFields func(*ServerSession) internal.Fields,
) {
	sessionID := session.ID.String()
	defer sm.CloseSession(sessionID)

	addr, err := sm.awaitHello(session)
	if err != nil {
		internal.Error("failed to receive udp hello", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
			"path":              session.Path,
		})
		return
	}

	if err := handler(session, addr); err != nil {
		fields := internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
			"path":              session.Path,
		}
		internal.Error(failureMsg, fields)
		return
	}

	fields := internal.Fields{
		"session_id": sessionID,
		"path":       session.Path,
	}
	if successFields != nil {
		for k, v := range successFields(session) {
			fields[k] = v
		}
	}

	internal.Info(successMsg, fields)
}

func (sm *ServerSessions) awaitHello(session *ServerSession) (*net.UDPAddr, error) {
	conn := session.Conn()
	if conn == nil {
		return nil, errors.New("nil udp connection for session")
	}

	buf := make([]byte, 1024)
	_ = conn.SetReadDeadline(time.Now().Add(helloTimeout))
	defer conn.SetReadDeadline(time.Time{})
	var hp udpwire.HelloPacket
	listenAddr := ""
	if conn.LocalAddr() != nil {
		listenAddr = conn.LocalAddr().String()
	}
	internal.Info("waiting for udp hello", internal.Fields{
		"session_id":  session.ID.String(),
		"path":        session.Path,
		"listen_addr": listenAddr,
		"token_len":   len(session.Token),
	})

	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			continue
		}
		if _, err := hp.Decode(buf[:n]); err != nil {
			continue
		}
		if !bytes.Equal(hp.SessionID, session.ID[:]) {
			continue
		}
		if !bytes.Equal(hp.Token, session.Token) {
			continue
		}

		session.SetRemoteAddr(addr)
		internal.Info("received udp hello", internal.Fields{
			"session_id":  session.ID.String(),
			"path":        session.Path,
			"remote_addr": addr.String(),
			"token_len":   len(hp.Token),
		})
		return addr, nil
	}
}

func (sm *ServerSessions) recvLoop(session *ServerSession) {
	conn := session.Conn()
	if conn == nil {
		return
	}
	buf := make([]byte, sm.recvBufferSize())
	var sp udpwire.StatusPacket

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		if n == 0 {
			continue
		}

		kind, ok := udpwire.PeekKind(buf[:n])
		if !ok {
			continue
		}

		if kind != udpwire.KindStatus {
			continue
		}
		if _, err := sp.Decode(buf[:n]); err != nil {
			continue
		}
		// TODO: feed ACK/SACK information into congestion control once implemented.
	}
}

func (sm *ServerSessions) receiveFile(session *ServerSession) error {
	conn := session.Conn()
	if conn == nil {
		return errors.New("nil udp connection for session")
	}
	file := session.File()
	if file == nil {
		return errors.New("no file attached to upload session")
	}
	if len(session.StreamIDs) == 0 {
		return errors.New("no stream IDs configured for upload session")
	}

	validStreams := make(map[uint32]struct{}, len(session.StreamIDs))
	for _, sid := range session.StreamIDs {
		validStreams[sid] = struct{}{}
	}

	buf := make([]byte, sm.recvBufferSize())
	var packet udpwire.DataPacket
	var offset uint64

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if isClosedNetworkError(err) {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		if n == 0 {
			continue
		}
		if !udpwire.IsDataPacket(buf[:n]) {
			continue
		}
		if _, err := packet.Decode(buf[:n]); err != nil {
			continue
		}
		if _, ok := validStreams[packet.StreamID]; !ok {
			continue
		}

		if packet.Offset != offset {
			if packet.Offset < offset {
				continue
			}
			if _, err := file.Seek(int64(packet.Offset), io.SeekStart); err != nil {
				return fmt.Errorf("seek file: %w", err)
			}
			offset = packet.Offset
		}

		if _, err := file.Write(packet.Payload); err != nil {
			return fmt.Errorf("write file: %w", err)
		}
		offset += uint64(len(packet.Payload))
		session.SetFileSize(int64(offset))
	}
}

func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrClosed) {
		return true
	}
	// Fallback for platforms that wrap the error string.
	return strings.Contains(err.Error(), "use of closed network connection")
}

func (sm *ServerSessions) sendFile(session *ServerSession, addr *net.UDPAddr) error {
	conn := session.Conn()
	if conn == nil {
		return errors.New("nil udp connection for session")
	}
	file := session.File()
	if file == nil {
		return errors.New("no file attached to download session")
	}
	if len(session.StreamIDs) == 0 {
		return errors.New("no stream IDs configured for session")
	}
	if addr == nil {
		return errors.New("nil remote address for session")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek file: %w", err)
	}

	payloadSize := sm.payloadSize(session)
	if payloadSize <= 0 {
		payloadSize = 1024
	}

	payloadBuf := make([]byte, payloadSize)
	packetBuf := make([]byte, udpwire.DataHeaderLen+payloadSize+4)
	sessionID32 := binary.BigEndian.Uint32(session.ID[:4])
	streamID := session.StreamIDs[0]

	var seq uint32
	var offset uint64

	for {
		n, readErr := file.Read(payloadBuf)
		if n > 0 {
			seq++
			dp := udpwire.DataPacket{
				SessionID: sessionID32,
				StreamID:  streamID,
				Seq:       seq,
				Offset:    offset,
				Payload:   payloadBuf[:n],
			}
			pktLen, err := dp.Encode(packetBuf)
			if err != nil {
				return fmt.Errorf("encode data packet: %w", err)
			}
			if _, err := conn.WriteToUDP(packetBuf[:pktLen], addr); err != nil {
				return fmt.Errorf("write udp packet: %w", err)
			}
			offset += uint64(n)
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return fmt.Errorf("read file: %w", readErr)
		}
	}
	return nil
}

func (sm *ServerSessions) payloadSize(session *ServerSession) int {
	mtu := int(session.MTU)
	if mtu <= 0 {
		mtu = defaultMTU
	}
	payload := mtu - udpwire.DataHeaderLen - 4 // checksum trailer
	if payload < 256 {
		payload = 256
	}
	return payload
}

func (sm *ServerSessions) recvBufferSize() int {
	if sm.cfg != nil && sm.cfg.UDPReadBufferSize > 0 {
		return sm.cfg.UDPReadBufferSize
	}
	return 64 * 1024
}

func (sm *ServerSessions) openFileForRead(path string) (*os.File, int64, error) {
	if path == "" {
		return nil, 0, errors.New("path is required for read sessions")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, 0, fmt.Errorf("%w: %s", errNotRegularFile, path)
	}
	return f, info.Size(), nil
}

func (sm *ServerSessions) openFileForWrite(path string) (*os.File, error) {
	if path == "" {
		return nil, errors.New("path is required for write sessions")
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." && dir != "/" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// CloseSession tears down the UDP socket and removes all metadata for the
// session. The bool return indicates whether we actually had the session.
func (sm *ServerSessions) CloseSession(sessionID string) (*ServerSession, bool) {
	sm.mu.Lock()
	meta, ok := sm.sessions[sessionID]
	if ok {
		delete(sm.sessions, sessionID)
		for _, sid := range meta.StreamIDs {
			delete(sm.streamLookup, sid)
		}
	}
	sm.mu.Unlock()

	if !ok {
		return nil, false
	}

	meta.CloseRuntime()
	return meta, true
}

// GetSession returns the metadata for a given session id.
func (sm *ServerSessions) GetSession(sessionID string) (*ServerSession, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	meta, ok := sm.sessions[sessionID]
	return meta, ok
}

// LookupByStream lets the UDP workers reverse-map a stream to its session.
func (sm *ServerSessions) LookupByStream(streamID uint32) (*ServerSession, bool) {
	sm.mu.RLock()
	sessionID, ok := sm.streamLookup[streamID]
	if !ok {
		sm.mu.RUnlock()
		return nil, false
	}
	meta := sm.sessions[sessionID]
	sm.mu.RUnlock()
	return meta, meta != nil
}

// UDPHost returns the host we should advertise back to clients. If we bound
// the socket to a concrete IP we prefer that over the announceHost fallback.
func (sm *ServerSessions) UDPHost(session *ServerSession) string {
	if session != nil {
		if addr := session.LocalAddr(); addr != nil && addr.IP != nil && !addr.IP.IsUnspecified() {
			return addr.IP.String()
		}
	}
	return sm.announceHost
}

func (sm *ServerSessions) nextStreamID() uint32 {
	id := sm.streamSeq.Add(1)
	if id == 0 {
		// Skip 0 as a stream identifier; wraparound here is very unlikely but
		// we handle it anyway.
		id = sm.streamSeq.Add(1)
	}
	return id
}

func (sm *ServerSessions) newUDPConn() (*net.UDPConn, error) {
	addr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: 0,
	}
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

func (sm *ServerSessions) mtuHint() uint32 {
	if sm.cfg != nil && sm.cfg.UDPQueueDepth > 0 {
		return uint32(sm.cfg.UDPQueueDepth)
	}
	return defaultMTU
}

func (sm *ServerSessions) ttlSeconds() uint32 {
	if sm.cfg != nil && sm.cfg.UDPReadTimeoutMs > 0 {
		return uint32(time.Duration(sm.cfg.UDPReadTimeoutMs) * time.Millisecond / time.Second)
	}
	return 30
}
