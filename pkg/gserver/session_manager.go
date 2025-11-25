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
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

// SessionManager tracks every live UDP transfer session along with the
// stream identifiers that belong to that session so that the UDP data plane
// can route packets without needing to hit the control channel again.
type SessionManager struct {
	mu           sync.RWMutex
	sessions     map[string]*SessionMeta
	streamLookup map[uint32]string

	cfg          *internal.ServerConfig
	announceHost string

	streamSeq atomic.Uint32
}

// SessionMeta contains the server-side state for a single Upload/Download
// session that mirrors what the client keeps locally. Each session has its
// own UDP socket (so we can start simple) and one or more logical stream IDs.
type SessionMeta struct {
	ID        uuid.UUID
	Token     []byte
	Mode      pb.OpenSessionRequest_Mode
	Path      string
	Size      int64
	StreamIDs []uint32

	Conn      *net.UDPConn
	LocalAddr *net.UDPAddr

	MTU        uint32
	TTLSeconds uint32
	TotalSize  uint64
	CreatedAt  time.Time

	file       *os.File
	fileSize   int64
	remoteMu   sync.RWMutex
	remoteAddr *net.UDPAddr
}

const (
	helloMagic   = "GRVR"
	helloVersion = 1
	helloTimeout = 30 * time.Second
	defaultMTU   = 1500
)

var errNotRegularFile = errors.New("path is not a regular file")

// NewSessionManager builds an instance that knows how to size the sockets and
// what host should be advertised back to clients. The host can be overridden
// via GROVER_UDP_HOST; otherwise we default to localhost.
func NewSessionManager(cfg *internal.ServerConfig) *SessionManager {
	host := os.Getenv("GROVER_UDP_HOST")
	if host == "" {
		host = "127.0.0.1"
	}

	return &SessionManager{
		sessions:     make(map[string]*SessionMeta),
		streamLookup: make(map[uint32]string),
		cfg:          cfg,
		announceHost: host,
	}
}

// CreateSession allocates a UDP socket, generates stream IDs, and stores the
// metadata so the control plane can reply with everything the client needs to
// dial the data plane.
func (sm *SessionManager) CreateSession(req *pb.OpenSessionRequest) (*SessionMeta, error) {
	if req == nil {
		return nil, errors.New("request cannot be nil")
	}

	conn, err := sm.newUDPConn()
	if err != nil {
		return nil, fmt.Errorf("allocate udp socket: %w", err)
	}

	laddr, _ := conn.LocalAddr().(*net.UDPAddr)
	if laddr == nil {
		conn.Close()
		return nil, errors.New("udp listener missing local address")
	}

	var (
		downloadFile *os.File
		fileSize     int64
	)
	if req.GetMode() == pb.OpenSessionRequest_READ {
		var err error
		downloadFile, fileSize, err = sm.openFileForRead(req.GetPath())
		if err != nil {
			conn.Close()
			return nil, err
		}
	}

	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		conn.Close()
		if downloadFile != nil {
			_ = downloadFile.Close()
		}
		return nil, fmt.Errorf("generate session token: %w", err)
	}

	sessionID := uuid.New()
	streamCount := req.GetParallelStreams()
	if streamCount == 0 {
		streamCount = 1
	}

	streamIDs := make([]uint32, 0, streamCount)
	for i := uint32(0); i < streamCount; i++ {
		streamIDs = append(streamIDs, sm.nextStreamID())
	}

	meta := &SessionMeta{
		ID:    sessionID,
		Token: token[:],
		Mode:  req.GetMode(),
		Path:  req.GetPath(),
		Size: func() int64 {
			if fileSize > 0 {
				return fileSize
			}
			return req.GetSize()
		}(),
		StreamIDs: streamIDs,
		Conn:      conn,
		LocalAddr: laddr,
		MTU:       sm.mtuHint(),
		TTLSeconds: func() uint32 {
			if sm.cfg != nil && sm.cfg.UDPReadTimeoutMs > 0 {
				return uint32(time.Duration(sm.cfg.UDPReadTimeoutMs) * time.Millisecond / time.Second)
			}
			return 30
		}(),
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
		file:      downloadFile,
		fileSize:  fileSize,
	}

	sessionKey := sessionID.String()
	sm.mu.Lock()
	sm.sessions[sessionKey] = meta
	for _, sid := range streamIDs {
		sm.streamLookup[sid] = sessionKey
	}
	sm.mu.Unlock()

	sm.launchSession(meta)

	return meta, nil
}

func (sm *SessionManager) launchSession(meta *SessionMeta) {
	if meta == nil {
		return
	}

	switch meta.Mode {
	case pb.OpenSessionRequest_READ:
		go sm.runDownload(meta)
	default:
		internal.Debug("session mode not implemented yet", internal.Fields{
			internal.FieldMsg: "mode not supported for udp transfer",
			"action":          "noop",
			"mode":            meta.Mode.String(),
			"session_id":      meta.ID.String(),
		})
	}
}

func (sm *SessionManager) runDownload(meta *SessionMeta) {
	sessionID := meta.ID.String()
	defer sm.CloseSession(sessionID)

	addr, err := sm.awaitHello(meta)
	if err != nil {
		internal.Error("failed to receive udp hello", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
			"path":              meta.Path,
		})
		return
	}

	go sm.recvLoop(meta)

	if err := sm.sendFile(meta, addr); err != nil {
		internal.Error("failed to stream file over udp", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
			"path":              meta.Path,
		})
		return
	}

	internal.Info("completed udp file stream", internal.Fields{
		"session_id": sessionID,
		"path":       meta.Path,
		"bytes":      meta.fileSize,
	})
}

func (sm *SessionManager) awaitHello(meta *SessionMeta) (*net.UDPAddr, error) {
	if meta.Conn == nil {
		return nil, errors.New("nil udp connection for session")
	}

	buf := make([]byte, 1024)
	_ = meta.Conn.SetReadDeadline(time.Now().Add(helloTimeout))
	defer meta.Conn.SetReadDeadline(time.Time{})

	for {
		n, addr, err := meta.Conn.ReadFromUDP(buf)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			continue
		}
		if n < len(helloMagic)+2 {
			continue
		}
		if string(buf[:len(helloMagic)]) != helloMagic {
			continue
		}
		if buf[len(helloMagic)] != helloVersion {
			return nil, fmt.Errorf("unexpected hello version %d", buf[len(helloMagic)])
		}

		sessionLen := int(buf[len(helloMagic)+1])
		offset := len(helloMagic) + 2
		if n < offset+sessionLen+2 {
			continue
		}
		if !bytes.Equal(buf[offset:offset+sessionLen], meta.ID[:]) {
			continue
		}
		offset += sessionLen

		tokenLen := int(binary.BigEndian.Uint16(buf[offset : offset+2]))
		offset += 2
		if n < offset+tokenLen {
			continue
		}
		if !bytes.Equal(buf[offset:offset+tokenLen], meta.Token) {
			continue
		}

		meta.remoteMu.Lock()
		meta.remoteAddr = addr
		meta.remoteMu.Unlock()
		return addr, nil
	}
}

func (sm *SessionManager) recvLoop(meta *SessionMeta) {
	if meta.Conn == nil {
		return
	}
	buf := make([]byte, sm.recvBufferSize())
	var sp udpwire.StatusPacket

	for {
		n, _, err := meta.Conn.ReadFromUDP(buf)
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

func (sm *SessionManager) sendFile(meta *SessionMeta, addr *net.UDPAddr) error {
	if meta.file == nil {
		return errors.New("no file attached to download session")
	}
	if len(meta.StreamIDs) == 0 {
		return errors.New("no stream IDs configured for session")
	}
	if addr == nil {
		return errors.New("nil remote address for session")
	}
	if _, err := meta.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek file: %w", err)
	}

	payloadSize := sm.payloadSize(meta)
	if payloadSize <= 0 {
		payloadSize = 1024
	}

	payloadBuf := make([]byte, payloadSize)
	packetBuf := make([]byte, udpwire.DataHeaderLen+payloadSize+4)
	sessionID32 := binary.BigEndian.Uint32(meta.ID[:4])
	streamID := meta.StreamIDs[0]

	var seq uint32
	var offset uint64

	for {
		n, readErr := meta.file.Read(payloadBuf)
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
			if _, err := meta.Conn.WriteToUDP(packetBuf[:pktLen], addr); err != nil {
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

func (sm *SessionManager) payloadSize(meta *SessionMeta) int {
	mtu := int(meta.MTU)
	if mtu <= 0 {
		mtu = defaultMTU
	}
	payload := mtu - udpwire.DataHeaderLen - 4 // checksum trailer
	if payload < 256 {
		payload = 256
	}
	return payload
}

func (sm *SessionManager) recvBufferSize() int {
	if sm.cfg != nil && sm.cfg.UDPReadBufferSize > 0 {
		return sm.cfg.UDPReadBufferSize
	}
	return 64 * 1024
}

func (sm *SessionManager) openFileForRead(path string) (*os.File, int64, error) {
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

// CloseSession tears down the UDP socket and removes all metadata for the
// session. The bool return indicates whether we actually had the session.
func (sm *SessionManager) CloseSession(sessionID string) (*SessionMeta, bool) {
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

	if meta.Conn != nil {
		_ = meta.Conn.Close()
	}
	if meta.file != nil {
		_ = meta.file.Close()
	}
	return meta, true
}

// GetSession returns the metadata for a given session id.
func (sm *SessionManager) GetSession(sessionID string) (*SessionMeta, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	meta, ok := sm.sessions[sessionID]
	return meta, ok
}

// LookupByStream lets the UDP workers reverse-map a stream to its session.
func (sm *SessionManager) LookupByStream(streamID uint32) (*SessionMeta, bool) {
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
func (sm *SessionManager) UDPHost(meta *SessionMeta) string {
	if meta != nil && meta.LocalAddr != nil && meta.LocalAddr.IP != nil && !meta.LocalAddr.IP.IsUnspecified() {
		return meta.LocalAddr.IP.String()
	}
	return sm.announceHost
}

func (sm *SessionManager) nextStreamID() uint32 {
	id := sm.streamSeq.Add(1)
	if id == 0 {
		// Skip 0 as a stream identifier; wraparound here is very unlikely but
		// we handle it anyway.
		id = sm.streamSeq.Add(1)
	}
	return id
}

func (sm *SessionManager) newUDPConn() (*net.UDPConn, error) {
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

func (sm *SessionManager) mtuHint() uint32 {
	if sm.cfg != nil && sm.cfg.UDPQueueDepth > 0 {
		return uint32(sm.cfg.UDPQueueDepth)
	}
	return defaultMTU
}
