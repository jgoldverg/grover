package gserver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

const defaultForwardTTL = 10 * time.Minute

type ForwardSessionManager struct {
	mu        sync.RWMutex
	cfg       *internal.ServerConfig
	ports     *DataPortAllocator
	sessions  map[string]*forwardSession
	stop      chan struct{}
	closeOnce sync.Once
}

type forwardSession struct {
	id       string
	routeID  string
	jobID    string
	hopIndex uint32
	protocol pb.DataProtocol
	ingress  *pb.DataEndpoint
	egress   *pb.DataEndpoint
	ttl      time.Duration
	expires  time.Time

	mu      sync.RWMutex
	state   pb.RuntimeState
	errText string

	lease  *DataPortLease
	cancel context.CancelFunc

	ingressBytes  atomic.Uint64
	egressBytes   atomic.Uint64
	packets       atomic.Uint64
	errors        atomic.Uint64
	activeConns   atomic.Uint32
	activeStreams atomic.Uint32
	startedAt     time.Time
}

type udpForwardKey struct {
	sessionID uint32
	streamID  uint32
}

func NewForwardSessionManager(cfg *internal.ServerConfig) (*ForwardSessionManager, error) {
	ports, err := NewDataPortAllocator(cfg)
	if err != nil {
		return nil, err
	}
	m := &ForwardSessionManager{
		cfg:      cfg,
		ports:    ports,
		sessions: make(map[string]*forwardSession),
		stop:     make(chan struct{}),
	}
	go m.expiryLoop()
	return m, nil
}

func (m *ForwardSessionManager) Close() {
	m.closeOnce.Do(func() {
		close(m.stop)
		m.mu.Lock()
		sessions := make([]*forwardSession, 0, len(m.sessions))
		for id, session := range m.sessions {
			delete(m.sessions, id)
			sessions = append(sessions, session)
		}
		m.mu.Unlock()
		for _, session := range sessions {
			session.close(pb.RuntimeState_RUNTIME_STATE_ABORTED, "")
		}
	})
}

func (m *ForwardSessionManager) Create(ctx context.Context, req *pb.CreateForwardRequest) (*pb.ForwardSession, error) {
	if req == nil {
		return nil, errors.New("create forward request is required")
	}
	protocol := req.GetProtocol()
	if protocol == pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED {
		protocol = pb.DataProtocol_DATA_PROTOCOL_TCP
	}
	if protocol != pb.DataProtocol_DATA_PROTOCOL_TCP && protocol != pb.DataProtocol_DATA_PROTOCOL_UDP {
		return nil, fmt.Errorf("unsupported forward protocol %s", protocol.String())
	}
	egress := req.GetEgress()
	if egress == nil || strings.TrimSpace(egress.GetHost()) == "" || egress.GetPort() == 0 {
		return nil, errors.New("egress host and port are required")
	}
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	if ttl <= 0 {
		ttl = defaultForwardTTL
	}

	var (
		lease *DataPortLease
		err   error
	)
	switch protocol {
	case pb.DataProtocol_DATA_PROTOCOL_TCP:
		lease, err = m.ports.AllocateTCP()
	case pb.DataProtocol_DATA_PROTOCOL_UDP:
		lease, err = m.ports.AllocateUDP()
	}
	if err != nil {
		return nil, err
	}

	id := uuid.NewString()
	ingress := &pb.DataEndpoint{
		Host: lease.AdvertiseHost,
		Port: uint32(lease.Port),
	}
	session := &forwardSession{
		id:       id,
		routeID:  strings.TrimSpace(req.GetRouteId()),
		jobID:    strings.TrimSpace(req.GetJobId()),
		hopIndex: req.GetHopIndex(),
		protocol: protocol,
		ingress:  ingress,
		egress: &pb.DataEndpoint{
			Host: strings.TrimSpace(egress.GetHost()),
			Port: egress.GetPort(),
		},
		ttl:       ttl,
		expires:   time.Now().Add(ttl),
		state:     pb.RuntimeState_RUNTIME_STATE_RUNNING,
		lease:     lease,
		startedAt: time.Now(),
	}
	runCtx, cancel := context.WithCancel(context.Background())
	session.cancel = cancel

	m.mu.Lock()
	m.sessions[id] = session
	m.mu.Unlock()

	switch protocol {
	case pb.DataProtocol_DATA_PROTOCOL_TCP:
		go session.runTCP(runCtx)
	case pb.DataProtocol_DATA_PROTOCOL_UDP:
		go session.runUDP(runCtx)
	}
	return session.snapshot(), nil
}

func (m *ForwardSessionManager) Get(id string) (*pb.ForwardSession, error) {
	session := m.lookup(id)
	if session == nil {
		return nil, fmt.Errorf("forward %q not found", id)
	}
	return session.snapshot(), nil
}

func (m *ForwardSessionManager) List(routeID, jobID string) []*pb.ForwardSession {
	routeID = strings.TrimSpace(routeID)
	jobID = strings.TrimSpace(jobID)
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*pb.ForwardSession, 0, len(m.sessions))
	for _, session := range m.sessions {
		if routeID != "" && session.routeID != routeID {
			continue
		}
		if jobID != "" && session.jobID != jobID {
			continue
		}
		out = append(out, session.snapshot())
	}
	return out
}

func (m *ForwardSessionManager) Delete(id string) bool {
	m.mu.Lock()
	session := m.sessions[strings.TrimSpace(id)]
	if session != nil {
		delete(m.sessions, session.id)
	}
	m.mu.Unlock()
	if session == nil {
		return false
	}
	session.close(pb.RuntimeState_RUNTIME_STATE_ABORTED, "")
	return true
}

func (m *ForwardSessionManager) Renew(id string, ttlSeconds uint32) (*pb.ForwardSession, error) {
	session := m.lookup(id)
	if session == nil {
		return nil, fmt.Errorf("forward %q not found", id)
	}
	ttl := time.Duration(ttlSeconds) * time.Second
	if ttl <= 0 {
		ttl = defaultForwardTTL
	}
	session.mu.Lock()
	session.ttl = ttl
	session.expires = time.Now().Add(ttl)
	session.mu.Unlock()
	return session.snapshot(), nil
}

func (m *ForwardSessionManager) lookup(id string) *forwardSession {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[strings.TrimSpace(id)]
}

func (m *ForwardSessionManager) expiryLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.expireNow(time.Now())
		}
	}
}

func (m *ForwardSessionManager) expireNow(now time.Time) {
	var expired []*forwardSession
	m.mu.Lock()
	for id, session := range m.sessions {
		session.mu.RLock()
		expires := session.expires
		state := session.state
		session.mu.RUnlock()
		if state == pb.RuntimeState_RUNTIME_STATE_RUNNING && !expires.IsZero() && !now.Before(expires) {
			delete(m.sessions, id)
			expired = append(expired, session)
		}
	}
	m.mu.Unlock()
	for _, session := range expired {
		session.close(pb.RuntimeState_RUNTIME_STATE_EXPIRED, "")
	}
}

func (s *forwardSession) runTCP(ctx context.Context) {
	listener := s.lease.TCPListener
	if listener == nil {
		s.close(pb.RuntimeState_RUNTIME_STATE_FAILED, "tcp listener missing")
		return
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || s.currentState() != pb.RuntimeState_RUNTIME_STATE_RUNNING {
				return
			}
			s.errors.Add(1)
			continue
		}
		s.activeConns.Add(1)
		go s.handleTCPConn(ctx, conn)
	}
}

func (s *forwardSession) handleTCPConn(ctx context.Context, inbound net.Conn) {
	defer s.activeConns.Add(^uint32(0))
	defer inbound.Close()
	dialer := net.Dialer{}
	outbound, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(s.egress.GetHost(), fmt.Sprintf("%d", s.egress.GetPort())))
	if err != nil {
		s.errors.Add(1)
		return
	}
	defer outbound.Close()
	connDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = inbound.Close()
			_ = outbound.Close()
		case <-connDone:
		}
	}()
	defer close(connDone)

	done := make(chan struct{}, 2)
	go func() {
		_, err := io.Copy(outbound, &countingReader{r: inbound, counter: &s.ingressBytes})
		if err != nil {
			s.errors.Add(1)
		}
		_ = closeWrite(outbound)
		done <- struct{}{}
	}()
	go func() {
		_, err := io.Copy(inbound, &countingReader{r: outbound, counter: &s.egressBytes})
		if err != nil {
			s.errors.Add(1)
		}
		_ = closeWrite(inbound)
		done <- struct{}{}
	}()
	<-done
	<-done
}

func (s *forwardSession) runUDP(ctx context.Context) {
	ingress := s.lease.UDPConn
	if ingress == nil {
		s.close(pb.RuntimeState_RUNTIME_STATE_FAILED, "udp socket missing")
		return
	}
	egressAddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(s.egress.GetHost(), fmt.Sprintf("%d", s.egress.GetPort())))
	if err != nil {
		s.close(pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
		return
	}
	egress, err := net.DialUDP("udp", nil, egressAddr)
	if err != nil {
		s.close(pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
		return
	}
	defer egress.Close()

	var clientMu sync.RWMutex
	var lastClientAddr *net.UDPAddr
	clientByKey := make(map[udpForwardKey]*net.UDPAddr)
	s.activeStreams.Store(1)
	defer s.activeStreams.Store(0)

	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, addr, err := ingress.ReadFromUDP(buf)
			if err != nil {
				if ctx.Err() == nil && s.currentState() == pb.RuntimeState_RUNTIME_STATE_RUNNING {
					s.errors.Add(1)
				}
				return
			}
			if key, ok := udpForwardIngressKey(buf[:n]); ok {
				clientMu.Lock()
				clientByKey[key] = cloneUDPAddr(addr)
				lastClientAddr = cloneUDPAddr(addr)
				clientMu.Unlock()
			} else {
				clientMu.Lock()
				lastClientAddr = cloneUDPAddr(addr)
				clientMu.Unlock()
			}
			written, err := egress.Write(buf[:n])
			if err != nil {
				s.errors.Add(1)
				continue
			}
			if written != n {
				s.errors.Add(1)
				continue
			}
			s.ingressBytes.Add(uint64(n))
			s.packets.Add(1)
		}
	}()

	buf := make([]byte, 64*1024)
	for {
		n, err := egress.Read(buf)
		if err != nil {
			if ctx.Err() == nil && s.currentState() == pb.RuntimeState_RUNTIME_STATE_RUNNING {
				s.errors.Add(1)
			}
			return
		}
		key, hasKey := udpForwardEgressKey(buf[:n])
		clientMu.RLock()
		addr := (*net.UDPAddr)(nil)
		if hasKey {
			addr = clientByKey[key]
		}
		if addr == nil {
			addr = lastClientAddr
		}
		addr = cloneUDPAddr(addr)
		clientMu.RUnlock()
		if addr == nil {
			continue
		}
		written, err := ingress.WriteToUDP(buf[:n], addr)
		if err != nil {
			s.errors.Add(1)
			continue
		}
		if written != n {
			s.errors.Add(1)
			continue
		}
		s.egressBytes.Add(uint64(n))
		s.packets.Add(1)
	}
}

func udpForwardIngressKey(packet []byte) (udpForwardKey, bool) {
	if len(packet) >= udpwire.DataHeaderLen && udpwire.IsDataPacket(packet) {
		return udpForwardKey{
			sessionID: binary.BigEndian.Uint32(packet[2:6]),
			streamID:  binary.BigEndian.Uint32(packet[6:10]),
		}, true
	}
	if len(packet) >= len(udpJobMagic)+1+4 && string(packet[:len(udpJobMagic)]) == string(udpJobMagic) && packet[len(udpJobMagic)] == udpJobPacketStart {
		return udpForwardKey{
			sessionID: binary.BigEndian.Uint32(packet[len(udpJobMagic)+1 : len(udpJobMagic)+5]),
			streamID:  0,
		}, true
	}
	return udpForwardKey{}, false
}

func udpForwardEgressKey(packet []byte) (udpForwardKey, bool) {
	if len(packet) >= udpwire.StatusHeaderLen && udpwire.IsStatusPacket(packet) {
		return udpForwardKey{
			sessionID: binary.BigEndian.Uint32(packet[2:6]),
			streamID:  binary.BigEndian.Uint32(packet[6:10]),
		}, true
	}
	if len(packet) == len(udpJobMagic)+1+4 && string(packet[:len(udpJobMagic)]) == string(udpJobMagic) {
		kind := packet[len(udpJobMagic)]
		if kind == udpJobPacketReady || kind == udpJobPacketDone {
			return udpForwardKey{
				sessionID: binary.BigEndian.Uint32(packet[len(udpJobMagic)+1 : len(udpJobMagic)+5]),
				streamID:  0,
			}, true
		}
	}
	return udpForwardKey{}, false
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	cp := *addr
	if addr.IP != nil {
		cp.IP = append(net.IP(nil), addr.IP...)
	}
	return &cp
}

func (s *forwardSession) close(state pb.RuntimeState, errText string) {
	s.mu.Lock()
	if s.state != pb.RuntimeState_RUNTIME_STATE_RUNNING {
		s.mu.Unlock()
		return
	}
	s.state = state
	s.errText = errText
	cancel := s.cancel
	lease := s.lease
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if lease != nil {
		_ = lease.Close()
	}
}

func (s *forwardSession) currentState() pb.RuntimeState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *forwardSession) snapshot() *pb.ForwardSession {
	s.mu.RLock()
	state := s.state
	errText := s.errText
	expires := s.expires
	ttl := s.ttl
	s.mu.RUnlock()

	elapsed := time.Since(s.startedAt).Seconds()
	totalBytes := s.ingressBytes.Load() + s.egressBytes.Load()
	avg := 0.0
	if elapsed > 0 {
		avg = float64(totalBytes) / elapsed
	}
	return &pb.ForwardSession{
		ForwardId:     s.id,
		RouteId:       s.routeID,
		JobId:         s.jobID,
		HopIndex:      s.hopIndex,
		Protocol:      s.protocol,
		Ingress:       cloneEndpoint(s.ingress),
		Egress:        cloneEndpoint(s.egress),
		State:         state,
		TtlSeconds:    uint32(ttl / time.Second),
		ExpiresAtUnix: expires.Unix(),
		Stats: &pb.StatsSnapshot{
			IngressBytes:         s.ingressBytes.Load(),
			EgressBytes:          s.egressBytes.Load(),
			Packets:              s.packets.Load(),
			CurrentThroughputBps: avg,
			AverageThroughputBps: avg,
			ActiveConnections:    s.activeConns.Load(),
			ActiveStreams:        s.activeStreams.Load(),
			Errors:               s.errors.Load(),
			SampledAtUnixNano:    time.Now().UnixNano(),
		},
		ErrorMessage: errText,
	}
}

type countingReader struct {
	r       io.Reader
	counter *atomic.Uint64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.counter.Add(uint64(n))
	}
	return n, err
}

func closeWrite(conn net.Conn) error {
	type closeWriter interface {
		CloseWrite() error
	}
	if cw, ok := conn.(closeWriter); ok {
		return cw.CloseWrite()
	}
	return conn.Close()
}

func cloneEndpoint(ep *pb.DataEndpoint) *pb.DataEndpoint {
	if ep == nil {
		return nil
	}
	return &pb.DataEndpoint{Host: ep.GetHost(), Port: ep.GetPort()}
}
