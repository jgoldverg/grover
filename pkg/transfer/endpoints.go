package transfer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal/dataport"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

const defaultTransferEndpointTTL = 10 * time.Minute
const defaultReverseTCPConnections = 32

// transferEndpointRegistry owns listener leases and reverse connection pools.
// It delegates file reception; it never schedules source jobs.
type transferEndpointRegistry struct {
	mu         sync.RWMutex
	endpoints  map[string]*preparedTransferEndpoint
	reverseTCP map[string]*reverseTCPPool
	ports      *dataport.Allocator
	portErr    error
	runtimeCfg internal.TransferRuntimeConfig
	receiver   *destinationReceiver
	stop       <-chan struct{}
}

func (m *Manager) PrepareEndpoint(ctx context.Context, req *pb.PrepareTransferEndpointRequest) (*pb.TransferEndpoint, error) {
	return m.registry.PrepareEndpoint(ctx, req)
}

func (m *Manager) GetEndpoint(id string) (*pb.TransferEndpoint, bool) {
	return m.registry.GetEndpoint(id)
}

func (m *transferEndpointRegistry) close() {
	m.mu.Lock()
	endpoints := m.endpoints
	m.endpoints = make(map[string]*preparedTransferEndpoint)
	m.reverseTCP = make(map[string]*reverseTCPPool)
	m.mu.Unlock()
	for _, endpoint := range endpoints {
		closePreparedTransferEndpoint(endpoint)
	}
}

type preparedTransferEndpoint struct {
	endpoint *pb.TransferEndpoint
	expires  time.Time
	lease    *dataport.Lease
	reverse  *reverseTCPPool
}

type reverseTCPPool struct {
	listener *net.TCPListener
	conns    chan net.Conn
	done     chan struct{}
	once     sync.Once
}

func (m *transferEndpointRegistry) PrepareEndpoint(ctx context.Context, req *pb.PrepareTransferEndpointRequest) (*pb.TransferEndpoint, error) {
	if req == nil {
		return nil, errors.New("prepare endpoint request is required")
	}
	if req.GetRole() == pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_UNSPECIFIED {
		return nil, errors.New("endpoint role is required")
	}
	root := strings.TrimSpace(req.GetRootPath())
	if root == "" {
		return nil, errors.New("root path is required")
	}
	protocol := req.GetProtocol()
	if protocol == pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED {
		protocol = pb.DataProtocol_DATA_PROTOCOL_TCP
	}
	connectionOrigin := req.GetConnectionOrigin()
	if connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_UNSPECIFIED {
		connectionOrigin = pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
	}
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	if ttl <= 0 {
		ttl = defaultTransferEndpointTTL
	}
	jobID := strings.TrimSpace(req.GetJobId())
	if jobID == "" {
		jobID = uuid.NewString()
	}
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		sessionID = jobID
	}
	var lease *dataport.Lease
	dataEndpoint := cloneEndpoint(req.GetBind())
	needsListener := req.GetRole() == pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION &&
		connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE &&
		(protocol == pb.DataProtocol_DATA_PROTOCOL_TCP || protocol == pb.DataProtocol_DATA_PROTOCOL_UDP)
	needsReverseSourceListener := req.GetRole() == pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE &&
		connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION &&
		protocol == pb.DataProtocol_DATA_PROTOCOL_TCP
	if needsListener || needsReverseSourceListener {
		if m.portErr != nil {
			return nil, m.portErr
		}
		var err error
		if protocol == pb.DataProtocol_DATA_PROTOCOL_UDP {
			lease, err = m.ports.AllocateUDP()
		} else {
			lease, err = m.ports.AllocateTCP()
		}
		if err != nil {
			return nil, err
		}
		dataEndpoint = &pb.DataEndpoint{Host: lease.AdvertiseHost, Port: uint32(lease.Port)}
	}
	if req.GetRole() == pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION &&
		connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION &&
		protocol == pb.DataProtocol_DATA_PROTOCOL_TCP {
		dataEndpoint = cloneEndpoint(req.GetBind())
		if dataEndpoint == nil || strings.TrimSpace(dataEndpoint.GetHost()) == "" || dataEndpoint.GetPort() == 0 {
			return nil, errors.New("destination-origin tcp endpoint requires source bind endpoint")
		}
	}
	endpoint := &pb.TransferEndpoint{
		EndpointId:    uuid.NewString(),
		RouteId:       strings.TrimSpace(req.GetRouteId()),
		JobId:         jobID,
		Role:          req.GetRole(),
		Protocol:      protocol,
		DataEndpoint:  dataEndpoint,
		RootPath:      root,
		TtlSeconds:    uint32(ttl / time.Second),
		ExpiresAtUnix: time.Now().Add(ttl).Unix(),
		SessionId:     sessionID,
	}
	var reverse *reverseTCPPool
	if lease != nil && needsReverseSourceListener {
		reverse = newReverseTCPPool(lease.TCPListener)
		m.mu.Lock()
		m.reverseTCP[sessionID] = reverse
		m.mu.Unlock()
		go reverse.acceptLoop()
	} else if req.GetRole() == pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION &&
		connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION &&
		protocol == pb.DataProtocol_DATA_PROTOCOL_TCP {
		go m.receiver.connectDestinationOriginTCPReceivers(context.Background(), dataEndpoint, root)
	} else if lease != nil && protocol == pb.DataProtocol_DATA_PROTOCOL_TCP {
		go m.receiver.serveTransferEndpointTCP(lease.TCPListener, root)
	} else if lease != nil && protocol == pb.DataProtocol_DATA_PROTOCOL_UDP {
		go m.receiver.serveTransferEndpointUDP(lease.UDPConn, root, m.runtimeCfg.UDP)
	}
	m.mu.Lock()
	m.endpoints[endpoint.EndpointId] = &preparedTransferEndpoint{endpoint: endpoint, expires: time.Now().Add(ttl), lease: lease, reverse: reverse}
	m.mu.Unlock()
	return cloneTransferEndpoint(endpoint), nil
}

func (m *transferEndpointRegistry) GetEndpoint(id string) (*pb.TransferEndpoint, bool) {
	m.mu.RLock()
	item := m.endpoints[strings.TrimSpace(id)]
	m.mu.RUnlock()
	if item == nil {
		return nil, false
	}
	if time.Now().After(item.expires) {
		m.expireEndpoints(time.Now())
		return nil, false
	}
	return cloneTransferEndpoint(item.endpoint), true
}

func (m *transferEndpointRegistry) expiryLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-ticker.C:
			m.expireEndpoints(now)
		}
	}
}

func (m *transferEndpointRegistry) expireEndpoints(now time.Time) {
	var expired []*preparedTransferEndpoint
	m.mu.Lock()
	for id, endpoint := range m.endpoints {
		if !endpoint.expires.IsZero() && !now.Before(endpoint.expires) {
			delete(m.endpoints, id)
			if endpoint.endpoint != nil {
				delete(m.reverseTCP, strings.TrimSpace(endpoint.endpoint.GetJobId()))
				delete(m.reverseTCP, strings.TrimSpace(endpoint.endpoint.GetSessionId()))
			}
			expired = append(expired, endpoint)
		}
	}
	m.mu.Unlock()
	for _, endpoint := range expired {
		closePreparedTransferEndpoint(endpoint)
	}
}

func closePreparedTransferEndpoint(endpoint *preparedTransferEndpoint) {
	if endpoint != nil && endpoint.reverse != nil {
		endpoint.reverse.close()
	}
	if endpoint != nil && endpoint.lease != nil {
		_ = endpoint.lease.Close()
	}
}

func newReverseTCPPool(listener *net.TCPListener) *reverseTCPPool {
	return &reverseTCPPool{
		listener: listener,
		conns:    make(chan net.Conn, 128),
		done:     make(chan struct{}),
	}
}

func (p *reverseTCPPool) acceptLoop() {
	if p == nil || p.listener == nil {
		return
	}
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			p.close()
			return
		}
		select {
		case p.conns <- conn:
		case <-p.done:
			_ = conn.Close()
			return
		}
	}
}

func (p *reverseTCPPool) take(ctx context.Context) (net.Conn, error) {
	if p == nil {
		return nil, errors.New("reverse tcp pool is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case conn := <-p.conns:
		if conn == nil {
			return nil, errors.New("reverse tcp connection closed")
		}
		return conn, nil
	case <-p.done:
		return nil, errors.New("reverse tcp pool closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, errors.New("timed out waiting for destination-origin tcp connection")
	}
}

func (p *reverseTCPPool) close() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		close(p.done)
		if p.listener != nil {
			_ = p.listener.Close()
		}
		for {
			select {
			case conn := <-p.conns:
				if conn != nil {
					_ = conn.Close()
				}
			default:
				return
			}
		}
	})
}

func (m *transferEndpointRegistry) takeReverseTCPConn(ctx context.Context, jobID string) (net.Conn, error) {
	m.mu.RLock()
	pool := m.reverseTCP[strings.TrimSpace(jobID)]
	m.mu.RUnlock()
	if pool == nil {
		return nil, fmt.Errorf("reverse tcp route session for job %q not found", strings.TrimSpace(jobID))
	}
	return pool.take(ctx)
}

func (m *destinationReceiver) connectDestinationOriginTCPReceivers(ctx context.Context, source *pb.DataEndpoint, root string) {
	if source == nil || strings.TrimSpace(source.GetHost()) == "" || source.GetPort() == 0 {
		return
	}
	target := net.JoinHostPort(source.GetHost(), fmt.Sprintf("%d", source.GetPort()))
	for i := 0; i < defaultReverseTCPConnections; i++ {
		go func() {
			dialer := net.Dialer{Timeout: 10 * time.Second}
			conn, err := dialer.DialContext(ctx, "tcp", target)
			if err != nil {
				internal.Warn("destination-origin tcp dial failed", internal.Fields{
					internal.FieldError: err.Error(),
					"source":            target,
				})
				return
			}
			m.receiveTransferFileTCP(conn, root)
		}()
	}
}

func cloneTransferEndpoint(ep *pb.TransferEndpoint) *pb.TransferEndpoint {
	if ep == nil {
		return nil
	}
	return &pb.TransferEndpoint{
		EndpointId:    ep.GetEndpointId(),
		RouteId:       ep.GetRouteId(),
		JobId:         ep.GetJobId(),
		Role:          ep.GetRole(),
		Protocol:      ep.GetProtocol(),
		DataEndpoint:  cloneEndpoint(ep.GetDataEndpoint()),
		RootPath:      ep.GetRootPath(),
		TtlSeconds:    ep.GetTtlSeconds(),
		ExpiresAtUnix: ep.GetExpiresAtUnix(),
		SessionId:     ep.GetSessionId(),
	}
}

var _ EndpointRegistry = (*transferEndpointRegistry)(nil)

var _ SourcePlanner = FilesystemSourcePlanner{}
var _ FileExecutor = ProtocolExecutor{}
