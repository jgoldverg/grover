package transfer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/jgoldverg/grover/internal/dataport"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/tuning"
	"github.com/jgoldverg/grover/internal/wire"
)

const defaultReverseConnections = 16

// endpoints owns prepared data-plane resources. Endpoints live until closed
// (or groverd exits); they never expire on their own.
type endpoints struct {
	ports  *dataport.Allocator
	recv   *receiver
	tuning tuning.Tuning

	mu   sync.Mutex
	byID map[string]*endpoint
}

type endpoint struct {
	pb      *pb.TransferEndpoint
	lease   *dataport.Lease
	reverse *reversePool // SOURCE + origin DESTINATION
	cancel  context.CancelFunc
}

func newEndpoints(ports *dataport.Allocator, recv *receiver, t tuning.Tuning) *endpoints {
	return &endpoints{ports: ports, recv: recv, tuning: t, byID: map[string]*endpoint{}}
}

// Prepare creates one endpoint. See pb.TransferEndpoint for the three kinds.
func (m *endpoints) Prepare(req *pb.PrepareEndpointRequest) (*pb.TransferEndpoint, error) {
	protocol := req.GetProtocol()
	if protocol == pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED {
		protocol = pb.DataProtocol_DATA_PROTOCOL_TCP
	}
	origin := req.GetConnectFrom()
	if origin == pb.ConnectionOrigin_CONNECTION_ORIGIN_UNSPECIFIED {
		origin = pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
	}
	if origin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION && protocol != pb.DataProtocol_DATA_PROTOCOL_TCP {
		return nil, errors.New("connect_from=destination is only supported for tcp")
	}
	ctx, cancel := context.WithCancel(context.Background())
	ep := &endpoint{cancel: cancel, pb: &pb.TransferEndpoint{
		EndpointId:        uuid.NewString(),
		SessionId:         req.GetSessionId(),
		RouteId:           req.GetRouteId(),
		Role:              req.GetRole(),
		Protocol:          protocol,
		ConnectFrom:       origin,
		CreatedAtUnixNano: time.Now().UnixNano(),
	}}
	var err error
	switch {
	case req.GetRole() == pb.EndpointRole_ENDPOINT_ROLE_DESTINATION && origin == pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE:
		err = m.listenDestination(ctx, ep)
	case req.GetRole() == pb.EndpointRole_ENDPOINT_ROLE_SOURCE && origin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION:
		err = m.listenReverse(ep)
	case req.GetRole() == pb.EndpointRole_ENDPOINT_ROLE_DESTINATION && origin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION:
		err = m.dialSource(ctx, ep, req)
	default:
		err = fmt.Errorf("nothing to prepare for role %s with connect_from %s", req.GetRole(), origin)
	}
	if err != nil {
		cancel()
		return nil, err
	}
	m.mu.Lock()
	m.byID[ep.pb.EndpointId] = ep
	m.mu.Unlock()
	slog.Info("endpoint prepared", "endpoint_id", ep.pb.EndpointId, "session_id", ep.pb.SessionId,
		"role", ep.pb.Role.String(), "protocol", protocol.String(), "connect_from", origin.String(), "data", endpointAddr(ep.pb.Data))
	return proto2(ep.pb), nil
}

// listenDestination: senders dial us.
func (m *endpoints) listenDestination(ctx context.Context, ep *endpoint) error {
	ep.pb.Token = wire.NewToken()
	var err error
	if ep.pb.Protocol == pb.DataProtocol_DATA_PROTOCOL_UDP {
		ep.lease, err = m.ports.AllocateUDP()
		if err != nil {
			return err
		}
		go newUDPEndpoint(ctx, ep.lease.UDP, ep.pb.Token, m.recv, m.tuning).serve()
	} else {
		ep.lease, err = m.ports.AllocateTCP()
		if err != nil {
			return err
		}
		go acceptLoop(ep.lease.TCP, func(c net.Conn) {
			if err := serverHandshake(c, ep.pb.Token); err != nil {
				slog.Warn("tcp data connection rejected", "remote", c.RemoteAddr().String(), "error", err)
				_ = c.Close()
				return
			}
			m.recv.serveRanges(c)
		})
	}
	host, port := ep.lease.Endpoint()
	ep.pb.Data = &pb.DataEndpoint{Host: host, Port: port}
	return nil
}

// listenReverse: the destination dials us and we send on its connections.
func (m *endpoints) listenReverse(ep *endpoint) error {
	lease, err := m.ports.AllocateTCP()
	if err != nil {
		return err
	}
	ep.lease = lease
	ep.pb.Token = wire.NewToken()
	ep.reverse = newReversePool()
	host, port := lease.Endpoint()
	ep.pb.Data = &pb.DataEndpoint{Host: host, Port: port}
	go acceptLoop(lease.TCP, func(c net.Conn) {
		if err := serverHandshake(c, ep.pb.Token); err != nil {
			slog.Warn("destination-origin connection rejected", "remote", c.RemoteAddr().String(), "error", err)
			_ = c.Close()
			return
		}
		ep.reverse.add(c)
	})
	return nil
}

// dialSource: we are a destination that can only dial out. Keep `connections`
// connections open to the source (or the first relay toward it), receiving
// ranges on each and redialing whenever one drops.
func (m *endpoints) dialSource(ctx context.Context, ep *endpoint, req *pb.PrepareEndpointRequest) error {
	dial := req.GetDial()
	if dial.GetHost() == "" || dial.GetPort() == 0 {
		return errors.New("connect_from=destination requires the source endpoint to dial")
	}
	if len(req.GetToken()) != wire.TokenLen {
		return errors.New("connect_from=destination requires the source endpoint token")
	}
	n := int(req.GetConnections())
	if n <= 0 {
		n = defaultReverseConnections
	}
	ep.pb.Data = proto.Clone(dial).(*pb.DataEndpoint)
	ep.pb.Token = append([]byte(nil), req.GetToken()...)
	addr := endpointAddr(dial)
	for i := 0; i < n; i++ {
		go func() {
			backoff := 100 * time.Millisecond
			for ctx.Err() == nil {
				d := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
				c, err := d.DialContext(ctx, "tcp", addr)
				if err == nil {
					if err = clientHandshake(c, ep.pb.Token); err == nil {
						backoff = 100 * time.Millisecond
						stop := context.AfterFunc(ctx, func() { _ = c.Close() })
						m.recv.serveRanges(c) // returns when the connection closes
						stop()
						continue
					}
					_ = c.Close()
				}
				if ctx.Err() != nil {
					return
				}
				slog.Debug("destination-origin dial failed", "endpoint_id", ep.pb.EndpointId, "source", addr, "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, 5*time.Second)
			}
		}()
	}
	return nil
}

func acceptLoop(ln *net.TCPListener, handle func(net.Conn)) {
	for {
		c, err := ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				slog.Warn("data listener stopped", "addr", ln.Addr().String(), "error", err)
			}
			return
		}
		go handle(c)
	}
}

// Close releases an endpoint. Closing an unknown id is not an error.
func (m *endpoints) Close(id string) bool {
	m.mu.Lock()
	ep := m.byID[id]
	delete(m.byID, id)
	m.mu.Unlock()
	if ep == nil {
		return false
	}
	ep.close()
	slog.Info("endpoint closed", "endpoint_id", id, "session_id", ep.pb.SessionId)
	return true
}

func (ep *endpoint) close() {
	ep.cancel()
	if ep.reverse != nil {
		ep.reverse.shutdown()
	}
	if ep.lease != nil {
		_ = ep.lease.Close()
	}
}

func (m *endpoints) reversePool(id string) (*reversePool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ep := m.byID[id]
	if ep == nil || ep.reverse == nil {
		return nil, fmt.Errorf("source endpoint %q not found on this node; prepare the route again", id)
	}
	return ep.reverse, nil
}

func (m *endpoints) closeAll() {
	m.mu.Lock()
	all := m.byID
	m.byID = map[string]*endpoint{}
	m.mu.Unlock()
	for _, ep := range all {
		ep.close()
	}
}

func proto2(ep *pb.TransferEndpoint) *pb.TransferEndpoint {
	return proto.Clone(ep).(*pb.TransferEndpoint)
}
