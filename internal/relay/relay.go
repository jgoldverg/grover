// Package relay implements dumb forwards on relay nodes: bytes (TCP) or
// datagrams (UDP) from an allocated ingress port to a fixed egress address.
// A forward knows nothing about files or jobs. It lives until deleted or
// until groverd exits.
//
// UDP forwards must send replies back to the right sender. They learn that
// from packet headers only: data/status packets carry a session key and stream
// id (udpwire), control packets carry a session key (wire.RoutingKey).
package relay

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/jgoldverg/grover/internal/dataport"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/udpwire"
	"github.com/jgoldverg/grover/internal/wire"
)

var ErrNotFound = errors.New("forward not found")

type Manager struct {
	ports *dataport.Allocator
	mu    sync.Mutex
	byID  map[string]*forward
}

func NewManager(ports *dataport.Allocator) *Manager {
	return &Manager{ports: ports, byID: map[string]*forward{}}
}

type forward struct {
	pb     *pb.Forward
	lease  *dataport.Lease
	cancel context.CancelFunc

	mu      sync.Mutex
	state   pb.RuntimeState
	errText string

	bytesIn, bytesOut, packets, errors atomic.Uint64
	conns                              atomic.Int32
}

func (m *Manager) Create(req *pb.CreateForwardRequest) (*pb.Forward, error) {
	egress := req.GetEgress()
	if egress.GetHost() == "" || egress.GetPort() == 0 {
		return nil, errors.New("egress host and port are required")
	}
	protocol := req.GetProtocol()
	var (
		lease *dataport.Lease
		err   error
	)
	switch protocol {
	case pb.DataProtocol_DATA_PROTOCOL_TCP, pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED:
		protocol = pb.DataProtocol_DATA_PROTOCOL_TCP
		lease, err = m.ports.AllocateTCP()
	case pb.DataProtocol_DATA_PROTOCOL_UDP:
		lease, err = m.ports.AllocateUDP()
	default:
		return nil, fmt.Errorf("unsupported protocol %s", protocol)
	}
	if err != nil {
		return nil, err
	}
	host, port := lease.Endpoint()
	ctx, cancel := context.WithCancel(context.Background())
	f := &forward{
		lease: lease, cancel: cancel, state: pb.RuntimeState_RUNTIME_STATE_RUNNING,
		pb: &pb.Forward{
			ForwardId:         uuid.NewString(),
			RouteId:           req.GetRouteId(),
			SessionId:         req.GetSessionId(),
			HopIndex:          req.GetHopIndex(),
			Protocol:          protocol,
			Ingress:           &pb.DataEndpoint{Host: host, Port: port},
			Egress:            proto.Clone(egress).(*pb.DataEndpoint),
			CreatedAtUnixNano: time.Now().UnixNano(),
		},
	}
	m.mu.Lock()
	m.byID[f.pb.ForwardId] = f
	m.mu.Unlock()
	if protocol == pb.DataProtocol_DATA_PROTOCOL_UDP {
		go f.runUDP(ctx)
	} else {
		go f.runTCP(ctx)
	}
	slog.Info("forward created", "forward_id", f.pb.ForwardId, "session_id", f.pb.SessionId, "hop", f.pb.HopIndex,
		"protocol", protocol.String(), "ingress", addr(f.pb.Ingress), "egress", addr(f.pb.Egress))
	return f.snapshot(), nil
}

func (m *Manager) Get(id string) (*pb.Forward, error) {
	m.mu.Lock()
	f := m.byID[id]
	m.mu.Unlock()
	if f == nil {
		return nil, fmt.Errorf("%q: %w", id, ErrNotFound)
	}
	return f.snapshot(), nil
}

func (m *Manager) List(routeID, sessionID string) []*pb.Forward {
	m.mu.Lock()
	var out []*pb.Forward
	for _, f := range m.byID {
		if (routeID == "" || f.pb.RouteId == routeID) && (sessionID == "" || f.pb.SessionId == sessionID) {
			out = append(out, f.snapshot())
		}
	}
	m.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAtUnixNano < out[j].CreatedAtUnixNano })
	return out
}

func (m *Manager) Delete(id string) error {
	m.mu.Lock()
	f := m.byID[id]
	delete(m.byID, id)
	m.mu.Unlock()
	if f == nil {
		return fmt.Errorf("%q: %w", id, ErrNotFound)
	}
	f.stop(pb.RuntimeState_RUNTIME_STATE_ABORTED, "")
	slog.Info("forward deleted", "forward_id", id)
	return nil
}

func (m *Manager) Close() {
	m.mu.Lock()
	all := m.byID
	m.byID = map[string]*forward{}
	m.mu.Unlock()
	for _, f := range all {
		f.stop(pb.RuntimeState_RUNTIME_STATE_ABORTED, "")
	}
}

func (f *forward) stop(state pb.RuntimeState, errText string) {
	f.mu.Lock()
	if f.state != pb.RuntimeState_RUNTIME_STATE_RUNNING {
		f.mu.Unlock()
		return
	}
	f.state, f.errText = state, errText
	f.mu.Unlock()
	f.cancel()
	_ = f.lease.Close()
}

func (f *forward) snapshot() *pb.Forward {
	f.mu.Lock()
	state, errText := f.state, f.errText
	f.mu.Unlock()
	out := proto.Clone(f.pb).(*pb.Forward)
	out.State, out.Error = state, errText
	in, outB := f.bytesIn.Load(), f.bytesOut.Load()
	avg := 0.0
	if secs := time.Since(time.Unix(0, f.pb.CreatedAtUnixNano)).Seconds(); secs > 0 {
		avg = float64(in+outB) / secs
	}
	out.Stats = &pb.Stats{
		BytesIn: in, BytesOut: outB, Packets: f.packets.Load(), Errors: f.errors.Load(),
		ActiveConnections: uint32(f.conns.Load()), AverageBps: avg,
	}
	return out
}

func (f *forward) runTCP(ctx context.Context) {
	for {
		in, err := f.lease.TCP.Accept()
		if err != nil {
			if ctx.Err() == nil {
				f.stop(pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
			}
			return
		}
		go f.pipeTCP(ctx, in)
	}
}

func (f *forward) pipeTCP(ctx context.Context, in net.Conn) {
	f.conns.Add(1)
	defer f.conns.Add(-1)
	defer in.Close()
	d := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	out, err := d.DialContext(ctx, "tcp", addr(f.pb.Egress))
	if err != nil {
		f.errors.Add(1)
		slog.Warn("forward dial failed", "forward_id", f.pb.ForwardId, "egress", addr(f.pb.Egress), "error", err)
		return
	}
	defer out.Close()
	stop := context.AfterFunc(ctx, func() { _ = in.Close(); _ = out.Close() })
	defer stop()
	var wg sync.WaitGroup
	copyHalf := func(dst, src net.Conn, counter *atomic.Uint64) {
		defer wg.Done()
		if _, err := io.Copy(dst, &countReader{src, counter}); err != nil && ctx.Err() == nil {
			f.errors.Add(1)
		}
		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
	}
	wg.Add(2)
	go copyHalf(out, in, &f.bytesIn)
	go copyHalf(in, out, &f.bytesOut)
	wg.Wait()
}

type udpKey struct{ session, stream uint32 }

func (f *forward) runUDP(ctx context.Context) {
	ingress := f.lease.UDP
	egressAddr, err := net.ResolveUDPAddr("udp", addr(f.pb.Egress))
	if err != nil {
		f.stop(pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
		return
	}
	egress, err := net.DialUDP("udp", nil, egressAddr)
	if err != nil {
		f.stop(pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
		return
	}
	defer egress.Close()
	stop := context.AfterFunc(ctx, func() { _ = egress.Close() })
	defer stop()

	var (
		mu      sync.RWMutex
		senders = map[uint32]map[uint32]*net.UDPAddr{} // session -> stream -> sender
		last    *net.UDPAddr
	)
	go func() { // sender -> receiver
		buf := make([]byte, 64<<10)
		for {
			n, from, err := ingress.ReadFromUDP(buf)
			if err != nil {
				return
			}
			mu.Lock()
			if k, ok := ingressKey(buf[:n]); ok {
				if senders[k.session] == nil {
					senders[k.session] = map[uint32]*net.UDPAddr{}
				}
				senders[k.session][k.stream] = from
			}
			last = from
			mu.Unlock()
			if _, err := egress.Write(buf[:n]); err != nil {
				f.errors.Add(1)
				continue
			}
			f.bytesIn.Add(uint64(n))
			f.packets.Add(1)
		}
	}()
	buf := make([]byte, 64<<10)
	for { // receiver -> sender
		n, err := egress.Read(buf)
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				f.errors.Add(1)
				continue // e.g. ICMP unreachable while the receiver restarts
			}
			return
		}
		k, keyed := egressKey(buf[:n])
		mu.RLock()
		to := last
		if keyed && senders[k.session][k.stream] != nil {
			to = senders[k.session][k.stream]
		}
		mu.RUnlock()
		if kind, _, ok := wire.PeekUDPControl(buf[:n]); ok && (kind == wire.UDPDone || kind == wire.UDPError) {
			mu.Lock()
			delete(senders, k.session) // a resent START re-registers the sender
			mu.Unlock()
		}
		if to == nil {
			continue
		}
		if _, err := ingress.WriteToUDP(buf[:n], to); err != nil {
			f.errors.Add(1)
			continue
		}
		f.bytesOut.Add(uint64(n))
		f.packets.Add(1)
	}
}

// ingressKey identifies sender->receiver packets: data packets by
// (session, stream), START packets by (session, 0).
func ingressKey(p []byte) (udpKey, bool) {
	if len(p) >= udpwire.DataHeaderLen && udpwire.IsDataPacket(p) {
		return udpKey{binary.BigEndian.Uint32(p[2:6]), binary.BigEndian.Uint32(p[6:10])}, true
	}
	if k, ok := wire.RoutingKey(p, true); ok {
		return udpKey{k, 0}, true
	}
	return udpKey{}, false
}

// egressKey identifies receiver->sender packets: status packets go to the
// stream's socket, control replies to the START sender.
func egressKey(p []byte) (udpKey, bool) {
	if len(p) >= udpwire.StatusHeaderLen && udpwire.IsStatusPacket(p) {
		return udpKey{binary.BigEndian.Uint32(p[2:6]), binary.BigEndian.Uint32(p[6:10])}, true
	}
	if k, ok := wire.RoutingKey(p, false); ok {
		return udpKey{k, 0}, true
	}
	return udpKey{}, false
}

type countReader struct {
	r io.Reader
	n *atomic.Uint64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(uint64(n))
	return n, err
}

func addr(ep *pb.DataEndpoint) string {
	return net.JoinHostPort(ep.GetHost(), fmt.Sprint(ep.GetPort()))
}
