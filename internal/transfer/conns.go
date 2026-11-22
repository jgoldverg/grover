package transfer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/wire"
)

// connSource hands out authenticated TCP data connections on which this node
// is the sender. Get blocks until a connection is available; Put returns a
// healthy connection for reuse; Discard drops a broken one.
type connSource interface {
	Get(ctx context.Context) (net.Conn, error)
	Put(net.Conn)
	Discard(net.Conn)
	Close()
}

// dialPool dials the destination (origin SOURCE) and keeps idle connections
// for reuse across files of the same job.
type dialPool struct {
	addr   string
	token  []byte
	mu     sync.Mutex
	idle   []net.Conn
	closed bool
}

func newDialPool(ep *pb.DataEndpoint, token []byte) *dialPool {
	return &dialPool{addr: endpointAddr(ep), token: token}
}

func (p *dialPool) Get(ctx context.Context) (net.Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("connection pool closed")
	}
	if n := len(p.idle); n > 0 {
		c := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	d := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	c, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return nil, fmt.Errorf("dial data endpoint %s: %w", p.addr, err)
	}
	if err := clientHandshake(c, p.token); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("data endpoint %s: %w", p.addr, err)
	}
	return c, nil
}

func (p *dialPool) Put(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = c.Close()
		return
	}
	p.idle = append(p.idle, c)
}

func (p *dialPool) Discard(c net.Conn) { _ = c.Close() }

func (p *dialPool) Close() {
	p.mu.Lock()
	idle := p.idle
	p.idle, p.closed = nil, true
	p.mu.Unlock()
	for _, c := range idle {
		_ = c.Close()
	}
}

// reversePool holds connections the destination dialed in to us (origin
// DESTINATION). It outlives jobs: it belongs to the session's source endpoint.
type reversePool struct {
	conns chan net.Conn
	live  atomic.Int32
	done  chan struct{}
	once  sync.Once
}

// reverseWaitLimit is how long Get waits when the destination has no live
// connections at all, i.e. it is probably not dialing in.
const reverseWaitLimit = 30 * time.Second

func newReversePool() *reversePool {
	return &reversePool{conns: make(chan net.Conn, 4096), done: make(chan struct{})}
}

func (p *reversePool) add(c net.Conn) {
	p.live.Add(1)
	select {
	case p.conns <- c:
	case <-p.done:
		p.Discard(c)
	}
}

func (p *reversePool) Get(ctx context.Context) (net.Conn, error) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	start := time.Now()
	for {
		select {
		case c := <-p.conns:
			return c, nil
		case <-p.done:
			return nil, errors.New("destination-origin session closed")
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tick.C:
			if p.live.Load() > 0 {
				start = time.Now() // connections exist, they are just busy
			} else if time.Since(start) > reverseWaitLimit {
				return nil, fmt.Errorf("no destination-origin connections for %s; is the destination node dialing in?", reverseWaitLimit)
			}
		}
	}
}

func (p *reversePool) Put(c net.Conn) {
	select {
	case <-p.done:
		p.Discard(c)
	case p.conns <- c:
	}
}

func (p *reversePool) Discard(c net.Conn) {
	_ = c.Close()
	p.live.Add(-1)
}

// Close is a no-op for jobs: the endpoint owns the pool's lifetime.
func (p *reversePool) Close() {}

func (p *reversePool) shutdown() {
	p.once.Do(func() {
		close(p.done)
		for {
			select {
			case c := <-p.conns:
				p.Discard(c)
			default:
				return
			}
		}
	})
}

// clientHandshake is run by whichever side dialed.
func clientHandshake(c net.Conn, token []byte) error {
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	defer c.SetDeadline(time.Time{}) //nolint:errcheck
	if err := wire.WriteHello(c, token); err != nil {
		return err
	}
	return wire.ReadHelloStatus(c)
}

// serverHandshake is run by whichever side accepted.
func serverHandshake(c net.Conn, token []byte) error {
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	defer c.SetDeadline(time.Time{}) //nolint:errcheck
	got, err := wire.ReadHello(c)
	if err != nil {
		return err
	}
	ok := wire.TokenEqual(got, token)
	if err := wire.WriteHelloStatus(c, ok); err != nil {
		return err
	}
	if !ok {
		return errors.New("rejected data connection with bad token")
	}
	return nil
}

func endpointAddr(ep *pb.DataEndpoint) string {
	return net.JoinHostPort(ep.GetHost(), fmt.Sprint(ep.GetPort()))
}
