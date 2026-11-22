// Package dataport hands out TCP listeners and UDP sockets for the data plane.
// Leases come from [min,max] when a range is configured (so firewalls can be
// opened for a known range), otherwise from OS ephemeral ports. It has no job
// or file semantics; transfer endpoints and relay forwards both use it.
package dataport

import (
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/jgoldverg/grover/internal/config"
)

// Options configures an Allocator.
type Options struct {
	BindHost      string // local address to bind, e.g. 0.0.0.0
	AdvertiseHost string // address peers should dial
	MinPort       int    // 0,0 means OS ephemeral ports
	MaxPort       int
}

type Allocator struct {
	opts  Options
	mu    sync.Mutex
	inUse map[int]struct{}
}

// Lease is one bound socket. Exactly one of TCP or UDP is set.
type Lease struct {
	AdvertiseHost string
	Port          int
	TCP           *net.TCPListener
	UDP           *net.UDPConn

	alloc *Allocator
	once  sync.Once
}

func New(opts Options) (*Allocator, error) {
	if opts.BindHost == "" {
		opts.BindHost = "0.0.0.0"
	}
	if opts.AdvertiseHost == "" {
		opts.AdvertiseHost = "127.0.0.1"
	}
	if err := config.ValidatePortRange(opts.MinPort, opts.MaxPort); err != nil {
		return nil, err
	}
	return &Allocator{opts: opts, inUse: make(map[int]struct{})}, nil
}

func (a *Allocator) AllocateTCP() (*Lease, error) {
	return a.allocate("tcp", func(port int) (*Lease, error) {
		ln, err := net.Listen("tcp", net.JoinHostPort(a.opts.BindHost, strconv.Itoa(port)))
		if err != nil {
			return nil, err
		}
		tcp := ln.(*net.TCPListener)
		return &Lease{TCP: tcp, Port: tcp.Addr().(*net.TCPAddr).Port}, nil
	})
}

func (a *Allocator) AllocateUDP() (*Lease, error) {
	return a.allocate("udp", func(port int) (*Lease, error) {
		addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(a.opts.BindHost, strconv.Itoa(port)))
		if err != nil {
			return nil, err
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			return nil, err
		}
		return &Lease{UDP: conn, Port: conn.LocalAddr().(*net.UDPAddr).Port}, nil
	})
}

func (a *Allocator) allocate(network string, bind func(port int) (*Lease, error)) (*Lease, error) {
	finish := func(l *Lease) *Lease {
		l.AdvertiseHost = a.opts.AdvertiseHost
		l.alloc = a
		return l
	}
	if a.opts.MinPort == 0 {
		l, err := bind(0)
		if err != nil {
			return nil, fmt.Errorf("bind %s data port: %w", network, err)
		}
		return finish(l), nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for port := a.opts.MinPort; port <= a.opts.MaxPort; port++ {
		if _, busy := a.inUse[port]; busy {
			continue
		}
		l, err := bind(port)
		if err != nil {
			continue // in use by another process
		}
		a.inUse[port] = struct{}{}
		return finish(l), nil
	}
	return nil, fmt.Errorf("no free %s data ports in range %d-%d", network, a.opts.MinPort, a.opts.MaxPort)
}

// Endpoint returns the advertised host and port.
func (l *Lease) Endpoint() (string, uint32) { return l.AdvertiseHost, uint32(l.Port) }

// Close releases the socket and its port. Safe to call more than once.
func (l *Lease) Close() error {
	if l == nil {
		return nil
	}
	var err error
	l.once.Do(func() {
		if l.TCP != nil {
			err = l.TCP.Close()
		}
		if l.UDP != nil {
			err = l.UDP.Close()
		}
		if a := l.alloc; a != nil && a.opts.MinPort != 0 {
			a.mu.Lock()
			delete(a.inUse, l.Port)
			a.mu.Unlock()
		}
	})
	return err
}
