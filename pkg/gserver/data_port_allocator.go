package gserver

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/jgoldverg/grover/internal"
)

type DataPortAllocator struct {
	mu            sync.Mutex
	bindHost      string
	advertiseHost string
	minPort       int
	maxPort       int
	inUse         map[int]struct{}
}

type DataPortLease struct {
	Protocol      string
	BindHost      string
	AdvertiseHost string
	Port          int

	UDPConn     *net.UDPConn
	TCPListener *net.TCPListener

	allocator *DataPortAllocator
	closed    bool
}

func NewDataPortAllocator(cfg *internal.ServerConfig) (*DataPortAllocator, error) {
	bindHost := "0.0.0.0"
	advertiseHost := "127.0.0.1"
	minPort := 0
	maxPort := 0
	if cfg != nil {
		if strings.TrimSpace(cfg.DataBindHost) != "" {
			bindHost = strings.TrimSpace(cfg.DataBindHost)
		}
		if strings.TrimSpace(cfg.DataAdvertiseHost) != "" {
			advertiseHost = strings.TrimSpace(cfg.DataAdvertiseHost)
		}
		minPort = cfg.DataPortMin
		maxPort = cfg.DataPortMax
	}
	if err := internal.ValidateDataPortRange(minPort, maxPort); err != nil {
		return nil, err
	}
	return &DataPortAllocator{
		bindHost:      bindHost,
		advertiseHost: advertiseHost,
		minPort:       minPort,
		maxPort:       maxPort,
		inUse:         make(map[int]struct{}),
	}, nil
}

func (a *DataPortAllocator) AllocateUDP() (*DataPortLease, error) {
	if a == nil {
		return nil, errors.New("nil data port allocator")
	}
	if !a.hasRange() {
		addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(a.bindHost, "0"))
		if err != nil {
			return nil, err
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			return nil, err
		}
		port := conn.LocalAddr().(*net.UDPAddr).Port
		return &DataPortLease{Protocol: "udp", BindHost: a.bindHost, AdvertiseHost: a.advertiseHost, Port: port, UDPConn: conn, allocator: a}, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for port := a.minPort; port <= a.maxPort; port++ {
		if _, ok := a.inUse[port]; ok {
			continue
		}
		addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(a.bindHost, fmt.Sprintf("%d", port)))
		if err != nil {
			return nil, err
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			continue
		}
		a.inUse[port] = struct{}{}
		return &DataPortLease{Protocol: "udp", BindHost: a.bindHost, AdvertiseHost: a.advertiseHost, Port: port, UDPConn: conn, allocator: a}, nil
	}
	return nil, fmt.Errorf("no available udp data ports in range %d-%d", a.minPort, a.maxPort)
}

func (a *DataPortAllocator) AllocateTCP() (*DataPortLease, error) {
	if a == nil {
		return nil, errors.New("nil data port allocator")
	}
	if !a.hasRange() {
		addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(a.bindHost, "0"))
		if err != nil {
			return nil, err
		}
		listener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return nil, err
		}
		port := listener.Addr().(*net.TCPAddr).Port
		return &DataPortLease{Protocol: "tcp", BindHost: a.bindHost, AdvertiseHost: a.advertiseHost, Port: port, TCPListener: listener, allocator: a}, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for port := a.minPort; port <= a.maxPort; port++ {
		if _, ok := a.inUse[port]; ok {
			continue
		}
		addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(a.bindHost, fmt.Sprintf("%d", port)))
		if err != nil {
			return nil, err
		}
		listener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			continue
		}
		a.inUse[port] = struct{}{}
		return &DataPortLease{Protocol: "tcp", BindHost: a.bindHost, AdvertiseHost: a.advertiseHost, Port: port, TCPListener: listener, allocator: a}, nil
	}
	return nil, fmt.Errorf("no available tcp data ports in range %d-%d", a.minPort, a.maxPort)
}

func (a *DataPortAllocator) hasRange() bool {
	return a.minPort > 0 || a.maxPort > 0
}

func (l *DataPortLease) Close() error {
	if l == nil {
		return nil
	}
	if l.closed {
		return nil
	}
	l.closed = true
	var err error
	if l.UDPConn != nil {
		err = l.UDPConn.Close()
	}
	if l.TCPListener != nil {
		if closeErr := l.TCPListener.Close(); err == nil {
			err = closeErr
		}
	}
	if l.allocator != nil && l.allocator.hasRange() {
		l.allocator.release(l.Port)
	}
	return err
}

func (a *DataPortAllocator) release(port int) {
	a.mu.Lock()
	delete(a.inUse, port)
	a.mu.Unlock()
}
