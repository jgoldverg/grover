// Package client dials groverd control endpoints. A Conn exposes the generated
// gRPC clients directly; there is no wrapper layer to keep in sync.
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/jgoldverg/grover/internal/config"
	"github.com/jgoldverg/grover/internal/pb"
)

// Conn is one groverd control connection.
type Conn struct {
	Node      config.Node
	cc        *grpc.ClientConn
	Transfers pb.TransferControlClient
	Relays    pb.RelayControlClient
	Routes    pb.RouteControlClient
	Sessions  pb.SessionControlClient
	History   pb.HistoryControlClient
}

const dialTimeout = 10 * time.Second

// Dial connects to a node and waits until the connection is ready, so
// unreachable nodes fail fast with a clear error.
func Dial(ctx context.Context, node config.Node) (*Conn, error) {
	var creds credentials.TransportCredentials
	if node.Insecure {
		creds = insecure.NewCredentials()
	} else {
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if node.CACert != "" {
			pem, err := os.ReadFile(node.CACert)
			if err != nil {
				return nil, fmt.Errorf("node %s: read CA cert: %w", node.Name, err)
			}
			roots.AppendCertsFromPEM(pem)
		}
		creds = credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})
	}
	cc, err := grpc.NewClient(node.Addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("node %s (%s): %w", node.Name, node.Addr, err)
	}
	if err := waitReady(ctx, cc); err != nil {
		_ = cc.Close()
		hint := ""
		if !node.Insecure {
			hint = " (TLS is on; if the node runs with --insecure-control, set insecure_control for this profile or pass --insecure)"
		}
		return nil, fmt.Errorf("node %s (%s) unreachable: %w%s", node.Name, node.Addr, err, hint)
	}
	return &Conn{
		Node: node, cc: cc,
		Transfers: pb.NewTransferControlClient(cc),
		Relays:    pb.NewRelayControlClient(cc),
		Routes:    pb.NewRouteControlClient(cc),
		Sessions:  pb.NewSessionControlClient(cc),
		History:   pb.NewHistoryControlClient(cc),
	}, nil
}

func (c *Conn) Close() error { return c.cc.Close() }

func waitReady(ctx context.Context, cc *grpc.ClientConn) error {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	cc.Connect()
	for {
		s := cc.GetState()
		switch s {
		case connectivity.Ready:
			return nil
		case connectivity.TransientFailure:
			return errors.New("connection failed (wrong address, node down, or TLS mismatch)")
		}
		if !cc.WaitForStateChange(ctx, s) {
			return ctx.Err()
		}
	}
}

// Pool caches one Conn per node address for the life of a command.
type Pool struct {
	Config config.Client
	mu     sync.Mutex
	conns  map[string]*Conn
}

func NewPool(cfg config.Client) *Pool { return &Pool{Config: cfg, conns: map[string]*Conn{}} }

// Get resolves a profile name or host:port (empty = default node) and dials it.
func (p *Pool) Get(ctx context.Context, nameOrAddr string) (*Conn, error) {
	node, err := p.Config.Resolve(nameOrAddr)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if c := p.conns[node.Addr]; c != nil {
		return c, nil
	}
	c, err := Dial(ctx, node)
	if err != nil {
		return nil, err
	}
	p.conns[node.Addr] = c
	return c, nil
}

// Addr resolves a node name to its control address without dialing.
func (p *Pool) Addr(nameOrAddr string) (string, error) {
	node, err := p.Config.Resolve(nameOrAddr)
	return node.Addr, err
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, c := range p.conns {
		_ = c.Close()
		delete(p.conns, k)
	}
}
