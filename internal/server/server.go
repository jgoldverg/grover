// Package server wires groverd: it owns the component instances and exposes
// them over gRPC. Service methods are thin adapters; behaviour lives in the
// transfer, relay, routes, session, and history packages.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/jgoldverg/grover/internal/config"
	"github.com/jgoldverg/grover/internal/dataport"
	"github.com/jgoldverg/grover/internal/energy"
	"github.com/jgoldverg/grover/internal/history"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/relay"
	"github.com/jgoldverg/grover/internal/routes"
	"github.com/jgoldverg/grover/internal/session"
	"github.com/jgoldverg/grover/internal/transfer"
)

type Server struct {
	cfg       config.Server
	grpc      *grpc.Server
	Transfers *transfer.Manager
	Relays    *relay.Manager
	Routes    *routes.Store
	Sessions  *session.Store
	History   *history.Reader
}

// New builds every component. monitor may be nil (energy monitoring off).
func New(cfg config.Server, monitor *energy.RAPLMonitor) (*Server, error) {
	ports, err := dataport.New(dataport.Options{
		BindHost: cfg.DataBindHost, AdvertiseHost: cfg.DataAdvertiseHost,
		MinPort: cfg.DataPortMin, MaxPort: cfg.DataPortMax,
	})
	if err != nil {
		return nil, err
	}
	routeStore, err := routes.Open(cfg.RouteStoreFile)
	if err != nil {
		return nil, err
	}
	sessions := session.NewStore()
	transfers, err := transfer.NewManager(transfer.Options{
		Ports: ports, Tuning: cfg.Tuning, JobLogDir: cfg.JobLogDir, Sessions: sessions,
		Energy: monitor, EnergyEvery: time.Duration(cfg.EnergySampleMs) * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}
	var opts []grpc.ServerOption
	if !cfg.InsecureControl {
		creds, err := credentials.NewServerTLSFromFile(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			transfers.Close()
			return nil, fmt.Errorf("load TLS cert %s / key %s: %w (use --insecure-control for local testing)", cfg.CertFile, cfg.KeyFile, err)
		}
		opts = append(opts, grpc.Creds(creds))
	}
	opts = append(opts, grpc.ChainUnaryInterceptor(unaryInterceptor), grpc.ChainStreamInterceptor(streamInterceptor))
	s := &Server{
		cfg:       cfg,
		grpc:      grpc.NewServer(opts...),
		Transfers: transfers,
		Relays:    relay.NewManager(ports),
		Routes:    routeStore,
		Sessions:  sessions,
		History:   history.NewReader(cfg.JobLogDir),
	}
	pb.RegisterTransferControlServer(s.grpc, &transferService{s: s})
	pb.RegisterRelayControlServer(s.grpc, &relayService{s: s})
	pb.RegisterRouteControlServer(s.grpc, &routeService{s: s})
	pb.RegisterSessionControlServer(s.grpc, &sessionService{s: s})
	pb.RegisterHistoryControlServer(s.grpc, &historyService{s: s})
	reflection.Register(s.grpc)
	return s, nil
}

// Run listens on the configured port and serves until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(s.cfg.Port)))
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Serve serves on ln until ctx is cancelled, then shuts down gracefully.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	slog.Info("groverd listening", "addr", ln.Addr().String(), "tls", !s.cfg.InsecureControl,
		"data_advertise_host", s.cfg.DataAdvertiseHost, "job_log_dir", s.cfg.JobLogDir, "pid", os.Getpid())
	errc := make(chan error, 1)
	go func() { errc <- s.grpc.Serve(ln) }()
	select {
	case err := <-errc:
		s.shutdown()
		return err
	case <-ctx.Done():
	}
	slog.Info("groverd shutting down")
	stopped := make(chan struct{})
	go func() { s.grpc.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		slog.Warn("graceful stop timed out; closing open streams")
		s.grpc.Stop()
	}
	s.shutdown()
	return nil
}

func (s *Server) shutdown() {
	s.Transfers.Close()
	s.Relays.Close()
}

// toStatus maps component errors to gRPC codes so clients get NotFound,
// InvalidArgument, etc. instead of Unknown.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	code := codes.Unknown
	switch {
	case errors.Is(err, transfer.ErrNotFound), errors.Is(err, relay.ErrNotFound), errors.Is(err, routes.ErrNotFound),
		errors.Is(err, session.ErrNotFound), errors.Is(err, history.ErrNotFound), errors.Is(err, os.ErrNotExist):
		code = codes.NotFound
	case errors.Is(err, transfer.ErrInvalid), errors.Is(err, routes.ErrInvalid), errors.Is(err, session.ErrInvalid):
		code = codes.InvalidArgument
	case errors.Is(err, session.ErrBusy):
		code = codes.FailedPrecondition
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	}
	return status.Error(code, err.Error())
}

func unaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	err = toStatus(err)
	logRPC(info.FullMethod, start, err)
	return resp, err
}

func streamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := toStatus(handler(srv, ss))
	if status.Code(err) == codes.Canceled {
		err = nil // client went away; normal for watch streams
	}
	logRPC(info.FullMethod, start, err)
	return err
}

func logRPC(method string, start time.Time, err error) {
	if err != nil {
		slog.Warn("rpc failed", "method", method, "ms", time.Since(start).Milliseconds(), "code", status.Code(err).String(), "error", status.Convert(err).Message())
		return
	}
	slog.Debug("rpc", "method", method, "ms", time.Since(start).Milliseconds())
}
