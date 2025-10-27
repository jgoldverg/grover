package gserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg"
	groverPbUdp "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	groverPb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/gserver/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type GroverServer struct {
	grpcServer   *grpc.Server
	listener     net.Listener
	config       *internal.ServerConfig
	ctx          context.Context
	shutdownChan chan struct{}

	registry *backend.TransferRegistry
}

func NewGroverServer(ctx context.Context, serverConfig *internal.ServerConfig) *GroverServer {
	certs, err := serverConfig.LoadTLSCredentials()
	if err != nil {
		internal.Error("failed to load grover certificates", internal.Fields{
			internal.ServerCertificatePath: serverConfig.ServerCertificatePath,
			internal.ServerKeyPath:         serverConfig.ServerKeyPath,
			internal.FieldError:            err.Error(),
		})
	}
	server := grpc.NewServer(grpc.Creds(certs))
	reflection.Register(server)
	opts := pkg.DefaultOptions()
	if serverConfig.UDPReadBufferSize > 0 {
		opts.ReadBufferSize = serverConfig.UDPReadBufferSize
	}
	if serverConfig.UDPWriteBufferSize >= 0 {
		opts.WriteBufferSize = serverConfig.UDPWriteBufferSize
	}
	if serverConfig.UDPPacketWorkers > 0 {
		opts.PacketProcessingWorkers = serverConfig.UDPPacketWorkers
	}
	if serverConfig.UDPReadTimeoutMs >= 0 {
		opts.ReadTimeout = time.Duration(serverConfig.UDPReadTimeoutMs) * time.Millisecond
	}
	if serverConfig.UDPQueueDepth >= 0 {
		opts.QueueDepth = serverConfig.UDPQueueDepth
	}
	lm := pkg.NewListenerManagerWithOptions(opts)
	store, err := backend.NewTomlCredentialStorage(serverConfig.CredentialsFile)
	jobRegistry := backend.NewTransferRegistry(nil)

	fs, _ := control.NewFileService(serverConfig)
	cs := control.NewCredentialOps(store)
	hs := control.NewHeartBeatService(serverConfig)
	ss := NewGroverUdpServer(lm, nil)
	ts := control.NewTransferService(serverConfig, store, jobRegistry, nil)
	err = ss.EnableMtuListener(ctx)
	if err != nil {
		internal.Error("failed to enable mtu listener", internal.Fields{
			internal.FieldError: err.Error(),
		})
	}
	groverPb.RegisterHeartBeatServer(server, hs)
	groverPb.RegisterFileServiceServer(server, fs)
	groverPb.RegisterCredentialServiceServer(server, cs)
	groverPbUdp.RegisterGroverServerServer(server, ss)
	groverPbUdp.RegisterTransferServiceServer(server, ts)

	return &GroverServer{
		config:       serverConfig,
		grpcServer:   server,
		ctx:          ctx,
		shutdownChan: make(chan struct{}),
		registry:     jobRegistry,
	}
}

func (gs *GroverServer) StartServer(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", gs.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	gs.listener = listener

	go func() {
		internal.Info("starting grover server", internal.Fields{
			internal.FieldMsg:  "server started",
			internal.FieldPort: addr,
		})

		if err := gs.grpcServer.Serve(gs.listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			internal.Error("grpc server exited with error", internal.Fields{
				internal.FieldError: err.Error(),
			})
		}
	}()

	// Handle shutdown signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	select {
	case <-ctx.Done(): // External cancellation
		internal.Warn("shutdown initiated via context", nil)
	case sig := <-signalChan: // OS signal
		internal.Warn("shutdown initiated via signal", internal.Fields{
			"signal": sig.String(),
		})
	case <-gs.shutdownChan: // Internal shutdown
		internal.Warn("shutdown initiated internally", nil)
	}

	// Graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stopped := make(chan struct{})
	go func() {
		gs.grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		// Normal shutdown
	case <-shutdownCtx.Done():
		internal.Error("graceful shutdown timed out - forcing exit", nil)
		gs.grpcServer.Stop() // Forceful shutdown
	}

	return nil
}

// Stop triggers a graceful shutdown programmatically
func (gs *GroverServer) Stop() {
	select {
	case <-gs.shutdownChan:
		// Already closed
	default:
		close(gs.shutdownChan)
	}
}
