// Package gserver package is where the grover server code is stored
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
	groverPb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type GroverServer struct {
	grpcServer   *grpc.Server
	listener     net.Listener
	config       *internal.ServerConfig
	ctx          context.Context
	shutdownChan chan struct{}
}

func NewGroverServer(ctx context.Context, serverConfig *internal.ServerConfig) *GroverServer {
	var opts []grpc.ServerOption
	if !serverConfig.InsecureControl {
		certs, err := serverConfig.LoadTLSCredentials()
		if err != nil {
			internal.Error("failed to load grover certificates", internal.Fields{
				internal.ServerCertificatePath: serverConfig.ServerCertificatePath,
				internal.ServerKeyPath:         serverConfig.ServerKeyPath,
				internal.FieldError:            err.Error(),
			})
		}
		if certs != nil {
			opts = append(opts, grpc.Creds(certs))
		}
	} else {
		internal.Warn("starting control-plane gRPC in insecure mode (no TLS)", nil)
	}
	server := grpc.NewServer(opts...)
	reflection.Register(server)
	store, err := backend.NewTomlCredentialStorage(serverConfig.CredentialsFile)
	if err != nil {
		internal.Error("failed to load grover store", internal.Fields{
			"credential_path": serverConfig.ServerCertificatePath,
		})
	}
	fs, _ := NewFileService(serverConfig)
	cs := NewCredentialOps(store)
	relayControl, err := NewRelayControlService(serverConfig)
	if err != nil {
		internal.Error("failed to initialize relay control service", internal.Fields{
			internal.FieldError: err.Error(),
		})
		relayControl = &RelayControlService{}
	}

	groverPb.RegisterFileServiceServer(server, fs)
	groverPb.RegisterCredentialServiceServer(server, cs)
	groverPb.RegisterRelayControlServer(server, relayControl)
	groverPb.RegisterTransferJobControlServer(server, NewTransferJobControlService(serverConfig))

	return &GroverServer{
		config:       serverConfig,
		grpcServer:   server,
		ctx:          ctx,
		shutdownChan: make(chan struct{}),
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
