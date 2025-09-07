package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jgoldverg/grover/config"
	"github.com/jgoldverg/grover/pb"
	"github.com/jgoldverg/grover/server/log"
	"github.com/pterm/pterm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type GroverServer struct {
	grpcServer   *grpc.Server
	listener     net.Listener
	config       *config.ServerConfig
	shutdownChan chan struct{}
}

func NewGroverServer(serverConfig *config.ServerConfig) *GroverServer {
	certs, err := serverConfig.LoadTLSCredentials()

	if err != nil {
		log.Structured(&pterm.Error, "failed to load grover certificates", log.Fields{
			log.ServerCertificatePath: serverConfig.ServerCertificatePath,
			log.ServerKeyPath:         serverConfig.ServerKeyPath,
		})
	}
	server := grpc.NewServer(grpc.Creds(certs))
	reflection.Register(server)
	fs, _ := NewFileService(serverConfig)
	cs := NewCredentialOps(serverConfig)

	pb.RegisterFileServiceServer(server, fs)
	pb.RegisterCredentialServiceServer(server, cs)

	return &GroverServer{
		config:       serverConfig,
		grpcServer:   server,
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
		log.Structured(&pterm.Info, "starting grover server", log.Fields{
			log.FieldMsg:              "server started",
			log.FieldPort:             addr,
			log.ServerCertificatePath: gs.config.ServerCertificatePath,
			log.ServerKeyPath:         gs.config.ServerKeyPath,
		})

		if err := gs.grpcServer.Serve(gs.listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Structured(&pterm.Error, "server error", log.Fields{
				log.FieldError: err,
			})
		}
	}()

	// Handle shutdown signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	select {
	case <-ctx.Done(): // External cancellation
		log.Structured(&pterm.Warning, "shutdown initiated via context", nil)
	case sig := <-signalChan: // OS signal
		log.Structured(&pterm.Warning, "shutdown initiated via signal", log.Fields{
			"signal": sig.String(),
		})
	case <-gs.shutdownChan: // Internal shutdown
		log.Structured(&pterm.Warning, "shutdown initiated internally", nil)
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
		log.Structured(&pterm.Error, "graceful shutdown timed out - forcing exit", nil)
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
