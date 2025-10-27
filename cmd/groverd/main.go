package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/jgoldverg/grover/internal"
	gs "github.com/jgoldverg/grover/pkg/gserver"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server
	cfg, err := internal.LoadServerConfig("")
	if err != nil {
		internal.Error("failed to load server config", internal.Fields{
			internal.FieldError: err.Error(),
		})
		return
	}
	if err := internal.ConfigureLogger(cfg.LogLevel); err != nil {
		internal.Warn("invalid log level in server config, defaulting to info", internal.Fields{
			internal.FieldError: err.Error(),
		})
	}
	server := gs.NewGroverServer(ctx, cfg)
	go func() {
		if err := server.StartServer(ctx); err != nil {
			internal.Error("grover server exited with error", internal.Fields{
				internal.FieldError: err.Error(),
			})
			cancel()
		}
	}()

	// Wait for shutdown signal
	select {
	case <-ctx.Done():
		internal.Info("context cancelled - shutting down", nil)
	case sig := <-sigChan:
		internal.Info("received shutdown signal", internal.Fields{
			"signal": sig.String(),
		})
		cancel()
	}
	internal.Info("grover-server shutdown complete", nil)
}
