package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/jgoldverg/grover/config"
	gs "github.com/jgoldverg/grover/server"
	"github.com/jgoldverg/grover/server/log"
	"github.com/pterm/pterm"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server
	cfg, err := config.LoadServerConfig("")
	if err != nil {

	}
	server := gs.NewGroverServer(cfg)
	go func() {
		if err := server.StartServer(ctx); err != nil {
			log.Structured(&pterm.Error, "Server error %v", log.Fields{
				"error": err,
			})
			cancel()
		}
	}()

	// Wait for shutdown signal
	select {
	case <-ctx.Done():
		log.Structured(&pterm.Info, "Context cancelled - shutting down", nil)
	case sig := <-sigChan:
		log.Structured(&pterm.Info, "Received %s - shutting down", log.Fields{
			"signal": sig.String(),
		})
		cancel()
	}
	log.Structured(&pterm.Info, "Grover-server shutdown complete", nil)
}
