package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jgoldverg/grover/internal"
	gs "github.com/jgoldverg/grover/pkg/gserver"
)

func main() {
	var (
		configPath      string
		serverConfig    string
		port            int
		protocol        string
		insecureControl bool
		credentialsFile string
		logLevel        string
		certFile        string
		keyFile         string
	)
	flag.StringVar(&configPath, "config", "", "Path to server config file (TOML)")
	flag.StringVar(&serverConfig, "server-config", "", "Path to server config file (TOML)")
	flag.IntVar(&port, "port", 0, "gRPC control-plane listen port")
	flag.StringVar(&protocol, "protocol", "", "Transfer data-plane protocol (udp|tcp)")
	flag.BoolVar(&insecureControl, "insecure-control", false, "Start gRPC control-plane without TLS")
	flag.StringVar(&credentialsFile, "credentials-file", "", "Path to credential store file")
	flag.StringVar(&logLevel, "log-level", "", "Log level")
	flag.StringVar(&certFile, "server-cert", "", "Path to TLS certificate PEM")
	flag.StringVar(&keyFile, "server-key", "", "Path to TLS private key PEM")
	flag.Parse()
	if strings.TrimSpace(serverConfig) != "" {
		configPath = serverConfig
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server
	cfg, err := internal.LoadServerConfig(configPath)
	if err != nil {
		internal.Error("failed to load server config", internal.Fields{
			internal.FieldError: err.Error(),
		})
		return
	}
	if port != 0 {
		cfg.Port = port
	}
	if strings.TrimSpace(protocol) != "" {
		p := strings.ToLower(strings.TrimSpace(protocol))
		if p != "udp" && p != "tcp" {
			fmt.Fprintf(os.Stderr, "invalid --protocol %q: must be udp or tcp\n", protocol)
			os.Exit(2)
		}
		cfg.TransferProtocol = p
	}
	if insecureControl {
		cfg.InsecureControl = true
	}
	if strings.TrimSpace(credentialsFile) != "" {
		cfg.CredentialsFile = credentialsFile
	}
	if strings.TrimSpace(logLevel) != "" {
		cfg.LogLevel = logLevel
	}
	if strings.TrimSpace(certFile) != "" {
		cfg.ServerCertificatePath = certFile
	}
	if strings.TrimSpace(keyFile) != "" {
		cfg.ServerKeyPath = keyFile
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
