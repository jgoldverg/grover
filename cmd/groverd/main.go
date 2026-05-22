package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jgoldverg/grover/internal"
	gs "github.com/jgoldverg/grover/pkg/gserver"
	"github.com/spf13/pflag"
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
		dataBindHost    string
		dataAdvertise   string
		dataPortMin     int
		dataPortMax     int
		udpMTU          int
		udpWindow       int
		udpAckPackets   int
		udpAckMs        int
		udpBatchPackets int
		udpReadBuffer   int
		udpWriteBuffer  int
	)
	flags := pflag.NewFlagSet("groverd", pflag.ContinueOnError)
	flags.SortFlags = false
	flags.SetOutput(os.Stderr)
	flags.StringVar(&configPath, "config", "", "Path to server config file (TOML)")
	flags.StringVar(&serverConfig, "server-config", "", "Path to server config file (TOML)")
	flags.IntVar(&port, "port", 0, "gRPC control-plane listen port")
	flags.StringVar(&protocol, "protocol", "", "Transfer data-plane protocol (udp|tcp)")
	flags.BoolVar(&insecureControl, "insecure-control", false, "Start gRPC control-plane without TLS")
	flags.StringVar(&credentialsFile, "credentials-file", "", "Path to credential store file")
	flags.StringVar(&logLevel, "log-level", "", "Log level")
	flags.StringVar(&certFile, "server-cert", "", "Path to TLS certificate PEM")
	flags.StringVar(&keyFile, "server-key", "", "Path to TLS private key PEM")
	flags.StringVar(&dataBindHost, "data-bind-host", "", "Data-plane bind host")
	flags.StringVar(&dataAdvertise, "data-advertise-host", "", "Data-plane advertised host")
	flags.IntVar(&dataPortMin, "data-port-min", 0, "Minimum server-allocated data-plane port (0 uses OS ephemeral ports)")
	flags.IntVar(&dataPortMax, "data-port-max", 0, "Maximum server-allocated data-plane port (0 uses OS ephemeral ports)")
	flags.IntVar(&udpMTU, "udp-mtu", 0, "UDP datagram MTU for server-sent transfers")
	flags.IntVar(&udpWindow, "udp-window-packets", 0, "UDP max in-flight packets per stream for server-sent transfers")
	flags.IntVar(&udpAckPackets, "udp-ack-every-packets", 0, "UDP ACK every N packets for server receive path")
	flags.IntVar(&udpAckMs, "udp-ack-every-ms", 0, "UDP ACK interval in milliseconds for server receive path")
	flags.IntVar(&udpBatchPackets, "udp-batch-packets", 0, "UDP packets per kernel batch call")
	flags.IntVar(&udpReadBuffer, "udp-read-buffer", 0, "UDP socket read buffer bytes")
	flags.IntVar(&udpWriteBuffer, "udp-write-buffer", 0, "UDP socket write buffer bytes")
	if err := flags.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			os.Exit(0)
		}
		os.Exit(2)
	}
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
	if strings.TrimSpace(dataBindHost) != "" {
		cfg.DataBindHost = dataBindHost
	}
	if strings.TrimSpace(dataAdvertise) != "" {
		cfg.DataAdvertiseHost = dataAdvertise
	}
	if dataPortMin != 0 {
		cfg.DataPortMin = dataPortMin
	}
	if dataPortMax != 0 {
		cfg.DataPortMax = dataPortMax
	}
	if udpMTU > 0 {
		cfg.UDPMTUSize = udpMTU
	}
	if udpWindow > 0 {
		cfg.UDPWindowPackets = udpWindow
	}
	if udpAckPackets > 0 {
		cfg.UDPAckEveryPackets = udpAckPackets
	}
	if udpAckMs > 0 {
		cfg.UDPAckEveryMs = udpAckMs
	}
	if udpBatchPackets > 0 {
		cfg.UDPBatchPackets = udpBatchPackets
	}
	if udpReadBuffer > 0 {
		cfg.UDPReadBufferSize = udpReadBuffer
	}
	if udpWriteBuffer > 0 {
		cfg.UDPWriteBufferSize = udpWriteBuffer
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
