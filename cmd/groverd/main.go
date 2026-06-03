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
	"github.com/jgoldverg/grover/pkg/energy"
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
		routeStoreFile  string
		jobLogDir       string
		energyMonitor   bool
		energySampleMs  int
		udpMTU          int
		udpFlowControl  string
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
	flags.StringVar(&routeStoreFile, "route-store-file", "", "Path to server route JSON file")
	flags.StringVar(&jobLogDir, "job-log-dir", "", "Directory for historical transfer job logs")
	flags.BoolVar(&energyMonitor, "energy-monitor", false, "Enable per-job Intel RAPL energy capture; server startup fails if RAPL is unavailable")
	flags.IntVar(&energySampleMs, "energy-sample-ms", 0, "Per-job energy sampling interval in milliseconds")
	flags.IntVar(&udpMTU, "udp-mtu", 0, "UDP datagram MTU for server-sent transfers")
	flags.StringVar(&udpFlowControl, "udp-flow-control", "", "UDP flow-control mode for server-sent transfers (fixed|bbr)")
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
	if strings.TrimSpace(routeStoreFile) != "" {
		cfg.RouteStoreFile = routeStoreFile
	}
	if strings.TrimSpace(jobLogDir) != "" {
		cfg.JobLogDir = jobLogDir
	}
	if flags.Changed("energy-monitor") {
		cfg.EnergyMonitor = energyMonitor
	}
	if energySampleMs > 0 {
		cfg.EnergySampleMs = energySampleMs
	}
	if udpMTU > 0 {
		cfg.UDPMTUSize = udpMTU
	}
	if strings.TrimSpace(udpFlowControl) != "" {
		mode := strings.ToLower(strings.TrimSpace(udpFlowControl))
		if mode != "fixed" && mode != "bbr" {
			fmt.Fprintf(os.Stderr, "invalid --udp-flow-control %q: must be fixed or bbr\n", udpFlowControl)
			os.Exit(2)
		}
		cfg.UDPFlowControl = mode
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
	if cfg.EnergyMonitor {
		monitor, err := energy.NewRAPLMonitor("")
		if err != nil {
			internal.Error("energy monitor requested but unavailable", internal.Fields{
				internal.FieldError: err.Error(),
			})
			os.Exit(1)
		}
		internal.Info("energy monitor enabled", internal.Fields{
			"domains":          len(monitor.Domains()),
			"sample_interval":  cfg.EnergySampleMs,
			"job_log_dir":      cfg.JobLogDir,
			"powercap_backend": "rapl",
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
