// Command groverd is the Grover node daemon: it serves the control API and
// moves data for transfer jobs and relay forwards.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"

	"github.com/jgoldverg/grover/internal/config"
	"github.com/jgoldverg/grover/internal/energy"
	"github.com/jgoldverg/grover/internal/logging"
	"github.com/jgoldverg/grover/internal/server"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "groverd:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	// Find --config first so the file's values become the flag defaults.
	pre := pflag.NewFlagSet("groverd", pflag.ContinueOnError)
	pre.ParseErrorsWhitelist.UnknownFlags = true
	pre.Usage = func() {}
	configPath := pre.String("config", "", "")
	_ = pre.Parse(args)

	cfg, err := config.LoadServer(*configPath)
	if err != nil {
		return err
	}
	fs := pflag.NewFlagSet("groverd", pflag.ContinueOnError)
	fs.SortFlags = false
	fs.String("config", "", "server config TOML (default ~/.grover/server_config.toml)")
	fs.IntVar(&cfg.Port, "port", cfg.Port, "gRPC control port")
	fs.BoolVar(&cfg.InsecureControl, "insecure-control", cfg.InsecureControl, "serve the control API without TLS")
	fs.StringVar(&cfg.CertFile, "server-cert", cfg.CertFile, "TLS certificate PEM")
	fs.StringVar(&cfg.KeyFile, "server-key", cfg.KeyFile, "TLS private key PEM")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "debug|info|warn|error")
	fs.StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "text|json")
	fs.StringVar(&cfg.DataBindHost, "data-bind-host", cfg.DataBindHost, "address data sockets bind to")
	fs.StringVar(&cfg.DataAdvertiseHost, "data-advertise-host", cfg.DataAdvertiseHost, "address peers dial for data (must be reachable from other nodes)")
	fs.IntVar(&cfg.DataPortMin, "data-port-min", cfg.DataPortMin, "lowest data port (0 = OS ephemeral ports)")
	fs.IntVar(&cfg.DataPortMax, "data-port-max", cfg.DataPortMax, "highest data port")
	fs.StringVar(&cfg.RouteStoreFile, "route-store-file", cfg.RouteStoreFile, "route definitions JSON")
	fs.StringVar(&cfg.JobLogDir, "job-log-dir", cfg.JobLogDir, "job log directory (empty disables job logs)")
	fs.BoolVar(&cfg.EnergyMonitor, "energy-monitor", cfg.EnergyMonitor, "sample Intel RAPL energy; startup fails if unavailable")
	fs.IntVar(&cfg.EnergySampleMs, "energy-sample-ms", cfg.EnergySampleMs, "energy sampling interval in ms")
	t := &cfg.Tuning
	fs.Uint32Var(&t.Concurrency, "concurrency", t.Concurrency, "default files in flight per job")
	fs.Uint32Var(&t.Parallelism, "parallelism", t.Parallelism, "default streams per file")
	fs.Uint64Var(&t.ChunkSizeBytes, "chunk-size-bytes", t.ChunkSizeBytes, "default read/write buffer size")
	fs.Uint32Var(&t.UDPMTU, "udp-mtu", t.UDPMTU, "default UDP datagram size")
	fs.StringVar(&t.UDPFlowControl, "udp-flow-control", t.UDPFlowControl, "default UDP flow control: fixed|bbr")
	fs.Uint32Var(&t.UDPWindowPackets, "udp-window-packets", t.UDPWindowPackets, "default UDP in-flight packets per stream")
	fs.Uint32Var(&t.UDPBatchPackets, "udp-batch-packets", t.UDPBatchPackets, "UDP packets per sendmmsg/recvmmsg")
	fs.Uint32Var(&t.UDPAckEveryPackets, "udp-ack-every-packets", t.UDPAckEveryPackets, "default UDP ACK every N packets")
	fs.Uint32Var(&t.UDPAckEveryMs, "udp-ack-every-ms", t.UDPAckEveryMs, "default UDP ACK interval in ms")
	fs.Uint64Var(&t.UDPWriteFlushBytes, "udp-write-flush-bytes", t.UDPWriteFlushBytes, "default UDP receive write coalescing")
	fs.Uint64Var(&t.UDPSocketRead, "udp-read-buffer", t.UDPSocketRead, "UDP socket receive buffer bytes")
	fs.Uint64Var(&t.UDPSocketWrite, "udp-write-buffer", t.UDPSocketWrite, "UDP socket send buffer bytes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := logging.Setup(cfg.LogLevel, cfg.LogFormat, os.Stderr); err != nil {
		return err
	}

	var monitor *energy.RAPLMonitor
	if cfg.EnergyMonitor {
		if monitor, err = energy.NewRAPLMonitor(""); err != nil {
			return fmt.Errorf("--energy-monitor: %w", err)
		}
		if cfg.JobLogDir == "" {
			return errors.New("--energy-monitor needs --job-log-dir to write energy.csv")
		}
		slog.Info("energy monitor on", "domains", len(monitor.Domains()), "sample_ms", cfg.EnergySampleMs)
	}
	srv, err := server.New(cfg, monitor)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return srv.Run(ctx)
}
