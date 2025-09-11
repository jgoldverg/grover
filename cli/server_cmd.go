package cli

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jgoldverg/grover/client"
	"github.com/jgoldverg/grover/log"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

type PingOpts struct {
	ipAddr string
	port   int
}

func GroverServerOps() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "server",
		Aliases: []string{"s", "ser"},
		Short:   "server",
		Long:    "management operations related to the grover server.",
	}
	cmd.AddCommand(GroverPing())
	cmd.AddCommand(StartServer())
	return cmd
}

func GroverPing() *cobra.Command {
	var opts PingOpts

	cmd := &cobra.Command{
		Use:     "ping",
		Aliases: []string{"p"},
		Short:   "Ping",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			appConfig := GetAppConfig(cmd)

			route := appConfig.Route
			if f := cmd.Flags().Lookup("via"); f != nil && f.Changed {
				if v, err := cmd.Flags().GetString("via"); err == nil && v != "" {
					route = v
				}
			}

			if cmd.Flags().Changed("ip-addr") || cmd.Flags().Changed("port") {
				appConfig.ServerURL = fmt.Sprintf("%s:%d", opts.ipAddr, opts.port)
			}

			log.Structured(&pterm.Info, "running ping", log.Fields{
				"server":           appConfig.ServerURL,
				"route":            route,
				"hb_interval_ms":   appConfig.HeartBeatInterval,
				"hb_timeout_secs":  appConfig.HeartBeatTimeout,
				"hb_err_threshold": appConfig.HeartBeatErrorCount,
			})

			policy := client.ParseRoutePolicy(route)

			gc := client.NewGroverClient(*appConfig)
			if err := gc.Initialize(ctx, policy); err != nil {
				return err
			}

			gc.HeartBeatClient.StartPulse(ctx)
			defer gc.HeartBeatClient.StopPulse()

			<-ctx.Done()
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.ipAddr, "ip-addr", "localhost", "IP address of grover")
	cmd.Flags().IntVar(&opts.port, "port", 22444, "Port of grover")
	cmd.Flags().String("via", "", "routing policy (overrides config)")

	return cmd
}

func StartServer() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "start-udp",
		Aliases: []string{"start"},
		Short:   "Start server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := GetAppConfig(cmd)
			gc := client.NewGroverClient(*cfg)
			rp := client.ParseRoutePolicy(cfg.Route)
			err := gc.Initialize(cmd.Context(), rp)
			if err != nil {
				return err
			}
			port, err := gc.ServerClient.StartServer(cmd.Context())
			if err != nil {
				return err
			}
			log.Structured(&pterm.Info, "server started with udp port: ", log.Fields{
				log.FieldPort: port,
			})
			return nil
		},
	}
	return cmd
}
