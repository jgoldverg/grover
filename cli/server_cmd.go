package cli

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/gclient"
	"github.com/jgoldverg/grover/pkg/util"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

type PingOpts struct {
	ipAddr string
	port   int
}

type MTUProbeOpts struct {
	ipAddr    string
	udpPort   int
	maxSize   uint
	minSize   uint
	timeoutMs int
	attempts  int
	keepUDP   bool
}

type OpenPorts struct {
	portCount uint
}

type DeletePorts struct {
	ports []uint
}

func GroverServerOps() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "server",
		Aliases: []string{"s", "ser"},
		Short:   "server",
		Long:    "management operations related to the grover server.",
	}
	cmd.AddCommand(StartServer(), StopServer(), MtuProbe(), ListPorts(), OpenUdpPorts(), CloseUdpPorts())
	return cmd
}

func StartServer() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "start-udp",
		Aliases: []string{"start"},
		Short:   "Start server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := GetAppConfig(cmd)
			gc := gclient.NewClient(*cfg)
			rp := util.ParseRoutePolicy(cfg.Route)
			if err := gc.Initialize(cmd.Context(), rp); err != nil {
				return err
			}
			defer gc.Close()
			serverAPI := gc.Server()
			if serverAPI == nil {
				return errors.New("server client not initialized")
			}
			port, err := serverAPI.StartServer(cmd.Context())

			if err != nil {
				return err
			}
			internal.Info("server started with udp port", internal.Fields{
				internal.FieldPort: port,
			})
			return nil
		},
	}
	return cmd
}

func StopServer() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "stop-udp",
		Aliases: []string{"stop"},
		Short:   "Stop udp server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := GetAppConfig(cmd)
			gc := gclient.NewClient(*cfg)
			rp := util.ParseRoutePolicy(cfg.Route)
			if err := gc.Initialize(cmd.Context(), rp); err != nil {
				return err
			}
			defer gc.Close()
			serverAPI := gc.Server()
			if serverAPI == nil {
				return errors.New("server client not initialized")
			}
			msg, err := serverAPI.StopServer(cmd.Context())
			if err != nil {
				return err
			}
			internal.Info("udp server stopped", internal.Fields{
				internal.FieldMsg: msg,
			})
			return nil
		},
	}
	return cmd
}

func ListPorts() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list-ports",
		Aliases: []string{"l"},
		Short:   "List ports",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := GetAppConfig(cmd)
			gc := gclient.NewClient(*cfg)
			rp := util.ParseRoutePolicy(cfg.Route)
			if err := gc.Initialize(cmd.Context(), rp); err != nil {
				return err
			}
			defer gc.Close()
			serverAPI := gc.Server()
			if serverAPI == nil {
				return errors.New("server client not initialized")
			}
			ports, err := serverAPI.ListPorts(cmd.Context())
			if err != nil {
				return err
			}
			internal.Info("running list-ports", internal.Fields{
				"ports": ports,
			})
			return nil
		},
	}
	return cmd
}

func MtuProbe() *cobra.Command {
	opts := &MTUProbeOpts{
		timeoutMs: 300,
		attempts:  2,
	}

	cmd := &cobra.Command{
		Use:   "mtu-probe",
		Short: "Probe UDP MTU for a server listener",
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

			policy := util.ParseRoutePolicy(route)
			if policy == util.RouteForceLocal {
				return fmt.Errorf("mtu probe requires a grover udp serverserver route; rerun with --via server or configure server_url")
			}

			gc := gclient.NewClient(*appConfig)
			if err := gc.Initialize(ctx, policy); err != nil {
				return err
			}
			defer gc.Close()

			host := opts.ipAddr
			if host == "" {
				if h, _, err := net.SplitHostPort(appConfig.ServerURL); err == nil && h != "" {
					host = h
				} else {
					host = "127.0.0.1"
				}
			}
			pterm.DefaultSection.Println("MTU probe inputs")
			pterm.DefaultBasicText.Println("  Target Host:", host)
			size, err := gc.MTU().DiscoverPMTU(cmd.Context(), host, opts.udpPort, int(opts.minSize), int(opts.maxSize), time.Millisecond*time.Duration(opts.timeoutMs))
			if err != nil {
				return err
			}

			pterm.DefaultSection.Println("MTU probe results: size: ", size)
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.ipAddr, "ip-addr", "", "IP or hostname to probe (default derived from server_url)")
	cmd.Flags().UintVar(&opts.minSize, "min-size", 1200, "smallest mtu to start from(default is 1200)")
	cmd.Flags().UintVar(&opts.maxSize, "max-size", 65000, "maximum mtu to start from(default is 65000)")
	cmd.Flags().String("via", "", "routing policy (overrides config)")

	return cmd
}

func OpenUdpPorts() *cobra.Command {
	opts := &OpenPorts{}
	cmd := &cobra.Command{
		Use:     "open-ports",
		Short:   "Open Udp ports",
		Aliases: []string{"op"},
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

			policy := util.ParseRoutePolicy(route)
			if policy == util.RouteForceLocal {
				return fmt.Errorf("mtu probe requires a grover udp serverserver route; rerun with --via server or configure server_url")
			}

			gc := gclient.NewClient(*appConfig)
			if err := gc.Initialize(ctx, policy); err != nil {
				return err
			}
			defer gc.Close()

			serverAPI := gc.Server()
			if serverAPI == nil {
				return errors.New("server client not initialized")
			}
			ports, err := serverAPI.CreatePorts(ctx, uint32(opts.portCount))
			if err != nil {
				return err
			}
			internal.Info("running open-ports", internal.Fields{
				"\n":           "",
				"opened-ports": ports,
			})
			return nil
		},
	}
	cmd.Flags().UintVar(&opts.portCount, "port-count", 0, "number of ports to open")
	cmd.Flags().String("via", "", "routing policy (overrides config)")
	return cmd
}

func CloseUdpPorts() *cobra.Command {
	opts := &DeletePorts{}
	cmd := &cobra.Command{
		Use:     "close-ports",
		Short:   "Close Udp ports",
		Aliases: []string{"op"},
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

			policy := util.ParseRoutePolicy(route)
			if policy == util.RouteForceLocal {
				return fmt.Errorf("mtu probe requires a grover udp serverserver route; rerun with --via server or configure server_url")
			}

			gc := gclient.NewClient(*appConfig)
			if err := gc.Initialize(ctx, policy); err != nil {
				return err
			}
			defer gc.Close()

			serverAPI := gc.Server()
			if serverAPI == nil {
				return errors.New("server client not initialized")
			}

			ports := make([]uint32, len(opts.ports))
			for i, port := range opts.ports {
				ports[i] = uint32(port)
			}
			res, err := serverAPI.DeletePorts(cmd.Context(), ports)
			if err != nil {
				return err
			}

			internal.Info("closed ports", internal.Fields{
				"result": res,
			})
			return nil
		},
	}
	cmd.Flags().UintSliceVar(&opts.ports, "ports", make([]uint, 0), "port to delete")
	cmd.Flags().String("via", "", "routing policy (overrides config)")
	return cmd
}
