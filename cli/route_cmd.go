package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/gclient"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/util"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

type routeCommandOptions struct {
	storePath         string
	via               []string
	protocol          string
	parallelStreams   int
	concurrency       int
	watch             bool
	sourceServer      string
	destinationServer string
	connectionOrigin  string
	dataDirection     string
	jsonOutput        bool
	sessionID         string
}

type routeTemplateStore struct {
	path string
}

type routeTemplateFile struct {
	Routes []storedRouteTemplate `toml:"routes"`
}

type storedRouteTemplate struct {
	Name             string    `toml:"name"`
	Source           string    `toml:"source"`
	Destination      string    `toml:"destination"`
	Via              []string  `toml:"via"`
	Protocol         string    `toml:"protocol"`
	ParallelStreams  int       `toml:"parallel_streams"`
	Concurrency      int       `toml:"concurrency"`
	ConnectionOrigin string    `toml:"connection_origin"`
	DataDirection    string    `toml:"data_direction"`
	State            string    `toml:"state"`
	CreatedAt        time.Time `toml:"created_at"`
	UpdatedAt        time.Time `toml:"updated_at"`
}

func RouteCommand() *cobra.Command {
	opts := routeCommandOptions{}
	cmd := &cobra.Command{
		Use:   "route",
		Short: "Prepare and inspect routed transfer templates",
		Long:  "Prepare and inspect routed transfer templates. Route execution is materialized by groverd in later routed-transfer phases.",
	}
	cmd.PersistentFlags().StringVar(&opts.storePath, "route-store", "", "Path to local route template store")
	_ = cmd.PersistentFlags().MarkHidden("route-store")
	cmd.AddCommand(routePrepareCommand(&opts))
	cmd.AddCommand(routePutCommand(&opts))
	cmd.AddCommand(routeGetCommand(&opts))
	cmd.AddCommand(routeListCommand(&opts))
	cmd.AddCommand(routeDeleteCommand(&opts))
	cmd.AddCommand(routeStartCommand(&opts))
	cmd.AddCommand(routeStatusCommand(&opts))
	cmd.AddCommand(routeAbortCommand(&opts))
	cmd.AddCommand(routeCloseCommand(&opts))
	return cmd
}

func routePutCommand(opts *routeCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "put <name>",
		Short:        "Store a named route on the configured groverd",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			source := strings.TrimSpace(opts.sourceServer)
			destination := strings.TrimSpace(opts.destinationServer)
			if source == "" || destination == "" {
				return fmt.Errorf("route put requires --source and --destination groverd control endpoints")
			}
			connectionOrigin, err := normalizeConnectionOrigin(opts.connectionOrigin)
			if err != nil {
				return err
			}
			dataDirection, err := normalizeDataDirection(opts.dataDirection)
			if err != nil {
				return err
			}
			protocol := strings.ToLower(strings.TrimSpace(opts.protocol))
			if protocol == "" {
				protocol = "tcp"
			}
			if protocol != "tcp" && protocol != "udp" {
				return fmt.Errorf("invalid --protocol %q: must be tcp or udp", opts.protocol)
			}
			relays, err := parseTransferRelays(opts.via)
			if err != nil {
				return err
			}
			via := make([]string, 0, len(relays))
			for _, relay := range relays {
				via = append(via, relay.ControlEndpoint)
			}
			routeClient, closeFn, err := openRouteConfigControl(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			route, err := routeClient.PutRoute(cmd.Context(), &pb.RouteConfig{
				Name:             strings.TrimSpace(args[0]),
				Source:           source,
				Destination:      destination,
				Via:              via,
				Protocol:         dataProtocol(protocol),
				ConnectionOrigin: pbConnectionOrigin(connectionOrigin),
				DataDirection:    pbDataDirection(dataDirection),
			})
			if err != nil {
				return err
			}
			if opts.jsonOutput {
				return writeRouteConfigJSON(cmd.OutOrStdout(), route)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "stored route %s\n", route.GetName())
			return printServerRouteTable(cmd.OutOrStdout(), []*pb.RouteConfig{route})
		},
	}
	cmd.Flags().StringVar(&opts.sourceServer, "source", "", "Source groverd control endpoint")
	cmd.Flags().StringVar(&opts.destinationServer, "destination", "", "Destination groverd control endpoint")
	cmd.Flags().StringArrayVar(&opts.via, "via", nil, "Relay groverd control endpoint; repeat or use comma-separated values")
	cmd.Flags().StringVar(&opts.protocol, "protocol", "tcp", "Route data-plane protocol (tcp|udp)")
	addRouteDirectionFlags(cmd, &opts.connectionOrigin, &opts.dataDirection)
	cmd.Flags().BoolVar(&opts.jsonOutput, "json", false, "Print route as JSON")
	return cmd
}

func addRouteDirectionFlags(cmd *cobra.Command, origin *string, direction *string) {
	cmd.Flags().StringVar(origin, "connect-from", "", "Endpoint that opens route data connections (source|destination)")
	cmd.Flags().StringVar(origin, "connection-origin", "", "Deprecated: use --connect-from")
	_ = cmd.Flags().MarkHidden("connection-origin")
	cmd.Flags().StringVar(direction, "flow", "", "Byte flow through the route (forward|reverse|source-to-destination|destination-to-source)")
	cmd.Flags().StringVar(direction, "data-direction", "", "Deprecated: use --flow")
	_ = cmd.Flags().MarkHidden("data-direction")
}

func commandFlagChanged(cmd *cobra.Command, names ...string) bool {
	if cmd == nil {
		return false
	}
	for _, name := range names {
		flag := cmd.Flags().Lookup(name)
		if flag != nil && flag.Changed {
			return true
		}
	}
	return false
}

func routeGetCommand(opts *routeCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "get <name>",
		Short:        "Show a named route from the configured groverd",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			routeClient, closeFn, err := openRouteConfigControl(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			route, err := routeClient.GetRoute(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if opts.jsonOutput {
				return writeRouteConfigJSON(cmd.OutOrStdout(), route)
			}
			return printServerRouteTable(cmd.OutOrStdout(), []*pb.RouteConfig{route})
		},
	}
	cmd.Flags().BoolVar(&opts.jsonOutput, "json", false, "Print route as JSON")
	return cmd
}

func routePrepareCommand(opts *routeCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prepare <name> [source] [destination]",
		Short: "Store a local routed transfer template",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 || len(args) == 3 {
				return nil
			}
			return fmt.Errorf("accepts <name> or <name> <source> <destination>")
		},
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			copyOpts := CopyOptions{
				Via:              opts.via,
				Protocol:         opts.protocol,
				ParallelStreams:  opts.parallelStreams,
				Concurrency:      opts.concurrency,
				ConnectionOrigin: opts.connectionOrigin,
				DataDirection:    opts.dataDirection,
			}
			if err := copyOpts.validate(); err != nil {
				return err
			}
			if strings.TrimSpace(opts.storePath) == "" {
				if len(args) != 1 {
					return fmt.Errorf("server route prepare accepts <name>; transfer paths are supplied to transfer --route")
				}
				routeClient, closeFn, err := openRouteConfigControl(cmd)
				if err != nil {
					return err
				}
				defer closeFn()
				serverRoute, err := routeClient.GetRoute(cmd.Context(), args[0])
				if err != nil {
					return err
				}
				session, err := prepareServerRouteSession(cmd, serverRoute, opts.sessionID)
				if err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "prepared route session %s\n", session.GetSessionId())
				printRouteSessions(cmd.OutOrStdout(), []*pb.RouteSession{session})
				return nil
			}
			var source, destination string
			if len(args) == 3 {
				if _, err := parseLocation(args[1]); err != nil {
					return err
				}
				if _, err := parseLocation(args[2]); err != nil {
					return err
				}
				source = args[1]
				destination = args[2]
			}
			relays, err := parseTransferRelays(opts.via)
			if err != nil {
				return err
			}
			store, err := newRouteTemplateStore(opts.storePath)
			if err != nil {
				return err
			}
			now := time.Now().UTC()
			tmpl := storedRouteTemplate{
				Name:             strings.TrimSpace(args[0]),
				Source:           source,
				Destination:      destination,
				Via:              append([]string(nil), opts.via...),
				Protocol:         routeProtocol(copyOpts.Protocol),
				ParallelStreams:  routeParallelStreams(copyOpts.ParallelStreams),
				Concurrency:      copyOpts.effectiveConcurrency(),
				ConnectionOrigin: routeConnectionOrigin(copyOpts.ConnectionOrigin),
				DataDirection:    routeDataDirection(copyOpts.DataDirection),
				State:            "prepared",
				CreatedAt:        now,
				UpdatedAt:        now,
			}
			if err := validateRouteTemplate(tmpl); err != nil {
				return err
			}
			if err := store.upsert(tmpl); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "prepared route %s\n", tmpl.Name)
			printStoredRouteTemplatePlan(cmd.OutOrStdout(), tmpl, relays)
			return nil
		},
	}
	cmd.Flags().StringArrayVar(&opts.via, "via", nil, "Relay hop to insert into the transfer route; repeat or use comma-separated values")
	cmd.Flags().StringVar(&opts.protocol, "protocol", "", "Transfer data-plane protocol (udp|tcp)")
	cmd.Flags().IntVar(&opts.parallelStreams, "parallel-streams", 0, "Per-file parallel streams/ranges (0 uses config)")
	cmd.Flags().IntVar(&opts.concurrency, "concurrency", 4, "Maximum number of files to transfer in parallel")
	addRouteDirectionFlags(cmd, &opts.connectionOrigin, &opts.dataDirection)
	cmd.Flags().StringVar(&opts.sessionID, "session-id", "", "Session ID for a materialized server route")
	return cmd
}

func routeListCommand(opts *routeCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "list",
		Short:        "List routes on the configured groverd",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.storePath) == "" {
				routeClient, closeFn, err := openRouteConfigControl(cmd)
				if err != nil {
					return err
				}
				defer closeFn()
				routes, err := routeClient.ListRoutes(cmd.Context())
				if err != nil {
					return err
				}
				if opts.jsonOutput {
					return writeRouteConfigsJSON(cmd.OutOrStdout(), routes)
				}
				if len(routes) == 0 {
					fmt.Fprintln(cmd.OutOrStdout(), "no routes configured")
					return nil
				}
				return printServerRouteTable(cmd.OutOrStdout(), routes)
			}
			store, err := newRouteTemplateStore(opts.storePath)
			if err != nil {
				return err
			}
			routes, err := store.list()
			if err != nil {
				return err
			}
			if len(routes) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no routes prepared")
				return nil
			}
			return printStoredRouteTable(cmd.OutOrStdout(), routes)
		},
	}
	cmd.Flags().BoolVar(&opts.jsonOutput, "json", false, "Print routes as JSON")
	return cmd
}

func routeDeleteCommand(opts *routeCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "delete <name>",
		Aliases:      []string{"rm"},
		Short:        "Delete a named route from the configured groverd",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			routeClient, closeFn, err := openRouteConfigControl(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			ok, err := routeClient.DeleteRoute(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("route %q not found", args[0])
			}
			fmt.Fprintf(cmd.OutOrStdout(), "deleted route %s\n", strings.TrimSpace(args[0]))
			return nil
		},
	}
	return cmd
}

func routeStartCommand(opts *routeCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "start <name>",
		Short:        "Materialize a prepared route",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("route start does not copy files; use transfer --route %s <source> <destination>", strings.TrimSpace(args[0]))
		},
	}
	cmd.Flags().StringVar(&opts.sourceServer, "source-server", "", "Control address of the source groverd (defaults to --server-url)")
	cmd.Flags().StringVar(&opts.destinationServer, "destination-server", "", "Control address of the destination groverd (defaults to source server)")
	return cmd
}

func monitorRoutedTransferJob(cmd *cobra.Command, routed gclient.RoutedTransferAPI, job *pb.TransferJob, opts CopyOptions) (*pb.TransferJob, error) {
	if job == nil {
		return nil, fmt.Errorf("transfer job was not returned by source groverd")
	}
	out := cmd.OutOrStdout()
	sampler := &transferRateSampler{}
	if opts.uiMode() == "live" {
		printTransferJobStatus(out, job, sampler.Observe(job, time.Now()))
	} else {
		fmt.Fprintf(out, "transfer_job: %s\n", job.GetJobId())
		fmt.Fprintf(out, "state: %s\n", job.GetState().String())
	}
	interval := opts.uiInterval()
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for isActiveTransferState(job.GetState()) {
		select {
		case <-cmd.Context().Done():
			return nil, cmd.Context().Err()
		case <-ticker.C:
		}
		next, err := routed.GetTransferJob(cmd.Context(), job.GetJobId())
		if err != nil {
			return nil, err
		}
		job = next
		if opts.uiMode() == "live" {
			printTransferJobStatus(out, job, sampler.Observe(job, time.Now()))
		}
	}
	return job, nil
}

func isActiveTransferState(state pb.RuntimeState) bool {
	return state == pb.RuntimeState_RUNTIME_STATE_RUNNING || state == pb.RuntimeState_RUNTIME_STATE_PREPARING
}

func materializeRelayForwards(cmd *cobra.Command, routeID string, jobID string, protocol pb.DataProtocol, relays []TransferRelayHop, dest *pb.TransferEndpoint) (func(), []*pb.ForwardSession, error) {
	cleanup, forwards, ingress, err := materializeRelayChainToEndpoint(cmd, routeID, jobID, protocol, relays, dest.GetDataEndpoint())
	if err != nil {
		return cleanup, forwards, err
	}
	dest.DataEndpoint = ingress
	return cleanup, forwards, nil
}

func materializeRelayChainToEndpoint(cmd *cobra.Command, routeID string, jobID string, protocol pb.DataProtocol, relays []TransferRelayHop, egress *pb.DataEndpoint) (func(), []*pb.ForwardSession, *pb.DataEndpoint, error) {
	next := clonePBEndpoint(egress)
	if next == nil || strings.TrimSpace(next.GetHost()) == "" || next.GetPort() == 0 {
		return func() {}, nil, nil, fmt.Errorf("data endpoint is required to materialize relay chain")
	}
	internal.Info("materializing relay chain", internal.Fields{
		"route_id":  routeID,
		"job_id":    jobID,
		"protocol":  protocol.String(),
		"relays":    len(relays),
		"final_hop": endpointLabelForLog(next),
	})
	baseCfg := GetAppConfig(cmd)
	if baseCfg == nil && len(relays) > 0 {
		return func() {}, nil, nil, fmt.Errorf("app config unavailable")
	}
	type relayLease struct {
		client *gclient.Client
		id     string
	}
	leases := []relayLease{}
	forwards := []*pb.ForwardSession{}
	cleanup := func() {
		internal.Info("calling cleanup in materializeRelayForwards", internal.Fields{
			"jobID":   jobID,
			"routeID": routeID,
			"relays":  relays,
		})
		for i := len(leases) - 1; i >= 0; i-- {
			if relay := leases[i].client.RelayControl(); relay != nil {
				_, _ = relay.DeleteForward(cmd.Context(), leases[i].id)
			}
			_ = leases[i].client.Close()
		}
	}
	for i := len(relays) - 1; i >= 0; i-- {
		relayCfg := *baseCfg
		relayCfg.ServerURL = relays[i].ControlEndpoint
		internal.Info("dialing relay control endpoint", internal.Fields{
			"route_id": routeID,
			"job_id":   jobID,
			"relay":    relays[i].ControlEndpoint,
			"hop":      i + 1,
			"egress":   endpointLabelForLog(next),
		})
		client := gclient.NewClient(relayCfg)
		if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
			cleanup()
			return func() {}, nil, nil, err
		}
		relay := client.RelayControl()
		if relay == nil {
			_ = client.Close()
			cleanup()
			return func() {}, nil, nil, fmt.Errorf("relay control service unavailable on %s", relays[i].ControlEndpoint)
		}
		forward, err := relay.CreateForward(cmd.Context(), &pb.CreateForwardRequest{
			RouteId:    routeID,
			JobId:      jobID,
			HopIndex:   uint32(i + 1),
			Protocol:   protocol,
			Egress:     next,
			TtlSeconds: 600,
		})
		if err != nil {
			_ = client.Close()
			cleanup()
			return func() {}, nil, nil, err
		}
		internal.Info("relay forward materialized", internal.Fields{
			"route_id":   routeID,
			"job_id":     jobID,
			"relay":      relays[i].ControlEndpoint,
			"forward_id": forward.GetForwardId(),
			"hop":        forward.GetHopIndex(),
			"ingress":    endpointLabelForLog(forward.GetIngress()),
			"egress":     endpointLabelForLog(forward.GetEgress()),
		})
		leases = append(leases, relayLease{client: client, id: forward.GetForwardId()})
		forwards = append(forwards, forward)
		next = clonePBEndpoint(forward.GetIngress())
	}
	sort.Slice(forwards, func(i, j int) bool {
		return forwards[i].GetHopIndex() < forwards[j].GetHopIndex()
	})
	return cleanup, forwards, next, nil
}

func routeSessionHopsFromForwards(relays []TransferRelayHop, forwards []*pb.ForwardSession) []*pb.RouteSessionHop {
	if len(forwards) == 0 {
		return nil
	}
	hops := make([]*pb.RouteSessionHop, 0, len(forwards))
	for _, forward := range forwards {
		control := ""
		index := int(forward.GetHopIndex()) - 1
		if index >= 0 && index < len(relays) {
			control = relays[index].ControlEndpoint
		}
		hops = append(hops, &pb.RouteSessionHop{
			HopIndex:        forward.GetHopIndex(),
			ControlEndpoint: control,
			Ingress:         clonePBEndpoint(forward.GetIngress()),
			Egress:          clonePBEndpoint(forward.GetEgress()),
			State:           forward.GetState(),
			Stats:           forward.GetStats(),
			ErrorMessage:    forward.GetErrorMessage(),
		})
	}
	return hops
}

func routeSessionStateForJob(job *pb.TransferJob) pb.RuntimeState {
	if job == nil {
		return pb.RuntimeState_RUNTIME_STATE_FAILED
	}
	switch job.GetState() {
	case pb.RuntimeState_RUNTIME_STATE_DONE:
		return pb.RuntimeState_RUNTIME_STATE_DONE
	case pb.RuntimeState_RUNTIME_STATE_ABORTED:
		return pb.RuntimeState_RUNTIME_STATE_ABORTED
	case pb.RuntimeState_RUNTIME_STATE_FAILED:
		return pb.RuntimeState_RUNTIME_STATE_FAILED
	default:
		return pb.RuntimeState_RUNTIME_STATE_RUNNING
	}
}

func prepareServerRouteSession(cmd *cobra.Command, route *pb.RouteConfig, sessionID string) (*pb.RouteSession, error) {
	if route == nil {
		return nil, fmt.Errorf("route config is required")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		sessionID = newTransferJobID(route.GetName(), time.Now())
	}
	internal.Info("preparing route session", internal.Fields{
		"route_id":          route.GetName(),
		"session_id":        sessionID,
		"source":            route.GetSource(),
		"destination":       route.GetDestination(),
		"relays":            len(route.GetVia()),
		"protocol":          route.GetProtocol().String(),
		"connection_origin": route.GetConnectionOrigin().String(),
		"data_direction":    route.GetDataDirection().String(),
	})
	tmpl, err := storedRouteTemplateFromServerRoute(route, "/", "/")
	if err != nil {
		return nil, err
	}
	plan, err := tmpl.plan()
	if err != nil {
		return nil, err
	}
	baseCfg := GetAppConfig(cmd)
	if baseCfg == nil {
		return nil, fmt.Errorf("app config unavailable")
	}
	sourceCfg := *baseCfg
	sourceCfg.ServerURL = route.GetSource()
	destCfg := *baseCfg
	destCfg.ServerURL = route.GetDestination()
	internal.Info("opening source route control", internal.Fields{
		"route_id":   route.GetName(),
		"session_id": sessionID,
		"source":     sourceCfg.ServerURL,
	})
	sourceClient := gclient.NewClient(sourceCfg)
	if err := sourceClient.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
		return nil, err
	}
	defer sourceClient.Close()
	destClient := sourceClient
	if destCfg.ServerURL != sourceCfg.ServerURL {
		internal.Info("opening destination route control", internal.Fields{
			"route_id":    route.GetName(),
			"session_id":  sessionID,
			"destination": destCfg.ServerURL,
		})
		destClient = gclient.NewClient(destCfg)
		if err := destClient.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
			return nil, err
		}
		defer destClient.Close()
	}
	sourceRouted := sourceClient.RoutedTransfer()
	destRouted := destClient.RoutedTransfer()
	routeSessions := sourceClient.RouteSessionControl()
	if sourceRouted == nil || destRouted == nil || routeSessions == nil {
		return nil, fmt.Errorf("route session services unavailable")
	}
	protocol := route.GetProtocol()
	if protocol == pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED {
		protocol = pb.DataProtocol_DATA_PROTOCOL_TCP
	}
	connectionOrigin := route.GetConnectionOrigin()
	if connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_UNSPECIFIED {
		connectionOrigin = pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
	}
	if connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION && protocol != pb.DataProtocol_DATA_PROTOCOL_TCP {
		return nil, fmt.Errorf("connection_origin=destination is currently supported for tcp only")
	}
	source, err := sourceRouted.PrepareTransferEndpoint(cmd.Context(), &pb.PrepareTransferEndpointRequest{
		RouteId:          route.GetName(),
		JobId:            sessionID,
		SessionId:        sessionID,
		Role:             pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol:         protocol,
		RootPath:         plan.Source.Path,
		ConnectionOrigin: connectionOrigin,
		TtlSeconds:       3600,
	})
	if err != nil {
		internal.Error("failed to prepare source endpoint", internal.Fields{
			internal.FieldError: err.Error(),
			"sessionID":         sessionID,
			"jobID":             sessionID,
			"Role":              pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
			"Protocol":          protocol,
			"ConnectionOrigin":  connectionOrigin,
		})
		return nil, err
	}
	internal.Info("source endpoint prepared", internal.Fields{
		"route_id":    route.GetName(),
		"session_id":  sessionID,
		"endpoint_id": source.GetEndpointId(),
		"data":        endpointLabelForLog(source.GetDataEndpoint()),
		"root":        source.GetRootPath(),
	})
	cleanup := func() {}
	forwards := []*pb.ForwardSession(nil)
	destBind := (*pb.DataEndpoint)(nil)
	if connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
		if len(plan.Relays) > 0 {
			cleanup, forwards, destBind, err = materializeRelayChainToEndpoint(cmd, route.GetName(), sessionID, protocol, plan.Relays, source.GetDataEndpoint())
			if err != nil {
				return nil, err
			}
		} else {
			destBind = clonePBEndpoint(source.GetDataEndpoint())
		}
	}
	dest, err := destRouted.PrepareTransferEndpoint(cmd.Context(), &pb.PrepareTransferEndpointRequest{
		RouteId:          route.GetName(),
		JobId:            sessionID,
		SessionId:        sessionID,
		Role:             pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol:         protocol,
		RootPath:         plan.Destination.Path,
		Bind:             destBind,
		ConnectionOrigin: connectionOrigin,
		TtlSeconds:       3600,
	})
	if err != nil {
		cleanup()
		return nil, err
	}
	internal.Info("destination endpoint prepared", internal.Fields{
		"route_id":    route.GetName(),
		"session_id":  sessionID,
		"endpoint_id": dest.GetEndpointId(),
		"data":        endpointLabelForLog(dest.GetDataEndpoint()),
		"root":        dest.GetRootPath(),
	})
	if connectionOrigin != pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
		cleanup, forwards, err = materializeRelayForwards(cmd, route.GetName(), sessionID, protocol, plan.Relays, dest)
		if err != nil {
			return nil, err
		}
	}
	var reverseSource *pb.TransferEndpoint
	var reverseDest *pb.TransferEndpoint
	var reverseForwards []*pb.ForwardSession
	reverseCleanup := func() {}
	if connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE {
		reverseSource, reverseDest, reverseForwards, reverseCleanup, err = prepareReverseSourceOriginLeg(cmd, route, sessionID, protocol, plan, sourceRouted, destRouted)
		if err != nil {
			cleanup()
			return nil, err
		}
	}
	session, err := routeSessions.CreateRouteSession(cmd.Context(), &pb.CreateRouteSessionRequest{
		SessionId:          sessionID,
		RouteId:            route.GetName(),
		JobId:              sessionID,
		Protocol:           protocol,
		ConnectionOrigin:   connectionOrigin,
		DataDirection:      route.GetDataDirection(),
		Source:             source,
		Destination:        dest,
		Hops:               routeSessionHopsFromForwards(plan.Relays, forwards),
		ReverseSource:      reverseSource,
		ReverseDestination: reverseDest,
		ReverseHops:        routeSessionHopsFromForwards(reverseTransferRelays(plan.Relays), reverseForwards),
	})
	if err != nil {
		reverseCleanup()
		cleanup()
		return nil, err
	}
	internal.Info("route session ready", internal.Fields{
		"route_id":            session.GetRouteId(),
		"session_id":          session.GetSessionId(),
		"state":               session.GetState().String(),
		"source_data":         endpointLabelForLog(session.GetSource().GetDataEndpoint()),
		"destination_data":    endpointLabelForLog(session.GetDestination().GetDataEndpoint()),
		"reverse_source":      endpointLabelForLog(session.GetReverseSource().GetDataEndpoint()),
		"reverse_destination": endpointLabelForLog(session.GetReverseDestination().GetDataEndpoint()),
		"hops":                len(session.GetHops()),
		"reverse_hops":        len(session.GetReverseHops()),
	})
	return session, nil
}

func prepareReverseSourceOriginLeg(
	cmd *cobra.Command,
	route *pb.RouteConfig,
	sessionID string,
	protocol pb.DataProtocol,
	plan TransferRoutePlan,
	sourceRouted gclient.RoutedTransferAPI,
	destRouted gclient.RoutedTransferAPI,
) (*pb.TransferEndpoint, *pb.TransferEndpoint, []*pb.ForwardSession, func(), error) {
	noop := func() {}
	reverseSource, err := destRouted.PrepareTransferEndpoint(cmd.Context(), &pb.PrepareTransferEndpointRequest{
		RouteId:          route.GetName(),
		JobId:            sessionID,
		SessionId:        sessionID,
		Role:             pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol:         protocol,
		RootPath:         "/",
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE,
		TtlSeconds:       3600,
	})
	if err != nil {
		return nil, nil, nil, noop, err
	}
	reverseDest, err := sourceRouted.PrepareTransferEndpoint(cmd.Context(), &pb.PrepareTransferEndpointRequest{
		RouteId:          route.GetName(),
		JobId:            sessionID,
		SessionId:        sessionID,
		Role:             pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol:         protocol,
		RootPath:         "/",
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE,
		TtlSeconds:       3600,
	})
	if err != nil {
		return nil, nil, nil, noop, err
	}
	reverseRelays := reverseTransferRelays(plan.Relays)
	cleanup, forwards, err := materializeRelayForwards(cmd, route.GetName(), sessionID, protocol, reverseRelays, reverseDest)
	if err != nil {
		return nil, nil, nil, cleanup, err
	}
	return reverseSource, reverseDest, forwards, cleanup, nil
}

func reverseTransferRelays(relays []TransferRelayHop) []TransferRelayHop {
	if len(relays) == 0 {
		return nil
	}
	out := make([]TransferRelayHop, 0, len(relays))
	for i := len(relays) - 1; i >= 0; i-- {
		out = append(out, relays[i])
	}
	return out
}

func startTransferOverPreparedRouteSession(cmd *cobra.Command, src string, dst string, opts CopyOptions) (*pb.TransferJob, error) {
	routeName := strings.TrimSpace(opts.RouteName)
	if routeName == "" {
		return nil, fmt.Errorf("--route is required")
	}
	routeClient, closeFn, err := openRouteConfigControl(cmd)
	if err != nil {
		return nil, err
	}
	route, err := routeClient.GetRoute(cmd.Context(), routeName)
	closeFn()
	if err != nil {
		return nil, err
	}
	sourceRef, err := parseLocation(src)
	if err != nil {
		return nil, err
	}
	destRef, err := parseLocation(dst)
	if err != nil {
		return nil, err
	}
	if err := resolveTransferEndpointCredentials(cmd, &sourceRef, &destRef); err != nil {
		return nil, err
	}
	plan, err := buildTransferRoutePlan(sourceRef, destRef, opts)
	if err != nil {
		return nil, err
	}
	baseCfg := GetAppConfig(cmd)
	if baseCfg == nil {
		return nil, fmt.Errorf("app config unavailable")
	}
	sourceCfg := *baseCfg
	sourceCfg.ServerURL = route.GetSource()
	sourceClient := gclient.NewClient(sourceCfg)
	if err := sourceClient.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
		return nil, err
	}
	defer sourceClient.Close()
	routeSessions := sourceClient.RouteSessionControl()
	forwardSourceRouted := sourceClient.RoutedTransfer()
	if routeSessions == nil || forwardSourceRouted == nil {
		return nil, fmt.Errorf("route session services unavailable")
	}
	session, err := selectPreparedRouteSession(cmd, routeSessions, routeName, opts.SessionID)
	if err != nil {
		return nil, err
	}
	if session.GetState() == pb.RuntimeState_RUNTIME_STATE_RUNNING {
		return nil, fmt.Errorf("route session %s is already running a transfer", session.GetSessionId())
	}
	if session.GetState() != pb.RuntimeState_RUNTIME_STATE_READY {
		return nil, fmt.Errorf("route session %s is %s; prepare or choose a READY session", session.GetSessionId(), session.GetState().String())
	}
	jobID := strings.TrimSpace(opts.JobID)
	if jobID == "" {
		jobID = newTransferJobID(routeName, time.Now())
	}
	direction, err := normalizeTransferDirection(opts.Direction)
	if err != nil {
		return nil, err
	}
	connectionOrigin := session.GetConnectionOrigin()
	jobRouted := forwardSourceRouted
	source := clonePBTransferEndpoint(session.GetSource())
	dest := clonePBTransferEndpoint(session.GetDestination())
	if direction == "reverse" {
		if session.GetConnectionOrigin() != pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE {
			return nil, fmt.Errorf("reverse transfers over destination-origin sessions are not supported yet; prepare the route with --connect-from=source")
		}
		source = clonePBTransferEndpoint(session.GetReverseSource())
		dest = clonePBTransferEndpoint(session.GetReverseDestination())
		if source == nil || dest == nil {
			return nil, fmt.Errorf("route session %s has no reverse leg; close and prepare the route again", session.GetSessionId())
		}
		destCfg := *baseCfg
		destCfg.ServerURL = route.GetDestination()
		destClient := gclient.NewClient(destCfg)
		if err := destClient.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
			return nil, err
		}
		defer destClient.Close()
		jobRouted = destClient.RoutedTransfer()
		if jobRouted == nil {
			return nil, fmt.Errorf("destination transfer service unavailable")
		}
		connectionOrigin = pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
	}
	source.RootPath = plan.Source.Path
	dest.RootPath = plan.Destination.Path
	_, _ = routeSessions.UpdateRouteSessionState(cmd.Context(), session.GetSessionId(), pb.RuntimeState_RUNTIME_STATE_RUNNING, "")
	job, err := jobRouted.StartTransferJob(cmd.Context(), &pb.StartTransferJobRequest{
		RouteId:            routeName,
		JobId:              jobID,
		SessionId:          session.GetSessionId(),
		Source:             source,
		Destination:        dest,
		Paths:              append([]string(nil), opts.Paths...),
		FilesInFlight:      uint32(opts.effectiveConcurrency()),
		StreamsPerFile:     uint32(opts.effectiveParallelismPerFile()),
		Concurrency:        uint32(opts.effectiveConcurrency()),
		ParallelismPerFile: uint32(opts.effectiveParallelismPerFile()),
		ChunkSizeBytes:     mustParseOptionalByteSize(opts.ChunkSize),
		ConnectionOrigin:   connectionOrigin,
	})
	if err != nil {
		_, _ = routeSessions.UpdateRouteSessionState(cmd.Context(), session.GetSessionId(), pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
		return nil, err
	}
	job, err = monitorRoutedTransferJob(cmd, jobRouted, job, opts)
	if err != nil {
		_, _ = routeSessions.UpdateRouteSessionState(cmd.Context(), session.GetSessionId(), pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
		return nil, err
	}
	switch job.GetState() {
	case pb.RuntimeState_RUNTIME_STATE_DONE:
		_, _ = routeSessions.UpdateRouteSessionState(cmd.Context(), session.GetSessionId(), pb.RuntimeState_RUNTIME_STATE_READY, "")
	case pb.RuntimeState_RUNTIME_STATE_FAILED, pb.RuntimeState_RUNTIME_STATE_ABORTED:
		_, _ = routeSessions.UpdateRouteSessionState(cmd.Context(), session.GetSessionId(), job.GetState(), job.GetErrorMessage())
	}
	return job, nil
}

func selectPreparedRouteSession(cmd *cobra.Command, routeSessions gclient.RouteSessionAPI, routeName, sessionID string) (*pb.RouteSession, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID != "" {
		return routeSessions.GetRouteSession(cmd.Context(), sessionID)
	}
	sessions, err := routeSessions.ListRouteSessions(cmd.Context(), routeName, "")
	if err != nil {
		return nil, err
	}
	var newest *pb.RouteSession
	for _, session := range sessions {
		if session.GetState() != pb.RuntimeState_RUNTIME_STATE_READY {
			continue
		}
		if newest == nil || session.GetUpdatedAtUnixNano() > newest.GetUpdatedAtUnixNano() {
			newest = session
		}
	}
	if newest == nil {
		return nil, fmt.Errorf("route %s has no prepared session; run: grover route prepare %s", routeName, routeName)
	}
	return newest, nil
}

func pbConnectionOrigin(origin string) pb.ConnectionOrigin {
	switch routeConnectionOrigin(origin) {
	case "destination":
		return pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION
	default:
		return pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
	}
}

func pbDataDirection(direction string) pb.DataDirection {
	switch routeDataDirection(direction) {
	case "destination-to-source":
		return pb.DataDirection_DATA_DIRECTION_DESTINATION_TO_SOURCE
	default:
		return pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION
	}
}

func clonePBEndpoint(ep *pb.DataEndpoint) *pb.DataEndpoint {
	if ep == nil {
		return nil
	}
	return &pb.DataEndpoint{Host: ep.GetHost(), Port: ep.GetPort()}
}

func endpointLabelForLog(ep *pb.DataEndpoint) string {
	if ep == nil || strings.TrimSpace(ep.GetHost()) == "" || ep.GetPort() == 0 {
		return "(none)"
	}
	return fmt.Sprintf("%s:%d", ep.GetHost(), ep.GetPort())
}

func clonePBTransferEndpoint(ep *pb.TransferEndpoint) *pb.TransferEndpoint {
	if ep == nil {
		return nil
	}
	return &pb.TransferEndpoint{
		EndpointId:    ep.GetEndpointId(),
		RouteId:       ep.GetRouteId(),
		JobId:         ep.GetJobId(),
		SessionId:     ep.GetSessionId(),
		Role:          ep.GetRole(),
		Protocol:      ep.GetProtocol(),
		DataEndpoint:  clonePBEndpoint(ep.GetDataEndpoint()),
		RootPath:      ep.GetRootPath(),
		TtlSeconds:    ep.GetTtlSeconds(),
		ExpiresAtUnix: ep.GetExpiresAtUnix(),
	}
}

func dataProtocol(protocol string) pb.DataProtocol {
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case "udp":
		return pb.DataProtocol_DATA_PROTOCOL_UDP
	default:
		return pb.DataProtocol_DATA_PROTOCOL_TCP
	}
}

func routeStatusCommand(opts *routeCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "status <name>",
		Short:        "Show local route template status",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.storePath) == "" {
				routeClient, closeFn, err := openRouteConfigControl(cmd)
				if err != nil {
					return err
				}
				defer closeFn()
				serverRoute, err := routeClient.GetRoute(cmd.Context(), args[0])
				if err != nil {
					return err
				}
				printServerRouteConfig(cmd.OutOrStdout(), serverRoute)
				route, err := storedRouteTemplateFromServerRoute(serverRoute, "/unused/source-root", "/unused/destination-root")
				if err != nil {
					return err
				}
				sourceServer := opts.sourceServer
				if strings.TrimSpace(sourceServer) == "" {
					sourceServer = serverRoute.GetSource()
				}
				active, err := printRouteRuntimeStatus(cmd, cmd.OutOrStdout(), route, sourceServer)
				if err != nil {
					return err
				}
				if opts.watch {
					for active {
						time.Sleep(1 * time.Second)
						active, err = printRouteRuntimeStatus(cmd, cmd.OutOrStdout(), route, sourceServer)
						if err != nil {
							return err
						}
					}
				}
				return nil
			}
			store, err := newRouteTemplateStore(opts.storePath)
			if err != nil {
				return err
			}
			route, err := store.get(args[0])
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "route_id: %s\n", route.Name)
			fmt.Fprintf(cmd.OutOrStdout(), "state: %s\n", route.State)
			printStoredRouteTemplatePlan(cmd.OutOrStdout(), route, nil)
			active, err := printRouteRuntimeStatus(cmd, cmd.OutOrStdout(), route, opts.sourceServer)
			if err != nil {
				return err
			}
			if opts.watch {
				for active {
					time.Sleep(1 * time.Second)
					active, err = printRouteRuntimeStatus(cmd, cmd.OutOrStdout(), route, opts.sourceServer)
					if err != nil {
						return err
					}
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&opts.watch, "watch", false, "Watch route status")
	cmd.Flags().StringVar(&opts.sourceServer, "source-server", "", "Control address of the source groverd that owns route jobs")
	return cmd
}

func printRouteRuntimeStatus(cmd *cobra.Command, w io.Writer, route storedRouteTemplate, sourceServer string) (bool, error) {
	if w == nil {
		return false, nil
	}
	cfg := GetAppConfig(cmd)
	sourceEndpoint := strings.TrimSpace(sourceServer)
	if sourceEndpoint == "" {
		if plan, err := route.plan(); err == nil {
			_ = resolveTransferEndpointCredentials(cmd, &plan.Source, &plan.Destination)
			sourceEndpoint = strings.TrimSpace(plan.Source.ControlEndpoint)
		}
	}
	if sourceEndpoint == "" && cfg != nil {
		sourceEndpoint = strings.TrimSpace(cfg.ServerURL)
	}

	active := false
	if sourceEndpoint != "" && cfg != nil {
		sourceCfg := *cfg
		sourceCfg.ServerURL = sourceEndpoint
		client := gclient.NewClient(sourceCfg)
		if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
			return false, err
		}
		routed := client.RoutedTransfer()
		routeSessions := client.RouteSessionControl()
		if routeSessions != nil {
			sessions, err := routeSessions.ListRouteSessions(cmd.Context(), route.Name, "")
			if err != nil {
				_ = client.Close()
				return false, err
			}
			printRouteSessions(w, sessions)
			for _, session := range sessions {
				if runtimeStateActive(session.GetState()) {
					active = true
				}
			}
		}
		if routed != nil {
			jobs, err := routed.ListTransferJobs(cmd.Context(), route.Name)
			if err != nil {
				_ = client.Close()
				return false, err
			}
			printRouteJobs(w, jobs)
			for _, job := range jobs {
				if runtimeStateActive(job.GetState()) {
					active = true
				}
			}
		}
		_ = client.Close()
	} else {
		fmt.Fprintln(w, "jobs: unavailable")
	}

	relays, err := parseTransferRelays(route.Via)
	if err != nil {
		return active, err
	}
	if len(relays) == 0 {
		fmt.Fprintln(w, "forwards: none")
		return active, nil
	}
	if cfg == nil {
		fmt.Fprintln(w, "forwards: unavailable")
		return active, nil
	}
	for _, relayHop := range relays {
		relayCfg := *cfg
		relayCfg.ServerURL = relayHop.ControlEndpoint
		client := gclient.NewClient(relayCfg)
		if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
			return active, err
		}
		relay := client.RelayControl()
		if relay == nil {
			_ = client.Close()
			return active, fmt.Errorf("relay control service unavailable on %s", relayHop.ControlEndpoint)
		}
		forwards, err := relay.ListForwards(cmd.Context(), route.Name, "")
		if err != nil {
			_ = client.Close()
			return active, err
		}
		printRouteForwards(w, relayHop.ControlEndpoint, forwards)
		for _, forward := range forwards {
			if runtimeStateActive(forward.GetState()) {
				active = true
			}
		}
		_ = client.Close()
	}
	return active, nil
}

func printRouteSessions(w io.Writer, sessions []*pb.RouteSession) {
	if len(sessions) == 0 {
		fmt.Fprintln(w, "sessions: none")
		return
	}
	for _, session := range sessions {
		stats := session.GetStats()
		fmt.Fprintf(w, "session[%s]: state=%s protocol=%s origin=%s direction=%s hops=%d throughput_bps=%.0f errors=%d\n",
			session.GetSessionId(),
			session.GetState().String(),
			session.GetProtocol().String(),
			session.GetConnectionOrigin().String(),
			session.GetDataDirection().String(),
			len(session.GetHops()),
			stats.GetCurrentThroughputBps(),
			stats.GetErrors(),
		)
		printRouteSessionPath(w, session)
		if session.GetReverseSource() != nil || session.GetReverseDestination() != nil || len(session.GetReverseHops()) > 0 {
			printReverseRouteSessionPath(w, session)
		}
	}
}

func printRouteSessionPath(w io.Writer, session *pb.RouteSession) {
	if w == nil || session == nil {
		return
	}
	fmt.Fprintf(w, "  source: %s root=%s\n",
		dataEndpointLabel(session.GetSource().GetDataEndpoint()),
		session.GetSource().GetRootPath(),
	)
	for _, hop := range session.GetHops() {
		stats := hop.GetStats()
		fmt.Fprintf(w, "  hop[%d] %s: %s -> %s state=%s throughput_bps=%.0f errors=%d drops=%d\n",
			hop.GetHopIndex(),
			emptyLabel(hop.GetControlEndpoint(), "relay"),
			dataEndpointLabel(hop.GetIngress()),
			dataEndpointLabel(hop.GetEgress()),
			hop.GetState().String(),
			stats.GetCurrentThroughputBps(),
			stats.GetErrors(),
			stats.GetDrops(),
		)
	}
	fmt.Fprintf(w, "  destination: %s root=%s\n",
		dataEndpointLabel(session.GetDestination().GetDataEndpoint()),
		session.GetDestination().GetRootPath(),
	)
}

func printReverseRouteSessionPath(w io.Writer, session *pb.RouteSession) {
	if w == nil || session == nil {
		return
	}
	fmt.Fprintf(w, "  reverse_source: %s root=%s\n",
		dataEndpointLabel(session.GetReverseSource().GetDataEndpoint()),
		session.GetReverseSource().GetRootPath(),
	)
	for _, hop := range session.GetReverseHops() {
		stats := hop.GetStats()
		fmt.Fprintf(w, "  reverse_hop[%d] %s: %s -> %s state=%s throughput_bps=%.0f errors=%d drops=%d\n",
			hop.GetHopIndex(),
			emptyLabel(hop.GetControlEndpoint(), "relay"),
			dataEndpointLabel(hop.GetIngress()),
			dataEndpointLabel(hop.GetEgress()),
			hop.GetState().String(),
			stats.GetCurrentThroughputBps(),
			stats.GetErrors(),
			stats.GetDrops(),
		)
	}
	fmt.Fprintf(w, "  reverse_destination: %s root=%s\n",
		dataEndpointLabel(session.GetReverseDestination().GetDataEndpoint()),
		session.GetReverseDestination().GetRootPath(),
	)
}

func printRouteJobs(w io.Writer, jobs []*pb.TransferJob) {
	if len(jobs) == 0 {
		fmt.Fprintln(w, "jobs: none")
		return
	}
	for _, job := range jobs {
		stats := job.GetStats()
		fmt.Fprintf(w, "job[%s]: state=%s protocol=%s good_bytes=%d network_bytes=%d files_done=%d files_active=%d throughput_bps=%.0f errors=%d\n",
			job.GetJobId(),
			job.GetState().String(),
			job.GetProtocol().String(),
			job.GetGoodBytes(),
			job.GetNetworkBytes(),
			job.GetFilesDone(),
			job.GetFilesActive(),
			stats.GetCurrentThroughputBps(),
			stats.GetErrors(),
		)
	}
}

func printRouteForwards(w io.Writer, relayEndpoint string, forwards []*pb.ForwardSession) {
	if len(forwards) == 0 {
		fmt.Fprintf(w, "relay[%s]: forwards=none\n", relayEndpoint)
		return
	}
	for _, forward := range forwards {
		stats := forward.GetStats()
		fmt.Fprintf(w, "relay[%s] forward[%s]: hop=%d state=%s protocol=%s ingress_bytes=%d egress_bytes=%d packets=%d throughput_bps=%.0f errors=%d\n",
			relayEndpoint,
			forward.GetForwardId(),
			forward.GetHopIndex(),
			forward.GetState().String(),
			forward.GetProtocol().String(),
			stats.GetIngressBytes(),
			stats.GetEgressBytes(),
			stats.GetPackets(),
			stats.GetCurrentThroughputBps(),
			stats.GetErrors(),
		)
	}
}

func dataEndpointLabel(endpoint *pb.DataEndpoint) string {
	if endpoint == nil || strings.TrimSpace(endpoint.GetHost()) == "" || endpoint.GetPort() == 0 {
		return "(none)"
	}
	host := strings.TrimSpace(endpoint.GetHost())
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	return fmt.Sprintf("%s:%d", host, endpoint.GetPort())
}

func emptyLabel(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func runtimeStateActive(state pb.RuntimeState) bool {
	return state == pb.RuntimeState_RUNTIME_STATE_PREPARING || state == pb.RuntimeState_RUNTIME_STATE_RUNNING || state == pb.RuntimeState_RUNTIME_STATE_READY
}

func routeAbortCommand(opts *routeCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "abort <name>",
		Short:        "Mark a local route template aborted",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.storePath) == "" {
				routeClient, closeFn, err := openRouteConfigControl(cmd)
				if err != nil {
					return err
				}
				defer closeFn()
				serverRoute, err := routeClient.GetRoute(cmd.Context(), args[0])
				if err != nil {
					return err
				}
				route, err := storedRouteTemplateFromServerRoute(serverRoute, "/unused/source-root", "/unused/destination-root")
				if err != nil {
					return err
				}
				sourceServer := opts.sourceServer
				if strings.TrimSpace(sourceServer) == "" {
					sourceServer = serverRoute.GetSource()
				}
				if err := abortRouteRuntime(cmd, route, sourceServer); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "aborted route runtime %s\n", route.Name)
				return nil
			}
			store, err := newRouteTemplateStore(opts.storePath)
			if err != nil {
				return err
			}
			route, err := store.get(args[0])
			if err != nil {
				return err
			}
			route.State = "aborted"
			route.UpdatedAt = time.Now().UTC()
			if err := store.upsert(route); err != nil {
				return err
			}
			if err := abortRouteRuntime(cmd, route, opts.sourceServer); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "aborted route %s\n", route.Name)
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.sourceServer, "source-server", "", "Control address of the source groverd that owns route jobs")
	return cmd
}

func routeCloseCommand(opts *routeCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "close <name>",
		Short:        "Close a prepared route session",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			routeClient, closeFn, err := openRouteConfigControl(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			serverRoute, err := routeClient.GetRoute(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if err := closePreparedRouteSession(cmd, serverRoute, opts.sessionID); err != nil {
				return err
			}
			if strings.TrimSpace(opts.sessionID) == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "closed route sessions for %s\n", serverRoute.GetName())
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "closed route session %s\n", strings.TrimSpace(opts.sessionID))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.sessionID, "session-id", "", "Prepared route session ID to close")
	return cmd
}

func closePreparedRouteSession(cmd *cobra.Command, route *pb.RouteConfig, sessionID string) error {
	if route == nil {
		return fmt.Errorf("route config is required")
	}
	cfg := GetAppConfig(cmd)
	if cfg == nil {
		return fmt.Errorf("app config unavailable")
	}
	sourceCfg := *cfg
	sourceCfg.ServerURL = route.GetSource()
	sourceClient := gclient.NewClient(sourceCfg)
	if err := sourceClient.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
		return err
	}
	defer sourceClient.Close()
	routeSessions := sourceClient.RouteSessionControl()
	if routeSessions == nil {
		return fmt.Errorf("route session service unavailable")
	}
	sessions := []*pb.RouteSession(nil)
	if strings.TrimSpace(sessionID) != "" {
		session, err := routeSessions.GetRouteSession(cmd.Context(), sessionID)
		if err != nil {
			return err
		}
		sessions = append(sessions, session)
	} else {
		listed, err := routeSessions.ListRouteSessions(cmd.Context(), route.GetName(), "")
		if err != nil {
			return err
		}
		sessions = listed
	}
	if len(sessions) == 0 {
		return fmt.Errorf("route %s has no prepared sessions", route.GetName())
	}
	if err := deleteRouteRelayForwards(cmd, route, sessions); err != nil {
		return err
	}
	for _, session := range sessions {
		_, _ = routeSessions.UpdateRouteSessionState(cmd.Context(), session.GetSessionId(), pb.RuntimeState_RUNTIME_STATE_DONE, "")
		_, _ = routeSessions.DeleteRouteSession(cmd.Context(), session.GetSessionId())
	}
	return nil
}

func deleteRouteRelayForwards(cmd *cobra.Command, route *pb.RouteConfig, sessions []*pb.RouteSession) error {
	cfg := GetAppConfig(cmd)
	if cfg == nil || route == nil {
		return nil
	}
	sessionIDs := map[string]struct{}{}
	for _, session := range sessions {
		if session != nil {
			sessionIDs[session.GetSessionId()] = struct{}{}
		}
	}
	for _, relayEndpoint := range route.GetVia() {
		relayCfg := *cfg
		relayCfg.ServerURL = relayEndpoint
		client := gclient.NewClient(relayCfg)
		if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
			return err
		}
		relay := client.RelayControl()
		if relay == nil {
			_ = client.Close()
			return fmt.Errorf("relay control service unavailable on %s", relayEndpoint)
		}
		forwards, err := relay.ListForwards(cmd.Context(), route.GetName(), "")
		if err != nil {
			_ = client.Close()
			return err
		}
		for _, forward := range forwards {
			if len(sessionIDs) > 0 {
				if _, ok := sessionIDs[forward.GetJobId()]; !ok {
					continue
				}
			}
			_, _ = relay.DeleteForward(cmd.Context(), forward.GetForwardId())
		}
		_ = client.Close()
	}
	return nil
}

func abortRouteRuntime(cmd *cobra.Command, route storedRouteTemplate, sourceServer string) error {
	cfg := GetAppConfig(cmd)
	if cfg == nil {
		return nil
	}
	sourceEndpoint := strings.TrimSpace(sourceServer)
	if sourceEndpoint == "" {
		if plan, err := route.plan(); err == nil {
			_ = resolveTransferEndpointCredentials(cmd, &plan.Source, &plan.Destination)
			sourceEndpoint = strings.TrimSpace(plan.Source.ControlEndpoint)
		}
	}
	if sourceEndpoint == "" {
		sourceEndpoint = strings.TrimSpace(cfg.ServerURL)
	}
	if sourceEndpoint != "" {
		sourceCfg := *cfg
		sourceCfg.ServerURL = sourceEndpoint
		client := gclient.NewClient(sourceCfg)
		if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
			return err
		}
		routed := client.RoutedTransfer()
		routeSessions := client.RouteSessionControl()
		if routeSessions != nil {
			sessions, err := routeSessions.ListRouteSessions(cmd.Context(), route.Name, "")
			if err != nil {
				_ = client.Close()
				return err
			}
			for _, session := range sessions {
				if runtimeStateActive(session.GetState()) {
					_, _ = routeSessions.AbortRouteSession(cmd.Context(), session.GetSessionId())
				}
			}
		}
		if routed != nil {
			jobs, err := routed.ListTransferJobs(cmd.Context(), route.Name)
			if err != nil {
				_ = client.Close()
				return err
			}
			for _, job := range jobs {
				if runtimeStateActive(job.GetState()) {
					_, _ = routed.AbortTransferJob(cmd.Context(), job.GetJobId())
				}
			}
		}
		_ = client.Close()
	}

	relays, err := parseTransferRelays(route.Via)
	if err != nil {
		return err
	}
	for _, relayHop := range relays {
		relayCfg := *cfg
		relayCfg.ServerURL = relayHop.ControlEndpoint
		client := gclient.NewClient(relayCfg)
		if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
			return err
		}
		relay := client.RelayControl()
		if relay == nil {
			_ = client.Close()
			return fmt.Errorf("relay control service unavailable on %s", relayHop.ControlEndpoint)
		}
		forwards, err := relay.ListForwards(cmd.Context(), route.Name, "")
		if err != nil {
			_ = client.Close()
			return err
		}
		for _, forward := range forwards {
			_, _ = relay.DeleteForward(cmd.Context(), forward.GetForwardId())
		}
		_ = client.Close()
	}
	return nil
}

func newRouteTemplateStore(path string) (routeTemplateStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return routeTemplateStore{}, err
		}
		path = filepath.Join(home, ".grover", "routes.toml")
	}
	return routeTemplateStore{path: path}, nil
}

func validateRouteTemplate(route storedRouteTemplate) error {
	if strings.TrimSpace(route.Name) == "" {
		return fmt.Errorf("route name is required")
	}
	if strings.ContainsAny(route.Name, " \t\r\n/\\") {
		return fmt.Errorf("route name %q must not contain whitespace or path separators", route.Name)
	}
	if (strings.TrimSpace(route.Source) == "") != (strings.TrimSpace(route.Destination) == "") {
		return fmt.Errorf("route source and destination defaults must be set together")
	}
	if _, err := normalizeConnectionOrigin(route.ConnectionOrigin); err != nil {
		return err
	}
	if _, err := normalizeDataDirection(route.DataDirection); err != nil {
		return err
	}
	return nil
}

func (s routeTemplateStore) load() (routeTemplateFile, error) {
	var file routeTemplateFile
	if strings.TrimSpace(s.path) == "" {
		return file, fmt.Errorf("route store path is required")
	}
	if _, err := os.Stat(s.path); err != nil {
		if os.IsNotExist(err) {
			return file, nil
		}
		return file, err
	}
	if _, err := toml.DecodeFile(s.path, &file); err != nil {
		return file, fmt.Errorf("load route store %s: %w", s.path, err)
	}
	return file, nil
}

func (s routeTemplateStore) save(file routeTemplateFile) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	sort.Slice(file.Routes, func(i, j int) bool {
		return file.Routes[i].Name < file.Routes[j].Name
	})
	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(file); err != nil {
		return err
	}
	return os.WriteFile(s.path, buf.Bytes(), 0o600)
}

func (s routeTemplateStore) upsert(route storedRouteTemplate) error {
	if err := validateRouteTemplate(route); err != nil {
		return err
	}
	file, err := s.load()
	if err != nil {
		return err
	}
	for i := range file.Routes {
		if file.Routes[i].Name == route.Name {
			if route.CreatedAt.IsZero() {
				route.CreatedAt = file.Routes[i].CreatedAt
			}
			file.Routes[i] = route
			return s.save(file)
		}
	}
	file.Routes = append(file.Routes, route)
	return s.save(file)
}

func (s routeTemplateStore) list() ([]storedRouteTemplate, error) {
	file, err := s.load()
	if err != nil {
		return nil, err
	}
	routes := append([]storedRouteTemplate(nil), file.Routes...)
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].Name < routes[j].Name
	})
	return routes, nil
}

func (s routeTemplateStore) get(name string) (storedRouteTemplate, error) {
	name = strings.TrimSpace(name)
	routes, err := s.list()
	if err != nil {
		return storedRouteTemplate{}, err
	}
	for _, route := range routes {
		if route.Name == name {
			return route, nil
		}
	}
	return storedRouteTemplate{}, fmt.Errorf("route %q not found", name)
}

func (r storedRouteTemplate) plan() (TransferRoutePlan, error) {
	if strings.TrimSpace(r.Source) == "" || strings.TrimSpace(r.Destination) == "" {
		return TransferRoutePlan{}, fmt.Errorf("route %q has no source/destination defaults; use transfer --route %s <source> <destination>", r.Name, r.Name)
	}
	src, err := parseLocation(r.Source)
	if err != nil {
		return TransferRoutePlan{}, err
	}
	dst, err := parseLocation(r.Destination)
	if err != nil {
		return TransferRoutePlan{}, err
	}
	return buildTransferRoutePlan(src, dst, CopyOptions{
		Via:              r.Via,
		Protocol:         r.Protocol,
		ParallelStreams:  r.ParallelStreams,
		Concurrency:      r.Concurrency,
		ConnectionOrigin: r.ConnectionOrigin,
		DataDirection:    r.DataDirection,
	})
}

func routeProtocol(protocol string) string {
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	if protocol == "" {
		return "config"
	}
	return protocol
}

func routeParallelStreams(streams int) int {
	if streams <= 0 {
		return 1
	}
	return streams
}

func routeConnectionOrigin(origin string) string {
	normalized, err := normalizeConnectionOrigin(origin)
	if err != nil {
		return "source"
	}
	return normalized
}

func routeDataDirection(direction string) string {
	normalized, err := normalizeDataDirection(direction)
	if err != nil {
		return "source-to-destination"
	}
	return normalized
}

func routeRelaysLabel(route storedRouteTemplate) string {
	relays, err := parseTransferRelays(route.Via)
	if err != nil || len(relays) == 0 {
		return "(direct)"
	}
	labels := make([]string, 0, len(relays))
	for _, relay := range relays {
		labels = append(labels, relay.Raw)
	}
	return strings.Join(labels, " -> ")
}

func printStoredRouteTemplatePlan(w interface {
	Write([]byte) (int, error)
}, route storedRouteTemplate, relays []TransferRelayHop,
) {
	if w == nil {
		return
	}
	if relays == nil {
		relays, _ = parseTransferRelays(route.Via)
	}
	fmt.Fprintf(w, "relays: %s\n", routeRelaysLabel(route))
	fmt.Fprintf(w, "protocol: %s\n", routeProtocol(route.Protocol))
	fmt.Fprintf(w, "connection_origin: %s\n", routeConnectionOrigin(route.ConnectionOrigin))
	fmt.Fprintf(w, "data_direction: %s\n", routeDataDirection(route.DataDirection))
	fmt.Fprintf(w, "parallel_streams: %d\n", routeParallelStreams(route.ParallelStreams))
	fmt.Fprintf(w, "concurrency: %d\n", route.Concurrency)
	if strings.TrimSpace(route.Source) != "" || strings.TrimSpace(route.Destination) != "" {
		fmt.Fprintf(w, "defaults: %s -> %s\n", route.Source, route.Destination)
	}
	_ = relays
}

func storedRouteTemplateFromServerRoute(route *pb.RouteConfig, sourcePath, destinationPath string) (storedRouteTemplate, error) {
	if route == nil {
		return storedRouteTemplate{}, fmt.Errorf("server route is required")
	}
	source, err := routeEndpointLocation(route.GetSource(), sourcePath, "source")
	if err != nil {
		return storedRouteTemplate{}, err
	}
	destination, err := routeEndpointLocation(route.GetDestination(), destinationPath, "destination")
	if err != nil {
		return storedRouteTemplate{}, err
	}
	return storedRouteTemplate{
		Name:             route.GetName(),
		Source:           source,
		Destination:      destination,
		Via:              append([]string(nil), route.GetVia()...),
		Protocol:         routeProtocolLabel(route.GetProtocol()),
		ParallelStreams:  1,
		Concurrency:      1,
		ConnectionOrigin: routeConnectionOriginLabel(route.GetConnectionOrigin()),
		DataDirection:    routeDataDirectionLabel(route.GetDataDirection()),
		State:            "configured",
		CreatedAt:        time.Unix(0, route.GetCreatedAtUnixNano()).UTC(),
		UpdatedAt:        time.Unix(0, route.GetUpdatedAtUnixNano()).UTC(),
	}, nil
}

func openRouteConfigControl(cmd *cobra.Command) (gclient.RouteConfigAPI, func(), error) {
	cfg := GetAppConfig(cmd)
	if cfg == nil {
		return nil, func() {}, fmt.Errorf("app config unavailable")
	}
	client := gclient.NewClient(*cfg)
	if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
		return nil, func() {}, err
	}
	routeClient := client.RouteConfigControl()
	if routeClient == nil {
		_ = client.Close()
		return nil, func() {}, fmt.Errorf("route config service unavailable")
	}
	return routeClient, func() { _ = client.Close() }, nil
}

func printServerRouteConfig(w io.Writer, route *pb.RouteConfig) {
	if w == nil || route == nil {
		return
	}
	fmt.Fprintf(w, "route_id: %s\n", route.GetName())
	fmt.Fprintf(w, "source: %s\n", route.GetSource())
	fmt.Fprintf(w, "destination: %s\n", route.GetDestination())
	fmt.Fprintf(w, "relays: %s\n", serverRouteRelaysLabel(route))
	fmt.Fprintf(w, "protocol: %s\n", routeProtocolLabel(route.GetProtocol()))
	fmt.Fprintf(w, "connection_origin: %s\n", routeConnectionOriginLabel(route.GetConnectionOrigin()))
	fmt.Fprintf(w, "data_direction: %s\n", routeDataDirectionLabel(route.GetDataDirection()))
}

func printServerRouteTable(w io.Writer, routes []*pb.RouteConfig) error {
	if w == nil {
		return nil
	}
	rows := pterm.TableData{{
		"ROUTE",
		"SOURCE",
		"RELAYS",
		"DESTINATION",
		"PROTOCOL",
		"CONNECT FROM",
		"FLOW",
	}}
	for _, route := range routes {
		if route == nil {
			continue
		}
		rows = append(rows, []string{
			route.GetName(),
			route.GetSource(),
			serverRouteRelaysLabel(route),
			route.GetDestination(),
			routeProtocolLabel(route.GetProtocol()),
			routeConnectionOriginLabel(route.GetConnectionOrigin()),
			routeDataDirectionLabel(route.GetDataDirection()),
		})
	}
	rendered, err := pterm.DefaultTable.WithHasHeader().WithData(rows).Srender()
	if err != nil {
		return err
	}
	fmt.Fprintln(w, rendered)
	return nil
}

func printStoredRouteTable(w io.Writer, routes []storedRouteTemplate) error {
	if w == nil {
		return nil
	}
	rows := pterm.TableData{{
		"ROUTE",
		"STATE",
		"RELAYS",
		"PROTOCOL",
		"CONNECT FROM",
		"FLOW",
		"DEFAULTS",
	}}
	for _, route := range routes {
		defaults := "-"
		if strings.TrimSpace(route.Source) != "" || strings.TrimSpace(route.Destination) != "" {
			defaults = strings.TrimSpace(route.Source) + " -> " + strings.TrimSpace(route.Destination)
		}
		rows = append(rows, []string{
			route.Name,
			route.State,
			routeRelaysLabel(route),
			routeProtocol(route.Protocol),
			routeConnectionOrigin(route.ConnectionOrigin),
			routeDataDirection(route.DataDirection),
			defaults,
		})
	}
	rendered, err := pterm.DefaultTable.WithHasHeader().WithData(rows).Srender()
	if err != nil {
		return err
	}
	fmt.Fprintln(w, rendered)
	return nil
}

func serverRouteRelaysLabel(route *pb.RouteConfig) string {
	if route == nil || len(route.GetVia()) == 0 {
		return "(direct)"
	}
	return strings.Join(route.GetVia(), " -> ")
}

type routeConfigView struct {
	Name             string   `json:"name"`
	Source           string   `json:"source"`
	Destination      string   `json:"destination"`
	Via              []string `json:"via,omitempty"`
	Protocol         string   `json:"protocol"`
	ConnectionOrigin string   `json:"connection_origin"`
	DataDirection    string   `json:"data_direction"`
	CreatedAt        string   `json:"created_at,omitempty"`
	UpdatedAt        string   `json:"updated_at,omitempty"`
}

func routeConfigViewFromPB(route *pb.RouteConfig) routeConfigView {
	if route == nil {
		return routeConfigView{}
	}
	view := routeConfigView{
		Name:             route.GetName(),
		Source:           route.GetSource(),
		Destination:      route.GetDestination(),
		Via:              append([]string(nil), route.GetVia()...),
		Protocol:         routeProtocolLabel(route.GetProtocol()),
		ConnectionOrigin: routeConnectionOriginLabel(route.GetConnectionOrigin()),
		DataDirection:    routeDataDirectionLabel(route.GetDataDirection()),
	}
	if route.GetCreatedAtUnixNano() > 0 {
		view.CreatedAt = time.Unix(0, route.GetCreatedAtUnixNano()).UTC().Format(time.RFC3339Nano)
	}
	if route.GetUpdatedAtUnixNano() > 0 {
		view.UpdatedAt = time.Unix(0, route.GetUpdatedAtUnixNano()).UTC().Format(time.RFC3339Nano)
	}
	return view
}

func writeRouteConfigJSON(w io.Writer, route *pb.RouteConfig) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(routeConfigViewFromPB(route))
}

func writeRouteConfigsJSON(w io.Writer, routes []*pb.RouteConfig) error {
	views := make([]routeConfigView, 0, len(routes))
	for _, route := range routes {
		views = append(views, routeConfigViewFromPB(route))
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(struct {
		Routes []routeConfigView `json:"routes"`
	}{Routes: views})
}

func routeProtocolLabel(protocol pb.DataProtocol) string {
	switch protocol {
	case pb.DataProtocol_DATA_PROTOCOL_UDP:
		return "udp"
	default:
		return "tcp"
	}
}

func routeConnectionOriginLabel(origin pb.ConnectionOrigin) string {
	switch origin {
	case pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION:
		return "destination"
	default:
		return "source"
	}
}

func routeDataDirectionLabel(direction pb.DataDirection) string {
	switch direction {
	case pb.DataDirection_DATA_DIRECTION_DESTINATION_TO_SOURCE:
		return "destination-to-source"
	default:
		return "source-to-destination"
	}
}
