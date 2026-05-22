package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/jgoldverg/grover/pkg/gclient"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/util"
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
}

type routeTemplateStore struct {
	path string
}

type routeTemplateFile struct {
	Routes []storedRouteTemplate `toml:"routes"`
}

type storedRouteTemplate struct {
	Name            string    `toml:"name"`
	Source          string    `toml:"source"`
	Destination     string    `toml:"destination"`
	Via             []string  `toml:"via"`
	Protocol        string    `toml:"protocol"`
	ParallelStreams int       `toml:"parallel_streams"`
	Concurrency     int       `toml:"concurrency"`
	State           string    `toml:"state"`
	CreatedAt       time.Time `toml:"created_at"`
	UpdatedAt       time.Time `toml:"updated_at"`
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
	cmd.AddCommand(routeListCommand(&opts))
	cmd.AddCommand(routeStartCommand(&opts))
	cmd.AddCommand(routeStatusCommand(&opts))
	cmd.AddCommand(routeAbortCommand(&opts))
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
				Via:             opts.via,
				Protocol:        opts.protocol,
				ParallelStreams: opts.parallelStreams,
				Concurrency:     opts.concurrency,
			}
			if err := copyOpts.validate(); err != nil {
				return err
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
				Name:            strings.TrimSpace(args[0]),
				Source:          source,
				Destination:     destination,
				Via:             append([]string(nil), opts.via...),
				Protocol:        routeProtocol(copyOpts.Protocol),
				ParallelStreams: routeParallelStreams(copyOpts.ParallelStreams),
				Concurrency:     copyOpts.effectiveConcurrency(),
				State:           "prepared",
				CreatedAt:       now,
				UpdatedAt:       now,
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
	return cmd
}

func routeListCommand(opts *routeCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:          "list",
		Short:        "List local route templates",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
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
			for _, route := range routes {
				fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\n", route.Name, route.State, routeRelaysLabel(route))
			}
			return nil
		},
	}
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

func startDirectRoute(cmd *cobra.Command, route storedRouteTemplate, opts CopyOptions) (*pb.TransferJob, error) {
	plan, err := route.plan()
	if err != nil {
		return nil, err
	}
	if err := resolveTransferEndpointCredentials(cmd, &plan.Source, &plan.Destination); err != nil {
		return nil, err
	}
	baseCfg := GetAppConfig(cmd)
	if baseCfg == nil {
		return nil, fmt.Errorf("app config unavailable")
	}
	sourceCfg := *baseCfg
	if strings.TrimSpace(plan.Source.ControlEndpoint) != "" {
		sourceCfg.ServerURL = strings.TrimSpace(plan.Source.ControlEndpoint)
	} else if strings.TrimSpace(opts.SourceServer) != "" {
		sourceCfg.ServerURL = strings.TrimSpace(opts.SourceServer)
	}
	destCfg := sourceCfg
	if strings.TrimSpace(plan.Destination.ControlEndpoint) != "" {
		destCfg.ServerURL = strings.TrimSpace(plan.Destination.ControlEndpoint)
	} else if strings.TrimSpace(opts.DestinationServer) != "" {
		destCfg.ServerURL = strings.TrimSpace(opts.DestinationServer)
	}
	sourceClient := gclient.NewClient(sourceCfg)
	if err := sourceClient.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
		return nil, err
	}
	defer sourceClient.Close()
	destClient := sourceClient
	if destCfg.ServerURL != sourceCfg.ServerURL {
		destClient = gclient.NewClient(destCfg)
		if err := destClient.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
			return nil, err
		}
		defer destClient.Close()
	}
	sourceRouted := sourceClient.RoutedTransfer()
	destRouted := destClient.RoutedTransfer()
	if sourceRouted == nil || destRouted == nil {
		return nil, fmt.Errorf("routed transfer service unavailable")
	}
	jobID := fmt.Sprintf("%s-%d", route.Name, time.Now().UnixNano())
	protocol := dataProtocol(plan.Protocol)
	source, err := sourceRouted.PrepareTransferEndpoint(cmd.Context(), &pb.PrepareTransferEndpointRequest{
		RouteId:  route.Name,
		JobId:    jobID,
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol: protocol,
		RootPath: plan.Source.Path,
	})
	if err != nil {
		return nil, err
	}
	dest, err := destRouted.PrepareTransferEndpoint(cmd.Context(), &pb.PrepareTransferEndpointRequest{
		RouteId:  route.Name,
		JobId:    jobID,
		Role:     pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol: protocol,
		RootPath: plan.Destination.Path,
	})
	if err != nil {
		return nil, err
	}
	cleanup, err := materializeRelayForwards(cmd, route.Name, jobID, protocol, plan.Relays, dest)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	job, err := sourceRouted.StartTransferJob(cmd.Context(), &pb.StartTransferJobRequest{
		RouteId:        route.Name,
		JobId:          jobID,
		Source:         source,
		Destination:    dest,
		FilesInFlight:  uint32(plan.Concurrency),
		StreamsPerFile: uint32(plan.ParallelStreams),
	})
	if err != nil {
		return nil, err
	}
	return monitorRoutedTransferJob(cmd, sourceRouted, job, opts)
}

func monitorRoutedTransferJob(cmd *cobra.Command, routed gclient.RoutedTransferAPI, job *pb.TransferJob, opts CopyOptions) (*pb.TransferJob, error) {
	if job == nil {
		return nil, fmt.Errorf("transfer job was not returned by source groverd")
	}
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "transfer_job: %s\n", job.GetJobId())
	fmt.Fprintf(out, "state: %s\n", job.GetState().String())
	if opts.uiMode() == "live" {
		printTransferJobStatus(out, job)
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
			printTransferJobStatus(out, job)
		}
	}
	return job, nil
}

func isActiveTransferState(state pb.RuntimeState) bool {
	return state == pb.RuntimeState_RUNTIME_STATE_RUNNING || state == pb.RuntimeState_RUNTIME_STATE_PREPARING
}

func materializeRelayForwards(cmd *cobra.Command, routeID string, jobID string, protocol pb.DataProtocol, relays []TransferRelayHop, dest *pb.TransferEndpoint) (func(), error) {
	next := clonePBEndpoint(dest.GetDataEndpoint())
	if next == nil || strings.TrimSpace(next.GetHost()) == "" || next.GetPort() == 0 {
		return func() {}, fmt.Errorf("destination did not return a data endpoint")
	}
	baseCfg := GetAppConfig(cmd)
	if baseCfg == nil && len(relays) > 0 {
		return func() {}, fmt.Errorf("app config unavailable")
	}
	type relayLease struct {
		client *gclient.Client
		id     string
	}
	leases := []relayLease{}
	cleanup := func() {
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
		client := gclient.NewClient(relayCfg)
		if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
			cleanup()
			return func() {}, err
		}
		relay := client.RelayControl()
		if relay == nil {
			_ = client.Close()
			cleanup()
			return func() {}, fmt.Errorf("relay control service unavailable on %s", relays[i].ControlEndpoint)
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
			return func() {}, err
		}
		leases = append(leases, relayLease{client: client, id: forward.GetForwardId()})
		next = clonePBEndpoint(forward.GetIngress())
	}
	dest.DataEndpoint = next
	return cleanup, nil
}

func clonePBEndpoint(ep *pb.DataEndpoint) *pb.DataEndpoint {
	if ep == nil {
		return nil
	}
	return &pb.DataEndpoint{Host: ep.GetHost(), Port: ep.GetPort()}
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
		Via:             r.Via,
		Protocol:        r.Protocol,
		ParallelStreams: r.ParallelStreams,
		Concurrency:     r.Concurrency,
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
}, route storedRouteTemplate, relays []TransferRelayHop) {
	if w == nil {
		return
	}
	if relays == nil {
		relays, _ = parseTransferRelays(route.Via)
	}
	fmt.Fprintf(w, "relays: %s\n", routeRelaysLabel(route))
	fmt.Fprintf(w, "protocol: %s\n", routeProtocol(route.Protocol))
	fmt.Fprintf(w, "parallel_streams: %d\n", routeParallelStreams(route.ParallelStreams))
	fmt.Fprintf(w, "concurrency: %d\n", route.Concurrency)
	if strings.TrimSpace(route.Source) != "" || strings.TrimSpace(route.Destination) != "" {
		fmt.Fprintf(w, "defaults: %s -> %s\n", route.Source, route.Destination)
	}
	_ = relays
}
