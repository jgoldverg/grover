package cli

import (
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/gclient"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/util"
	"github.com/spf13/cobra"
)

type RemoteRef struct {
	isRemote        bool
	RemoteName      string
	ControlEndpoint string
	Bucket          string
	Path            string
	Raw             string
	ExpectDirectory bool
}

type CopyOptions struct {
	Concurrency        int
	NoUI               bool
	DryRun             bool
	RouteFile          string
	RouteName          string
	RouteStore         string
	Protocol           string
	UIMode             string
	UIIntervalMs       int
	ParallelStreams    int
	Via                []string
	SourceServer       string
	DestinationServer  string
	UDPFlowControl     string
	UDPWindowPackets   int
	UDPWindowBytes     int
	UDPAckEveryPackets int
	UDPAckEveryMs      int
	UDPBatchPackets    int
	MTU                string
}

func SimpleCopy() *cobra.Command {
	var opts CopyOptions
	cmd := &cobra.Command{
		Use:     "transfer <source> <destination>",
		Short:   "Simple grover udp based copy to and from grover server",
		Long:    "Simple grover udp based copy to and from grover server",
		Aliases: []string{"c", "cp"},
		Args: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.RouteFile) != "" || strings.TrimSpace(opts.RouteName) != "" {
				if len(args) == 0 || len(args) == 2 {
					return nil
				}
				return fmt.Errorf("transfer with --route or --route-file accepts either zero args or <source> <destination>")
			}
			return cobra.ExactArgs(2)(cmd, args)
		},
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			args, err = applyTransferJobSpec(cmd, opts.RouteFile, args, &opts)
			if err != nil {
				return err
			}
			if err := opts.validate(); err != nil {
				return err
			}
			args, err = applyPreparedRouteTemplate(cmd, args, &opts)
			if err != nil {
				return err
			}
			src, err := parseLocation(args[0])
			if err != nil {
				return err
			}
			dst, err := parseLocation(args[1])
			if err != nil {
				return err
			}
			if err := resolveTransferEndpointCredentials(cmd, &src, &dst); err != nil {
				return err
			}
			plan, err := buildTransferRoutePlan(src, dst, opts)
			if err != nil {
				return err
			}
			if opts.DryRun {
				printTransferRoutePlan(cmd.OutOrStdout(), plan)
				return nil
			}
			if opts.shouldUseRoutedJob(plan) {
				job, err := startOneShotRoutedTransfer(cmd, args[0], args[1], opts)
				if err != nil {
					return err
				}
				if opts.uiMode() == "live" {
					return nil
				}
				fmt.Fprintf(cmd.OutOrStdout(), "transfer_job: %s\n", job.GetJobId())
				fmt.Fprintf(cmd.OutOrStdout(), "state: %s\n", job.GetState().String())
				if job.GetErrorMessage() != "" {
					fmt.Fprintf(cmd.OutOrStdout(), "error: %s\n", job.GetErrorMessage())
				}
				fmt.Fprintf(cmd.OutOrStdout(), "files_done: %d\n", job.GetFilesDone())
				fmt.Fprintf(cmd.OutOrStdout(), "good_bytes: %d\n", job.GetGoodBytes())
				return nil
			}
			return fmt.Errorf("transfer requires local paths or host:port:/path endpoints so groverd owns the data plane")
		},
	}
	cmd.Flags().IntVar(&opts.Concurrency, "concurrency", 4, "Maximum number of files to transfer in parallel")
	cmd.Flags().BoolVar(&opts.NoUI, "no-ui", false, "Disable live progress and metrics output")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "Print the planned transfer route without starting a transfer")
	cmd.Flags().StringVar(&opts.RouteFile, "route-file", "", "Path to a TOML transfer route/job spec")
	cmd.Flags().StringVar(&opts.RouteName, "route", "", "Prepared route template name")
	cmd.Flags().StringVar(&opts.RouteStore, "route-store", "", "Path to local route template store")
	_ = cmd.Flags().MarkHidden("route-store")
	cmd.Flags().StringVar(&opts.Protocol, "protocol", "", "Transfer data-plane protocol (udp|tcp)")
	cmd.Flags().IntVar(&opts.ParallelStreams, "parallel-streams", 0, "Per-file parallel streams/ranges (0 uses config)")
	cmd.Flags().StringArrayVar(&opts.Via, "via", nil, "Relay hop to insert into the transfer route; repeat or use comma-separated values")
	cmd.Flags().StringVar(&opts.SourceServer, "source-server", "", "Control address of the source groverd for routed transfer jobs")
	cmd.Flags().StringVar(&opts.DestinationServer, "destination-server", "", "Control address of the destination groverd for routed transfer jobs")
	cmd.Flags().StringVar(&opts.UDPFlowControl, "udp-flow-control", "", "UDP flow control mode (fixed|bbr)")
	cmd.Flags().IntVar(&opts.UDPWindowPackets, "udp-window-packets", 0, "UDP max in-flight packets per stream (0 uses config)")
	cmd.Flags().IntVar(&opts.UDPWindowBytes, "udp-window-bytes", 0, "UDP max in-flight bytes per stream (0 derives from packets)")
	cmd.Flags().IntVar(&opts.UDPAckEveryPackets, "udp-ack-every-packets", 0, "UDP ACK every N packets (0 uses config)")
	cmd.Flags().IntVar(&opts.UDPAckEveryMs, "udp-ack-every-ms", 0, "UDP ACK interval in milliseconds (0 uses config)")
	cmd.Flags().IntVar(&opts.UDPBatchPackets, "udp-batch-packets", 0, "UDP packets per kernel batch call (0 uses config)")
	cmd.Flags().StringVar(&opts.MTU, "mtu", "", "UDP MTU override (auto|bytes)")
	cmd.Flags().StringVar(&opts.UIMode, "ui", "summary", "Transfer UI mode (summary|live|none)")
	cmd.Flags().IntVar(&opts.UIIntervalMs, "ui-interval-ms", 2000, "Live metrics UI refresh interval in milliseconds")
	cmd.AddCommand(TransferTuneCommand())
	cmd.AddCommand(TransferStatusCommand())
	return cmd
}

func startOneShotRoutedTransfer(cmd *cobra.Command, src string, dst string, opts CopyOptions) (*pb.TransferJob, error) {
	name := strings.TrimSpace(opts.RouteName)
	if name == "" {
		name = fmt.Sprintf("transfer-%d", time.Now().UnixNano())
	}
	route := storedRouteTemplate{
		Name:            name,
		Source:          src,
		Destination:     dst,
		Via:             append([]string(nil), opts.Via...),
		Protocol:        opts.Protocol,
		ParallelStreams: opts.ParallelStreams,
		Concurrency:     opts.effectiveConcurrency(),
		State:           "prepared",
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}
	return startDirectRoute(cmd, route, opts)
}

func applyPreparedRouteTemplate(cmd *cobra.Command, args []string, opts *CopyOptions) ([]string, error) {
	if opts == nil {
		return args, nil
	}
	routeName := strings.TrimSpace(opts.RouteName)
	if routeName == "" {
		return args, nil
	}
	store, err := newRouteTemplateStore(opts.RouteStore)
	if err != nil {
		return nil, err
	}
	route, err := store.get(routeName)
	if err != nil {
		return nil, err
	}
	if len(args) == 0 {
		source := strings.TrimSpace(route.Source)
		destination := strings.TrimSpace(route.Destination)
		if source == "" || destination == "" {
			return nil, fmt.Errorf("route %q has no source/destination defaults; pass <source> <destination>", routeName)
		}
		args = []string{source, destination}
	}
	if !cmd.Flags().Changed("protocol") && strings.TrimSpace(route.Protocol) != "" {
		opts.Protocol = route.Protocol
	}
	if !cmd.Flags().Changed("parallel-streams") && route.ParallelStreams > 0 {
		opts.ParallelStreams = route.ParallelStreams
	}
	if !cmd.Flags().Changed("concurrency") && route.Concurrency > 0 {
		opts.Concurrency = route.Concurrency
	}
	combinedVia := append([]string(nil), route.Via...)
	combinedVia = append(combinedVia, opts.Via...)
	opts.Via = combinedVia
	return args, nil
}

func resolveTransferEndpointCredentials(cmd *cobra.Command, refs ...*RemoteRef) error {
	for _, ref := range refs {
		if ref == nil || !ref.isRemote || strings.TrimSpace(ref.ControlEndpoint) != "" {
			continue
		}
		name := strings.TrimSpace(ref.RemoteName)
		if name == "" {
			continue
		}
		if strings.TrimSpace(ref.Bucket) != "" {
			return fmt.Errorf("credential endpoint %q must use name:/absolute/path for routed transfers", ref.Raw)
		}
		cfg := GetAppConfig(cmd)
		if cfg == nil {
			return fmt.Errorf("app config unavailable while resolving credential endpoint %q", name)
		}
		cred, err := loadCredentialByRef(cfg, name, uuid.Nil)
		if err != nil {
			return fmt.Errorf("load credential %q: %w", name, err)
		}
		basic, ok := cred.(*backend.BasicAuthCredential)
		if !ok {
			return fmt.Errorf("credential %q must be a basic credential with a groverd control URL", cred.GetName())
		}
		endpoint := strings.TrimSpace(basic.GetUrl())
		if endpoint == "" {
			return fmt.Errorf("credential %q has empty groverd control URL", cred.GetName())
		}
		ref.ControlEndpoint = endpoint
		ref.RemoteName = endpoint
	}
	return nil
}

func TransferTuneCommand() *cobra.Command {
	var concurrency int
	var parallelStreams int
	var sourceServer string
	cmd := &cobra.Command{
		Use:          "tune <transfer_id>",
		Short:        "Update runtime transfer concurrency",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if concurrency < 0 {
				return fmt.Errorf("--concurrency must be >= 0")
			}
			if parallelStreams < 0 {
				return fmt.Errorf("--parallel-streams must be >= 0")
			}
			if concurrency == 0 && parallelStreams == 0 {
				return fmt.Errorf("set at least one of --concurrency or --parallel-streams")
			}
			cfg := GetAppConfig(cmd)
			if cfg == nil {
				return fmt.Errorf("app config unavailable")
			}
			if strings.TrimSpace(sourceServer) != "" {
				cfg = cloneAppConfig(cfg)
				cfg.ServerURL = strings.TrimSpace(sourceServer)
			}
			client := gclient.NewClient(*cfg)
			if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
				return err
			}
			defer client.Close()
			routed := client.RoutedTransfer()
			if routed == nil {
				return fmt.Errorf("routed transfer service unavailable")
			}
			job, err := routed.UpdateTransferConcurrency(cmd.Context(), args[0], uint32(concurrency), uint32(parallelStreams))
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "transfer_job: %s\n", job.GetJobId())
			fmt.Fprintf(cmd.OutOrStdout(), "state: %s\n", job.GetState().String())
			fmt.Fprintf(cmd.OutOrStdout(), "files_in_flight: %d\n", job.GetFilesInFlight())
			fmt.Fprintf(cmd.OutOrStdout(), "streams_per_file: %d\n", job.GetStreamsPerFile())
			return nil
		},
	}
	cmd.Flags().IntVar(&concurrency, "concurrency", 0, "Maximum number of files to transfer in parallel")
	cmd.Flags().IntVar(&parallelStreams, "parallel-streams", 0, "Per-file parallel streams/ranges")
	cmd.Flags().StringVar(&sourceServer, "source-server", "", "Control address of the source groverd that owns the transfer job")
	return cmd
}

func TransferStatusCommand() *cobra.Command {
	var sourceServer string
	var watch bool
	cmd := &cobra.Command{
		Use:          "status <transfer_id>",
		Short:        "Show routed transfer job stats",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := GetAppConfig(cmd)
			if cfg == nil {
				return fmt.Errorf("app config unavailable")
			}
			if strings.TrimSpace(sourceServer) != "" {
				cfg = cloneAppConfig(cfg)
				cfg.ServerURL = strings.TrimSpace(sourceServer)
			}
			client := gclient.NewClient(*cfg)
			if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
				return err
			}
			defer client.Close()
			routed := client.RoutedTransfer()
			if routed == nil {
				return fmt.Errorf("routed transfer service unavailable")
			}
			if !watch {
				job, err := routed.GetTransferJob(cmd.Context(), args[0])
				if err != nil {
					return err
				}
				printTransferJobStatus(cmd.OutOrStdout(), job)
				return nil
			}
			stream, err := routed.StreamTransferStats(cmd.Context(), args[0], "")
			if err != nil {
				return err
			}
			for {
				job, err := stream.Recv()
				if err != nil {
					return err
				}
				printTransferJobStatus(cmd.OutOrStdout(), job)
				if job.GetState() != pb.RuntimeState_RUNTIME_STATE_RUNNING && job.GetState() != pb.RuntimeState_RUNTIME_STATE_PREPARING {
					return nil
				}
			}
		},
	}
	cmd.Flags().StringVar(&sourceServer, "source-server", "", "Control address of the source groverd that owns the transfer job")
	cmd.Flags().BoolVar(&watch, "watch", false, "Stream transfer stats until the job exits")
	return cmd
}

func printTransferJobStatus(w io.Writer, job *pb.TransferJob) {
	if w == nil || job == nil {
		return
	}
	stats := job.GetStats()
	fmt.Fprintf(w, "transfer_job: %s\n", job.GetJobId())
	fmt.Fprintf(w, "route_id: %s\n", job.GetRouteId())
	fmt.Fprintf(w, "state: %s\n", job.GetState().String())
	if job.GetErrorMessage() != "" {
		fmt.Fprintf(w, "error: %s\n", job.GetErrorMessage())
	}
	fmt.Fprintf(w, "protocol: %s\n", job.GetProtocol().String())
	fmt.Fprintf(w, "good_bytes: %d\n", job.GetGoodBytes())
	fmt.Fprintf(w, "network_bytes: %d\n", job.GetNetworkBytes())
	fmt.Fprintf(w, "disk_read_bytes: %d\n", job.GetDiskReadBytes())
	fmt.Fprintf(w, "disk_write_bytes: %d\n", job.GetDiskWriteBytes())
	fmt.Fprintf(w, "files_done: %d\n", job.GetFilesDone())
	fmt.Fprintf(w, "files_active: %d\n", job.GetFilesActive())
	fmt.Fprintf(w, "streams_active: %d\n", job.GetStreamsActive())
	fmt.Fprintf(w, "throughput_bps: %.0f\n", stats.GetCurrentThroughputBps())
	fmt.Fprintf(w, "avg_throughput_bps: %.0f\n", stats.GetAverageThroughputBps())
	fmt.Fprintf(w, "errors: %d\n", stats.GetErrors())
}

func cloneAppConfig(cfg *internal.AppConfig) *internal.AppConfig {
	if cfg == nil {
		return nil
	}
	cp := *cfg
	return &cp
}

var remoteRe = regexp.MustCompile(`^([A-Za-z0-9_\-]+):(.*)$`)

func parseLocation(input string) (RemoteRef, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return RemoteRef{}, fmt.Errorf("location is required")
	}
	ref := RemoteRef{Raw: trimmed}

	if ref, ok, err := parseGroverdEndpointLocation(trimmed); ok || err != nil {
		return ref, err
	}

	if m := remoteRe.FindStringSubmatch(trimmed); m != nil {
		ref.isRemote = true
		ref.RemoteName = m[1]
		remainder := m[2]
		if remainder == "" {
			return ref, fmt.Errorf("invalid remote spec %q", input)
		}
		ref.ExpectDirectory = remainder == "/" || strings.HasSuffix(remainder, "/")
		if ref.ExpectDirectory && remainder != "/" {
			remainder = strings.TrimSuffix(remainder, "/")
		}
		if strings.HasPrefix(remainder, "/") {
			if remainder == "" {
				remainder = "/"
			}
			ref.Path = remainder
			return ref, nil
		}
		slash := strings.IndexByte(remainder, '/')
		if slash < 0 {
			ref.Bucket = remainder
			return ref, nil
		}
		ref.Bucket = remainder[:slash]
		if slash+1 < len(remainder) {
			ref.Path = remainder[slash+1:]
		} else {
			ref.ExpectDirectory = true
		}
		return ref, nil
	}

	ref.Path = trimmed
	if trimmed != "/" && strings.HasSuffix(trimmed, "/") {
		ref.ExpectDirectory = true
		ref.Path = strings.TrimSuffix(trimmed, "/")
		if ref.Path == "" {
			ref.Path = "/"
		}
	}
	return ref, nil
}

func parseGroverdEndpointLocation(trimmed string) (RemoteRef, bool, error) {
	idx := strings.Index(trimmed, ":/")
	if idx < 0 {
		return RemoteRef{}, false, nil
	}
	controlEndpoint := strings.TrimSpace(trimmed[:idx])
	if controlEndpoint == "" {
		return RemoteRef{}, false, nil
	}
	if _, _, err := net.SplitHostPort(controlEndpoint); err != nil {
		if net.ParseIP(controlEndpoint) != nil || strings.HasPrefix(controlEndpoint, "[") || strings.Count(controlEndpoint, ":") >= 2 {
			return RemoteRef{}, true, fmt.Errorf("invalid groverd endpoint %q: expected host:port:/path", trimmed)
		}
		return RemoteRef{}, false, nil
	}
	pathPart := trimmed[idx+1:]
	if pathPart == "" {
		return RemoteRef{}, true, fmt.Errorf("invalid groverd endpoint path %q", trimmed)
	}
	ref := RemoteRef{
		isRemote:        true,
		RemoteName:      controlEndpoint,
		ControlEndpoint: controlEndpoint,
		Path:            pathPart,
		Raw:             trimmed,
		ExpectDirectory: pathPart == "/" || strings.HasSuffix(pathPart, "/"),
	}
	if ref.ExpectDirectory && pathPart != "/" {
		ref.Path = strings.TrimSuffix(pathPart, "/")
	}
	return ref, true, nil
}

func (opts CopyOptions) effectiveConcurrency() int {
	if opts.Concurrency <= 0 {
		return 1
	}
	return opts.Concurrency
}

func (opts CopyOptions) shouldUseRoutedJob(plan TransferRoutePlan) bool {
	return strings.TrimSpace(opts.RouteName) != "" ||
		strings.TrimSpace(plan.Source.ControlEndpoint) != "" ||
		strings.TrimSpace(plan.Destination.ControlEndpoint) != "" ||
		(!plan.Source.isRemote && !plan.Destination.isRemote) ||
		len(plan.Relays) > 0 ||
		strings.TrimSpace(opts.SourceServer) != "" ||
		strings.TrimSpace(opts.DestinationServer) != ""
}

func (opts CopyOptions) uiInterval() time.Duration {
	if opts.UIIntervalMs <= 0 {
		return 2 * time.Second
	}
	return time.Duration(opts.UIIntervalMs) * time.Millisecond
}

func (opts CopyOptions) uiMode() string {
	if opts.NoUI {
		return "none"
	}
	mode := strings.ToLower(strings.TrimSpace(opts.UIMode))
	if mode == "" {
		return "summary"
	}
	return mode
}

func (opts CopyOptions) validate() error {
	switch opts.uiMode() {
	case "summary", "live", "none":
	default:
		return fmt.Errorf("invalid --ui %q: must be summary, live, or none", opts.UIMode)
	}
	if opts.ParallelStreams < 0 {
		return fmt.Errorf("--parallel-streams must be >= 0")
	}
	if opts.UDPWindowPackets < 0 || opts.UDPWindowBytes < 0 || opts.UDPAckEveryPackets < 0 || opts.UDPAckEveryMs < 0 || opts.UDPBatchPackets < 0 {
		return fmt.Errorf("udp tuning values must be >= 0")
	}
	switch strings.ToLower(strings.TrimSpace(opts.UDPFlowControl)) {
	case "", "fixed", "bbr":
	default:
		return fmt.Errorf("invalid --udp-flow-control %q: must be fixed or bbr", opts.UDPFlowControl)
	}
	if mtu := strings.TrimSpace(opts.MTU); mtu != "" && !strings.EqualFold(mtu, "auto") {
		var parsed int
		if _, err := fmt.Sscanf(mtu, "%d", &parsed); err != nil || parsed <= 0 {
			return fmt.Errorf("invalid --mtu %q: must be auto or a positive integer", opts.MTU)
		}
	}
	return nil
}
