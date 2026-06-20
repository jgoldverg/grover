package cli

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"regexp"
	"strconv"
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
	RouteName          string
	RouteStore         string
	Protocol           string
	UIMode             string
	UIIntervalMs       int
	ParallelStreams    int
	ParallelismPerFile int
	ChunkSize          string
	Via                []string
	UDPFlowControl     string
	UDPWindowPackets   int
	UDPWindowBytes     int
	UDPAckEveryPackets int
	UDPAckEveryMs      int
	UDPBatchPackets    int
	MTU                string
	Paths              []string
	JobID              string
	SessionID          string
	Direction          string
	ConnectionOrigin   string
	DataDirection      string
}

const defaultTransferRoutePrefix = "transfer"

type transferRateSample struct {
	NowBps float64
	AvgBps float64
	Trend  string
	Valid  bool
}

type transferRateSampler struct {
	lastBytes uint64
	lastTime  time.Time
	lastBps   float64
}

func SimpleCopy() *cobra.Command {
	var opts CopyOptions
	cmd := &cobra.Command{
		Use:     "transfer <source> <destination>",
		Short:   "Simple grover udp based copy to and from grover server",
		Long:    "Simple grover udp based copy to and from grover server",
		Aliases: []string{"c", "cp"},
		Args: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.RouteName) != "" {
				if len(args) == 0 || len(args) == 2 {
					return nil
				}
				return fmt.Errorf("transfer with --route accepts either zero args or <source> <destination>")
			}
			return cobra.ExactArgs(2)(cmd, args)
		},
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := opts.validate(); err != nil {
				return err
			}
			var err error
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
				job, err := startRoutedTransfer(cmd, args[0], args[1], opts)
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
	cmd.Flags().StringVar(&opts.RouteName, "route", "", "Prepared route template name")
	cmd.Flags().StringVar(&opts.RouteStore, "route-store", "", "Path to local route template store")
	_ = cmd.Flags().MarkHidden("route-store")
	cmd.Flags().StringVar(&opts.Protocol, "protocol", "", "Transfer data-plane protocol (udp|tcp)")
	cmd.Flags().IntVar(&opts.ParallelismPerFile, "parallelism-per-file", 0, "Per-file parallel streams/ranges (0 uses config)")
	cmd.Flags().IntVar(&opts.ParallelStreams, "parallel-streams", 0, "Compatibility alias for --parallelism-per-file")
	_ = cmd.Flags().MarkHidden("parallel-streams")
	cmd.Flags().StringVar(&opts.ChunkSize, "chunk-size", "", "Read/write chunk size per worker, such as 128KiB, 8MiB, or 1048576")
	addRouteDirectionFlags(cmd, &opts.ConnectionOrigin, &opts.DataDirection)
	cmd.Flags().StringVar(&opts.Direction, "direction", "forward", "Transfer direction over the prepared route (forward|reverse)")
	cmd.Flags().StringVar(&opts.SessionID, "session-id", "", "Prepared route session ID")
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
	cmd.AddCommand(TransferHistoryCommand())
	return cmd
}

func startRoutedTransfer(cmd *cobra.Command, src string, dst string, opts CopyOptions) (*pb.TransferJob, error) {
	if strings.TrimSpace(opts.RouteName) != "" && strings.TrimSpace(opts.RouteStore) == "" {
		return startTransferOverPreparedRouteSession(cmd, src, dst, opts)
	}
	if strings.TrimSpace(opts.RouteStore) != "" {
		return nil, fmt.Errorf("local route-store execution is no longer supported; store the route on groverd, run route prepare, then transfer --route")
	}
	return nil, fmt.Errorf("transfer requires --route with a prepared route session; run route put, route prepare, then transfer --route")
}

func newTransferRouteName(now time.Time) string {
	return fmt.Sprintf("%s-%d", defaultTransferRoutePrefix, now.UnixNano())
}

func newTransferJobID(routeName string, now time.Time) string {
	routeName = strings.TrimSpace(routeName)
	if routeName == "" {
		routeName = newTransferRouteName(now)
	}
	return fmt.Sprintf("%s-%d", routeName, now.UnixNano())
}

func applyPreparedRouteTemplate(cmd *cobra.Command, args []string, opts *CopyOptions) ([]string, error) {
	if opts == nil {
		return args, nil
	}
	routeName := strings.TrimSpace(opts.RouteName)
	if routeName == "" {
		return args, nil
	}
	if strings.TrimSpace(opts.RouteStore) == "" {
		serverArgs, applied, err := applyServerRouteConfig(cmd, args, opts, routeName)
		if err != nil {
			return nil, err
		}
		if applied {
			return serverArgs, nil
		}
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
	if !commandFlagChanged(cmd, "connect-from", "connection-origin") && strings.TrimSpace(route.ConnectionOrigin) != "" {
		opts.ConnectionOrigin = route.ConnectionOrigin
	}
	if !commandFlagChanged(cmd, "flow", "data-direction") && strings.TrimSpace(route.DataDirection) != "" {
		opts.DataDirection = route.DataDirection
	}
	combinedVia := append([]string(nil), route.Via...)
	combinedVia = append(combinedVia, opts.Via...)
	opts.Via = combinedVia
	return args, nil
}

func applyServerRouteConfig(cmd *cobra.Command, args []string, opts *CopyOptions, routeName string) ([]string, bool, error) {
	routeClient, closeFn, err := openRouteConfigControl(cmd)
	if err != nil {
		return nil, false, err
	}
	defer closeFn()
	route, err := routeClient.GetRoute(cmd.Context(), routeName)
	if err != nil {
		return nil, false, err
	}
	if len(args) == 0 {
		return nil, true, fmt.Errorf("server route %q requires <source-path> <destination-path>", routeName)
	}
	if len(args) != 2 {
		return nil, true, fmt.Errorf("server route %q accepts <source-path> <destination-path>", routeName)
	}
	sourceControl := route.GetSource()
	destinationControl := route.GetDestination()
	direction, err := normalizeTransferDirection(opts.Direction)
	if err != nil {
		return nil, true, err
	}
	if direction == "reverse" {
		sourceControl = route.GetDestination()
		destinationControl = route.GetSource()
	}
	source, err := routeEndpointLocation(sourceControl, args[0], "source")
	if err != nil {
		return nil, true, err
	}
	destination, err := routeEndpointLocation(destinationControl, args[1], "destination")
	if err != nil {
		return nil, true, err
	}
	if !cmd.Flags().Changed("protocol") {
		opts.Protocol = routeProtocolLabel(route.GetProtocol())
	}
	if !commandFlagChanged(cmd, "connect-from", "connection-origin") {
		opts.ConnectionOrigin = routeConnectionOriginLabel(route.GetConnectionOrigin())
	}
	if !commandFlagChanged(cmd, "flow", "data-direction") {
		opts.DataDirection = routeDataDirectionLabel(route.GetDataDirection())
	}
	combinedVia := append([]string(nil), route.GetVia()...)
	combinedVia = append(combinedVia, opts.Via...)
	opts.Via = combinedVia
	return []string{source, destination}, true, nil
}

func routeEndpointLocation(controlEndpoint, pathValue, role string) (string, error) {
	controlEndpoint = strings.TrimSpace(controlEndpoint)
	if controlEndpoint == "" {
		return "", fmt.Errorf("route %s endpoint is empty", role)
	}
	pathValue = strings.TrimSpace(pathValue)
	if pathValue == "" {
		return "", fmt.Errorf("%s path is required", role)
	}
	if ref, err := parseLocation(pathValue); err == nil && ref.isRemote && strings.TrimSpace(ref.ControlEndpoint) != "" {
		return pathValue, nil
	}
	if !filepath.IsAbs(pathValue) {
		return "", fmt.Errorf("%s path %q must be absolute when using a server route", role, pathValue)
	}
	return controlEndpoint + ":" + pathValue, nil
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
	var parallelismPerFile int
	var parallelStreams int
	var chunkSize string
	var tcpBuffer string
	var udpMTU string
	var udpWindowPackets int
	var udpBatchPackets int
	var udpAckEveryPackets int
	var udpAckEveryMs int
	var udpSocketReadBuffer string
	var udpSocketWriteBuffer string
	var udpFlowControl string
	var sourceServer string
	cmd := &cobra.Command{
		Use:          "tune <transfer_id>",
		Short:        "Update runtime transfer tuning",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			req, err := buildTransferTuningRequest(args[0], transferTuningFlags{
				concurrency:              concurrency,
				parallelismPerFile:       parallelismPerFile,
				parallelStreams:          parallelStreams,
				chunkSize:                chunkSize,
				tcpBuffer:                tcpBuffer,
				udpMTU:                   udpMTU,
				udpWindowPackets:         udpWindowPackets,
				udpBatchPackets:          udpBatchPackets,
				udpAckEveryPackets:       udpAckEveryPackets,
				udpAckEveryMs:            udpAckEveryMs,
				udpSocketReadBuffer:      udpSocketReadBuffer,
				udpSocketWriteBuffer:     udpSocketWriteBuffer,
				udpFlowControl:           udpFlowControl,
				parallelismFlagPreferred: cmd.Flags().Changed("parallelism-per-file"),
			})
			if err != nil {
				return err
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
			job, err := routed.UpdateTransferTuning(cmd.Context(), req)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "transfer_job: %s\n", job.GetJobId())
			fmt.Fprintf(cmd.OutOrStdout(), "state: %s\n", job.GetState().String())
			fmt.Fprintf(cmd.OutOrStdout(), "concurrency: %d\n", job.GetConcurrency())
			fmt.Fprintf(cmd.OutOrStdout(), "parallelism_per_file: %d\n", job.GetParallelismPerFile())
			fmt.Fprintf(cmd.OutOrStdout(), "chunk_size: %s\n", formatBytes(job.GetChunkSizeBytes()))
			return nil
		},
	}
	cmd.Flags().IntVar(&concurrency, "concurrency", 0, "Maximum number of files to transfer in parallel")
	cmd.Flags().IntVar(&parallelismPerFile, "parallelism-per-file", 0, "Per-file parallel streams/ranges")
	cmd.Flags().IntVar(&parallelStreams, "parallel-streams", 0, "Compatibility alias for --parallelism-per-file")
	_ = cmd.Flags().MarkHidden("parallel-streams")
	cmd.Flags().StringVar(&chunkSize, "chunk-size", "", "Read/write chunk size per worker, such as 128KiB or 8MiB")
	cmd.Flags().StringVar(&tcpBuffer, "tcp-buffer", "", "TCP copy buffer size, such as 1MiB")
	cmd.Flags().StringVar(&udpMTU, "udp-mtu", "", "UDP payload size in bytes")
	cmd.Flags().IntVar(&udpWindowPackets, "udp-window-packets", 0, "UDP max in-flight packets per stream")
	cmd.Flags().IntVar(&udpBatchPackets, "udp-batch-packets", 0, "UDP packets per kernel batch call")
	cmd.Flags().IntVar(&udpAckEveryPackets, "udp-ack-every-packets", 0, "UDP ACK every N packets")
	cmd.Flags().IntVar(&udpAckEveryMs, "udp-ack-every-ms", 0, "UDP ACK interval in milliseconds")
	cmd.Flags().StringVar(&udpSocketReadBuffer, "udp-socket-read-buffer", "", "UDP socket read buffer size")
	cmd.Flags().StringVar(&udpSocketWriteBuffer, "udp-socket-write-buffer", "", "UDP socket write buffer size")
	cmd.Flags().StringVar(&udpFlowControl, "udp-flow-control", "", "UDP flow control mode (fixed|bbr)")
	cmd.Flags().StringVar(&sourceServer, "source-server", "", "Control address of the source groverd that owns the transfer job")
	return cmd
}

type transferTuningFlags struct {
	concurrency              int
	parallelismPerFile       int
	parallelStreams          int
	chunkSize                string
	tcpBuffer                string
	udpMTU                   string
	udpWindowPackets         int
	udpBatchPackets          int
	udpAckEveryPackets       int
	udpAckEveryMs            int
	udpSocketReadBuffer      string
	udpSocketWriteBuffer     string
	udpFlowControl           string
	parallelismFlagPreferred bool
}

func buildTransferTuningRequest(jobID string, flags transferTuningFlags) (*pb.UpdateTransferTuningRequest, error) {
	req := &pb.UpdateTransferTuningRequest{JobId: strings.TrimSpace(jobID)}
	if req.JobId == "" {
		return nil, fmt.Errorf("transfer_id is required")
	}
	if flags.concurrency < 0 {
		return nil, fmt.Errorf("--concurrency must be >= 0")
	}
	if flags.parallelismPerFile < 0 {
		return nil, fmt.Errorf("--parallelism-per-file must be >= 0")
	}
	if flags.parallelStreams < 0 {
		return nil, fmt.Errorf("--parallel-streams must be >= 0")
	}
	if flags.udpWindowPackets < 0 || flags.udpBatchPackets < 0 || flags.udpAckEveryPackets < 0 || flags.udpAckEveryMs < 0 {
		return nil, fmt.Errorf("udp numeric tuning values must be >= 0")
	}
	req.Concurrency = uint32(flags.concurrency)
	if flags.parallelismFlagPreferred || flags.parallelismPerFile > 0 {
		req.ParallelismPerFile = uint32(flags.parallelismPerFile)
	} else {
		req.ParallelismPerFile = uint32(flags.parallelStreams)
	}
	var err error
	if req.ChunkSizeBytes, err = parseByteSize(flags.chunkSize); err != nil {
		return nil, fmt.Errorf("--chunk-size: %w", err)
	}
	if req.TcpBufferBytes, err = parseByteSize(flags.tcpBuffer); err != nil {
		return nil, fmt.Errorf("--tcp-buffer: %w", err)
	}
	var udpMTU uint64
	if udpMTU, err = parseByteSize(flags.udpMTU); err != nil {
		return nil, fmt.Errorf("--udp-mtu: %w", err)
	}
	if udpMTU > 0 {
		if udpMTU > uint64(^uint32(0)) {
			return nil, fmt.Errorf("--udp-mtu is too large")
		}
		req.UdpMtuBytes = uint32(udpMTU)
	}
	if req.UdpSocketReadBufferBytes, err = parseByteSize(flags.udpSocketReadBuffer); err != nil {
		return nil, fmt.Errorf("--udp-socket-read-buffer: %w", err)
	}
	if req.UdpSocketWriteBufferBytes, err = parseByteSize(flags.udpSocketWriteBuffer); err != nil {
		return nil, fmt.Errorf("--udp-socket-write-buffer: %w", err)
	}
	req.UdpWindowPackets = uint32(flags.udpWindowPackets)
	req.UdpBatchPackets = uint32(flags.udpBatchPackets)
	req.UdpAckEveryPackets = uint32(flags.udpAckEveryPackets)
	req.UdpAckEveryMs = uint32(flags.udpAckEveryMs)
	req.UdpFlowControl = strings.TrimSpace(flags.udpFlowControl)
	if req.GetConcurrency() == 0 &&
		req.GetParallelismPerFile() == 0 &&
		req.GetChunkSizeBytes() == 0 &&
		req.GetTcpBufferBytes() == 0 &&
		req.GetUdpMtuBytes() == 0 &&
		req.GetUdpWindowPackets() == 0 &&
		req.GetUdpBatchPackets() == 0 &&
		req.GetUdpAckEveryPackets() == 0 &&
		req.GetUdpAckEveryMs() == 0 &&
		req.GetUdpSocketReadBufferBytes() == 0 &&
		req.GetUdpSocketWriteBufferBytes() == 0 &&
		strings.TrimSpace(req.GetUdpFlowControl()) == "" {
		return nil, fmt.Errorf("set at least one tuning flag")
	}
	return req, nil
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
				printTransferJobStatus(cmd.OutOrStdout(), job, transferRateSample{})
				return nil
			}
			stream, err := routed.StreamTransferStats(cmd.Context(), args[0], "")
			if err != nil {
				return err
			}
			sampler := &transferRateSampler{}
			for {
				job, err := stream.Recv()
				if err != nil {
					return err
				}
				printTransferJobStatus(cmd.OutOrStdout(), job, sampler.Observe(job, time.Now()))
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

func (s *transferRateSampler) Observe(job *pb.TransferJob, now time.Time) transferRateSample {
	if job == nil {
		return transferRateSample{}
	}
	bytes := job.GetGoodBytes()
	avg := job.GetStats().GetAverageThroughputBps()
	if s.lastTime.IsZero() {
		s.lastBytes = bytes
		s.lastTime = now
		s.lastBps = 0
		return transferRateSample{AvgBps: avg}
	}
	elapsed := now.Sub(s.lastTime).Seconds()
	if elapsed <= 0 {
		return transferRateSample{AvgBps: avg}
	}
	delta := uint64(0)
	if bytes >= s.lastBytes {
		delta = bytes - s.lastBytes
	}
	current := float64(delta) / elapsed
	trend := "flat"
	if s.lastBps > 0 {
		switch {
		case current > s.lastBps*1.10:
			trend = "up"
		case current < s.lastBps*0.90:
			trend = "down"
		}
	}
	s.lastBytes = bytes
	s.lastTime = now
	s.lastBps = current
	return transferRateSample{NowBps: current, AvgBps: avg, Trend: trend, Valid: true}
}

func printTransferJobStatus(w io.Writer, job *pb.TransferJob, sample transferRateSample) {
	if w == nil || job == nil {
		return
	}
	printTransferRouteVisualization(w, job, sample)
}

func printTransferRouteVisualization(w io.Writer, job *pb.TransferJob, sample transferRateSample) {
	stats := job.GetStats()
	totalBytes := transferTotalBytes(job)
	progressPercent := percentComplete(job.GetGoodBytes(), totalBytes)
	nowRate := stats.GetCurrentThroughputBps()
	avgRate := stats.GetAverageThroughputBps()
	trend := "warmup"
	if sample.Valid {
		nowRate = sample.NowBps
		avgRate = sample.AvgBps
		trend = sample.Trend
	}

	fmt.Fprintf(w, "Transfer %s\n", job.GetJobId())
	fmt.Fprintf(w, "  State:       %-10s Route: %s\n", shortRuntimeState(job.GetState()), emptyDash(job.GetRouteId()))
	if job.GetErrorMessage() != "" {
		fmt.Fprintf(w, "  Error: %s\n", job.GetErrorMessage())
	}
	fmt.Fprintf(w, "  Transferred: %s / %s, %s, now %s, avg %s, ETA %s, trend %s\n",
		formatBytes(job.GetGoodBytes()),
		formatTotalBytes(totalBytes),
		formatPercent(progressPercent),
		formatByteRate(nowRate),
		formatByteRate(avgRate),
		formatETA(remainingBytes(job.GetGoodBytes(), totalBytes), nowRate),
		trend,
	)
	fmt.Fprintf(w, "  Files:       %d done / %d total, %d active, %d streams\n",
		job.GetFilesDone(),
		len(job.GetFiles()),
		job.GetFilesActive(),
		job.GetStreamsActive(),
	)
	printActiveTransferFiles(w, job, nowRate)
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "Grover network\n")
	fmt.Fprintf(w, "  Path:        %s -> %s -> %s\n", endpointSummary(job.GetSource()), shortDataProtocol(job.GetProtocol()), endpointSummary(job.GetDestination()))
	fmt.Fprintf(w, "  Health:      rtt=%s packets=%d retransmits=%d drops=%d errors=%d efficiency=%s\n",
		formatLatency(stats.GetLatencyMs()),
		stats.GetPackets(),
		job.GetRetransmits(),
		stats.GetDrops(),
		stats.GetErrors(),
		formatEfficiency(job.GetGoodBytes(), job.GetNetworkBytes()),
	)
	fmt.Fprintf(w, "  Bytes:       good=%s wire=%s disk_read=%s\n",
		formatBytes(job.GetGoodBytes()),
		formatBytes(job.GetNetworkBytes()),
		formatBytes(job.GetDiskReadBytes()),
	)
	if job.GetDestination().GetDataEndpoint() == nil {
		fmt.Fprintf(w, "  Destination: disk_write=%s\n", formatBytes(job.GetDiskWriteBytes()))
	} else {
		fmt.Fprintf(w, "  Destination: expected=%s metrics=pending\n", formatBytes(job.GetGoodBytes()))
	}
	fmt.Fprintln(w)
}

func transferTotalBytes(job *pb.TransferJob) uint64 {
	var total uint64
	for _, file := range job.GetFiles() {
		total += file.GetSize()
	}
	return total
}

func remainingBytes(done, total uint64) uint64 {
	if total == 0 || done >= total {
		return 0
	}
	return total - done
}

func percentComplete(done, total uint64) float64 {
	if total == 0 {
		return 0
	}
	if done >= total {
		return 100
	}
	return float64(done) * 100 / float64(total)
}

func printActiveTransferFiles(w io.Writer, job *pb.TransferJob, nowRate float64) {
	active := 0
	for _, file := range job.GetFiles() {
		if file.GetState() != pb.RuntimeState_RUNTIME_STATE_RUNNING {
			continue
		}
		active++
		if active == 1 {
			fmt.Fprintln(w, "  Transferring:")
		}
		fmt.Fprintf(w, "   * %s: %s / %s, %s, ETA %s\n",
			fileDisplayPath(file.GetRelativePath(), file.GetPath()),
			formatBytes(file.GetBytesDone()),
			formatTotalBytes(file.GetSize()),
			formatPercent(percentComplete(file.GetBytesDone(), file.GetSize())),
			formatETA(remainingBytes(file.GetBytesDone(), file.GetSize()), nowRate),
		)
		printActiveTransferStreams(w, file)
		if active >= 3 {
			break
		}
	}
}

func printActiveTransferStreams(w io.Writer, file *pb.TransferFileState) {
	printed := 0
	for _, stream := range file.GetStreams() {
		if stream.GetState() != pb.RuntimeState_RUNTIME_STATE_RUNNING {
			continue
		}
		fmt.Fprintf(w, "     - stream %d: %s / %s, now %s, avg %s, offset %s\n",
			stream.GetStreamId(),
			formatBytes(stream.GetBytesDone()),
			formatTotalBytes(stream.GetSize()),
			formatByteRate(stream.GetCurrentThroughputBps()),
			formatByteRate(stream.GetAverageThroughputBps()),
			formatBytes(stream.GetOffset()),
		)
		printed++
		if printed >= 4 {
			if remaining := countActiveTransferStreams(file) - printed; remaining > 0 {
				fmt.Fprintf(w, "     - ... %d more active streams\n", remaining)
			}
			return
		}
	}
}

func countActiveTransferStreams(file *pb.TransferFileState) int {
	count := 0
	for _, stream := range file.GetStreams() {
		if stream.GetState() == pb.RuntimeState_RUNTIME_STATE_RUNNING {
			count++
		}
	}
	return count
}

func fileDisplayPath(relativePath, fullPath string) string {
	if strings.TrimSpace(relativePath) != "" {
		return relativePath
	}
	if strings.TrimSpace(fullPath) != "" {
		return fullPath
	}
	return "unknown"
}

func endpointSummary(ep *pb.TransferEndpoint) string {
	if ep == nil {
		return "unknown"
	}
	data := ep.GetDataEndpoint()
	control := ""
	if data != nil && strings.TrimSpace(data.GetHost()) != "" && data.GetPort() != 0 {
		control = net.JoinHostPort(data.GetHost(), fmt.Sprintf("%d", data.GetPort()))
	}
	root := strings.TrimSpace(ep.GetRootPath())
	switch {
	case control != "" && root != "":
		return fmt.Sprintf("%s root=%s", control, root)
	case control != "":
		return control
	case root != "":
		return fmt.Sprintf("root=%s", root)
	default:
		return "unknown"
	}
}

func dataEndpointSummary(ep *pb.TransferEndpoint) string {
	if ep == nil || ep.GetDataEndpoint() == nil {
		return "local"
	}
	data := ep.GetDataEndpoint()
	if strings.TrimSpace(data.GetHost()) == "" || data.GetPort() == 0 {
		return "local"
	}
	return net.JoinHostPort(data.GetHost(), fmt.Sprintf("%d", data.GetPort()))
}

func shortRuntimeState(state pb.RuntimeState) string {
	s := strings.TrimPrefix(state.String(), "RUNTIME_STATE_")
	if s == "" || s == "UNSPECIFIED" {
		return "UNKNOWN"
	}
	return s
}

func shortDataProtocol(protocol pb.DataProtocol) string {
	s := strings.TrimPrefix(protocol.String(), "DATA_PROTOCOL_")
	if s == "" || s == "UNSPECIFIED" {
		return "UNKNOWN"
	}
	return s
}

func emptyDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

func formatBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value := float64(n)
	for _, suffix := range []string{"KiB", "MiB", "GiB", "TiB", "PiB"} {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.2f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.2f EiB", value/unit)
}

func formatTotalBytes(n uint64) string {
	if n == 0 {
		return "?"
	}
	return formatBytes(n)
}

func formatByteRate(bytesPerSecond float64) string {
	if bytesPerSecond <= 0 {
		return "0 B/s"
	}
	return fmt.Sprintf("%s/s", formatBytes(uint64(bytesPerSecond)))
}

func parseByteSize(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	lower := strings.ToLower(raw)
	multipliers := []struct {
		suffix string
		value  uint64
	}{
		{"kib", 1024},
		{"kb", 1000},
		{"k", 1024},
		{"mib", 1024 * 1024},
		{"mb", 1000 * 1000},
		{"m", 1024 * 1024},
		{"gib", 1024 * 1024 * 1024},
		{"gb", 1000 * 1000 * 1000},
		{"g", 1024 * 1024 * 1024},
		{"b", 1},
	}
	multiplier := uint64(1)
	number := raw
	for _, candidate := range multipliers {
		if strings.HasSuffix(lower, candidate.suffix) {
			multiplier = candidate.value
			number = strings.TrimSpace(raw[:len(raw)-len(candidate.suffix)])
			break
		}
	}
	if number == "" {
		return 0, fmt.Errorf("missing numeric value in %q", raw)
	}
	value, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, fmt.Errorf("must be >= 0")
	}
	return uint64(value * float64(multiplier)), nil
}

func mustParseOptionalByteSize(raw string) uint64 {
	value, err := parseByteSize(raw)
	if err != nil {
		return 0
	}
	return value
}

func formatPercent(v float64) string {
	if v <= 0 {
		return "0%"
	}
	if v >= 100 {
		return "100%"
	}
	return fmt.Sprintf("%.1f%%", v)
}

func formatETA(remaining uint64, bytesPerSecond float64) string {
	if remaining == 0 {
		return "-"
	}
	if bytesPerSecond <= 0 {
		return "?"
	}
	return formatDuration(time.Duration(float64(time.Second) * (float64(remaining) / bytesPerSecond)))
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return d.Truncate(100 * time.Millisecond).String()
	}
	return d.Truncate(time.Second).String()
}

func formatLatency(ms float64) string {
	if ms <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2fms", ms)
}

func formatEfficiency(goodBytes, networkBytes uint64) string {
	if goodBytes == 0 || networkBytes == 0 {
		return "-"
	}
	eff := float64(goodBytes) * 100 / float64(networkBytes)
	if eff > 100 {
		eff = 100
	}
	return fmt.Sprintf("%.2f%%", eff)
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

func (opts CopyOptions) effectiveParallelismPerFile() int {
	if opts.ParallelismPerFile > 0 {
		return opts.ParallelismPerFile
	}
	if opts.ParallelStreams > 0 {
		return opts.ParallelStreams
	}
	return 1
}

func (opts CopyOptions) shouldUseRoutedJob(plan TransferRoutePlan) bool {
	return strings.TrimSpace(opts.RouteName) != "" ||
		strings.TrimSpace(plan.Source.ControlEndpoint) != "" ||
		strings.TrimSpace(plan.Destination.ControlEndpoint) != "" ||
		(!plan.Source.isRemote && !plan.Destination.isRemote) ||
		len(plan.Relays) > 0
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
	if opts.ParallelismPerFile < 0 {
		return fmt.Errorf("--parallelism-per-file must be >= 0")
	}
	if opts.ParallelStreams < 0 {
		return fmt.Errorf("--parallel-streams must be >= 0")
	}
	if strings.TrimSpace(opts.ChunkSize) != "" {
		if _, err := parseByteSize(opts.ChunkSize); err != nil {
			return fmt.Errorf("invalid --chunk-size: %w", err)
		}
	}
	if opts.UDPWindowPackets < 0 || opts.UDPWindowBytes < 0 || opts.UDPAckEveryPackets < 0 || opts.UDPAckEveryMs < 0 || opts.UDPBatchPackets < 0 {
		return fmt.Errorf("udp tuning values must be >= 0")
	}
	switch strings.ToLower(strings.TrimSpace(opts.UDPFlowControl)) {
	case "", "fixed", "bbr":
	default:
		return fmt.Errorf("invalid --udp-flow-control %q: must be fixed or bbr", opts.UDPFlowControl)
	}
	if _, err := normalizeConnectionOrigin(opts.ConnectionOrigin); err != nil {
		return err
	}
	if _, err := normalizeDataDirection(opts.DataDirection); err != nil {
		return err
	}
	if _, err := normalizeTransferDirection(opts.Direction); err != nil {
		return err
	}
	if mtu := strings.TrimSpace(opts.MTU); mtu != "" && !strings.EqualFold(mtu, "auto") {
		var parsed int
		if _, err := fmt.Sscanf(mtu, "%d", &parsed); err != nil || parsed <= 0 {
			return fmt.Errorf("invalid --mtu %q: must be auto or a positive integer", opts.MTU)
		}
	}
	return nil
}
