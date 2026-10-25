package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jgoldverg/grover/pkg/gclient"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/util"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func JobCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "job",
		Short: "Inspect and tune live and historical transfer jobs",
	}
	cmd.AddCommand(JobListCommand())
	cmd.AddCommand(JobGetCommand())
	cmd.AddCommand(JobMonitorCommand())
	cmd.AddCommand(JobTuneCommand())
	cmd.AddCommand(JobHistoryCommand())
	return cmd
}

func JobListCommand() *cobra.Command {
	var routeID string
	var jsonOut bool
	cmd := &cobra.Command{
		Use:          "list",
		Short:        "List live transfer jobs on the selected groverd",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, closeFn, err := openJobClient(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			jobs, err := client.RoutedTransfer().ListTransferJobs(cmd.Context(), routeID)
			if err != nil {
				return err
			}
			if jsonOut {
				return writeProtoJSON(cmd.OutOrStdout(), &pb.ListTransferJobsResponse{Jobs: jobs})
			}
			printJobTable(cmd.OutOrStdout(), jobs)
			return nil
		},
	}
	cmd.Flags().StringVar(&routeID, "route", "", "Filter live jobs by route ID")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Print JSON")
	return cmd
}

func JobGetCommand() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:          "get <job_id>",
		Short:        "Show a live transfer job",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, closeFn, err := openJobClient(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			job, err := client.RoutedTransfer().GetTransferJob(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if jsonOut {
				return writeProtoJSON(cmd.OutOrStdout(), job)
			}
			printTransferJobStatus(cmd.OutOrStdout(), job, transferRateSample{})
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Print JSON")
	return cmd
}

func JobMonitorCommand() *cobra.Command {
	var routeID string
	cmd := &cobra.Command{
		Use:          "monitor <job_id>",
		Short:        "Watch a live transfer job until it exits",
		Aliases:      []string{"watch"},
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, closeFn, err := openJobClient(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			stream, err := client.RoutedTransfer().StreamTransferStats(cmd.Context(), args[0], routeID)
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
	cmd.Flags().StringVar(&routeID, "route", "", "Optional route ID hint for the stats stream")
	return cmd
}

func JobTuneCommand() *cobra.Command {
	var flags transferTuningFlags
	var jsonOut bool
	cmd := &cobra.Command{
		Use:          "tune <job_id>",
		Short:        "Update runtime transfer tuning",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags.parallelismFlagPreferred = cmd.Flags().Changed("parallelism-per-file")
			req, err := buildTransferTuningRequest(args[0], flags)
			if err != nil {
				return err
			}
			client, closeFn, err := openJobClient(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			job, err := client.RoutedTransfer().UpdateTransferTuning(cmd.Context(), req)
			if err != nil {
				return err
			}
			if jsonOut {
				return writeProtoJSON(cmd.OutOrStdout(), job)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "job: %s\n", job.GetJobId())
			fmt.Fprintf(cmd.OutOrStdout(), "state: %s\n", shortRuntimeState(job.GetState()))
			fmt.Fprintf(cmd.OutOrStdout(), "concurrency: %d\n", job.GetConcurrency())
			fmt.Fprintf(cmd.OutOrStdout(), "parallelism_per_file: %d\n", job.GetParallelismPerFile())
			fmt.Fprintf(cmd.OutOrStdout(), "chunk_size: %s\n", formatBytes(job.GetChunkSizeBytes()))
			return nil
		},
	}
	addJobTuningFlags(cmd, &flags)
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Print JSON")
	return cmd
}

func JobHistoryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "history",
		Short: "Query historical job logs from groverd",
	}
	cmd.AddCommand(JobHistoryListCommand())
	cmd.AddCommand(JobHistoryGetCommand())
	cmd.AddCommand(JobHistorySnapshotsCommand())
	cmd.AddCommand(JobHistoryEnergyCommand())
	return cmd
}

func JobHistoryListCommand() *cobra.Command {
	var routeID string
	var limit uint32
	var jsonOut bool
	cmd := &cobra.Command{
		Use:          "list",
		Short:        "List historical jobs from groverd job logs",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, closeFn, err := openJobClient(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			jobs, err := client.JobHistoryControl().ListJobHistory(cmd.Context(), &pb.ListJobHistoryRequest{RouteId: routeID, Limit: limit})
			if err != nil {
				return err
			}
			if jsonOut {
				return writeProtoJSON(cmd.OutOrStdout(), &pb.ListJobHistoryResponse{Jobs: jobs})
			}
			printHistoryJobTable(cmd.OutOrStdout(), jobs)
			return nil
		},
	}
	cmd.Flags().StringVar(&routeID, "route", "", "Filter historical jobs by route ID")
	cmd.Flags().Uint32Var(&limit, "limit", 25, "Maximum jobs to show")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Print JSON")
	return cmd
}

func JobHistoryGetCommand() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:          "get <job_id>",
		Short:        "Show a historical final job snapshot",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, closeFn, err := openJobClient(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			job, err := client.JobHistoryControl().GetJobFinal(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if jsonOut {
				return writeProtoJSON(cmd.OutOrStdout(), job)
			}
			printTransferJobStatus(cmd.OutOrStdout(), job, transferRateSample{})
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Print JSON")
	return cmd
}

func JobHistorySnapshotsCommand() *cobra.Command {
	var limit uint32
	var jsonOut bool
	cmd := &cobra.Command{
		Use:          "snapshots <job_id>",
		Short:        "Show historical job progress snapshots",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, closeFn, err := openJobClient(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			snapshots, err := client.JobHistoryControl().ListJobSnapshots(cmd.Context(), &pb.ListJobSnapshotsRequest{JobId: args[0], Limit: limit})
			if err != nil {
				return err
			}
			if jsonOut {
				return writeProtoJSON(cmd.OutOrStdout(), &pb.ListJobSnapshotsResponse{Snapshots: snapshots})
			}
			printSnapshotTable(cmd.OutOrStdout(), snapshots)
			return nil
		},
	}
	cmd.Flags().Uint32Var(&limit, "limit", 20, "Maximum snapshots to show")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Print JSON")
	return cmd
}

func JobHistoryEnergyCommand() *cobra.Command {
	var limit uint32
	cmd := &cobra.Command{
		Use:          "energy <job_id>",
		Short:        "Print historical per-job energy CSV rows",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, closeFn, err := openJobClient(cmd)
			if err != nil {
				return err
			}
			defer closeFn()
			resp, err := client.JobHistoryControl().ListJobEnergy(cmd.Context(), &pb.ListJobEnergyRequest{JobId: args[0], Limit: limit})
			if err != nil {
				return err
			}
			if strings.TrimSpace(resp.GetHeader()) != "" {
				fmt.Fprintln(cmd.OutOrStdout(), resp.GetHeader())
			}
			for _, record := range resp.GetRecords() {
				fmt.Fprintln(cmd.OutOrStdout(), record.GetCsv())
			}
			return nil
		},
	}
	cmd.Flags().Uint32Var(&limit, "limit", 0, "Maximum energy rows to show (0 means all)")
	return cmd
}

func addJobTuningFlags(cmd *cobra.Command, flags *transferTuningFlags) {
	cmd.Flags().IntVar(&flags.concurrency, "concurrency", 0, "Maximum number of files to transfer in parallel")
	cmd.Flags().IntVar(&flags.parallelismPerFile, "parallelism-per-file", 0, "Per-file parallel streams/ranges")
	cmd.Flags().IntVar(&flags.parallelStreams, "parallel-streams", 0, "Compatibility alias for --parallelism-per-file")
	_ = cmd.Flags().MarkHidden("parallel-streams")
	cmd.Flags().StringVar(&flags.chunkSize, "chunk-size", "", "Read/write chunk size per worker, such as 128KiB or 8MiB")
	cmd.Flags().StringVar(&flags.tcpBuffer, "tcp-buffer", "", "TCP copy buffer size, such as 1MiB")
	cmd.Flags().StringVar(&flags.udpMTU, "udp-mtu", "", "UDP payload size in bytes")
	cmd.Flags().IntVar(&flags.udpWindowPackets, "udp-window-packets", 0, "UDP max in-flight packets per stream")
	cmd.Flags().IntVar(&flags.udpBatchPackets, "udp-batch-packets", 0, "UDP packets per kernel batch call")
	cmd.Flags().IntVar(&flags.udpAckEveryPackets, "udp-ack-every-packets", 0, "UDP ACK every N packets")
	cmd.Flags().IntVar(&flags.udpAckEveryMs, "udp-ack-every-ms", 0, "UDP ACK interval in milliseconds")
	cmd.Flags().StringVar(&flags.udpSocketReadBuffer, "udp-socket-read-buffer", "", "UDP socket read buffer size")
	cmd.Flags().StringVar(&flags.udpSocketWriteBuffer, "udp-socket-write-buffer", "", "UDP socket write buffer size")
	cmd.Flags().StringVar(&flags.udpFlowControl, "udp-flow-control", "", "UDP flow control mode (fixed|bbr)")
}

func openJobClient(cmd *cobra.Command) (*gclient.Client, func(), error) {
	cfg := GetAppConfig(cmd)
	if cfg == nil {
		return nil, nil, fmt.Errorf("app config unavailable")
	}
	client := gclient.NewClient(*cfg)
	if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
		return nil, nil, err
	}
	if client.RoutedTransfer() == nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("routed transfer service unavailable")
	}
	return client, func() { _ = client.Close() }, nil
}

func printJobTable(w io.Writer, jobs []*pb.TransferJob) {
	if len(jobs) == 0 {
		fmt.Fprintln(w, "no live jobs")
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB\tROUTE\tSTATE\tDONE\tBYTES\tRATE\tCONC\tPARALLEL")
	for _, job := range jobs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d/%d\t%s\t%s\t%d\t%d\n",
			job.GetJobId(),
			emptyDash(job.GetRouteId()),
			shortRuntimeState(job.GetState()),
			job.GetFilesDone(),
			len(job.GetFiles()),
			formatBytes(job.GetGoodBytes()),
			formatByteRate(job.GetStats().GetCurrentThroughputBps()),
			job.GetConcurrency(),
			job.GetParallelismPerFile(),
		)
	}
	_ = tw.Flush()
}

func printHistoryJobTable(w io.Writer, jobs []*pb.JobHistoryEntry) {
	if len(jobs) == 0 {
		fmt.Fprintln(w, "no historical jobs")
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB\tROUTE\tSTATE\tGOOD\tWIRE\tTHROUGHPUT\tENERGY\tCREATED\tPATH")
	for _, job := range jobs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			job.GetJobId(),
			emptyDash(job.GetRouteId()),
			emptyDash(strings.TrimPrefix(job.GetState(), "RUNTIME_STATE_")),
			formatBytes(job.GetGoodBytes()),
			formatBytes(job.GetNetworkBytes()),
			formatMbps(job.GetThroughputMbps()),
			formatJoules(job.GetEnergyJoules()),
			formatUnixNano(job.GetCreatedAtUnixNano()),
			job.GetPath(),
		)
	}
	_ = tw.Flush()
}

func printSnapshotTable(w io.Writer, snapshots []*pb.TransferJob) {
	if len(snapshots) == 0 {
		fmt.Fprintln(w, "no snapshots")
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TIME\tSTATE\tGOOD\tWIRE\tRATE\tFILES\tSTREAMS")
	for _, job := range snapshots {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d/%d\t%d\n",
			formatUnixNano(job.GetStats().GetSampledAtUnixNano()),
			shortRuntimeState(job.GetState()),
			formatBytes(job.GetGoodBytes()),
			formatBytes(job.GetNetworkBytes()),
			formatByteRate(job.GetStats().GetCurrentThroughputBps()),
			job.GetFilesDone(),
			len(job.GetFiles()),
			job.GetStreamsActive(),
		)
	}
	_ = tw.Flush()
}

func writeProtoJSON(w io.Writer, msg proto.Message) error {
	payload, err := protojson.MarshalOptions{Multiline: true, EmitUnpopulated: true}.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(payload))
	return err
}

func formatUnixNano(ns int64) string {
	if ns <= 0 {
		return "-"
	}
	return time.Unix(0, ns).Format(time.RFC3339)
}

func formatMbps(value float64) string {
	if value <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f Mbps", value)
}

func formatJoules(value float64) string {
	if value <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.3f J", value)
}
