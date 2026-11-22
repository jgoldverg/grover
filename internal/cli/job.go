package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/jgoldverg/grover/internal/client"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/tuning"
)

// nodeFlags selects which node a job command talks to.
type nodeFlags struct{ node, route string }

func (n *nodeFlags) register(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&n.node, "node", "n", "", "node to query (profile or host:port; default node if empty)")
	cmd.Flags().StringVarP(&n.route, "route", "r", "", "query the source node of this route")
}

func (n *nodeFlags) conn(ctx context.Context, a *app) (*client.Conn, error) {
	if n.route != "" {
		if n.node != "" {
			return nil, errorf("use --node or --route, not both")
		}
		r, err := a.route(ctx, n.route)
		if err != nil {
			return nil, err
		}
		return a.conn(ctx, r.Source)
	}
	return a.conn(ctx, n.node)
}

func jobCommand(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "job",
		Short: "Inspect, watch, tune, and abort jobs; read job history",
		Long: `Jobs live on the node that sends the data (a route's source node). The
destination node also answers "job get" for the same id with its receive-side view.`,
	}
	cmd.AddCommand(jobList(a), jobGet(a), jobWatch(a), jobTune(a), jobAbort(a), historyCommand(a))
	return cmd
}

func jobList(a *app) *cobra.Command {
	var n nodeFlags
	cmd := &cobra.Command{
		Use: "list", Aliases: []string{"ls"}, Short: "List jobs on a node (source jobs and receives)", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := n.conn(cmd.Context(), a)
			if err != nil {
				return err
			}
			list, err := c.Transfers.ListJobs(cmd.Context(), &pb.ListJobsRequest{RouteId: n.route})
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), list)
			}
			printJobTable(cmd.OutOrStdout(), list.GetJobs())
			return nil
		},
	}
	n.register(cmd)
	return cmd
}

func jobGet(a *app) *cobra.Command {
	var n nodeFlags
	var all bool
	cmd := &cobra.Command{
		Use: "get <job-id>", Short: "Show a job (running and failed files by default)", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := n.conn(cmd.Context(), a)
			if err != nil {
				return err
			}
			j, err := c.Transfers.GetJob(cmd.Context(), &pb.GetJobRequest{JobId: args[0], Files: fileView(all)})
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), j)
			}
			printJobSummary(cmd.OutOrStdout(), j)
			return nil
		},
	}
	n.register(cmd)
	cmd.Flags().BoolVar(&all, "files", false, "list every file, not only running/failed ones")
	return cmd
}

func jobWatch(a *app) *cobra.Command {
	var n nodeFlags
	var interval time.Duration
	cmd := &cobra.Command{
		Use: "watch <job-id>", Short: "Follow a job until it finishes", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := n.conn(cmd.Context(), a)
			if err != nil {
				return err
			}
			final, err := watchJob(cmd.Context(), a, c, args[0], interval, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			return reportFinal(a, cmd.OutOrStdout(), c, final, false)
		},
	}
	n.register(cmd)
	cmd.Flags().DurationVar(&interval, "interval", time.Second, "refresh interval")
	return cmd
}

func jobTune(a *app) *cobra.Command {
	var n nodeFlags
	var tf tuningFlags
	cmd := &cobra.Command{
		Use:   "tune <job-id>",
		Short: "Change a running job's tuning (applies to files that start afterwards)",
		Long: `Concurrency takes effect immediately for scheduling. Other settings apply to
files that start after the change; files already in flight keep their settings.`,
		Example: "  grover job tune job-20261122-1 --route uc-tacc -c 16 -P 4",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			t, err := tf.tuning()
			if err != nil {
				return err
			}
			if t == (tuning.Tuning{}) {
				return errorf("no tuning flags given; see --help")
			}
			c, err := n.conn(cmd.Context(), a)
			if err != nil {
				return err
			}
			j, err := c.Transfers.TuneJob(cmd.Context(), &pb.TuneJobRequest{JobId: args[0], Tuning: t.PB()})
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), j)
			}
			printJobSummary(cmd.OutOrStdout(), j)
			return nil
		},
	}
	n.register(cmd)
	tf.register(cmd.Flags())
	return cmd
}

func jobAbort(a *app) *cobra.Command {
	var n nodeFlags
	cmd := &cobra.Command{
		Use: "abort <job-id>", Short: "Abort a running job", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := n.conn(cmd.Context(), a)
			if err != nil {
				return err
			}
			j, err := c.Transfers.AbortJob(cmd.Context(), &pb.JobRef{JobId: args[0]})
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "job %s: %s\n", j.JobId, stateName(j.State))
			return nil
		},
	}
	n.register(cmd)
	return cmd
}

func printJobTable(w io.Writer, jobs []*pb.Job) {
	t := table(w)
	fmt.Fprintln(t, "JOB\tROLE\tSTATE\tROUTE\tPROTO\tFILES\tPROGRESS\tAVG\tSTARTED")
	for _, j := range jobs {
		fmt.Fprintf(t, "%s\t%s\t%s\t%s\t%s\t%d/%d\t%s of %s\t%s\t%s\n", j.JobId, roleName(j.Role), stateName(j.State),
			dash(j.RouteId), protoName(j.Protocol), j.FilesDone, j.FilesTotal,
			formatBytes(j.BytesDone), formatBytes(j.BytesTotal), formatRate(j.AverageBps), formatTime(j.StartedAtUnixNano))
	}
	t.Flush()
}

// ---------------------------------------------------------------- history

func historyCommand(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "history",
		Short: "Read job logs kept on a node (manifest, final state, snapshots, energy)",
	}
	cmd.AddCommand(historyList(a), historyGet(a), historySnapshots(a), historyEnergy(a))
	return cmd
}

type historyFlags struct {
	nodeFlags
	dest bool
}

func (h *historyFlags) register(cmd *cobra.Command) {
	h.nodeFlags.register(cmd)
	cmd.Flags().BoolVar(&h.dest, "dest", false, "read the destination-side log instead of the source-side one")
}

func (h *historyFlags) role() pb.EndpointRole {
	if h.dest {
		return pb.EndpointRole_ENDPOINT_ROLE_DESTINATION
	}
	return pb.EndpointRole_ENDPOINT_ROLE_SOURCE
}

func historyList(a *app) *cobra.Command {
	var n nodeFlags
	var limit uint32
	var since time.Duration
	cmd := &cobra.Command{
		Use: "list", Aliases: []string{"ls"}, Short: "List logged jobs, newest first", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := n.conn(cmd.Context(), a)
			if err != nil {
				return err
			}
			req := &pb.ListHistoryRequest{RouteId: n.route, Limit: limit}
			if since > 0 {
				req.SinceUnixNano = time.Now().Add(-since).UnixNano()
			}
			list, err := c.History.ListHistory(cmd.Context(), req)
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), list)
			}
			t := table(cmd.OutOrStdout())
			fmt.Fprintln(t, "JOB\tROLE\tSTATE\tROUTE\tBYTES\tDURATION\tAVG\tENERGY\tSTARTED")
			for _, e := range list.GetJobs() {
				energy := "-"
				if e.EnergyJoules > 0 {
					energy = fmt.Sprintf("%.1f J", e.EnergyJoules)
				}
				fmt.Fprintf(t, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", e.JobId, roleName(e.Role), stateName(e.State), dash(e.RouteId),
					formatBytes(e.BytesDone), formatDuration(time.Duration(e.DurationSeconds*float64(time.Second))),
					formatRate(e.ThroughputBps), energy, formatTime(e.StartedAtUnixNano))
			}
			return t.Flush()
		},
	}
	n.register(cmd)
	cmd.Flags().Uint32Var(&limit, "limit", 50, "maximum entries (0 = all)")
	cmd.Flags().DurationVar(&since, "since", 0, "only jobs started within this duration, e.g. 24h")
	return cmd
}

func historyGet(a *app) *cobra.Command {
	var h historyFlags
	cmd := &cobra.Command{
		Use: "get <job-id>", Short: "Show a logged job's summary, manifest, and final state", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := h.conn(cmd.Context(), a)
			if err != nil {
				return err
			}
			rec, err := c.History.GetHistory(cmd.Context(), &pb.HistoryRef{JobId: args[0], Role: h.role()})
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), rec)
			}
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "log dir: %s\n", rec.Entry.Dir)
			if rec.Entry.EnergyJoules > 0 {
				fmt.Fprintf(w, "energy:  %.1f J\n", rec.Entry.EnergyJoules)
			}
			if rec.Final != nil {
				printJobSummary(w, rec.Final)
			} else {
				fmt.Fprintf(w, "no final.json yet (job running or groverd stopped mid-job); %d files planned\n", len(rec.Manifest.GetFiles()))
			}
			return nil
		},
	}
	h.register(cmd)
	return cmd
}

func historySnapshots(a *app) *cobra.Command {
	var h historyFlags
	var limit uint32
	cmd := &cobra.Command{
		Use: "snapshots <job-id>", Short: "Print per-second snapshots (use --json for full records)", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := h.conn(cmd.Context(), a)
			if err != nil {
				return err
			}
			list, err := c.History.ListSnapshots(cmd.Context(), &pb.ListSnapshotsRequest{JobId: args[0], Role: h.role(), Limit: limit})
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), list)
			}
			t := table(cmd.OutOrStdout())
			fmt.Fprintln(t, "TIME\tSTATE\tBYTES\tCURRENT\tAVERAGE\tSTREAMS\tRETRANSMITS")
			for _, j := range list.GetJobs() {
				fmt.Fprintf(t, "%s\t%s\t%s\t%s\t%s\t%d\t%d\n", formatTime(j.SampledAtUnixNano), stateName(j.State),
					formatBytes(j.BytesDone), formatRate(j.CurrentBps), formatRate(j.AverageBps), j.StreamsActive, j.Retransmits)
			}
			return t.Flush()
		},
	}
	h.register(cmd)
	cmd.Flags().Uint32Var(&limit, "limit", 0, "maximum snapshots (0 = all)")
	return cmd
}

func historyEnergy(a *app) *cobra.Command {
	var h historyFlags
	var limit uint32
	cmd := &cobra.Command{
		Use: "energy <job-id>", Short: "Print the job's raw energy.csv", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := h.conn(cmd.Context(), a)
			if err != nil {
				return err
			}
			csv, err := c.History.GetEnergy(cmd.Context(), &pb.EnergyRequest{JobId: args[0], Role: h.role(), Limit: limit})
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), csv)
			}
			if csv.Header == "" {
				return errorf("no energy log for %s (was groverd started with --energy-monitor?)", args[0])
			}
			fmt.Fprintln(cmd.OutOrStdout(), csv.Header)
			fmt.Fprintln(cmd.OutOrStdout(), strings.Join(csv.Rows, "\n"))
			return nil
		},
	}
	h.register(cmd)
	cmd.Flags().Uint32Var(&limit, "limit", 0, "maximum rows (0 = all)")
	return cmd
}
