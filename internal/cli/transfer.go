package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/jgoldverg/grover/internal/client"
	"github.com/jgoldverg/grover/internal/orchestrate"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/routes"
)

type transferOpts struct {
	route, sessionID, jobID string
	protocol, connectFrom   string
	via                     []string
	detach, showFiles       bool
	interval                time.Duration
	tune                    tuningFlags
}

func transferCommand(a *app) *cobra.Command {
	o := &transferOpts{}
	cmd := &cobra.Command{
		Use:     "transfer [flags] <source>... <destination-dir>",
		Aliases: []string{"cp"},
		Short:   "Copy files between groverd nodes (groverd moves the bytes)",
		Long: `Copy files into a destination directory. A source file lands at
<destination-dir>/<name>; a source directory's contents land under <destination-dir>/.

Without --route, grover opens a temporary direct session between the source and
destination nodes, runs the job, and closes the session. If both locations are
on the same node, the node copies locally.

With --route, the job runs on a session prepared by "grover route prepare";
paths are plain absolute paths on the route's source and destination nodes.

Ctrl-C detaches from a --route job (it keeps running; see "grover job watch")
and aborts a direct job.`,
		Example: `  grover transfer uc:/data/run1 tacc:/scratch/in/
  grover transfer --route uc-tacc -c 8 -P 4 /data/run1 /scratch/in
  grover transfer --protocol udp uc:synthetic://10737418240/10g.bin tacc:/scratch/
  grover transfer /data/a.bin /data/copies/          # local copy on the default node`,
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error { return runTransfer(cmd, a, o, args) },
	}
	f := cmd.Flags()
	f.StringVarP(&o.route, "route", "r", "", "run over a prepared session of this route")
	f.StringVar(&o.sessionID, "session-id", "", "with --route: use this session (default: newest idle)")
	f.StringVar(&o.jobID, "job-id", "", "job id (default: generated)")
	f.StringVar(&o.protocol, "protocol", "tcp", "direct only: data protocol tcp|udp")
	f.StringVar(&o.connectFrom, "connect-from", "source", "direct only: which side dials, source|destination")
	f.StringSliceVar(&o.via, "via", nil, "direct only: relay nodes, source to destination")
	f.BoolVarP(&o.detach, "detach", "d", false, "start the job and return (route jobs only)")
	f.BoolVar(&o.showFiles, "files", false, "print the per-file table when the job ends")
	f.DurationVar(&o.interval, "interval", time.Second, "progress refresh interval")
	o.tune.register(f)
	return cmd
}

func runTransfer(cmd *cobra.Command, a *app, o *transferOpts, args []string) error {
	ctx := cmd.Context()
	t, err := o.tune.tuning()
	if err != nil {
		return err
	}
	var srcs []location
	for _, raw := range args[:len(args)-1] {
		l, err := parseLocation(raw)
		if err != nil {
			return err
		}
		srcs = append(srcs, l)
	}
	dst, err := parseLocation(args[len(args)-1])
	if err != nil {
		return err
	}
	if strings.HasPrefix(dst.Path, "synthetic:") {
		return errorf("destination must be a directory path, not %s", dst.Path)
	}
	req := &pb.StartJobRequest{JobId: o.jobID, Destination: dst.Path, Tuning: t.PB()}

	var (
		srcConn   *client.Conn
		ephemeral *pb.RouteSession
	)
	if o.route != "" {
		for _, name := range []string{"protocol", "connect-from", "via"} {
			if cmd.Flags().Changed(name) {
				return errorf("--%s comes from the route definition and cannot be combined with --route", name)
			}
		}
		r, err := a.route(ctx, o.route)
		if err != nil {
			return err
		}
		if err := checkRouteNodes(a, r, srcs, dst); err != nil {
			return err
		}
		if srcConn, err = a.conn(ctx, r.Source); err != nil {
			return err
		}
		s, err := orchestrate.Pick(ctx, srcConn, r.Name, o.sessionID)
		if err != nil {
			return err
		}
		req.SessionId = s.SessionId
	} else {
		if o.sessionID != "" {
			return errorf("--session-id requires --route")
		}
		if o.detach {
			return errorf("--detach is only for --route jobs (direct sessions close when the job ends)")
		}
		srcNode := srcs[0].Node
		for _, s := range srcs[1:] {
			if s.Node != srcNode {
				return errorf("all sources must be on one node (got %q and %q)", srcNode, s.Node)
			}
		}
		if srcConn, err = a.conn(ctx, srcNode); err != nil {
			return err
		}
		dstAddr, err := a.pool.Addr(dst.Node)
		if err != nil {
			return err
		}
		sameNode := dstAddr == srcConn.Node.Addr && len(o.via) == 0
		if !sameNode || cmd.Flags().Changed("protocol") || cmd.Flags().Changed("connect-from") {
			proto, err := routes.ParseProtocol(o.protocol)
			if err != nil {
				return err
			}
			origin, err := routes.ParseOrigin(o.connectFrom)
			if err != nil {
				return err
			}
			ephemeral, err = orchestrate.Prepare(ctx, a.pool, orchestrate.Spec{
				Source: srcConn.Node.Addr, Destination: dstAddr, Via: splitList(o.via), Protocol: proto, ConnectFrom: origin,
			})
			if err != nil {
				return err
			}
			req.SessionId = ephemeral.SessionId
			defer func() {
				if err := orchestrate.Close(context.WithoutCancel(ctx), a.pool, srcConn.Node.Addr, ephemeral.SessionId); err != nil {
					slog.Warn("closing direct session failed", "session_id", ephemeral.SessionId, "error", err)
				}
			}()
		}
	}
	for _, s := range srcs {
		req.Sources = append(req.Sources, s.Path)
	}

	job, err := srcConn.Transfers.StartJob(ctx, req)
	if err != nil {
		return err
	}
	out := cmd.OutOrStdout()
	if o.detach {
		if a.jsonOut {
			return printProto(out, job)
		}
		fmt.Fprintf(out, "started job %s on %s\nwatch with: grover job watch %s --node %s\n", job.JobId, srcConn.Node.Name, job.JobId, srcConn.Node.Name)
		return nil
	}
	final, err := watchJob(ctx, a, srcConn, job.JobId, o.interval, out)
	if err != nil {
		if ctx.Err() == nil {
			return err
		}
		if ephemeral != nil {
			_, _ = srcConn.Transfers.AbortJob(context.WithoutCancel(ctx), &pb.JobRef{JobId: job.JobId})
			fmt.Fprintf(out, "\naborted job %s\n", job.JobId)
		} else {
			fmt.Fprintf(out, "\ndetached; job %s keeps running. watch: grover job watch %s --node %s\n", job.JobId, job.JobId, srcConn.Node.Name)
		}
		return ctx.Err()
	}
	return reportFinal(a, out, srcConn, final, o.showFiles)
}

// checkRouteNodes rejects node prefixes that disagree with the route.
func checkRouteNodes(a *app, r *pb.Route, srcs []location, dst location) error {
	check := func(l location, want, role string) error {
		if l.Node == "" {
			return nil
		}
		addr, err := a.pool.Addr(l.Node)
		if err != nil {
			return err
		}
		if addr != want {
			return errorf("%s %s is not on route %s's %s node (%s); with --route use plain paths", role, l, r.Name, role, want)
		}
		return nil
	}
	for _, s := range srcs {
		if err := check(s, r.Source, "source"); err != nil {
			return err
		}
	}
	return check(dst, r.Destination, "destination")
}

// watchJob shows live progress until the job ends.
func watchJob(ctx context.Context, a *app, c *client.Conn, id string, interval time.Duration, out io.Writer) (*pb.Job, error) {
	tty := isTTY(out) && !a.jsonOut
	var lastPrint time.Time
	final, err := orchestrate.Watch(ctx, c, id, interval, pb.FileView_FILE_VIEW_NONE, func(j *pb.Job) {
		switch {
		case a.jsonOut:
		case tty:
			fmt.Fprintf(out, "\r\033[K%s", progressLine(j))
		case time.Since(lastPrint) >= 10*time.Second || orchestrate.IsTerminal(j.State):
			fmt.Fprintln(out, progressLine(j))
			lastPrint = time.Now()
		}
	})
	if tty {
		fmt.Fprintln(out)
	}
	return final, err
}

// reportFinal prints the result and turns a failed job into a non-zero exit.
func reportFinal(a *app, out io.Writer, c *client.Conn, j *pb.Job, files bool) error {
	if files || j.State != pb.RuntimeState_RUNTIME_STATE_DONE {
		if full, err := c.Transfers.GetJob(context.Background(), &pb.GetJobRequest{JobId: j.JobId, Files: fileView(files)}); err == nil {
			j = full
		}
	}
	if a.jsonOut {
		if err := printProto(out, j); err != nil {
			return err
		}
	} else {
		printJobSummary(out, j)
	}
	if j.State != pb.RuntimeState_RUNTIME_STATE_DONE {
		return errorf("job %s %s", j.JobId, stateName(j.State))
	}
	return nil
}

func fileView(all bool) pb.FileView {
	if all {
		return pb.FileView_FILE_VIEW_ALL
	}
	return pb.FileView_FILE_VIEW_ACTIVE
}
