package cli

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/jgoldverg/grover/internal/client"
	"github.com/jgoldverg/grover/internal/orchestrate"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/transfer"
	"github.com/jgoldverg/grover/internal/tuning"
)

func scheduleCommand(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Run GreenTransferScheduler CSV schedules, or queue future transfers",
	}
	cmd.AddCommand(scheduleRun(a), scheduleAdd(a), scheduleList(a), scheduleRunPending(a))
	return cmd
}

// ---------------------------------------------------------------- CSV schedules

type scheduleRunOpts struct {
	routeKey, route, destRoot string
	limit, offset             int
	dryRun, keepGoing, keep   bool
	timeScale                 float64
	tune                      tuningFlags
}

func scheduleRun(a *app) *cobra.Command {
	o := &scheduleRunOpts{}
	cmd := &cobra.Command{
		Use:   "run <schedule.csv>",
		Short: "Execute schedule rows as synthetic transfers over a prepared route",
		Long: `Each CSV row becomes one synthetic transfer of allocated_bytes over a prepared
session of the route (prepare it first with "grover route prepare"). Rows are
grouped by forecast slot (forecast_id x slot_duration_seconds) and each slot
starts at its scheduled offset; rows within a slot run one after another.

Required columns: job_id, route, allocated_bytes.
Optional: forecast_id, slot_duration_seconds, flow_count (streams per file), and
anything else, which is kept as metadata.

Output lands at <destination-root>/schedule/<route-key>/job-<job_id>.bin and is
deleted after each row unless --keep-data is set.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error { return runScheduleCSV(cmd, a, o, args[0]) },
	}
	f := cmd.Flags()
	f.StringVar(&o.routeKey, "route-key", "", "schedule route key to execute (rows whose 'route' column matches)")
	f.StringVar(&o.route, "route", "", "Grover route to run on (default: the route key)")
	f.StringVar(&o.destRoot, "destination-root", "", "destination directory on the route's destination node")
	f.IntVar(&o.limit, "limit", 0, "maximum matching rows (0 = all)")
	f.IntVar(&o.offset, "offset", 0, "skip this many matching rows")
	f.BoolVar(&o.dryRun, "dry-run", false, "print the plan without starting jobs")
	f.BoolVar(&o.keepGoing, "continue-on-error", false, "keep running after a failed row")
	f.BoolVar(&o.keep, "keep-data", false, "keep synthetic output files")
	f.Float64Var(&o.timeScale, "time-scale", 1, "scale slot delays; 1 = real time, 0 = run all slots back to back")
	o.tune.register(f)
	return cmd
}

type scheduleRow struct {
	JobID          string
	RouteKey       string
	AllocatedBytes uint64
	FlowCount      int
	ForecastIndex  int
	SlotDuration   time.Duration
	Raw            map[string]string
}

func (r scheduleRow) offset() time.Duration {
	if r.ForecastIndex <= 1 || r.SlotDuration <= 0 {
		return 0
	}
	return time.Duration(r.ForecastIndex-1) * r.SlotDuration
}

func runScheduleCSV(cmd *cobra.Command, a *app, o *scheduleRunOpts, csvPath string) error {
	ctx, out := cmd.Context(), cmd.OutOrStdout()
	if o.routeKey == "" {
		return errorf("--route-key is required")
	}
	if o.destRoot == "" || !strings.HasPrefix(o.destRoot, "/") {
		return errorf("--destination-root must be an absolute directory on the destination node")
	}
	if o.timeScale < 0 {
		return errorf("--time-scale must be >= 0")
	}
	base, err := o.tune.tuning()
	if err != nil {
		return err
	}
	f, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	rows, err := parseScheduleRows(f, csvPath)
	f.Close()
	if err != nil {
		return err
	}
	selected, matched := selectScheduleRows(rows, o.routeKey, o.offset, o.limit)
	if matched == 0 {
		return errorf("no rows with route %q in %s", o.routeKey, csvPath)
	}
	routeName := o.route
	if routeName == "" {
		routeName = o.routeKey
	}
	var (
		r   *pb.Route
		src *client.Conn
	)
	if !o.dryRun {
		if r, err = a.route(ctx, routeName); err != nil {
			return err
		}
		if src, err = a.conn(ctx, r.Source); err != nil {
			return err
		}
	}
	started := time.Now()
	failed := 0
	for _, slot := range scheduleSlots(selected) {
		if !o.dryRun {
			if err := sleepUntil(ctx, started.Add(time.Duration(float64(slot.offset)*o.timeScale))); err != nil {
				return err
			}
		}
		fmt.Fprintf(out, "slot +%s: %d rows\n", slot.offset, len(slot.rows))
		for _, row := range slot.rows {
			rel := path.Join("schedule", transfer.SafeName(row.RouteKey), "job-"+transfer.SafeName(row.JobID)+".bin")
			t := base
			if row.FlowCount > 0 {
				t.Parallelism = uint32(row.FlowCount)
			}
			fmt.Fprintf(out, "  row %s: %s, parallelism %d -> %s/%s\n", row.JobID, formatBytes(row.AllocatedBytes), t.Parallelism, o.destRoot, rel)
			if o.dryRun {
				continue
			}
			if err := runScheduleRow(ctx, a, out, r, src, row, rel, o, t); err != nil {
				failed++
				fmt.Fprintf(out, "  row %s FAILED: %v\n", row.JobID, err)
				if !o.keepGoing {
					return err
				}
			}
		}
	}
	if failed > 0 {
		return errorf("%d of %d rows failed", failed, len(selected))
	}
	return nil
}

func runScheduleRow(ctx context.Context, a *app, out io.Writer, r *pb.Route, src *client.Conn, row scheduleRow, rel string, o *scheduleRunOpts, t tuning.Tuning) error {
	s, err := orchestrate.Pick(ctx, src, r.Name, "")
	if err != nil {
		return err
	}
	jobID := fmt.Sprintf("schedule-%s-%s-%d", transfer.SafeName(row.RouteKey), transfer.SafeName(row.JobID), time.Now().Unix())
	job, err := src.Transfers.StartJob(ctx, &pb.StartJobRequest{
		JobId: jobID, SessionId: s.SessionId, Destination: o.destRoot,
		Sources: []string{transfer.SyntheticSource(row.AllocatedBytes, rel)}, Tuning: t.PB(),
	})
	if err != nil {
		return err
	}
	final, err := orchestrate.Watch(ctx, src, job.JobId, time.Second, pb.FileView_FILE_VIEW_NONE, nil)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  row %s: job %s %s in %s (%s)\n", row.JobID, final.JobId, stateName(final.State),
		formatDuration(jobElapsed(final)), formatRate(final.AverageBps))
	if !o.keep {
		if dst, err := a.conn(ctx, r.Destination); err == nil {
			if _, err := dst.Transfers.DeletePath(ctx, &pb.DeletePathRequest{Path: path.Join(o.destRoot, rel)}); err != nil {
				fmt.Fprintf(out, "  row %s: cleanup failed: %v\n", row.JobID, err)
			}
		}
	}
	if final.State != pb.RuntimeState_RUNTIME_STATE_DONE {
		return errors.New(final.Error)
	}
	return nil
}

type scheduleSlot struct {
	offset time.Duration
	rows   []scheduleRow
}

func scheduleSlots(rows []scheduleRow) []scheduleSlot {
	by := map[time.Duration][]scheduleRow{}
	for _, r := range rows {
		by[r.offset()] = append(by[r.offset()], r)
	}
	out := make([]scheduleSlot, 0, len(by))
	for off, rs := range by {
		out = append(out, scheduleSlot{off, rs})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].offset < out[j].offset })
	return out
}

func selectScheduleRows(rows []scheduleRow, key string, offset, limit int) ([]scheduleRow, int) {
	matched := 0
	var out []scheduleRow
	for _, r := range rows {
		if r.RouteKey != key {
			continue
		}
		matched++
		if matched <= offset || (limit > 0 && len(out) >= limit) {
			continue
		}
		out = append(out, r)
	}
	return out, matched
}

func parseScheduleRows(r io.Reader, label string) ([]scheduleRow, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	records, err := cr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", label, err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("%s has no header", label)
	}
	header := map[string]int{}
	for i, name := range records[0] {
		header[strings.ToLower(strings.TrimSpace(name))] = i
	}
	for _, col := range []string{"job_id", "route", "allocated_bytes"} {
		if _, ok := header[col]; !ok {
			return nil, fmt.Errorf("%s is missing required column %q", label, col)
		}
	}
	var rows []scheduleRow
	for n, rec := range records[1:] {
		raw := map[string]string{}
		blank := true
		for name, i := range header {
			if i < len(rec) {
				raw[name] = strings.TrimSpace(rec[i])
				blank = blank && raw[name] == ""
			}
		}
		if blank {
			continue
		}
		line := n + 2
		bytes, err := parseNonNegative(raw["allocated_bytes"], "allocated_bytes", false)
		if err != nil {
			return nil, fmt.Errorf("%s line %d: %w", label, line, err)
		}
		flows, err := parseNonNegative(raw["flow_count"], "flow_count", true)
		if err != nil {
			return nil, fmt.Errorf("%s line %d: %w", label, line, err)
		}
		forecast, err := parseNonNegative(raw["forecast_id"], "forecast_id", true)
		if err != nil {
			return nil, fmt.Errorf("%s line %d: %w", label, line, err)
		}
		var slot time.Duration
		if s := raw["slot_duration_seconds"]; s != "" {
			v, err := strconv.ParseFloat(s, 64)
			if err != nil || v < 0 {
				return nil, fmt.Errorf("%s line %d: invalid slot_duration_seconds %q", label, line, s)
			}
			slot = time.Duration(v * float64(time.Second))
		}
		rows = append(rows, scheduleRow{
			JobID: raw["job_id"], RouteKey: raw["route"], AllocatedBytes: uint64(bytes),
			FlowCount: int(flows), ForecastIndex: int(forecast), SlotDuration: slot, Raw: raw,
		})
	}
	return rows, nil
}

// parseNonNegative parses a number that may be written as a float ("1.2e9").
// integer=true additionally rejects fractions.
func parseNonNegative(s, name string, integer bool) (float64, error) {
	if s == "" {
		if integer {
			return 0, nil
		}
		return 0, fmt.Errorf("%s is empty", name)
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("invalid %s %q", name, s)
	}
	if integer && v != float64(int64(v)) {
		return 0, fmt.Errorf("%s must be an integer, got %q", name, s)
	}
	return v, nil
}

func sleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
