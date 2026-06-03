package cli

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/spf13/cobra"
)

type scheduleRunOptions struct {
	RouteKey        string
	RouteTemplate   string
	RouteStore      string
	DestinationRoot string
	Protocol        string
	Concurrency     int
	ParallelStreams int
	Limit           int
	Offset          int
	DryRun          bool
	ContinueOnError bool
	UIMode          string
	UIIntervalMs    int
}

type scheduleRow struct {
	JobID            string
	RouteKey         string
	AllocatedBytes   uint64
	SourceNode       string
	DestinationNode  string
	ForecastID       string
	AllocatedTime    string
	CarbonEmissions  string
	TransferTime     string
	SlotDurationSecs string
	Raw              map[string]string
}

func ScheduleCommand() *cobra.Command {
	opts := scheduleRunOptions{}
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Run GreenTransferScheduler CSV schedules through groverd",
	}
	cmd.AddCommand(scheduleRunCommand(&opts))
	cmd.AddCommand(scheduleAddCommand())
	cmd.AddCommand(scheduleListCommand())
	cmd.AddCommand(scheduleRunPendingCommand())
	return cmd
}

func scheduleRunCommand(opts *scheduleRunOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "run <schedule.csv>",
		Short:        "Execute schedule rows as synthetic Grover transfers",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSchedule(cmd, args[0], opts)
		},
	}
	cmd.Flags().StringVar(&opts.RouteKey, "route-key", "", "Schedule route key to execute, such as tacc_buff")
	cmd.Flags().StringVar(&opts.RouteTemplate, "route-template", "", "Prepared Grover route template name (defaults to --route-key)")
	cmd.Flags().StringVar(&opts.RouteStore, "route-store", "", "Path to local route template store")
	_ = cmd.Flags().MarkHidden("route-store")
	cmd.Flags().StringVar(&opts.DestinationRoot, "destination-root", "", "Destination root for synthetic schedule payloads when using server routes")
	cmd.Flags().StringVar(&opts.Protocol, "protocol", "", "Override route protocol (tcp|udp)")
	cmd.Flags().IntVar(&opts.Concurrency, "concurrency", 0, "Override route files-in-flight")
	cmd.Flags().IntVar(&opts.ParallelStreams, "parallel-streams", 0, "Override route streams per file")
	cmd.Flags().IntVar(&opts.Limit, "limit", 0, "Maximum matching rows to execute (0 means all)")
	cmd.Flags().IntVar(&opts.Offset, "offset", 0, "Skip this many matching rows before executing")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "Print planned synthetic transfers without starting jobs")
	cmd.Flags().BoolVar(&opts.ContinueOnError, "continue-on-error", false, "Continue running later schedule rows if a transfer fails")
	cmd.Flags().StringVar(&opts.UIMode, "ui", "summary", "Transfer UI mode for each row (summary|live|none)")
	cmd.Flags().IntVar(&opts.UIIntervalMs, "ui-interval-ms", 2000, "Live metrics UI refresh interval in milliseconds")
	return cmd
}

type scheduleAddOptions struct {
	Store           string
	ID              string
	At              string
	Delay           time.Duration
	Protocol        string
	Concurrency     int
	ParallelStreams int
	UIMode          string
	JSON            bool
}

type scheduleListOptions struct {
	Store string
	State string
	JSON  bool
}

type schedulePendingOptions struct {
	Store        string
	Watch        bool
	PollInterval time.Duration
	Limit        int
	Continue     bool
	JSON         bool
}

func scheduleAddCommand() *cobra.Command {
	opts := scheduleAddOptions{UIMode: "summary"}
	cmd := &cobra.Command{
		Use:          "add <route> <source-path> <destination-path>",
		Short:        "Add a future transfer to the local schedule JSON store",
		Args:         cobra.ExactArgs(3),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			runAt, err := parseScheduledRunAt(opts.At, opts.Delay, time.Now())
			if err != nil {
				return err
			}
			store, err := newScheduledTransferStore(opts.Store)
			if err != nil {
				return err
			}
			entry, err := store.add(cmd.Context(), scheduledTransferEntry{
				ID:              opts.ID,
				Route:           args[0],
				Source:          args[1],
				Destination:     args[2],
				RunAt:           runAt,
				Protocol:        opts.Protocol,
				Concurrency:     opts.Concurrency,
				ParallelStreams: opts.ParallelStreams,
				UIMode:          opts.UIMode,
				State:           scheduledTransferPending,
			})
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), entry)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "scheduled_transfer: %s\n", entry.ID)
			fmt.Fprintf(cmd.OutOrStdout(), "state: %s\n", entry.State)
			fmt.Fprintf(cmd.OutOrStdout(), "route: %s\n", entry.Route)
			fmt.Fprintf(cmd.OutOrStdout(), "run_at: %s\n", entry.RunAt.Format(time.RFC3339Nano))
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.Store, "schedule-store", "", "Path to local scheduled transfer JSON store")
	cmd.Flags().StringVar(&opts.ID, "id", "", "Scheduled transfer id")
	cmd.Flags().StringVar(&opts.At, "at", "", "Run time as RFC3339/RFC3339Nano")
	cmd.Flags().DurationVar(&opts.Delay, "delay", 0, "Run after this duration, such as 30s or 15m")
	cmd.Flags().StringVar(&opts.Protocol, "protocol", "", "Transfer protocol override (tcp|udp)")
	cmd.Flags().IntVar(&opts.Concurrency, "concurrency", 0, "Files in flight override")
	cmd.Flags().IntVar(&opts.ParallelStreams, "parallel-streams", 0, "Streams per file override")
	cmd.Flags().StringVar(&opts.UIMode, "ui", "summary", "Transfer UI mode when executed (summary|live|none)")
	cmd.Flags().BoolVar(&opts.JSON, "json", false, "Print JSON")
	return cmd
}

func scheduleListCommand() *cobra.Command {
	opts := scheduleListOptions{}
	cmd := &cobra.Command{
		Use:          "list",
		Short:        "List scheduled transfers from the local JSON store",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := newScheduledTransferStore(opts.Store)
			if err != nil {
				return err
			}
			entries, err := store.list()
			if err != nil {
				return err
			}
			entries = filterScheduledTransfers(entries, opts.State)
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), struct {
					Entries []scheduledTransferEntry `json:"entries"`
				}{Entries: entries})
			}
			if len(entries) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no scheduled transfers")
				return nil
			}
			for _, entry := range entries {
				fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\t%s\t%s\n",
					entry.ID,
					entry.State,
					entry.RunAt.Format(time.RFC3339),
					entry.Route,
					entry.TransferJobID,
				)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.Store, "schedule-store", "", "Path to local scheduled transfer JSON store")
	cmd.Flags().StringVar(&opts.State, "state", "", "Filter by state")
	cmd.Flags().BoolVar(&opts.JSON, "json", false, "Print JSON")
	return cmd
}

func scheduleRunPendingCommand() *cobra.Command {
	opts := schedulePendingOptions{PollInterval: defaultSchedulePollInterval}
	cmd := &cobra.Command{
		Use:          "run-pending",
		Short:        "Run due scheduled transfers from the local JSON store",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := newScheduledTransferStore(opts.Store)
			if err != nil {
				return err
			}
			for {
				ran, err := runDueScheduledTransfers(cmd, store, opts)
				if err != nil {
					return err
				}
				if !opts.Watch {
					if ran == 0 {
						fmt.Fprintln(cmd.OutOrStdout(), "no scheduled transfers due")
					}
					return nil
				}
				select {
				case <-cmd.Context().Done():
					return cmd.Context().Err()
				case <-time.After(opts.PollInterval):
				}
			}
		},
	}
	cmd.Flags().StringVar(&opts.Store, "schedule-store", "", "Path to local scheduled transfer JSON store")
	cmd.Flags().BoolVar(&opts.Watch, "watch", false, "Keep polling and execute entries as they become due")
	cmd.Flags().DurationVar(&opts.PollInterval, "poll-interval", defaultSchedulePollInterval, "Polling interval for --watch")
	cmd.Flags().IntVar(&opts.Limit, "limit", 0, "Maximum due entries to run per poll")
	cmd.Flags().BoolVar(&opts.Continue, "continue-on-error", false, "Continue executing due entries after a failure")
	cmd.Flags().BoolVar(&opts.JSON, "json", false, "Print per-entry JSON results")
	return cmd
}

func runSchedule(cmd *cobra.Command, schedulePath string, opts *scheduleRunOptions) error {
	if opts == nil {
		return fmt.Errorf("schedule options are required")
	}
	routeKey := strings.TrimSpace(opts.RouteKey)
	if routeKey == "" {
		return fmt.Errorf("--route-key is required")
	}
	templateName := strings.TrimSpace(opts.RouteTemplate)
	if templateName == "" {
		templateName = routeKey
	}
	rows, err := loadScheduleRows(schedulePath)
	if err != nil {
		return err
	}
	route, err := scheduleRouteTemplate(cmd, templateName, routeKey, opts)
	if err != nil {
		return err
	}
	matched := 0
	executed := 0
	for _, row := range rows {
		if row.RouteKey != routeKey {
			continue
		}
		matched++
		if opts.Offset > 0 && matched <= opts.Offset {
			continue
		}
		if opts.Limit > 0 && executed >= opts.Limit {
			break
		}
		executed++
		jobID := scheduleTransferJobID(routeKey, row.JobID)
		syntheticPath := syntheticScheduleSource(row.AllocatedBytes, routeKey, row.JobID)
		fmt.Fprintf(cmd.OutOrStdout(), "schedule_row: job_id=%s route=%s bytes=%d synthetic=%s destination=%s\n", row.JobID, row.RouteKey, row.AllocatedBytes, syntheticPath, route.Destination)
		if opts.DryRun {
			continue
		}
		rowRoute := route
		if strings.TrimSpace(opts.Protocol) != "" {
			rowRoute.Protocol = opts.Protocol
		}
		if opts.Concurrency > 0 {
			rowRoute.Concurrency = opts.Concurrency
		}
		if opts.ParallelStreams > 0 {
			rowRoute.ParallelStreams = opts.ParallelStreams
		}
		copyOpts := CopyOptions{
			Concurrency:     rowRoute.Concurrency,
			ParallelStreams: rowRoute.ParallelStreams,
			Protocol:        rowRoute.Protocol,
			Via:             append([]string(nil), rowRoute.Via...),
			RouteStore:      opts.RouteStore,
			RouteName:       rowRoute.Name,
			UIMode:          opts.UIMode,
			UIIntervalMs:    opts.UIIntervalMs,
			Paths:           []string{syntheticPath},
			JobID:           jobID,
		}
		var job *pb.TransferJob
		if strings.TrimSpace(opts.RouteStore) != "" {
			err = fmt.Errorf("local route-store schedule execution is no longer supported; store the route on groverd and prepare a route session")
		} else {
			job, err = startTransferOverPreparedRouteSession(cmd, rowRoute.Source, rowRoute.Destination, copyOpts)
		}
		if err != nil {
			if opts.ContinueOnError {
				fmt.Fprintf(cmd.OutOrStdout(), "schedule_row_failed: job_id=%s error=%s\n", row.JobID, err)
				continue
			}
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "schedule_row_done: job_id=%s transfer_job=%s state=%s\n", row.JobID, job.GetJobId(), shortRuntimeState(job.GetState()))
		if job.GetErrorMessage() != "" && !opts.ContinueOnError {
			return fmt.Errorf("schedule row %s failed: %s", row.JobID, job.GetErrorMessage())
		}
	}
	if matched == 0 {
		return fmt.Errorf("no schedule rows matched route key %q", routeKey)
	}
	if executed == 0 {
		return fmt.Errorf("no schedule rows executed for route key %q after offset/limit", routeKey)
	}
	return nil
}

func scheduleRouteTemplate(cmd *cobra.Command, templateName, routeKey string, opts *scheduleRunOptions) (storedRouteTemplate, error) {
	if opts != nil && strings.TrimSpace(opts.RouteStore) != "" {
		store, err := newRouteTemplateStore(opts.RouteStore)
		if err != nil {
			return storedRouteTemplate{}, err
		}
		route, err := store.get(templateName)
		if err != nil {
			return storedRouteTemplate{}, fmt.Errorf("load route template %q for route key %q: %w", templateName, routeKey, err)
		}
		return route, nil
	}
	routeClient, closeFn, err := openRouteConfigControl(cmd)
	if err != nil {
		return storedRouteTemplate{}, err
	}
	defer closeFn()
	cfg, err := routeClient.GetRoute(cmd.Context(), templateName)
	if err != nil {
		return storedRouteTemplate{}, fmt.Errorf("load server route %q for route key %q: %w", templateName, routeKey, err)
	}
	return scheduleTemplateFromServerRoute(cfg, strings.TrimSpace(opts.DestinationRoot), opts)
}

func scheduleTemplateFromServerRoute(cfg *pb.RouteConfig, destinationRoot string, opts *scheduleRunOptions) (storedRouteTemplate, error) {
	if cfg == nil {
		return storedRouteTemplate{}, fmt.Errorf("server route is required")
	}
	if opts == nil {
		opts = &scheduleRunOptions{}
	}
	destinationRoot = strings.TrimSpace(destinationRoot)
	if destinationRoot == "" {
		return storedRouteTemplate{}, fmt.Errorf("--destination-root is required when schedule run uses server routes")
	}
	source, err := routeEndpointLocation(cfg.GetSource(), "/unused/synthetic-source-root", "source")
	if err != nil {
		return storedRouteTemplate{}, err
	}
	destination, err := routeEndpointLocation(cfg.GetDestination(), destinationRoot, "destination")
	if err != nil {
		return storedRouteTemplate{}, err
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	streams := opts.ParallelStreams
	if streams <= 0 {
		streams = 1
	}
	protocol := routeProtocolLabel(cfg.GetProtocol())
	if strings.TrimSpace(opts.Protocol) != "" {
		protocol = strings.ToLower(strings.TrimSpace(opts.Protocol))
	}
	return storedRouteTemplate{
		Name:             cfg.GetName(),
		Source:           source,
		Destination:      destination,
		Via:              append([]string(nil), cfg.GetVia()...),
		Protocol:         protocol,
		ParallelStreams:  streams,
		Concurrency:      concurrency,
		ConnectionOrigin: routeConnectionOriginLabel(cfg.GetConnectionOrigin()),
		DataDirection:    routeDataDirectionLabel(cfg.GetDataDirection()),
		State:            "prepared",
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}, nil
}

func loadScheduleRows(path string) ([]scheduleRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseScheduleRows(f, path)
}

func parseScheduleRows(r io.Reader, label string) ([]scheduleRow, error) {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read schedule csv %s: %w", label, err)
	}
	if len(records) == 0 || allBlank(records[0]) {
		return nil, fmt.Errorf("schedule csv %s has no header", label)
	}
	header := map[string]int{}
	for i, name := range records[0] {
		header[normalizeScheduleColumn(name)] = i
	}
	required := []string{"job_id", "route", "allocated_bytes"}
	for _, name := range required {
		if _, ok := header[name]; !ok {
			return nil, fmt.Errorf("schedule csv %s missing required column %q", label, name)
		}
	}
	rows := make([]scheduleRow, 0, len(records)-1)
	for line, record := range records[1:] {
		if allBlank(record) {
			continue
		}
		raw := make(map[string]string, len(header))
		for name, idx := range header {
			raw[name] = scheduleCell(record, idx)
		}
		bytes, err := parseScheduleBytes(raw["allocated_bytes"])
		if err != nil {
			return nil, fmt.Errorf("schedule csv %s line %d: %w", label, line+2, err)
		}
		rows = append(rows, scheduleRow{
			JobID:            raw["job_id"],
			RouteKey:         raw["route"],
			AllocatedBytes:   bytes,
			SourceNode:       raw["source_node"],
			DestinationNode:  raw["destination_node"],
			ForecastID:       raw["forecast_id"],
			AllocatedTime:    raw["allocated_time"],
			CarbonEmissions:  raw["carbon_emissions"],
			TransferTime:     raw["transfer_time"],
			SlotDurationSecs: raw["slot_duration_seconds"],
			Raw:              raw,
		})
	}
	return rows, nil
}

func normalizeScheduleColumn(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func allBlank(record []string) bool {
	for _, cell := range record {
		if strings.TrimSpace(cell) != "" {
			return false
		}
	}
	return true
}

func scheduleCell(record []string, idx int) string {
	if idx < 0 || idx >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[idx])
}

func parseScheduleBytes(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("allocated_bytes is empty")
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid allocated_bytes %q", raw)
	}
	if value < 0 {
		return 0, fmt.Errorf("allocated_bytes must be non-negative, got %q", raw)
	}
	return uint64(value), nil
}

func syntheticScheduleSource(size uint64, routeKey, jobID string) string {
	rel := filepath.ToSlash(filepath.Join("schedule", safeSchedulePathPart(routeKey), "job-"+safeSchedulePathPart(jobID)+".bin"))
	return fmt.Sprintf("synthetic://%d/%s", size, escapeSchedulePath(rel))
}

func escapeSchedulePath(path string) string {
	parts := strings.Split(filepath.ToSlash(path), "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}

func scheduleTransferJobID(routeKey, jobID string) string {
	now := time.Now().UnixNano()
	return fmt.Sprintf("schedule-%s-%s-%d", safeSchedulePathPart(routeKey), safeSchedulePathPart(jobID), now)
}

func safeSchedulePathPart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}

func parseScheduledRunAt(at string, delay time.Duration, now time.Time) (time.Time, error) {
	at = strings.TrimSpace(at)
	if at != "" && delay > 0 {
		return time.Time{}, fmt.Errorf("set only one of --at or --delay")
	}
	if delay > 0 {
		return now.Add(delay).UTC(), nil
	}
	if at == "" {
		return time.Time{}, fmt.Errorf("set --at or --delay")
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		parsed, err := time.Parse(layout, at)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid --at %q: use RFC3339, for example 2026-06-01T12:00:00Z", at)
}

func filterScheduledTransfers(entries []scheduledTransferEntry, state string) []scheduledTransferEntry {
	state = strings.ToLower(strings.TrimSpace(state))
	if state == "" {
		return entries
	}
	out := make([]scheduledTransferEntry, 0, len(entries))
	for _, entry := range entries {
		if string(entry.State) == state {
			out = append(out, entry)
		}
	}
	return out
}

func runDueScheduledTransfers(cmd *cobra.Command, store scheduledTransferStore, opts schedulePendingOptions) (int, error) {
	entries, err := store.list()
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	ran := 0
	for _, entry := range entries {
		if !scheduledTransferDue(entry, now) {
			continue
		}
		if opts.Limit > 0 && ran >= opts.Limit {
			break
		}
		ran++
		entry.State = scheduledTransferRunning
		entry.Error = ""
		if err := store.update(cmd.Context(), entry); err != nil {
			return ran, err
		}
		job, execErr := executeScheduledTransfer(cmd, entry)
		if job != nil {
			entry.TransferJobID = job.GetJobId()
		}
		if execErr != nil {
			entry.State = scheduledTransferFailed
			entry.Error = execErr.Error()
		} else if job != nil && job.GetState() == pb.RuntimeState_RUNTIME_STATE_FAILED {
			entry.State = scheduledTransferFailed
			entry.Error = job.GetErrorMessage()
			if entry.Error == "" {
				entry.Error = "transfer failed"
			}
		} else {
			entry.State = scheduledTransferDone
			entry.Error = ""
		}
		if err := store.update(cmd.Context(), entry); err != nil {
			return ran, err
		}
		if opts.JSON {
			_ = writeJSON(cmd.OutOrStdout(), entry)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "scheduled_transfer_done: id=%s state=%s transfer_job=%s error=%s\n",
				entry.ID,
				entry.State,
				entry.TransferJobID,
				entry.Error,
			)
		}
		if entry.State == scheduledTransferFailed && !opts.Continue {
			return ran, fmt.Errorf("scheduled transfer %s failed: %s", entry.ID, entry.Error)
		}
	}
	return ran, nil
}

func executeScheduledTransfer(cmd *cobra.Command, entry scheduledTransferEntry) (*pb.TransferJob, error) {
	copyOpts := CopyOptions{
		RouteName:        entry.Route,
		Protocol:         entry.Protocol,
		Concurrency:      entry.Concurrency,
		ParallelStreams:  entry.ParallelStreams,
		UIMode:           entry.UIMode,
		UIIntervalMs:     2000,
		JobID:            entry.ID,
		ConnectionOrigin: "",
		DataDirection:    "",
	}
	if err := copyOpts.validate(); err != nil {
		return nil, err
	}
	args, err := applyPreparedRouteTemplate(cmd, []string{entry.Source, entry.Destination}, &copyOpts)
	if err != nil {
		return nil, err
	}
	return startRoutedTransfer(cmd, args[0], args[1], copyOpts)
}

func writeJSON(w io.Writer, v interface{}) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
