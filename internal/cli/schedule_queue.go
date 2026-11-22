package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/jgoldverg/grover/internal/config"
	"github.com/jgoldverg/grover/internal/orchestrate"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/tuning"
)

// The queue is a small local JSON file of future transfers over routes:
// "schedule add" appends, "schedule run-pending" runs whatever is due.

type queued struct {
	ID          string        `json:"id"`
	Route       string        `json:"route"`
	Sources     []string      `json:"sources"`
	Destination string        `json:"destination"`
	RunAt       time.Time     `json:"run_at"`
	Tuning      tuning.Tuning `json:"tuning"`
	State       string        `json:"state"` // pending | running | done | failed
	JobID       string        `json:"job_id,omitempty"`
	Error       string        `json:"error,omitempty"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

type queueFile struct {
	Entries []queued `json:"entries"`
}

func queuePath(p string) string {
	if p != "" {
		return config.ExpandPath(p)
	}
	return filepath.Join(config.GroverDir(), "scheduled_jobs.json")
}

func loadQueue(p string) (queueFile, error) {
	var q queueFile
	data, err := os.ReadFile(queuePath(p))
	if errors.Is(err, fs.ErrNotExist) {
		return q, nil
	}
	if err != nil {
		return q, err
	}
	if err := json.Unmarshal(data, &q); err != nil {
		return q, fmt.Errorf("parse %s: %w", queuePath(p), err)
	}
	return q, nil
}

func saveQueue(p string, q queueFile) error {
	sort.Slice(q.Entries, func(i, j int) bool { return q.Entries[i].RunAt.Before(q.Entries[j].RunAt) })
	data, err := json.MarshalIndent(q, "", "  ")
	if err != nil {
		return err
	}
	path := queuePath(p)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func updateQueued(p string, e queued) error {
	q, err := loadQueue(p)
	if err != nil {
		return err
	}
	e.UpdatedAt = time.Now().UTC()
	for i := range q.Entries {
		if q.Entries[i].ID == e.ID {
			q.Entries[i] = e
			return saveQueue(p, q)
		}
	}
	return fmt.Errorf("queued transfer %q not found", e.ID)
}

func parseRunAt(at string, delay time.Duration, now time.Time) (time.Time, error) {
	switch {
	case at != "" && delay > 0:
		return time.Time{}, errorf("use --at or --in, not both")
	case delay > 0:
		return now.Add(delay).UTC(), nil
	case at == "":
		return time.Time{}, errorf("--at or --in is required")
	}
	t, err := time.Parse(time.RFC3339, at)
	if err != nil {
		return time.Time{}, errorf("--at %q must be RFC3339, e.g. 2026-06-01T12:00:00Z", at)
	}
	return t.UTC(), nil
}

func scheduleAdd(a *app) *cobra.Command {
	var store, id, at string
	var in time.Duration
	var tf tuningFlags
	cmd := &cobra.Command{
		Use:     "add <route> <source>... <destination-dir>",
		Short:   "Queue a transfer over a route for later",
		Example: "  grover schedule add uc-tacc /data/run1 /scratch/in --in 30m -P 4",
		Args:    cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			runAt, err := parseRunAt(at, in, time.Now())
			if err != nil {
				return err
			}
			t, err := tf.tuning()
			if err != nil {
				return err
			}
			if id == "" {
				id = uuid.NewString()[:8]
			}
			if strings.ContainsAny(id, " /\\") {
				return errorf("--id must not contain spaces or slashes")
			}
			q, err := loadQueue(store)
			if err != nil {
				return err
			}
			for _, e := range q.Entries {
				if e.ID == id {
					return errorf("queued transfer %q already exists", id)
				}
			}
			e := queued{ID: id, Route: args[0], Sources: args[1 : len(args)-1], Destination: args[len(args)-1],
				RunAt: runAt, Tuning: t, State: "pending", UpdatedAt: time.Now().UTC()}
			q.Entries = append(q.Entries, e)
			if err := saveQueue(store, q); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "queued %s on route %s at %s\n", e.ID, e.Route, e.RunAt.Local().Format(time.RFC3339))
			return nil
		},
	}
	cmd.Flags().StringVar(&store, "queue", "", "queue file (default ~/.grover/scheduled_jobs.json)")
	cmd.Flags().StringVar(&id, "id", "", "entry id (default random)")
	cmd.Flags().StringVar(&at, "at", "", "run time, RFC3339")
	cmd.Flags().DurationVar(&in, "in", 0, "run after this delay, e.g. 30m")
	tf.register(cmd.Flags())
	return cmd
}

func scheduleList(a *app) *cobra.Command {
	var store, state string
	cmd := &cobra.Command{
		Use: "list", Aliases: []string{"ls"}, Short: "List queued transfers", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			q, err := loadQueue(store)
			if err != nil {
				return err
			}
			var entries []queued
			for _, e := range q.Entries {
				if state == "" || e.State == state {
					entries = append(entries, e)
				}
			}
			if a.jsonOut {
				return printJSON(cmd.OutOrStdout(), entries)
			}
			t := table(cmd.OutOrStdout())
			fmt.Fprintln(t, "ID\tSTATE\tRUN AT\tROUTE\tJOB\tERROR")
			for _, e := range entries {
				fmt.Fprintf(t, "%s\t%s\t%s\t%s\t%s\t%s\n", e.ID, e.State, e.RunAt.Local().Format(time.RFC3339), e.Route, dash(e.JobID), dash(e.Error))
			}
			return t.Flush()
		},
	}
	cmd.Flags().StringVar(&store, "queue", "", "queue file")
	cmd.Flags().StringVar(&state, "state", "", "filter by state")
	return cmd
}

func scheduleRunPending(a *app) *cobra.Command {
	var store string
	var watch, keepGoing bool
	var poll time.Duration
	cmd := &cobra.Command{
		Use:   "run-pending",
		Short: "Run queued transfers that are due (keep polling with --watch)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, out := cmd.Context(), cmd.OutOrStdout()
			for {
				q, err := loadQueue(store)
				if err != nil {
					return err
				}
				ran := 0
				for _, e := range q.Entries {
					if e.State != "pending" || e.RunAt.After(time.Now()) {
						continue
					}
					ran++
					e.State = "running"
					if err := updateQueued(store, e); err != nil {
						return err
					}
					job, err := runQueued(cmd, a, e)
					e.State, e.Error = "done", ""
					if job != nil {
						e.JobID = job.JobId
					}
					if err != nil {
						e.State, e.Error = "failed", err.Error()
					}
					if uerr := updateQueued(store, e); uerr != nil {
						return uerr
					}
					fmt.Fprintf(out, "%s: %s %s\n", e.ID, e.State, e.Error)
					if err != nil && !keepGoing {
						return err
					}
				}
				if !watch {
					if ran == 0 {
						fmt.Fprintln(out, "nothing due")
					}
					return nil
				}
				if err := sleepUntil(ctx, time.Now().Add(poll)); err != nil {
					return err
				}
			}
		},
	}
	cmd.Flags().StringVar(&store, "queue", "", "queue file")
	cmd.Flags().BoolVar(&watch, "watch", false, "keep polling for due entries")
	cmd.Flags().DurationVar(&poll, "poll", 5*time.Second, "poll interval with --watch")
	cmd.Flags().BoolVar(&keepGoing, "continue-on-error", false, "keep going after a failed entry")
	return cmd
}

func runQueued(cmd *cobra.Command, a *app, e queued) (*pb.Job, error) {
	ctx := cmd.Context()
	r, err := a.route(ctx, e.Route)
	if err != nil {
		return nil, err
	}
	src, err := a.conn(ctx, r.Source)
	if err != nil {
		return nil, err
	}
	s, err := orchestrate.Pick(ctx, src, r.Name, "")
	if err != nil {
		return nil, err
	}
	job, err := src.Transfers.StartJob(ctx, &pb.StartJobRequest{
		JobId: "queued-" + e.ID, SessionId: s.SessionId, Sources: e.Sources, Destination: e.Destination, Tuning: e.Tuning.PB(),
	})
	if err != nil {
		return nil, err
	}
	final, err := watchJob(ctx, a, src, job.JobId, time.Second, cmd.OutOrStdout())
	if err != nil {
		return job, err
	}
	if final.State != pb.RuntimeState_RUNTIME_STATE_DONE {
		return final, errors.New(final.Error)
	}
	return final, nil
}
