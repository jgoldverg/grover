package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

const defaultSchedulePollInterval = time.Second

type scheduledTransferState string

const (
	scheduledTransferPending scheduledTransferState = "pending"
	scheduledTransferRunning scheduledTransferState = "running"
	scheduledTransferDone    scheduledTransferState = "done"
	scheduledTransferFailed  scheduledTransferState = "failed"
)

type scheduledTransferEntry struct {
	ID              string                 `json:"id"`
	Route           string                 `json:"route"`
	Source          string                 `json:"source"`
	Destination     string                 `json:"destination"`
	RunAt           time.Time              `json:"run_at"`
	Protocol        string                 `json:"protocol,omitempty"`
	Concurrency     int                    `json:"concurrency,omitempty"`
	ParallelStreams int                    `json:"parallel_streams,omitempty"`
	UIMode          string                 `json:"ui,omitempty"`
	State           scheduledTransferState `json:"state"`
	TransferJobID   string                 `json:"transfer_job_id,omitempty"`
	Error           string                 `json:"error,omitempty"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
}

type scheduledTransferFile struct {
	Entries []scheduledTransferEntry `json:"entries"`
}

type scheduledTransferStore struct {
	path string
}

func newScheduledTransferStore(path string) (scheduledTransferStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return scheduledTransferStore{}, err
		}
		path = filepath.Join(home, ".grover", "scheduled_jobs.json")
	}
	return scheduledTransferStore{path: path}, nil
}

func (s scheduledTransferStore) add(ctx context.Context, entry scheduledTransferEntry) (scheduledTransferEntry, error) {
	if err := ctx.Err(); err != nil {
		return scheduledTransferEntry{}, err
	}
	entry, err := normalizeScheduledTransferEntry(entry)
	if err != nil {
		return scheduledTransferEntry{}, err
	}
	file, err := s.load()
	if err != nil {
		return scheduledTransferEntry{}, err
	}
	for _, existing := range file.Entries {
		if existing.ID == entry.ID {
			return scheduledTransferEntry{}, fmt.Errorf("scheduled transfer %q already exists", entry.ID)
		}
	}
	file.Entries = append(file.Entries, entry)
	if err := s.save(file); err != nil {
		return scheduledTransferEntry{}, err
	}
	return entry, nil
}

func (s scheduledTransferStore) list() ([]scheduledTransferEntry, error) {
	file, err := s.load()
	if err != nil {
		return nil, err
	}
	entries := append([]scheduledTransferEntry(nil), file.Entries...)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].RunAt.Equal(entries[j].RunAt) {
			return entries[i].ID < entries[j].ID
		}
		return entries[i].RunAt.Before(entries[j].RunAt)
	})
	return entries, nil
}

func (s scheduledTransferStore) update(ctx context.Context, entry scheduledTransferEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := s.load()
	if err != nil {
		return err
	}
	for i := range file.Entries {
		if file.Entries[i].ID == entry.ID {
			entry.UpdatedAt = time.Now().UTC()
			file.Entries[i] = entry
			return s.save(file)
		}
	}
	return fmt.Errorf("scheduled transfer %q not found", entry.ID)
}

func (s scheduledTransferStore) load() (scheduledTransferFile, error) {
	var file scheduledTransferFile
	if strings.TrimSpace(s.path) == "" {
		return file, errors.New("schedule store path is required")
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return file, nil
		}
		return file, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return file, nil
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return file, fmt.Errorf("load schedule store %s: %w", s.path, err)
	}
	for i := range file.Entries {
		normalized, err := normalizeScheduledTransferEntry(file.Entries[i])
		if err != nil {
			return file, fmt.Errorf("load scheduled transfer %q: %w", file.Entries[i].ID, err)
		}
		file.Entries[i] = normalized
	}
	return file, nil
}

func (s scheduledTransferStore) save(file scheduledTransferFile) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	sort.Slice(file.Entries, func(i, j int) bool {
		if file.Entries[i].RunAt.Equal(file.Entries[j].RunAt) {
			return file.Entries[i].ID < file.Entries[j].ID
		}
		return file.Entries[i].RunAt.Before(file.Entries[j].RunAt)
	})
	payload, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	tmp := fmt.Sprintf("%s.tmp.%s", s.path, uuid.NewString())
	if err := os.WriteFile(tmp, payload, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func normalizeScheduledTransferEntry(entry scheduledTransferEntry) (scheduledTransferEntry, error) {
	entry.ID = strings.TrimSpace(entry.ID)
	if entry.ID == "" {
		entry.ID = uuid.NewString()
	}
	if strings.ContainsAny(entry.ID, " \t\r\n/\\") {
		return scheduledTransferEntry{}, fmt.Errorf("scheduled transfer id %q must not contain whitespace or path separators", entry.ID)
	}
	entry.Route = strings.TrimSpace(entry.Route)
	if entry.Route == "" {
		return scheduledTransferEntry{}, errors.New("route is required")
	}
	entry.Source = strings.TrimSpace(entry.Source)
	if entry.Source == "" {
		return scheduledTransferEntry{}, errors.New("source is required")
	}
	entry.Destination = strings.TrimSpace(entry.Destination)
	if entry.Destination == "" {
		return scheduledTransferEntry{}, errors.New("destination is required")
	}
	if entry.RunAt.IsZero() {
		return scheduledTransferEntry{}, errors.New("run_at is required")
	}
	entry.Protocol = strings.ToLower(strings.TrimSpace(entry.Protocol))
	if entry.Protocol != "" && entry.Protocol != "tcp" && entry.Protocol != "udp" {
		return scheduledTransferEntry{}, fmt.Errorf("protocol %q must be tcp or udp", entry.Protocol)
	}
	if entry.Concurrency < 0 || entry.ParallelStreams < 0 {
		return scheduledTransferEntry{}, errors.New("concurrency and parallel_streams must be >= 0")
	}
	if entry.UIMode == "" {
		entry.UIMode = "summary"
	}
	switch entry.State {
	case "", scheduledTransferPending:
		entry.State = scheduledTransferPending
	case scheduledTransferRunning, scheduledTransferDone, scheduledTransferFailed:
	default:
		return scheduledTransferEntry{}, fmt.Errorf("unsupported scheduled transfer state %q", entry.State)
	}
	now := time.Now().UTC()
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = now
	}
	entry.UpdatedAt = now
	return entry, nil
}

func scheduledTransferDue(entry scheduledTransferEntry, now time.Time) bool {
	return entry.State == scheduledTransferPending && !entry.RunAt.After(now)
}
