package transfer

import (
	"encoding/csv"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/jgoldverg/grover/internal/energy"
	"github.com/jgoldverg/grover/internal/pb"
)

// Job log layout, one directory per job and role:
//
//	<job_log_dir>/<job_id>/        source side
//	<job_log_dir>/<job_id>.dest/   destination side
//	    manifest.json    pb.Manifest, written at start
//	    snapshots.jsonl  one pb.Job per line, every second (no file list)
//	    final.json       pb.Job with every file, written at the end
//	    energy.csv       RAPL samples while the job ran (with --energy-monitor)
const (
	ManifestFile  = "manifest.json"
	SnapshotsFile = "snapshots.jsonl"
	FinalFile     = "final.json"
	EnergyFile    = "energy.csv"
	destDirSuffix = ".dest"
)

// JobLogDir returns the log directory for a job id and role.
func JobLogDir(base, jobID string, role pb.EndpointRole) string {
	name := SafeName(jobID)
	if role == pb.EndpointRole_ENDPOINT_ROLE_DESTINATION {
		name += destDirSuffix
	}
	return filepath.Join(base, name)
}

// SafeName maps an id to a filesystem-safe name.
func SafeName(id string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(id) {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 || b.String() == "." || b.String() == ".." {
		return "unknown"
	}
	return b.String()
}

type jobHistory struct {
	dir       string
	mu        sync.Mutex
	snapshots *os.File
	energyF   *os.File
	energyW   *csv.Writer
	monitor   *energy.RAPLMonitor
	tick      uint64
	jobID     string
	routeID   string
}

// openHistory creates the job log directory. It returns nil, nil when logging
// is disabled (empty base).
func openHistory(base, jobID, routeID string, role pb.EndpointRole, monitor *energy.RAPLMonitor) (*jobHistory, error) {
	if strings.TrimSpace(base) == "" {
		return nil, nil
	}
	dir := JobLogDir(base, jobID, role)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	snaps, err := os.OpenFile(filepath.Join(dir, SnapshotsFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	h := &jobHistory{dir: dir, snapshots: snaps, monitor: monitor, jobID: jobID, routeID: routeID}
	if monitor != nil {
		f, err := os.OpenFile(filepath.Join(dir, EnergyFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			_ = snaps.Close()
			return nil, err
		}
		h.energyF, h.energyW = f, csv.NewWriter(f)
		if err := monitor.WriteCSVHeader(h.energyW); err != nil {
			_ = snaps.Close()
			_ = f.Close()
			return nil, err
		}
	}
	return h, nil
}

func (h *jobHistory) writeManifest(m *pb.Manifest) {
	if h == nil {
		return
	}
	if err := writeProtoFile(filepath.Join(h.dir, ManifestFile), m); err != nil {
		slog.Warn("write job manifest failed", "job_id", h.jobID, "dir", h.dir, "error", err)
	}
}

func (h *jobHistory) appendSnapshot(j *pb.Job) {
	if h == nil {
		return
	}
	b, err := protojson.Marshal(j)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.snapshots != nil {
		_, _ = h.snapshots.Write(append(b, '\n'))
	}
}

func (h *jobHistory) sampleEnergy(now time.Time) {
	if h == nil || h.monitor == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.energyW == nil {
		return
	}
	if err := h.monitor.WriteCSVRecord(h.energyW, h.tick, h.jobID, h.routeID, now); err != nil {
		slog.Warn("energy sample failed", "job_id", h.jobID, "error", err)
		return
	}
	h.tick++
}

// finish writes final.json and closes the log files.
func (h *jobHistory) finish(final *pb.Job) {
	if h == nil {
		return
	}
	h.sampleEnergy(time.Now())
	h.appendSnapshot(withoutFiles(final))
	if err := writeProtoFile(filepath.Join(h.dir, FinalFile), final); err != nil {
		slog.Warn("write final job log failed", "job_id", h.jobID, "dir", h.dir, "error", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var errs []error
	if h.snapshots != nil {
		errs = append(errs, h.snapshots.Close())
		h.snapshots = nil
	}
	if h.energyW != nil {
		h.energyW.Flush()
		errs = append(errs, h.energyW.Error(), h.energyF.Close())
		h.energyW, h.energyF = nil, nil
	}
	if err := errors.Join(errs...); err != nil {
		slog.Warn("close job log failed", "job_id", h.jobID, "dir", h.dir, "error", err)
	}
}

func withoutFiles(j *pb.Job) *pb.Job {
	c := proto.Clone(j).(*pb.Job)
	c.Files = nil
	return c
}

func writeProtoFile(path string, m proto.Message) error {
	b, err := protojson.MarshalOptions{Multiline: true, EmitUnpopulated: true}.Marshal(m)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// nodeEnergyLog is the continuous <job_log_dir>/energy.csv used for
// baseline-vs-job analysis. Idle rows have empty job_id/route_id. It appends
// across restarts so earlier experiment data is never truncated.
type nodeEnergyLog struct {
	path    string
	mu      sync.Mutex
	file    *os.File
	writer  *csv.Writer
	monitor *energy.RAPLMonitor
	tick    uint64
}

func openNodeEnergyLog(base string, monitor *energy.RAPLMonitor) (*nodeEnergyLog, error) {
	if err := os.MkdirAll(base, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(base, EnergyFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	l := &nodeEnergyLog{path: path, file: f, writer: csv.NewWriter(f), monitor: monitor}
	if info, err := f.Stat(); err == nil && info.Size() == 0 {
		header := append([]string{"timestamp_ns", "tick", "active_job_count", "job_id", "route_id"}, monitor.EnergyColumnNames()...)
		if err := l.writer.Write(header); err != nil {
			_ = f.Close()
			return nil, err
		}
		l.writer.Flush()
	}
	return l, nil
}

func (l *nodeEnergyLog) append(now time.Time, jobIDs, routeIDs []string) {
	values, sumAll, total, err := l.monitor.ReadEnergyValues()
	if err != nil {
		slog.Warn("node energy sample failed", "path", l.path, "error", err)
		return
	}
	rec := []string{
		itoa(uint64(now.UnixNano())), itoa(l.tick), itoa(uint64(len(jobIDs))),
		strings.Join(jobIDs, ";"), strings.Join(routeIDs, ";"),
	}
	for _, v := range values {
		rec = append(rec, itoa(v))
	}
	rec = append(rec, itoa(sumAll), itoa(total))
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writer == nil {
		return
	}
	if err := l.writer.Write(rec); err != nil {
		slog.Warn("node energy write failed", "path", l.path, "error", err)
		return
	}
	l.writer.Flush()
	l.tick++
}

func (l *nodeEnergyLog) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writer != nil {
		l.writer.Flush()
		_ = l.file.Close()
		l.writer = nil
	}
}
