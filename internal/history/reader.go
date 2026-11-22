// Package history reads the job logs written by package transfer (see
// transfer.JobLogDir for the layout). It is read-only; groverd serves it over
// HistoryControl so experiment tooling can pull logs from any node.
package history

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/transfer"
)

var ErrNotFound = errors.New("job history not found")

type Reader struct{ dir string }

func NewReader(dir string) *Reader { return &Reader{dir: dir} }

// List returns history entries, newest first.
func (r *Reader) List(req *pb.ListHistoryRequest) ([]*pb.HistoryEntry, error) {
	if r.dir == "" {
		return nil, errors.New("job logs are disabled on this node (job_log_dir is empty)")
	}
	dirs, err := os.ReadDir(r.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []*pb.HistoryEntry
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		rec, err := r.read(filepath.Join(r.dir, d.Name()), false)
		if err != nil {
			continue // not a job dir, or a job that is still being written
		}
		e := rec.Entry
		if (req.GetRouteId() != "" && e.RouteId != req.GetRouteId()) ||
			(req.GetSinceUnixNano() > 0 && e.StartedAtUnixNano < req.GetSinceUnixNano()) ||
			(req.GetUntilUnixNano() > 0 && e.StartedAtUnixNano > req.GetUntilUnixNano()) {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAtUnixNano > out[j].StartedAtUnixNano })
	if n := int(req.GetLimit()); n > 0 && len(out) > n {
		out = out[:n]
	}
	return out, nil
}

// Get returns the manifest, final job, and summary for one job and role.
func (r *Reader) Get(jobID string, role pb.EndpointRole) (*pb.HistoryRecord, error) {
	dir, err := r.jobDir(jobID, role)
	if err != nil {
		return nil, err
	}
	return r.read(dir, true)
}

// Snapshots returns up to limit snapshots (all when 0).
func (r *Reader) Snapshots(jobID string, role pb.EndpointRole, limit uint32) ([]*pb.Job, error) {
	dir, err := r.jobDir(jobID, role)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(dir, transfer.SnapshotsFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []*pb.Job
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var j pb.Job
		if err := protojson.Unmarshal([]byte(line), &j); err != nil {
			continue
		}
		out = append(out, &j)
		if limit > 0 && len(out) >= int(limit) {
			break
		}
	}
	return out, sc.Err()
}

// Energy returns the raw energy.csv header and rows.
func (r *Reader) Energy(jobID string, role pb.EndpointRole, limit uint32) (*pb.EnergyCSV, error) {
	dir, err := r.jobDir(jobID, role)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(dir, transfer.EnergyFile))
	if errors.Is(err, fs.ErrNotExist) {
		return &pb.EnergyCSV{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := &pb.EnergyCSV{}
	sc := bufio.NewScanner(f)
	if sc.Scan() {
		out.Header = sc.Text()
	}
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			out.Rows = append(out.Rows, line)
			if limit > 0 && len(out.Rows) >= int(limit) {
				break
			}
		}
	}
	return out, sc.Err()
}

func (r *Reader) jobDir(jobID string, role pb.EndpointRole) (string, error) {
	if strings.TrimSpace(jobID) == "" {
		return "", errors.New("job id is required")
	}
	if role == pb.EndpointRole_ENDPOINT_ROLE_UNSPECIFIED {
		role = pb.EndpointRole_ENDPOINT_ROLE_SOURCE
	}
	dir := transfer.JobLogDir(r.dir, jobID, role)
	if _, err := os.Stat(filepath.Join(dir, transfer.ManifestFile)); err != nil {
		return "", fmt.Errorf("%s (%s) in %s: %w", jobID, strings.ToLower(strings.TrimPrefix(role.String(), "ENDPOINT_ROLE_")), r.dir, ErrNotFound)
	}
	return dir, nil
}

func (r *Reader) read(dir string, full bool) (*pb.HistoryRecord, error) {
	var m pb.Manifest
	if err := readProto(filepath.Join(dir, transfer.ManifestFile), &m); err != nil {
		return nil, err
	}
	e := &pb.HistoryEntry{
		JobId: m.JobId, RouteId: m.RouteId, Role: m.Role, BytesTotal: m.BytesTotal,
		StartedAtUnixNano: m.CreatedAtUnixNano, Dir: dir, State: pb.RuntimeState_RUNTIME_STATE_RUNNING,
	}
	var final *pb.Job
	var fj pb.Job
	if err := readProto(filepath.Join(dir, transfer.FinalFile), &fj); err == nil {
		final = &fj
		e.State, e.Error = fj.State, fj.Error
		e.BytesDone = fj.BytesDone
		if e.BytesTotal == 0 {
			e.BytesTotal = fj.BytesTotal
		}
		if fj.FinishedAtUnixNano > fj.StartedAtUnixNano && fj.StartedAtUnixNano > 0 {
			e.DurationSeconds = float64(fj.FinishedAtUnixNano-fj.StartedAtUnixNano) / 1e9
			e.ThroughputBps = float64(fj.BytesDone) / e.DurationSeconds
		}
	}
	e.EnergyJoules = energyJoules(filepath.Join(dir, transfer.EnergyFile))
	rec := &pb.HistoryRecord{Entry: e}
	if full {
		rec.Manifest, rec.Final = &m, final
	}
	return rec, nil
}

func readProto(path string, m proto.Message) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, m)
}

// energyJoules sums increases of energy_uj_total (falling back to
// energy_uj_sum_all) across rows. Intervals where a RAPL counter wrapped are
// skipped rather than turning the whole job into zero.
func energyJoules(path string) float64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return 0
	}
	col := -1
	for _, name := range []string{"energy_uj_total", "energy_uj_sum_all"} {
		for i, h := range header {
			if strings.TrimSpace(h) == name {
				col = i
				break
			}
		}
		if col >= 0 {
			break
		}
	}
	if col < 0 {
		return 0
	}
	var total, prev float64
	havePrev := false
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || col >= len(rec) {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rec[col]), 64)
		if err != nil {
			continue
		}
		if havePrev && v >= prev {
			total += v - prev
		}
		prev, havePrev = v, true
	}
	return total / 1e6
}
