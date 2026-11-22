package transfer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/jgoldverg/grover/internal/dataport"
	"github.com/jgoldverg/grover/internal/energy"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/tuning"
)

var (
	ErrNotFound = errors.New("not found")
	ErrInvalid  = errors.New("invalid request")
)

// finishedJobRetention is how long finished jobs stay queryable in memory.
// Job logs on disk keep them forever.
const finishedJobRetention = time.Hour

// Sessions is the part of the route-session store the Manager needs.
// Claim marks a session busy for one job; Release frees it.
type Sessions interface {
	Claim(sessionID, jobID string) (*pb.RouteSession, error)
	Release(sessionID, jobID string)
}

type Options struct {
	Ports       *dataport.Allocator
	Tuning      tuning.Tuning // node defaults, merged under each job's request
	JobLogDir   string        // empty disables job logs
	Energy      *energy.RAPLMonitor
	EnergyEvery time.Duration
	Sessions    Sessions
	Executor    FileExecutor // nil uses the built-in local/TCP/UDP executors
}

// Manager runs transfer jobs on this node (as source) and receives data for
// jobs running elsewhere (as destination).
type Manager struct {
	opts      Options
	endpoints *endpoints
	recv      *receiver
	nodeLog   *nodeEnergyLog

	mu   sync.Mutex
	jobs map[string]*job

	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

func NewManager(opts Options) (*Manager, error) {
	if opts.Ports == nil {
		p, err := dataport.New(dataport.Options{})
		if err != nil {
			return nil, err
		}
		opts.Ports = p
	}
	opts.Tuning = tuning.Defaults().Merge(opts.Tuning)
	if err := opts.Tuning.Validate(); err != nil {
		return nil, err
	}
	if opts.Executor == nil {
		opts.Executor = defaultExecutor()
	}
	if opts.EnergyEvery <= 0 {
		opts.EnergyEvery = time.Second
	}
	m := &Manager{opts: opts, jobs: map[string]*job{}, stop: make(chan struct{})}
	m.recv = newReceiver(opts.JobLogDir, opts.Energy)
	m.endpoints = newEndpoints(opts.Ports, m.recv, opts.Tuning)
	if opts.Energy != nil && opts.JobLogDir != "" {
		l, err := openNodeEnergyLog(opts.JobLogDir, opts.Energy)
		if err != nil {
			return nil, fmt.Errorf("open node energy log: %w", err)
		}
		m.nodeLog = l
	}
	m.wg.Add(1)
	go m.loop()
	return m, nil
}

// Close aborts running jobs, closes endpoints, and finalizes logs.
func (m *Manager) Close() {
	m.once.Do(func() {
		m.mu.Lock()
		for _, j := range m.jobs {
			j.cancel()
		}
		m.mu.Unlock()
		m.endpoints.closeAll()
		close(m.stop)
		m.wg.Wait()
		m.recv.closeAll()
		if m.nodeLog != nil {
			m.nodeLog.close()
		}
	})
}

func (m *Manager) PrepareEndpoint(req *pb.PrepareEndpointRequest) (*pb.TransferEndpoint, error) {
	ep, err := m.endpoints.Prepare(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return ep, nil
}

func (m *Manager) CloseEndpoint(id string) error {
	if !m.endpoints.Close(id) {
		return fmt.Errorf("endpoint %q: %w", id, ErrNotFound)
	}
	return nil
}

// StartJob validates and plans a job, then runs it in the background.
func (m *Manager) StartJob(ctx context.Context, req *pb.StartJobRequest) (*pb.Job, error) {
	reqTuning := tuning.FromPB(req.GetTuning())
	if err := reqTuning.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	dest := strings.TrimSpace(req.GetDestination())
	if !filepath.IsAbs(dest) {
		return nil, fmt.Errorf("%w: destination %q must be an absolute directory", ErrInvalid, dest)
	}
	plans, err := PlanSources(ctx, req.GetSources())
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	id := strings.TrimSpace(req.GetJobId())
	if id == "" {
		id = "job-" + time.Now().UTC().Format("20060102-150405") + "-" + uuid.NewString()[:8]
	}
	j := newJob(id, plans, m.opts.Tuning.Merge(reqTuning))
	j.destination = filepath.Clean(dest)

	m.mu.Lock()
	if _, exists := m.jobs[id]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: job %q already exists", ErrInvalid, id)
	}
	m.jobs[id] = j // reserve the id before claiming the session
	m.mu.Unlock()

	if sid := strings.TrimSpace(req.GetSessionId()); sid != "" {
		if err := m.attachSession(j, sid); err != nil {
			m.mu.Lock()
			delete(m.jobs, id)
			m.mu.Unlock()
			return nil, err
		}
	}

	runCtx, cancel := context.WithCancel(context.Background())
	j.cancel = cancel
	if h, err := openHistory(m.opts.JobLogDir, id, j.routeID, pb.EndpointRole_ENDPOINT_ROLE_SOURCE, m.opts.Energy); err != nil {
		slog.Warn("job log disabled", "job_id", id, "error", err)
	} else if h != nil {
		j.history = h
		h.writeManifest(j.manifest())
	}
	snap := j.snapshot(pb.FileView_FILE_VIEW_NONE)
	mode := "local"
	if j.target != nil {
		mode = strings.ToLower(strings.TrimPrefix(j.target.protocol.String(), "DATA_PROTOCOL_"))
	}
	slog.Info("job started", "job_id", id, "route_id", j.routeID, "session_id", j.sessionID,
		"mode", mode, "destination", j.destination, "files", len(plans),
		"bytes", snap.GetBytesTotal(), "concurrency", j.tuning.Concurrency, "parallelism", j.tuning.Parallelism)
	m.wg.Add(1)
	go m.run(runCtx, j)
	return snap, nil
}

func (m *Manager) attachSession(j *job, sid string) error {
	if m.opts.Sessions == nil {
		return fmt.Errorf("%w: this node has no session store", ErrInvalid)
	}
	s, err := m.opts.Sessions.Claim(sid, j.id)
	if err != nil {
		return err
	}
	t, err := m.targetFor(s)
	if err != nil {
		m.opts.Sessions.Release(sid, j.id)
		return err
	}
	j.sessionID, j.routeID, j.target = sid, s.GetRouteId(), t
	return nil
}

func (m *Manager) targetFor(s *pb.RouteSession) (*target, error) {
	t := &target{protocol: s.GetProtocol()}
	if s.GetConnectFrom() == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
		pool, err := m.endpoints.reversePool(s.GetSource().GetEndpointId())
		if err != nil {
			return nil, err
		}
		t.token, t.conns = s.GetSource().GetToken(), pool
		return t, nil
	}
	if s.GetData().GetPort() == 0 {
		return nil, fmt.Errorf("%w: session %s has no data endpoint", ErrInvalid, s.GetSessionId())
	}
	t.data, t.token = s.GetData(), s.GetDestination().GetToken()
	if t.protocol == pb.DataProtocol_DATA_PROTOCOL_TCP {
		t.conns = newDialPool(t.data, t.token)
	}
	return t, nil
}

func (m *Manager) run(ctx context.Context, j *job) {
	defer m.wg.Done()
	var wg sync.WaitGroup
	for {
		j.mu.Lock()
		for !j.aborted && j.next < len(j.files) && j.active >= int(max(j.tuning.Concurrency, 1)) {
			j.cond.Wait()
		}
		if j.aborted || j.next >= len(j.files) {
			j.mu.Unlock()
			break
		}
		f := j.files[j.next]
		j.next++
		j.active++
		f.state = pb.RuntimeState_RUNTIME_STATE_RUNNING
		task := &FileTask{
			JobID: j.id, RouteID: j.routeID, Plan: f.plan, Destination: j.destination,
			Target: j.target, Tuning: j.tuning, Counters: j.counters, Progress: fileProgress{j, f},
		}
		j.mu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := m.opts.Executor.TransferFile(ctx, task)
			j.mu.Lock()
			j.active--
			switch {
			case err == nil:
				f.state = pb.RuntimeState_RUNTIME_STATE_DONE
				j.done++
			case ctx.Err() != nil:
				f.state, f.err = pb.RuntimeState_RUNTIME_STATE_ABORTED, "aborted"
			default:
				f.state, f.err = pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error()
				j.failed++
				slog.Warn("file failed", "job_id", j.id, "path", f.plan.RelativePath, "error", err)
			}
			j.cond.Broadcast()
			j.mu.Unlock()
		}()
	}
	wg.Wait()
	m.finish(ctx, j)
}

// finish releases the job's resources first and only then publishes the
// terminal state, so a client that sees DONE/ABORTED can immediately reuse
// the session.
func (m *Manager) finish(ctx context.Context, j *job) {
	cancelled := ctx.Err() != nil // before our own cancel below
	j.cancel()
	if j.target != nil && j.target.conns != nil {
		j.target.conns.Close()
	}
	if j.sessionID != "" {
		m.opts.Sessions.Release(j.sessionID, j.id)
	}
	j.mu.Lock()
	switch {
	case j.aborted || cancelled:
		j.state = pb.RuntimeState_RUNTIME_STATE_ABORTED
		if j.err == "" {
			j.err = "aborted"
		}
	case j.failed > 0:
		j.state = pb.RuntimeState_RUNTIME_STATE_FAILED
		j.err = fmt.Sprintf("%d of %d files failed; first: %s", j.failed, len(j.files), firstFileError(j.files))
	default:
		j.state = pb.RuntimeState_RUNTIME_STATE_DONE
	}
	j.finishedAt = time.Now()
	j.finalizing = true
	j.mu.Unlock()
	final := j.render(pb.FileView_FILE_VIEW_ALL, true)
	j.history.finish(final)
	j.mu.Lock()
	j.finalizing = false
	j.mu.Unlock()
	slog.Info("job finished", "job_id", j.id, "route_id", j.routeID, "state", final.GetState().String(),
		"bytes", final.GetBytesDone(), "files_done", final.GetFilesDone(), "files_failed", final.GetFilesFailed(),
		"avg_mbps", fmt.Sprintf("%.1f", final.GetAverageBps()*8/1e6), "error", final.GetError())
}

func firstFileError(files []*fileRun) string {
	for _, f := range files {
		if f.state == pb.RuntimeState_RUNTIME_STATE_FAILED {
			return f.plan.RelativePath + ": " + f.err
		}
	}
	return ""
}

// GetJob returns this node's source job, or else its receive-side record.
func (m *Manager) GetJob(id string, view pb.FileView) (*pb.Job, error) {
	if j := m.lookup(id); j != nil {
		return j.snapshot(view), nil
	}
	if r := m.recv.get(id); r != nil {
		return r.snapshot(view), nil
	}
	return nil, fmt.Errorf("job %q: %w", id, ErrNotFound)
}

// ListJobs lists source jobs and receive records, newest first.
func (m *Manager) ListJobs(routeID string) []*pb.Job {
	m.mu.Lock()
	jobs := make([]*job, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	m.mu.Unlock()
	var out []*pb.Job
	for _, j := range jobs {
		if routeID == "" || j.routeID == routeID {
			out = append(out, j.snapshot(pb.FileView_FILE_VIEW_NONE))
		}
	}
	for _, r := range m.recv.list() {
		if routeID == "" || r.routeID == routeID {
			out = append(out, r.snapshot(pb.FileView_FILE_VIEW_NONE))
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].GetStartedAtUnixNano() > out[b].GetStartedAtUnixNano() })
	return out
}

func (m *Manager) AbortJob(id string) (*pb.Job, error) {
	j := m.lookup(id)
	if j == nil {
		return nil, fmt.Errorf("job %q: %w", id, ErrNotFound)
	}
	j.mu.Lock()
	if j.finishedAt.IsZero() && !j.aborted {
		j.aborted, j.err = true, "aborted by request"
	}
	j.cond.Broadcast()
	j.mu.Unlock()
	j.cancel()
	return j.snapshot(pb.FileView_FILE_VIEW_NONE), nil
}

// TuneJob changes tuning for files that start after the call. Concurrency
// changes apply immediately to scheduling.
func (m *Manager) TuneJob(id string, t tuning.Tuning) (*pb.Job, error) {
	if err := t.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	j := m.lookup(id)
	if j == nil {
		return nil, fmt.Errorf("job %q: %w", id, ErrNotFound)
	}
	j.mu.Lock()
	j.tuning = j.tuning.Merge(t)
	j.cond.Broadcast()
	j.mu.Unlock()
	slog.Info("job tuned", "job_id", id, "concurrency", t.Concurrency, "parallelism", t.Parallelism)
	return j.snapshot(pb.FileView_FILE_VIEW_NONE), nil
}

// DeletePath removes one regular file (used to clean up synthetic output).
func (m *Manager) DeletePath(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%w: path must be absolute", ErrInvalid)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: %s is not a regular file", ErrInvalid, path)
	}
	return os.Remove(path)
}

func (m *Manager) lookup(id string) *job {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs[strings.TrimSpace(id)]
}

// loop samples rates, appends history snapshots and energy, and evicts old jobs.
func (m *Manager) loop() {
	defer m.wg.Done()
	tick := time.NewTicker(time.Second)
	energyTick := time.NewTicker(m.opts.EnergyEvery)
	defer tick.Stop()
	defer energyTick.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-tick.C:
			for _, j := range m.running() {
				j.sample(now)
				j.history.appendSnapshot(j.snapshot(pb.FileView_FILE_VIEW_NONE))
			}
			m.evict(now)
		case now := <-energyTick.C:
			var ids, routes []string
			for _, j := range m.running() {
				j.history.sampleEnergy(now)
				ids, routes = append(ids, j.id), append(routes, j.routeID)
			}
			if m.nodeLog != nil {
				m.nodeLog.append(now, ids, routes)
			}
		}
	}
}

func (m *Manager) running() []*job {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*job
	for _, j := range m.jobs {
		j.mu.Lock()
		if j.finishedAt.IsZero() {
			out = append(out, j)
		}
		j.mu.Unlock()
	}
	return out
}

func (m *Manager) evict(now time.Time) {
	m.mu.Lock()
	for id, j := range m.jobs {
		j.mu.Lock()
		old := !j.finishedAt.IsZero() && now.Sub(j.finishedAt) > finishedJobRetention
		j.mu.Unlock()
		if old {
			delete(m.jobs, id)
		}
	}
	m.mu.Unlock()
	m.recv.evict(finishedJobRetention)
}

func (j *job) manifest() *pb.Manifest {
	m := &pb.Manifest{
		JobId: j.id, RouteId: j.routeID, SessionId: j.sessionID, Role: pb.EndpointRole_ENDPOINT_ROLE_SOURCE,
		Destination: j.destination, Tuning: j.tuning.PB(), FilesTotal: uint32(len(j.files)),
		CreatedAtUnixNano: j.startedAt.UnixNano(),
	}
	if j.target != nil {
		m.Protocol = j.target.protocol
	}
	for _, f := range j.files {
		m.BytesTotal += f.plan.Size
		m.Files = append(m.Files, &pb.ManifestFile{SourcePath: f.plan.SourcePath, RelativePath: f.plan.RelativePath, Size: f.plan.Size})
	}
	return m
}
