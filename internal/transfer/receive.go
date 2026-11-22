package transfer

import (
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jgoldverg/grover/internal/energy"
	"github.com/jgoldverg/grover/internal/metrics"
	"github.com/jgoldverg/grover/internal/pb"
)

// receiveIdleFinalize is how long a destination job must be idle (no active
// streams) before its log is finalized. More ranges for the same job id
// arriving later reopen nothing; they start a new record.
const receiveIdleFinalize = 5 * time.Second

// receiver tracks destination-side progress per job id. It knows nothing
// about scheduling; TCP and UDP receive paths report into it.
type receiver struct {
	logDir string
	energy *energy.RAPLMonitor

	mu   sync.Mutex
	jobs map[string]*receiveJob
}

type receiveJob struct {
	id        string
	routeID   string
	protocol  pb.DataProtocol
	dest      string
	startedAt time.Time
	counters  *metrics.Counters
	history   *jobHistory
	bytes     atomic.Uint64

	mu         sync.Mutex
	files      map[string]*receiveFile
	active     int
	failed     int
	lastErr    string
	finishedAt time.Time
	finalize   *time.Timer
	rate       rateSample
	stopLoop   chan struct{}
}

type receiveFile struct {
	rel       string
	size      uint64
	bytesDone atomic.Uint64
	streams   []*receiveStream
}

type receiveStream struct {
	id, offset, size uint64
	bytesDone        atomic.Uint64
	state            pb.RuntimeState
	err              string
}

// receiveInfo identifies one incoming range or UDP file session.
type receiveInfo struct {
	JobID, RouteID, Destination, RelativePath string
	Protocol                                  pb.DataProtocol
	FileSize                                  uint64
	StreamID                                  uint32
	Offset, Length                            uint64
}

// receiveHandle reports progress for one range; Done must be called once.
type receiveHandle struct {
	r *receiver
	j *receiveJob
	f *receiveFile
	s *receiveStream
}

func newReceiver(logDir string, monitor *energy.RAPLMonitor) *receiver {
	return &receiver{logDir: logDir, energy: monitor, jobs: make(map[string]*receiveJob)}
}

func (r *receiver) begin(info receiveInfo) *receiveHandle {
	id := info.JobID
	if id == "" {
		id = "unknown"
	}
	r.mu.Lock()
	j := r.jobs[id]
	if j == nil || !j.finishedAt.IsZero() {
		j = &receiveJob{
			id: id, routeID: info.RouteID, protocol: info.Protocol, dest: info.Destination,
			startedAt: time.Now(), counters: new(metrics.Counters), files: map[string]*receiveFile{},
			stopLoop: make(chan struct{}),
		}
		if h, err := openHistory(r.logDir, id, info.RouteID, pb.EndpointRole_ENDPOINT_ROLE_DESTINATION, r.energy); err == nil {
			j.history = h
			h.writeManifest(&pb.Manifest{
				JobId: id, RouteId: info.RouteID, Role: pb.EndpointRole_ENDPOINT_ROLE_DESTINATION,
				Protocol: info.Protocol, Destination: info.Destination, CreatedAtUnixNano: j.startedAt.UnixNano(),
			})
		}
		r.jobs[id] = j
		go j.loop()
	}
	r.mu.Unlock()

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.finalize != nil {
		j.finalize.Stop()
		j.finalize = nil
	}
	f := j.files[info.RelativePath]
	if f == nil {
		f = &receiveFile{rel: info.RelativePath, size: info.FileSize}
		j.files[info.RelativePath] = f
	}
	s := &receiveStream{id: uint64(info.StreamID), offset: info.Offset, size: info.Length, state: pb.RuntimeState_RUNTIME_STATE_RUNNING}
	f.streams = append(f.streams, s)
	j.active++
	return &receiveHandle{r: r, j: j, f: f, s: s}
}

func (h *receiveHandle) Add(n int) {
	h.s.bytesDone.Add(uint64(n))
	h.f.bytesDone.Add(uint64(n))
	h.j.bytes.Add(uint64(n))
	h.j.counters.ObserveDiskWrite(n)
}

func (h *receiveHandle) Done(err error) {
	j := h.j
	j.mu.Lock()
	defer j.mu.Unlock()
	h.s.state = pb.RuntimeState_RUNTIME_STATE_DONE
	if err != nil {
		h.s.state, h.s.err = pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error()
		j.failed++
		j.lastErr = err.Error()
	}
	j.active--
	if j.active == 0 {
		j.finalize = time.AfterFunc(receiveIdleFinalize, func() { h.r.finish(j) })
	}
}

// loop samples throughput and appends snapshots/energy while the job is open.
func (j *receiveJob) loop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-j.stopLoop:
			return
		case now := <-t.C:
			j.mu.Lock()
			j.rate.update(now, j.bytes.Load())
			j.mu.Unlock()
			j.history.appendSnapshot(j.snapshot(pb.FileView_FILE_VIEW_NONE))
			j.history.sampleEnergy(now)
		}
	}
}

func (r *receiver) finish(j *receiveJob) {
	j.mu.Lock()
	if j.active > 0 || !j.finishedAt.IsZero() {
		j.mu.Unlock()
		return
	}
	j.finishedAt = time.Now()
	close(j.stopLoop)
	j.mu.Unlock()
	j.history.finish(j.snapshot(pb.FileView_FILE_VIEW_ALL))
}

func (r *receiver) get(id string) *receiveJob {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.jobs[id]
}

func (r *receiver) list() []*receiveJob {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*receiveJob, 0, len(r.jobs))
	for _, j := range r.jobs {
		out = append(out, j)
	}
	return out
}

// evict drops finished records older than keep.
func (r *receiver) evict(keep time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, j := range r.jobs {
		j.mu.Lock()
		old := !j.finishedAt.IsZero() && time.Since(j.finishedAt) > keep
		j.mu.Unlock()
		if old {
			delete(r.jobs, id)
		}
	}
}

// closeAll finalizes every open record (server shutdown).
func (r *receiver) closeAll() {
	for _, j := range r.list() {
		j.mu.Lock()
		if j.finalize != nil {
			j.finalize.Stop()
		}
		j.active = 0
		j.mu.Unlock()
		r.finish(j)
	}
}

func (j *receiveJob) snapshot(view pb.FileView) *pb.Job {
	now := time.Now()
	c := j.counters.Snapshot()
	j.mu.Lock()
	defer j.mu.Unlock()
	state := pb.RuntimeState_RUNTIME_STATE_RUNNING
	if !j.finishedAt.IsZero() || j.active == 0 {
		state = pb.RuntimeState_RUNTIME_STATE_DONE
		if j.failed > 0 {
			state = pb.RuntimeState_RUNTIME_STATE_FAILED
		}
	}
	out := &pb.Job{
		JobId:             j.id,
		RouteId:           j.routeID,
		Role:              pb.EndpointRole_ENDPOINT_ROLE_DESTINATION,
		State:             state,
		Protocol:          j.protocol,
		Destination:       j.dest,
		FilesTotal:        uint32(len(j.files)),
		FilesActive:       0,
		StreamsActive:     uint32(j.active),
		BytesDone:         j.bytes.Load(),
		NetworkBytes:      c.NetworkReceived,
		DiskWriteBytes:    c.DiskWrite,
		CurrentBps:        j.rate.bps,
		StartedAtUnixNano: j.startedAt.UnixNano(),
		SampledAtUnixNano: now.UnixNano(),
		Error:             j.lastErr,
	}
	end := now
	if !j.finishedAt.IsZero() {
		out.FinishedAtUnixNano = j.finishedAt.UnixNano()
		out.CurrentBps = 0
		end = j.finishedAt
	}
	if secs := end.Sub(j.startedAt).Seconds(); secs > 0 {
		out.AverageBps = float64(out.BytesDone) / secs
	}
	rels := make([]string, 0, len(j.files))
	for rel := range j.files {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	for _, rel := range rels {
		f := j.files[rel]
		fs := &pb.FileState{RelativePath: rel, Size: f.size, BytesDone: f.bytesDone.Load(), State: pb.RuntimeState_RUNTIME_STATE_DONE}
		out.BytesTotal += f.size
		for _, s := range f.streams {
			switch s.state {
			case pb.RuntimeState_RUNTIME_STATE_RUNNING:
				fs.State = pb.RuntimeState_RUNTIME_STATE_RUNNING
			case pb.RuntimeState_RUNTIME_STATE_FAILED:
				if fs.State != pb.RuntimeState_RUNTIME_STATE_RUNNING {
					fs.State, fs.Error = pb.RuntimeState_RUNTIME_STATE_FAILED, s.err
				}
			}
			fs.Streams = append(fs.Streams, &pb.StreamState{
				StreamId: uint32(s.id), Offset: s.offset, Size: s.size,
				BytesDone: s.bytesDone.Load(), State: s.state, Error: s.err,
			})
		}
		switch fs.State {
		case pb.RuntimeState_RUNTIME_STATE_RUNNING:
			out.FilesActive++
		case pb.RuntimeState_RUNTIME_STATE_FAILED:
			out.FilesFailed++
		default:
			out.FilesDone++
		}
		if includeFile(view, fs.State) {
			out.Files = append(out.Files, fs)
		}
	}
	return out
}

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }
