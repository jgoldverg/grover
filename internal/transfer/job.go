package transfer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jgoldverg/grover/internal/metrics"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/tuning"
)

// job is the source-side runtime of one transfer. Byte counters are atomics
// so data paths never take the job lock; everything else is guarded by mu.
type job struct {
	id          string
	routeID     string
	sessionID   string
	destination string
	target      *target // nil for local copies
	counters    *metrics.Counters
	history     *jobHistory
	cancel      context.CancelFunc
	startedAt   time.Time

	mu         sync.Mutex
	cond       *sync.Cond
	tuning     tuning.Tuning
	state      pb.RuntimeState // RUNNING until finish publishes the outcome
	aborted    bool            // abort requested; no new files are scheduled
	finalizing bool            // outcome decided, final log not yet written; still reported as RUNNING
	err        string
	finishedAt time.Time
	files      []*fileRun
	next       int // next file index to schedule
	active     int
	done       int
	failed     int
	rate       rateSample // job-level, updated by the sampler
}

type fileRun struct {
	plan      FilePlan
	bytesDone atomic.Uint64
	// guarded by job.mu
	state   pb.RuntimeState
	err     string
	streams []*streamRun
}

type streamRun struct {
	id        uint32
	offset    uint64
	size      uint64
	bytesDone atomic.Uint64
	// guarded by job.mu
	state pb.RuntimeState
	err   string
	rate  rateSample
}

type rateSample struct {
	at    time.Time
	bytes uint64
	bps   float64
}

func (r *rateSample) update(now time.Time, bytes uint64) {
	if !r.at.IsZero() {
		if dt := now.Sub(r.at).Seconds(); dt > 0 && bytes >= r.bytes {
			r.bps = float64(bytes-r.bytes) / dt
		}
	}
	r.at, r.bytes = now, bytes
}

func newJob(id string, plans []FilePlan, t tuning.Tuning) *job {
	j := &job{
		id:        id,
		counters:  new(metrics.Counters),
		startedAt: time.Now(),
		tuning:    t,
		state:     pb.RuntimeState_RUNTIME_STATE_RUNNING,
		files:     make([]*fileRun, len(plans)),
	}
	j.cond = sync.NewCond(&j.mu)
	for i, p := range plans {
		j.files[i] = &fileRun{plan: p, state: pb.RuntimeState_RUNTIME_STATE_READY}
	}
	return j
}

// fileProgress implements Progress for one file of a job.
type fileProgress struct {
	j *job
	f *fileRun
}

func (p fileProgress) StartStream(id uint32, offset, size uint64) StreamProgress {
	s := &streamRun{id: id, offset: offset, size: size, state: pb.RuntimeState_RUNTIME_STATE_RUNNING}
	p.j.mu.Lock()
	p.f.streams = append(p.f.streams, s)
	p.j.mu.Unlock()
	return streamProgress{p.j, p.f, s}
}

type streamProgress struct {
	j *job
	f *fileRun
	s *streamRun
}

func (p streamProgress) Add(n int64) {
	if n >= 0 {
		p.s.bytesDone.Add(uint64(n))
		p.f.bytesDone.Add(uint64(n))
		return
	}
	p.s.bytesDone.Add(^uint64(-n - 1))
	p.f.bytesDone.Add(^uint64(-n - 1))
}

func (p streamProgress) Done(err error) {
	p.j.mu.Lock()
	defer p.j.mu.Unlock()
	p.s.rate.bps = 0
	if err != nil {
		p.s.state, p.s.err = pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error()
		return
	}
	p.s.state = pb.RuntimeState_RUNTIME_STATE_DONE
}

// sample refreshes current throughput for the job and its running streams.
// It runs on one ticker so readers never disturb the measurement.
func (j *job) sample(now time.Time) {
	var total uint64
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, f := range j.files {
		total += f.bytesDone.Load()
		if f.state != pb.RuntimeState_RUNTIME_STATE_RUNNING {
			continue
		}
		for _, s := range f.streams {
			if s.state == pb.RuntimeState_RUNTIME_STATE_RUNNING {
				s.rate.update(now, s.bytesDone.Load())
			}
		}
	}
	j.rate.update(now, total)
}

// snapshot is the externally visible state. A job is reported RUNNING until
// its final log is written, so DONE always implies final.json exists.
func (j *job) snapshot(view pb.FileView) *pb.Job { return j.render(view, false) }

func (j *job) render(view pb.FileView, final bool) *pb.Job {
	now := time.Now()
	c := j.counters.Snapshot()
	j.mu.Lock()
	defer j.mu.Unlock()
	out := &pb.Job{
		JobId:             j.id,
		RouteId:           j.routeID,
		SessionId:         j.sessionID,
		Role:              pb.EndpointRole_ENDPOINT_ROLE_SOURCE,
		State:             j.state,
		Destination:       j.destination,
		Tuning:            j.tuning.PB(),
		FilesTotal:        uint32(len(j.files)),
		FilesDone:         uint32(j.done),
		FilesFailed:       uint32(j.failed),
		FilesActive:       uint32(j.active),
		NetworkBytes:      c.BytesSent + c.BytesRetransmit,
		DiskReadBytes:     c.DiskRead,
		DiskWriteBytes:    c.DiskWrite,
		Retransmits:       c.Retransmits,
		RttMs:             c.RTTMs,
		CurrentBps:        j.rate.bps,
		StartedAtUnixNano: j.startedAt.UnixNano(),
		SampledAtUnixNano: now.UnixNano(),
		Error:             j.err,
	}
	if j.target != nil {
		out.Protocol = j.target.protocol
		out.DestinationData = j.target.data
	}
	hidden := j.finalizing && !final
	if hidden {
		out.State, out.Error = pb.RuntimeState_RUNTIME_STATE_RUNNING, ""
	}
	if !j.finishedAt.IsZero() && !hidden {
		out.FinishedAtUnixNano = j.finishedAt.UnixNano()
		out.CurrentBps = 0
	}
	for _, f := range j.files {
		out.BytesTotal += f.plan.Size
		out.BytesDone += f.bytesDone.Load()
		for _, s := range f.streams {
			if s.state == pb.RuntimeState_RUNTIME_STATE_RUNNING {
				out.StreamsActive++
			}
		}
		if includeFile(view, f.state) {
			out.Files = append(out.Files, f.pb())
		}
	}
	end := now
	if !j.finishedAt.IsZero() && !hidden {
		end = j.finishedAt
	}
	if secs := end.Sub(j.startedAt).Seconds(); secs > 0 {
		out.AverageBps = float64(out.BytesDone) / secs
	}
	return out
}

func includeFile(view pb.FileView, state pb.RuntimeState) bool {
	switch view {
	case pb.FileView_FILE_VIEW_ALL:
		return true
	case pb.FileView_FILE_VIEW_ACTIVE:
		return state == pb.RuntimeState_RUNTIME_STATE_RUNNING || state == pb.RuntimeState_RUNTIME_STATE_FAILED
	default:
		return false
	}
}

// pb renders a file; the caller holds job.mu.
func (f *fileRun) pb() *pb.FileState {
	out := &pb.FileState{
		SourcePath:   f.plan.SourcePath,
		RelativePath: f.plan.RelativePath,
		Size:         f.plan.Size,
		BytesDone:    f.bytesDone.Load(),
		State:        f.state,
		Error:        f.err,
	}
	for _, s := range f.streams {
		out.Streams = append(out.Streams, &pb.StreamState{
			StreamId:   s.id,
			Offset:     s.offset,
			Size:       s.size,
			BytesDone:  s.bytesDone.Load(),
			State:      s.state,
			CurrentBps: s.rate.bps,
			Error:      s.err,
		})
	}
	return out
}

func isTerminal(s pb.RuntimeState) bool {
	switch s {
	case pb.RuntimeState_RUNTIME_STATE_DONE, pb.RuntimeState_RUNTIME_STATE_FAILED, pb.RuntimeState_RUNTIME_STATE_ABORTED:
		return true
	}
	return false
}
