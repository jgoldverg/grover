package transfer

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/energy"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

// destinationReceiver owns receive-side progress and history independently of
// the source job scheduler. TCP and UDP handlers report into this component.
type destinationReceiver struct {
	mu        sync.RWMutex
	receives  map[string]*destinationReceiveRuntime
	jobLogDir string
	energy    *energy.RAPLMonitor
}

type destinationReceiveRuntime struct {
	mu               sync.Mutex
	jobID            string
	routeID          string
	protocol         pb.DataProtocol
	root             string
	startedAt        time.Time
	updatedAt        time.Time
	totalBytes       uint64
	receivedBytes    uint64
	activeStreams    int
	completedStreams int
	failedStreams    int
	files            map[string]uint64
	streams          map[string]*destinationReceiveStream
	history          *transferJobHistory
	finalizeTimer    *time.Timer
}

type destinationReceiveStream struct {
	RelativePath string          `json:"relative_path"`
	StreamID     uint32          `json:"stream_id"`
	Offset       uint64          `json:"offset"`
	Size         uint64          `json:"size"`
	BytesDone    uint64          `json:"bytes_done"`
	Remote       string          `json:"remote"`
	State        pb.RuntimeState `json:"state"`
	Error        string          `json:"error,omitempty"`
	StartedAt    time.Time       `json:"started_at"`
	FinishedAt   time.Time       `json:"finished_at,omitempty"`
}

// receiveStream describes a received file range, shared by start and completion.
type receiveStream struct {
	Job          JobIdentity
	Protocol     pb.DataProtocol
	Root         string
	RelativePath string
	TotalSize    uint64
	Range        StreamPlan
	Remote       string
}

type receiveResult struct {
	Stream    receiveStream
	BytesDone uint64
	State     pb.RuntimeState
	Error     string
}

func (m *destinationReceiver) beginDestinationReceive(receive receiveStream) {
	jobID, routeID := receive.Job.ID, receive.Job.RouteID
	protocol, root, relPath := receive.Protocol, receive.Root, receive.RelativePath
	totalSize, offset, size := receive.TotalSize, receive.Range.Offset, receive.Range.Size
	streamID, remote := receive.Range.StreamID, receive.Remote
	jobID = strings.TrimSpace(jobID)
	if jobID == "" || m == nil {
		return
	}
	now := time.Now()
	m.mu.Lock()
	runtime := m.receives[jobID]
	if runtime == nil {
		runtime = &destinationReceiveRuntime{
			jobID:     jobID,
			routeID:   strings.TrimSpace(routeID),
			protocol:  protocol,
			root:      root,
			startedAt: now,
			files:     make(map[string]uint64),
			streams:   make(map[string]*destinationReceiveStream),
		}
		if m.jobLogDir != "" {
			if history, err := newTransferJobHistory(m.jobLogDir, jobID, m.energy); err != nil {
				internal.Warn(jobLogMessage(jobID, "destination receive history disabled"), internal.Fields{
					internal.FieldError: err.Error(),
					"job_log_dir":       m.jobLogDir,
				})
			} else {
				runtime.history = history
				history.appendReceiveEnergy(jobID, runtime.routeID, now)
			}
		}
		m.receives[jobID] = runtime
	}
	m.mu.Unlock()

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.finalizeTimer != nil {
		runtime.finalizeTimer.Stop()
		runtime.finalizeTimer = nil
	}
	if runtime.routeID == "" {
		runtime.routeID = strings.TrimSpace(routeID)
	}
	if runtime.root == "" {
		runtime.root = root
	}
	runtime.updatedAt = now
	if _, ok := runtime.files[relPath]; !ok {
		runtime.files[relPath] = totalSize
		runtime.totalBytes += totalSize
	}
	key := destinationReceiveStreamKey(relPath, streamID, offset)
	runtime.streams[key] = &destinationReceiveStream{
		RelativePath: relPath,
		StreamID:     streamID,
		Offset:       offset,
		Size:         size,
		Remote:       remote,
		State:        pb.RuntimeState_RUNTIME_STATE_RUNNING,
		StartedAt:    now,
	}
	runtime.activeStreams++
	if runtime.history != nil {
		job := destinationReceiveSnapshotLocked(runtime)
		_ = runtime.history.writeReceiveManifest(job)
		runtime.history.appendSnapshot(job)
	}
}

func (m *destinationReceiver) finishDestinationReceive(result receiveResult) {
	jobID, relPath := result.Stream.Job.ID, result.Stream.RelativePath
	offset, streamID, remote := result.Stream.Range.Offset, result.Stream.Range.StreamID, result.Stream.Remote
	bytesDone, state, errText := result.BytesDone, result.State, result.Error
	jobID = strings.TrimSpace(jobID)
	if jobID == "" || m == nil {
		return
	}
	m.mu.RLock()
	runtime := m.receives[jobID]
	m.mu.RUnlock()
	if runtime == nil {
		return
	}
	now := time.Now()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.updatedAt = now
	key := destinationReceiveStreamKey(relPath, streamID, offset)
	stream := runtime.streams[key]
	if stream == nil {
		stream = &destinationReceiveStream{RelativePath: relPath, StreamID: streamID, Offset: offset, Remote: remote}
		runtime.streams[key] = stream
	}
	stream.BytesDone = bytesDone
	stream.State = state
	stream.Error = errText
	stream.FinishedAt = now
	if runtime.activeStreams > 0 {
		runtime.activeStreams--
	}
	if state == pb.RuntimeState_RUNTIME_STATE_DONE {
		runtime.completedStreams++
		runtime.receivedBytes += bytesDone
	} else {
		runtime.failedStreams++
	}
	if runtime.history != nil {
		runtime.history.appendSnapshot(destinationReceiveSnapshotLocked(runtime))
	}
	if runtime.activeStreams == 0 {
		if runtime.history != nil {
			runtime.history.appendReceiveEnergy(runtime.jobID, runtime.routeID, now)
		}
		runtime.finalizeTimer = time.AfterFunc(2*time.Second, func() {
			m.finalizeDestinationReceive(jobID)
		})
	}
}

func (m *destinationReceiver) finalizeDestinationReceives() {
	if m == nil {
		return
	}
	m.mu.RLock()
	ids := make([]string, 0, len(m.receives))
	for id := range m.receives {
		ids = append(ids, id)
	}
	m.mu.RUnlock()
	for _, id := range ids {
		m.finalizeDestinationReceive(id)
	}
}

func (m *destinationReceiver) finalizeDestinationReceive(jobID string) {
	m.mu.Lock()
	runtime := m.receives[jobID]
	if runtime != nil {
		delete(m.receives, jobID)
	}
	m.mu.Unlock()
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	if runtime.finalizeTimer != nil {
		runtime.finalizeTimer.Stop()
		runtime.finalizeTimer = nil
	}
	job := destinationReceiveSnapshotLocked(runtime)
	history := runtime.history
	runtime.mu.Unlock()
	if history != nil {
		history.appendReceiveEnergy(job.GetJobId(), job.GetRouteId(), time.Now())
		history.appendSnapshot(job)
		if err := history.writeFinal(job); err != nil {
			internal.Warn(jobLogMessage(jobID, "failed to write destination final log"), internal.Fields{
				internal.FieldError: err.Error(),
				"job_log_path":      history.dir,
			})
		}
		if err := history.close(); err != nil {
			internal.Warn(jobLogMessage(jobID, "failed to close destination receive log"), internal.Fields{
				internal.FieldError: err.Error(),
				"job_log_path":      history.dir,
			})
		}
	}
}

func destinationReceiveStreamKey(relPath string, streamID uint32, offset uint64) string {
	return fmt.Sprintf("%s:%d:%d", relPath, streamID, offset)
}

func destinationReceiveSnapshotLocked(runtime *destinationReceiveRuntime) *pb.TransferJob {
	if runtime == nil {
		return nil
	}
	files := make([]*pb.TransferFileState, 0, len(runtime.files))
	paths := make([]string, 0, len(runtime.files))
	for path := range runtime.files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	streamsByPath := make(map[string][]*pb.TransferStreamState)
	for _, stream := range runtime.streams {
		streamsByPath[stream.RelativePath] = append(streamsByPath[stream.RelativePath], &pb.TransferStreamState{
			StreamId:             stream.StreamID,
			Offset:               stream.Offset,
			Size:                 stream.Size,
			BytesDone:            stream.BytesDone,
			State:                stream.State,
			NetworkBytes:         stream.BytesDone,
			AverageThroughputBps: receiveStreamAverageBps(stream),
			ErrorMessage:         stream.Error,
		})
	}
	for _, path := range paths {
		streams := streamsByPath[path]
		sort.Slice(streams, func(i, j int) bool {
			if streams[i].GetOffset() == streams[j].GetOffset() {
				return streams[i].GetStreamId() < streams[j].GetStreamId()
			}
			return streams[i].GetOffset() < streams[j].GetOffset()
		})
		var done uint64
		for _, stream := range streams {
			done += stream.GetBytesDone()
		}
		state := pb.RuntimeState_RUNTIME_STATE_RUNNING
		if done >= runtime.files[path] && runtime.files[path] > 0 {
			state = pb.RuntimeState_RUNTIME_STATE_DONE
		}
		files = append(files, &pb.TransferFileState{
			Path:         path,
			RelativePath: path,
			Size:         runtime.files[path],
			BytesDone:    done,
			State:        state,
			Streams:      streams,
		})
	}
	state := pb.RuntimeState_RUNTIME_STATE_RUNNING
	if runtime.activeStreams == 0 {
		if runtime.failedStreams > 0 {
			state = pb.RuntimeState_RUNTIME_STATE_FAILED
		} else {
			state = pb.RuntimeState_RUNTIME_STATE_DONE
		}
	}
	elapsed := runtime.updatedAt.Sub(runtime.startedAt).Seconds()
	var avg float64
	if elapsed > 0 {
		avg = float64(runtime.receivedBytes) / elapsed
	}
	return &pb.TransferJob{
		JobId:              runtime.jobID,
		RouteId:            runtime.routeID,
		State:              state,
		Protocol:           runtime.protocol,
		Destination:        &pb.TransferEndpoint{Role: pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION, Protocol: runtime.protocol, RootPath: runtime.root},
		FilesInFlight:      uint32(runtime.activeStreams),
		StreamsPerFile:     uint32(len(runtime.streams)),
		Files:              files,
		GoodBytes:          runtime.receivedBytes,
		NetworkBytes:       runtime.receivedBytes,
		DiskWriteBytes:     runtime.receivedBytes,
		FilesDone:          uint32(len(files)),
		FilesActive:        uint32(runtime.activeStreams),
		StreamsActive:      uint32(runtime.activeStreams),
		ParallelismPerFile: uint32(len(runtime.streams)),
		Stats: &pb.StatsSnapshot{
			IngressBytes:         runtime.receivedBytes,
			EgressBytes:          runtime.receivedBytes,
			AverageThroughputBps: avg,
			ActiveStreams:        uint32(runtime.activeStreams),
			SampledAtUnixNano:    time.Now().UnixNano(),
		},
	}
}

func receiveStreamAverageBps(stream *destinationReceiveStream) float64 {
	if stream == nil || stream.StartedAt.IsZero() || stream.FinishedAt.IsZero() {
		return 0
	}
	if elapsed := stream.FinishedAt.Sub(stream.StartedAt).Seconds(); elapsed > 0 {
		return float64(stream.BytesDone) / elapsed
	}
	return 0
}
