package transfer

import (
	"context"
	"sync"
	"time"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/metrics"
)

type transferJobRuntime struct {
	mu     sync.Mutex
	cond   *sync.Cond
	cancel context.CancelFunc

	jobID     string
	sessionID string
	routeID   string
	protocol  pb.DataProtocol
	source    *pb.TransferEndpoint
	dest      *pb.TransferEndpoint
	origin    pb.ConnectionOrigin

	state          pb.RuntimeState
	filesInFlight  uint32
	streamsPerFile uint32
	chunkSizeBytes uint64
	tcpBufferBytes uint64
	udpMTUBytes    uint32
	udpWindow      uint32
	udpBatch       uint32
	udpAckPackets  uint32
	udpAckMs       uint32
	udpSocketRead  uint64
	udpSocketWrite uint64
	udpFlowControl string
	errorMessage   string
	startedAt      time.Time
	collector      *metrics.TransferCollector
	history        *transferJobHistory

	files     []*pb.TransferFileState
	nextIndex int
	active    uint32
	doneCount uint32

	streamStartedAt map[transferStreamKey]time.Time
	streamLastAt    map[transferStreamKey]time.Time
	streamSampleAt  map[transferStreamKey]time.Time
	streamSample    map[transferStreamKey]uint64
}

type transferStreamKey struct {
	fileIndex int
	streamID  uint32
}

func (r *transferJobRuntime) addProgress(index int, n uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index >= 0 && index < len(r.files) {
		r.files[index].BytesDone += n
	}
}

func (r *transferJobRuntime) startStream(index int, plan StreamPlan) {
	if plan.StreamID == 0 {
		plan.StreamID = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if index < 0 || index >= len(r.files) {
		return
	}
	streams := r.files[index].Streams
	for _, stream := range streams {
		if stream.GetStreamId() == plan.StreamID {
			stream.State = pb.RuntimeState_RUNTIME_STATE_RUNNING
			stream.Offset = plan.Offset
			stream.Size = plan.Size
			stream.ErrorMessage = ""
			key := transferStreamKey{fileIndex: index, streamID: plan.StreamID}
			now := time.Now()
			r.streamStartedAt[key] = now
			r.streamLastAt[key] = now
			r.streamSampleAt[key] = now
			r.streamSample[key] = stream.GetBytesDone()
			return
		}
	}
	now := time.Now()
	key := transferStreamKey{fileIndex: index, streamID: plan.StreamID}
	r.files[index].Streams = append(r.files[index].Streams, &pb.TransferStreamState{
		StreamId: plan.StreamID,
		Offset:   plan.Offset,
		Size:     plan.Size,
		State:    pb.RuntimeState_RUNTIME_STATE_RUNNING,
	})
	r.streamStartedAt[key] = now
	r.streamLastAt[key] = now
	r.streamSampleAt[key] = now
	r.streamSample[key] = 0
}

func (r *transferJobRuntime) addStreamProgress(index int, streamID uint32, n uint64) {
	if streamID == 0 {
		streamID = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if index < 0 || index >= len(r.files) {
		return
	}
	stream := r.findOrCreateStreamLocked(index, streamID)
	if stream == nil {
		return
	}
	now := time.Now()
	key := transferStreamKey{fileIndex: index, streamID: streamID}
	startedAt := r.streamStartedAt[key]
	if startedAt.IsZero() {
		startedAt = now
		r.streamStartedAt[key] = now
	}
	stream.BytesDone += n
	stream.NetworkBytes += n
	if elapsed := now.Sub(startedAt).Seconds(); elapsed > 0 {
		stream.AverageThroughputBps = float64(stream.BytesDone) / elapsed
	}
	stream.State = pb.RuntimeState_RUNTIME_STATE_RUNNING
	r.streamLastAt[key] = now
}

func (r *transferJobRuntime) finishStream(index int, streamID uint32, state pb.RuntimeState, errText string) {
	if streamID == 0 {
		streamID = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if index < 0 || index >= len(r.files) {
		return
	}
	stream := r.findOrCreateStreamLocked(index, streamID)
	if stream == nil {
		return
	}
	stream.State = state
	stream.ErrorMessage = errText
	key := transferStreamKey{fileIndex: index, streamID: streamID}
	if startedAt := r.streamStartedAt[key]; !startedAt.IsZero() {
		if elapsed := time.Since(startedAt).Seconds(); elapsed > 0 {
			stream.AverageThroughputBps = float64(stream.BytesDone) / elapsed
		}
	}
}

func (r *transferJobRuntime) findOrCreateStreamLocked(index int, streamID uint32) *pb.TransferStreamState {
	for _, stream := range r.files[index].Streams {
		if stream.GetStreamId() == streamID {
			return stream
		}
	}
	now := time.Now()
	key := transferStreamKey{fileIndex: index, streamID: streamID}
	stream := &pb.TransferStreamState{
		StreamId: streamID,
		State:    pb.RuntimeState_RUNTIME_STATE_RUNNING,
	}
	r.files[index].Streams = append(r.files[index].Streams, stream)
	r.streamStartedAt[key] = now
	r.streamLastAt[key] = now
	r.streamSampleAt[key] = now
	r.streamSample[key] = 0
	return stream
}

func (r *transferJobRuntime) finishFile(index int, state pb.RuntimeState, errText string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index >= 0 && index < len(r.files) {
		r.files[index].State = state
		r.files[index].ErrorMessage = errText
		for _, stream := range r.files[index].Streams {
			if stream.GetState() == pb.RuntimeState_RUNTIME_STATE_RUNNING {
				stream.State = state
				stream.ErrorMessage = errText
			}
		}
		if state == pb.RuntimeState_RUNTIME_STATE_DONE {
			r.doneCount++
		}
	}
	if r.active > 0 {
		r.active--
	}
	r.cond.Broadcast()
}

func (r *transferJobRuntime) snapshot() *pb.TransferJob {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	files := make([]*pb.TransferFileState, 0, len(r.files))
	var goodBytes uint64
	var observedBytes uint64
	var activeStreams uint32
	for _, f := range r.files {
		cp := *f
		if len(f.Streams) > 0 {
			cp.Streams = make([]*pb.TransferStreamState, 0, len(f.Streams))
			for _, stream := range f.Streams {
				if stream.GetState() == pb.RuntimeState_RUNTIME_STATE_RUNNING {
					activeStreams++
					r.sampleStreamRateLocked(now, f, stream)
				} else {
					stream.CurrentThroughputBps = 0
				}
				streamCopy := *stream
				cp.Streams = append(cp.Streams, &streamCopy)
			}
		}
		files = append(files, &cp)
		observedBytes += f.BytesDone
		goodBytes += f.BytesDone
	}
	elapsed := time.Since(r.startedAt).Seconds()
	avg := 0.0
	if elapsed > 0 {
		avg = float64(observedBytes) / elapsed
	}
	var protocolStats metrics.TransferSnapshot
	if r.collector != nil {
		protocolStats = r.collector.Snapshot()
		if protocolStats.BytesSent > 0 || protocolStats.BytesReceived > 0 || protocolStats.NetworkReceived > 0 {
			observedBytes = protocolStats.BytesSent + protocolStats.BytesRetransmit
			if protocolStats.BytesReceived > observedBytes {
				observedBytes = protocolStats.BytesReceived
			}
			if protocolStats.NetworkReceived > observedBytes {
				observedBytes = protocolStats.NetworkReceived
			}
			avg = protocolStats.GoodputBps
			if protocolStats.BytesSent > 0 {
				goodBytes = protocolStats.BytesSent
			}
			if protocolStats.BytesReceived > goodBytes {
				goodBytes = protocolStats.BytesReceived
			}
		}
	}
	diskReadBytes := uint64(0)
	diskWriteBytes := uint64(0)
	if protocolStats.DiskReadBytes > 0 {
		diskReadBytes = protocolStats.DiskReadBytes
	}
	if protocolStats.DiskWriteBytes > 0 {
		diskWriteBytes = protocolStats.DiskWriteBytes
	}
	return &pb.TransferJob{
		JobId:                     r.jobID,
		RouteId:                   r.routeID,
		SessionId:                 r.sessionID,
		State:                     r.state,
		Protocol:                  r.protocol,
		Source:                    cloneTransferEndpoint(r.source),
		Destination:               cloneTransferEndpoint(r.dest),
		FilesInFlight:             r.filesInFlight,
		StreamsPerFile:            r.streamsPerFile,
		Concurrency:               r.filesInFlight,
		ParallelismPerFile:        r.streamsPerFile,
		ChunkSizeBytes:            r.chunkSizeBytes,
		TcpBufferBytes:            r.tcpBufferBytes,
		UdpMtuBytes:               r.udpMTUBytes,
		UdpWindowPackets:          r.udpWindow,
		UdpBatchPackets:           r.udpBatch,
		UdpAckEveryPackets:        r.udpAckPackets,
		UdpAckEveryMs:             r.udpAckMs,
		UdpSocketReadBufferBytes:  r.udpSocketRead,
		UdpSocketWriteBufferBytes: r.udpSocketWrite,
		UdpFlowControl:            r.udpFlowControl,
		GoodBytes:                 goodBytes,
		NetworkBytes:              observedBytes,
		DiskReadBytes:             diskReadBytes,
		DiskWriteBytes:            diskWriteBytes,
		FilesDone:                 r.doneCount,
		FilesActive:               r.active,
		StreamsActive:             activeStreams,
		Retransmits:               protocolStats.Retransmissions,
		Files:                     files,
		Stats: &pb.StatsSnapshot{
			IngressBytes:         observedBytes,
			EgressBytes:          observedBytes,
			Packets:              protocolStats.PacketsSent + protocolStats.PacketsReceived,
			AverageThroughputBps: avg,
			CurrentThroughputBps: protocolStats.GoodputBps,
			ActiveStreams:        activeStreams,
			LatencyMs:            protocolStats.RttMs,
			SampledAtUnixNano:    now.UnixNano(),
		},
		ErrorMessage: r.errorMessage,
	}
}

func (r *transferJobRuntime) sampleStreamRateLocked(now time.Time, file *pb.TransferFileState, stream *pb.TransferStreamState) {
	if file == nil || stream == nil {
		return
	}
	key := transferStreamKey{streamID: stream.GetStreamId()}
	for i, candidate := range r.files {
		if candidate == file {
			key.fileIndex = i
			break
		}
	}
	startedAt := r.streamStartedAt[key]
	if !startedAt.IsZero() {
		if elapsed := now.Sub(startedAt).Seconds(); elapsed > 0 {
			stream.AverageThroughputBps = float64(stream.GetBytesDone()) / elapsed
		}
	}
	lastAt := r.streamSampleAt[key]
	lastBytes := r.streamSample[key]
	if lastAt.IsZero() {
		r.streamSampleAt[key] = now
		r.streamSample[key] = stream.GetBytesDone()
		stream.CurrentThroughputBps = 0
		return
	}
	elapsed := now.Sub(lastAt).Seconds()
	if elapsed <= 0 {
		return
	}
	bytesDone := stream.GetBytesDone()
	if bytesDone >= lastBytes {
		stream.CurrentThroughputBps = float64(bytesDone-lastBytes) / elapsed
	} else {
		stream.CurrentThroughputBps = 0
	}
	r.streamSampleAt[key] = now
	r.streamSample[key] = bytesDone
}
