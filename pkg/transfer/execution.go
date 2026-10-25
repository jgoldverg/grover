package transfer

import (
	"context"
	"net"
	"strings"
	"time"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

// executionForFile snapshots tuning under the job lock and binds progress to one file.
func (m *Manager) executionForFile(runtime *transferJobRuntime, index int) *Execution {
	runtime.mu.Lock()
	chunkSizeBytes := runtime.chunkSizeBytes
	tcpBufferBytes := runtime.tcpBufferBytes
	udpMTUBytes := runtime.udpMTUBytes
	udpWindow := runtime.udpWindow
	udpBatch := runtime.udpBatch
	udpAckPackets := runtime.udpAckPackets
	udpAckMs := runtime.udpAckMs
	udpSocketRead := runtime.udpSocketRead
	udpSocketWrite := runtime.udpSocketWrite
	udpFlowControl := runtime.udpFlowControl
	runtime.mu.Unlock()
	if chunkSizeBytes == 0 {
		chunkSizeBytes = uint64(m.runtimeCfg.ChunkSizeBytes)
	}
	if udpMTUBytes == 0 {
		udpMTUBytes = uint32(m.runtimeCfg.UDP.MTU)
	}
	if udpWindow == 0 {
		udpWindow = uint32(m.runtimeCfg.UDP.WindowPackets)
	}
	if udpBatch == 0 {
		udpBatch = uint32(m.runtimeCfg.UDP.BatchPackets)
	}
	if udpAckPackets == 0 {
		udpAckPackets = uint32(m.runtimeCfg.UDP.AckEveryPackets)
	}
	if udpAckMs == 0 {
		udpAckMs = uint32(m.runtimeCfg.UDP.AckEvery / time.Millisecond)
	}
	if strings.TrimSpace(udpFlowControl) == "" {
		udpFlowControl = m.runtimeCfg.UDP.FlowControl
	}
	return &Execution{
		Job: JobIdentity{
			ID:      runtime.jobID,
			RouteID: runtime.routeID,
		},
		Paths: PathConfig{
			SourceRoot: runtime.source.GetRootPath(),
			DestRoot:   runtime.dest.GetRootPath(),
		},
		Target: TargetConfig{
			Data:     cloneEndpoint(runtime.dest.GetDataEndpoint()),
			Protocol: runtime.protocol,
		},
		IO:  IOConfig{ChunkSizeBytes: chunkSizeBytes},
		TCP: TCPConfig{BufferBytes: tcpBufferBytes},
		UDP: UDPConfig{
			MTU:              int(udpMTUBytes),
			FlowControl:      udpFlowControl,
			WindowPackets:    int(udpWindow),
			BatchPackets:     int(udpBatch),
			AckPackets:       int(udpAckPackets),
			AckEvery:         time.Duration(udpAckMs) * time.Millisecond,
			SocketReadBytes:  udpSocketRead,
			SocketWriteBytes: udpSocketWrite,
		},
		Collector: runtime.collector,
		Hooks: ExecutionHooks{
			TCPConnProvider: func(ctx context.Context) (net.Conn, error) {
				if runtime.origin != pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
					return nil, nil
				}
				return m.endpointStore.takeReverseTCPConn(ctx, runtime.sessionID)
			},
			StreamsPerFile: func() uint32 {
				runtime.mu.Lock()
				defer runtime.mu.Unlock()
				return runtime.streamsPerFile
			},
		},
		Progress: ProgressCallbacks{
			OnProgress: func(filePath string, bytesRead int) {
				runtime.addProgress(index, uint64(bytesRead))
			},
			OnStreamStart: func(filePath string, stream StreamPlan) {
				runtime.startStream(index, stream)
			},
			OnStreamProgress: func(filePath string, streamID uint32, bytesRead int) {
				runtime.addStreamProgress(index, streamID, uint64(bytesRead))
			},
			OnStreamDone: func(filePath string, streamID uint32, state pb.RuntimeState, errText string) {
				runtime.finishStream(index, streamID, state, errText)
			},
		},
	}
}
