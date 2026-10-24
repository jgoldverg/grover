package gserver

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func (m *TransferJobManager) runJob(ctx context.Context, runtime *transferJobRuntime, plans []TransferFilePlan) {
	var stopHistory chan struct{}
	if runtime.history != nil {
		stopHistory = make(chan struct{})
		go runtime.history.snapshotLoop(runtime, stopHistory)
		if runtime.history.energy != nil {
			go runtime.history.energyLoop(runtime, m.energyEvery, stopHistory)
		}
	}
	defer func() {
		if stopHistory != nil {
			close(stopHistory)
		}
		m.appendNodeEnergy(time.Now())
		job := runtime.snapshot()
		if runtime.history != nil {
			runtime.history.appendSnapshot(job)
			runtime.history.appendEnergy(runtime, time.Now())
			if err := runtime.history.writeFinal(job); err != nil {
				internal.Warn(jobLogMessage(job.GetJobId(), "failed to write final job log"), internal.Fields{
					internal.FieldError: err.Error(),
					"job_log_path":      runtime.history.dir,
				})
			}
			if err := runtime.history.close(); err != nil {
				internal.Warn(jobLogMessage(job.GetJobId(), "failed to close job log"), internal.Fields{
					internal.FieldError: err.Error(),
					"job_log_path":      runtime.history.dir,
				})
			}
		}
		fields := internal.Fields{
			"route_id":       job.GetRouteId(),
			"state":          job.GetState().String(),
			"good_bytes":     job.GetGoodBytes(),
			"network_bytes":  job.GetNetworkBytes(),
			"files_done":     job.GetFilesDone(),
			"files_active":   job.GetFilesActive(),
			"throughput_bps": uint64(job.GetStats().GetAverageThroughputBps()),
		}
		if job.GetErrorMessage() != "" {
			fields[internal.FieldError] = job.GetErrorMessage()
		}
		internal.Info(jobLogMessage(job.GetJobId(), "transfer finished"), fields)
	}()
	var wg sync.WaitGroup
	errCh := make(chan error, len(plans))
	for {
		runtime.mu.Lock()
		for runtime.state == pb.RuntimeState_RUNTIME_STATE_RUNNING &&
			runtime.nextIndex < len(plans) &&
			runtime.active >= runtime.filesInFlight {
			runtime.cond.Wait()
		}
		if runtime.state != pb.RuntimeState_RUNTIME_STATE_RUNNING || runtime.nextIndex >= len(plans) {
			runtime.mu.Unlock()
			break
		}
		index := runtime.nextIndex
		runtime.nextIndex++
		runtime.active++
		runtime.files[index].State = pb.RuntimeState_RUNTIME_STATE_RUNNING
		runtime.mu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
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
			execCtx := &TransferExecutionContext{
				JobID:          runtime.jobID,
				RouteID:        runtime.routeID,
				SourceRoot:     runtime.source.GetRootPath(),
				DestRoot:       runtime.dest.GetRootPath(),
				DestData:       cloneEndpoint(runtime.dest.GetDataEndpoint()),
				Protocol:       runtime.protocol,
				ChunkSizeBytes: chunkSizeBytes,
				TCPBufferBytes: tcpBufferBytes,
				UDPPayload:     int(udpMTUBytes),
				UDPFlow:        udpFlowControl,
				UDPWindow:      int(udpWindow),
				UDPBatch:       int(udpBatch),
				UDPAckPackets:  int(udpAckPackets),
				UDPAckEvery:    time.Duration(udpAckMs) * time.Millisecond,
				UDPSocketRead:  udpSocketRead,
				UDPSocketWrite: udpSocketWrite,
				Collector:      runtime.collector,
				TCPConnProvider: func(ctx context.Context) (net.Conn, error) {
					if runtime.origin != pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
						return nil, nil
					}
					return m.endpointStore.takeReverseTCPConn(ctx, runtime.sessionID)
				},
				StreamsFunc: func() uint32 {
					runtime.mu.Lock()
					defer runtime.mu.Unlock()
					return runtime.streamsPerFile
				},
				OnProgress: func(filePath string, bytesRead int) {
					runtime.addProgress(index, uint64(bytesRead))
				},
				OnStreamStart: func(filePath string, stream TransferStreamPlan) {
					runtime.startStream(index, stream)
				},
				OnStreamProgress: func(filePath string, streamID uint32, bytesRead int) {
					runtime.addStreamProgress(index, streamID, uint64(bytesRead))
				},
				OnStreamDone: func(filePath string, streamID uint32, state pb.RuntimeState, errText string) {
					runtime.finishStream(index, streamID, state, errText)
				},
			}
			if err := m.executor.TransferFile(ctx, execCtx, plans[index]); err != nil {
				errCh <- err
				runtime.finishFile(index, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
				return
			}
			runtime.finishFile(index, pb.RuntimeState_RUNTIME_STATE_DONE, "")
		}()
	}
	wg.Wait()
	close(errCh)

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.state == pb.RuntimeState_RUNTIME_STATE_ABORTED {
		return
	}
	if ctx.Err() != nil {
		runtime.state = pb.RuntimeState_RUNTIME_STATE_ABORTED
		return
	}
	if err, ok := <-errCh; ok {
		runtime.state = pb.RuntimeState_RUNTIME_STATE_FAILED
		runtime.errorMessage = err.Error()
		return
	}
	runtime.state = pb.RuntimeState_RUNTIME_STATE_DONE
}

func totalPlannedBytes(plans []TransferFilePlan) uint64 {
	var total uint64
	for _, plan := range plans {
		total += plan.Size
	}
	return total
}
