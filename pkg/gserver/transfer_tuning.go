package gserver

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func (m *TransferJobManager) UpdateConcurrency(jobID string, filesInFlight, streamsPerFile uint32) (*pb.TransferJob, error) {
	return m.UpdateTuning(&pb.UpdateTransferTuningRequest{
		JobId:              jobID,
		Concurrency:        filesInFlight,
		ParallelismPerFile: streamsPerFile,
	})
}

func (m *TransferJobManager) UpdateTuning(req *pb.UpdateTransferTuningRequest) (*pb.TransferJob, error) {
	if req == nil {
		return nil, errors.New("transfer tuning request is required")
	}
	jobID := strings.TrimSpace(req.GetJobId())
	runtime := m.lookupJob(jobID)
	if runtime == nil {
		return nil, fmt.Errorf("transfer job %q not found", jobID)
	}
	if err := validateTransferTuning(req); err != nil {
		return nil, err
	}
	runtime.mu.Lock()
	if req.GetConcurrency() > 0 {
		runtime.filesInFlight = req.GetConcurrency()
	}
	if req.GetParallelismPerFile() > 0 {
		runtime.streamsPerFile = req.GetParallelismPerFile()
	}
	if req.GetChunkSizeBytes() > 0 {
		runtime.chunkSizeBytes = req.GetChunkSizeBytes()
	}
	if req.GetTcpBufferBytes() > 0 {
		runtime.tcpBufferBytes = req.GetTcpBufferBytes()
	}
	if req.GetUdpMtuBytes() > 0 {
		runtime.udpMTUBytes = req.GetUdpMtuBytes()
	}
	if req.GetUdpWindowPackets() > 0 {
		runtime.udpWindow = req.GetUdpWindowPackets()
	}
	if req.GetUdpBatchPackets() > 0 {
		runtime.udpBatch = req.GetUdpBatchPackets()
	}
	if req.GetUdpAckEveryPackets() > 0 {
		runtime.udpAckPackets = req.GetUdpAckEveryPackets()
	}
	if req.GetUdpAckEveryMs() > 0 {
		runtime.udpAckMs = req.GetUdpAckEveryMs()
	}
	if req.GetUdpSocketReadBufferBytes() > 0 {
		runtime.udpSocketRead = req.GetUdpSocketReadBufferBytes()
	}
	if req.GetUdpSocketWriteBufferBytes() > 0 {
		runtime.udpSocketWrite = req.GetUdpSocketWriteBufferBytes()
	}
	if strings.TrimSpace(req.GetUdpFlowControl()) != "" {
		runtime.udpFlowControl = strings.ToLower(strings.TrimSpace(req.GetUdpFlowControl()))
	}
	runtime.cond.Broadcast()
	runtime.mu.Unlock()
	return runtime.snapshot(), nil
}

func validateTransferTuning(req *pb.UpdateTransferTuningRequest) error {
	if req == nil {
		return errors.New("transfer tuning request is required")
	}
	if req.GetChunkSizeBytes() > 0 && req.GetChunkSizeBytes() < 4*1024 {
		return fmt.Errorf("chunk_size_bytes must be >= 4096")
	}
	if req.GetChunkSizeBytes() > 1<<30 {
		return fmt.Errorf("chunk_size_bytes must be <= 1GiB")
	}
	if req.GetTcpBufferBytes() > 0 && req.GetTcpBufferBytes() < 4*1024 {
		return fmt.Errorf("tcp_buffer_bytes must be >= 4096")
	}
	if req.GetUdpMtuBytes() > 0 {
		if req.GetUdpMtuBytes() < minUDPJobPayloadSize {
			return fmt.Errorf("udp_mtu_bytes must be >= %d", minUDPJobPayloadSize)
		}
		if req.GetUdpMtuBytes() > maxUDPDatagramPayloadSize {
			return fmt.Errorf("udp_mtu_bytes must be <= %d", maxUDPDatagramPayloadSize)
		}
	}
	switch strings.ToLower(strings.TrimSpace(req.GetUdpFlowControl())) {
	case "", "fixed", "bbr":
		return nil
	default:
		return fmt.Errorf("unsupported udp_flow_control %q", req.GetUdpFlowControl())
	}
}

func validateStartTransferTuning(req *pb.StartTransferJobRequest) error {
	if req == nil {
		return errors.New("start transfer job request is required")
	}
	if req.GetChunkSizeBytes() > 0 && req.GetChunkSizeBytes() < 4*1024 {
		return fmt.Errorf("chunk_size_bytes must be >= 4096")
	}
	if req.GetChunkSizeBytes() > 1<<30 {
		return fmt.Errorf("chunk_size_bytes must be <= 1GiB")
	}
	if req.GetTcpBufferBytes() > 0 && req.GetTcpBufferBytes() < 4*1024 {
		return fmt.Errorf("tcp_buffer_bytes must be >= 4096")
	}
	if req.GetUdpMtuBytes() > 0 {
		if req.GetUdpMtuBytes() < minUDPJobPayloadSize {
			return fmt.Errorf("udp_mtu_bytes must be >= %d", minUDPJobPayloadSize)
		}
		if req.GetUdpMtuBytes() > maxUDPDatagramPayloadSize {
			return fmt.Errorf("udp_mtu_bytes must be <= %d", maxUDPDatagramPayloadSize)
		}
	}
	switch strings.ToLower(strings.TrimSpace(req.GetUdpFlowControl())) {
	case "", "fixed", "bbr":
		return nil
	default:
		return fmt.Errorf("unsupported udp_flow_control %q", req.GetUdpFlowControl())
	}
}

func normalizeTransferRuntimeConfig(cfg internal.TransferRuntimeConfig) internal.TransferRuntimeConfig {
	defaults := internal.DefaultTransferRuntimeConfig()
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaults.Concurrency
	}
	if cfg.ParallelismPerFile <= 0 {
		cfg.ParallelismPerFile = defaults.ParallelismPerFile
	}
	if cfg.ChunkSizeBytes <= 0 {
		cfg.ChunkSizeBytes = defaults.ChunkSizeBytes
	}
	if cfg.TCP.BufferSizeBytes <= 0 {
		cfg.TCP.BufferSizeBytes = defaults.TCP.BufferSizeBytes
	}
	if cfg.UDP.MTU <= 0 {
		cfg.UDP.MTU = defaults.UDP.MTU
	}
	if cfg.UDP.MTU > maxUDPDatagramPayloadSize {
		cfg.UDP.MTU = maxUDPDatagramPayloadSize
	}
	if cfg.UDP.MTU < minUDPJobPayloadSize {
		cfg.UDP.MTU = minUDPJobPayloadSize
	}
	if cfg.UDP.WindowPackets <= 0 {
		cfg.UDP.WindowPackets = defaults.UDP.WindowPackets
	}
	if cfg.UDP.BatchPackets <= 0 {
		cfg.UDP.BatchPackets = defaults.UDP.BatchPackets
	}
	if cfg.UDP.AckEveryPackets <= 0 {
		cfg.UDP.AckEveryPackets = defaults.UDP.AckEveryPackets
	}
	if cfg.UDP.AckEvery <= 0 {
		cfg.UDP.AckEvery = defaults.UDP.AckEvery
	}
	if cfg.UDP.WriteFlushBytes <= 0 {
		cfg.UDP.WriteFlushBytes = defaults.UDP.WriteFlushBytes
	}
	if cfg.UDP.ReadBufferBytes <= 0 {
		cfg.UDP.ReadBufferBytes = defaults.UDP.ReadBufferBytes
	}
	if cfg.UDP.WriteBufferBytes <= 0 {
		cfg.UDP.WriteBufferBytes = defaults.UDP.WriteBufferBytes
	}
	switch strings.ToLower(strings.TrimSpace(cfg.UDP.FlowControl)) {
	case "bbr":
		cfg.UDP.FlowControl = "bbr"
	default:
		cfg.UDP.FlowControl = "fixed"
	}
	return cfg
}

func applyStartTransferRuntimeConfig(cfg internal.TransferRuntimeConfig, req *pb.StartTransferJobRequest) internal.TransferRuntimeConfig {
	cfg = normalizeTransferRuntimeConfig(cfg)
	if req == nil {
		return cfg
	}
	if req.GetConcurrency() > 0 {
		cfg.Concurrency = int(req.GetConcurrency())
	} else if req.GetFilesInFlight() > 0 {
		cfg.Concurrency = int(req.GetFilesInFlight())
	}
	if req.GetParallelismPerFile() > 0 {
		cfg.ParallelismPerFile = int(req.GetParallelismPerFile())
	} else if req.GetStreamsPerFile() > 0 {
		cfg.ParallelismPerFile = int(req.GetStreamsPerFile())
	}
	if req.GetChunkSizeBytes() > 0 {
		cfg.ChunkSizeBytes = int(req.GetChunkSizeBytes())
	}
	if req.GetTcpBufferBytes() > 0 {
		cfg.TCP.BufferSizeBytes = int(req.GetTcpBufferBytes())
	}
	if req.GetUdpMtuBytes() > 0 {
		cfg.UDP.MTU = int(req.GetUdpMtuBytes())
	}
	if req.GetUdpWindowPackets() > 0 {
		cfg.UDP.WindowPackets = int(req.GetUdpWindowPackets())
	}
	if req.GetUdpBatchPackets() > 0 {
		cfg.UDP.BatchPackets = int(req.GetUdpBatchPackets())
	}
	if req.GetUdpAckEveryPackets() > 0 {
		cfg.UDP.AckEveryPackets = int(req.GetUdpAckEveryPackets())
	}
	if req.GetUdpAckEveryMs() > 0 {
		cfg.UDP.AckEvery = time.Duration(req.GetUdpAckEveryMs()) * time.Millisecond
	}
	if req.GetUdpSocketReadBufferBytes() > 0 {
		cfg.UDP.ReadBufferBytes = int(req.GetUdpSocketReadBufferBytes())
	}
	if req.GetUdpSocketWriteBufferBytes() > 0 {
		cfg.UDP.WriteBufferBytes = int(req.GetUdpSocketWriteBufferBytes())
	}
	if strings.TrimSpace(req.GetUdpFlowControl()) != "" {
		cfg.UDP.FlowControl = req.GetUdpFlowControl()
	}
	return normalizeTransferRuntimeConfig(cfg)
}
