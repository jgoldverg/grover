package gserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/energy"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/metrics"
)

// TransferJobManager owns source job lifecycle and composes endpoint,
// destination-receive, planning, execution, and recording components.
type TransferJobManager struct {
	mu            sync.RWMutex
	jobs          map[string]*transferJobRuntime
	endpointStore *transferEndpointRegistry
	receiver      *destinationReceiver
	registry      TransferEndpointRegistry
	planner       TransferSourcePlanner
	executor      TransferFileExecutor
	runtimeCfg    internal.TransferRuntimeConfig
	jobLogDir     string
	energy        *energy.RAPLMonitor
	energyEvery   time.Duration
	nodeEnergy    *nodeEnergyLog
	energyDone    chan struct{}
	stop          chan struct{}
	closeOnce     sync.Once
}

func NewTransferJobManager(cfg *internal.ServerConfig, executor TransferJobExecutor) *TransferJobManager {
	if executor != nil {
		return NewTransferJobManagerWithComponents(cfg, executor, executor)
	}
	return NewTransferJobManagerWithComponents(cfg, nil, nil)
}

// NewTransferJobManagerWithComponents allows source planning and file movement
// to be developed and tested independently. Nil components use local sources
// and the built-in TCP/UDP/local execution dispatcher.
func NewTransferJobManagerWithComponents(cfg *internal.ServerConfig, planner TransferSourcePlanner, executor TransferFileExecutor) *TransferJobManager {
	if planner == nil {
		planner = FilesystemSourcePlanner{}
	}
	if executor == nil {
		executor = ProtocolTransferExecutor{}
	}
	runtimeCfg := normalizeTransferRuntimeConfig(internal.DefaultTransferRuntimeConfig())
	jobLogDir := ""
	var energyMonitor *energy.RAPLMonitor
	energyEvery := time.Second
	if cfg != nil {
		runtimeCfg = normalizeTransferRuntimeConfig(cfg.TransferRuntimeConfig())
		jobLogDir = strings.TrimSpace(cfg.JobLogDir)
		if cfg.EnergySampleMs > 0 {
			energyEvery = time.Duration(cfg.EnergySampleMs) * time.Millisecond
		}
		if cfg.EnergyMonitor {
			if monitor, err := energy.NewRAPLMonitor(""); err != nil {
				internal.Error("energy monitor unavailable", internal.Fields{internal.FieldError: err.Error()})
			} else {
				energyMonitor = monitor
			}
		}
	}
	ports, portErr := NewDataPortAllocator(cfg)
	m := &TransferJobManager{
		jobs:        make(map[string]*transferJobRuntime),
		executor:    executor,
		planner:     planner,
		runtimeCfg:  runtimeCfg,
		jobLogDir:   jobLogDir,
		energy:      energyMonitor,
		energyEvery: energyEvery,
		stop:        make(chan struct{}),
	}
	m.receiver = &destinationReceiver{receives: make(map[string]*destinationReceiveRuntime), jobLogDir: jobLogDir, energy: energyMonitor}
	m.endpointStore = &transferEndpointRegistry{
		endpoints: make(map[string]*preparedTransferEndpoint), reverseTCP: make(map[string]*reverseTCPPool),
		ports: ports, portErr: portErr, runtimeCfg: runtimeCfg, receiver: m.receiver, stop: m.stop,
	}
	m.registry = m.endpointStore
	go m.endpointStore.expiryLoop()
	if m.energy != nil && m.jobLogDir != "" {
		if nodeEnergy, err := newNodeEnergyLog(m.jobLogDir, m.energy); err != nil {
			internal.Error("node energy log unavailable", internal.Fields{
				internal.FieldError: err.Error(),
				"job_log_dir":       m.jobLogDir,
			})
		} else {
			m.nodeEnergy = nodeEnergy
			m.energyDone = make(chan struct{})
			nodeEnergy.append(time.Now(), nil)
			go m.nodeEnergyLoop()
		}
	}
	return m
}

func (m *TransferJobManager) Close() {
	m.closeOnce.Do(func() {
		close(m.stop)
		m.mu.Lock()
		jobs := make([]*transferJobRuntime, 0, len(m.jobs))
		for _, job := range m.jobs {
			jobs = append(jobs, job)
		}
		m.mu.Unlock()
		for _, job := range jobs {
			job.cancel()
		}
		m.endpointStore.close()
		m.receiver.finalizeDestinationReceives()
		if m.energyDone != nil {
			<-m.energyDone
		}
		if m.nodeEnergy != nil {
			if err := m.nodeEnergy.close(); err != nil {
				internal.Warn("failed to close node energy log", internal.Fields{
					internal.FieldError: err.Error(),
					"energy_log_path":   m.nodeEnergy.path,
				})
			}
		}
	})
}

func (m *TransferJobManager) StartJob(ctx context.Context, req *pb.StartTransferJobRequest) (*pb.TransferJob, error) {
	if req == nil {
		return nil, errors.New("start transfer job request is required")
	}
	if err := validateStartTransferTuning(req); err != nil {
		return nil, err
	}
	source := cloneTransferEndpoint(req.GetSource())
	dest := cloneTransferEndpoint(req.GetDestination())
	if source == nil {
		return nil, errors.New("source endpoint is required")
	}
	if dest == nil {
		return nil, errors.New("destination endpoint is required")
	}
	sourceRootOverride := strings.TrimSpace(source.GetRootPath())
	destRootOverride := strings.TrimSpace(dest.GetRootPath())
	if source.GetEndpointId() != "" {
		if prepared, ok := m.registry.GetEndpoint(source.GetEndpointId()); ok {
			source = prepared
			if sourceRootOverride != "" {
				source.RootPath = sourceRootOverride
			}
		}
	}
	if dest.GetEndpointId() != "" {
		if prepared, ok := m.registry.GetEndpoint(dest.GetEndpointId()); ok {
			dest = prepared
			if destRootOverride != "" {
				dest.RootPath = destRootOverride
			}
		}
	}
	if strings.TrimSpace(source.GetRootPath()) == "" || strings.TrimSpace(dest.GetRootPath()) == "" {
		return nil, errors.New("source and destination root paths are required")
	}
	protocol := source.GetProtocol()
	if protocol == pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED {
		protocol = dest.GetProtocol()
	}
	if protocol == pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED {
		protocol = pb.DataProtocol_DATA_PROTOCOL_TCP
	}
	jobID := strings.TrimSpace(req.GetJobId())
	if jobID == "" {
		jobID = strings.TrimSpace(source.GetJobId())
	}
	if jobID == "" {
		jobID = strings.TrimSpace(dest.GetJobId())
	}
	if jobID == "" {
		jobID = uuid.NewString()
	}
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		sessionID = strings.TrimSpace(source.GetSessionId())
	}
	if sessionID == "" {
		sessionID = strings.TrimSpace(dest.GetSessionId())
	}
	if sessionID == "" {
		sessionID = jobID
	}
	jobCfg := applyStartTransferRuntimeConfig(m.runtimeCfg, req)
	filesInFlight := req.GetConcurrency()
	if filesInFlight == 0 {
		filesInFlight = req.GetFilesInFlight()
	}
	if filesInFlight == 0 {
		filesInFlight = uint32(jobCfg.Concurrency)
	}
	streamsPerFile := req.GetParallelismPerFile()
	if streamsPerFile == 0 {
		streamsPerFile = req.GetStreamsPerFile()
	}
	if streamsPerFile == 0 {
		streamsPerFile = uint32(jobCfg.ParallelismPerFile)
	}
	chunkSizeBytes := req.GetChunkSizeBytes()
	if chunkSizeBytes == 0 {
		chunkSizeBytes = uint64(jobCfg.ChunkSizeBytes)
	}
	connectionOrigin := req.GetConnectionOrigin()
	if connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_UNSPECIFIED {
		connectionOrigin = pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
	}
	if protocol == pb.DataProtocol_DATA_PROTOCOL_UDP && filesInFlight > 1 {
		filesInFlight = 1
	}
	if connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION && protocol != pb.DataProtocol_DATA_PROTOCOL_TCP {
		return nil, errors.New("destination-origin transfers currently support tcp only")
	}

	plans, err := m.planner.PlanFiles(ctx, source, req.GetPaths())
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return nil, errors.New("no files to transfer")
	}

	jobCtx, cancel := context.WithCancel(context.Background())
	runtime := &transferJobRuntime{
		cancel:          cancel,
		jobID:           jobID,
		sessionID:       sessionID,
		routeID:         strings.TrimSpace(req.GetRouteId()),
		protocol:        protocol,
		source:          source,
		dest:            dest,
		origin:          connectionOrigin,
		state:           pb.RuntimeState_RUNTIME_STATE_RUNNING,
		filesInFlight:   filesInFlight,
		streamsPerFile:  streamsPerFile,
		chunkSizeBytes:  chunkSizeBytes,
		tcpBufferBytes:  uint64(jobCfg.TCP.BufferSizeBytes),
		udpMTUBytes:     uint32(jobCfg.UDP.MTU),
		udpWindow:       uint32(jobCfg.UDP.WindowPackets),
		udpBatch:        uint32(jobCfg.UDP.BatchPackets),
		udpAckPackets:   uint32(jobCfg.UDP.AckEveryPackets),
		udpAckMs:        uint32(jobCfg.UDP.AckEvery / time.Millisecond),
		udpSocketRead:   uint64(jobCfg.UDP.ReadBufferBytes),
		udpSocketWrite:  uint64(jobCfg.UDP.WriteBufferBytes),
		udpFlowControl:  jobCfg.UDP.FlowControl,
		startedAt:       time.Now(),
		collector:       metrics.NewTransferCollector("grover"),
		files:           make([]*pb.TransferFileState, 0, len(plans)),
		streamStartedAt: make(map[transferStreamKey]time.Time),
		streamLastAt:    make(map[transferStreamKey]time.Time),
		streamSampleAt:  make(map[transferStreamKey]time.Time),
		streamSample:    make(map[transferStreamKey]uint64),
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	for _, plan := range plans {
		runtime.files = append(runtime.files, &pb.TransferFileState{
			Path:         plan.SourcePath,
			RelativePath: plan.RelativePath,
			Size:         plan.Size,
			State:        pb.RuntimeState_RUNTIME_STATE_READY,
		})
	}
	if m.jobLogDir != "" {
		history, err := newTransferJobHistory(m.jobLogDir, jobID, m.energy)
		if err != nil {
			internal.Warn(jobLogMessage(jobID, "historical job log disabled"), internal.Fields{
				internal.FieldError: err.Error(),
				"job_log_dir":       m.jobLogDir,
			})
		} else {
			runtime.history = history
			if err := history.writeManifest(runtime, plans); err != nil {
				internal.Warn(jobLogMessage(jobID, "failed to write job manifest"), internal.Fields{
					internal.FieldError: err.Error(),
					"job_log_path":      history.dir,
				})
			}
			history.appendSnapshot(runtime.snapshot())
			history.appendEnergy(runtime, time.Now())
		}
	}
	internal.Info(jobLogMessage(jobID, "transfer accepted"), internal.Fields{
		"route_id":         runtime.routeID,
		"session_id":       runtime.sessionID,
		"protocol":         protocol.String(),
		"source_root":      source.GetRootPath(),
		"destination_root": dest.GetRootPath(),
		"destination_data": endpointLabel(dest.GetDataEndpoint()),
		"files":            len(plans),
		"bytes":            totalPlannedBytes(plans),
		"concurrency":      filesInFlight,
		"parallelism":      streamsPerFile,
		"chunk_size_bytes": chunkSizeBytes,
		"udp_flow_control": runtime.udpFlowControl,
	})

	m.mu.Lock()
	if _, exists := m.jobs[jobID]; exists {
		m.mu.Unlock()
		cancel()
		if runtime.history != nil {
			_ = runtime.history.close()
		}
		return nil, fmt.Errorf("transfer job %q already exists", jobID)
	}
	m.jobs[jobID] = runtime
	m.mu.Unlock()
	m.appendNodeEnergy(time.Now())

	go m.runJob(jobCtx, runtime, plans)
	return runtime.snapshot(), nil
}

func (m *TransferJobManager) GetJob(jobID string) (*pb.TransferJob, error) {
	runtime := m.lookupJob(jobID)
	if runtime == nil {
		return nil, fmt.Errorf("transfer job %q not found", jobID)
	}
	return runtime.snapshot(), nil
}

func (m *TransferJobManager) ListJobs(routeID string) []*pb.TransferJob {
	routeID = strings.TrimSpace(routeID)
	m.mu.RLock()
	defer m.mu.RUnlock()
	jobs := make([]*pb.TransferJob, 0, len(m.jobs))
	for _, runtime := range m.jobs {
		if routeID != "" && runtime.routeID != routeID {
			continue
		}
		jobs = append(jobs, runtime.snapshot())
	}
	return jobs
}

func (m *TransferJobManager) AbortJob(jobID string) (*pb.TransferJob, error) {
	runtime := m.lookupJob(jobID)
	if runtime == nil {
		return nil, fmt.Errorf("transfer job %q not found", jobID)
	}
	runtime.cancel()
	runtime.mu.Lock()
	if runtime.state == pb.RuntimeState_RUNTIME_STATE_RUNNING {
		runtime.state = pb.RuntimeState_RUNTIME_STATE_ABORTED
	}
	runtime.cond.Broadcast()
	runtime.mu.Unlock()
	return runtime.snapshot(), nil
}

func (m *TransferJobManager) lookupJob(jobID string) *transferJobRuntime {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[strings.TrimSpace(jobID)]
}

func endpointLabel(ep *pb.DataEndpoint) string {
	if ep == nil || strings.TrimSpace(ep.GetHost()) == "" || ep.GetPort() == 0 {
		return ""
	}
	return net.JoinHostPort(ep.GetHost(), fmt.Sprintf("%d", ep.GetPort()))
}

func jobLogMessage(jobID, statement string) string {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		jobID = "unknown-job"
	}
	return fmt.Sprintf("[%s] -> %s", jobID, statement)
}
