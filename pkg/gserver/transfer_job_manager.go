package gserver

import (
	"bufio"
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/udpdataplane"
)

const (
	defaultTransferEndpointTTL    = 10 * time.Minute
	defaultTransferCopyBufferSize = 128 * 1024
	maxUDPDatagramPayloadSize     = 65507
	minUDPJobPayloadSize          = 512
)

var udpJobMagic = []byte("GROVERJOBUDP2")

const (
	udpJobPacketStart byte = 1
	udpJobPacketReady byte = 2
)

type TransferEndpointRegistry interface {
	PrepareEndpoint(context.Context, *pb.PrepareTransferEndpointRequest) (*pb.TransferEndpoint, error)
	GetEndpoint(id string) (*pb.TransferEndpoint, bool)
}

type TransferJobExecutor interface {
	PlanFiles(context.Context, *pb.TransferEndpoint, []string) ([]TransferFilePlan, error)
	TransferFile(context.Context, *TransferExecutionContext, TransferFilePlan) error
}

type TransferExecutionContext struct {
	JobID       string
	RouteID     string
	SourceRoot  string
	DestRoot    string
	DestData    *pb.DataEndpoint
	Protocol    pb.DataProtocol
	UDPPayload  int
	UDPWindow   int
	UDPBatch    int
	StreamsFunc func() uint32
	OnProgress  func(filePath string, bytesRead int)
}

type TransferFilePlan struct {
	SourcePath   string
	RelativePath string
	Size         uint64
}

type TransferJobManager struct {
	mu        sync.RWMutex
	endpoints map[string]*preparedTransferEndpoint
	jobs      map[string]*transferJobRuntime
	registry  TransferEndpointRegistry
	executor  TransferJobExecutor
	ports     *DataPortAllocator
	portErr   error
	udp       udpTransferTuning
	stop      chan struct{}
	closeOnce sync.Once
}

type udpTransferTuning struct {
	mtu             int
	windowPackets   int
	ackEveryPackets int
	ackEvery        time.Duration
	batchPackets    int
}

type preparedTransferEndpoint struct {
	endpoint *pb.TransferEndpoint
	expires  time.Time
	lease    *DataPortLease
}

type transferJobRuntime struct {
	mu     sync.Mutex
	cond   *sync.Cond
	cancel context.CancelFunc

	jobID    string
	routeID  string
	protocol pb.DataProtocol
	source   *pb.TransferEndpoint
	dest     *pb.TransferEndpoint

	state          pb.RuntimeState
	filesInFlight  uint32
	streamsPerFile uint32
	errorMessage   string
	startedAt      time.Time

	files     []*pb.TransferFileState
	nextIndex int
	active    uint32
	doneCount uint32
}

type localFilesystemTransferExecutor struct{}

func NewTransferJobManager(cfg *internal.ServerConfig, executor TransferJobExecutor) *TransferJobManager {
	if executor == nil {
		executor = localFilesystemTransferExecutor{}
	}
	ports, portErr := NewDataPortAllocator(cfg)
	m := &TransferJobManager{
		endpoints: make(map[string]*preparedTransferEndpoint),
		jobs:      make(map[string]*transferJobRuntime),
		executor:  executor,
		ports:     ports,
		portErr:   portErr,
		udp:       normalizedUDPTransferTuning(cfg),
		stop:      make(chan struct{}),
	}
	m.registry = m
	go m.expiryLoop()
	return m
}

func (m *TransferJobManager) Close() {
	m.closeOnce.Do(func() {
		close(m.stop)
		m.mu.Lock()
		endpoints := make([]*preparedTransferEndpoint, 0, len(m.endpoints))
		for id, endpoint := range m.endpoints {
			delete(m.endpoints, id)
			endpoints = append(endpoints, endpoint)
		}
		jobs := make([]*transferJobRuntime, 0, len(m.jobs))
		for _, job := range m.jobs {
			jobs = append(jobs, job)
		}
		m.mu.Unlock()
		for _, job := range jobs {
			job.cancel()
		}
		for _, endpoint := range endpoints {
			closePreparedTransferEndpoint(endpoint)
		}
	})
}

func (m *TransferJobManager) PrepareEndpoint(ctx context.Context, req *pb.PrepareTransferEndpointRequest) (*pb.TransferEndpoint, error) {
	if req == nil {
		return nil, errors.New("prepare endpoint request is required")
	}
	if req.GetRole() == pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_UNSPECIFIED {
		return nil, errors.New("endpoint role is required")
	}
	root := strings.TrimSpace(req.GetRootPath())
	if root == "" {
		return nil, errors.New("root path is required")
	}
	protocol := req.GetProtocol()
	if protocol == pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED {
		protocol = pb.DataProtocol_DATA_PROTOCOL_TCP
	}
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	if ttl <= 0 {
		ttl = defaultTransferEndpointTTL
	}
	jobID := strings.TrimSpace(req.GetJobId())
	if jobID == "" {
		jobID = uuid.NewString()
	}
	var lease *DataPortLease
	dataEndpoint := cloneEndpoint(req.GetBind())
	if req.GetRole() == pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION &&
		(protocol == pb.DataProtocol_DATA_PROTOCOL_TCP || protocol == pb.DataProtocol_DATA_PROTOCOL_UDP) {
		if m.portErr != nil {
			return nil, m.portErr
		}
		var err error
		if protocol == pb.DataProtocol_DATA_PROTOCOL_UDP {
			lease, err = m.ports.AllocateUDP()
		} else {
			lease, err = m.ports.AllocateTCP()
		}
		if err != nil {
			return nil, err
		}
		dataEndpoint = &pb.DataEndpoint{Host: lease.AdvertiseHost, Port: uint32(lease.Port)}
	}
	endpoint := &pb.TransferEndpoint{
		EndpointId:    uuid.NewString(),
		RouteId:       strings.TrimSpace(req.GetRouteId()),
		JobId:         jobID,
		Role:          req.GetRole(),
		Protocol:      protocol,
		DataEndpoint:  dataEndpoint,
		RootPath:      root,
		TtlSeconds:    uint32(ttl / time.Second),
		ExpiresAtUnix: time.Now().Add(ttl).Unix(),
	}
	if lease != nil && protocol == pb.DataProtocol_DATA_PROTOCOL_TCP {
		go serveTransferEndpointTCP(lease.TCPListener, root)
	} else if lease != nil && protocol == pb.DataProtocol_DATA_PROTOCOL_UDP {
		go serveTransferEndpointUDP(lease.UDPConn, root, m.udp)
	}
	m.mu.Lock()
	m.endpoints[endpoint.EndpointId] = &preparedTransferEndpoint{endpoint: endpoint, expires: time.Now().Add(ttl), lease: lease}
	m.mu.Unlock()
	return cloneTransferEndpoint(endpoint), nil
}

func (m *TransferJobManager) GetEndpoint(id string) (*pb.TransferEndpoint, bool) {
	m.mu.RLock()
	item := m.endpoints[strings.TrimSpace(id)]
	m.mu.RUnlock()
	if item == nil {
		return nil, false
	}
	if time.Now().After(item.expires) {
		m.expireEndpoints(time.Now())
		return nil, false
	}
	return cloneTransferEndpoint(item.endpoint), true
}

func (m *TransferJobManager) StartJob(ctx context.Context, req *pb.StartTransferJobRequest) (*pb.TransferJob, error) {
	if req == nil {
		return nil, errors.New("start transfer job request is required")
	}
	source := cloneTransferEndpoint(req.GetSource())
	dest := cloneTransferEndpoint(req.GetDestination())
	if source == nil {
		return nil, errors.New("source endpoint is required")
	}
	if dest == nil {
		return nil, errors.New("destination endpoint is required")
	}
	if source.GetEndpointId() != "" {
		if prepared, ok := m.registry.GetEndpoint(source.GetEndpointId()); ok {
			source = prepared
		}
	}
	if dest.GetEndpointId() != "" {
		if prepared, ok := m.registry.GetEndpoint(dest.GetEndpointId()); ok {
			dest = prepared
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
	filesInFlight := req.GetFilesInFlight()
	if filesInFlight == 0 {
		filesInFlight = 1
	}
	streamsPerFile := req.GetStreamsPerFile()
	if streamsPerFile == 0 {
		streamsPerFile = 1
	}
	if protocol == pb.DataProtocol_DATA_PROTOCOL_UDP && filesInFlight > 1 {
		filesInFlight = 1
	}

	plans, err := m.executor.PlanFiles(ctx, source, req.GetPaths())
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return nil, errors.New("no files to transfer")
	}

	jobCtx, cancel := context.WithCancel(context.Background())
	runtime := &transferJobRuntime{
		cancel:         cancel,
		jobID:          jobID,
		routeID:        strings.TrimSpace(req.GetRouteId()),
		protocol:       protocol,
		source:         source,
		dest:           dest,
		state:          pb.RuntimeState_RUNTIME_STATE_RUNNING,
		filesInFlight:  filesInFlight,
		streamsPerFile: streamsPerFile,
		startedAt:      time.Now(),
		files:          make([]*pb.TransferFileState, 0, len(plans)),
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

	m.mu.Lock()
	if _, exists := m.jobs[jobID]; exists {
		m.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("transfer job %q already exists", jobID)
	}
	m.jobs[jobID] = runtime
	m.mu.Unlock()

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

func (m *TransferJobManager) UpdateConcurrency(jobID string, filesInFlight, streamsPerFile uint32) (*pb.TransferJob, error) {
	runtime := m.lookupJob(jobID)
	if runtime == nil {
		return nil, fmt.Errorf("transfer job %q not found", jobID)
	}
	runtime.mu.Lock()
	if filesInFlight > 0 {
		runtime.filesInFlight = filesInFlight
	}
	if streamsPerFile > 0 {
		runtime.streamsPerFile = streamsPerFile
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

func (m *TransferJobManager) expiryLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-ticker.C:
			m.expireEndpoints(now)
		}
	}
}

func (m *TransferJobManager) expireEndpoints(now time.Time) {
	var expired []*preparedTransferEndpoint
	m.mu.Lock()
	for id, endpoint := range m.endpoints {
		if !endpoint.expires.IsZero() && !now.Before(endpoint.expires) {
			delete(m.endpoints, id)
			expired = append(expired, endpoint)
		}
	}
	m.mu.Unlock()
	for _, endpoint := range expired {
		closePreparedTransferEndpoint(endpoint)
	}
}

func closePreparedTransferEndpoint(endpoint *preparedTransferEndpoint) {
	if endpoint != nil && endpoint.lease != nil {
		_ = endpoint.lease.Close()
	}
}

func normalizedUDPTransferTuning(cfg *internal.ServerConfig) udpTransferTuning {
	tuning := udpTransferTuning{
		mtu:             1200,
		windowPackets:   4096,
		ackEveryPackets: 32,
		ackEvery:        5 * time.Millisecond,
		batchPackets:    64,
	}
	if cfg == nil {
		return tuning
	}
	if cfg.UDPMTUSize > 0 {
		tuning.mtu = cfg.UDPMTUSize
		if tuning.mtu > maxUDPDatagramPayloadSize {
			tuning.mtu = maxUDPDatagramPayloadSize
		}
		if tuning.mtu < minUDPJobPayloadSize {
			tuning.mtu = minUDPJobPayloadSize
		}
	}
	if cfg.UDPWindowPackets > 0 {
		tuning.windowPackets = cfg.UDPWindowPackets
	}
	if cfg.UDPAckEveryPackets > 0 {
		tuning.ackEveryPackets = cfg.UDPAckEveryPackets
	}
	if cfg.UDPAckEveryMs > 0 {
		tuning.ackEvery = time.Duration(cfg.UDPAckEveryMs) * time.Millisecond
	}
	if cfg.UDPBatchPackets > 0 {
		tuning.batchPackets = cfg.UDPBatchPackets
	}
	return tuning
}

func (m *TransferJobManager) runJob(ctx context.Context, runtime *transferJobRuntime, plans []TransferFilePlan) {
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
			execCtx := &TransferExecutionContext{
				JobID:      runtime.jobID,
				RouteID:    runtime.routeID,
				SourceRoot: runtime.source.GetRootPath(),
				DestRoot:   runtime.dest.GetRootPath(),
				DestData:   cloneEndpoint(runtime.dest.GetDataEndpoint()),
				Protocol:   runtime.protocol,
				UDPPayload: m.udp.mtu,
				UDPWindow:  m.udp.windowPackets,
				UDPBatch:   m.udp.batchPackets,
				StreamsFunc: func() uint32 {
					runtime.mu.Lock()
					defer runtime.mu.Unlock()
					return runtime.streamsPerFile
				},
				OnProgress: func(filePath string, bytesRead int) {
					runtime.addProgress(index, uint64(bytesRead))
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

func (r *transferJobRuntime) addProgress(index int, n uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index >= 0 && index < len(r.files) {
		r.files[index].BytesDone += n
	}
}

func (r *transferJobRuntime) finishFile(index int, state pb.RuntimeState, errText string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index >= 0 && index < len(r.files) {
		r.files[index].State = state
		r.files[index].ErrorMessage = errText
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
	files := make([]*pb.TransferFileState, 0, len(r.files))
	var goodBytes uint64
	var observedBytes uint64
	for _, f := range r.files {
		cp := *f
		files = append(files, &cp)
		observedBytes += f.BytesDone
		if f.State == pb.RuntimeState_RUNTIME_STATE_DONE {
			goodBytes += f.BytesDone
		}
	}
	elapsed := time.Since(r.startedAt).Seconds()
	avg := 0.0
	if elapsed > 0 {
		avg = float64(observedBytes) / elapsed
	}
	return &pb.TransferJob{
		JobId:          r.jobID,
		RouteId:        r.routeID,
		State:          r.state,
		Protocol:       r.protocol,
		Source:         cloneTransferEndpoint(r.source),
		Destination:    cloneTransferEndpoint(r.dest),
		FilesInFlight:  r.filesInFlight,
		StreamsPerFile: r.streamsPerFile,
		GoodBytes:      goodBytes,
		NetworkBytes:   observedBytes,
		DiskReadBytes:  observedBytes,
		DiskWriteBytes: observedBytes,
		FilesDone:      r.doneCount,
		FilesActive:    r.active,
		StreamsActive:  r.active * r.streamsPerFile,
		Files:          files,
		Stats: &pb.StatsSnapshot{
			IngressBytes:         observedBytes,
			EgressBytes:          observedBytes,
			AverageThroughputBps: avg,
			CurrentThroughputBps: avg,
			ActiveStreams:        r.active * r.streamsPerFile,
			SampledAtUnixNano:    time.Now().UnixNano(),
		},
		ErrorMessage: r.errorMessage,
	}
}

func (localFilesystemTransferExecutor) PlanFiles(ctx context.Context, source *pb.TransferEndpoint, paths []string) ([]TransferFilePlan, error) {
	root := filepath.Clean(source.GetRootPath())
	var plans []TransferFilePlan
	addPath := func(p string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		full := p
		if !filepath.IsAbs(full) {
			full = filepath.Join(root, full)
		}
		info, err := os.Stat(full)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return filepath.WalkDir(full, func(path string, d os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if d.IsDir() {
					return nil
				}
				info, err := d.Info()
				if err != nil {
					return err
				}
				rel, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				plans = append(plans, TransferFilePlan{SourcePath: path, RelativePath: filepath.ToSlash(rel), Size: uint64(info.Size())})
				return nil
			})
		}
		rel, err := filepath.Rel(root, full)
		if err != nil || strings.HasPrefix(rel, "..") {
			rel = filepath.Base(full)
		}
		plans = append(plans, TransferFilePlan{SourcePath: full, RelativePath: filepath.ToSlash(rel), Size: uint64(info.Size())})
		return nil
	}
	if len(paths) == 0 {
		if err := addPath(root); err != nil {
			return nil, err
		}
		return plans, nil
	}
	for _, p := range paths {
		if err := addPath(strings.TrimSpace(p)); err != nil {
			return nil, err
		}
	}
	return plans, nil
}

func (localFilesystemTransferExecutor) TransferFile(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan) error {
	if exec.DestData != nil && strings.TrimSpace(exec.DestData.GetHost()) != "" && exec.DestData.GetPort() != 0 {
		if exec.Protocol == pb.DataProtocol_DATA_PROTOCOL_UDP {
			return sendFileToUDPDestination(ctx, exec, plan)
		}
		return sendFileToTCPDestination(ctx, exec, plan)
	}
	dstPath := filepath.Join(exec.DestRoot, filepath.FromSlash(plan.RelativePath))
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	src, err := os.Open(plan.SourcePath)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer dst.Close()
	buf := make([]byte, defaultTransferCopyBufferSize)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			if err := writeFull(ctx, dst, buf[:n]); err != nil {
				return err
			}
			if exec.OnProgress != nil {
				exec.OnProgress(plan.SourcePath, n)
			}
		}
		if readErr == io.EOF {
			return dst.Sync()
		}
		if readErr != nil {
			return readErr
		}
	}
}

func sendFileToUDPDestination(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan) error {
	src, err := os.Open(plan.SourcePath)
	if err != nil {
		return err
	}
	defer src.Close()
	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(exec.DestData.GetHost(), fmt.Sprintf("%d", exec.DestData.GetPort())))
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	streamCount := 1
	if exec.StreamsFunc != nil {
		streamCount = int(exec.StreamsFunc())
	}
	if streamCount <= 0 {
		streamCount = 1
	}
	if plan.Size == 0 {
		streamCount = 1
	}
	if uint64(streamCount) > plan.Size && plan.Size > 0 {
		streamCount = int(plan.Size)
	}
	streamIDs := make([]uint32, streamCount)
	for i := range streamIDs {
		streamIDs[i] = uint32(i + 1)
	}
	sessionKey, err := randomUint32()
	if err != nil {
		return err
	}
	if err := sendUDPJobStartAndWait(ctx, conn, plan.RelativePath, plan.Size, sessionKey, streamIDs); err != nil {
		return err
	}
	if plan.Size == 0 {
		return nil
	}
	if len(streamIDs) == 1 {
		n, err := udpdataplane.Send(ctx, udpdataplane.SendConfig{
			Transport:       udpdataplane.NewUDPConnTransport(conn),
			SessionID:       exec.JobID,
			SessionKey:      sessionKey,
			StreamID:        streamIDs[0],
			MTU:             exec.UDPPayload,
			FlowControl:     "fixed",
			WindowPackets:   udpWindowPackets(exec),
			BatchPackets:    udpBatchPackets(exec),
			RequireFinalAck: true,
		}, src)
		if n > 0 && exec.OnProgress != nil {
			exec.OnProgress(plan.SourcePath, int(n))
		}
		return err
	}
	return sendFileToUDPDestinationParallel(ctx, exec, plan, sessionKey, streamIDs)
}

func sendFileToUDPDestinationParallel(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan, sessionKey uint32, streamIDs []uint32) error {
	source, err := os.Open(plan.SourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	ranges, err := planByteRanges(int64(plan.Size), len(streamIDs))
	if err != nil {
		return err
	}
	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(exec.DestData.GetHost(), fmt.Sprintf("%d", exec.DestData.GetPort())))
	if err != nil {
		return err
	}
	errCh := make(chan error, len(ranges))
	var wg sync.WaitGroup
	for i, br := range ranges {
		streamID := streamIDs[i]
		wg.Add(1)
		go func(streamID uint32, br byteRange) {
			defer wg.Done()
			conn, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close()
			sr := io.NewSectionReader(source, br.offset, br.length)
			n, err := udpdataplane.Send(ctx, udpdataplane.SendConfig{
				Transport:       udpdataplane.NewUDPConnTransport(conn),
				SessionID:       exec.JobID,
				SessionKey:      sessionKey,
				StreamID:        streamID,
				BaseOffset:      uint64(br.offset),
				MTU:             exec.UDPPayload,
				FlowControl:     "fixed",
				WindowPackets:   udpWindowPackets(exec),
				BatchPackets:    udpBatchPackets(exec),
				RequireFinalAck: true,
			}, sr)
			if n > 0 && exec.OnProgress != nil {
				exec.OnProgress(plan.SourcePath, int(n))
			}
			if err != nil {
				errCh <- err
			}
		}(streamID, br)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func udpWindowPackets(exec *TransferExecutionContext) int {
	if exec != nil && exec.UDPWindow > 0 {
		return exec.UDPWindow
	}
	return 4096
}

func udpBatchPackets(exec *TransferExecutionContext) int {
	if exec != nil && exec.UDPBatch > 0 {
		return exec.UDPBatch
	}
	return 64
}

func udpJobPayloadSize(exec *TransferExecutionContext, relPath string) int {
	mtu := 1200
	if exec != nil && exec.UDPPayload > 0 {
		mtu = exec.UDPPayload
	}
	headerLen := len(udpJobMagic) + 4 + 8 + 8 + 1 + len([]byte(relPath))
	payload := mtu - headerLen
	if payload < minUDPJobPayloadSize {
		payload = minUDPJobPayloadSize
	}
	maxPayload := maxUDPDatagramPayloadSize - headerLen
	if payload > maxPayload {
		payload = maxPayload
	}
	if payload < 1 {
		return 1
	}
	return payload
}

type udpJobStartPacket struct {
	sessionKey uint32
	size       uint64
	streamIDs  []uint32
	relPath    string
}

func encodeUDPJobStartPacket(start udpJobStartPacket) ([]byte, error) {
	pathBytes := []byte(start.relPath)
	if len(pathBytes) == 0 {
		return nil, errors.New("udp job path is required")
	}
	if len(start.streamIDs) == 0 {
		return nil, errors.New("at least one udp stream id is required")
	}
	if len(start.streamIDs) > 1<<16-1 {
		return nil, fmt.Errorf("too many udp streams: %d", len(start.streamIDs))
	}
	headerLen := len(udpJobMagic) + 1 + 4 + 8 + 2 + len(start.streamIDs)*4 + 4
	if headerLen+len(pathBytes) > maxUDPDatagramPayloadSize {
		return nil, fmt.Errorf("udp start packet exceeds datagram payload limit: path=%d streams=%d", len(pathBytes), len(start.streamIDs))
	}
	packet := make([]byte, headerLen+len(pathBytes))
	copy(packet, udpJobMagic)
	pos := len(udpJobMagic)
	packet[pos] = udpJobPacketStart
	pos++
	binary.BigEndian.PutUint32(packet[pos:pos+4], start.sessionKey)
	pos += 4
	binary.BigEndian.PutUint64(packet[pos:pos+8], start.size)
	pos += 8
	binary.BigEndian.PutUint16(packet[pos:pos+2], uint16(len(start.streamIDs)))
	pos += 2
	for _, streamID := range start.streamIDs {
		if streamID == 0 {
			return nil, errors.New("udp stream id must be non-zero")
		}
		binary.BigEndian.PutUint32(packet[pos:pos+4], streamID)
		pos += 4
	}
	binary.BigEndian.PutUint32(packet[pos:pos+4], uint32(len(pathBytes)))
	pos += 4
	copy(packet[pos:], pathBytes)
	return packet, nil
}

func decodeUDPJobStartPacket(packet []byte) (udpJobStartPacket, bool, error) {
	minLen := len(udpJobMagic) + 1 + 4 + 8 + 2 + 4
	if len(packet) < minLen || string(packet[:len(udpJobMagic)]) != string(udpJobMagic) {
		return udpJobStartPacket{}, false, nil
	}
	pos := len(udpJobMagic)
	if packet[pos] != udpJobPacketStart {
		return udpJobStartPacket{}, false, nil
	}
	pos++
	start := udpJobStartPacket{}
	start.sessionKey = binary.BigEndian.Uint32(packet[pos : pos+4])
	pos += 4
	start.size = binary.BigEndian.Uint64(packet[pos : pos+8])
	pos += 8
	streamCount := int(binary.BigEndian.Uint16(packet[pos : pos+2]))
	pos += 2
	if streamCount == 0 || pos+streamCount*4+4 > len(packet) {
		return udpJobStartPacket{}, true, errors.New("invalid udp start packet stream ids")
	}
	start.streamIDs = make([]uint32, streamCount)
	for i := 0; i < streamCount; i++ {
		start.streamIDs[i] = binary.BigEndian.Uint32(packet[pos : pos+4])
		if start.streamIDs[i] == 0 {
			return udpJobStartPacket{}, true, errors.New("invalid zero udp stream id")
		}
		pos += 4
	}
	pathLen := int(binary.BigEndian.Uint32(packet[pos : pos+4]))
	pos += 4
	if pathLen == 0 || pos+pathLen != len(packet) {
		return udpJobStartPacket{}, true, errors.New("invalid udp start packet path")
	}
	start.relPath = string(packet[pos : pos+pathLen])
	return start, true, nil
}

func encodeUDPJobReadyPacket(sessionKey uint32) []byte {
	packet := make([]byte, len(udpJobMagic)+1+4)
	copy(packet, udpJobMagic)
	pos := len(udpJobMagic)
	packet[pos] = udpJobPacketReady
	pos++
	binary.BigEndian.PutUint32(packet[pos:pos+4], sessionKey)
	return packet
}

func isUDPJobReadyPacket(packet []byte, sessionKey uint32) bool {
	if len(packet) != len(udpJobMagic)+1+4 || string(packet[:len(udpJobMagic)]) != string(udpJobMagic) {
		return false
	}
	pos := len(udpJobMagic)
	if packet[pos] != udpJobPacketReady {
		return false
	}
	pos++
	return binary.BigEndian.Uint32(packet[pos:pos+4]) == sessionKey
}

func sendUDPJobStartAndWait(ctx context.Context, conn *net.UDPConn, relPath string, size uint64, sessionKey uint32, streamIDs []uint32) error {
	packet, err := encodeUDPJobStartPacket(udpJobStartPacket{
		sessionKey: sessionKey,
		size:       size,
		streamIDs:  streamIDs,
		relPath:    relPath,
	})
	if err != nil {
		return err
	}
	defer conn.SetReadDeadline(time.Time{})
	buf := make([]byte, len(udpJobMagic)+1+4)
	for attempt := 0; attempt < 10; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := writeUDPDatagram(conn, packet); err != nil {
			return err
		}
		if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			return err
		}
		n, err := conn.Read(buf)
		if err == nil && isUDPJobReadyPacket(buf[:n], sessionKey) {
			return nil
		}
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
	}
	return fmt.Errorf("timed out waiting for udp transfer endpoint to accept %s", relPath)
}

func writeUDPDatagram(conn *net.UDPConn, packet []byte) error {
	if len(packet) > maxUDPDatagramPayloadSize {
		return fmt.Errorf("udp datagram exceeds payload limit: %d", len(packet))
	}
	n, err := conn.Write(packet)
	if err != nil {
		return err
	}
	if n != len(packet) {
		return io.ErrShortWrite
	}
	return nil
}

func randomUint32() (uint32, error) {
	var buf [4]byte
	for {
		if _, err := cryptorand.Read(buf[:]); err != nil {
			return 0, err
		}
		v := binary.BigEndian.Uint32(buf[:])
		if v != 0 {
			return v, nil
		}
	}
}

func writeFull(ctx context.Context, w io.Writer, p []byte) error {
	for len(p) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func sendFileToTCPDestination(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan) error {
	src, err := os.Open(plan.SourcePath)
	if err != nil {
		return err
	}
	defer src.Close()
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(exec.DestData.GetHost(), fmt.Sprintf("%d", exec.DestData.GetPort())))
	if err != nil {
		return err
	}
	defer conn.Close()
	pathBytes := []byte(plan.RelativePath)
	if len(pathBytes) > 1<<20 {
		return fmt.Errorf("relative path too long: %s", plan.RelativePath)
	}
	if err := writeFull(ctx, conn, []byte("GROVERJOB1\n")); err != nil {
		return err
	}
	var header [12]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(pathBytes)))
	binary.BigEndian.PutUint64(header[4:], plan.Size)
	if err := writeFull(ctx, conn, header[:]); err != nil {
		return err
	}
	if err := writeFull(ctx, conn, pathBytes); err != nil {
		return err
	}
	buf := make([]byte, defaultTransferCopyBufferSize)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			if err := writeFull(ctx, conn, buf[:n]); err != nil {
				return err
			}
			if exec.OnProgress != nil {
				exec.OnProgress(plan.SourcePath, n)
			}
		}
		if readErr == io.EOF {
			if cw, ok := conn.(interface{ CloseWrite() error }); ok {
				_ = cw.CloseWrite()
			}
			var ack [2]byte
			if _, err := io.ReadFull(conn, ack[:]); err != nil {
				return err
			}
			if string(ack[:]) != "OK" {
				return errors.New("destination did not acknowledge tcp transfer")
			}
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func serveTransferEndpointTCP(listener *net.TCPListener, root string) {
	if listener == nil {
		return
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go receiveTransferFileTCP(conn, root)
	}
}

func serveTransferEndpointUDP(conn *net.UDPConn, root string, tuning udpTransferTuning) {
	if conn == nil {
		return
	}
	buf := make([]byte, 64*1024)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		start, ok, err := decodeUDPJobStartPacket(buf[:n])
		if !ok {
			continue
		}
		if err != nil {
			internal.Debug("drop udp transfer start packet", internal.Fields{internal.FieldError: err.Error()})
			continue
		}
		if err := receiveUDPJobFile(context.Background(), conn, root, addr, start, tuning); err != nil {
			internal.Debug("drop udp transfer packet", internal.Fields{internal.FieldError: err.Error()})
		}
	}
}

func receiveUDPJobFile(ctx context.Context, conn *net.UDPConn, root string, remote *net.UDPAddr, start udpJobStartPacket, tuning udpTransferTuning) error {
	if remote == nil {
		return errors.New("udp transfer start missing remote address")
	}
	relPath := filepath.Clean(filepath.FromSlash(start.relPath))
	if filepath.IsAbs(relPath) || strings.HasPrefix(relPath, "..") {
		return fmt.Errorf("invalid relative path %q", relPath)
	}
	dstPath := filepath.Join(root, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if start.size > uint64(1<<63-1) {
		return fmt.Errorf("udp transfer file too large: %d", start.size)
	}
	if err := f.Truncate(int64(start.size)); err != nil {
		return err
	}
	readyDone := make(chan struct{})
	go repeatUDPReady(conn, remote, start.sessionKey, readyDone)
	defer close(readyDone)
	if start.size == 0 {
		return f.Sync()
	}
	cfg := udpdataplane.ReceiveConfig{
		Transport:       udpdataplane.NewUDPConnTransport(conn),
		RemoteAddr:      remote,
		SessionID:       fmt.Sprintf("%d", start.sessionKey),
		SessionKey:      start.sessionKey,
		StreamID:        start.streamIDs[0],
		StreamIDs:       append([]uint32(nil), start.streamIDs...),
		BufferSize:      64 * 1024,
		ExpectedSize:    start.size,
		AckEveryPackets: tuning.ackEveryPackets,
		AckEvery:        tuning.ackEvery,
		BatchPackets:    tuning.batchPackets,
	}
	var receiveErr error
	if len(start.streamIDs) > 1 {
		_, receiveErr = udpdataplane.ReceiveMany(ctx, cfg, f)
	} else {
		_, receiveErr = udpdataplane.Receive(ctx, cfg, f)
	}
	if receiveErr != nil {
		return receiveErr
	}
	return f.Sync()
}

func repeatUDPReady(conn *net.UDPConn, remote *net.UDPAddr, sessionKey uint32, done <-chan struct{}) {
	packet := encodeUDPJobReadyPacket(sessionKey)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, _ = conn.WriteToUDP(packet, remote)
		select {
		case <-done:
			return
		case <-ticker.C:
		}
	}
}

func receiveTransferFileTCP(conn net.Conn, root string) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	magic, err := reader.ReadString('\n')
	if err != nil || magic != "GROVERJOB1\n" {
		return
	}
	var header [12]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return
	}
	pathLen := binary.BigEndian.Uint32(header[:4])
	size := binary.BigEndian.Uint64(header[4:])
	if pathLen == 0 || pathLen > 1<<20 {
		return
	}
	pathBytes := make([]byte, pathLen)
	if _, err := io.ReadFull(reader, pathBytes); err != nil {
		return
	}
	rel := filepath.Clean(filepath.FromSlash(string(pathBytes)))
	if filepath.IsAbs(rel) || rel == "." || strings.HasPrefix(rel, "..") {
		return
	}
	dstPath := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return
	}
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return
	}
	defer dst.Close()
	if _, err := io.CopyN(dst, reader, int64(size)); err != nil {
		return
	}
	if err := dst.Sync(); err != nil {
		return
	}
	_ = writeFull(context.Background(), conn, []byte("OK"))
}

func cloneTransferEndpoint(ep *pb.TransferEndpoint) *pb.TransferEndpoint {
	if ep == nil {
		return nil
	}
	return &pb.TransferEndpoint{
		EndpointId:    ep.GetEndpointId(),
		RouteId:       ep.GetRouteId(),
		JobId:         ep.GetJobId(),
		Role:          ep.GetRole(),
		Protocol:      ep.GetProtocol(),
		DataEndpoint:  cloneEndpoint(ep.GetDataEndpoint()),
		RootPath:      ep.GetRootPath(),
		TtlSeconds:    ep.GetTtlSeconds(),
		ExpiresAtUnix: ep.GetExpiresAtUnix(),
	}
}

var _ TransferEndpointRegistry = (*TransferJobManager)(nil)
var _ TransferJobExecutor = localFilesystemTransferExecutor{}
