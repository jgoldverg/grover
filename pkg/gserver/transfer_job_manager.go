package gserver

import (
	"bufio"
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/energy"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/metrics"
	"github.com/jgoldverg/grover/pkg/udpdataplane"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultTransferEndpointTTL    = 10 * time.Minute
	defaultTransferCopyBufferSize = 128 * 1024
	defaultReverseTCPConnections  = 32
	maxUDPDatagramPayloadSize     = 65507
	minUDPJobPayloadSize          = 512
)

const (
	tcpJobMagicV1 = "GROVERJOB1\n"
	tcpJobMagicV2 = "GROVERJOB2\n"
	tcpJobMagicV3 = "GROVERJOB3\n"
	tcpJobMagicV4 = "GROVERJOB4\n"
	udpJobMagicV2 = "GROVERJOBUDP2"

	currentTCPJobMagic = tcpJobMagicV4
	currentUDPJobMagic = udpJobMagicV2

	udpJobPacketStart byte = 1
	udpJobPacketReady byte = 2
	udpJobPacketDone  byte = 3
)

const syntheticTransferScheme = "synthetic"

var udpJobMagic = []byte(currentUDPJobMagic)

type TransferEndpointRegistry interface {
	PrepareEndpoint(context.Context, *pb.PrepareTransferEndpointRequest) (*pb.TransferEndpoint, error)
	GetEndpoint(id string) (*pb.TransferEndpoint, bool)
}

type TransferJobExecutor interface {
	PlanFiles(context.Context, *pb.TransferEndpoint, []string) ([]TransferFilePlan, error)
	TransferFile(context.Context, *TransferExecutionContext, TransferFilePlan) error
}

type TransferExecutionContext struct {
	JobID            string
	RouteID          string
	SourceRoot       string
	DestRoot         string
	DestData         *pb.DataEndpoint
	Protocol         pb.DataProtocol
	UDPPayload       int
	UDPFlow          string
	UDPWindow        int
	UDPBatch         int
	Collector        *metrics.TransferCollector
	TCPConnProvider  func(context.Context) (net.Conn, error)
	StreamsFunc      func() uint32
	OnProgress       func(filePath string, bytesRead int)
	OnStreamStart    func(filePath string, stream TransferStreamPlan)
	OnStreamProgress func(filePath string, streamID uint32, bytesRead int)
	OnStreamDone     func(filePath string, streamID uint32, state pb.RuntimeState, errText string)
}

type TransferFilePlan struct {
	SourcePath   string
	RelativePath string
	Size         uint64
}

type syntheticTransferSource struct {
	RelativePath string
	Size         uint64
}

type zeroReader struct{}

type TransferStreamPlan struct {
	StreamID uint32
	Offset   uint64
	Size     uint64
}

type TransferJobManager struct {
	mu          sync.RWMutex
	endpoints   map[string]*preparedTransferEndpoint
	jobs        map[string]*transferJobRuntime
	reverseTCP  map[string]*reverseTCPPool
	registry    TransferEndpointRegistry
	executor    TransferJobExecutor
	ports       *DataPortAllocator
	portErr     error
	udp         udpTransferTuning
	jobLogDir   string
	energy      *energy.RAPLMonitor
	energyEvery time.Duration
	stop        chan struct{}
	closeOnce   sync.Once
}

type udpTransferTuning struct {
	mtu             int
	flowControl     string
	windowPackets   int
	ackEveryPackets int
	ackEvery        time.Duration
	batchPackets    int
}

type preparedTransferEndpoint struct {
	endpoint *pb.TransferEndpoint
	expires  time.Time
	lease    *DataPortLease
	reverse  *reverseTCPPool
}

type reverseTCPPool struct {
	listener *net.TCPListener
	conns    chan net.Conn
	done     chan struct{}
	once     sync.Once
}

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

type localFilesystemTransferExecutor struct{}

type transferStreamKey struct {
	fileIndex int
	streamID  uint32
}

type transferJobHistory struct {
	dir          string
	snapshots    *os.File
	energyFile   *os.File
	energyWriter *csv.Writer
	energy       *energy.RAPLMonitor
	energyTick   uint64
	mu           sync.Mutex
}

type transferJobManifest struct {
	JobID           string                    `json:"job_id"`
	RouteID         string                    `json:"route_id,omitempty"`
	Protocol        string                    `json:"protocol"`
	SourceRoot      string                    `json:"source_root"`
	DestinationRoot string                    `json:"destination_root"`
	DestinationData string                    `json:"destination_data,omitempty"`
	FilesInFlight   uint32                    `json:"files_in_flight"`
	StreamsPerFile  uint32                    `json:"streams_per_file"`
	TotalFiles      int                       `json:"total_files"`
	TotalBytes      uint64                    `json:"total_bytes"`
	CreatedAt       time.Time                 `json:"created_at"`
	Files           []transferJobManifestFile `json:"files"`
}

type transferJobManifestFile struct {
	SourcePath   string `json:"source_path"`
	RelativePath string `json:"relative_path"`
	Size         uint64 `json:"size"`
}

func NewTransferJobManager(cfg *internal.ServerConfig, executor TransferJobExecutor) *TransferJobManager {
	if executor == nil {
		executor = localFilesystemTransferExecutor{}
	}
	jobLogDir := ""
	var energyMonitor *energy.RAPLMonitor
	energyEvery := time.Second
	if cfg != nil {
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
		endpoints:   make(map[string]*preparedTransferEndpoint),
		jobs:        make(map[string]*transferJobRuntime),
		reverseTCP:  make(map[string]*reverseTCPPool),
		executor:    executor,
		ports:       ports,
		portErr:     portErr,
		udp:         normalizedUDPTransferTuning(cfg),
		jobLogDir:   jobLogDir,
		energy:      energyMonitor,
		energyEvery: energyEvery,
		stop:        make(chan struct{}),
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
	connectionOrigin := req.GetConnectionOrigin()
	if connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_UNSPECIFIED {
		connectionOrigin = pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
	}
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	if ttl <= 0 {
		ttl = defaultTransferEndpointTTL
	}
	jobID := strings.TrimSpace(req.GetJobId())
	if jobID == "" {
		jobID = uuid.NewString()
	}
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		sessionID = jobID
	}
	var lease *DataPortLease
	dataEndpoint := cloneEndpoint(req.GetBind())
	needsListener := req.GetRole() == pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION &&
		connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE &&
		(protocol == pb.DataProtocol_DATA_PROTOCOL_TCP || protocol == pb.DataProtocol_DATA_PROTOCOL_UDP)
	needsReverseSourceListener := req.GetRole() == pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE &&
		connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION &&
		protocol == pb.DataProtocol_DATA_PROTOCOL_TCP
	if needsListener || needsReverseSourceListener {
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
	if req.GetRole() == pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION &&
		connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION &&
		protocol == pb.DataProtocol_DATA_PROTOCOL_TCP {
		dataEndpoint = cloneEndpoint(req.GetBind())
		if dataEndpoint == nil || strings.TrimSpace(dataEndpoint.GetHost()) == "" || dataEndpoint.GetPort() == 0 {
			return nil, errors.New("destination-origin tcp endpoint requires source bind endpoint")
		}
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
		SessionId:     sessionID,
	}
	var reverse *reverseTCPPool
	if lease != nil && needsReverseSourceListener {
		reverse = newReverseTCPPool(lease.TCPListener)
		m.mu.Lock()
		m.reverseTCP[sessionID] = reverse
		m.mu.Unlock()
		go reverse.acceptLoop()
	} else if req.GetRole() == pb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION &&
		connectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION &&
		protocol == pb.DataProtocol_DATA_PROTOCOL_TCP {
		go connectDestinationOriginTCPReceivers(context.Background(), dataEndpoint, root)
	} else if lease != nil && protocol == pb.DataProtocol_DATA_PROTOCOL_TCP {
		go serveTransferEndpointTCP(lease.TCPListener, root)
	} else if lease != nil && protocol == pb.DataProtocol_DATA_PROTOCOL_UDP {
		go serveTransferEndpointUDP(lease.UDPConn, root, m.udp)
	}
	m.mu.Lock()
	m.endpoints[endpoint.EndpointId] = &preparedTransferEndpoint{endpoint: endpoint, expires: time.Now().Add(ttl), lease: lease, reverse: reverse}
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
	filesInFlight := req.GetFilesInFlight()
	if filesInFlight == 0 {
		filesInFlight = 1
	}
	streamsPerFile := req.GetStreamsPerFile()
	if streamsPerFile == 0 {
		streamsPerFile = 1
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

	plans, err := m.executor.PlanFiles(ctx, source, req.GetPaths())
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
		"files_in_flight":  filesInFlight,
		"streams_per_file": streamsPerFile,
		"udp_flow_control": m.udp.flowControl,
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
			if endpoint.endpoint != nil {
				delete(m.reverseTCP, strings.TrimSpace(endpoint.endpoint.GetJobId()))
				delete(m.reverseTCP, strings.TrimSpace(endpoint.endpoint.GetSessionId()))
			}
			expired = append(expired, endpoint)
		}
	}
	m.mu.Unlock()
	for _, endpoint := range expired {
		closePreparedTransferEndpoint(endpoint)
	}
}

func closePreparedTransferEndpoint(endpoint *preparedTransferEndpoint) {
	if endpoint != nil && endpoint.reverse != nil {
		endpoint.reverse.close()
	}
	if endpoint != nil && endpoint.lease != nil {
		_ = endpoint.lease.Close()
	}
}

func newReverseTCPPool(listener *net.TCPListener) *reverseTCPPool {
	return &reverseTCPPool{
		listener: listener,
		conns:    make(chan net.Conn, 128),
		done:     make(chan struct{}),
	}
}

func (p *reverseTCPPool) acceptLoop() {
	if p == nil || p.listener == nil {
		return
	}
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			p.close()
			return
		}
		select {
		case p.conns <- conn:
		case <-p.done:
			_ = conn.Close()
			return
		}
	}
}

func (p *reverseTCPPool) take(ctx context.Context) (net.Conn, error) {
	if p == nil {
		return nil, errors.New("reverse tcp pool is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case conn := <-p.conns:
		if conn == nil {
			return nil, errors.New("reverse tcp connection closed")
		}
		return conn, nil
	case <-p.done:
		return nil, errors.New("reverse tcp pool closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, errors.New("timed out waiting for destination-origin tcp connection")
	}
}

func (p *reverseTCPPool) close() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		close(p.done)
		if p.listener != nil {
			_ = p.listener.Close()
		}
		for {
			select {
			case conn := <-p.conns:
				if conn != nil {
					_ = conn.Close()
				}
			default:
				return
			}
		}
	})
}

func (m *TransferJobManager) takeReverseTCPConn(ctx context.Context, jobID string) (net.Conn, error) {
	m.mu.RLock()
	pool := m.reverseTCP[strings.TrimSpace(jobID)]
	m.mu.RUnlock()
	if pool == nil {
		return nil, fmt.Errorf("reverse tcp route session for job %q not found", strings.TrimSpace(jobID))
	}
	return pool.take(ctx)
}

func connectDestinationOriginTCPReceivers(ctx context.Context, source *pb.DataEndpoint, root string) {
	if source == nil || strings.TrimSpace(source.GetHost()) == "" || source.GetPort() == 0 {
		return
	}
	target := net.JoinHostPort(source.GetHost(), fmt.Sprintf("%d", source.GetPort()))
	for i := 0; i < defaultReverseTCPConnections; i++ {
		go func() {
			dialer := net.Dialer{Timeout: 10 * time.Second}
			conn, err := dialer.DialContext(ctx, "tcp", target)
			if err != nil {
				internal.Warn("destination-origin tcp dial failed", internal.Fields{
					internal.FieldError: err.Error(),
					"source":            target,
				})
				return
			}
			receiveTransferFileTCP(conn, root)
		}()
	}
}

func normalizedUDPTransferTuning(cfg *internal.ServerConfig) udpTransferTuning {
	tuning := udpTransferTuning{
		mtu:             1200,
		flowControl:     "fixed",
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
	if strings.TrimSpace(cfg.UDPFlowControl) != "" {
		tuning.flowControl = strings.ToLower(strings.TrimSpace(cfg.UDPFlowControl))
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
			execCtx := &TransferExecutionContext{
				JobID:      runtime.jobID,
				RouteID:    runtime.routeID,
				SourceRoot: runtime.source.GetRootPath(),
				DestRoot:   runtime.dest.GetRootPath(),
				DestData:   cloneEndpoint(runtime.dest.GetDataEndpoint()),
				Protocol:   runtime.protocol,
				UDPPayload: m.udp.mtu,
				UDPFlow:    m.udp.flowControl,
				UDPWindow:  m.udp.windowPackets,
				UDPBatch:   m.udp.batchPackets,
				Collector:  runtime.collector,
				TCPConnProvider: func(ctx context.Context) (net.Conn, error) {
					if runtime.origin != pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
						return nil, nil
					}
					return m.takeReverseTCPConn(ctx, runtime.sessionID)
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

func newTransferJobHistory(baseDir, jobID string, monitor *energy.RAPLMonitor) (*transferJobHistory, error) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		return nil, errors.New("job log directory is empty")
	}
	dir := filepath.Join(baseDir, safeJobLogName(jobID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	snapshots, err := os.OpenFile(filepath.Join(dir, "snapshots.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	history := &transferJobHistory{dir: dir, snapshots: snapshots, energy: monitor}
	if monitor != nil {
		energyFile, err := os.OpenFile(filepath.Join(dir, "energy.csv"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			_ = snapshots.Close()
			return nil, err
		}
		history.energyFile = energyFile
		history.energyWriter = csv.NewWriter(energyFile)
		if err := monitor.WriteCSVHeader(history.energyWriter); err != nil {
			_ = snapshots.Close()
			_ = energyFile.Close()
			return nil, err
		}
	}
	return history, nil
}

func safeJobLogName(jobID string) string {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return "unknown-job"
	}
	var b strings.Builder
	for _, r := range jobID {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "unknown-job"
	}
	return b.String()
}

func (h *transferJobHistory) writeManifest(runtime *transferJobRuntime, plans []TransferFilePlan) error {
	if h == nil {
		return nil
	}
	files := make([]transferJobManifestFile, 0, len(plans))
	for _, plan := range plans {
		files = append(files, transferJobManifestFile{
			SourcePath:   plan.SourcePath,
			RelativePath: plan.RelativePath,
			Size:         plan.Size,
		})
	}
	manifest := transferJobManifest{
		JobID:           runtime.jobID,
		RouteID:         runtime.routeID,
		Protocol:        runtime.protocol.String(),
		SourceRoot:      runtime.source.GetRootPath(),
		DestinationRoot: runtime.dest.GetRootPath(),
		DestinationData: endpointLabel(runtime.dest.GetDataEndpoint()),
		FilesInFlight:   runtime.filesInFlight,
		StreamsPerFile:  runtime.streamsPerFile,
		TotalFiles:      len(plans),
		TotalBytes:      totalPlannedBytes(plans),
		CreatedAt:       runtime.startedAt,
		Files:           files,
	}
	return h.writeJSONFile("manifest.json", manifest)
}

func (h *transferJobHistory) writeFinal(job *pb.TransferJob) error {
	if h == nil || job == nil {
		return nil
	}
	payload, err := protojson.MarshalOptions{
		Multiline:       true,
		EmitUnpopulated: true,
	}.Marshal(job)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	return os.WriteFile(filepath.Join(h.dir, "final.json"), payload, 0o644)
}

func (h *transferJobHistory) writeJSONFile(name string, value any) error {
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	return os.WriteFile(filepath.Join(h.dir, name), payload, 0o644)
}

func (h *transferJobHistory) snapshotLoop(runtime *transferJobRuntime, stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			h.appendSnapshot(runtime.snapshot())
		}
	}
}

func (h *transferJobHistory) energyLoop(runtime *transferJobRuntime, interval time.Duration, stop <-chan struct{}) {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			h.appendEnergy(runtime, now)
		}
	}
}

func (h *transferJobHistory) appendSnapshot(job *pb.TransferJob) {
	if h == nil || job == nil {
		return
	}
	payload, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(job)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.snapshots == nil {
		return
	}
	_, _ = h.snapshots.Write(append(payload, '\n'))
}

func (h *transferJobHistory) appendEnergy(runtime *transferJobRuntime, now time.Time) {
	if h == nil || h.energy == nil || runtime == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.energyWriter == nil {
		return
	}
	if err := h.energy.WriteCSVRecord(h.energyWriter, h.energyTick, runtime.jobID, runtime.routeID, now); err != nil {
		internal.Warn(jobLogMessage(runtime.jobID, "failed to sample energy"), internal.Fields{
			internal.FieldError: err.Error(),
			"job_log_path":      h.dir,
		})
		return
	}
	h.energyTick++
}

func (h *transferJobHistory) close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var err error
	if h.snapshots != nil {
		err = h.snapshots.Close()
		h.snapshots = nil
	}
	if h.energyWriter != nil {
		h.energyWriter.Flush()
		if writerErr := h.energyWriter.Error(); err == nil {
			err = writerErr
		}
		h.energyWriter = nil
	}
	if h.energyFile != nil {
		if closeErr := h.energyFile.Close(); err == nil {
			err = closeErr
		}
		h.energyFile = nil
	}
	return err
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

func (r *transferJobRuntime) addProgress(index int, n uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index >= 0 && index < len(r.files) {
		r.files[index].BytesDone += n
	}
}

func (r *transferJobRuntime) startStream(index int, plan TransferStreamPlan) {
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
		JobId:          r.jobID,
		RouteId:        r.routeID,
		SessionId:      r.sessionID,
		State:          r.state,
		Protocol:       r.protocol,
		Source:         cloneTransferEndpoint(r.source),
		Destination:    cloneTransferEndpoint(r.dest),
		FilesInFlight:  r.filesInFlight,
		StreamsPerFile: r.streamsPerFile,
		GoodBytes:      goodBytes,
		NetworkBytes:   observedBytes,
		DiskReadBytes:  diskReadBytes,
		DiskWriteBytes: diskWriteBytes,
		FilesDone:      r.doneCount,
		FilesActive:    r.active,
		StreamsActive:  activeStreams,
		Retransmits:    protocolStats.Retransmissions,
		Files:          files,
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

func (localFilesystemTransferExecutor) PlanFiles(ctx context.Context, source *pb.TransferEndpoint, paths []string) ([]TransferFilePlan, error) {
	root := filepath.Clean(source.GetRootPath())
	var plans []TransferFilePlan
	relativePathForFile := func(full string) (string, error) {
		rel, err := filepath.Rel(root, full)
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
			rel = filepath.Base(full)
		}
		rel = filepath.Clean(rel)
		if rel == "." || filepath.IsAbs(rel) || strings.HasPrefix(rel, "..") {
			return "", fmt.Errorf("invalid transfer relative path %q for %q", rel, full)
		}
		return filepath.ToSlash(rel), nil
	}
	addPath := func(p string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if synthetic, ok, err := parseSyntheticTransferSource(p); ok || err != nil {
			if err != nil {
				return err
			}
			plans = append(plans, TransferFilePlan{
				SourcePath:   p,
				RelativePath: synthetic.RelativePath,
				Size:         synthetic.Size,
			})
			return nil
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
				rel, err := relativePathForFile(path)
				if err != nil {
					return err
				}
				plans = append(plans, TransferFilePlan{SourcePath: path, RelativePath: rel, Size: uint64(info.Size())})
				return nil
			})
		}
		rel, err := relativePathForFile(full)
		if err != nil {
			return err
		}
		plans = append(plans, TransferFilePlan{SourcePath: full, RelativePath: rel, Size: uint64(info.Size())})
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

func resolveDestinationFilePath(root, relPath string) (string, error) {
	root = filepath.Clean(root)
	rel := filepath.Clean(filepath.FromSlash(relPath))
	if root == "" || root == "." {
		return "", errors.New("destination root is required")
	}
	if filepath.IsAbs(rel) || rel == "." || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("invalid transfer relative path %q", rel)
	}
	if info, err := os.Stat(root); err == nil && info.IsDir() {
		return filepath.Join(root, rel), nil
	}
	if filepath.Base(root) == filepath.Base(rel) {
		return root, nil
	}
	return filepath.Join(root, rel), nil
}

func (localFilesystemTransferExecutor) TransferFile(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan) error {
	if exec.DestData != nil && strings.TrimSpace(exec.DestData.GetHost()) != "" && exec.DestData.GetPort() != 0 {
		if exec.Protocol == pb.DataProtocol_DATA_PROTOCOL_UDP {
			return sendFileToUDPDestination(ctx, exec, plan)
		}
		return sendFileToTCPDestination(ctx, exec, plan)
	}
	dstPath, err := resolveDestinationFilePath(exec.DestRoot, plan.RelativePath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	src, err := openTransferPlanReader(plan, 0, int64(plan.Size))
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer dst.Close()
	reportStreamStart(exec, plan, 1, 0, plan.Size)
	buf := make([]byte, defaultTransferCopyBufferSize)
	for {
		if err := ctx.Err(); err != nil {
			reportStreamDone(exec, plan, 1, pb.RuntimeState_RUNTIME_STATE_ABORTED, err.Error())
			return err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			if exec.Collector != nil && transferPlanReadsDisk(plan) {
				exec.Collector.ObserveDiskRead(n)
			}
			if err := writeFull(ctx, dst, buf[:n]); err != nil {
				reportStreamDone(exec, plan, 1, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
				return err
			}
			if exec.Collector != nil {
				exec.Collector.ObserveDiskWrite(n)
				exec.Collector.ObserveSend(n, false)
			}
			reportStreamProgress(exec, plan, 1, n)
		}
		if readErr == io.EOF {
			if err := dst.Sync(); err != nil {
				reportStreamDone(exec, plan, 1, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
				return err
			}
			reportStreamDone(exec, plan, 1, pb.RuntimeState_RUNTIME_STATE_DONE, "")
			return nil
		}
		if readErr != nil {
			reportStreamDone(exec, plan, 1, pb.RuntimeState_RUNTIME_STATE_FAILED, readErr.Error())
			return readErr
		}
	}
}

func reportStreamStart(exec *TransferExecutionContext, plan TransferFilePlan, streamID uint32, offset uint64, size uint64) {
	if exec != nil && exec.OnStreamStart != nil {
		exec.OnStreamStart(plan.SourcePath, TransferStreamPlan{StreamID: streamID, Offset: offset, Size: size})
	}
}

func reportStreamProgress(exec *TransferExecutionContext, plan TransferFilePlan, streamID uint32, n int) {
	if exec == nil || n <= 0 {
		return
	}
	if exec.OnProgress != nil {
		exec.OnProgress(plan.SourcePath, n)
	}
	if exec.OnStreamProgress != nil {
		exec.OnStreamProgress(plan.SourcePath, streamID, n)
	}
}

func reportStreamDone(exec *TransferExecutionContext, plan TransferFilePlan, streamID uint32, state pb.RuntimeState, errText string) {
	if exec != nil && exec.OnStreamDone != nil {
		exec.OnStreamDone(plan.SourcePath, streamID, state, errText)
	}
}

func parseSyntheticTransferSource(raw string) (syntheticTransferSource, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return syntheticTransferSource{}, false, nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != syntheticTransferScheme {
		return syntheticTransferSource{}, false, nil
	}
	size, err := parseSyntheticSize(u.Host)
	if err != nil {
		return syntheticTransferSource{}, true, err
	}
	rel := strings.TrimPrefix(u.EscapedPath(), "/")
	if rel == "" {
		rel = fmt.Sprintf("synthetic-%d.bin", size)
	}
	if decoded, err := url.PathUnescape(rel); err == nil {
		rel = decoded
	}
	rel = filepath.Clean(filepath.FromSlash(rel))
	if filepath.IsAbs(rel) || rel == "." || strings.HasPrefix(rel, "..") {
		return syntheticTransferSource{}, true, fmt.Errorf("invalid synthetic transfer path %q", rel)
	}
	return syntheticTransferSource{RelativePath: filepath.ToSlash(rel), Size: size}, true, nil
}

func parseSyntheticSize(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New("synthetic transfer size is required")
	}
	var size uint64
	if _, err := fmt.Sscanf(raw, "%d", &size); err != nil {
		return 0, fmt.Errorf("invalid synthetic transfer size %q", raw)
	}
	return size, nil
}

func openTransferPlanReader(plan TransferFilePlan, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("invalid transfer reader range: offset=%d length=%d", offset, length)
	}
	if _, ok, err := parseSyntheticTransferSource(plan.SourcePath); ok || err != nil {
		if err != nil {
			return nil, err
		}
		return io.NopCloser(io.LimitReader(zeroReader{}, length)), nil
	}
	src, err := os.Open(plan.SourcePath)
	if err != nil {
		return nil, err
	}
	if offset == 0 {
		return struct {
			io.Reader
			io.Closer
		}{Reader: io.LimitReader(src, length), Closer: src}, nil
	}
	if _, err := src.Seek(offset, io.SeekStart); err != nil {
		_ = src.Close()
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{Reader: io.LimitReader(src, length), Closer: src}, nil
}

func transferPlanReadsDisk(plan TransferFilePlan) bool {
	_, ok, err := parseSyntheticTransferSource(plan.SourcePath)
	return err == nil && !ok
}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func sendFileToUDPDestination(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan) error {
	src, err := openTransferPlanReader(plan, 0, int64(plan.Size))
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
	if err := sendUDPJobStartAndWait(ctx, conn, exec, plan.RelativePath, plan.Size, sessionKey, streamIDs); err != nil {
		return err
	}
	if plan.Size == 0 {
		reportStreamStart(exec, plan, streamIDs[0], 0, 0)
		if err := waitUDPJobDone(ctx, conn, sessionKey); err != nil {
			reportStreamDone(exec, plan, streamIDs[0], pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
			return err
		}
		reportStreamDone(exec, plan, streamIDs[0], pb.RuntimeState_RUNTIME_STATE_DONE, "")
		return nil
	}
	if len(streamIDs) == 1 {
		reportStreamStart(exec, plan, streamIDs[0], 0, plan.Size)
		_, err := udpdataplane.Send(ctx, udpdataplane.SendConfig{
			Transport:       udpdataplane.NewUDPConnTransport(conn),
			SessionID:       exec.JobID,
			SessionKey:      sessionKey,
			StreamID:        streamIDs[0],
			MTU:             exec.UDPPayload,
			Collector:       exec.Collector,
			FlowControl:     udpFlowControl(exec),
			WindowPackets:   udpWindowPackets(exec),
			BatchPackets:    udpBatchPackets(exec),
			RequireFinalAck: false,
		}, newProgressReader(src, func(n int) {
			if exec.Collector != nil && transferPlanReadsDisk(plan) {
				exec.Collector.ObserveDiskRead(n)
			}
			reportStreamProgress(exec, plan, streamIDs[0], n)
		}))
		if err != nil {
			reportStreamDone(exec, plan, streamIDs[0], pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
			return err
		}
		if err := waitUDPJobDone(ctx, conn, sessionKey); err != nil {
			reportStreamDone(exec, plan, streamIDs[0], pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
			return err
		}
		reportStreamDone(exec, plan, streamIDs[0], pb.RuntimeState_RUNTIME_STATE_DONE, "")
		return nil
	}
	if err := sendFileToUDPDestinationParallel(ctx, exec, plan, sessionKey, streamIDs); err != nil {
		return err
	}
	return waitUDPJobDone(ctx, conn, sessionKey)
}

func sendFileToUDPDestinationParallel(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan, sessionKey uint32, streamIDs []uint32) error {
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
			reportStreamStart(exec, plan, streamID, uint64(br.offset), uint64(br.length))
			conn, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
				errCh <- err
				return
			}
			defer conn.Close()
			sr, err := openTransferPlanReader(plan, br.offset, br.length)
			if err != nil {
				reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
				errCh <- err
				return
			}
			defer sr.Close()
			_, err = udpdataplane.Send(ctx, udpdataplane.SendConfig{
				Transport:       udpdataplane.NewUDPConnTransport(conn),
				SessionID:       exec.JobID,
				SessionKey:      sessionKey,
				StreamID:        streamID,
				BaseOffset:      uint64(br.offset),
				MTU:             exec.UDPPayload,
				Collector:       exec.Collector,
				FlowControl:     udpFlowControl(exec),
				WindowPackets:   udpWindowPackets(exec),
				BatchPackets:    udpBatchPackets(exec),
				RequireFinalAck: false,
			}, newProgressReader(sr, func(n int) {
				if exec.Collector != nil && transferPlanReadsDisk(plan) {
					exec.Collector.ObserveDiskRead(n)
				}
				reportStreamProgress(exec, plan, streamID, n)
			}))
			if err != nil {
				reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
				errCh <- err
				return
			}
			reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_DONE, "")
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

func udpFlowControl(exec *TransferExecutionContext) string {
	if exec != nil {
		switch strings.ToLower(strings.TrimSpace(exec.UDPFlow)) {
		case "bbr":
			return "bbr"
		}
	}
	return "fixed"
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
	jobLen := 0
	routeLen := 0
	if exec != nil {
		jobLen = len([]byte(exec.JobID))
		routeLen = len([]byte(exec.RouteID))
	}
	headerLen := len(udpJobMagic) + 1 + 4 + 8 + 2 + 4 + 2 + 2 + len([]byte(relPath)) + jobLen + routeLen
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
	jobID      string
	routeID    string
}

func encodeUDPJobStartPacket(start udpJobStartPacket) ([]byte, error) {
	pathBytes := []byte(start.relPath)
	jobBytes := []byte(strings.TrimSpace(start.jobID))
	routeBytes := []byte(strings.TrimSpace(start.routeID))
	if len(pathBytes) == 0 {
		return nil, errors.New("udp job path is required")
	}
	if len(start.streamIDs) == 0 {
		return nil, errors.New("at least one udp stream id is required")
	}
	if len(start.streamIDs) > 1<<16-1 {
		return nil, fmt.Errorf("too many udp streams: %d", len(start.streamIDs))
	}
	if len(jobBytes) > 1<<16-1 || len(routeBytes) > 1<<16-1 {
		return nil, fmt.Errorf("udp job metadata exceeds packet limit: job_id=%d route_id=%d", len(jobBytes), len(routeBytes))
	}
	headerLen := len(udpJobMagic) + 1 + 4 + 8 + 2 + len(start.streamIDs)*4 + 4 + 2 + 2
	if headerLen+len(pathBytes)+len(jobBytes)+len(routeBytes) > maxUDPDatagramPayloadSize {
		return nil, fmt.Errorf("udp start packet exceeds datagram payload limit: path=%d job_id=%d route_id=%d streams=%d", len(pathBytes), len(jobBytes), len(routeBytes), len(start.streamIDs))
	}
	packet := make([]byte, headerLen+len(pathBytes)+len(jobBytes)+len(routeBytes))
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
	binary.BigEndian.PutUint16(packet[pos:pos+2], uint16(len(jobBytes)))
	pos += 2
	binary.BigEndian.PutUint16(packet[pos:pos+2], uint16(len(routeBytes)))
	pos += 2
	copy(packet[pos:], pathBytes)
	pos += len(pathBytes)
	copy(packet[pos:], jobBytes)
	pos += len(jobBytes)
	copy(packet[pos:], routeBytes)
	return packet, nil
}

func decodeUDPJobStartPacket(packet []byte) (udpJobStartPacket, bool, error) {
	minLen := len(udpJobMagic) + 1 + 4 + 8 + 2 + 4 + 2 + 2
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
	jobLen := int(binary.BigEndian.Uint16(packet[pos : pos+2]))
	pos += 2
	routeLen := int(binary.BigEndian.Uint16(packet[pos : pos+2]))
	pos += 2
	if pathLen == 0 || pos+pathLen+jobLen+routeLen != len(packet) {
		return udpJobStartPacket{}, true, errors.New("invalid udp start packet path")
	}
	start.relPath = string(packet[pos : pos+pathLen])
	pos += pathLen
	start.jobID = string(packet[pos : pos+jobLen])
	pos += jobLen
	start.routeID = string(packet[pos : pos+routeLen])
	return start, true, nil
}

func encodeUDPJobReadyPacket(sessionKey uint32) []byte {
	return encodeUDPJobControlPacket(udpJobPacketReady, sessionKey)
}

func encodeUDPJobDonePacket(sessionKey uint32) []byte {
	return encodeUDPJobControlPacket(udpJobPacketDone, sessionKey)
}

func encodeUDPJobControlPacket(kind byte, sessionKey uint32) []byte {
	packet := make([]byte, len(udpJobMagic)+1+4)
	copy(packet, udpJobMagic)
	pos := len(udpJobMagic)
	packet[pos] = kind
	pos++
	binary.BigEndian.PutUint32(packet[pos:pos+4], sessionKey)
	return packet
}

func isUDPJobReadyPacket(packet []byte, sessionKey uint32) bool {
	return isUDPJobControlPacket(packet, udpJobPacketReady, sessionKey)
}

func isUDPJobDonePacket(packet []byte, sessionKey uint32) bool {
	return isUDPJobControlPacket(packet, udpJobPacketDone, sessionKey)
}

func isUDPJobControlPacket(packet []byte, kind byte, sessionKey uint32) bool {
	if len(packet) != len(udpJobMagic)+1+4 || string(packet[:len(udpJobMagic)]) != string(udpJobMagic) {
		return false
	}
	pos := len(udpJobMagic)
	if packet[pos] != kind {
		return false
	}
	pos++
	return binary.BigEndian.Uint32(packet[pos:pos+4]) == sessionKey
}

func sendUDPJobStartAndWait(ctx context.Context, conn *net.UDPConn, exec *TransferExecutionContext, relPath string, size uint64, sessionKey uint32, streamIDs []uint32) error {
	packet, err := encodeUDPJobStartPacket(udpJobStartPacket{
		sessionKey: sessionKey,
		size:       size,
		streamIDs:  streamIDs,
		relPath:    relPath,
		jobID:      exec.JobID,
		routeID:    exec.RouteID,
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

func waitUDPJobDone(ctx context.Context, conn *net.UDPConn, sessionKey uint32) error {
	deadline := time.Now().Add(5 * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	defer conn.SetReadDeadline(time.Time{})
	buf := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			return err
		}
		n, err := conn.Read(buf)
		if err == nil {
			if isUDPJobDonePacket(buf[:n], sessionKey) {
				return nil
			}
			continue
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			if time.Now().After(deadline) {
				return fmt.Errorf("timed out waiting for udp transfer completion acknowledgement")
			}
			continue
		}
		return err
	}
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

type progressReader struct {
	r      io.Reader
	onRead func(int)
}

func newProgressReader(r io.Reader, onRead func(int)) io.Reader {
	return progressReader{r: r, onRead: onRead}
}

func (r progressReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 && r.onRead != nil {
		r.onRead(n)
	}
	return n, err
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
	streamCount := 1
	if exec.StreamsFunc != nil {
		streamCount = int(exec.StreamsFunc())
	}
	if streamCount <= 0 || plan.Size == 0 {
		streamCount = 1
	}
	if uint64(streamCount) > plan.Size && plan.Size > 0 {
		streamCount = int(plan.Size)
	}
	ranges, err := planByteRanges(int64(plan.Size), streamCount)
	if err != nil {
		return err
	}
	if len(ranges) == 0 {
		ranges = []byteRange{{offset: 0, length: 0}}
	}
	if len(ranges) == 1 {
		return sendTCPRange(ctx, exec, plan, 1, ranges[0])
	}

	errCh := make(chan error, len(ranges))
	var wg sync.WaitGroup
	for i, br := range ranges {
		streamID := uint32(i + 1)
		wg.Add(1)
		go func(streamID uint32, br byteRange) {
			defer wg.Done()
			if err := sendTCPRange(ctx, exec, plan, streamID, br); err != nil {
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

func sendTCPRange(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan, streamID uint32, br byteRange) (err error) {
	if streamID == 0 {
		streamID = 1
	}
	if br.offset < 0 || br.length < 0 {
		return fmt.Errorf("invalid tcp byte range: offset=%d length=%d", br.offset, br.length)
	}
	src, err := openTransferPlanReader(plan, br.offset, br.length)
	if err != nil {
		return err
	}
	defer src.Close()

	reportStreamStart(exec, plan, streamID, uint64(br.offset), uint64(br.length))
	defer func() {
		if err != nil {
			reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
			return
		}
		reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_DONE, "")
	}()

	var conn net.Conn
	if exec.TCPConnProvider != nil {
		conn, err = exec.TCPConnProvider(ctx)
		if err != nil {
			return err
		}
	}
	if conn == nil {
		dialer := net.Dialer{}
		conn, err = dialer.DialContext(ctx, "tcp", net.JoinHostPort(exec.DestData.GetHost(), fmt.Sprintf("%d", exec.DestData.GetPort())))
		if err != nil {
			return err
		}
	}
	defer conn.Close()

	pathBytes := []byte(plan.RelativePath)
	if len(pathBytes) > 1<<20 {
		return fmt.Errorf("relative path too long: %s", plan.RelativePath)
	}
	if err := writeFull(ctx, conn, []byte(currentTCPJobMagic)); err != nil {
		return err
	}
	jobBytes := []byte(strings.TrimSpace(exec.JobID))
	routeBytes := []byte(strings.TrimSpace(exec.RouteID))
	destRootBytes := []byte(strings.TrimSpace(exec.DestRoot))
	if len(jobBytes) > 1<<16-1 || len(routeBytes) > 1<<16-1 || len(destRootBytes) > 1<<20 {
		return fmt.Errorf("tcp job metadata too long: job_id=%d route_id=%d dest_root=%d", len(jobBytes), len(routeBytes), len(destRootBytes))
	}
	var header [40]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(pathBytes)))
	binary.BigEndian.PutUint64(header[4:12], plan.Size)
	binary.BigEndian.PutUint64(header[12:20], uint64(br.offset))
	binary.BigEndian.PutUint64(header[20:28], uint64(br.length))
	binary.BigEndian.PutUint32(header[28:32], streamID)
	binary.BigEndian.PutUint16(header[32:34], uint16(len(jobBytes)))
	binary.BigEndian.PutUint16(header[34:36], uint16(len(routeBytes)))
	binary.BigEndian.PutUint32(header[36:40], uint32(len(destRootBytes)))
	if err := writeFull(ctx, conn, header[:]); err != nil {
		return err
	}
	if err := writeFull(ctx, conn, pathBytes); err != nil {
		return err
	}
	if err := writeFull(ctx, conn, jobBytes); err != nil {
		return err
	}
	if err := writeFull(ctx, conn, routeBytes); err != nil {
		return err
	}
	if err := writeFull(ctx, conn, destRootBytes); err != nil {
		return err
	}

	reader := src
	buf := make([]byte, defaultTransferCopyBufferSize)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := reader.Read(buf)
		if n > 0 {
			if exec.Collector != nil && transferPlanReadsDisk(plan) {
				exec.Collector.ObserveDiskRead(n)
			}
			if err := writeFull(ctx, conn, buf[:n]); err != nil {
				return err
			}
			if exec.Collector != nil {
				exec.Collector.ObserveSend(n, false)
			}
			reportStreamProgress(exec, plan, streamID, n)
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
			internal.Debug("tcp transfer listener closed", internal.Fields{internal.FieldError: err.Error()})
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
			internal.Warn("drop invalid udp transfer start packet", internal.Fields{internal.FieldError: err.Error(), "remote": addr.String()})
			continue
		}
		if err := receiveUDPJobFile(context.Background(), conn, root, addr, start, tuning); err != nil {
			internal.Warn("udp transfer receive failed", internal.Fields{internal.FieldError: err.Error(), "remote": addr.String()})
		}
	}
}

func receiveUDPJobFile(ctx context.Context, conn *net.UDPConn, root string, remote *net.UDPAddr, start udpJobStartPacket, tuning udpTransferTuning) error {
	if remote == nil {
		return errors.New("udp transfer start missing remote address")
	}
	jobID := start.jobID
	relPath := filepath.Clean(filepath.FromSlash(start.relPath))
	if filepath.IsAbs(relPath) || relPath == "." || strings.HasPrefix(relPath, "..") {
		return fmt.Errorf("invalid relative path %q", relPath)
	}
	dstPath, err := resolveDestinationFilePath(root, relPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	internal.Info(jobLogMessage(jobID, "udp receive started"), internal.Fields{
		"route_id":   start.routeID,
		"remote":     remote.String(),
		"root":       root,
		"path":       relPath,
		"bytes":      start.size,
		"streams":    len(start.streamIDs),
		"session_id": start.sessionKey,
	})
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
		if err := f.Sync(); err != nil {
			return err
		}
		sendUDPJobDoneBurst(conn, remote, start.sessionKey)
		internal.Info(jobLogMessage(jobID, "udp receive completed"), internal.Fields{
			"route_id":   start.routeID,
			"remote":     remote.String(),
			"path":       relPath,
			"bytes":      start.size,
			"streams":    len(start.streamIDs),
			"session_id": start.sessionKey,
		})
		return nil
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
	if err := f.Sync(); err != nil {
		return err
	}
	sendUDPJobDoneBurst(conn, remote, start.sessionKey)
	internal.Info(jobLogMessage(jobID, "udp receive completed"), internal.Fields{
		"route_id":   start.routeID,
		"remote":     remote.String(),
		"path":       relPath,
		"bytes":      start.size,
		"streams":    len(start.streamIDs),
		"session_id": start.sessionKey,
	})
	return nil
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

func sendUDPJobDoneBurst(conn *net.UDPConn, remote *net.UDPAddr, sessionKey uint32) {
	packet := encodeUDPJobDonePacket(sessionKey)
	for i := 0; i < 20; i++ {
		_, _ = conn.WriteToUDP(packet, remote)
		time.Sleep(25 * time.Millisecond)
	}
}

func receiveTransferFileTCP(conn net.Conn, root string) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	reader := bufio.NewReader(conn)
	magic, err := reader.ReadString('\n')
	if err != nil || (magic != tcpJobMagicV1 && magic != tcpJobMagicV2 && magic != tcpJobMagicV3 && magic != tcpJobMagicV4) {
		if errors.Is(err, io.EOF) && magic == "" {
			internal.Debug("tcp transfer idle connection closed", internal.Fields{"remote": remote})
			return
		}
		fields := internal.Fields{"remote": remote}
		if err != nil {
			fields[internal.FieldError] = err.Error()
		} else {
			fields["magic"] = strings.TrimSpace(magic)
		}
		internal.Warn("drop invalid tcp transfer header", fields)
		return
	}
	var header [40]byte
	headerLen := 12
	if magic == tcpJobMagicV2 {
		headerLen = 16
	} else if magic == tcpJobMagicV3 {
		headerLen = 36
	} else if magic == tcpJobMagicV4 {
		headerLen = 40
	}
	if _, err := io.ReadFull(reader, header[:headerLen]); err != nil {
		internal.Warn("tcp transfer header read failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote})
		return
	}
	pathLen := binary.BigEndian.Uint32(header[:4])
	size := binary.BigEndian.Uint64(header[4:])
	totalSize := size
	offset := uint64(0)
	streamID := uint32(1)
	jobLen := uint16(0)
	routeLen := uint16(0)
	rootLen := uint32(0)
	if magic == tcpJobMagicV2 {
		jobLen = binary.BigEndian.Uint16(header[12:14])
		routeLen = binary.BigEndian.Uint16(header[14:16])
	} else if magic == tcpJobMagicV3 || magic == tcpJobMagicV4 {
		totalSize = binary.BigEndian.Uint64(header[4:12])
		offset = binary.BigEndian.Uint64(header[12:20])
		size = binary.BigEndian.Uint64(header[20:28])
		streamID = binary.BigEndian.Uint32(header[28:32])
		jobLen = binary.BigEndian.Uint16(header[32:34])
		routeLen = binary.BigEndian.Uint16(header[34:36])
		if magic == tcpJobMagicV4 {
			rootLen = binary.BigEndian.Uint32(header[36:40])
		}
	}
	if pathLen == 0 || pathLen > 1<<20 {
		internal.Warn("drop invalid tcp transfer path length", internal.Fields{"remote": remote, "path_len": pathLen})
		return
	}
	if streamID == 0 || offset > totalSize || size > totalSize-offset {
		internal.Warn("drop invalid tcp transfer range", internal.Fields{
			"remote":    remote,
			"stream_id": streamID,
			"offset":    offset,
			"bytes":     size,
			"total":     totalSize,
		})
		return
	}
	if totalSize > uint64(1<<63-1) || offset > uint64(1<<63-1) {
		internal.Warn("drop oversized tcp transfer range", internal.Fields{"remote": remote, "offset": offset, "total": totalSize})
		return
	}
	pathBytes := make([]byte, pathLen)
	if _, err := io.ReadFull(reader, pathBytes); err != nil {
		internal.Warn("tcp transfer path read failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote})
		return
	}
	jobID := ""
	if jobLen > 0 {
		jobBytes := make([]byte, jobLen)
		if _, err := io.ReadFull(reader, jobBytes); err != nil {
			internal.Warn("tcp transfer job metadata read failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote})
			return
		}
		jobID = string(jobBytes)
	}
	routeID := ""
	if routeLen > 0 {
		routeBytes := make([]byte, routeLen)
		if _, err := io.ReadFull(reader, routeBytes); err != nil {
			internal.Warn("tcp transfer route metadata read failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote})
			return
		}
		routeID = string(routeBytes)
	}
	effectiveRoot := strings.TrimSpace(root)
	if rootLen > 0 {
		if rootLen > 1<<20 {
			internal.Warn("drop invalid tcp transfer root length", internal.Fields{"remote": remote, "root_len": rootLen})
			return
		}
		rootBytes := make([]byte, rootLen)
		if _, err := io.ReadFull(reader, rootBytes); err != nil {
			internal.Warn("tcp transfer root metadata read failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote})
			return
		}
		transferRoot := filepath.Clean(string(rootBytes))
		if !filepath.IsAbs(transferRoot) {
			internal.Warn("drop relative tcp transfer root", internal.Fields{"remote": remote, "root": transferRoot})
			return
		}
		effectiveRoot = transferRoot
	}
	rel := filepath.Clean(filepath.FromSlash(string(pathBytes)))
	if filepath.IsAbs(rel) || rel == "." || strings.HasPrefix(rel, "..") {
		internal.Warn("drop invalid tcp transfer path", internal.Fields{"remote": remote, "path": rel})
		return
	}
	dstPath, err := resolveDestinationFilePath(effectiveRoot, rel)
	if err != nil {
		internal.Warn("tcp transfer destination path failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel})
		return
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		internal.Warn("tcp transfer destination mkdir failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel})
		return
	}
	flags := os.O_CREATE | os.O_WRONLY
	if magic != tcpJobMagicV3 && magic != tcpJobMagicV4 {
		flags |= os.O_TRUNC
	}
	dst, err := os.OpenFile(dstPath, flags, 0o644)
	if err != nil {
		internal.Warn("tcp transfer destination open failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel})
		return
	}
	defer dst.Close()
	if (magic == tcpJobMagicV3 || magic == tcpJobMagicV4) && offset == 0 {
		if err := dst.Truncate(int64(totalSize)); err != nil {
			internal.Warn("tcp transfer destination truncate failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel, "bytes": totalSize})
			return
		}
	}
	if magic == tcpJobMagicV3 || magic == tcpJobMagicV4 {
		if _, err := dst.Seek(int64(offset), io.SeekStart); err != nil {
			internal.Warn("tcp transfer destination seek failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel, "offset": offset})
			return
		}
	}
	internal.Info(jobLogMessage(jobID, "tcp receive started"), internal.Fields{
		"route_id":  routeID,
		"remote":    remote,
		"root":      effectiveRoot,
		"path":      rel,
		"bytes":     size,
		"offset":    offset,
		"stream_id": streamID,
	})
	if _, err := io.CopyN(dst, reader, int64(size)); err != nil {
		internal.Warn("tcp transfer receive failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel, "bytes": size})
		return
	}
	_ = writeFull(context.Background(), conn, []byte("OK"))
	internal.Info(jobLogMessage(jobID, "tcp receive completed"), internal.Fields{
		"route_id":  routeID,
		"remote":    remote,
		"path":      rel,
		"bytes":     size,
		"offset":    offset,
		"stream_id": streamID,
	})
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
		SessionId:     ep.GetSessionId(),
	}
}

var _ TransferEndpointRegistry = (*TransferJobManager)(nil)
var _ TransferJobExecutor = localFilesystemTransferExecutor{}
