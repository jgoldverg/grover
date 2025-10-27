package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrJobNotFound signals that the requested job ID is unknown to the registry.
	ErrJobNotFound = errors.New("transfer job not found")
	// ErrJobAlreadyStarted is returned if Start is invoked twice for the same job.
	ErrJobAlreadyStarted = errors.New("transfer job already started")
	// ErrJobAlreadyCompleted is returned when attempting to complete an already terminal job.
	ErrJobAlreadyCompleted = errors.New("transfer job already completed")
)

type TransportRegistration struct {
	JobID        uuid.UUID
	Port         uint32
	SessionToken []byte
}

// TransportEngine abstracts the data-plane binding for transfer jobs.
type TransportEngine interface {
	RegisterJob(ctx context.Context, jobID uuid.UUID, exec *TransferExecutor) (*TransportRegistration, error)
	Lookup(jobID uuid.UUID) (*TransferExecutor, bool)
	LookupByPort(port uint32) (*TransferExecutor, uuid.UUID, bool)
	UnregisterJob(ctx context.Context, jobID uuid.UUID) error
	Shutdown(ctx context.Context) error
}

// TransferRegistry owns the in-memory view of active transfer jobs and mirrors
// lifecycle transitions into the configured JobPersistence backend.
type TransferRegistry struct {
	store JobPersistence
	mu    sync.Mutex // guards multi-map mutations
	jobs  sync.Map   // uuid.UUID -> *transferJob
	ports sync.Map   // uint32 -> uuid.UUID
}

// transferJob holds the mutable state for a single transfer while it is active.
type transferJob struct {
	id          uuid.UUID
	request     *TransferRequest
	executor    *TransferExecutor
	cancel      context.CancelFunc
	udpPort     uint32
	status      Status
	createdAt   time.Time
	startedAt   time.Time
	completedAt time.Time
	exitMessage string
}

// JobSnapshot is an immutable view of a job's metadata suitable for status APIs.
type JobSnapshot struct {
	ID          uuid.UUID
	Request     *TransferRequest
	Status      Status
	UdpPort     uint32
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	ExitMessage string
}

// NewTransferRegistry constructs a registry. If store is nil the no-op persistence
// implementation is used.
func NewTransferRegistry(store JobPersistence) *TransferRegistry {
	if store == nil {
		store = JobPersistenceFactory(NoopStore)
	}
	return &TransferRegistry{
		store: store,
	}
}

// Create registers a new job and returns the generated UUID.
func (r *TransferRegistry) Create(ctx context.Context, req *TransferRequest, exec *TransferExecutor, cancel context.CancelFunc, udpPort uint32) (uuid.UUID, error) {
	if req == nil {
		return uuid.Nil, errors.New("transfer request cannot be nil")
	}
	if exec == nil {
		return uuid.Nil, errors.New("transfer executor cannot be nil")
	}

	jobID := uuid.New()
	now := time.Now()
	job := &transferJob{
		id:        jobID,
		request:   req,
		executor:  exec,
		cancel:    cancel,
		udpPort:   udpPort,
		status:    StatusStarting,
		createdAt: now,
	}

	r.jobs.Store(jobID, job)
	if udpPort != 0 {
		r.ports.Store(udpPort, jobID)
	}

	initErr := r.store.InitJob(ctx, JobInit{
		JobID:     jobID.String(),
		StartedAt: now,
		Meta: map[string]string{
			"udp_port": fmt.Sprintf("%d", udpPort),
		},
	})
	if initErr != nil {
		r.jobs.Delete(jobID)
		if udpPort != 0 {
			r.ports.Delete(udpPort)
		}
		return uuid.Nil, initErr
	}

	return jobID, nil
}

// Start transitions a job into the running state.
func (r *TransferRegistry) Start(ctx context.Context, jobID uuid.UUID) error {
	job, ok := r.loadJob(jobID)
	if !ok {
		return ErrJobNotFound
	}
	if !job.completedAt.IsZero() {
		return ErrJobAlreadyCompleted
	}
	if !job.startedAt.IsZero() {
		return ErrJobAlreadyStarted
	}

	now := time.Now()
	job.startedAt = now
	job.status = StatusStarted
	if err := r.store.StartJob(ctx, jobID.String(), now); err != nil {
		return err
	}
	return nil
}

// Complete finalises a job with the supplied terminal status.
func (r *TransferRegistry) Complete(ctx context.Context, jobID uuid.UUID, status Status, exitMessage string) error {
	if status != StatusCompleted && status != StatusFailed && status != StatusStopped {
		return fmt.Errorf("invalid terminal status: %v", status)
	}

	job, ok := r.loadJob(jobID)
	if !ok {
		return ErrJobNotFound
	}
	if !job.completedAt.IsZero() {
		return ErrJobAlreadyCompleted
	}

	now := time.Now()
	job.completedAt = now
	job.status = status
	job.exitMessage = exitMessage

	r.jobs.Delete(jobID)
	if job.udpPort != 0 {
		r.ports.Delete(job.udpPort)
	}

	return r.store.MarkJobComplete(ctx, jobID.String(), now)
}

// Cancel requests cancellation for the job. It returns false if no such job exists.
func (r *TransferRegistry) Cancel(jobID uuid.UUID) bool {
	job, ok := r.loadJob(jobID)
	if !ok {
		return false
	}
	if job.cancel != nil {
		job.cancel()
	}
	return true
}

// Get returns a snapshot of the job if present.
func (r *TransferRegistry) Get(jobID uuid.UUID) (JobSnapshot, bool) {
	job, ok := r.loadJob(jobID)
	if !ok {
		return JobSnapshot{}, false
	}
	return job.snapshot(), true
}

// FindByPort looks up the job listening on the provided UDP port.
func (r *TransferRegistry) FindByPort(port uint32) (JobSnapshot, bool) {
	rawID, ok := r.ports.Load(port)
	if !ok {
		return JobSnapshot{}, false
	}
	jobID, _ := rawID.(uuid.UUID)
	return r.Get(jobID)
}

// Executor retrieves the executor associated with a job.
func (r *TransferRegistry) Executor(jobID uuid.UUID) (*TransferExecutor, bool) {
	job, ok := r.loadJob(jobID)
	if !ok {
		return nil, false
	}
	return job.executor, true
}

// ExecutorByPort returns the executor for the job bound to the given UDP port.
func (r *TransferRegistry) ExecutorByPort(port uint32) (*TransferExecutor, uuid.UUID, bool) {
	rawID, ok := r.ports.Load(port)
	if !ok {
		return nil, uuid.Nil, false
	}
	jobID, _ := rawID.(uuid.UUID)
	exec, ok := r.Executor(jobID)
	if !ok {
		return nil, uuid.Nil, false
	}
	return exec, jobID, true
}

// SetUDPPort updates the UDP port associated with a job after registration.
func (r *TransferRegistry) SetUDPPort(jobID uuid.UUID, port uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	value, ok := r.jobs.Load(jobID)
	if !ok {
		return
	}
	job, ok := value.(*transferJob)
	if !ok {
		return
	}

	// Remove old mapping if present.
	if job.udpPort != 0 {
		r.ports.Delete(job.udpPort)
	}

	job.udpPort = port
	if port != 0 {
		r.ports.Store(port, jobID)
	}
}

// ListSnapshots materialises all active jobs into snapshots.
func (r *TransferRegistry) ListSnapshots() []JobSnapshot {
	snapshots := make([]JobSnapshot, 0)
	r.jobs.Range(func(_, value any) bool {
		if job, ok := value.(*transferJob); ok {
			snapshots = append(snapshots, job.snapshot())
		}
		return true
	})
	return snapshots
}

// Shutdown cancels every active job and clears registry state.
func (r *TransferRegistry) Shutdown(ctx context.Context) {
	r.jobs.Range(func(key, value any) bool {
		jobID := key.(uuid.UUID)
		job := value.(*transferJob)
		if job.cancel != nil {
			job.cancel()
		}
		r.jobs.Delete(jobID)
		return true
	})
	r.ports.Range(func(key, _ any) bool {
		r.ports.Delete(key)
		return true
	})
	_ = r.store.Close(ctx)
}

func (r *TransferRegistry) loadJob(jobID uuid.UUID) (*transferJob, bool) {
	raw, ok := r.jobs.Load(jobID)
	if !ok {
		return nil, false
	}
	job, ok := raw.(*transferJob)
	return job, ok
}

func (j *transferJob) snapshot() JobSnapshot {
	return JobSnapshot{
		ID:          j.id,
		Request:     j.request,
		Status:      j.status,
		UdpPort:     j.udpPort,
		CreatedAt:   j.createdAt,
		StartedAt:   j.startedAt,
		CompletedAt: j.completedAt,
		ExitMessage: j.exitMessage,
	}
}
