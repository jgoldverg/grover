package backend

import (
	"context"
	"time"
)

// ======= Status =======

type Status int

const (
	StatusUnknown Status = iota
	StatusStarting
	StatusStarted
	StatusCompleted
	StatusFailed
	StatusStopping
	StatusStopped
)

// ======= Init specs (job + file only) =======

type JobInit struct {
	JobID     string
	StartedAt time.Time // optional; can be set later via StartJob
	Meta      map[string]string
}

type FileInit struct {
	JobID       string
	FileID      string
	BytesTotal  uint64
	ChunksTotal uint64 // 0 if unknown/variable
	Meta        map[string]string
}

// ======= Producer update (creates chunks lazily) =======

type ChunkUpdate struct {
	JobID      string
	FileID     string
	ChunkID    string
	StartIndex uint64
	EndIndex   uint64
	BytesMoved uint64
	Status     Status
	AtUnix     time.Time // optional
}

// ======= In-memory state (owned by a single writer per job/shard) =======

type ChunkProgress struct {
	ChunkID     string
	StartIndex  uint64
	EndIndex    uint64
	BytesMoved  uint64
	Status      Status
	LastUpdated time.Time
}

type FileProgress struct {
	FileID         string
	Status         Status
	StartedAt      time.Time
	CompletedAt    time.Time
	BytesTotal     uint64
	BytesCompleted uint64
	ChunksTotal    uint64
	ChunksDone     uint64
	Meta           map[string]string
	Chunks         map[string]*ChunkProgress // created lazily on first ChunkUpdate
}

type JobState struct {
	JobID       string
	Status      Status
	StartedAt   time.Time
	CompletedAt time.Time
	ExitCode    string
	ExitMessage string
	Meta        map[string]string
	Files       map[string]*FileProgress

	// Hot aggregates
	BytesCompleted  uint64
	ChunksCompleted uint64
	FilesCompleted  uint64

	// Monotonic sequence for optimistic reads (optional)
	Seq uint64
}

// ======= Read API shape =======

type JobView struct {
	JobID       string
	Seq         uint64
	GeneratedAt time.Time

	Status      Status
	StartedAt   time.Time
	CompletedAt time.Time
	ExitCode    string
	ExitMessage string
	Meta        map[string]string

	BytesCompleted  uint64
	ChunksCompleted uint64
	FilesCompleted  uint64

	Files map[string]*FileView
}

type FileView struct {
	FileID         string
	Status         Status
	StartedAt      time.Time
	CompletedAt    time.Time
	BytesTotal     uint64
	BytesCompleted uint64
	ChunksTotal    uint64
	ChunksDone     uint64
	Meta           map[string]string

	// Optional: include when verbose views are requested
	Chunks map[string]*ChunkView
}

type ChunkView struct {
	ChunkID     string
	StartIndex  uint64
	EndIndex    uint64
	BytesMoved  uint64
	Status      Status
	LastUpdated time.Time
}

// ======= Minimal in-memory interface (no disk, no streaming) =======

type JobPersistence interface {
	// Outline:
	InitJob(ctx context.Context, job JobInit) error
	InitFile(ctx context.Context, file FileInit) error

	// Lifecycle (timestamps/status):
	StartJob(ctx context.Context, jobID string, t time.Time) error
	StartFile(ctx context.Context, jobID, fileID string, t time.Time) error

	// Hot path:
	RecordChunkUpdate(ctx context.Context, upd ChunkUpdate) error // auto-creates file/chunk if missing

	// Reads:
	GetJobView(ctx context.Context, jobID string) (*JobView, bool)
	ListActiveJobs(ctx context.Context) []string
	JobSeq(ctx context.Context, jobID string) (uint64, bool)

	// Completion:
	MarkFileComplete(ctx context.Context, jobID, fileID string, t time.Time) error
	MarkJobComplete(ctx context.Context, jobID string, t time.Time) error

	// Shutdown:
	Close(ctx context.Context) error
}

// ======= Trivial No-Op & Factory (placeholders) =======

type JobPersistenceType int

const (
	NoopStore JobPersistenceType = iota
)

// NoOpStore implements Store with no-ops (useful for tests/wiring).
type NoOpStore struct{}

func (s *NoOpStore) InitJob(ctx context.Context, job JobInit) error                { return nil }
func (s *NoOpStore) InitFile(ctx context.Context, file FileInit) error             { return nil }
func (s *NoOpStore) StartJob(ctx context.Context, jobID string, t time.Time) error { return nil }
func (s *NoOpStore) StartFile(ctx context.Context, jobID, fileID string, t time.Time) error {
	return nil
}
func (s *NoOpStore) RecordChunkUpdate(ctx context.Context, upd ChunkUpdate) error  { return nil }
func (s *NoOpStore) GetJobView(ctx context.Context, jobID string) (*JobView, bool) { return nil, false }
func (s *NoOpStore) ListActiveJobs(ctx context.Context) []string                   { return nil }
func (s *NoOpStore) JobSeq(ctx context.Context, jobID string) (uint64, bool)       { return 0, false }
func (s *NoOpStore) MarkFileComplete(ctx context.Context, jobID, fileID string, t time.Time) error {
	return nil
}
func (s *NoOpStore) MarkJobComplete(ctx context.Context, jobID string, t time.Time) error { return nil }
func (s *NoOpStore) Close(ctx context.Context) error                                      { return nil }

// Factory — wire your real in-memory store here when you implement it.
func JobPersistenceFactory(kind JobPersistenceType) JobPersistence {
	switch kind {
	case NoopStore:
		return &NoOpStore{}
	default:
		return &NoOpStore{}
	}
}
