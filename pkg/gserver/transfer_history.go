package gserver

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/energy"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/protobuf/encoding/protojson"
)

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
	Concurrency     uint32                    `json:"concurrency"`
	Parallelism     uint32                    `json:"parallelism_per_file"`
	ChunkSizeBytes  uint64                    `json:"chunk_size_bytes,omitempty"`
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
		Concurrency:     runtime.filesInFlight,
		Parallelism:     runtime.streamsPerFile,
		ChunkSizeBytes:  runtime.chunkSizeBytes,
		FilesInFlight:   runtime.filesInFlight,
		StreamsPerFile:  runtime.streamsPerFile,
		TotalFiles:      len(plans),
		TotalBytes:      totalPlannedBytes(plans),
		CreatedAt:       runtime.startedAt,
		Files:           files,
	}
	return h.writeJSONFile("manifest.json", manifest)
}

func (h *transferJobHistory) writeReceiveManifest(job *pb.TransferJob) error {
	if h == nil || job == nil {
		return nil
	}
	files := make([]transferJobManifestFile, 0, len(job.GetFiles()))
	var totalBytes uint64
	for _, file := range job.GetFiles() {
		files = append(files, transferJobManifestFile{
			SourcePath:   file.GetPath(),
			RelativePath: file.GetRelativePath(),
			Size:         file.GetSize(),
		})
		totalBytes += file.GetSize()
	}
	manifest := transferJobManifest{
		JobID:           job.GetJobId(),
		RouteID:         job.GetRouteId(),
		Protocol:        job.GetProtocol().String(),
		SourceRoot:      "",
		DestinationRoot: job.GetDestination().GetRootPath(),
		DestinationData: endpointLabel(job.GetDestination().GetDataEndpoint()),
		Concurrency:     job.GetFilesActive(),
		Parallelism:     job.GetParallelismPerFile(),
		FilesInFlight:   job.GetFilesInFlight(),
		StreamsPerFile:  job.GetStreamsPerFile(),
		TotalFiles:      len(files),
		TotalBytes:      totalBytes,
		CreatedAt:       time.Unix(0, job.GetStats().GetSampledAtUnixNano()),
		Files:           files,
	}
	if manifest.CreatedAt.IsZero() || job.GetStats().GetSampledAtUnixNano() == 0 {
		manifest.CreatedAt = time.Now()
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

func (h *transferJobHistory) appendReceiveEnergy(jobID, routeID string, now time.Time) {
	if h == nil || h.energy == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.energyWriter == nil {
		return
	}
	if err := h.energy.WriteCSVRecord(h.energyWriter, h.energyTick, jobID, routeID, now); err != nil {
		internal.Warn(jobLogMessage(jobID, "failed to sample destination receive energy"), internal.Fields{
			internal.FieldError: err.Error(),
			"job_log_path":      h.dir,
		})
		return
	}
	h.energyTick++
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
