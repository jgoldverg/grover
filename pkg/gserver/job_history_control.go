package gserver

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type JobHistoryControlService struct {
	pb.UnimplementedJobHistoryControlServer
	logDir string
}

func NewJobHistoryControlService(cfg *internal.ServerConfig) *JobHistoryControlService {
	logDir := "/var/log/grover"
	if cfg != nil && strings.TrimSpace(cfg.JobLogDir) != "" {
		logDir = strings.TrimSpace(cfg.JobLogDir)
	}
	return &JobHistoryControlService{logDir: logDir}
}

func (s *JobHistoryControlService) ListJobHistory(ctx context.Context, req *pb.ListJobHistoryRequest) (*pb.ListJobHistoryResponse, error) {
	const rpcName = "JobHistoryControl.ListJobHistory"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{
		"route_id": req.GetRouteId(),
		"limit":    req.GetLimit(),
	})
	entries, err := s.listHistory(req)
	if err != nil {
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, status.Error(codes.Internal, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{"jobs": len(entries)}, time.Since(started))
	return &pb.ListJobHistoryResponse{Jobs: entries}, nil
}

func (s *JobHistoryControlService) GetJobManifest(ctx context.Context, req *pb.GetJobManifestRequest) (*pb.GetJobManifestResponse, error) {
	const rpcName = "JobHistoryControl.GetJobManifest"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{"job_id": req.GetJobId()})
	dir, err := s.findJobDir(req.GetJobId())
	if err != nil {
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, status.Error(codes.NotFound, err.Error())
	}
	manifest, err := readJobHistoryManifest(dir)
	if err != nil {
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, status.Error(codes.Internal, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{"job_id": manifest.GetJobId()}, time.Since(started))
	return &pb.GetJobManifestResponse{Manifest: manifest}, nil
}

func (s *JobHistoryControlService) GetJobFinal(ctx context.Context, req *pb.GetJobFinalRequest) (*pb.GetJobFinalResponse, error) {
	const rpcName = "JobHistoryControl.GetJobFinal"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{"job_id": req.GetJobId()})
	dir, err := s.findJobDir(req.GetJobId())
	if err != nil {
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, status.Error(codes.NotFound, err.Error())
	}
	job, err := readJobFinal(dir)
	if err != nil {
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, status.Error(codes.NotFound, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{
		"job_id": job.GetJobId(),
		"state":  job.GetState().String(),
	}, time.Since(started))
	return &pb.GetJobFinalResponse{Job: job}, nil
}

func (s *JobHistoryControlService) ListJobSnapshots(ctx context.Context, req *pb.ListJobSnapshotsRequest) (*pb.ListJobSnapshotsResponse, error) {
	const rpcName = "JobHistoryControl.ListJobSnapshots"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{
		"job_id": req.GetJobId(),
		"limit":  req.GetLimit(),
	})
	dir, err := s.findJobDir(req.GetJobId())
	if err != nil {
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, status.Error(codes.NotFound, err.Error())
	}
	snapshots, err := readJobSnapshots(dir, req)
	if err != nil {
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, status.Error(codes.Internal, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{"snapshots": len(snapshots)}, time.Since(started))
	return &pb.ListJobSnapshotsResponse{Snapshots: snapshots}, nil
}

func (s *JobHistoryControlService) ListJobEnergy(ctx context.Context, req *pb.ListJobEnergyRequest) (*pb.ListJobEnergyResponse, error) {
	const rpcName = "JobHistoryControl.ListJobEnergy"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{
		"job_id": req.GetJobId(),
		"limit":  req.GetLimit(),
	})
	dir, err := s.findJobDir(req.GetJobId())
	if err != nil {
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, status.Error(codes.NotFound, err.Error())
	}
	resp, err := readJobEnergy(dir, req.GetLimit())
	if err != nil {
		internal.RPCFailed(rpcName, err, nil, time.Since(started))
		return nil, status.Error(codes.Internal, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{"records": len(resp.GetRecords())}, time.Since(started))
	return resp, nil
}

func (s *JobHistoryControlService) listHistory(req *pb.ListJobHistoryRequest) ([]*pb.JobHistoryEntry, error) {
	dirs, err := os.ReadDir(s.logDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	entries := make([]*pb.JobHistoryEntry, 0, len(dirs))
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		entry, err := readJobHistoryEntry(filepath.Join(s.logDir, dir.Name()))
		if err != nil {
			continue
		}
		if strings.TrimSpace(req.GetRouteId()) != "" && entry.GetRouteId() != strings.TrimSpace(req.GetRouteId()) {
			continue
		}
		if req.GetSinceUnixNano() > 0 && entry.GetCreatedAtUnixNano() < req.GetSinceUnixNano() {
			continue
		}
		if req.GetUntilUnixNano() > 0 && entry.GetCreatedAtUnixNano() > req.GetUntilUnixNano() {
			continue
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].GetCreatedAtUnixNano() > entries[j].GetCreatedAtUnixNano()
	})
	if req.GetLimit() > 0 && len(entries) > int(req.GetLimit()) {
		entries = entries[:int(req.GetLimit())]
	}
	return entries, nil
}

func (s *JobHistoryControlService) findJobDir(jobID string) (string, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return "", fmt.Errorf("job_id is required")
	}
	direct := filepath.Join(s.logDir, safeJobLogDirName(jobID))
	if manifest, err := readJobHistoryManifest(direct); err == nil && manifest.GetJobId() == jobID {
		return direct, nil
	}
	entries, err := s.listHistory(&pb.ListJobHistoryRequest{})
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if entry.GetJobId() == jobID {
			return entry.GetPath(), nil
		}
	}
	return "", fmt.Errorf("job history %q not found in %s", jobID, s.logDir)
}

func readJobHistoryEntry(dir string) (*pb.JobHistoryEntry, error) {
	manifest, err := readJobHistoryManifest(dir)
	if err != nil {
		return nil, err
	}
	entry := &pb.JobHistoryEntry{
		JobId:             manifest.GetJobId(),
		RouteId:           manifest.GetRouteId(),
		TotalBytes:        manifest.GetTotalBytes(),
		CreatedAtUnixNano: manifest.GetCreatedAtUnixNano(),
		Path:              dir,
		Manifest:          manifest,
	}
	if final, err := readJobFinal(dir); err == nil && final != nil {
		entry.State = final.GetState().String()
		entry.ErrorMessage = final.GetErrorMessage()
		entry.GoodBytes = final.GetGoodBytes()
		entry.NetworkBytes = final.GetNetworkBytes()
	}
	return entry, nil
}

func readJobHistoryManifest(dir string) (*pb.JobHistoryManifest, error) {
	path := filepath.Join(dir, "manifest.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var manifest transferJobManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return &pb.JobHistoryManifest{
		JobId:              manifest.JobID,
		RouteId:            manifest.RouteID,
		Protocol:           manifest.Protocol,
		SourceRoot:         manifest.SourceRoot,
		DestinationRoot:    manifest.DestinationRoot,
		DestinationData:    manifest.DestinationData,
		Concurrency:        uint32(manifest.Concurrency),
		ParallelismPerFile: uint32(manifest.Parallelism),
		FilesInFlight:      uint32(manifest.FilesInFlight),
		StreamsPerFile:     uint32(manifest.StreamsPerFile),
		ChunkSizeBytes:     manifest.ChunkSizeBytes,
		TotalFiles:         uint32(manifest.TotalFiles),
		TotalBytes:         manifest.TotalBytes,
		CreatedAtUnixNano:  manifest.CreatedAt.UnixNano(),
	}, nil
}

func readJobFinal(dir string) (*pb.TransferJob, error) {
	path := filepath.Join(dir, "final.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var job pb.TransferJob
	if err := protojson.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return &job, nil
}

func readJobSnapshots(dir string, req *pb.ListJobSnapshotsRequest) ([]*pb.TransferJob, error) {
	path := filepath.Join(dir, "snapshots.jsonl")
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()
	var snapshots []*pb.TransferJob
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1024*1024), 64*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var job pb.TransferJob
		if err := protojson.Unmarshal([]byte(line), &job); err != nil {
			continue
		}
		ts := job.GetStats().GetSampledAtUnixNano()
		if req.GetSinceUnixNano() > 0 && ts < req.GetSinceUnixNano() {
			continue
		}
		if req.GetUntilUnixNano() > 0 && ts > req.GetUntilUnixNano() {
			continue
		}
		snapshots = append(snapshots, &job)
		if req.GetLimit() > 0 && len(snapshots) >= int(req.GetLimit()) {
			break
		}
	}
	return snapshots, scanner.Err()
}

func readJobEnergy(dir string, limit uint32) (*pb.ListJobEnergyResponse, error) {
	path := filepath.Join(dir, "energy.csv")
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &pb.ListJobEnergyResponse{}, nil
		}
		return nil, err
	}
	defer file.Close()
	resp := &pb.ListJobEnergyResponse{}
	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		resp.Header = scanner.Text()
	}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		resp.Records = append(resp.Records, &pb.JobEnergyRecord{Csv: line})
		if limit > 0 && len(resp.Records) >= int(limit) {
			break
		}
	}
	return resp, scanner.Err()
}

func safeJobLogDirName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range value {
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
		return "unknown"
	}
	return b.String()
}
