package gclient

import (
	"context"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc"
)

type JobHistoryService struct {
	hc pb.JobHistoryControlClient
}

func NewJobHistoryService(conn *grpc.ClientConn) *JobHistoryService {
	return &JobHistoryService{hc: pb.NewJobHistoryControlClient(conn)}
}

func (s *JobHistoryService) ListJobHistory(ctx context.Context, req *pb.ListJobHistoryRequest) ([]*pb.JobHistoryEntry, error) {
	started := time.Now()
	internal.Debug("grpc JobHistoryControl.ListJobHistory start", internal.Fields{
		"route_id": req.GetRouteId(),
		"limit":    req.GetLimit(),
	})
	resp, err := s.hc.ListJobHistory(ctx, req)
	if err != nil {
		internal.Warn("grpc JobHistoryControl.ListJobHistory failed", internal.Fields{
			internal.FieldError: err.Error(),
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	internal.Debug("grpc JobHistoryControl.ListJobHistory done", internal.Fields{
		"jobs":       len(resp.GetJobs()),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return resp.GetJobs(), nil
}

func (s *JobHistoryService) GetJobManifest(ctx context.Context, jobID string) (*pb.JobHistoryManifest, error) {
	started := time.Now()
	internal.Debug("grpc JobHistoryControl.GetJobManifest start", internal.Fields{"job_id": jobID})
	resp, err := s.hc.GetJobManifest(ctx, &pb.GetJobManifestRequest{JobId: jobID})
	if err != nil {
		internal.Warn("grpc JobHistoryControl.GetJobManifest failed", internal.Fields{
			internal.FieldError: err.Error(),
			"job_id":            jobID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	internal.Debug("grpc JobHistoryControl.GetJobManifest done", internal.Fields{
		"job_id":     resp.GetManifest().GetJobId(),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return resp.GetManifest(), nil
}

func (s *JobHistoryService) GetJobFinal(ctx context.Context, jobID string) (*pb.TransferJob, error) {
	started := time.Now()
	internal.Debug("grpc JobHistoryControl.GetJobFinal start", internal.Fields{"job_id": jobID})
	resp, err := s.hc.GetJobFinal(ctx, &pb.GetJobFinalRequest{JobId: jobID})
	if err != nil {
		internal.Warn("grpc JobHistoryControl.GetJobFinal failed", internal.Fields{
			internal.FieldError: err.Error(),
			"job_id":            jobID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	internal.Debug("grpc JobHistoryControl.GetJobFinal done", internal.Fields{
		"job_id":     resp.GetJob().GetJobId(),
		"state":      resp.GetJob().GetState().String(),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return resp.GetJob(), nil
}

func (s *JobHistoryService) ListJobSnapshots(ctx context.Context, req *pb.ListJobSnapshotsRequest) ([]*pb.TransferJob, error) {
	started := time.Now()
	internal.Debug("grpc JobHistoryControl.ListJobSnapshots start", internal.Fields{
		"job_id": req.GetJobId(),
		"limit":  req.GetLimit(),
	})
	resp, err := s.hc.ListJobSnapshots(ctx, req)
	if err != nil {
		internal.Warn("grpc JobHistoryControl.ListJobSnapshots failed", internal.Fields{
			internal.FieldError: err.Error(),
			"job_id":            req.GetJobId(),
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	internal.Debug("grpc JobHistoryControl.ListJobSnapshots done", internal.Fields{
		"snapshots":  len(resp.GetSnapshots()),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return resp.GetSnapshots(), nil
}

func (s *JobHistoryService) ListJobEnergy(ctx context.Context, req *pb.ListJobEnergyRequest) (*pb.ListJobEnergyResponse, error) {
	started := time.Now()
	internal.Debug("grpc JobHistoryControl.ListJobEnergy start", internal.Fields{
		"job_id": req.GetJobId(),
		"limit":  req.GetLimit(),
	})
	resp, err := s.hc.ListJobEnergy(ctx, req)
	if err != nil {
		internal.Warn("grpc JobHistoryControl.ListJobEnergy failed", internal.Fields{
			internal.FieldError: err.Error(),
			"job_id":            req.GetJobId(),
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	internal.Debug("grpc JobHistoryControl.ListJobEnergy done", internal.Fields{
		"records":    len(resp.GetRecords()),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return resp, nil
}
