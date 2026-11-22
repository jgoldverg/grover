package server

import (
	"context"
	"time"

	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/tuning"
)

var empty = &pb.Empty{}

// ---------------------------------------------------------------- transfers

type transferService struct {
	pb.UnimplementedTransferControlServer
	s *Server
}

func (t *transferService) PrepareEndpoint(_ context.Context, req *pb.PrepareEndpointRequest) (*pb.TransferEndpoint, error) {
	return t.s.Transfers.PrepareEndpoint(req)
}

func (t *transferService) CloseEndpoint(_ context.Context, req *pb.EndpointRef) (*pb.Empty, error) {
	return empty, t.s.Transfers.CloseEndpoint(req.GetEndpointId())
}

func (t *transferService) StartJob(ctx context.Context, req *pb.StartJobRequest) (*pb.Job, error) {
	return t.s.Transfers.StartJob(ctx, req)
}

func (t *transferService) GetJob(_ context.Context, req *pb.GetJobRequest) (*pb.Job, error) {
	return t.s.Transfers.GetJob(req.GetJobId(), req.GetFiles())
}

func (t *transferService) ListJobs(_ context.Context, req *pb.ListJobsRequest) (*pb.JobList, error) {
	return &pb.JobList{Jobs: t.s.Transfers.ListJobs(req.GetRouteId())}, nil
}

func (t *transferService) AbortJob(_ context.Context, req *pb.JobRef) (*pb.Job, error) {
	return t.s.Transfers.AbortJob(req.GetJobId())
}

func (t *transferService) TuneJob(_ context.Context, req *pb.TuneJobRequest) (*pb.Job, error) {
	return t.s.Transfers.TuneJob(req.GetJobId(), tuning.FromPB(req.GetTuning()))
}

// WatchJob streams snapshots until the job finishes or the client leaves.
// The last message sent is always the job's terminal state.
func (t *transferService) WatchJob(req *pb.WatchJobRequest, stream pb.TransferControl_WatchJobServer) error {
	interval := time.Duration(req.GetIntervalMs()) * time.Millisecond
	if interval < 100*time.Millisecond {
		interval = time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		job, err := t.s.Transfers.GetJob(req.GetJobId(), req.GetFiles())
		if err != nil {
			return err
		}
		if err := stream.Send(job); err != nil {
			return err
		}
		if s := job.GetState(); s == pb.RuntimeState_RUNTIME_STATE_DONE || s == pb.RuntimeState_RUNTIME_STATE_FAILED || s == pb.RuntimeState_RUNTIME_STATE_ABORTED {
			return nil
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-tick.C:
		}
	}
}

func (t *transferService) DeletePath(_ context.Context, req *pb.DeletePathRequest) (*pb.Empty, error) {
	return empty, t.s.Transfers.DeletePath(req.GetPath())
}

// ---------------------------------------------------------------- relays

type relayService struct {
	pb.UnimplementedRelayControlServer
	s *Server
}

func (r *relayService) CreateForward(_ context.Context, req *pb.CreateForwardRequest) (*pb.Forward, error) {
	return r.s.Relays.Create(req)
}

func (r *relayService) GetForward(_ context.Context, req *pb.ForwardRef) (*pb.Forward, error) {
	return r.s.Relays.Get(req.GetForwardId())
}

func (r *relayService) ListForwards(_ context.Context, req *pb.ListForwardsRequest) (*pb.ForwardList, error) {
	return &pb.ForwardList{Forwards: r.s.Relays.List(req.GetRouteId(), req.GetSessionId())}, nil
}

func (r *relayService) DeleteForward(_ context.Context, req *pb.ForwardRef) (*pb.Empty, error) {
	return empty, r.s.Relays.Delete(req.GetForwardId())
}

// ---------------------------------------------------------------- routes

type routeService struct {
	pb.UnimplementedRouteControlServer
	s *Server
}

func (r *routeService) PutRoute(_ context.Context, req *pb.Route) (*pb.Route, error) {
	return r.s.Routes.Put(req)
}

func (r *routeService) GetRoute(_ context.Context, req *pb.RouteRef) (*pb.Route, error) {
	return r.s.Routes.Get(req.GetName())
}

func (r *routeService) ListRoutes(context.Context, *pb.Empty) (*pb.RouteList, error) {
	return &pb.RouteList{Routes: r.s.Routes.List()}, nil
}

func (r *routeService) DeleteRoute(_ context.Context, req *pb.RouteRef) (*pb.Empty, error) {
	return empty, r.s.Routes.Delete(req.GetName())
}

// ---------------------------------------------------------------- sessions

type sessionService struct {
	pb.UnimplementedSessionControlServer
	s *Server
}

func (x *sessionService) CreateSession(_ context.Context, req *pb.RouteSession) (*pb.RouteSession, error) {
	return x.s.Sessions.Create(req)
}

func (x *sessionService) GetSession(_ context.Context, req *pb.SessionRef) (*pb.RouteSession, error) {
	return x.s.Sessions.Get(req.GetSessionId())
}

func (x *sessionService) ListSessions(_ context.Context, req *pb.ListSessionsRequest) (*pb.SessionList, error) {
	return &pb.SessionList{Sessions: x.s.Sessions.List(req.GetRouteId())}, nil
}

func (x *sessionService) DeleteSession(_ context.Context, req *pb.SessionRef) (*pb.Empty, error) {
	return empty, x.s.Sessions.Delete(req.GetSessionId())
}

// ---------------------------------------------------------------- history

type historyService struct {
	pb.UnimplementedHistoryControlServer
	s *Server
}

func (h *historyService) ListHistory(_ context.Context, req *pb.ListHistoryRequest) (*pb.HistoryList, error) {
	jobs, err := h.s.History.List(req)
	return &pb.HistoryList{Jobs: jobs}, err
}

func (h *historyService) GetHistory(_ context.Context, req *pb.HistoryRef) (*pb.HistoryRecord, error) {
	return h.s.History.Get(req.GetJobId(), req.GetRole())
}

func (h *historyService) ListSnapshots(_ context.Context, req *pb.ListSnapshotsRequest) (*pb.JobList, error) {
	jobs, err := h.s.History.Snapshots(req.GetJobId(), req.GetRole(), req.GetLimit())
	return &pb.JobList{Jobs: jobs}, err
}

func (h *historyService) GetEnergy(_ context.Context, req *pb.EnergyRequest) (*pb.EnergyCSV, error) {
	return h.s.History.Energy(req.GetJobId(), req.GetRole(), req.GetLimit())
}
