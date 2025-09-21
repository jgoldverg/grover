package control

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type clientState struct {
	LastSeq  uint64
	LastSeen time.Time
}

type HeartBeatService struct {
	pb.UnimplementedHeartBeatServer
	pulse               *pb.HeartBeatServer
	cfg                 *internal.ServerConfig
	clients             sync.Map
	recommendedInterval time.Duration
	status              atomic.Value
	serverId            uuid.UUID
}

func NewHeartBeatService(cfg *internal.ServerConfig) *HeartBeatService {
	serverId, err := uuid.Parse(cfg.ServerId)
	if err != nil {
		internal.Error("failed to parse server uuid from server config", internal.Fields{
			"server_id":         cfg.ServerId,
			internal.FieldError: err.Error(),
		})
		return nil
	}
	hbs := &HeartBeatService{
		cfg:                 cfg,
		recommendedInterval: time.Duration(cfg.HeartBeatInterval) * time.Millisecond,
		serverId:            serverId,
	}

	internal.Info("heart beat service initialized", internal.Fields{
		internal.FieldKey("heart_beat_interval"): hbs.recommendedInterval,
	})
	hbs.status.Store(pb.HeartbeatStatus_OK)

	return hbs
}

func (h *HeartBeatService) SetStatus(heartbeatStatus pb.HeartbeatStatus) {
	h.status.Store(heartbeatStatus)
}

func (h *HeartBeatService) Pulse(ctx context.Context, req *pb.PulseRequest) (*pb.PulseResponse, error) {
	if req.GetClientId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing client id")
	}
	now := time.Now()

	if val, ok := h.clients.Load(req.GetClientId()); ok {
		prev := val.(*clientState)
		cs := clientState{
			LastSeq:  maxU64(prev.LastSeq, req.GetSeq()),
			LastSeen: maxTime(prev.LastSeen, now),
		}
		h.clients.Store(req.GetClientId(), &cs)
	} else {
		h.clients.Store(req.GetClientId(), &clientState{
			LastSeq:  req.Seq,
			LastSeen: now,
		})
	}

	internal.Debug("heartbeat pulse received", internal.Fields{
		internal.FieldKey("client_id"):     req.GetClientId(),
		internal.FieldKey("seq"):           req.GetSeq(),
		internal.FieldKey("received_at"):   now,
		internal.FieldKey("server_status"): h.status.Load().(pb.HeartbeatStatus),
		internal.FieldKey("interval_ms"):   h.recommendedInterval.Milliseconds(),
	})
	resp := &pb.PulseResponse{
		Seq:                   req.GetSeq(),
		ServerId:              h.serverId.String(),
		RecvAt:                timestamppb.New(now),
		SentAt:                timestamppb.Now(),
		Status:                h.status.Load().(pb.HeartbeatStatus),
		RecommendedIntervalMs: h.recommendedInterval.Milliseconds(),
	}

	return resp, nil
}

func maxU64(a, b uint64) uint64 {
	if b > a {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
