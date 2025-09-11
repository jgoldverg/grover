package control

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/config"
	"github.com/jgoldverg/grover/log"
	"github.com/jgoldverg/grover/pb"
	"github.com/pterm/pterm"
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
	cfg                 *config.ServerConfig
	clients             sync.Map
	recommendedInterval time.Duration
	status              atomic.Value
	serverId            uuid.UUID
}

func NewHeartBeatService(cfg *config.ServerConfig) *HeartBeatService {
	serverId, err := uuid.Parse(cfg.ServerId)
	if err != nil {
		log.Structured(&pterm.Error, "failed to parse server uuid from server config ", log.Fields{
			"server_id": cfg.ServerId,
		})
		return nil
	}
	hbs := &HeartBeatService{
		cfg:                 cfg,
		recommendedInterval: time.Duration(cfg.HeartBeatInterval) * time.Millisecond,
		serverId:            serverId,
	}

	log.Structured(&pterm.Info, "heart beat service initialized", log.Fields{
		"heart_beat_interval": hbs.recommendedInterval,
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

	log.Structured(&pterm.Info, "Pulse Received from: ", log.Fields{
		"clientId":        req.GetClientId(),
		"seqNum":          req.GetSeq(),
		"recv_at":         now,
		"client_id":       req.GetClientId(),
		"server_status":   h.status.Load().(pb.HeartbeatStatus),
		"rec_interval_ms": h.recommendedInterval.Milliseconds(),
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
