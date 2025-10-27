package gclient

import (
	"context"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type HeartBeatService struct {
	client       pb.HeartBeatClient
	cfg          *internal.AppConfig
	targetServer string
	done         chan struct{}
	seq          uint64
	errCount     uint
	maxErrors    uint
	rtts         []time.Duration
	maxRTTs      int
}

func NewHeartBeatService(cfg *internal.AppConfig, conn *grpc.ClientConn) *HeartBeatService {
	return &HeartBeatService{
		client:       pb.NewHeartBeatClient(conn),
		cfg:          cfg,
		targetServer: conn.Target(),
		done:         make(chan struct{}),
		maxErrors:    uint(cfg.HeartBeatErrorCount),
		maxRTTs:      cfg.HeartBeatRtts,
	}
}

// StartPulse launches exactly one goroutine that ticks forever until ctx or StopPulse cancels it.
// If you prefer to keep a return value, change the signature back and just `return nil` at the end.
func (h *HeartBeatService) StartPulse(ctx context.Context) {
	// Guard against bad config
	interval := time.Duration(h.cfg.HeartBeatInterval) * time.Millisecond
	if interval <= 0 {
		interval = 20 * time.Second
	}
	timeout := time.Duration(h.cfg.HeartBeatTimeout) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				internal.Info("heart beat service context cancelled", nil)
				return
			case <-h.done:
				internal.Info("heart beat service stop requested", nil)
				return
			case sentAt := <-ticker.C:
				internal.Debug("heart beat service sending pulse", nil)
				req := &pb.PulseRequest{
					ClientId: h.cfg.ClientUuid,
					Seq:      h.seq,
					SentAt:   timestamppb.New(sentAt),
				}
				h.seq++

				resp, err := h.client.Pulse(ctx, req)

				if err != nil {
					h.errCount++
					if h.errCount > h.maxErrors {
						h.StopPulse()
						return
					}
					continue
				}

				h.errCount = 0
				rtt := time.Since(req.GetSentAt().AsTime())

				if h.maxRTTs > 0 {
					if len(h.rtts) < h.maxRTTs {
						h.rtts = append(h.rtts, rtt)
					} else {
						copy(h.rtts, h.rtts[1:])
						h.rtts[h.maxRTTs-1] = rtt
					}
				}

				if ms := resp.GetRecommendedIntervalMs(); ms > 0 && ms != int64(h.cfg.HeartBeatInterval) {
					newInterval := time.Duration(ms) * time.Millisecond
					internal.Info("updating heart beat interval", internal.Fields{
						internal.FieldKey("new_interval"): newInterval,
					})
					if newInterval > 0 && newInterval != interval {
						interval = newInterval
						ticker.Reset(interval)
					}
				}

				internal.Debug("heart beat pulse acknowledged", internal.Fields{
					internal.FieldRtt:    rtt,
					internal.FieldServer: h.targetServer,
				})
			}
		}
	}()
}

// StopPulse stops the single heartbeat goroutine. It’s idempotent.
func (h *HeartBeatService) StopPulse() {
	select {
	case <-h.done:
		// already closed
	default:
		close(h.done)
	}
}
