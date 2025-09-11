package client

import (
	"context"
	"fmt"
	"time"

	"github.com/jgoldverg/grover/config"
	"github.com/jgoldverg/grover/log"
	"github.com/jgoldverg/grover/pb"
	"github.com/pterm/pterm"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type HeartBeatService struct {
	client       pb.HeartBeatClient
	cfg          *config.AppConfig
	targetServer string
	done         chan struct{}
	seq          uint64
	errCount     uint
	maxErrors    uint
	rtts         []time.Duration
	maxRTTs      int
}

func NewHeartBeatService(cfg *config.AppConfig, conn *grpc.ClientConn) *HeartBeatService {
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
			log.Structured(&pterm.Info, "For loop msg", nil)
			select {
			case <-ctx.Done():
				log.Structured(&pterm.Info, "Context done", nil)
				return
			case <-h.done:
				log.Structured(&pterm.Info, "Channel done", nil)
				return
			case sentAt := <-ticker.C:
				log.Structured(&pterm.Info, "Ticker does tick", nil)
				req := &pb.PulseRequest{
					ClientId: h.cfg.ClientUuid, // <-- FIX: string, not uuid.UUID
					Seq:      h.seq,
					SentAt:   timestamppb.New(sentAt),
				}
				h.seq++

				rpcCtx, cancel := context.WithTimeout(ctx, timeout)
				resp, err := h.client.Pulse(rpcCtx, req)
				cancel()

				fmt.Println(resp.GetServerId())
				fmt.Println(resp.GetSeq())
				fmt.Println(resp.GetRecvAt().AsTime())
				fmt.Println(resp.GetStatus())
				fmt.Println(resp.GetSentAt().AsTime())
				fmt.Println(resp.GetRecommendedIntervalMs())

				if err != nil {
					h.errCount++
					if h.errCount > h.maxErrors {
						h.StopPulse()
						return
					}
					continue
				}

				// Success
				h.errCount = 0
				rtt := time.Since(sentAt)

				// Fixed-size RTT buffer (no growth past maxRTTs)
				if h.maxRTTs > 0 {
					if len(h.rtts) < h.maxRTTs {
						h.rtts = append(h.rtts, rtt)
					} else {
						// drop oldest, append newest
						copy(h.rtts, h.rtts[1:])
						h.rtts[h.maxRTTs-1] = rtt
					}
				}

				if ms := resp.GetRecommendedIntervalMs(); ms > 0 {
					newInterval := time.Duration(ms) * time.Millisecond
					log.Structured(&pterm.Info, "setting heart beat interval following server rec ", log.Fields{
						"new_interval": newInterval,
					})
					if newInterval > 0 && newInterval != interval {
						interval = newInterval
						ticker.Reset(interval)
					}
				}

				log.Structured(&pterm.Info, "", log.Fields{
					log.FieldMsg:    "HeartBeat",
					log.FieldRtt:    rtt,
					log.FieldServer: h.targetServer,
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
