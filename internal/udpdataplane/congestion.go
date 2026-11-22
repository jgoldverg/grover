package udpdataplane

import (
	"math"
	"sync"
	"time"
)

type FixedCongestionController struct {
	targetBytes int64
}

func newFixedCongestionController(targetBytes int64) FixedCongestionController {
	return FixedCongestionController{targetBytes: targetBytes}
}

func (c FixedCongestionController) TargetInflightBytes(state SendState) int64 {
	if c.targetBytes > 0 {
		return c.targetBytes
	}
	if state.PayloadSize > 0 {
		return int64(4096 * state.PayloadSize)
	}
	return 10 * 1024 * 1024
}

func (c FixedCongestionController) AckTimeout() time.Duration {
	return 200 * time.Millisecond
}

func (c FixedCongestionController) OnAck(AckSample) {}

type BBRLikeCongestionController struct {
	minWindowBytes int64

	mu         sync.Mutex
	minRTT     time.Duration
	srtt       time.Duration
	rttVar     time.Duration
	bwEstimate float64
	lastAck    time.Time
}

func newBBRLikeCongestionController(minWindowBytes int64) *BBRLikeCongestionController {
	if minWindowBytes <= 0 {
		minWindowBytes = 10 * 1024 * 1024
	}
	return &BBRLikeCongestionController{minWindowBytes: minWindowBytes}
}

func (c *BBRLikeCongestionController) TargetInflightBytes(SendState) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	window := float64(c.minWindowBytes)
	if c.bwEstimate > 0 && c.minRTT > 0 {
		window = math.Max(window, c.bwEstimate*c.minRTT.Seconds())
	}
	if c.srtt > 0 && c.minRTT > 0 {
		ratio := float64(c.minRTT) / float64(c.srtt)
		if ratio < 0.5 {
			ratio = 0.5
		} else if ratio > 1.5 {
			ratio = 1.5
		}
		window *= ratio
	}
	if window < float64(c.minWindowBytes) {
		window = float64(c.minWindowBytes)
	}
	return int64(window)
}

func (c *BBRLikeCongestionController) AckTimeout() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	timeout := 200 * time.Millisecond
	if c.srtt > 0 {
		timeout = c.srtt * 5
	}
	if timeout < 20*time.Millisecond {
		timeout = 20 * time.Millisecond
	}
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	return timeout
}

func (c *BBRLikeCongestionController) OnAck(sample AckSample) {
	if sample.Bytes <= 0 {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if sample.RTT > 0 {
		if c.minRTT == 0 || sample.RTT < c.minRTT {
			c.minRTT = sample.RTT
		}
		if c.srtt == 0 {
			c.srtt = sample.RTT
			c.rttVar = sample.RTT / 2
		} else {
			delta := sample.RTT - c.srtt
			c.srtt += delta / 8
			if delta < 0 {
				delta = -delta
			}
			c.rttVar += (delta - c.rttVar) / 4
		}
	}
	if !c.lastAck.IsZero() {
		dt := now.Sub(c.lastAck)
		if dt > 0 {
			rate := float64(sample.Bytes) / dt.Seconds()
			if rate > c.bwEstimate {
				c.bwEstimate = rate
			} else {
				c.bwEstimate = c.bwEstimate*0.95 + rate*0.05
			}
		}
	} else if sample.RTT > 0 {
		c.bwEstimate = float64(sample.Bytes) / sample.RTT.Seconds()
	}
	c.lastAck = now
}
