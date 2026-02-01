package gclient

import (
	"context"
	"errors"
	"sync"

	"github.com/jgoldverg/grover/pkg/udpwire"
)

const (
	defaultLossRatio       = 0.01
	lossSamplePackets      = 64
	defaultMinWindowBytes  = 4 * 1024
	assumedRTTMilliseconds = 5
)

var (
	errTxWindowClosed = errors.New("udp tx window closed")
)

func targetLossRatio(percent int) float64 {
	if percent <= 0 {
		return defaultLossRatio
	}
	return float64(percent) / 100
}

func estimateBDPBytes(linkMbps int) int {
	if linkMbps <= 0 {
		return 0
	}
	bytesPerSecond := linkMbps * 125000
	return bytesPerSecond * assumedRTTMilliseconds / 1000
}

func clampInt(v, min, max int) int {
	if max > 0 && v > max {
		v = max
	}
	if v < min {
		v = min
	}
	return v
}

type txWindow struct {
	mu              sync.Mutex
	waitCh          chan struct{}
	maxBytes        int
	minBytes        int
	maxBytesCeiling int
	inFlight        int
	pending         []pendingPacket
	closed          bool
	closeErr        error

	targetLossRatio float64
	lossSampleSize  int
	sampleSent      int
	sampleLost      int
}

type pendingPacket struct {
	seq   uint32
	bytes int
}

func newTxWindow(initial int, ceiling int, targetLoss float64) *txWindow {
	if initial <= 0 {
		initial = 64 * 1024
	}
	if ceiling <= 0 {
		ceiling = initial
	}
	minBytes := initial / 4
	if minBytes < defaultMinWindowBytes {
		minBytes = defaultMinWindowBytes
		if minBytes > initial {
			minBytes = initial
		}
	}
	if targetLoss <= 0 {
		targetLoss = defaultLossRatio
	}
	tw := &txWindow{
		maxBytes:        initial,
		minBytes:        minBytes,
		maxBytesCeiling: ceiling,
		waitCh:          make(chan struct{}),
		targetLossRatio: targetLoss,
		lossSampleSize:  lossSamplePackets,
	}
	return tw
}

func (tw *txWindow) reserve(ctx context.Context, seq uint32, bytes int) error {
	if bytes <= 0 {
		return nil
	}
	for {
		tw.mu.Lock()
		if tw.closed {
			err := tw.closeErr
			if err == nil {
				err = errTxWindowClosed
			}
			tw.mu.Unlock()
			return err
		}
		allowed := tw.maxBytes <= 0 || tw.inFlight+bytes <= tw.maxBytes
		waitCh := tw.waitCh
		if allowed {
			tw.pending = append(tw.pending, pendingPacket{seq: seq, bytes: bytes})
			tw.inFlight += bytes
			tw.mu.Unlock()
			return nil
		}
		tw.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waitCh:
		}
	}
}

func (tw *txWindow) releaseThrough(seq uint32) {
	if seq == 0 {
		return
	}
	tw.mu.Lock()
	if len(tw.pending) == 0 {
		tw.mu.Unlock()
		return
	}
	released := 0
	idx := 0
	for idx < len(tw.pending) && tw.pending[idx].seq <= seq {
		released += tw.pending[idx].bytes
		idx++
	}
	if idx > 0 {
		tw.pending = append(tw.pending[:0], tw.pending[idx:]...)
		tw.inFlight -= released
		if tw.inFlight < 0 {
			tw.inFlight = 0
		}
		tw.sampleSent += idx
		tw.signalLocked()
		tw.maybeAdjustLocked()
	}
	tw.mu.Unlock()
}

func (tw *txWindow) releaseRanges(ranges []udpwire.SackRange) {
	if len(ranges) == 0 {
		return
	}
	tw.mu.Lock()
	if len(tw.pending) == 0 {
		tw.mu.Unlock()
		return
	}
	released := 0
	dst := tw.pending[:0]
	lostPackets := 0
	for _, pkt := range tw.pending {
		acked := false
		for _, rng := range ranges {
			if rng.End < rng.Start {
				continue
			}
			if pkt.seq >= rng.Start && pkt.seq <= rng.End {
				acked = true
				break
			}
		}
		if acked {
			lostPackets++
			released += pkt.bytes
			continue
		}
		dst = append(dst, pkt)
	}
	if released > 0 {
		tw.pending = dst
		tw.inFlight -= released
		if tw.inFlight < 0 {
			tw.inFlight = 0
		}
		tw.sampleSent += lostPackets
		tw.sampleLost += lostPackets
		tw.signalLocked()
		tw.maybeAdjustLocked()
	} else {
		tw.pending = dst
	}
	tw.mu.Unlock()
}

func (tw *txWindow) cancel(seq uint32) {
	if seq == 0 {
		return
	}
	tw.mu.Lock()
	for i, pkt := range tw.pending {
		if pkt.seq == seq {
			tw.inFlight -= pkt.bytes
			if tw.inFlight < 0 {
				tw.inFlight = 0
			}
			tw.pending = append(tw.pending[:i], tw.pending[i+1:]...)
			tw.signalLocked()
			break
		}
	}
	tw.mu.Unlock()
}

func (tw *txWindow) close(err error) {
	tw.mu.Lock()
	if tw.closed {
		tw.mu.Unlock()
		return
	}
	tw.closed = true
	tw.closeErr = err
	tw.signalLocked()
	tw.mu.Unlock()
}

func (tw *txWindow) signalLocked() {
	if tw.waitCh == nil {
		tw.waitCh = make(chan struct{})
		return
	}
	old := tw.waitCh
	tw.waitCh = make(chan struct{})
	close(old)
}

func (tw *txWindow) maybeAdjustLocked() {
	if tw.targetLossRatio <= 0 || tw.lossSampleSize <= 0 {
		return
	}
	if tw.sampleSent < tw.lossSampleSize {
		return
	}
	ratio := 0.0
	if tw.sampleSent > 0 {
		ratio = float64(tw.sampleLost) / float64(tw.sampleSent)
	}
	switch {
	case ratio > tw.targetLossRatio:
		scale := tw.targetLossRatio / ratio
		if scale < 0.5 {
			scale = 0.5
		}
		newMax := int(float64(tw.maxBytes) * scale)
		if newMax < tw.minBytes {
			newMax = tw.minBytes
		}
		tw.maxBytes = newMax
	case ratio < tw.targetLossRatio/2:
		increase := tw.maxBytes / 10
		if increase < 1 {
			increase = 1
		}
		newMax := tw.maxBytes + increase
		if tw.maxBytesCeiling > 0 && newMax > tw.maxBytesCeiling {
			newMax = tw.maxBytesCeiling
		}
		tw.maxBytes = newMax
	}
	tw.sampleSent = 0
	tw.sampleLost = 0
}
