package output

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jgoldverg/grover/pkg/metrics"
)

// TransferSummary prints append-only status lines. It is intentionally boring:
// this remains readable over SSH and in redirected logs.
type TransferSummary struct {
	direction  string
	collector  *metrics.TransferCollector
	interval   time.Duration
	totalFiles int
	totalBytes uint64
	completed  *atomic.Uint64
	writer     io.Writer

	mu       sync.Mutex
	cancel   context.CancelFunc
	active   bool
	lastAt   time.Time
	lastGood uint64
	lastNet  uint64
}

func NewTransferSummary(direction string, collector *metrics.TransferCollector, totalFiles int, totalBytes uint64, completed *atomic.Uint64) *TransferSummary {
	direction = strings.ToLower(strings.TrimSpace(direction))
	if direction == "" {
		direction = "transfer"
	}
	return &TransferSummary{
		direction:  direction,
		collector:  collector,
		interval:   2 * time.Second,
		totalFiles: totalFiles,
		totalBytes: totalBytes,
		completed:  completed,
		writer:     io.Discard,
	}
}

func (s *TransferSummary) WithInterval(interval time.Duration) *TransferSummary {
	if s == nil || interval <= 0 {
		return s
	}
	s.interval = interval
	return s
}

func (s *TransferSummary) WithWriter(w io.Writer) *TransferSummary {
	if s == nil || w == nil {
		return s
	}
	s.writer = w
	return s
}

func (s *TransferSummary) Start(ctx context.Context) error {
	if s == nil || s.collector == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		cancel()
		return nil
	}
	s.cancel = cancel
	s.active = true
	s.mu.Unlock()

	fmt.Fprintf(s.writer, "starting %s: files=%d bytes=%s interval=%s\n", s.direction, s.totalFiles, humanBytes(s.totalBytes), s.interval)
	go s.loop(ctx)
	return nil
}

func (s *TransferSummary) Stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	cancel := s.cancel
	active := s.active
	s.cancel = nil
	s.active = false
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if active {
		s.print("done")
	}
}

func (s *TransferSummary) loop(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.print("progress")
		}
	}
}

func (s *TransferSummary) print(state string) {
	if s == nil || s.collector == nil {
		return
	}
	snap := s.collector.Snapshot()
	completed := uint64(0)
	if s.completed != nil {
		completed = s.completed.Load()
	}
	goodBytes := snap.BytesSent
	networkBytes := snap.BytesSent + snap.BytesRetransmit
	if snap.Direction == "download" || snap.BytesReceived > goodBytes {
		goodBytes = snap.BytesReceived
		networkBytes = snap.NetworkReceived
		if networkBytes < goodBytes {
			networkBytes = goodBytes
		}
	}
	now := time.Now()
	curGoodMbps, curNetMbps := s.intervalRates(now, goodBytes, networkBytes)
	fmt.Fprintf(
		s.writer,
		"%s %s elapsed=%s files=%d/%d good=%s/%s net=%s cur_net=%s cur_good=%s avg_net=%s avg_good=%s efficiency=%.1f%% disk_read=%s disk_write=%s retrans=%d retrans_bytes=%s\n",
		s.direction,
		state,
		shortDuration(snap.Elapsed),
		completed,
		s.totalFiles,
		humanBytes(goodBytes),
		humanBytes(s.totalBytes),
		humanBytes(networkBytes),
		humanMbps(curNetMbps),
		humanMbps(curGoodMbps),
		humanMbps(snap.ThroughputMbps),
		humanMbps(snap.GoodputMbps),
		summaryRatioOrZero(snap.GoodputBps, snap.ThroughputBps)*100,
		humanBytes(snap.DiskReadBytes),
		humanBytes(snap.DiskWriteBytes),
		snap.Retransmissions,
		humanBytes(snap.BytesRetransmit),
	)
}

func (s *TransferSummary) intervalRates(now time.Time, goodBytes, networkBytes uint64) (float64, float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastAt.IsZero() {
		s.lastAt = now
		s.lastGood = goodBytes
		s.lastNet = networkBytes
		return 0, 0
	}
	elapsed := now.Sub(s.lastAt)
	prevGood := s.lastGood
	prevNet := s.lastNet
	s.lastAt = now
	s.lastGood = goodBytes
	s.lastNet = networkBytes
	if elapsed <= 0 {
		return 0, 0
	}
	return mbpsFromDelta(goodBytes, prevGood, elapsed), mbpsFromDelta(networkBytes, prevNet, elapsed)
}

func mbpsFromDelta(cur, prev uint64, elapsed time.Duration) float64 {
	if cur <= prev || elapsed <= 0 {
		return 0
	}
	return float64(cur-prev) * 8 / elapsed.Seconds() / 1_000_000
}

func summaryRatioOrZero(num, den float64) float64 {
	if den <= 0 {
		return 0
	}
	return num / den
}

func humanBytes(b uint64) string {
	const kb = 1024
	const mb = kb * 1024
	const gb = mb * 1024
	switch {
	case b >= gb:
		return fmt.Sprintf("%.2fGB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.2fMB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.2fKB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

func humanMbps(mbps float64) string {
	if mbps <= 0 {
		return "--"
	}
	return fmt.Sprintf("%.2fMb/s", mbps)
}

func shortDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	return d.Truncate(time.Second).String()
}
