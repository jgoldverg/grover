package output

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/jgoldverg/grover/pkg/metrics"
	"github.com/pterm/pterm"
)

// MetricsDisplay renders live telemetry using pterm primitives.
type MetricsDisplay struct {
	title     string
	collector *metrics.TransferCollector
	interval  time.Duration

	mu     sync.Mutex
	area   *pterm.AreaPrinter
	ticker *time.Ticker
	cancel context.CancelFunc
	active bool
	writer io.Writer
}

func NewMetricsDisplay(title string, collector *metrics.TransferCollector) *MetricsDisplay {
	if strings.TrimSpace(title) == "" {
		title = "Transfer Metrics"
	}
	return &MetricsDisplay{
		title:     title,
		collector: collector,
		interval:  500 * time.Millisecond,
	}
}

// WithWriter allows rendering into an existing writer (e.g. MultiPrinter section).
func (d *MetricsDisplay) WithWriter(w io.Writer) *MetricsDisplay {
	d.writer = w
	return d
}

// Start begins rendering the live dashboard. No-op when collector is nil.
func (d *MetricsDisplay) Start(ctx context.Context) error {
	if d == nil || d.collector == nil || d.active {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	d.mu.Lock()
	d.ticker = time.NewTicker(d.interval)
	d.cancel = cancel
	d.active = true
	useArea := d.writer == nil
	d.mu.Unlock()

	if useArea {
		area, err := pterm.DefaultArea.WithRemoveWhenDone(false).Start()
		if err != nil {
			d.cleanup()
			return err
		}
		d.mu.Lock()
		d.area = area
		d.mu.Unlock()
	}

	go d.loop(ctx)
	return nil
}

func (d *MetricsDisplay) loop(ctx context.Context) {
	d.render()
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.ticker.C:
			d.render()
		}
	}
}

// Stop clears the live board and prints a final snapshot.
func (d *MetricsDisplay) Stop() {
	if d == nil {
		return
	}
	if !d.cleanup() {
		d.printFinal()
		return
	}
	d.printFinal()
}

func (d *MetricsDisplay) cleanup() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.active {
		return false
	}
	cancel := d.cancel
	ticker := d.ticker
	area := d.area
	d.area = nil
	d.ticker = nil
	d.cancel = nil
	d.active = false
	if cancel != nil {
		cancel()
	}
	if ticker != nil {
		ticker.Stop()
	}
	if area != nil {
		_ = area.Stop()
	}
	return true
}

func (d *MetricsDisplay) render() {
	if d.collector == nil {
		return
	}
	snapshot := d.collector.Snapshot()
	content := d.renderContent(snapshot)

	d.mu.Lock()
	area := d.area
	writer := d.writer
	d.mu.Unlock()
	switch {
	case writer != nil:
		_, _ = fmt.Fprintf(writer, "%s\r", content)
	case area != nil:
		area.Update(content)
	}
}

func (d *MetricsDisplay) renderContent(snap metrics.TransferSnapshot) string {
	table := d.tableString(snap)
	header := pterm.DefaultHeader.
		WithBackgroundStyle(pterm.NewStyle(pterm.BgBlue)).
		WithTextStyle(pterm.NewStyle(pterm.FgLightWhite, pterm.Bold)).
		WithFullWidth().
		Sprint(d.title)

	meta := fmt.Sprintf("Elapsed: %s    Direction: %s",
		formatDuration(snap.Elapsed),
		strings.ToUpper(strings.TrimSpace(snap.Direction)))

	return fmt.Sprintf("%s\n%s\n%s", header, table, meta)
}

func (d *MetricsDisplay) tableString(snap metrics.TransferSnapshot) string {
	data := pterm.TableData{
		{"Metric", "Value"},
		{"Network Throughput", formatMbps(snap.ThroughputMbps)},
		{"Goodput", formatMbps(snap.GoodputMbps)},
		{"Disk Read", formatMbps(snap.DiskReadMbps)},
		{"Disk Write", formatMbps(snap.DiskWriteMbps)},
		{"TX PPS", formatPps(snap.TxPacketsPerSec)},
		{"RX PPS", formatPps(snap.RxPacketsPerSec)},
		{"TX Mpps", formatMpps(snap.TxMpps)},
		{"RX Mpps", formatMpps(snap.RxMpps)},
		{"Goodput Efficiency", formatPercent(ratioOrZero(snap.GoodputBps, snap.ThroughputBps))},
		{"RTT", formatMillis(snap.RttMs)},
		{"Jitter", formatMillis(snap.JitterMs)},
		{"Retransmissions", fmt.Sprintf("%d (%s)", snap.Retransmissions, formatPercent(snap.RetransmitRate))},
		{"Retransmitted Bytes", formatBytes(snap.BytesRetransmit)},
		{"Bytes Sent", formatBytes(snap.BytesSent)},
		{"Bytes Received", formatBytes(snap.BytesReceived)},
		{"Disk Read Bytes", formatBytes(snap.DiskReadBytes)},
		{"Disk Write Bytes", formatBytes(snap.DiskWriteBytes)},
	}
	table, err := pterm.DefaultTable.WithHasHeader().WithData(data).Srender()
	if err != nil {
		return ""
	}
	return table
}

func (d *MetricsDisplay) printFinal() {
	if d.collector == nil {
		return
	}
	snap := d.collector.Snapshot()
	if snap.BytesSent == 0 && snap.BytesReceived == 0 {
		return
	}
	table := d.tableString(snap)
	d.mu.Lock()
	writer := d.writer
	d.mu.Unlock()
	if writer != nil {
		fmt.Fprintf(writer, "%s\nElapsed: %s    Direction: %s\r",
			table,
			formatDuration(snap.Elapsed),
			strings.ToUpper(strings.TrimSpace(snap.Direction)))
		return
	}
	pterm.Println()
	pterm.DefaultSection.Println(d.title)
	fmt.Println(table)
	fmt.Printf("Elapsed: %s    Direction: %s\n",
		formatDuration(snap.Elapsed),
		strings.ToUpper(strings.TrimSpace(snap.Direction)))
}

func formatMbps(mbps float64) string {
	if mbps <= 0 {
		return "--"
	}
	return fmt.Sprintf("%.2f Mb/s", mbps)
}

func formatMillis(ms float64) string {
	if ms <= 0 {
		return "--"
	}
	return fmt.Sprintf("%.2f ms", ms)
}

func formatPps(pps float64) string {
	if pps <= 0 {
		return "--"
	}
	return fmt.Sprintf("%.0f pkt/s", pps)
}

func formatMpps(mpps float64) string {
	if mpps <= 0 {
		return "--"
	}
	return fmt.Sprintf("%.3f Mpps", mpps)
}

func formatBytes(b uint64) string {
	const kb = 1024
	const mb = kb * 1024
	const gb = mb * 1024
	switch {
	case b >= gb:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(kb))
	case b > 0:
		return fmt.Sprintf("%d B", b)
	default:
		return "0 B"
	}
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "--"
	}
	if d < time.Second {
		return fmt.Sprintf("%d ms", d.Milliseconds())
	}
	rounded := d.Truncate(100 * time.Millisecond)
	return rounded.String()
}

func formatPercent(ratio float64) string {
	if ratio <= 0 {
		return "0%"
	}
	return fmt.Sprintf("%.2f%%", ratio*100)
}

func ratioOrZero(num, denom float64) float64 {
	if denom <= 0 {
		return 0
	}
	return num / denom
}
