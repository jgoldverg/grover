package metrics

import (
	"math"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultNamespace  = "grover"
	subsystemTransfer = "transfer"
)

// TransferCollector keeps track of client side network statistics and exposes
// them via Prometheus compatible collectors.
type TransferCollector struct {
	mu        sync.RWMutex
	namespace string
	registry  *prometheus.Registry

	startTime       time.Time
	bytesSent       uint64
	bytesRetransmit uint64
	bytesReceived   uint64
	diskReadBytes   uint64
	diskWriteBytes  uint64
	packetsSent     uint64
	packetsReceived uint64
	retransmissions uint64
	ackSamples      uint64
	lastAckMs       float64
	rttAvgMs        float64
	jitterMs        float64
}

// TransferSnapshot represents a point-in-time view of the collected metrics.
type TransferSnapshot struct {
	Direction       string
	Elapsed         time.Duration
	BytesSent       uint64
	BytesReceived   uint64
	DiskReadBytes   uint64
	DiskWriteBytes  uint64
	BytesRetransmit uint64
	PacketsSent     uint64
	PacketsReceived uint64
	Retransmissions uint64
	ThroughputBps   float64
	GoodputBps      float64
	ThroughputMbps  float64
	GoodputMbps     float64
	DiskReadBps     float64
	DiskWriteBps    float64
	DiskReadMbps    float64
	DiskWriteMbps   float64
	TxPacketsPerSec float64
	RxPacketsPerSec float64
	TxMpps          float64
	RxMpps          float64
	RetransmitRate  float64
	RttMs           float64
	JitterMs        float64
}

// NewTransferCollector creates a collector and wires up prometheus collectors.
func NewTransferCollector(namespace string) *TransferCollector {
	if strings.TrimSpace(namespace) == "" {
		namespace = defaultNamespace
	}
	reg := prometheus.NewRegistry()
	tc := &TransferCollector{
		namespace: namespace,
		registry:  reg,
	}
	tc.registerMetrics()
	return tc
}

// Registry returns the prometheus registry managed by this collector.
func (c *TransferCollector) Registry() *prometheus.Registry {
	return c.registry
}

// ObserveSend records bytes that were sent by the client. When retransmit is
// true the bytes are accounted separately to derive goodput vs throughput.
func (c *TransferCollector) ObserveSend(bytes int, retransmit bool) {
	if bytes <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ensureStartTimeLocked()
	if retransmit {
		c.bytesRetransmit += uint64(bytes)
		c.retransmissions++
		return
	}
	c.bytesSent += uint64(bytes)
}

// ObserveReceive records bytes received from the server.
func (c *TransferCollector) ObserveReceive(bytes int) {
	if bytes <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ensureStartTimeLocked()
	c.bytesReceived += uint64(bytes)
}

// ObservePacketSend records a network packet transmission.
func (c *TransferCollector) ObservePacketSend() {
	c.mu.Lock()
	c.ensureStartTimeLocked()
	c.packetsSent++
	c.mu.Unlock()
}

// ObservePacketReceive records a received network packet.
func (c *TransferCollector) ObservePacketReceive() {
	c.mu.Lock()
	c.ensureStartTimeLocked()
	c.packetsReceived++
	c.mu.Unlock()
}

// ObserveDiskRead records bytes sourced from local disk.
func (c *TransferCollector) ObserveDiskRead(bytes int) {
	if bytes <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ensureStartTimeLocked()
	c.diskReadBytes += uint64(bytes)
}

// ObserveDiskWrite records bytes written to local disk.
func (c *TransferCollector) ObserveDiskWrite(bytes int) {
	if bytes <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ensureStartTimeLocked()
	c.diskWriteBytes += uint64(bytes)
}

// ObserveAck stores RTT/jitter data derived from ACK round trips.
func (c *TransferCollector) ObserveAck(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ensureStartTimeLocked()
	sample := float64(d) / float64(time.Millisecond)
	if c.ackSamples == 0 {
		c.rttAvgMs = sample
		c.jitterMs = 0
	} else {
		diff := math.Abs(sample - c.lastAckMs)
		if c.jitterMs == 0 {
			c.jitterMs = diff
		} else {
			c.jitterMs = (c.jitterMs*0.7 + diff*0.3)
		}
		c.rttAvgMs = (c.rttAvgMs*float64(c.ackSamples) + sample) / float64(c.ackSamples+1)
	}
	c.lastAckMs = sample
	c.ackSamples++
}

// Snapshot creates a read-only view of the collected metrics.
func (c *TransferCollector) Snapshot() TransferSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.buildSnapshotLocked(time.Now())
}

func (c *TransferCollector) buildSnapshotLocked(now time.Time) TransferSnapshot {
	primaryBytes := c.bytesSent
	retransBytes := c.bytesRetransmit
	direction := "idle"

	if c.bytesReceived > primaryBytes {
		primaryBytes = c.bytesReceived
		retransBytes = 0
		if c.bytesReceived > 0 {
			direction = "download"
		}
	} else if primaryBytes > 0 {
		direction = "upload"
	}

	elapsed := time.Duration(0)
	if !c.startTime.IsZero() {
		elapsed = now.Sub(c.startTime)
	}

	throughput := rateFromBytes(primaryBytes+retransBytes, elapsed)
	goodput := rateFromBytes(primaryBytes, elapsed)

	var retransRatio float64
	if primaryBytes+retransBytes > 0 {
		retransRatio = float64(retransBytes) / float64(primaryBytes+retransBytes)
	}

	txPktRate := rateFromPackets(c.packetsSent, elapsed)
	rxPktRate := rateFromPackets(c.packetsReceived, elapsed)

	return TransferSnapshot{
		Direction:       direction,
		Elapsed:         elapsed,
		BytesSent:       c.bytesSent,
		BytesReceived:   c.bytesReceived,
		DiskReadBytes:   c.diskReadBytes,
		DiskWriteBytes:  c.diskWriteBytes,
		BytesRetransmit: c.bytesRetransmit,
		PacketsSent:     c.packetsSent,
		PacketsReceived: c.packetsReceived,
		Retransmissions: c.retransmissions,
		ThroughputBps:   throughput,
		GoodputBps:      goodput,
		ThroughputMbps:  throughput * 8 / 1e6,
		GoodputMbps:     goodput * 8 / 1e6,
		DiskReadBps:     rateFromBytes(c.diskReadBytes, elapsed),
		DiskWriteBps:    rateFromBytes(c.diskWriteBytes, elapsed),
		DiskReadMbps:    rateFromBytes(c.diskReadBytes, elapsed) * 8 / 1e6,
		DiskWriteMbps:   rateFromBytes(c.diskWriteBytes, elapsed) * 8 / 1e6,
		TxPacketsPerSec: txPktRate,
		RxPacketsPerSec: rxPktRate,
		TxMpps:          txPktRate / 1e6,
		RxMpps:          rxPktRate / 1e6,
		RetransmitRate:  retransRatio,
		RttMs:           c.rttAvgMs,
		JitterMs:        c.jitterMs,
	}
}

func (c *TransferCollector) registerMetrics() {
	makeGauge := func(name, help string, valueFn func(TransferSnapshot) float64) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: c.namespace,
			Subsystem: subsystemTransfer,
			Name:      name,
			Help:      help,
		}, func() float64 {
			c.mu.RLock()
			defer c.mu.RUnlock()
			return valueFn(c.buildSnapshotLocked(time.Now()))
		})
	}

	makeCounter := func(name, help string, valueFn func() float64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: c.namespace,
			Subsystem: subsystemTransfer,
			Name:      name,
			Help:      help,
		}, valueFn)
	}

	c.registry.MustRegister(makeGauge(
		"throughput_bytes_per_second",
		"Current transfer throughput including retransmissions.",
		func(s TransferSnapshot) float64 { return s.ThroughputBps },
	))
	c.registry.MustRegister(makeGauge(
		"goodput_bytes_per_second",
		"Effective data rate after excluding retransmissions.",
		func(s TransferSnapshot) float64 { return s.GoodputBps },
	))
	c.registry.MustRegister(makeGauge(
		"disk_read_bytes_per_second",
		"Observed local disk read throughput.",
		func(s TransferSnapshot) float64 { return s.DiskReadBps },
	))
	c.registry.MustRegister(makeGauge(
		"disk_write_bytes_per_second",
		"Observed local disk write throughput.",
		func(s TransferSnapshot) float64 { return s.DiskWriteBps },
	))
	c.registry.MustRegister(makeGauge(
		"packets_tx_per_second",
		"Network packets transmitted per second.",
		func(s TransferSnapshot) float64 { return s.TxPacketsPerSec },
	))
	c.registry.MustRegister(makeGauge(
		"packets_rx_per_second",
		"Network packets received per second.",
		func(s TransferSnapshot) float64 { return s.RxPacketsPerSec },
	))
	c.registry.MustRegister(makeGauge(
		"rtt_milliseconds",
		"Smoothed round-trip time derived from ACK samples.",
		func(s TransferSnapshot) float64 { return s.RttMs },
	))
	c.registry.MustRegister(makeGauge(
		"jitter_milliseconds",
		"Average jitter between ACK samples.",
		func(s TransferSnapshot) float64 { return s.JitterMs },
	))
	c.registry.MustRegister(makeGauge(
		"retransmission_ratio",
		"Ratio of retransmitted bytes to total transmitted bytes.",
		func(s TransferSnapshot) float64 { return s.RetransmitRate },
	))

	c.registry.MustRegister(makeCounter(
		"bytes_sent_total",
		"Total payload bytes sent from the client.",
		func() float64 {
			c.mu.RLock()
			defer c.mu.RUnlock()
			return float64(c.bytesSent)
		},
	))
	c.registry.MustRegister(makeCounter(
		"bytes_retransmitted_total",
		"Bytes resent due to retransmissions.",
		func() float64 {
			c.mu.RLock()
			defer c.mu.RUnlock()
			return float64(c.bytesRetransmit)
		},
	))
	c.registry.MustRegister(makeCounter(
		"bytes_received_total",
		"Total bytes received by the client.",
		func() float64 {
			c.mu.RLock()
			defer c.mu.RUnlock()
			return float64(c.bytesReceived)
		},
	))
	c.registry.MustRegister(makeCounter(
		"disk_read_bytes_total",
		"Bytes read from the local disk for this transfer.",
		func() float64 {
			c.mu.RLock()
			defer c.mu.RUnlock()
			return float64(c.diskReadBytes)
		},
	))
	c.registry.MustRegister(makeCounter(
		"disk_write_bytes_total",
		"Bytes written to the local disk for this transfer.",
		func() float64 {
			c.mu.RLock()
			defer c.mu.RUnlock()
			return float64(c.diskWriteBytes)
		},
	))
	c.registry.MustRegister(makeCounter(
		"packets_sent_total",
		"Total packets transmitted by the client.",
		func() float64 {
			c.mu.RLock()
			defer c.mu.RUnlock()
			return float64(c.packetsSent)
		},
	))
	c.registry.MustRegister(makeCounter(
		"packets_received_total",
		"Total packets received by the client.",
		func() float64 {
			c.mu.RLock()
			defer c.mu.RUnlock()
			return float64(c.packetsReceived)
		},
	))
	c.registry.MustRegister(makeCounter(
		"retransmissions_total",
		"Number of retransmission events observed by the client.",
		func() float64 {
			c.mu.RLock()
			defer c.mu.RUnlock()
			return float64(c.retransmissions)
		},
	))
}

func (c *TransferCollector) ensureStartTimeLocked() {
	if c.startTime.IsZero() {
		c.startTime = time.Now()
	}
}

func rateFromBytes(bytes uint64, elapsed time.Duration) float64 {
	if bytes == 0 {
		return 0
	}
	if elapsed <= 0 {
		return 0
	}
	return float64(bytes) / elapsed.Seconds()
}

func rateFromPackets(pkts uint64, elapsed time.Duration) float64 {
	if pkts == 0 || elapsed <= 0 {
		return 0
	}
	return float64(pkts) / elapsed.Seconds()
}
