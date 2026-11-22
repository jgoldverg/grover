// Package metrics holds lock-free transfer counters shared by every stream of a
// job. Hot paths (per chunk, per packet) only do atomic adds; RTT smoothing
// takes a small lock once per ACK.
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Counters is safe for concurrent use. A nil *Counters ignores all updates.
type Counters struct {
	bytesSent       atomic.Uint64
	bytesRetransmit atomic.Uint64
	bytesReceived   atomic.Uint64 // new payload bytes accepted by a receiver
	networkReceived atomic.Uint64 // all payload bytes seen on the wire, including duplicates
	diskRead        atomic.Uint64
	diskWrite       atomic.Uint64
	packetsSent     atomic.Uint64
	packetsReceived atomic.Uint64
	retransmits     atomic.Uint64

	rttMu  sync.Mutex
	rttAvg float64 // milliseconds, EWMA
}

// Snapshot is a point-in-time copy of the counters.
type Snapshot struct {
	BytesSent       uint64
	BytesRetransmit uint64
	BytesReceived   uint64
	NetworkReceived uint64
	DiskRead        uint64
	DiskWrite       uint64
	PacketsSent     uint64
	PacketsReceived uint64
	Retransmits     uint64
	RTTMs           float64
}

func (c *Counters) ObserveSend(n int, retransmit bool) {
	if c == nil || n <= 0 {
		return
	}
	if retransmit {
		c.bytesRetransmit.Add(uint64(n))
		c.retransmits.Add(1)
		return
	}
	c.bytesSent.Add(uint64(n))
}

func (c *Counters) ObserveReceive(n int) {
	if c != nil && n > 0 {
		c.bytesReceived.Add(uint64(n))
	}
}

func (c *Counters) ObserveNetworkReceive(n int) {
	if c != nil && n > 0 {
		c.networkReceived.Add(uint64(n))
	}
}

func (c *Counters) ObserveDiskRead(n int) {
	if c != nil && n > 0 {
		c.diskRead.Add(uint64(n))
	}
}

func (c *Counters) ObserveDiskWrite(n int) {
	if c != nil && n > 0 {
		c.diskWrite.Add(uint64(n))
	}
}

func (c *Counters) ObservePacketSend() {
	if c != nil {
		c.packetsSent.Add(1)
	}
}

func (c *Counters) ObservePacketReceive() {
	if c != nil {
		c.packetsReceived.Add(1)
	}
}

// ObserveAck records one RTT sample.
func (c *Counters) ObserveAck(rtt time.Duration) {
	if c == nil || rtt <= 0 {
		return
	}
	ms := float64(rtt) / float64(time.Millisecond)
	c.rttMu.Lock()
	if c.rttAvg == 0 {
		c.rttAvg = ms
	} else {
		c.rttAvg = 0.875*c.rttAvg + 0.125*ms
	}
	c.rttMu.Unlock()
}

func (c *Counters) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	c.rttMu.Lock()
	rtt := c.rttAvg
	c.rttMu.Unlock()
	return Snapshot{
		BytesSent:       c.bytesSent.Load(),
		BytesRetransmit: c.bytesRetransmit.Load(),
		BytesReceived:   c.bytesReceived.Load(),
		NetworkReceived: c.networkReceived.Load(),
		DiskRead:        c.diskRead.Load(),
		DiskWrite:       c.diskWrite.Load(),
		PacketsSent:     c.packetsSent.Load(),
		PacketsReceived: c.packetsReceived.Load(),
		Retransmits:     c.retransmits.Load(),
		RTTMs:           rtt,
	}
}
