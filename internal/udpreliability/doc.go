// Package udpreliability tracks outstanding UDP packets and selects recovery
// candidates from ACK/SACK feedback. It performs no socket I/O, logging, metrics,
// congestion control, or pacing. A Tracker belongs to one sender goroutine.
package udpreliability
