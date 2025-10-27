package udpclient

import (
	"net"
	"time"
)

type SessionParams struct {
	SessionID   uint32
	Token       []byte
	RemoteAddr  *net.UDPAddr
	LocalAddr   *net.UDPAddr
	ChunkSize   int
	WindowSize  int
	RetryAfter  time.Duration
	AckInterval time.Duration
}

type StreamPlan struct {
	StreamID    uint32
	SourcePath  string
	DestPath    string
	StartOffset uint64
	SizeBytes   int64
}
