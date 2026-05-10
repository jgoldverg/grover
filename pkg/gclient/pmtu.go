package gclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

var ErrPMTUUnsupported = errors.New("pmtu discovery unsupported on this platform")

type PMTUService struct{}

func NewPMTUService() *PMTUService {
	return &PMTUService{}
}

func (p *PMTUService) DiscoverPMTU(ctx context.Context, server string, port int, minSize, maxSize int, perTry time.Duration) (int, error) {
	if minSize <= 0 {
		minSize = 1200
	}
	if maxSize <= 0 {
		maxSize = 9000
	}
	if minSize > maxSize {
		return 0, fmt.Errorf("min size %d is greater than max size %d", minSize, maxSize)
	}
	if perTry <= 0 {
		perTry = 300 * time.Millisecond
	}
	if port <= 0 {
		return 0, fmt.Errorf("udp port is required for pmtu discovery")
	}

	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(server, fmt.Sprint(port)))
	if err != nil {
		return 0, err
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	if err := enablePMTUDiscovery(conn, addr); err != nil {
		return 0, err
	}

	lo, hi := minSize, maxSize
	best := 0
	for lo <= hi {
		mid := lo + (hi-lo)/2
		ok, err := probeMTUSize(ctx, conn, mid, perTry)
		if err != nil {
			return 0, err
		}
		if ok {
			best = mid
			lo = mid + 1
			continue
		}
		hi = mid - 1
	}
	if best == 0 {
		return 0, fmt.Errorf("no working pmtu found between %d and %d", minSize, maxSize)
	}
	return best, nil
}

func probeMTUSize(ctx context.Context, conn *net.UDPConn, size int, perTry time.Duration) (bool, error) {
	if size <= 0 {
		return false, nil
	}
	deadline := time.Now().Add(perTry)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return false, err
	}
	buf := make([]byte, size)
	_, err := conn.Write(buf)
	if err == nil {
		return true, nil
	}
	if isPMTUTooLarge(err) {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return false, err
}
