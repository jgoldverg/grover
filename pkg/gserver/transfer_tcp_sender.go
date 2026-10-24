package gserver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

const (
	tcpJobMagicV1      = "GROVERJOB1\n"
	tcpJobMagicV2      = "GROVERJOB2\n"
	tcpJobMagicV3      = "GROVERJOB3\n"
	tcpJobMagicV4      = "GROVERJOB4\n"
	currentTCPJobMagic = tcpJobMagicV4
)

// TCPTransferExecutor executes file ranges over TCP, including supplied connections.
type TCPTransferExecutor struct{}

func (TCPTransferExecutor) TransferFile(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan) error {
	streamCount := 1
	if exec.StreamsFunc != nil {
		streamCount = int(exec.StreamsFunc())
	}
	if streamCount <= 0 || plan.Size == 0 {
		streamCount = 1
	}
	if uint64(streamCount) > plan.Size && plan.Size > 0 {
		streamCount = int(plan.Size)
	}
	ranges, err := planByteRanges(int64(plan.Size), streamCount)
	if err != nil {
		return err
	}
	if len(ranges) == 0 {
		ranges = []byteRange{{offset: 0, length: 0}}
	}
	if len(ranges) == 1 {
		return sendTCPRange(ctx, exec, plan, 1, ranges[0])
	}

	errCh := make(chan error, len(ranges))
	var wg sync.WaitGroup
	for i, br := range ranges {
		streamID := uint32(i + 1)
		wg.Add(1)
		go func(streamID uint32, br byteRange) {
			defer wg.Done()
			if err := sendTCPRange(ctx, exec, plan, streamID, br); err != nil {
				errCh <- err
			}
		}(streamID, br)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func sendTCPRange(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan, streamID uint32, br byteRange) (err error) {
	if streamID == 0 {
		streamID = 1
	}
	if br.offset < 0 || br.length < 0 {
		return fmt.Errorf("invalid tcp byte range: offset=%d length=%d", br.offset, br.length)
	}
	src, err := openTransferPlanReader(plan, br.offset, br.length)
	if err != nil {
		return err
	}
	defer src.Close()

	reportStreamStart(exec, plan, streamID, uint64(br.offset), uint64(br.length))
	defer func() {
		if err != nil {
			reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
			return
		}
		reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_DONE, "")
	}()

	var conn net.Conn
	if exec.TCPConnProvider != nil {
		conn, err = exec.TCPConnProvider(ctx)
		if err != nil {
			return err
		}
	}
	if conn == nil {
		dialer := net.Dialer{}
		conn, err = dialer.DialContext(ctx, "tcp", net.JoinHostPort(exec.DestData.GetHost(), fmt.Sprintf("%d", exec.DestData.GetPort())))
		if err != nil {
			return err
		}
	}
	defer conn.Close()

	pathBytes := []byte(plan.RelativePath)
	if len(pathBytes) > 1<<20 {
		return fmt.Errorf("relative path too long: %s", plan.RelativePath)
	}
	if err := writeFull(ctx, conn, []byte(currentTCPJobMagic)); err != nil {
		return err
	}
	jobBytes := []byte(strings.TrimSpace(exec.JobID))
	routeBytes := []byte(strings.TrimSpace(exec.RouteID))
	destRootBytes := []byte(strings.TrimSpace(exec.DestRoot))
	if len(jobBytes) > 1<<16-1 || len(routeBytes) > 1<<16-1 || len(destRootBytes) > 1<<20 {
		return fmt.Errorf("tcp job metadata too long: job_id=%d route_id=%d dest_root=%d", len(jobBytes), len(routeBytes), len(destRootBytes))
	}
	var header [40]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(pathBytes)))
	binary.BigEndian.PutUint64(header[4:12], plan.Size)
	binary.BigEndian.PutUint64(header[12:20], uint64(br.offset))
	binary.BigEndian.PutUint64(header[20:28], uint64(br.length))
	binary.BigEndian.PutUint32(header[28:32], streamID)
	binary.BigEndian.PutUint16(header[32:34], uint16(len(jobBytes)))
	binary.BigEndian.PutUint16(header[34:36], uint16(len(routeBytes)))
	binary.BigEndian.PutUint32(header[36:40], uint32(len(destRootBytes)))
	if err := writeFull(ctx, conn, header[:]); err != nil {
		return err
	}
	if err := writeFull(ctx, conn, pathBytes); err != nil {
		return err
	}
	if err := writeFull(ctx, conn, jobBytes); err != nil {
		return err
	}
	if err := writeFull(ctx, conn, routeBytes); err != nil {
		return err
	}
	if err := writeFull(ctx, conn, destRootBytes); err != nil {
		return err
	}

	reader := src
	buf := make([]byte, transferChunkSize(exec))
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := reader.Read(buf)
		if n > 0 {
			if exec.Collector != nil && transferPlanReadsDisk(plan) {
				exec.Collector.ObserveDiskRead(n)
			}
			if err := writeFull(ctx, conn, buf[:n]); err != nil {
				return err
			}
			if exec.Collector != nil {
				exec.Collector.ObserveSend(n, false)
			}
			reportStreamProgress(exec, plan, streamID, n)
		}
		if readErr == io.EOF {
			if cw, ok := conn.(interface{ CloseWrite() error }); ok {
				_ = cw.CloseWrite()
			}
			var ack [2]byte
			if _, err := io.ReadFull(conn, ack[:]); err != nil {
				return err
			}
			if string(ack[:]) != "OK" {
				return errors.New("destination did not acknowledge tcp transfer")
			}
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}
