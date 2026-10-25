package transfer

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/udpdataplane"
)

// UDPExecutor performs the job handshake and delegates packet movement
// to udpdataplane. File metadata remains in this transfer layer.
type UDPExecutor struct{}

func (UDPExecutor) TransferFile(ctx context.Context, exec *Execution, plan FilePlan) error {
	src, err := openTransferPlanReader(plan, 0, int64(plan.Size))
	if err != nil {
		return err
	}
	defer src.Close()
	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(exec.Target.Data.GetHost(), fmt.Sprintf("%d", exec.Target.Data.GetPort())))
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	streamCount := 1
	if exec.Hooks.StreamsPerFile != nil {
		streamCount = int(exec.Hooks.StreamsPerFile())
	}
	if streamCount <= 0 {
		streamCount = 1
	}
	if plan.Size == 0 {
		streamCount = 1
	}
	if uint64(streamCount) > plan.Size && plan.Size > 0 {
		streamCount = int(plan.Size)
	}
	streamIDs := make([]uint32, streamCount)
	for i := range streamIDs {
		streamIDs[i] = uint32(i + 1)
	}
	sessionKey, err := randomUint32()
	if err != nil {
		return err
	}
	start := udpJobStartPacket{sessionKey: sessionKey, size: plan.Size, streamIDs: streamIDs,
		relPath: plan.RelativePath, jobID: exec.Job.ID, routeID: exec.Job.RouteID}
	if err := sendUDPJobStartAndWait(ctx, conn, start); err != nil {
		return err
	}
	if plan.Size == 0 {
		reportStreamStart(exec, plan, streamIDs[0], 0, 0)
		if err := waitUDPJobDone(ctx, conn, sessionKey); err != nil {
			reportStreamDone(exec, plan, streamIDs[0], pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
			return err
		}
		reportStreamDone(exec, plan, streamIDs[0], pb.RuntimeState_RUNTIME_STATE_DONE, "")
		return nil
	}
	if len(streamIDs) == 1 {
		reportStreamStart(exec, plan, streamIDs[0], 0, plan.Size)
		_, err := udpdataplane.Send(ctx, udpdataplane.SendConfig{
			Transport:       udpdataplane.NewUDPConnTransport(conn),
			SessionID:       exec.Job.ID,
			SessionKey:      sessionKey,
			StreamID:        streamIDs[0],
			MTU:             exec.UDP.MTU,
			Collector:       exec.Collector,
			FlowControl:     udpFlowControl(exec),
			WindowPackets:   udpWindowPackets(exec),
			BatchPackets:    udpBatchPackets(exec),
			RequireFinalAck: true,
		}, newProgressReader(src, func(n int) {
			if exec.Collector != nil && transferPlanReadsDisk(plan) {
				exec.Collector.ObserveDiskRead(n)
			}
			reportStreamProgress(exec, plan, streamIDs[0], n)
		}))
		if err != nil {
			reportStreamDone(exec, plan, streamIDs[0], pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
			return err
		}
		if err := waitUDPJobDone(ctx, conn, sessionKey); err != nil {
			reportStreamDone(exec, plan, streamIDs[0], pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
			return err
		}
		reportStreamDone(exec, plan, streamIDs[0], pb.RuntimeState_RUNTIME_STATE_DONE, "")
		return nil
	}
	if err := sendFileToUDPDestinationParallel(ctx, exec, plan, sessionKey, streamIDs); err != nil {
		return err
	}
	return waitUDPJobDone(ctx, conn, sessionKey)
}

func sendFileToUDPDestinationParallel(ctx context.Context, exec *Execution, plan FilePlan, sessionKey uint32, streamIDs []uint32) error {
	ranges, err := planByteRanges(int64(plan.Size), len(streamIDs))
	if err != nil {
		return err
	}
	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(exec.Target.Data.GetHost(), fmt.Sprintf("%d", exec.Target.Data.GetPort())))
	if err != nil {
		return err
	}
	errCh := make(chan error, len(ranges))
	var wg sync.WaitGroup
	for i, br := range ranges {
		streamID := streamIDs[i]
		wg.Add(1)
		go func(streamID uint32, br byteRange) {
			defer wg.Done()
			reportStreamStart(exec, plan, streamID, uint64(br.offset), uint64(br.length))
			conn, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
				errCh <- err
				return
			}
			defer conn.Close()
			sr, err := openTransferPlanReader(plan, br.offset, br.length)
			if err != nil {
				reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
				errCh <- err
				return
			}
			defer sr.Close()
			_, err = udpdataplane.Send(ctx, udpdataplane.SendConfig{
				Transport:       udpdataplane.NewUDPConnTransport(conn),
				SessionID:       exec.Job.ID,
				SessionKey:      sessionKey,
				StreamID:        streamID,
				BaseOffset:      uint64(br.offset),
				MTU:             exec.UDP.MTU,
				Collector:       exec.Collector,
				FlowControl:     udpFlowControl(exec),
				WindowPackets:   udpWindowPackets(exec),
				BatchPackets:    udpBatchPackets(exec),
				RequireFinalAck: true,
			}, newProgressReader(sr, func(n int) {
				if exec.Collector != nil && transferPlanReadsDisk(plan) {
					exec.Collector.ObserveDiskRead(n)
				}
				reportStreamProgress(exec, plan, streamID, n)
			}))
			if err != nil {
				reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
				errCh <- err
				return
			}
			reportStreamDone(exec, plan, streamID, pb.RuntimeState_RUNTIME_STATE_DONE, "")
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

func udpWindowPackets(exec *Execution) int {
	if exec != nil && exec.UDP.WindowPackets > 0 {
		return exec.UDP.WindowPackets
	}
	return internal.DefaultTransferRuntimeConfig().UDP.WindowPackets
}

func udpFlowControl(exec *Execution) string {
	if exec != nil {
		switch strings.ToLower(strings.TrimSpace(exec.UDP.FlowControl)) {
		case "bbr":
			return "bbr"
		}
	}
	return internal.DefaultTransferRuntimeConfig().UDP.FlowControl
}

func udpBatchPackets(exec *Execution) int {
	if exec != nil && exec.UDP.BatchPackets > 0 {
		return exec.UDP.BatchPackets
	}
	return internal.DefaultTransferRuntimeConfig().UDP.BatchPackets
}

func udpJobPayloadSize(exec *Execution, relPath string) int {
	mtu := internal.DefaultTransferRuntimeConfig().UDP.MTU
	if exec != nil && exec.UDP.MTU > 0 {
		mtu = exec.UDP.MTU
	}
	jobLen := 0
	routeLen := 0
	if exec != nil {
		jobLen = len([]byte(exec.Job.ID))
		routeLen = len([]byte(exec.Job.RouteID))
	}
	headerLen := len(udpJobMagic) + 1 + 4 + 8 + 2 + 4 + 2 + 2 + len([]byte(relPath)) + jobLen + routeLen
	payload := mtu - headerLen
	if payload < minUDPJobPayloadSize {
		payload = minUDPJobPayloadSize
	}
	maxPayload := maxUDPDatagramPayloadSize - headerLen
	if payload > maxPayload {
		payload = maxPayload
	}
	if payload < 1 {
		return 1
	}
	return payload
}

func sendUDPJobStartAndWait(ctx context.Context, conn *net.UDPConn, start udpJobStartPacket) error {
	packet, err := encodeUDPJobStartPacket(start)
	if err != nil {
		return err
	}
	defer conn.SetReadDeadline(time.Time{})
	buf := make([]byte, len(udpJobMagic)+1+4)
	for attempt := 0; attempt < 10; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := writeUDPDatagram(conn, packet); err != nil {
			return err
		}
		if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			return err
		}
		n, err := conn.Read(buf)
		if err == nil && isUDPJobReadyPacket(buf[:n], start.sessionKey) {
			return nil
		}
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
	}
	return fmt.Errorf("timed out waiting for udp transfer endpoint to accept %s", start.relPath)
}

func waitUDPJobDone(ctx context.Context, conn *net.UDPConn, sessionKey uint32) error {
	deadline := time.Now().Add(5 * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	defer conn.SetReadDeadline(time.Time{})
	buf := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			return err
		}
		n, err := conn.Read(buf)
		if err == nil {
			if isUDPJobDonePacket(buf[:n], sessionKey) {
				return nil
			}
			continue
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			if time.Now().After(deadline) {
				return fmt.Errorf("timed out waiting for udp transfer completion acknowledgement")
			}
			continue
		}
		return err
	}
}

func writeUDPDatagram(conn *net.UDPConn, packet []byte) error {
	if len(packet) > maxUDPDatagramPayloadSize {
		return fmt.Errorf("udp datagram exceeds payload limit: %d", len(packet))
	}
	n, err := conn.Write(packet)
	if err != nil {
		return err
	}
	if n != len(packet) {
		return io.ErrShortWrite
	}
	return nil
}

func randomUint32() (uint32, error) {
	var buf [4]byte
	for {
		if _, err := cryptorand.Read(buf[:]); err != nil {
			return 0, err
		}
		v := binary.BigEndian.Uint32(buf[:])
		if v != 0 {
			return v, nil
		}
	}
}
