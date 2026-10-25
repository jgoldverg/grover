package transfer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/udpdataplane"
)

func (m *destinationReceiver) serveTransferEndpointUDP(conn *net.UDPConn, root string, tuning internal.UDPRuntimeConfig) {
	if conn == nil {
		return
	}
	buf := make([]byte, 64*1024)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		start, ok, err := decodeUDPJobStartPacket(buf[:n])
		if !ok {
			continue
		}
		if err != nil {
			internal.Warn("drop invalid udp transfer start packet", internal.Fields{internal.FieldError: err.Error(), "remote": addr.String()})
			continue
		}
		cfg := udpReceiveConfig{Root: root, Remote: addr, Start: start, Tuning: tuning}
		if err := m.receiveUDPJobFile(context.Background(), conn, cfg); err != nil {
			internal.Warn("udp transfer receive failed", internal.Fields{internal.FieldError: err.Error(), "remote": addr.String()})
		}
	}
}

type udpReceiveConfig struct {
	Root   string
	Remote *net.UDPAddr
	Start  udpJobStartPacket
	Tuning internal.UDPRuntimeConfig
}

func (m *destinationReceiver) receiveUDPJobFile(ctx context.Context, conn *net.UDPConn, cfg udpReceiveConfig) error {
	root, remote, start, tuning := cfg.Root, cfg.Remote, cfg.Start, cfg.Tuning
	if remote == nil {
		return errors.New("udp transfer start missing remote address")
	}
	jobID := start.jobID
	if len(start.streamIDs) == 0 {
		return errors.New("udp transfer start missing stream ids")
	}
	streamID := start.streamIDs[0]
	relPath := filepath.Clean(filepath.FromSlash(start.relPath))
	if filepath.IsAbs(relPath) || relPath == "." || strings.HasPrefix(relPath, "..") {
		return fmt.Errorf("invalid relative path %q", relPath)
	}
	dstPath, err := resolveDestinationFilePath(root, relPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	internal.Info(jobLogMessage(jobID, "udp receive started"), internal.Fields{
		"route_id":   start.routeID,
		"remote":     remote.String(),
		"root":       root,
		"path":       relPath,
		"bytes":      start.size,
		"streams":    len(start.streamIDs),
		"session_id": start.sessionKey,
	})
	received := receiveStream{Job: JobIdentity{ID: jobID, RouteID: start.routeID}, Protocol: pb.DataProtocol_DATA_PROTOCOL_UDP, Root: root, RelativePath: relPath, TotalSize: start.size, Range: StreamPlan{Offset: 0, Size: start.size, StreamID: streamID}, Remote: remote.String()}
	m.beginDestinationReceive(received)
	f, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		m.finishDestinationReceive(receiveResult{Stream: received, BytesDone: 0, State: pb.RuntimeState_RUNTIME_STATE_FAILED, Error: err.Error()})
		return err
	}
	defer f.Close()
	if start.size > uint64(1<<63-1) {
		return fmt.Errorf("udp transfer file too large: %d", start.size)
	}
	if err := f.Truncate(int64(start.size)); err != nil {
		return err
	}
	readyDone := make(chan struct{})
	go repeatUDPReady(conn, remote, start.sessionKey, readyDone)
	defer close(readyDone)
	if start.size == 0 {
		if err := f.Sync(); err != nil {
			return err
		}
		sendUDPJobDoneBurst(conn, remote, start.sessionKey)
		m.finishDestinationReceive(receiveResult{Stream: received, BytesDone: start.size, State: pb.RuntimeState_RUNTIME_STATE_DONE, Error: ""})
		internal.Info(jobLogMessage(jobID, "udp receive completed"), internal.Fields{
			"route_id":   start.routeID,
			"remote":     remote.String(),
			"path":       relPath,
			"bytes":      start.size,
			"streams":    len(start.streamIDs),
			"session_id": start.sessionKey,
		})
		return nil
	}
	receiveCfg := udpdataplane.ReceiveConfig{
		Transport:       udpdataplane.NewUDPConnTransport(conn),
		RemoteAddr:      remote,
		SessionID:       fmt.Sprintf("%d", start.sessionKey),
		SessionKey:      start.sessionKey,
		StreamID:        streamID,
		StreamIDs:       append([]uint32(nil), start.streamIDs...),
		BufferSize:      64 * 1024,
		ExpectedSize:    start.size,
		AckEveryPackets: tuning.AckEveryPackets,
		AckEvery:        tuning.AckEvery,
		BatchPackets:    tuning.BatchPackets,
		WriteFlushBytes: uint64(tuning.WriteFlushBytes),
	}
	var receiveErr error
	if len(start.streamIDs) > 1 {
		_, receiveErr = udpdataplane.ReceiveMany(ctx, receiveCfg, f)
	} else {
		_, receiveErr = udpdataplane.Receive(ctx, receiveCfg, f)
	}
	if receiveErr != nil {
		m.finishDestinationReceive(receiveResult{Stream: received, BytesDone: 0, State: pb.RuntimeState_RUNTIME_STATE_FAILED, Error: receiveErr.Error()})
		return receiveErr
	}
	if err := f.Sync(); err != nil {
		return err
	}
	sendUDPJobDoneBurst(conn, remote, start.sessionKey)
	m.finishDestinationReceive(receiveResult{Stream: received, BytesDone: start.size, State: pb.RuntimeState_RUNTIME_STATE_DONE, Error: ""})
	internal.Info(jobLogMessage(jobID, "udp receive completed"), internal.Fields{
		"route_id":   start.routeID,
		"remote":     remote.String(),
		"path":       relPath,
		"bytes":      start.size,
		"streams":    len(start.streamIDs),
		"session_id": start.sessionKey,
	})
	return nil
}

func repeatUDPReady(conn *net.UDPConn, remote *net.UDPAddr, sessionKey uint32, done <-chan struct{}) {
	packet := encodeUDPJobReadyPacket(sessionKey)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, _ = conn.WriteToUDP(packet, remote)
		select {
		case <-done:
			return
		case <-ticker.C:
		}
	}
}

func sendUDPJobDoneBurst(conn *net.UDPConn, remote *net.UDPAddr, sessionKey uint32) {
	packet := encodeUDPJobDonePacket(sessionKey)
	for i := 0; i < 20; i++ {
		_, _ = conn.WriteToUDP(packet, remote)
		time.Sleep(25 * time.Millisecond)
	}
}
