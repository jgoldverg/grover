package transfer

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func (m *destinationReceiver) serveTransferEndpointTCP(listener *net.TCPListener, root string) {
	if listener == nil {
		return
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			internal.Debug("tcp transfer listener closed", internal.Fields{internal.FieldError: err.Error()})
			return
		}
		go m.receiveTransferFileTCP(conn, root)
	}
}

func (m *destinationReceiver) receiveTransferFileTCP(conn net.Conn, root string) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	reader := bufio.NewReader(conn)
	magic, err := reader.ReadString('\n')
	if err != nil || (magic != tcpJobMagicV1 && magic != tcpJobMagicV2 && magic != tcpJobMagicV3 && magic != tcpJobMagicV4) {
		if errors.Is(err, io.EOF) && magic == "" {
			internal.Debug("tcp transfer idle connection closed", internal.Fields{"remote": remote})
			return
		}
		fields := internal.Fields{"remote": remote}
		if err != nil {
			fields[internal.FieldError] = err.Error()
		} else {
			fields["magic"] = strings.TrimSpace(magic)
		}
		internal.Warn("drop invalid tcp transfer header", fields)
		return
	}
	var header [40]byte
	headerLen := 12
	if magic == tcpJobMagicV2 {
		headerLen = 16
	} else if magic == tcpJobMagicV3 {
		headerLen = 36
	} else if magic == tcpJobMagicV4 {
		headerLen = 40
	}
	if _, err := io.ReadFull(reader, header[:headerLen]); err != nil {
		internal.Warn("tcp transfer header read failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote})
		return
	}
	pathLen := binary.BigEndian.Uint32(header[:4])
	size := binary.BigEndian.Uint64(header[4:])
	totalSize := size
	offset := uint64(0)
	streamID := uint32(1)
	jobLen := uint16(0)
	routeLen := uint16(0)
	rootLen := uint32(0)
	if magic == tcpJobMagicV2 {
		jobLen = binary.BigEndian.Uint16(header[12:14])
		routeLen = binary.BigEndian.Uint16(header[14:16])
	} else if magic == tcpJobMagicV3 || magic == tcpJobMagicV4 {
		totalSize = binary.BigEndian.Uint64(header[4:12])
		offset = binary.BigEndian.Uint64(header[12:20])
		size = binary.BigEndian.Uint64(header[20:28])
		streamID = binary.BigEndian.Uint32(header[28:32])
		jobLen = binary.BigEndian.Uint16(header[32:34])
		routeLen = binary.BigEndian.Uint16(header[34:36])
		if magic == tcpJobMagicV4 {
			rootLen = binary.BigEndian.Uint32(header[36:40])
		}
	}
	if pathLen == 0 || pathLen > 1<<20 {
		internal.Warn("drop invalid tcp transfer path length", internal.Fields{"remote": remote, "path_len": pathLen})
		return
	}
	if streamID == 0 || offset > totalSize || size > totalSize-offset {
		internal.Warn("drop invalid tcp transfer range", internal.Fields{
			"remote":    remote,
			"stream_id": streamID,
			"offset":    offset,
			"bytes":     size,
			"total":     totalSize,
		})
		return
	}
	if totalSize > uint64(1<<63-1) || offset > uint64(1<<63-1) {
		internal.Warn("drop oversized tcp transfer range", internal.Fields{"remote": remote, "offset": offset, "total": totalSize})
		return
	}
	pathBytes := make([]byte, pathLen)
	if _, err := io.ReadFull(reader, pathBytes); err != nil {
		internal.Warn("tcp transfer path read failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote})
		return
	}
	jobID := ""
	if jobLen > 0 {
		jobBytes := make([]byte, jobLen)
		if _, err := io.ReadFull(reader, jobBytes); err != nil {
			internal.Warn("tcp transfer job metadata read failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote})
			return
		}
		jobID = string(jobBytes)
	}
	routeID := ""
	if routeLen > 0 {
		routeBytes := make([]byte, routeLen)
		if _, err := io.ReadFull(reader, routeBytes); err != nil {
			internal.Warn("tcp transfer route metadata read failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote})
			return
		}
		routeID = string(routeBytes)
	}
	effectiveRoot := strings.TrimSpace(root)
	if rootLen > 0 {
		if rootLen > 1<<20 {
			internal.Warn("drop invalid tcp transfer root length", internal.Fields{"remote": remote, "root_len": rootLen})
			return
		}
		rootBytes := make([]byte, rootLen)
		if _, err := io.ReadFull(reader, rootBytes); err != nil {
			internal.Warn("tcp transfer root metadata read failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote})
			return
		}
		transferRoot := filepath.Clean(string(rootBytes))
		if !filepath.IsAbs(transferRoot) {
			internal.Warn("drop relative tcp transfer root", internal.Fields{"remote": remote, "root": transferRoot})
			return
		}
		effectiveRoot = transferRoot
	}
	rel := filepath.Clean(filepath.FromSlash(string(pathBytes)))
	if filepath.IsAbs(rel) || rel == "." || strings.HasPrefix(rel, "..") {
		internal.Warn("drop invalid tcp transfer path", internal.Fields{"remote": remote, "path": rel})
		return
	}
	dstPath, err := resolveDestinationFilePath(effectiveRoot, rel)
	if err != nil {
		internal.Warn("tcp transfer destination path failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel})
		return
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		internal.Warn("tcp transfer destination mkdir failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel})
		return
	}
	flags := os.O_CREATE | os.O_WRONLY
	if magic != tcpJobMagicV3 && magic != tcpJobMagicV4 {
		flags |= os.O_TRUNC
	}
	dst, err := os.OpenFile(dstPath, flags, 0o644)
	if err != nil {
		internal.Warn("tcp transfer destination open failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel})
		return
	}
	defer dst.Close()
	if (magic == tcpJobMagicV3 || magic == tcpJobMagicV4) && offset == 0 {
		if err := dst.Truncate(int64(totalSize)); err != nil {
			internal.Warn("tcp transfer destination truncate failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel, "bytes": totalSize})
			return
		}
	}
	if magic == tcpJobMagicV3 || magic == tcpJobMagicV4 {
		if _, err := dst.Seek(int64(offset), io.SeekStart); err != nil {
			internal.Warn("tcp transfer destination seek failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel, "offset": offset})
			return
		}
	}
	internal.Info(jobLogMessage(jobID, "tcp receive started"), internal.Fields{
		"route_id":  routeID,
		"remote":    remote,
		"root":      effectiveRoot,
		"path":      rel,
		"bytes":     size,
		"offset":    offset,
		"stream_id": streamID,
	})
	received := receiveStream{Job: JobIdentity{ID: jobID, RouteID: routeID}, Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP, Root: effectiveRoot, RelativePath: rel, TotalSize: totalSize, Range: StreamPlan{Offset: offset, Size: size, StreamID: streamID}, Remote: remote}
	m.beginDestinationReceive(received)
	if _, err := io.CopyN(dst, reader, int64(size)); err != nil {
		m.finishDestinationReceive(receiveResult{Stream: received, BytesDone: 0, State: pb.RuntimeState_RUNTIME_STATE_FAILED, Error: err.Error()})
		internal.Warn("tcp transfer receive failed", internal.Fields{internal.FieldError: err.Error(), "remote": remote, "path": rel, "bytes": size})
		return
	}
	_ = writeFull(context.Background(), conn, []byte("OK"))
	m.finishDestinationReceive(receiveResult{Stream: received, BytesDone: size, State: pb.RuntimeState_RUNTIME_STATE_DONE, Error: ""})
	internal.Info(jobLogMessage(jobID, "tcp receive completed"), internal.Fields{
		"route_id":  routeID,
		"remote":    remote,
		"path":      rel,
		"bytes":     size,
		"offset":    offset,
		"stream_id": streamID,
	})
}
