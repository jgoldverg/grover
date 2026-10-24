package gserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func resolveDestinationFilePath(root, relPath string) (string, error) {
	root = filepath.Clean(root)
	rel := filepath.Clean(filepath.FromSlash(relPath))
	if root == "" || root == "." {
		return "", errors.New("destination root is required")
	}
	if filepath.IsAbs(rel) || rel == "." || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("invalid transfer relative path %q", rel)
	}
	if info, err := os.Stat(root); err == nil && info.IsDir() {
		return filepath.Join(root, rel), nil
	}
	if filepath.Base(root) == filepath.Base(rel) {
		return root, nil
	}
	return filepath.Join(root, rel), nil
}

// ProtocolTransferExecutor dispatches a file to a transport-specific executor.
// Nil fields select the built-in implementation, so the zero value is usable.
type ProtocolTransferExecutor struct {
	Local TransferFileExecutor
	TCP   TransferFileExecutor
	UDP   TransferFileExecutor
}

func (e ProtocolTransferExecutor) TransferFile(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan) error {
	if exec.DestData != nil && strings.TrimSpace(exec.DestData.GetHost()) != "" && exec.DestData.GetPort() != 0 {
		if exec.Protocol == pb.DataProtocol_DATA_PROTOCOL_UDP {
			if e.UDP == nil {
				e.UDP = UDPTransferExecutor{}
			}
			return e.UDP.TransferFile(ctx, exec, plan)
		}
		if e.TCP == nil {
			e.TCP = TCPTransferExecutor{}
		}
		return e.TCP.TransferFile(ctx, exec, plan)
	}
	if e.Local == nil {
		e.Local = LocalFileExecutor{}
	}
	return e.Local.TransferFile(ctx, exec, plan)
}

// LocalFileExecutor copies between paths owned by this groverd.
type LocalFileExecutor struct{}

func (LocalFileExecutor) TransferFile(ctx context.Context, exec *TransferExecutionContext, plan TransferFilePlan) error {
	dstPath, err := resolveDestinationFilePath(exec.DestRoot, plan.RelativePath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	src, err := openTransferPlanReader(plan, 0, int64(plan.Size))
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer dst.Close()
	reportStreamStart(exec, plan, 1, 0, plan.Size)
	buf := make([]byte, transferChunkSize(exec))
	for {
		if err := ctx.Err(); err != nil {
			reportStreamDone(exec, plan, 1, pb.RuntimeState_RUNTIME_STATE_ABORTED, err.Error())
			return err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			if exec.Collector != nil && transferPlanReadsDisk(plan) {
				exec.Collector.ObserveDiskRead(n)
			}
			if err := writeFull(ctx, dst, buf[:n]); err != nil {
				reportStreamDone(exec, plan, 1, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
				return err
			}
			if exec.Collector != nil {
				exec.Collector.ObserveDiskWrite(n)
				exec.Collector.ObserveSend(n, false)
			}
			reportStreamProgress(exec, plan, 1, n)
		}
		if readErr == io.EOF {
			if err := dst.Sync(); err != nil {
				reportStreamDone(exec, plan, 1, pb.RuntimeState_RUNTIME_STATE_FAILED, err.Error())
				return err
			}
			reportStreamDone(exec, plan, 1, pb.RuntimeState_RUNTIME_STATE_DONE, "")
			return nil
		}
		if readErr != nil {
			reportStreamDone(exec, plan, 1, pb.RuntimeState_RUNTIME_STATE_FAILED, readErr.Error())
			return readErr
		}
	}
}

func transferChunkSize(exec *TransferExecutionContext) int {
	if exec != nil && exec.ChunkSizeBytes > 0 {
		if exec.ChunkSizeBytes > 1<<30 {
			return 1 << 30
		}
		return int(exec.ChunkSizeBytes)
	}
	return internal.DefaultTransferRuntimeConfig().ChunkSizeBytes
}

func reportStreamStart(exec *TransferExecutionContext, plan TransferFilePlan, streamID uint32, offset uint64, size uint64) {
	if exec != nil && exec.OnStreamStart != nil {
		exec.OnStreamStart(plan.SourcePath, TransferStreamPlan{StreamID: streamID, Offset: offset, Size: size})
	}
}

func reportStreamProgress(exec *TransferExecutionContext, plan TransferFilePlan, streamID uint32, n int) {
	if exec == nil || n <= 0 {
		return
	}
	if exec.OnProgress != nil {
		exec.OnProgress(plan.SourcePath, n)
	}
	if exec.OnStreamProgress != nil {
		exec.OnStreamProgress(plan.SourcePath, streamID, n)
	}
}

func reportStreamDone(exec *TransferExecutionContext, plan TransferFilePlan, streamID uint32, state pb.RuntimeState, errText string) {
	if exec != nil && exec.OnStreamDone != nil {
		exec.OnStreamDone(plan.SourcePath, streamID, state, errText)
	}
}

type progressReader struct {
	r      io.Reader
	onRead func(int)
}

func newProgressReader(r io.Reader, onRead func(int)) io.Reader {
	return progressReader{r: r, onRead: onRead}
}

func (r progressReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 && r.onRead != nil {
		r.onRead(n)
	}
	return n, err
}

func writeFull(ctx context.Context, w io.Writer, p []byte) error {
	for len(p) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
