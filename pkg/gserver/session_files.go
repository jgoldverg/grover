package gserver

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
)

func (sm *ServerSessions) prepareDataFile(req *pb.OpenSessionRequest) (*os.File, int64, error) {
	switch req.GetMode() {
	case pb.OpenSessionRequest_READ:
		return sm.openFileForRead(req.GetPath())
	case pb.OpenSessionRequest_WRITE:
		f, err := sm.openFileForWrite(req.GetPath())
		return f, 0, err
	default:
		return nil, 0, fmt.Errorf("unsupported session mode %s", req.GetMode().String())
	}
}

func (sm *ServerSessions) openFileForRead(path string) (*os.File, int64, error) {
	if path == "" {
		return nil, 0, errors.New("path is required for read sessions")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, 0, fmt.Errorf("%w: %s", errNotRegularFile, path)
	}
	return f, info.Size(), nil
}

func (sm *ServerSessions) openFileForWrite(path string) (*os.File, error) {
	if path == "" {
		return nil, errors.New("path is required for write sessions")
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." && dir != "/" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return f, nil
}
