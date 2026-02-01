package gserver

import (
	"os"

	"github.com/google/uuid"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

type streamBinding struct {
	streamID uint32
	leaseID  uuid.UUID

	path            string
	mode            pb.OpenSessionRequest_Mode
	sizeHint        int64
	verifyChecksum  bool
	overwritePolicy pb.OverwritePolicy

	file     *os.File
	fileSize int64
	offset   uint64

	tracker *udpwire.SackTracker
}

func newStreamBinding(
	streamID uint32,
	mode pb.OpenSessionRequest_Mode,
	path string,
	sizeHint int64,
	verifyChecksum bool,
	overwrite pb.OverwritePolicy,
) *streamBinding {
	return &streamBinding{
		streamID:        streamID,
		leaseID:         uuid.New(),
		mode:            mode,
		path:            path,
		sizeHint:        sizeHint,
		verifyChecksum:  verifyChecksum,
		overwritePolicy: overwrite,
		tracker:         udpwire.NewSackTracker(),
	}
}

func (s *ServerSession) binding(streamID uint32) *streamBinding {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.streams[streamID]
}

func (s *ServerSession) addBinding(binding *streamBinding) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams == nil {
		s.streams = make(map[uint32]*streamBinding)
	}
	s.streams[binding.streamID] = binding
}

func (s *ServerSession) removeBinding(streamID uint32) *streamBinding {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams == nil {
		return nil
	}
	binding := s.streams[streamID]
	delete(s.streams, streamID)
	return binding
}

func (s *ServerSession) nextIdleStream() (uint32, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, id := range s.StreamIDs {
		if s.streams == nil || s.streams[id] == nil {
			return id, true
		}
	}
	return 0, false
}

func (b *streamBinding) closeFile() {
	if b == nil || b.file == nil {
		return
	}
	_ = b.file.Close()
	b.file = nil
}
