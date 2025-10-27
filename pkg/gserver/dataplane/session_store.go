package dataplane

import (
	"errors"
	"sync"
)

// InMemorySessionStore provides a simple threadsafe implementation of SessionStore.
type InMemorySessionStore struct {
	mu       sync.RWMutex
	sessions map[uint32]*SessionMetadata
}

func NewInMemorySessionStore() *InMemorySessionStore {
	return &InMemorySessionStore{
		sessions: make(map[uint32]*SessionMetadata),
	}
}

func (s *InMemorySessionStore) Put(meta *SessionMetadata) error {
	if meta == nil {
		return errors.New("session metadata cannot be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[meta.SessionID] = cloneSessionMetadata(meta)
	return nil
}

func (s *InMemorySessionStore) Get(sessionID uint32) (*SessionMetadata, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.sessions[sessionID]
	if !ok {
		return nil, false
	}
	return cloneSessionMetadata(meta), true
}

func (s *InMemorySessionStore) Delete(sessionID uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sessionID)
}

func cloneSessionMetadata(meta *SessionMetadata) *SessionMetadata {
	if meta == nil {
		return nil
	}
	clone := &SessionMetadata{
		SessionID: meta.SessionID,
		Token:     append([]byte(nil), meta.Token...),
		Streams:   make(map[uint32]*StreamMetadata, len(meta.Streams)),
	}
	for id, stream := range meta.Streams {
		if stream == nil {
			clone.Streams[id] = nil
			continue
		}
		cpy := *stream
		clone.Streams[id] = &cpy
	}
	return clone
}
