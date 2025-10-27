package gio

import "github.com/jgoldverg/grover/backend/chunker"

type GroverReader struct {
}

func NewGroverReader() *GroverReader {
	return &GroverReader{}
}

func (gr *GroverReader) Open() error {
	return nil
}

func (gr *GroverReader) Close() error {
	return nil
}

func (gr *GroverReader) Read() (chunker.Chunk, error) {
	return nil, nil
}
