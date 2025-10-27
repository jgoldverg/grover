package gio

import (
	"github.com/jgoldverg/grover/backend/chunker"
)

type GroverWriter struct {
}

func NewGroverWriter() *GroverWriter {
	return &GroverWriter{}
}

func (grw *GroverWriter) Write(chunk chunker.Chunk) error {
	return nil
}
func (grw *GroverWriter) Close() error {
	return nil
}

func (grw *GroverWriter) Open() error {
	return nil
}
