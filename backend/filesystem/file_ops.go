package filesystem

import "context"

type Chunk interface {
	FileID() string
	Offset() uint64
	Length() uint64
	LastPart() bool
	Data() [][]byte
}

type FileOps interface {
	List(ctx context.Context, path string) ([]FileInfo, error)
	Remove(ctx context.Context, path string) error
}

type FileInfo struct {
	ID      string
	AbsPath string
	Size    uint64
}
