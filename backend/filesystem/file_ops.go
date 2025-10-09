package filesystem

import "context"

type FileOps interface {
	List(ctx context.Context, path string) ([]FileInfo, error)
	Remove(ctx context.Context, path string) error
}

type FileInfo struct {
	ID      string
	AbsPath string
	Size    uint64
}
