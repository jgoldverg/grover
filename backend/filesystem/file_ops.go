package filesystem

import "context"

type FileOps interface {
	List(ctx context.Context, path string, recursive bool) ([]FileInfo, error)
	Remove(ctx context.Context, path string) error
	Mkdir(ctx context.Context, path string) error
	Rename(ctx context.Context, oldPath, newPath string) error
}

type FileInfo struct {
	ID      string
	AbsPath string
	Size    uint64
}
