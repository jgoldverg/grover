package localfs

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"

	groverFs "github.com/jgoldverg/grover/backend/filesystem"
)

type FileSystemReader struct {
	absPath string
}

type FileSystemWriter struct {
	absPath string
}

type FileSystemOperations struct{}

func NewFileSystemOperations() *FileSystemOperations {
	return &FileSystemOperations{}
}

func (o *FileSystemOperations) List(ctx context.Context, root string) ([]groverFs.FileInfo, error) {
	files := make([]groverFs.FileInfo, 0, 128)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files = append(files, groverFs.FileInfo{
			ID:      d.Name(),
			AbsPath: path,
			Size:    uint64(info.Size()),
		})
		return nil
	})
	return files, err
}

func (o *FileSystemOperations) Remove(ctx context.Context, path string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return os.RemoveAll(path)
}
