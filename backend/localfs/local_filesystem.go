package localfs

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"

	groverFs "github.com/jgoldverg/grover/backend/filesystem"
)

type FileSystemOperations struct{}

func NewFileSystemOperations() *FileSystemOperations {
	return &FileSystemOperations{}
}

func (o *FileSystemOperations) List(ctx context.Context, root string, recursive bool) ([]groverFs.FileInfo, error) {
	files := make([]groverFs.FileInfo, 0, 128)

	fi, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}

	// If root is a file, return just that file.
	if !fi.IsDir() {
		return []groverFs.FileInfo{
			{
				ID:      filepath.Base(root),
				AbsPath: root,
				Size:    uint64(fi.Size()),
			},
		}, nil
	}

	if !recursive {
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			if e.IsDir() {
				continue
			}
			info, err := e.Info()
			if err != nil {
				return nil, err
			}
			fp := filepath.Join(root, e.Name())
			files = append(files, groverFs.FileInfo{
				ID:      e.Name(),
				AbsPath: fp,
				Size:    uint64(info.Size()),
			})
		}
		return files, nil
	}

	// Recursive: walk the whole tree.
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// We only collect files (not dirs). WalkDir already recurses.
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files = append(files, groverFs.FileInfo{
			ID:      d.Name(),
			AbsPath: p,
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

func (o *FileSystemOperations) Mkdir(ctx context.Context, path string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return os.MkdirAll(path, 0755)
}

func (o *FileSystemOperations) Rename(ctx context.Context, oldPath, newPath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return os.Rename(oldPath, newPath)
}
