package localfs

import (
	"io/fs"
	"os"
	"path/filepath"

	groverFs "github.com/jgoldverg/grover/backend/fs"
	"github.com/jgoldverg/grover/server/log"
	"github.com/pterm/pterm"
)

type FileSystemReader struct {
	absPath string
}

type FileSystemWriter struct {
	absPath string
}

type FileSystemOperations struct {
}

func NewFileSystemOperations() *FileSystemOperations {
	return &FileSystemOperations{}
}

func (fsl *FileSystemOperations) List(rootPath string) []groverFs.FileInfo {
	var files []groverFs.FileInfo

	err := filepath.WalkDir(rootPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}

			files = append(files, groverFs.FileInfo{
				ID:      d.Name(),
				AbsPath: path,
				Size:    uint64(info.Size()),
			})
		}
		return nil
	})

	if err != nil {
		return nil
	}

	return files
}

func (fsl *FileSystemOperations) Rm(path string) bool {
	err := os.RemoveAll(path)
	if err != nil {
		log.Structured(&pterm.Error, "failed to rm path", log.Fields{"path": path})
		return false
	}
	return true
}

func NewFileSystemReader(file groverFs.FileInfo) *FileSystemReader {
	return &FileSystemReader{
		absPath: file.AbsPath,
	}
}

func (fr *FileSystemReader) Read() (*groverFs.Chunk, error) {
	return nil, nil
}

func NewFileSystemWriter() *FileSystemWriter {
	return &FileSystemWriter{}
}

func (fr *FileSystemWriter) Write([]*groverFs.Chunk) error {
	return nil
}
