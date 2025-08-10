package localfs

import (
	"io/fs"
	"path/filepath"

	groverFs "github.com/jgoldverg/grover/backend/fs"
)

type FileSystemReader struct {
	absPath string
}

type FileSystemWriter struct {
	absPath string
}

type FileSystemLister struct {
}

func NewFileSystemLister() *FileSystemLister {
	return &FileSystemLister{}
}

func (fsl *FileSystemLister) List(rootPath string) []groverFs.FileInfo {
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
