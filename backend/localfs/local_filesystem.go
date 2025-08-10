package localfs

import "github.com/jgoldverg/GoRover/backend"

type FileSystemReader struct {
	absPath string
}

type FileSystemWriter struct {
	absPath string
}

func NewFileSystemReader(file backend.FileInfo) *FileSystemReader {
	return &FileSystemReader{
		absPath: file.AbsPath,
	}
}

func (fr *FileSystemReader) Read() (*backend.FileChunk, error) {

	return nil, nil
}

func NewFileSystemWriter() *FileSystemWriter {
	return &FileSystemWriter{}
}

func (fr *FileSystemWriter) Write([]*backend.FileChunk) error {
	return nil
}
