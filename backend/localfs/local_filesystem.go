package localfs

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/jgoldverg/grover/backend/chunker"
	"github.com/jgoldverg/grover/backend/filesystem"
	groverFs "github.com/jgoldverg/grover/backend/filesystem"
	"github.com/jgoldverg/grover/backend/pool"
)

type FileSystemIo struct {
	chunks     *chunker.Chunker
	fileInfo   *filesystem.FileInfo
	file       *os.File
	bufferPool *pool.BufferPool
}

func NewFileSystemIo(chunks *chunker.Chunker, file *filesystem.FileInfo) *FileSystemIo {
	return &FileSystemIo{
		chunks:     chunks,
		fileInfo:   file,
		bufferPool: pool.NewBufferPool(chunks.ChunkSize),
	}
}

func (fio *FileSystemIo) Open() error {
	file, err := os.Open(fio.fileInfo.AbsPath)
	if err != nil {
		return err
	}
	fio.file = file
	return nil
}

func (fio *FileSystemIo) Close() error {
	err := fio.file.Close()
	return err
}

func (fio *FileSystemIo) Read() (chunker.Chunk, error) {
	fileChunk, lastChunk := fio.chunks.NextChunk()

	buf := fio.bufferPool.GetBuffer()
	length := int(fileChunk.Length())
	offset := int64(fileChunk.Offset())

	read := 0
	for read < length {
		n, err := fio.file.ReadAt(buf[read:], offset+int64(read))
		read += n
		if err != nil {
			if err == io.EOF && read == length {
				break // exactly done
			}
			fio.bufferPool.PutBuffer(buf) //incase of error put the buffer back
			return nil, err
		}
	}
	ch := chunker.NewFileChunk(fio.fileInfo.ID, fileChunk.Offset(), fileChunk.Length(), lastChunk, buf)
	return ch, nil
}

func (fio *FileSystemIo) Write(chunks []chunker.Chunk) error {
	for _, chunk := range chunks {
		var written uint64 = 0
		for written < chunk.Length() {
			w, _ := fio.file.WriteAt(chunk.Data(), int64(chunk.Offset()))
			written += uint64(w)
		}
	}
	return nil
}

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
