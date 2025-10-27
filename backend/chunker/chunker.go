package chunker

import (
	"fmt"
	"math"

	"github.com/jgoldverg/grover/backend/filesystem"
)

type Chunk interface {
	FileID() string
	Offset() uint64
	Length() uint64
	LastPart() bool
	Data() []byte
}

type FileChunk struct {
	fileId   string
	offset   uint64
	length   uint64
	lastpart bool
	data     []byte
}

type Chunker struct {
	fileParts chan Chunk
	fileInfo  *filesystem.FileInfo
	ChunkSize uint64
}

func (fc *FileChunk) FileID() string { return fc.fileId }
func (fc *FileChunk) Offset() uint64 { return fc.offset }
func (fc *FileChunk) Length() uint64 { return fc.length }
func (fc *FileChunk) LastPart() bool { return fc.lastpart }
func (fc *FileChunk) Data() []byte   { return fc.data }

func NewFileChunk(fileId string, offset, length uint64, lastpart bool, data []byte) *FileChunk {
	return &FileChunk{
		fileId:   fileId,
		offset:   offset,
		length:   length,
		lastpart: lastpart,
		data:     data,
	}
}

func NewChunker(fileInfo *filesystem.FileInfo, chunkSize uint64) *Chunker {
	if chunkSize == 0 {
		panic("chunkSize must be > 0")
	}
	totalChunkCount := (fileInfo.Size + chunkSize - 1) / chunkSize // ceil div

	// channel capacity must be an int; guard overflow on 32-bit systems
	if totalChunkCount > uint64(math.MaxInt) {
		panic(fmt.Sprintf("chunk count %d exceeds int capacity", totalChunkCount))
	}

	return &Chunker{
		fileParts: make(chan Chunk, int(totalChunkCount)),
		fileInfo:  fileInfo,
		ChunkSize: chunkSize, // <-- critical fix
	}
}

func (ckr *Chunker) MakeChunks() {
	defer close(ckr.fileParts)

	fileSize := ckr.fileInfo.Size
	chunkSize := ckr.ChunkSize
	fileID := ckr.fileInfo.ID

	// Zero-length file: no chunks (or emit one empty chunk if that's your convention)
	if fileSize == 0 {
		return
	}

	for offset := uint64(0); offset < fileSize; offset += chunkSize {
		remaining := fileSize - offset
		length := chunkSize
		if remaining < chunkSize {
			length = remaining
		}
		lastPart := (offset+length == fileSize)

		ckr.fileParts <- &FileChunk{
			fileId:   fileID,
			offset:   offset,
			length:   length,
			lastpart: lastPart,
			data:     nil, // fill later by reader logic
		}
	}
}

// NextChunk receives the next chunk from the channel.
// ok == false means the channel is closed.
func (ckr *Chunker) NextChunk() (Chunk, bool) {
	ch, ok := <-ckr.fileParts
	return ch, ok
}
