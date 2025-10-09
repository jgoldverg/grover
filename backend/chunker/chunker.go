package chunker

import "github.com/jgoldverg/grover/backend/filesystem"

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
	fileParts chan FileChunk
	fileInfo  *filesystem.FileInfo
	ChunkSize uint64
}

func (fc *FileChunk) FileID() string {
	return fc.fileId
}

func (fc *FileChunk) Offset() uint64 {
	return fc.offset
}

func (fc *FileChunk) Length() uint64 {
	return fc.length
}

func (fc *FileChunk) LastPart() bool {
	return fc.lastpart
}

func (fc *FileChunk) Data() []byte {
	return fc.data
}

func NewFileChunk(fileId string, offset uint64, length uint64, lastpart bool, data []byte) *FileChunk {
	return &FileChunk{
		fileId:   fileId,
		offset:   offset,
		length:   length,
		lastpart: lastpart,
		data:     data,
	}
}

func NewChunker(fileInfo *filesystem.FileInfo, chunkSize uint64) *Chunker {
	totalChunkCount := (fileInfo.Size + chunkSize - 1) / chunkSize

	return &Chunker{
		fileParts: make(chan FileChunk, totalChunkCount),
		fileInfo:  fileInfo,
	}
}

// MakeChunks generates chunks for the entire file and sends them to the channel
func (ckr *Chunker) MakeChunks() {
	fileSize := ckr.fileInfo.Size
	chunkSize := ckr.ChunkSize
	fileId := ckr.fileInfo.ID

	var offset uint64 = 0
	for offset < fileSize {
		remaining := fileSize - offset
		length := chunkSize
		if remaining < chunkSize {
			length = remaining
		}

		lastPart := (offset+length == fileSize)

		chunk := FileChunk{
			fileId:   fileId,
			offset:   offset,
			length:   length,
			lastpart: lastPart,
			data:     nil, // data can be filled later by reader logic
		}

		ckr.fileParts <- chunk
		offset += length
	}
	close(ckr.fileParts)
}

func (ckr *Chunker) NextChunk() (FileChunk, bool) {
	chunk, ok := <-ckr.fileParts
	return chunk, ok
}
