package chunker

import (
	"testing"

	"github.com/jgoldverg/grover/backend/filesystem"
)

func TestNewFileChunk(t *testing.T) {
	fileId := "hello"
	var length uint64 = 2
	var offset uint64 = 2
	lastPart := true
	data := make([]byte, 100)

	fc := NewFileChunk(fileId, offset, length, lastPart, data)
	if len(fc.data) != len(data) {
		t.Errorf("buffer is of wrong size")
	}
	if fileId != fc.fileId {
		t.Error("file id does not match")
	}
	if lastPart != fc.lastpart {
		t.Error("last part is not equal")
	}
	if length != fc.length && offset != fc.offset {
		t.Error("length or offset do not match")
	}
}

func TestNewChunker(t *testing.T) {
	testFile := filesystem.FileInfo{
		ID:      "test",
		AbsPath: "/hello",
		Size:    1000,
	}
	var chunkSize uint64 = 1000

	chunkerTest := NewChunker(&testFile, chunkSize)
	if chunkerTest.fileInfo != &testFile {
		t.Error("files do not match")
	}

	chunkerTest.MakeChunks()
	if len(chunkerTest.fileParts) != 1 {
		t.Error("wrong number of chunks")
	}

}
