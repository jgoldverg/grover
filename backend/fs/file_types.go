package fs

type ListerType string

const (
	ListerFilesystem ListerType = "localfs"
	ListerHTTP       ListerType = "http"
)

type Chunk interface {
	FileID() string
	Offset() uint64
	Length() uint64
	Lastpart() bool
	Data() [][]byte
}

type FileLister interface {
	List(rootPath string) []FileInfo
}

type RmOperation interface {
	Rm(filePath string) FileInfo
}

type FileInfo struct {
	ID      string
	AbsPath string
	Size    uint64
}
