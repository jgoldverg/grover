package fs

type BackendType string

const (
	LOCALFSBackend BackendType = "localfs"
	HTTPBackend    BackendType = "http"
	GROVERBackend  BackendType = "grover"
)

var backends = map[BackendType]struct{}{
	LOCALFSBackend: {},
	HTTPBackend:    {},
	GROVERBackend:  {},
}

func IsBackendTypeValid(bt BackendType) bool {
	_, ok := backends[bt]
	return ok
}

type Chunk interface {
	FileID() string
	Offset() uint64
	Length() uint64
	Lastpart() bool
	Data() [][]byte
}

type ListOperation interface {
	List(rootPath string) []FileInfo
}

type RmOperation interface {
	Rm(filePath string) bool
}

type FileInfo struct {
	ID      string
	AbsPath string
	Size    uint64
}
