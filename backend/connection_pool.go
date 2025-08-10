package backend

// ConnectionPool manages a pool of storage connections
type ConnectionPool interface {
	// Pool management
	Init(size int) error
	Shutdown() error

	// Set connections per host
	SetConnectionSize(count int) error
	SetPoolSize(count int) error
	// Connection access
	Get(fileId string) (any, error)
	Put(any) error

	// Status
	Size() int
	Available() int
}
