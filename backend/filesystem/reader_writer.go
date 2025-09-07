package filesystem

type Stream interface {
	Open() error
	Close() error
}

type Reader interface {
	Read() (*Chunk, error)
}

type Writer interface {
	Write([]*Chunk) error
}
