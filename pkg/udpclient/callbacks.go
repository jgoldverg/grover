package udpclient

type StreamCallbacks struct {
	OnChunk    func(offset uint64, data []byte) error
	OnComplete func()
}
