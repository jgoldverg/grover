package http

import (
	"net/http"

	"github.com/jgoldverg/grover/backend/fs"
)

type HttpReader struct {
	clientPool  *HttpClientPool
	httpClient  *http.Client
	fileMetdata *fs.FileInfo
}

func NewHttpReader(clientPool *HttpClientPool, fileMetdata *fs.FileInfo) *HttpReader {
	return &HttpReader{
		clientPool:  clientPool,
		fileMetdata: fileMetdata,
	}
}

func (hr *HttpReader) Open() error {
	client, err := hr.clientPool.Get(hr.fileMetdata.ID)
	if err != nil {
		return err
	}
	hr.httpClient = client
	return nil
}

func (hr *HttpReader) Close() error {
	hr.clientPool.Put(hr.httpClient)
	hr.httpClient = nil
	return nil
}

func (hr *HttpReader) Read() (*fs.Chunk, error) {

	return nil, nil
}
