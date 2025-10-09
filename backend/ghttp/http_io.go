package ghttp

import (
	"net/http"

	"github.com/jgoldverg/grover/backend/chunker"
	"github.com/jgoldverg/grover/backend/filesystem"
)

type HttpIo struct {
	clientPool  *HttpClientPool
	httpClient  *http.Client
	fileMetdata *filesystem.FileInfo
}

func NewHttpIo(clientPool *HttpClientPool, fileMetdata *filesystem.FileInfo) *HttpIo {
	return &HttpIo{
		clientPool:  clientPool,
		fileMetdata: fileMetdata,
	}
}

func (hr *HttpIo) Open() error {
	client, err := hr.clientPool.Get(hr.fileMetdata.ID)
	if err != nil {
		return err
	}
	hr.httpClient = client
	return nil
}

func (hr *HttpIo) Close() error {
	hr.clientPool.Put(hr.httpClient)
	hr.httpClient = nil
	return nil
}

func (hr *HttpIo) Read() (chunker.Chunk, error) {

	return nil, nil
}

func (hr *HttpIo) Write([]chunker.Chunk) error {
	return nil
}
