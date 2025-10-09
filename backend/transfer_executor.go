package backend

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend/chunker"
	"github.com/jgoldverg/grover/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TransferExecutor struct {
	fileNames       []string
	reader          []Reader
	fileChunks      []*chunker.Chunker
	writer          []Writer
	transferRequest *TransferRequest
	credStore       CredentialStorage
	jobStorage      JobPersistence
	ccWaitGroup     sync.WaitGroup
	pWaitGroup      sync.WaitGroup
}

func NewTransferExecutor(tr *TransferRequest, ce CredentialStorage, persist JobPersistenceType) *TransferExecutor {
	ret := &TransferExecutor{
		transferRequest: tr,
		credStore:       ce,
		jobStorage:      JobPersistenceFactory(persist),
	}

	ret.ccWaitGroup.Add(int(tr.transferParams.Concurrency))
	ret.pWaitGroup.Add(int(tr.transferParams.Parallelism))
	internal.Info("created transfer executor", internal.Fields{
		"persistence_store": fmt.Sprintf("%T", ret.jobStorage),
		"persistence_type":  persist,
		"transfer_request":  fmt.Sprintf("%+v", tr),
	})
	return ret
}

func (te *TransferExecutor) PrepareTransfer() error {
	tr := te.transferRequest
	fn := make([]string, len(tr.fileEntries))
	ch := make([]*chunker.Chunker, len(tr.fileEntries))

	for i, _ := range tr.fileEntries {
		fn[i] = tr.fileEntries[i].AbsPath
		ch[i] = chunker.NewChunker(tr.fileEntries[i], uint64(tr.transferParams.ChunkSize))
		ch[i].MakeChunks()
	}
	te.fileNames = fn
	te.fileChunks = ch
	return te.prepareTransfer()
}

func (te *TransferExecutor) prepareTransfer() error {
	sourceCred, err := ResolveCredential(te.credStore, te.transferRequest.SourceCredId)
	if err != nil {
		return err
	}
	destCred, err := ResolveCredential(te.credStore, te.transferRequest.DestCredId)

	if err != nil {
		return err
	}

	//for every file we create exactly one reader and one writer
	for i, _ := range te.fileNames {
		reader, err := ReaderFactory(BackendType(sourceCred.GetType()), sourceCred, te.fileChunks[i], te.transferRequest.fileEntries[i])
		if err != nil {
			return err
		}
		te.reader[i] = reader
		writer, err := WriterFactory(BackendType(destCred.GetType()), destCred, te.fileChunks[i], te.transferRequest.fileEntries[i])
		te.writer[i] = writer
	}
	je := JobInit{
		JobID:     uuid.NewString(),
		StartedAt: time.Now(),
		Meta:      make(map[string]string, 0),
	}
	err = te.jobStorage.InitJob(context.Background(), je)
	if err != nil {
		return err
	}
	return nil
}

// StartTransfer launches a single file transfer and processes the fileChunks in parallel
func (te *TransferExecutor) StartTransfer() {
	// first we need to run a checkpoint to store the base parameters of the file transfer
	for i, _ := range te.fileNames {
		//concurrency is here
		te.ccWaitGroup.Go(func() {
			reader := te.reader[i]
			err := reader.Open()
			if err != nil {
				return
			}
			writer := te.writer[i]
			if err := writer.Open(); err != nil {
				return
			}
			for {
				commitSlice := make([]chunker.Chunk, te.transferRequest.transferParams.Pipelining)
				for i := 0; i < int(te.transferRequest.transferParams.Pipelining); i++ {
					chunk, err := reader.Read()
					if err != nil {
						// TODO we need to figure out a way to handle errors in this loop. Do we just keep retrying somehow?? Idk yet
					}
					if chunk == nil {
						break
					}
					commitSlice = append(commitSlice, chunk)

				}
				err := writer.Write(commitSlice)
				if err != nil {
					// TODO for now lets not worry about errors we will need to handle these to do retry somehow.
				}

			}

		})
	}
}

func ResolveCredential(storage CredentialStorage, nameOrUuid string) (Credential, error) {
	if nameOrUuid == "" || len(nameOrUuid) < 1 {
		return nil, nil
	}
	u, err := uuid.Parse(nameOrUuid)
	if err == nil {
		cred, err := storage.GetCredentialByUUID(u)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid credential UUID %q: %v", nameOrUuid, err)
		}
		return cred, nil

	}

	cred, err := storage.GetCredentialByName(nameOrUuid)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "credential name %q: %v", nameOrUuid, err)
	}

	return cred, nil
}
