package backend

import (
	"context"
	"fmt"
	"path"
	"runtime"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend/chunker"
	"github.com/jgoldverg/grover/backend/filesystem"
	"github.com/jgoldverg/grover/backend/pool"
	"github.com/jgoldverg/grover/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fileTask struct {
	sourceInfo filesystem.FileInfo // represents the file we are moving
	destInfo   filesystem.FileInfo // represents the data destination
	sourceCred Credential
	destCred   Credential
	chunkSize  uint64
	pipelining uint32
	maxRetries uint32
	backoffMs  uint32
}

type TransferExecutor struct {
	transferRequest *TransferRequest
	credStore       CredentialStorage
	jobStorage      JobPersistence
	tasksToProcess  []fileTask
	ConcurrencyPool *pool.WorkerPool[fileTask]
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewTransferExecutor(tr *TransferRequest, ce CredentialStorage, persist JobPersistenceType) *TransferExecutor {

	wp := pool.NewWorkerPool[fileTask](runtime.NumCPU()*2, int(tr.Params.Concurrency))

	ret := &TransferExecutor{
		transferRequest: tr,
		credStore:       ce,
		jobStorage:      JobPersistenceFactory(persist),
		tasksToProcess:  make([]fileTask, 0),
		ConcurrencyPool: wp,
	}
	internal.Info("created transfer executor", internal.Fields{
		"persistence_store": fmt.Sprintf("%T", ret.jobStorage),
		"persistence_type":  persist,
		"transfer_request":  fmt.Sprintf("%+v", tr),
	})
	return ret
}

// PrepareTransfer first we iterate through all user supplied paths to get a list of files, then we prepare all of the tasks to submit against the worker pool
func (te *TransferExecutor) PrepareTransfer() error {
	if len(te.transferRequest.Edges) == 0 {
		return fmt.Errorf("no mapping between sources and destinations provided")
	}
	taskList := make([]fileTask, 0)
	for _, edge := range te.transferRequest.Edges {
		srcEndpoint := te.transferRequest.Sources[edge.SourceIndex]
		srcCred, err := ResolveCredential(te.credStore, srcEndpoint.CredentialID)
		if err != nil {
			return fmt.Errorf("failed to resolve source credential: %v. Error: "+err.Error(), srcEndpoint)
		}
		dstEndpoint := te.transferRequest.Destinations[edge.DestIndex]

		if len(dstEndpoint.Paths) == 0 {
			dstEndpoint.Paths = append(dstEndpoint.Paths, "")
		}

		dstCred, err := ResolveCredential(te.credStore, dstEndpoint.CredentialID)
		if err != nil {
			return fmt.Errorf("failed to resolve destination credential: %v. Error: "+err.Error(), dstEndpoint)
		}
		ops, err := OpsFactory(BackendType(srcCred.GetType()), srcCred)
		if err != nil {
			return fmt.Errorf("failed to create backend for cred=%v. Error="+err.Error(), srcCred)
		}
		for _, sourcePath := range srcEndpoint.Paths {
			fileList, err := ops.List(context.Background(), sourcePath, true)
			if err != nil {
				internal.Error("failed to list files", internal.Fields{
					internal.FieldError: err.Error(),
					"path":              sourcePath,
				})
			}
			for _, file := range fileList {
				task := fileTask{
					sourceCred: srcCred,
					destCred:   dstCred,
					sourceInfo: file,
					destInfo: filesystem.FileInfo{
						ID:      file.ID,
						AbsPath: path.Join(dstEndpoint.Paths[0], file.ID),
						Size:    file.Size,
					},
					chunkSize:  te.transferRequest.Params.ChunkSize,
					pipelining: te.transferRequest.Params.Pipelining,
					maxRetries: te.transferRequest.Params.MaxRetries,
					backoffMs:  te.transferRequest.Params.RetryBackoffMs,
				}
				taskList = append(taskList, task)
			}
		}
	}
	te.tasksToProcess = taskList
	return nil
}

func (te *TransferExecutor) StartTransfer(ctx context.Context) {
	if len(te.tasksToProcess) == 0 {
		internal.Warn("start transfer called with no tasks to process", nil)
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	te.ctx = ctx
	te.cancel = cancel

	te.populateIngress(ctx)

	internal.Info("transfer executor starting worker pool", internal.Fields{
		"tasks":       len(te.tasksToProcess),
		"concurrency": te.transferRequest.Params.Concurrency,
	})

	te.ConcurrencyPool.Run(ctx, te.executeTask)

	internal.Info("transfer executor completed run", nil)
}

func (te *TransferExecutor) StopTransfer() {
	if te.cancel != nil {
		te.cancel()
	}
}

func (te *TransferExecutor) SetConcurrency(size int) {
	te.ConcurrencyPool.SetSize(size)
}

func (te *TransferExecutor) populateIngress(ctx context.Context) {
	ingress := te.ConcurrencyPool.Ingress()
	defer te.ConcurrencyPool.CloseIngress()

	for _, task := range te.tasksToProcess {
		select {
		case <-ctx.Done():
			return
		case ingress <- task:
		}
	}
}

func (te *TransferExecutor) executeTask(ctx context.Context, task fileTask) {
	internal.Info("processing file task", internal.Fields{
		"source": task.sourceInfo.AbsPath,
		"dest":   task.destInfo.AbsPath,
	})

	// Prepare chunks
	ch := chunker.NewChunker(&task.sourceInfo, task.chunkSize)
	ch.MakeChunks()

	// Prepare reader
	reader, err := ReaderFactory(BackendType(task.sourceCred.GetType()), task.sourceCred, ch, &task.sourceInfo)
	if err != nil {
		internal.Error("failed to create reader", internal.Fields{
			internal.FieldError: err,
			"file":              task.sourceInfo.AbsPath,
		})
		return
	}
	if err = reader.Open(); err != nil {
		internal.Error("failed to open reader", internal.Fields{
			internal.FieldError: err,
			"file":              task.sourceInfo.AbsPath,
		})
		return
	}
	defer reader.Close()

	// Prepare writer
	writer, err := WriterFactory(BackendType(task.destCred.GetType()), task.destCred, ch, &task.destInfo)
	if err != nil {
		internal.Error("failed to create writer", internal.Fields{
			internal.FieldError: err,
			"file":              task.destInfo.AbsPath,
		})
		return
	}
	if err = writer.Open(); err != nil {
		internal.Error("failed to open writer", internal.Fields{
			internal.FieldError: err,
			"file":              task.destInfo.AbsPath,
		})
		return
	}
	defer writer.Close()

	batchSize := te.transferRequest.Params.BatchSize
	batch := make([]chunker.Chunk, 0, batchSize)

	for {
		chunk, err := reader.Read()
		if err != nil {
			internal.Error("failed to read chunk", internal.Fields{
				internal.FieldError: err,
				"file":              task.sourceInfo.AbsPath,
			})
			break
		}
		if chunk == nil {
			if len(batch) > 0 {
				if err := writer.Write(batch); err != nil {
					internal.Error("failed to write final batch", internal.Fields{
						internal.FieldError: err,
						"file":              task.destInfo.AbsPath,
					})
				}
			}
			break
		}

		batch = append(batch, chunk)

		if len(batch) == int(batchSize) {
			if err := writer.Write(batch); err != nil {
				internal.Error("failed to write batch", internal.Fields{
					internal.FieldError: err,
					"file":              task.destInfo.AbsPath,
				})
				break
			}
			batch = batch[:0] // reset batch slice
		}
	}

	internal.Info("completed file task", internal.Fields{
		"source": task.sourceInfo.AbsPath,
		"dest":   task.destInfo.AbsPath,
	})
}

func ResolveCredential(storage CredentialStorage, nameOrUuid string) (Credential, error) {
	if nameOrUuid == "" {
		return nil, status.Error(codes.InvalidArgument, "empty credential reference")
	}
	if u, err := uuid.Parse(nameOrUuid); err == nil {
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
