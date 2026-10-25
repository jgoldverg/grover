package gserver

import (
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/transfer"
)

// These aliases preserve the original gserver API. New transfer implementations
// should import pkg/transfer; gserver is the RPC adapter, not the runtime owner.
type TransferJobManager = transfer.Manager
type TransferSourcePlanner = transfer.SourcePlanner
type TransferFileExecutor = transfer.FileExecutor
type TransferJobExecutor = transfer.JobExecutor
type TransferEndpointRegistry = transfer.EndpointRegistry
type TransferExecutionContext = transfer.Execution
type TransferFilePlan = transfer.FilePlan
type TransferStreamPlan = transfer.StreamPlan
type FilesystemSourcePlanner = transfer.FilesystemSourcePlanner
type ProtocolTransferExecutor = transfer.ProtocolExecutor
type LocalFileExecutor = transfer.LocalFileExecutor
type TCPTransferExecutor = transfer.TCPExecutor
type UDPTransferExecutor = transfer.UDPExecutor

func NewTransferJobManager(cfg *internal.ServerConfig, executor TransferJobExecutor) *TransferJobManager {
	return transfer.NewManager(cfg, executor)
}

func NewTransferJobManagerWithComponents(cfg *internal.ServerConfig, planner TransferSourcePlanner, executor TransferFileExecutor) *TransferJobManager {
	return transfer.NewManagerWithComponents(cfg, planner, executor)
}
