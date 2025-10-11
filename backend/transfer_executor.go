package backend

import (
	"fmt"

	"github.com/jgoldverg/grover/internal"
)

type TransferExecutor struct {
	transferRequest *TransferRequest
	credStore       CredentialStorage
	jobStorage      JobPersistence
}

func NewTransferExecutor(tr *TransferRequest, ce CredentialStorage, persist JobPersistenceType) *TransferExecutor {
	ret := &TransferExecutor{
		transferRequest: tr,
		credStore:       ce,
		jobStorage:      JobPersistenceFactory(persist),
	}
	internal.Info("created transfer executor", internal.Fields{
		"persistence_store": fmt.Sprintf("%T", ret.jobStorage),
		"persistence_type":  persist,
		"transfer_request":  fmt.Sprintf("%+v", tr),
	})
	return ret
}

func (te *TransferExecutor) PrepareTransfer() error {
	return fmt.Errorf("transfer executor not yet implemented for multi-endpoint requests")
}

func (te *TransferExecutor) StartTransfer() {
	internal.Warn("transfer executor StartTransfer invoked but not implemented", nil)
}
