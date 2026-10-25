package gserver

import (
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/internal/dataport"
)

// Compatibility names for the shared listener allocator.
type DataPortAllocator = dataport.Allocator
type DataPortLease = dataport.Lease

func NewDataPortAllocator(cfg *internal.ServerConfig) (*DataPortAllocator, error) {
	return dataport.NewAllocator(cfg)
}
