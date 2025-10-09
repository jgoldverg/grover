package ghttp

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
)

// BasicCredential captures the credential operations required by the HTTP client pool.
type BasicCredential interface {
	Validate() error
	GetUrl() string
	GetUserName() string
	GetPassword() string
}

var (
	ErrZeroCapacity       = errors.New("client pool has capacity of 0")
	ErrPoolOffline        = errors.New("client pool is offline")
	ErrNoClientsAvailable = errors.New("no clients available in pool")
)

type HttpClientPool struct {
	mu          sync.Mutex     // Primary mutex for the pool
	clients     []*http.Client // All available clients
	freeList    []int          // Available client indices (stack-like)
	inUse       map[int]bool   // Tracks which clients are currently in use
	fileToIndex map[string]int // Maps file IDs to client indices
	indexToFile map[int]string // Reverse mapping of client indices to file IDs
	cond        *sync.Cond     // Condition variable for waiting
	capacity    int            // Maximum pool size
	cred        BasicCredential
	online      bool // Pool status flag
}

func NewHttpClientPool(size int, credential BasicCredential) (*HttpClientPool, error) {
	if credential == nil {
		return nil, fmt.Errorf("credential is required")
	}
	if err := credential.Validate(); err != nil {
		return nil, fmt.Errorf("credential validation failed: %w", err)
	}

	hcp := &HttpClientPool{
		clients:     make([]*http.Client, size),
		freeList:    make([]int, 0, size),
		inUse:       make(map[int]bool),
		fileToIndex: make(map[string]int),
		indexToFile: make(map[int]string),
		capacity:    size,
		cred:        credential,
		online:      false,
	}
	hcp.cond = sync.NewCond(&hcp.mu)

	// Initialize all clients and add them to the free list
	for i := 0; i < size; i++ {
		hcp.clients[i] = &http.Client{}
		hcp.freeList = append(hcp.freeList, i)
	}

	hcp.online = true
	return hcp, nil
}

func (hcp *HttpClientPool) GetUrl() string {
	return hcp.cred.GetUrl()
}

func (hcp *HttpClientPool) ShutDown() error {
	hcp.mu.Lock()
	defer hcp.mu.Unlock()

	hcp.online = false

	// Close all client connections
	for _, client := range hcp.clients {
		if client != nil {
			client.CloseIdleConnections()
		}
	}

	// Clear all maps and slices
	hcp.clients = nil
	hcp.freeList = nil
	hcp.inUse = nil
	hcp.fileToIndex = nil
	hcp.indexToFile = nil

	// Wake all waiting goroutines
	hcp.cond.Broadcast()

	return nil
}

func (hcp *HttpClientPool) SetPoolSize(newSize int) error {
	hcp.mu.Lock()
	defer hcp.mu.Unlock()

	if !hcp.online {
		return ErrPoolOffline
	}

	if newSize == hcp.capacity {
		return nil
	}

	if newSize > hcp.capacity {
		// Expansion - only grow the free list
		for i := hcp.capacity; i < newSize; i++ {
			hcp.clients = append(hcp.clients, &http.Client{})
			hcp.freeList = append(hcp.freeList, i)
		}
		hcp.capacity = newSize
		hcp.cond.Broadcast()
	} else {
		// Shrinking - just adjust capacity and free list
		currentInUse := hcp.capacity - len(hcp.freeList)
		if newSize < currentInUse {
			return fmt.Errorf("cannot shrink below %d (%d clients in use)",
				currentInUse, currentInUse)
		}

		// Calculate how many free clients to keep
		freeToKeep := newSize - currentInUse

		// Trim free list (LIFO order)
		for len(hcp.freeList) > freeToKeep {
			idx := hcp.freeList[len(hcp.freeList)-1]
			hcp.clients[idx].CloseIdleConnections()
			hcp.freeList = hcp.freeList[:len(hcp.freeList)-1]
		}

		hcp.capacity = newSize
	}

	return nil
}

func (hcp *HttpClientPool) Get(fileId string) (*http.Client, error) {
	hcp.mu.Lock()
	defer hcp.mu.Unlock()

	if !hcp.online {
		return nil, ErrPoolOffline
	}

	if hcp.capacity == 0 {
		return nil, ErrZeroCapacity
	}

	// Check if we already have a client for this file
	if idx, exists := hcp.fileToIndex[fileId]; exists {
		return hcp.clients[idx], nil
	}

	// Wait for a free client if none available
	for len(hcp.freeList) == 0 {
		hcp.cond.Wait()
		if !hcp.online {
			return nil, fmt.Errorf("client pool went offline while waiting")
		}
	}

	// Get a client from the free list (LIFO order for better cache locality)
	idx := hcp.freeList[len(hcp.freeList)-1]
	hcp.freeList = hcp.freeList[:len(hcp.freeList)-1]

	// Mark client as in use
	hcp.inUse[idx] = true
	hcp.fileToIndex[fileId] = idx
	hcp.indexToFile[idx] = fileId

	return hcp.clients[idx], nil
}

func (hcp *HttpClientPool) Put(client *http.Client) error {
	hcp.mu.Lock()
	defer hcp.mu.Unlock()

	if !hcp.online {
		return ErrPoolOffline
	}

	// Find which client this is
	var idx int = -1
	for i, c := range hcp.clients {
		if c == client {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("client not found in pool")
	}

	// Check if this client is actually in use
	if !hcp.inUse[idx] {
		return nil // Client already free, nothing to do
	}

	// Get the file ID associated with this client
	fileId := hcp.indexToFile[idx]

	// Clean up tracking
	delete(hcp.inUse, idx)
	delete(hcp.fileToIndex, fileId)
	delete(hcp.indexToFile, idx)

	// Return client to free list
	hcp.freeList = append(hcp.freeList, idx)

	// Notify one waiting goroutine that a client is available
	hcp.cond.Signal()

	return nil
}

func (hcp *HttpClientPool) Size() int {
	hcp.mu.Lock()
	defer hcp.mu.Unlock()
	return len(hcp.freeList)
}

func (hcp *HttpClientPool) Available() int {
	hcp.mu.Lock()
	defer hcp.mu.Unlock()
	return len(hcp.freeList)
}

func (hcp *HttpClientPool) Capacity() int {
	return hcp.capacity
}
