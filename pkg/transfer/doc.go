// Package transfer runs groverd-owned transfer jobs independently of RPC
// handlers. Manager owns scheduling and job lifecycle; SourcePlanner discovers
// work and FileExecutor moves each file. Endpoint storage and destination
// reception have their own state and synchronization.
//
// Transport execution uses pkg/udpdataplane for UDP packets. This package owns
// file metadata and transfer framing; route/session and relay components do not.
package transfer
