# Developing Grover

Grover has one Go module with focused packages. Package imports enforce the
boundaries below; related implementations stay together instead of having one
package per interface or file.

## Package map

| Package | Owns | Does not own |
| --- | --- | --- |
| `pkg/gserver` | RPC adapters, route/session control, relay forwarding, history queries | Transfer execution and UDP recovery algorithms |
| `pkg/transfer` | Job scheduling, source planning, file execution, destination reception, progress and recording | RPC registration, relay forwarding |
| `pkg/udpdataplane` | UDP sender/receiver loops, feedback I/O, congestion, flow control, pacing, socket transport | Files, job scheduling, route configuration |
| `internal/udpreliability` | Outstanding packet accounting and SACK recovery selection | Sockets, metrics, congestion control |
| `internal/dataport` | TCP/UDP listener leases and port allocation | Files, jobs, routes |
| `internal/transferwire` | Existing UDP transfer control envelope and opaque routing key extraction | File execution or relay state |
| `pkg/udpwire` | Data/status packet formats and receiver SACK tracking | Transfer jobs |

```text
CLI / gclient --control RPC--> gserver
                                |
                                v
                             transfer -----> udpdataplane -----> udpreliability
                                |                 |                    |
                                v                 +------> udpwire <---+
                             dataport

gserver relay ----------------> dataport
gserver relay / transfer -----> transferwire (opaque control header)
```

`pkg/transfer` has no dependency on `gserver`. Neither UDP reliability nor port
allocation imports a job manager. `internal` packages are repository implementation
details rather than a promise of a separately supported public library.

## Trace a transfer job

1. `gserver.TransferJobControlService` validates/adapts RPC calls and delegates to
   `transfer.Manager`.
2. `Manager.StartJob` resolves endpoints and calls the `SourcePlanner` to discover
   files or synthetic payloads.
3. The scheduler chooses files according to concurrency and passes a `FilePlan`
   and `Execution` to the `FileExecutor`.
4. `ProtocolExecutor` selects local, TCP, or UDP movement. UDP performs its file
   handshake here, then delegates packet movement to `udpdataplane.Send`.
5. Destination handlers write incoming streams and report into a separate
   `destinationReceiver` state owner.
6. Source progress callbacks update `transferJobRuntime`; snapshots feed history
   recording. The manager determines the terminal job state.

Route sessions and relay forwards represent the network path. They do not plan
files or byte ranges. A transfer endpoint owns listeners/reverse connections,
while a destination receiver owns file reception. These are distinct objects
even when they live in the same groverd process.

## Where to edit transfer behavior

Paths in this table are relative to `pkg/transfer/`.

| Change | Start here | Owner |
| --- | --- | --- |
| Job lifecycle | `job_manager.go` | `Manager`, source job map |
| File concurrency | `scheduler.go` | Manager schedules; runtime tracks each job |
| Progress and snapshots | `runtime.go` | Per-job mutex, files and streams |
| Runtime tuning | `tuning.go` | Job settings and validation |
| Extension interfaces | `contracts.go` | Consumer-owned planner/executor contracts |
| Execution configuration | `config.go`, `execution.go` | Focused settings and per-file snapshot construction |
| File/synthetic source discovery | `source.go` | `FilesystemSourcePlanner` |
| Dispatch and local copying | `executor.go` | `ProtocolExecutor`, `LocalFileExecutor` |
| TCP sending / receiving | `tcp_sender.go`, `tcp_receiver.go` | `TCPExecutor`, destination receiver |
| UDP handshake and sending | `udp_sender.go`, `udp_protocol.go` | `UDPExecutor`, transfer wire codec |
| UDP reception | `udp_receiver.go` | Destination receiver |
| Listener TTL and reverse connections | `endpoints.go` | Endpoint registry with its own mutex/maps |
| Destination progress/finalization | `receive_progress.go` | Receiver with its own mutex/map |
| Job logs | `history.go` | Recorder owns files and serialization |
| Continuous energy log | `energy.go` | Node logger; manager supplies active job IDs |

Use the package's shorter, qualified names when developing new components:

```go
manager := transfer.NewManagerWithComponents(config, planner, executor)
defer manager.Close()
```

Implement `transfer.SourcePlanner` for discovery or `transfer.FileExecutor` for
movement. Nil components select defaults. `transfer.ProtocolExecutor` accepts
independent `Local`, `TCP`, and `UDP` executors; nil fields select defaults.
Executors use progress callbacks rather than mutating the manager's state.

`Execution` groups values explicitly rather than promoting fields through
embedding. `Job`, `Paths`, and `Target` describe the transfer; `IO`, `TCP`, and
`UDP` hold tuning snapshots. `Hooks` holds runtime dependencies, and `Progress`
holds callbacks. For example:

```go
execution := transfer.Execution{
    Job: transfer.JobIdentity{ID: "job-1", RouteID: "route-1"},
    Paths: transfer.PathConfig{SourceRoot: "/source", DestRoot: "/destination"},
    IO: transfer.IOConfig{ChunkSizeBytes: 128 * 1024},
    UDP: transfer.UDPConfig{MTU: 1400, WindowPackets: 4096},
    Progress: transfer.ProgressCallbacks{
        OnProgress: func(path string, bytesRead int) { /* record progress */ },
    },
}
```

`executionForFile` snapshots tuning under the job lock. Existing execution
configs do not change when a job is tuned; `Hooks.StreamsPerFile` deliberately
reads live parallelism for future scheduling. Executors still accept
`TransferFile(ctx, execution, plan)`, keeping cancellation separate from config.

Migration from flat execution fields is explicit: `exec.UDPPayload` becomes
`exec.UDP.MTU`, `exec.DestData` becomes `exec.Target.Data`, and `exec.OnProgress`
becomes `exec.Progress.OnProgress`. The old gserver type aliases remain, but
Go callers constructing the old flat struct literals must update their fields.
The CLI and protobuf APIs are unchanged.

Receive progress uses `receiveStream` and `receiveResult` records so job/range
identity is shared between start and completion. UDP handshake code accepts its
existing typed start packet; packet transmission accepts `packetSendConfig`.

Existing `gserver.TransferJobManager`, `TransferFilePlan`, constructors, and related
names remain compatibility aliases/wrappers in `gserver/transfer.go`. New code
should import `pkg/transfer` directly. The combined `JobExecutor` interface is
retained for compatibility; planner and executor can be supplied independently.

## Trace a UDP acknowledgment

```text
udpdataplane/sender.go: encode, send, TrackSent
  -> reliability.go: owns one stream's tracker and recovery policy
     -> ack.go: coordinate poll/progress/completion behavior
        -> status_reader.go: deadline, read, peer/stream filter, decode
        -> udpreliability.Tracker.Acknowledge: remove ACK/SACK coverage
        -> udpreliability.SACKRecovery: select retransmission indices
        -> packet_transmitter.go: send selected packets, record metrics
     -> congestion.go: consume samples, calculate in-flight target
```

`AckSeq` acknowledges outstanding sequences up to that value. SACK ranges cover
additional inclusive ranges. A hole before a higher SACK can trigger fast
retransmission; the unproven tail waits for timeout. Timeout recovery selects all
outstanding packets, with the existing retry limit enforced by `ack.go`.

`udpreliability.Tracker` is a concrete per-stream type, not a socket abstraction.
`SACKRecovery` only selects indices. The small `recoveryPolicy` interface lives
in `udpdataplane`, where it is consumed. This keeps policy tests independent of
network I/O. Borrowed packet slices have documented ownership and lifetime rules.

`PacketSender` runs both fixed and BBR-like modes. `BBRSender` and `NewBBRSender`
remain compatibility names. Use `NewPacketSender()` and `sender.Factories` to
replace reliability, congestion, flow control, or pacing. Factories construct a
fresh controller for each stream; configure them before concurrent use.

Receiver ingestion remains in `udpdataplane/receiver.go`. ACK coalescing, payload
buffering, and shared-socket feedback routing have their own concrete components
in `ack_coalescer.go`, `payload_writer.go`, and `status_demux.go`.

## Compatibility and remaining coupling

CLI commands, RPC schemas, transfer framing, and default transport policy remain
unchanged. Relays read only the opaque control envelope's routing key through
`transferwire.RoutingKey`; they do not import the transfer execution package.

The execution config now separates protocol tuning but still uses protobuf
endpoint/state types. History and energy are concrete components within
`pkg/transfer`. Further separation should follow a specific use case rather than
introducing a generic services or utilities package.

This is not a new reliability protocol. Feedback still filters by peer/stream
without adding session-key validation, preserves the existing pre-read RTT
sampling point, and retransmissions bypass the original-packet pacer. Sequence
wraparound and stronger loss/finalization policies remain transport work.

## Tests

- `pkg/transfer/components_test.go` uses the public API from `transfer_test`:
  independent planner/executor composition and protocol dispatch.
- `pkg/transfer/unit_test.go` covers source planning, path handling, tuning, framing.
- `pkg/gserver/transfer_job_manager_test.go` retains cross-component direct,
  reverse, relayed, synthetic, tuning, history, abort, and expiry tests.
- `internal/udpreliability/tracker_test.go` covers ACK/SACK accounting and recovery.
- `pkg/udpdataplane/ack_test.go`, `outstanding_packets_test.go`, and
  `sender_components_test.go` cover feedback I/O, retries, cancellation and injection.
- `internal/dataport/allocator_test.go` covers listener lease allocation/release.
- `internal/transferwire/header_test.go` covers opaque relay routing envelopes.

Run `go test ./...`. For ownership and concurrency changes, also run
`go test -race ./pkg/gserver ./pkg/transfer ./pkg/udpdataplane ./internal/dataport ./internal/udpreliability`.
Network tests require permission to bind local TCP/UDP sockets.
