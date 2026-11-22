# Working on Grover

Read `README.md` for what Grover does and `docs/architecture.md` for how it works.
This file covers where code lives and the rules for changing it.

## Layout

```
cmd/grover, cmd/groverd     entrypoints only (flag parsing, signal handling)
api/proto/grover.proto      the API; `make proto` regenerates internal/pb
internal/
  cli/          cobra commands; thin. One file per command group.
  orchestrate/  multi-node control logic (prepare/close sessions, pick, watch)
  client/       gRPC dialing + per-command connection pool
  server/       groverd wiring, gRPC service adapters, error→code mapping
  transfer/     jobs (manager, job, plan, executors), endpoints, receive side, job logs
  relay/        dumb TCP/UDP forwards
  session/      route-session store on the source node (claim/release)
  routes/       route definitions JSON store
  history/      read-only job log reader
  udpdataplane/ reliable UDP streams
  udpwire/      UDP data/status packet format
  wire/         TCP range framing, UDP control packets, data-plane tokens
  tuning/       the one Tuning type: defaults, merge, validate, pb conversion
  config/       groverd and CLI config (TOML), node resolution
  dataport/     data port allocation
  metrics/      atomic transfer counters
  energy/       Intel RAPL reader
  logging/      slog setup
integration/    in-process multi-node tests through orchestrate
```

## Rules

- The CLI never moves file bytes. groverd owns the data plane.
- Relays and sessions never see file names, ranges, or jobs. Jobs never create
  network paths.
- Tuning is defined once in `internal/tuning`. Add a knob there, in the proto
  `Tuning` message, and in `cli/tuning_flags.go`. Nowhere else.
- No compatibility shims. All nodes run the same build; when a wire format
  changes, bump its version string in `internal/wire` and redeploy every node.
- No silent no-op flags. If a flag can't apply in some mode, return an error.
- Errors: wrap with context (`fmt.Errorf("...: %w", err)`). Components return
  sentinel errors (`ErrNotFound`, `ErrInvalid`, `ErrBusy`), and
  `server.toStatus` maps them to gRPC codes.
- Logging: `log/slog` with snake_case keys (`job_id`, `session_id`, `route_id`,
  `path`, `error`). Nothing per-packet or per-chunk.
- Hot paths (per chunk, per packet) use atomics only, no job locks.
- Use `proto.Clone` for protobuf messages, never struct copies.

## Testing

```sh
make test          # everything, including in-process multi-node integration
make test-race     # run before finishing any concurrency change
make lint          # golangci-lint (config in .golangci.yml)
```

Tests bind loopback TCP/UDP sockets. In a sandbox they may need to run
unsandboxed.

- `internal/transfer/manager_test.go` runs two Managers against each other (TCP,
  UDP, destination-origin, abort, bad tokens, destination errors).
- `integration/` starts real gRPC servers and goes through
  `orchestrate` for direct, 1-relay, and 2-relay paths over TCP and UDP, plus
  destination-origin.
- `internal/cli/cli_test.go` runs the actual command tree end to end.

To try the binaries: `scripts/start-local-groverd-pair.sh` starts two groverd on
loopback; the script prints the CLI commands to use.

## Debugging tips

- Run groverd with `--log-level debug` to get every RPC with duration and source
  locations.
- The destination node answers `GetJob` for a job id with its receive view.
  Compare it with the source view to see where bytes stopped.
- `grover route status <route>` shows per-hop relay counters and errors.
- UDP stalls: look at the job's `retransmits` and `rtt_ms`. A stream fails with
  "no feedback from receiver" after 15 s of silence, which names the oldest
  unacked sequence number.

## Open work

- UDP tuning: pacing/startup window, receive-queue backpressure, and less loss
  with jumbo datagrams and large windows.
- UDP with connect-from destination.
- Experiment summary export (CSV/JSON from `HistoryControl`), done CLI-side.
- Persisting sessions across groverd restarts, if it ever matters.

The GreenTransferScheduler sweep schedules are at
`/Users/jacobgoldverg/GreenTransferScheduler/experiments/all_10k_sweep/schedules`.
