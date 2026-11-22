# Architecture

## Layers

```
grover CLI ──gRPC──> groverd (every node runs every service)
  internal/cli          internal/server      thin gRPC adapters + logging interceptor
  internal/orchestrate  internal/routes      route definitions (JSON file)
  internal/client       internal/session     prepared sessions on the source node
                        internal/relay       dumb TCP/UDP forwards
                        internal/transfer    jobs, endpoints, receive side, job logs
                        internal/udpdataplane reliable UDP streams (SACK, windows)
                        internal/wire        TCP range framing, UDP control packets
```

There are two layers, kept strictly apart:

1. **Route/session fabric.** Relays and endpoints know addresses, tokens, and
   byte counters. They never see file names.
2. **Transfer jobs.** Jobs know files, ranges, progress, and logs. They run over
   a session (or copy locally) and never create network paths themselves.

The CLI orchestrates across nodes (`internal/orchestrate`) and moves no data.

## Preparing a session

`orchestrate.Prepare` creates everything a session needs and records it on the
route's **source** node. If any step fails, the steps already done are rolled back.

**connect-from source** (the source dials):

```
1. destination.PrepareEndpoint(DESTINATION)   -> listener D + token
2. relays, last to first: CreateForward(egress = next)  -> ingress
3. source.CreateSession{data = first ingress (or D), destination token}
data:  source ──dial──> relay1 ──> relay2 ──> D
```

**connect-from destination** (the destination dials; for outbound-only nodes):

```
1. source.PrepareEndpoint(SOURCE)             -> listener S + token
2. relays, first to last: CreateForward(egress = next)  -> ingress
3. destination.PrepareEndpoint(DESTINATION, dial = last ingress, token, N)
   -> destination keeps N connections open to S, redialing when one drops
4. source.CreateSession{source endpoint}
data:  D ──dial──> relayN ──> … ──> S;  bytes then flow S -> D on those connections
```

Endpoints and forwards have no TTL. They live until `route close`, which deletes
the session record first (refused while a job is running), then the forwards,
then the endpoints. Everything is in memory, so a groverd restart drops its
sessions. Run `route prepare` again afterwards.

## Running a job

`TransferControl.StartJob` on the source node:

1. Plan sources into files (`transfer/plan.go`). Paths are validated so nothing
   can escape the destination directory.
2. Claim the session (`session.Store.Claim`). One job per session at a time.
3. Schedule files with live `concurrency` (`transfer/manager.go`). Each file
   gets a tuning snapshot and goes to an executor:
   - **local** – copy on this node.
   - **tcp** (`tcp_send.go`) – split into `parallelism` ranges. Each range is
     sent on a pooled, authenticated connection and acknowledged by the
     destination after `fsync`. Broken connections are retried on a fresh one.
     Errors the destination reports are not retried.
   - **udp** (`udp_send.go`) – START → READY handshake on a control socket,
     one `udpdataplane` stream per range, then START until DONE (the file is
     synced).
4. Release resources, then publish the terminal state. A client that sees
   DONE can immediately start the next job on the session.

`TuneJob` changes tuning: concurrency applies at once, everything else applies
to files that start afterwards.

## Data-plane wire formats (`internal/wire`)

**TCP**, per connection:

```
dialer → listener   "GRVR" v2 token[16]
listener → dialer   status byte (0 ok)
then repeatedly, source → destination:
   "RNG2" stream u32 | file_size u64 | offset u64 | length u64 |
   len(job) len(route) len(dest_dir) len(rel_path) u16 each | strings | <length bytes>
destination → source: status u8 | msg_len u16 | msg
```

The destination directory travels with each range, so one prepared session can
serve any destination path. The token gates every connection, so an
unauthenticated peer cannot write files.

**UDP.** Each destination endpoint is one socket with one dispatcher goroutine
(`udp_recv.go`). It routes datagrams by session key to per-file receivers, so
many files can be in flight. Control packets (`"GRVRUDP2" kind key …`) are
START (token, size, stream ids, receiver tuning, destination, path), READY,
DONE, and ERROR. A repeated START is answered with the session's current state,
so lost control packets only cost a resend interval. Data and status packets
are `internal/udpwire`.

Relays route UDP replies using only the session key and stream id in headers
(`wire.RoutingKey`, `udpwire`).

## UDP reliability (`internal/udpdataplane`)

- **Sender.** Packetize → batch send → track outstanding packets (`Tracker`) →
  drain status packets → retransmit (`SACKRecovery`).
  - The flow controller caps in-flight packets/bytes.
  - The congestion controller (`fixed` or BBR-like) sets the in-flight target.
  - RTT is sampled from the newest never-retransmitted packet per ACK (Karn's rule).
  - A packet is not resent again within ~2×SRTT.
  - A stream fails only after 15 s without any receiver feedback.
- **Receiver.** One `Receive` for any number of streams of a file.
  - Writes go through a coalescing writer that writes out-of-order data where it
    belongs instead of buffering without bound.
  - ACK cadence is set by `udp_ack_every_packets` / `udp_ack_every_ms`.
  - After completion: `OnComplete` (fsync + DONE), then a short linger that
    re-ACKs retransmissions.

The pieces (reliability, congestion, flow, pacing) are separate interfaces in
`sender_contracts.go`, so they can be tuned or replaced one at a time.

## Observability

- **Live.** `Job` snapshots (source and destination views), and relay `Forward`
  stats per hop. Current throughput is sampled by one ticker per node, so watching
  a job does not change its numbers.
- **Logs.** `slog` key/value lines. Every RPC is logged by an interceptor: debug
  on success, warn with code and message on error.
- **History.** Per-job directories plus a continuous node `energy.csv` (see
  README), served read-only by `HistoryControl`.

## Known limits and next steps

- UDP still loses heavily when the first window overruns the receiver (jumbo
  datagrams, large windows). It recovers, but pacing, a startup window, and
  receive-queue backpressure are the next tuning work.
- UDP with `connect-from destination` is not implemented. Routes reject it.
- Sessions are not persisted. After a groverd restart, run `route prepare` again.
- Control-plane TLS authenticates the server only. Any client that can reach the
  control port can drive the node.
