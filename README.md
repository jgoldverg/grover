# Whats the Point of this you will probably ask?? Not sure


## The overarhcing goal of this work

There are 3 cases for file transfers:
Downloading/uploading using the client to a supported endpoint or the server here.
Moving data between two servers that you launch, just hit one with the pb command or maybe i could write some cli support for this. Overlay network
Multiple server based file transfers.

Tbh I have no idea how I can construct such a system. Look at it, client -> server or client <- server is simple more or less. Nothing crazy to say the least but when you start optimizing it heavily:
1. Parallel chunk sending per file
2. Multiple file transfers in parallel
3. Somehow get zero-copy
4. Write the UDP protocol
5. Try and use eBPF for the server with XDP
6. We need to include metrics reporting as well: network metrics that we drive up via eBPF from the server side.
7. Can we utilize multiple nic's if they are available as well to split traffic across?
Hence the expectation this is a very slow burn of a project and one where I just simply play around with and try build something that craps on onedatashare and globus.


## Major things to work on still

### cli
There are many commands to add but to summarize we should support min: list(done), mkdir, touch, rename, move, transfer, sync, stream

These are just implementing the calls to backend using cobra but are crucial for actually doing integration testing and see how the protocol works.

All are simple aside from:
move - is simply moving a directory on the local file system like the Unix command.
sync - means that as changes to a file they are reflected on the upstream server, or the opposite.
transfer - an actual data transfer. It gets complicated when you want to do a broadcast or a gather of various files from various servers. Not sure how we are gonna do this but I think its just figuring out to associate together parts of file transfer to different servers. Hence we really call these two cases scatter and gather.

stream - means that we constantly read from a file or a socket in general and push that data to a destination. This I haven't specifically thought about much but if we have a directory with a file that gets say log entries every second we want to read and push that data deleting the original.

Management commands:
- credentials: here there isnt much for the Toml format of credentials but we can for example add encryption or password or jwt access.

### backend
This is where we have protocol implementations that the client's(cli for ex) and server will use to conduct the operations. It completely encapsulates all protocol based code, we really only have the concet of readers and writers and operating on chunks of data, we can include support for stream operations as well.

#### Checkpoint persistence for transfers

To keep the readers and writers fully parallel we track progress in memory and trickle it to disk in the background:
- Use a sharded map-of-maps: `file -> worker/thread -> chunk progress` so hot files do not serialize on one lock. Each entry keeps last read/written chunk, byte counters, status, and a seq number.
- Every update just bumps the in-memory structure; a background flusher waits for ~10 updates (or a time threshold) per file, then writes a snapshot to disk. Snapshots go to a temp file and get atomically `rename`d so crashes never leave partial JSON/binary artifacts.
- While a file is active we keep its state in memory; once the transfer finishes we force one last synchronous flush, drop the in-memory entry, and archive a compact `file.done` record. A tiny append-only index answers "is this file done?" without reloading snapshots.
- If redo cost needs to be near-zero, pair the periodic snapshot with a lightweight delta journal that appends each update; on recovery we load the latest snapshot and replay the tail of the journal to reconstruct exact state.
- Startup just scans active snapshots + journal to rebuild the map. Instrument the flush queue so we can add backpressure or spin up more flush workers if disk persistence ever lags behind the hot path.

### server
This is far more complicated and writing is confusing hence I need to iron this out more.
There are two primary connections to the grover server: grpc for management operations, and then the udp session that enables uploads/downloads

## Local transfer commands

Build the binaries from the repo root:

```bash
go build -o bin/grover ./cmd/grover
go build -o bin/groverd ./cmd/groverd
```

Start a UDP groverd. This enables the UDP data plane, starts the control plane without TLS, uses jumbo MTU sizing, and increases socket buffers for high-throughput testing:

```bash
./bin/groverd \
  --port=22444 \
  --protocol=udp \
  --insecure-control \
  --log-level=warn \
  --udp-mtu=8972 \
  --udp-window-packets=65536 \
  --udp-batch-packets=64 \
  --udp-ack-every-packets=128 \
  --udp-ack-every-ms=1 \
  --udp-read-buffer=134217728 \
  --udp-write-buffer=134217728
```

Start a TCP groverd:

```bash
./bin/groverd \
  --port=22444 \
  --protocol=tcp \
  --insecure-control \
  --log-level=warn
```

Start groverd with an explicit data-plane bind/advertise address and server-allocated data-port range:

```bash
./bin/groverd \
  --port=22444 \
  --protocol=tcp \
  --insecure-control \
  --data-bind-host=0.0.0.0 \
  --data-advertise-host=192.168.1.10 \
  --data-port-min=30000 \
  --data-port-max=30100
```

Run a direct groverd-to-groverd transfer over UDP. The CLI only talks to the control plane; bytes move from the source groverd to the destination groverd:

```bash
./bin/grover \
  --insecure-control \
  transfer 192.168.1.10:22444:/home/ubuntu/data/file-1gb.bin 192.168.1.20:22444:/home/ubuntu/data/file-1gb.bin \
  --protocol=udp \
  --parallel-streams=1 \
  --ui=summary \
  --ui-interval-ms=2000
```

Run a direct groverd-to-groverd transfer over TCP:

```bash
./bin/grover \
  --insecure-control \
  transfer 192.168.1.10:22444:/home/ubuntu/data/file-1gb.bin 192.168.1.20:22444:/home/ubuntu/data/file-1gb.bin \
  --protocol=tcp \
  --parallel-streams=4 \
  --ui=summary \
  --ui-interval-ms=2000
```

Store groverd control endpoints as credentials, then use `name:/path` in transfers:

```bash
./bin/grover \
  credential add-basic \
  --name source-a \
  --url 192.168.1.10:22444

./bin/grover \
  credential add-basic \
  --name dest-b \
  --url 192.168.1.20:22444

./bin/grover \
  --insecure-control \
  transfer source-a:/home/ubuntu/data/file-1gb.bin dest-b:/home/ubuntu/data/file-1gb.bin \
  --protocol=tcp
```

Show routed transfer observability from the source groverd:

```bash
./bin/grover \
  --insecure-control \
  transfer status <transfer_id> \
  --source-server 192.168.1.10:22444

./bin/grover \
  --insecure-control \
  transfer status <transfer_id> \
  --source-server 192.168.1.10:22444 \
  --watch
```

Use `--parallel-streams=1` for single-stream UDP testing and increase it, for example `--parallel-streams=4`, when testing per-file parallel streams. Server and client protocol values should match for these direct tests.

## Route commands working now

Prepare a relay route template under `~/.grover/routes.toml`. A route template describes the reusable network path only; file paths belong on `transfer`:

```bash
./bin/grover route prepare relay-a-b \
  --via relay-a \
  --via relay-b \
  --protocol=tcp \
  --parallel-streams=4 \
  --concurrency=2
```

You can optionally store default endpoints on a route, but `transfer --route` can always override them:

```bash
./bin/grover route prepare relay-a-b \
  10.0.0.10:22444:/mnt/src/default.bin \
  10.0.0.20:22444:/mnt/dst/default.bin \
  --via 10.0.0.15:22444 \
  --protocol=tcp
```

Then run the transfer with the stored defaults:

```bash
./bin/grover --insecure-control transfer --route relay-a-b
```

Run a local-path transfer through the configured groverd. Both paths are resolved on the groverd host, not by the CLI process:

```bash
./bin/grover \
  --server-url 127.0.0.1:22444 \
  --insecure-control \
  transfer /mnt/src/file-1gb.bin /mnt/dst/file-1gb.bin \
  --protocol=tcp \
  --parallel-streams=4 \
  --concurrency=2
```

Run a direct TCP transfer from one groverd to another. The endpoint syntax is `host:port:/absolute/path`; IPv6 endpoints use brackets, for example `[::1]:22444:/tmp/file.bin`:

```bash
./bin/grover \
  --insecure-control \
  transfer 10.0.0.10:22444:/mnt/src/file-1gb.bin 10.0.0.20:22444:/mnt/dst/file-1gb.bin \
  --protocol=tcp \
  --parallel-streams=1 \
  --concurrency=1
```

Run a TCP transfer over a prepared route:

```bash
./bin/grover \
  --insecure-control \
  transfer 10.0.0.10:22444:/mnt/src/file-1gb.bin 10.0.0.20:22444:/mnt/dst/file-1gb.bin \
  --route relay-a-b \
  --protocol=tcp \
  --parallel-streams=1 \
  --concurrency=1
```

Run a one-shot TCP transfer through one or more relay groverd instances without storing a route template. Relay values are groverd control-plane addresses, and each relay must be reachable by the CLI and able to reach the next hop's advertised data endpoint:

```bash
./bin/grover \
  --insecure-control \
  transfer 10.0.0.10:22444:/mnt/src/file-1gb.bin 10.0.0.20:22444:/mnt/dst/file-1gb.bin \
  --via 10.0.0.15:22444 \
  --protocol=tcp \
  --parallel-streams=1 \
  --concurrency=1
```

List and inspect prepared routes:

```bash
./bin/grover route list
./bin/grover route status daily-upload
./bin/grover route status relay-a-b --source-server 10.0.0.10:22444 --watch
```

Abort a prepared local route template. When the involved groverd instances are reachable, this also best-effort aborts active source jobs and deletes relay forwards for the route:

```bash
./bin/grover route abort relay-a-b --source-server 10.0.0.10:22444
```

Dry-run a one-shot routed transfer plan. This prints the planned source, relay, and destination hops without moving bytes:

```bash
./bin/grover transfer 10.0.0.10:22444:/mnt/src/file.bin 10.0.0.20:22444:/mnt/dst/file.bin \
  --route relay-a-b \
  --dry-run
```

`route start` does not copy files. Use `transfer --route <name> <source> <destination>` to move bytes over a prepared route.

Credential-style paths like `source-a:/path` now resolve to groverd control endpoints from local basic credentials. New routed/direct job commands should use local paths, `host:port:/path` endpoint paths, or credential endpoint aliases.

Tune a running routed transfer job on the source groverd:

```bash
./bin/grover \
  --insecure-control \
  transfer tune <transfer_id> \
  --source-server 10.0.0.10:22444 \
  --concurrency=8 \
  --parallel-streams=4
```

Use a TOML route/job spec for dry-run planning:

```toml
source = "10.0.0.10:22444:/mnt/src/file.bin"
destination = "10.0.0.20:22444:/mnt/dst/file.bin"

[transfer]
protocol = "tcp"
parallel_streams = 4
concurrency = 2

[route]
via = ["relay-a", "relay-b"]
```

```bash
./bin/grover transfer --route-file ./route.toml --dry-run
```

Metadata commands use `--execution`, not `--via`, when forcing local or remote metadata execution:

```bash
./bin/grover backend list localfs --path ~/data --execution client
./bin/grover backend list localfs --path /mnt/data --execution server
```

## Routed transfer status

Direct groverd-local, direct two-groverd TCP/UDP, TCP/UDP relay paths, one-shot `transfer`, prepared-route `transfer --route`, route status, transfer status, and `transfer tune` are wired. Use `transfer` for direct and relay groverd job benchmarks, and use `--dry-run` for planning without moving bytes.

### network focus

Ugh man in so many ways there are tons of protocols to use with various ideas, problem is I am sick of not having exactly want. A higher performance protocol that supports chunking, striping out of order packets, that does proper monitoring of itself. Using things like ftp, scp,,, leave you completely blind of whats going in the network and making certain decisions up front.

Decisions that we can consider:
1. In the overlay network scenario we can do tons of routing decisions that I think would be fun to implement in the user space
2. No eBPF support in tons of protocol servers when tbh we just want traffic to come in as fast as possible.
3. Lets see if we can do a zero copy approach for the client and server, but that would be a fun challenge to make a protocol server and client as fast as bleeding possible.
4. There are just tons of tricks you can do in networking that many file transfer protocols simply dont support and its killing me at this point. Whats crazy is http tends up being the most performant but is still extremely lacking when looking at performance.

### Transport roadmap (near-term)

- Per-stream TX windows: keep a `txWindow` per stream on the client that tracks bytes in flight (sequence + payload length). Gate `udpSession.Write` enqueueing on that budget so we stop shoving data when the server stops ACKing. Start with a byte limit tied to `socket_buffer_size`, grow toward selective retransmits driven by SACK ranges. Currently done via BBR + Sack so its similar to TCP 
- Error propagation + retries: treat fatal TX/RX errors as first-class signals (already started by surfacing `txLoop` failures). Next, wire status packets into the window logic and add bounded retries so slow links don’t silently drop uploads.
- Portable batching interface: wrap the send/recv path behind an interface so Linux builds can use `sendmmsg`/`recvmmsg` (and later `io_uring` or zero-copy `SEND_ZC`) while macOS/BSD fall back to tight per-packet writes with `kqueue`. Same batching loop, OS-specific backend.
- Future perf tracks: once single-stream reliability sticks, explore io_uring (multishot recv, batched send) and, if needed, userspace stacks (DPDK) or kernel hooks (XDP/eBPF). Keep the design flexible enough to slot these in without rewriting the client.
