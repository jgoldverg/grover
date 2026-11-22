# Grover

Grover moves files between machines over TCP or UDP, directly or through relay
nodes, and records per-job throughput and (optionally) RAPL energy for experiments.

- **`groverd`** runs on every node. It owns all data movement: listeners, relay
  forwards, transfer jobs, and job logs.
- **`grover`** is the CLI. It is control-plane only: it tells nodes what to
  prepare and which jobs to run, then shows progress. File bytes never pass
  through it.

See [docs/architecture.md](docs/architecture.md) for how the pieces fit.

## Build

Go 1.26+.

```sh
make            # bin/grover and bin/groverd
make linux      # bin/linux-amd64/* for remote nodes
make test       # unit + in-process integration tests (binds loopback sockets)
```

## Quick start (two nodes on one machine)

```sh
scripts/start-local-groverd-pair.sh            # groverd on :22444 (src) and :22445 (dst)
bin/grover profile set src --server 127.0.0.1:22444 --insecure-control
bin/grover profile set dst --server 127.0.0.1:22445 --insecure-control

bin/grover transfer src:$HOME/data/file.bin dst:/tmp/grover-dst/
bin/grover transfer --protocol udp -P 4 src:synthetic://1073741824/1g.bin dst:/tmp/grover-dst/
kill $(cat .grover-local/*.pid)
```

## Concepts

| Thing | What it is | Where it lives |
|---|---|---|
| **Profile** | A named node: control address + TLS settings. Profile names work as location prefixes (`uc:/data`). | `~/.grover/cli_config.toml` |
| **Route** | A named path: source node, relays (`--via`), destination node, protocol, and who dials (`--connect-from`). No file paths. | `routes.json` on the default node |
| **Session** | A prepared route: open data endpoints, relay forwards, and a token. Runs many jobs, one at a time. Lives until `route close` or a groverd restart. | memory of the route's source node |
| **Job** | One transfer: source paths → destination directory, with tuning. | the source node; the destination node reports its receive-side view under the same id |

Bytes always flow source → destination. `--connect-from destination` only
changes who opens the TCP connections, for nodes that can only dial out
(private/EDU networks). To move data the other way, define a route with the
endpoints swapped.

## Using it

### Nodes

```sh
grover profile set uc   --server uc.example.edu:22444 --ca-cert ~/.grover/certs/grover-ca.crt
grover profile set tacc --server tacc.example.edu:22444 --ca-cert ~/.grover/certs/grover-ca.crt
grover profile use uc   # default node for commands that don't name one
grover profile list
```

Global flags: `--profile/-p`, `--server host:port`, `--insecure` (no TLS to any
node), `--log-level debug`, `--json`.

### One-off transfers

```sh
grover transfer uc:/data/run1 tacc:/scratch/in/        # temporary direct session
grover transfer --via relay1 uc:/data/f tacc:/scratch/  # through a relay
grover transfer /data/a /data/b/                        # local copy on the default node
```

A source file lands at `<dest>/<name>`. A source directory's contents land under
`<dest>/`. Locations are `/abs/path` (default node), `node:/abs/path`,
`host:port:/abs/path`, or `[ipv6]:port:/abs/path`. A source can also be
`synthetic://<bytes>/<name>`, which generates zeros without disk reads.

### Routes and sessions

```sh
grover route put uc-tacc --source uc --destination tacc --protocol udp
grover route put edu-in  --source uc --destination edu  --connect-from destination
grover route prepare uc-tacc
grover transfer --route uc-tacc -c 8 -P 4 /data/run1 /scratch/in    # plain paths on the route's nodes
grover transfer --route uc-tacc --detach /data/run2 /scratch/in
grover route status uc-tacc     # sessions, per-hop relay counters, active job
grover route close uc-tacc
```

### Jobs

```sh
grover job list --route uc-tacc          # or --node uc
grover job watch <id> --route uc-tacc
grover job get <id> --node tacc          # destination's receive-side view
grover job get <id> --route uc-tacc --files
grover job tune <id> --route uc-tacc -c 16 -P 8    # applies to files that start afterwards
grover job abort <id> --route uc-tacc
```

### Tuning

Every transfer/tune/schedule command takes the same flags. Zero means "use the
node's default" (from groverd's config or flags).

| Flag | Meaning |
|---|---|
| `-c, --concurrency` | files in flight |
| `-P, --parallelism` | streams per file (byte ranges in parallel) |
| `--chunk-size` | read/write buffer, e.g. `4MiB` |
| `--udp-mtu` | UDP datagram size (1472 for 1500-MTU links, 8972 for jumbo) |
| `--udp-window` | max in-flight packets per stream |
| `--udp-batch` | packets per `sendmmsg`/`recvmmsg` (Linux) |
| `--udp-ack-packets`, `--udp-ack-ms` | receiver ACK cadence |
| `--udp-write-flush` | receiver write coalescing |
| `--udp-socket-read`, `--udp-socket-write` | sender socket buffers |
| `--udp-flow` | `fixed` or `bbr` |

### Schedules (GreenTransferScheduler)

```sh
grover route prepare tacc_buff
grover schedule run schedule.csv --route-key tacc_buff --destination-root /scratch/grover
grover schedule run schedule.csv --route-key tacc_buff --destination-root /scratch/grover --time-scale 0 --limit 5 --dry-run
```

Each row becomes a synthetic transfer of `allocated_bytes` over the route's
session. Rows are grouped into slots by `forecast_id × slot_duration_seconds`,
and `flow_count` sets streams per file. Output is deleted after each row unless
`--keep-data` is set.

Queued one-off transfers: `grover schedule add <route> <src>... <dst> --in 30m`,
`grover schedule list`, and `grover schedule run-pending --watch`.

## Running groverd

```sh
groverd --config /etc/grover/server.toml
groverd --insecure-control --data-advertise-host 10.0.0.5 --data-port-min 30000 --data-port-max 30199
```

Config file (all keys optional; flags override the file, the file overrides defaults):

```toml
port = 22444
insecure_control = false
server_certificate_path = "~/.grover/certs/grover-server.crt"
server_key_path = "~/.grover/certs/grover-server.key"
log_level = "info"          # debug adds file:line to every log line
log_format = "text"         # or json

data_bind_host = "0.0.0.0"
data_advertise_host = "10.0.0.5"   # the address OTHER nodes use to reach this one
data_port_min = 30000              # 0/0 = OS ephemeral ports
data_port_max = 30199

route_store_file = "~/.grover/routes.json"
job_log_dir = "/var/log/grover"    # "" disables job logs
energy_monitor = false             # true: sample RAPL; startup fails if unavailable
energy_sample_ms = 1000

[tuning]                           # node defaults for jobs started here
concurrency = 4
parallelism = 1
udp_mtu = 8972
udp_window_packets = 16384
```

Unknown keys are logged as warnings at startup, so stale configs show up.
`groverd --help` lists every flag.

TLS: `scripts/generate_grover_certs.sh -n grover -d 365 -s host1,10.0.0.5` creates
a CA and a server cert under `~/.grover/certs`. Point `--server-cert`/`--server-key`
at the server pair and give the CLI the CA with `--ca-cert`. If the certificate
cannot be loaded, groverd refuses to start rather than falling back to plaintext.

## Job logs and energy

With `job_log_dir` set, every job writes:

```
<job_log_dir>/<job_id>/         source side
<job_log_dir>/<job_id>.dest/    destination side
    manifest.json   planned files and tuning
    snapshots.jsonl one line per second
    final.json      final state with every file
    energy.csv      RAPL samples (with --energy-monitor)
<job_log_dir>/energy.csv        continuous node log (appended across restarts); idle rows have no job_id
```

Read them remotely with `grover job history list|get|snapshots|energy --node <n>`
(add `--dest` for the destination-side log).

## Debugging

- `--log-level debug` on groverd logs every RPC with its duration, plus source
  file:line. `--log-format json` gives machine-readable logs.
- `grover job get <id> --route r` shows running and failed files with their errors.
  The same id on the destination node (`--node dst`) shows what actually arrived.
- `grover route status r` shows each relay hop's byte counters, connections, and errors.
- Errors from the destination (bad path, disk full) are reported back on the data
  connection and show up in the job's error text as `destination error: ...`.
- Unreachable nodes fail in seconds with the address that failed and a hint about
  TLS mismatches.

## Development

See [AGENTS.md](AGENTS.md) for the code layout and conventions. `make proto`
regenerates `internal/pb` after editing `api/proto/grover.proto`.
