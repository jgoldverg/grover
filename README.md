# Grover

Grover is an experimental groverd-owned file transfer system. The CLI is the control plane: it resolves endpoints, prepares routes, starts jobs, and prints observability. File bytes move between `groverd` processes, not through the CLI process.

The current focus is benchmarking direct and relay transfers across real nodes, including throughput, per-stream progress, UDP protocol counters, historical job logs, and optional Intel RAPL energy capture.

## Build

Use `make all` from the repo root. It builds both binaries into `./bin`.

```bash
make all
```

Run tests with:

```bash
make test
```

`make clean` only removes `bin/grover` and `bin/groverd`.

## Start Groverd

Use one `groverd` for each node that can own files, receive data, or act as a relay. Data ports are allocated by the server from the configured range.

### Source Node

Use this on the node that has source files. The control plane listens on `22444`; data sockets bind on all interfaces and advertise the 100G-facing address.

```bash
./bin/groverd \
  --port=22444 \
  --protocol=tcp \
  --insecure-control \
  --log-level=info \
  --data-bind-host=0.0.0.0 \
  --data-advertise-host=10.137.1.2 \
  --data-port-min=30000 \
  --data-port-max=30099 \
  --job-log-dir=/var/log/grover \
  --credentials-file=$HOME/.grover/source-credentials.toml
```

Reason: this exposes the source file tree to routed transfer jobs and gives the destination or relay a stable data endpoint to connect to.

### Destination Node

Use a different data port range on the destination so concurrent local testing does not collide.

```bash
./bin/groverd \
  --port=22444 \
  --protocol=tcp \
  --insecure-control \
  --log-level=info \
  --data-bind-host=0.0.0.0 \
  --data-advertise-host=10.137.132.2 \
  --data-port-min=30100 \
  --data-port-max=30199 \
  --job-log-dir=/var/log/grover \
  --credentials-file=$HOME/.grover/dest-credentials.toml
```

Reason: destination groverd allocates receive endpoints and writes the incoming files to its local filesystem.

### UDP With BBR-Style Flow Control

Use UDP when testing Grover's userspace transport behavior. On jumbo-MTU links, set the interface MTU first and then use a payload below the link MTU.

```bash
sudo ip link set dev enp6s0 mtu 9000
```

```bash
./bin/groverd \
  --port=22444 \
  --protocol=udp \
  --udp-flow-control=bbr \
  --insecure-control \
  --log-level=info \
  --data-bind-host=0.0.0.0 \
  --data-advertise-host=10.137.1.2 \
  --data-port-min=30000 \
  --data-port-max=30099 \
  --udp-mtu=8972 \
  --udp-window-packets=65536 \
  --udp-batch-packets=64 \
  --udp-ack-every-packets=128 \
  --udp-ack-every-ms=1 \
  --udp-read-buffer=134217728 \
  --udp-write-buffer=134217728 \
  --job-log-dir=/var/log/grover
```

Reason: this enables the experimental UDP path, ACK/SACK-driven counters, BBR-like flow control, and larger socket buffers for high-throughput experiments.

### Energy Capture

Enable energy monitoring only on bare metal nodes with readable Intel RAPL counters.

`bin/install_deps.sh` configures Linux RAPL read access when `/sys/class/powercap/intel-rapl*/energy_uj` exists. It creates/adds the install user to group `rapl`, applies read permissions to current counters, and installs `/etc/tmpfiles.d/grover-rapl.conf` when `systemd-tmpfiles` is available. Disable that setup with `GROVER_SETUP_RAPL=0 bin/install_deps.sh`.

```bash
./bin/groverd \
  --port=22444 \
  --protocol=tcp \
  --insecure-control \
  --data-bind-host=0.0.0.0 \
  --data-advertise-host=10.137.1.2 \
  --job-log-dir=/var/log/grover \
  --energy-monitor \
  --energy-sample-ms=1000
```

Reason: when enabled, groverd samples RAPL continuously and writes `/var/log/grover/energy.csv`. Baseline rows have empty `job_id`/`route_id`; rows sampled while transfers run include the active job and route IDs. Per-job folders still include `energy.csv` for compatibility, but the root CSV is the better source for baseline-versus-job analysis. Startup fails if RAPL is unavailable so experiments do not silently miss energy data.

## Direct Transfers

Run these from any machine that can reach both groverd control-plane addresses.

### Direct TCP

```bash
./bin/grover --insecure-control \
  transfer 10.137.1.2:22444:/home/ubuntu/data/grover-src/file-20g.bin \
           10.137.132.2:22444:/home/ubuntu/data/grover-dst/file-20g.bin \
  --protocol=tcp \
  --parallelism-per-file=6 \
  --concurrency=1 \
  --ui=live \
  --ui-interval-ms=1000
```

Reason: this benchmarks a direct source-groverd to destination-groverd TCP transfer. `--parallelism-per-file` splits a large file into concurrent byte ranges; `--concurrency` controls files in flight.

### Direct UDP

```bash
./bin/grover --insecure-control \
  transfer 10.137.1.2:22444:/home/ubuntu/data/grover-src/file-20g.bin \
           10.137.132.2:22444:/home/ubuntu/data/grover-dst/file-20g.bin \
  --protocol=udp \
  --parallelism-per-file=4 \
  --concurrency=1 \
  --ui=live \
  --ui-interval-ms=1000
```

Reason: this exercises Grover's UDP data plane and exposes protocol counters such as packets, retransmits, drops, and RTT when available.

### Directory Transfer

```bash
./bin/grover --insecure-control \
  transfer 10.137.1.2:22444:/home/ubuntu/data/grover-src/ \
           10.137.132.2:22444:/home/ubuntu/data/grover-dst/ \
  --protocol=tcp \
  --parallelism-per-file=4 \
  --concurrency=3 \
  --ui=live
```

Reason: this transfers every file under the source directory and shows file-level plus stream-level progress.

### Local Two-Server Test

Start two local groverd instances:

```bash
./bin/groverd \
  --port=22444 \
  --protocol=tcp \
  --insecure-control \
  --log-level=debug \
  --data-bind-host=127.0.0.1 \
  --data-advertise-host=127.0.0.1 \
  --data-port-min=30000 \
  --data-port-max=30099
```

```bash
./bin/groverd \
  --port=22445 \
  --protocol=tcp \
  --insecure-control \
  --log-level=debug \
  --data-bind-host=127.0.0.1 \
  --data-advertise-host=127.0.0.1 \
  --data-port-min=30100 \
  --data-port-max=30199
```

Then transfer local test data:

```bash
./bin/grover --insecure-control \
  transfer 127.0.0.1:22444:$HOME/testData/src/ \
           127.0.0.1:22445:$HOME/testData/dst/ \
  --protocol=tcp \
  --parallelism-per-file=4 \
  --concurrency=2 \
  --ui=live
```

Reason: this verifies the groverd-owned path without needing multiple hosts.

## Credentials

Store groverd control endpoints as credential aliases when you do not want to type addresses.

```bash
./bin/grover credential add-basic \
  --name uc \
  --url 10.137.1.2:22444

./bin/grover credential add-basic \
  --name edu \
  --url 10.137.132.2:22444
```

Then use `name:/absolute/path`:

```bash
./bin/grover --insecure-control \
  transfer uc:/home/ubuntu/data/grover-src/file-20g.bin \
           edu:/home/ubuntu/data/grover-dst/file-20g.bin \
  --protocol=tcp \
  --parallelism-per-file=6 \
  --ui=live
```

Reason: this matches the rclone-style endpoint pattern while still resolving to groverd control addresses.

## CLI Profiles

Profiles store the groverd control endpoint and control-plane TLS mode you use for CLI API calls. Use them instead of repeating `--server-url` and `--insecure-control`.

Create profiles:

```bash
./bin/grover profile set tacc \
  --server-url 129.114.108.86:22444 \
  --insecure-control

./bin/grover profile set uc \
  --server-url 192.5.86.187:22444 \
  --insecure-control
```

Pick the default profile:

```bash
./bin/grover profile use tacc
```

Inspect profiles:

```bash
./bin/grover profile list
./bin/grover profile show tacc
```

Use a profile for one command without changing the default:

```bash
./bin/grover --profile uc route list
```

Reason: profiles are control-plane connection settings. Routes still describe the network path between groverd nodes.

## Jobs And Tuning

`job` commands query the selected groverd profile/server. Use them for live jobs and server-side history instead of scraping terminal output.

List live jobs:

```bash
./bin/grover --profile tacc job list
./bin/grover --profile tacc job list --route tacc-uc
```

Inspect one live job:

```bash
./bin/grover --profile tacc job get <job_id>
```

Tune a running job:

```bash
./bin/grover --profile tacc job tune <job_id> \
  --concurrency=2 \
  --parallelism-per-file=6 \
  --chunk-size=8MiB \
  --tcp-buffer=8MiB
```

Reason: `--concurrency` controls files in flight, `--parallelism-per-file` controls byte-range streams per file, and `--chunk-size` controls the read/write batch per worker. Runtime changes affect future scheduling and new chunks/ranges; completed work is not rewritten.

## Routes And Relays

Routes are named network paths stored on `groverd` as JSON. They are not file jobs. A route stores source/destination control endpoints, optional relay hops, protocol, which side opens the data connection, and which way bytes flow. File paths are supplied when a transfer runs.

Start the source-side groverd with an explicit route JSON file:

```bash
./bin/groverd \
  --port=22444 \
  --protocol=tcp \
  --insecure-control \
  --data-bind-host=0.0.0.0 \
  --data-advertise-host=10.137.1.2 \
  --data-port-min=30000 \
  --data-port-max=30099 \
  --route-store-file=$HOME/.grover/routes.json
```

Store a direct route on that server:

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control \
  route put uc-to-edu \
  --source 10.137.1.2:22444 \
  --destination 10.137.132.2:22444 \
  --protocol=tcp
```

For an EDU/private destination that can make outbound connections but cannot accept inbound data connections, configure the route with destination-origin TCP:

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control \
  route put uc-to-edu-pull \
  --source 10.137.1.2:22444 \
  --destination 10.137.132.2:22444 \
  --protocol=tcp \
  --connect-from=destination \
  --flow=forward
```

Prepare the route session, then run the transfer over that prepared session:

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control \
  route prepare uc-to-edu-pull --session-id edu-pull-001
```

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control \
  transfer --route uc-to-edu-pull \
  /home/ubuntu/data/grover-src/file-20g.bin \
  /home/ubuntu/data/grover-dst/file-20g.bin \
  --session-id edu-pull-001 \
  --parallelism-per-file=6 \
  --ui=live
```

Reason: route preparation opens the sockets first. The transfer then only supplies file paths and stream/concurrency settings. For destination-origin TCP, the destination groverd dials out to the prepared source endpoint and the source sends bytes over those destination-originated TCP connections.

Store a one-relay route:

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control \
  route put uc-to-edu-via-tacc \
  --source 10.137.1.2:22444 \
  --destination 10.137.132.2:22444 \
  --via 10.133.3.2:22444 \
  --protocol=tcp
```

Prepare and run a transfer over that route:

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control \
  route prepare uc-to-edu-via-tacc --session-id relay-test-001
```

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control \
  transfer --route uc-to-edu-via-tacc \
  /home/ubuntu/data/grover-src/file-20g.bin \
  /home/ubuntu/data/grover-dst/file-20g.bin \
  --session-id relay-test-001 \
  --parallelism-per-file=6 \
  --ui=live
```

Reason: the route supplies the node path; `route prepare` materializes sockets and relay forwards; `transfer --route` uses the prepared session.

Inspect route state:

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control route list
./bin/grover --server-url 10.137.1.2:22444 --insecure-control route get uc-to-edu-via-tacc --json
./bin/grover route status uc-to-edu-via-tacc --source-server 10.137.1.2:22444 --watch
```

Reason: `route list` prints a table of configured routes. `route status` shows the source-owned route session, transfer job state, and relay forwards for the route.

Abort route runtime resources:

```bash
./bin/grover route abort uc-to-edu-via-tacc --source-server 10.137.1.2:22444
```

Close prepared route sessions when you are done:

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control \
  route close uc-to-edu-via-tacc --session-id relay-test-001
```

`route start` intentionally does not copy files. Use `route prepare <name>` to open the data path, then `transfer --route <name> --session-id <id> <source-path> <destination-path>` to move bytes over it.

## Observability

The live UI follows rclone-style progress with Grover network metrics attached.

```bash
./bin/grover --insecure-control \
  transfer status <transfer_id> \
  --source-server 10.137.1.2:22444 \
  --watch
```

Important fields:

- `Transferred`: total good bytes, current rate (`now`), average rate (`avg`), ETA, and trend.
- `Transferring`: active files and per-stream byte-range progress.
- `Grover network`: packets, retransmits, drops, errors, RTT when available, and good/wire byte efficiency.
- `Destination`: expected bytes sent toward the destination. Destination-side metrics are still being expanded.

Historical logs are written by groverd when `--job-log-dir` is set:

```bash
sudo tail -f /var/log/grover/energy.csv
sudo ls /var/log/grover/<job_id>
sudo cat /var/log/grover/<job_id>/manifest.json
sudo tail -f /var/log/grover/<job_id>/snapshots.jsonl
sudo cat /var/log/grover/<job_id>/final.json
sudo cat /var/log/grover/<job_id>/energy.csv
```

Query historical job logs through the groverd API:

```bash
./bin/grover --profile tacc job history list --route tacc-uc --limit=20
./bin/grover --profile tacc job history get <job_id>
./bin/grover --profile tacc job history snapshots <job_id> --limit=10
./bin/grover --profile tacc job history energy <job_id>
```

Reason: CLI output is for live operation; `/var/log/grover/<job_id>` is for reproducible experiment records. The job history API lets scripts query those records remotely without SSHing into the node.

## Schedule Execution

The schedule runner executes GreenTransferScheduler CSV rows as synthetic Grover transfers. It sends zero bytes of the requested size, so the benchmark measures network and groverd behavior without requiring real files for each row.

Configure a route whose name matches the schedule route key:

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control \
  route put tacc_buff \
  --source 10.137.1.2:22444 \
  --destination 10.137.132.2:22444 \
  --protocol=tcp
```

Dry-run a few rows:

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control \
  schedule run \
  /Users/jacobgoldverg/GreenTransferScheduler/experiments/all_10k_sweep/schedules/<schedule>.csv \
  --route-key=tacc_buff \
  --destination-root=/home/ubuntu/data/grover-dst/schedule \
  --limit=3 \
  --dry-run
```

Execute rows:

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control \
  schedule run \
  /Users/jacobgoldverg/GreenTransferScheduler/experiments/all_10k_sweep/schedules/<schedule>.csv \
  --route-key=tacc_buff \
  --destination-root=/home/ubuntu/data/grover-dst/schedule \
  --limit=10 \
  --protocol=tcp \
  --parallelism-per-file=6 \
  --concurrency=1 \
  --ui=summary
```

Reason: this connects GreenTransferScheduler allocations to real Grover transfer jobs and lets groverd collect throughput and energy per scheduled job.

Queue one future transfer in a local JSON schedule store:

```bash
./bin/grover schedule add uc-to-edu \
  /home/ubuntu/data/grover-src/file-20g.bin \
  /home/ubuntu/data/grover-dst/file-20g.bin \
  --at 2026-06-01T22:00:00Z \
  --parallelism-per-file=6
```

Run due queued transfers once, or keep polling:

```bash
./bin/grover --server-url 10.137.1.2:22444 --insecure-control schedule run-pending
./bin/grover --server-url 10.137.1.2:22444 --insecure-control schedule run-pending --watch
```

Inspect queued/history state with `jq`:

```bash
./bin/grover schedule list --json | jq '.entries[] | {id, route, state, run_at, transfer_job_id}'
```

Inspect local groverd job logs:

```bash
./bin/grover transfer history --job-log-dir=/var/log/grover
./bin/grover transfer history <job_id> --job-log-dir=/var/log/grover --json | jq .
```

Inspect server-side job history through the API:

```bash
./bin/grover --profile tacc job history list --route tacc_buff --limit=50 --json | jq .
./bin/grover --profile tacc job history get <job_id> --json | jq .
```

## Generate Test Files

Create a sparse 20 GiB test file quickly:

```bash
mkdir -p ~/data/grover-src ~/data/grover-dst
truncate -s 20G ~/data/grover-src/file-20g.bin
```

Create real allocated bytes when disk behavior matters:

```bash
dd if=/dev/zero of=~/data/grover-src/file-20g.bin bs=1M count=20480 status=progress
```

Reason: sparse files are useful for network-only tests; real files are better when measuring disk read/write throughput.

## Current Architecture Direction

The new model is two layers:

- Route/session fabric: groverd nodes create TCP/UDP connections and relay forwards according to CLI orchestration. This layer should know endpoints, direction, connection origin, hops, and per-hop metrics, but not file names or chunk semantics.
- Transfer jobs: jobs run over a materialized route/session. They know files or synthetic byte payloads, streams per file, files in flight, progress, and energy.

This split matters for EDU/private-network nodes: the CLI should be able to tell nodes who must initiate connections, then run jobs over the established route regardless of which side can accept inbound traffic.
