#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"
STATE_DIR="${STATE_DIR:-$ROOT_DIR/.grover-local}"
PROTOCOL="${1:-${PROTOCOL:-udp}}"
ROLE="${ROLE:-both}"

SOURCE_PORT="${SOURCE_PORT:-22444}"
DEST_PORT="${DEST_PORT:-22445}"
SOURCE_DATA_MIN="${SOURCE_DATA_MIN:-30000}"
SOURCE_DATA_MAX="${SOURCE_DATA_MAX:-30099}"
DEST_DATA_MIN="${DEST_DATA_MIN:-30100}"
DEST_DATA_MAX="${DEST_DATA_MAX:-30199}"
DATA_ADVERTISE_HOST="${DATA_ADVERTISE_HOST:-127.0.0.1}"
DATA_BIND_HOST="${DATA_BIND_HOST:-127.0.0.1}"

case "$PROTOCOL" in
  tcp|udp) ;;
  *) echo "protocol must be tcp or udp" >&2; exit 2 ;;
esac
case "$ROLE" in
  source|dest|both) ;;
  *) echo "ROLE must be source, dest, or both" >&2; exit 2 ;;
esac

mkdir -p "$BIN_DIR" "$STATE_DIR"

if [[ ! -x "$BIN_DIR/groverd" ]]; then
  (cd "$ROOT_DIR" && go build -o "$BIN_DIR/groverd" ./cmd/groverd)
fi
if [[ ! -x "$BIN_DIR/grover" ]]; then
  (cd "$ROOT_DIR" && go build -o "$BIN_DIR/grover" ./cmd/grover)
fi

start_one() {
  local name="$1"
  local port="$2"
  local data_min="$3"
  local data_max="$4"
  local cred_file="$STATE_DIR/$name-credentials.toml"
  local log_file="$STATE_DIR/$name.log"
  local pid_file="$STATE_DIR/$name.pid"

  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    printf '%s already running pid=%s\n' "$name" "$(cat "$pid_file")"
    return
  fi

  printf '[credentials]\n' > "$cred_file"

  "$BIN_DIR/groverd" \
    --port="$port" \
    --protocol="$PROTOCOL" \
    --insecure-control \
    --log-level=warn \
    --data-bind-host="$DATA_BIND_HOST" \
    --data-advertise-host="$DATA_ADVERTISE_HOST" \
    --data-port-min="$data_min" \
    --data-port-max="$data_max" \
    --credentials-file="$cred_file" \
    --udp-mtu="${UDP_MTU:-8972}" \
    --udp-window-packets="${UDP_WINDOW_PACKETS:-65536}" \
    --udp-batch-packets="${UDP_BATCH_PACKETS:-64}" \
    --udp-ack-every-packets="${UDP_ACK_EVERY_PACKETS:-128}" \
    --udp-ack-every-ms="${UDP_ACK_EVERY_MS:-1}" \
    --udp-read-buffer="${UDP_READ_BUFFER:-134217728}" \
    --udp-write-buffer="${UDP_WRITE_BUFFER:-134217728}" \
    > "$log_file" 2>&1 &

  echo "$!" > "$pid_file"
  printf '%s pid=%s control=127.0.0.1:%s data=%s:%s-%s log=%s\n' \
    "$name" "$(cat "$pid_file")" "$port" "$DATA_ADVERTISE_HOST" "$data_min" "$data_max" "$log_file"
}

if [[ "$ROLE" == "source" || "$ROLE" == "both" ]]; then
  start_one source "$SOURCE_PORT" "$SOURCE_DATA_MIN" "$SOURCE_DATA_MAX"
fi
if [[ "$ROLE" == "dest" || "$ROLE" == "both" ]]; then
  start_one dest "$DEST_PORT" "$DEST_DATA_MIN" "$DEST_DATA_MAX"
fi

cat <<EOF

Stop them with:
  kill \$(cat "$STATE_DIR"/*.pid)

Local test endpoints:
  source: 127.0.0.1:$SOURCE_PORT:$HOME/data/grover-src/file-128m.bin
  dest:   127.0.0.1:$DEST_PORT:$HOME/data/grover-dst/file-128m.bin
EOF
