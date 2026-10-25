#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${DATA_DIR:-$HOME/data}"
SRC_DIR="${SRC_DIR:-$DATA_DIR/grover-src}"
DST_DIR="${DST_DIR:-$DATA_DIR/grover-dst}"

mkdir -p "$SRC_DIR" "$DST_DIR"

create_file() {
  local path="$1"
  local bytes="$2"

  if [[ -f "$path" ]] && [[ "$(wc -c < "$path" | tr -d ' ')" == "$bytes" ]]; then
    printf 'exists  %s (%s bytes)\n' "$path" "$bytes"
    return
  fi

  printf 'create  %s (%s bytes)\n' "$path" "$bytes"
  : > "$path"
  dd if=/dev/zero of="$path" bs=1048576 count=$((bytes / 1048576)) status=none
  local remainder=$((bytes % 1048576))
  if (( remainder > 0 )); then
    dd if=/dev/zero of="$path" bs=1 count="$remainder" seek=$((bytes - remainder)) status=none
  fi
}

create_file "$SRC_DIR/file-1k.bin" 1024
create_file "$SRC_DIR/file-64k.bin" 65536
create_file "$SRC_DIR/file-1m.bin" 1048576
create_file "$SRC_DIR/file-16m.bin" 16777216
create_file "$SRC_DIR/file-128m.bin" 134217728

if [[ "${CREATE_1G:-0}" == "1" ]]; then
  create_file "$SRC_DIR/file-1g.bin" 1073741824
fi

printf '\nsource:      %s\n' "$SRC_DIR"
printf 'destination: %s\n' "$DST_DIR"
printf 'set CREATE_1G=1 to also create file-1g.bin\n'
