#!/usr/bin/env bash

set -euo pipefail

usage() {
	cat <<'EOF'
Usage: generate_grover_certs.sh -n NAME_PREFIX -d DAYS -s SAN[,SAN...]

Options:
  -n, --name        Prefix for the generated files (e.g. "grover")
  -d, --days        Validity period in days for both CA and server certificates
  -s, --sans        Comma-separated list of SAN entries (hostnames or IPs)
  -o, --output      Output directory (default: ~/.grover/certs)
      --force       Overwrite existing files if they already exist

This script creates a small CA (NAME_PREFIX-ca.key/.crt) and a server key/csr/cert
signed by that CA (NAME_PREFIX-server.*). Supply all hostnames/IPs the server
will be accessed with via --sans so the resulting certificate validates in TLS.
EOF
}

NAME=""
DAYS=""
SANS_RAW=""
OUT_DIR="${HOME}/.grover/certs"
FORCE=0

while [[ $# -gt 0 ]]; do
	case "$1" in
		-n|--name)
			NAME="$2"; shift 2 ;;
		-d|--days)
			DAYS="$2"; shift 2 ;;
		-s|--sans)
			SANS_RAW="$2"; shift 2 ;;
		-o|--output)
			OUT_DIR="$2"; shift 2 ;;
		--force)
			FORCE=1; shift ;;
		-h|--help)
			usage; exit 0 ;;
		*)
			echo "Unknown argument: $1" >&2
			usage; exit 1 ;;
	esac
done

if [[ -z "$NAME" || -z "$DAYS" || -z "$SANS_RAW" ]]; then
	echo "name, days, and SANs are required." >&2
	usage
	exit 1
fi

if ! [[ "$DAYS" =~ ^[0-9]+$ ]]; then
	echo "--days must be a positive integer" >&2
	exit 1
fi

IFS=',' read -r -a SAN_ITEMS <<<"$SANS_RAW"
if [[ "${#SAN_ITEMS[@]}" -eq 0 ]]; then
	echo "at least one SAN entry is required" >&2
	exit 1
fi

mkdir -p "$OUT_DIR"

CA_KEY="${OUT_DIR}/${NAME}-ca.key"
CA_CRT="${OUT_DIR}/${NAME}-ca.crt"
CA_SRL="${OUT_DIR}/${NAME}-ca.srl"
SRV_KEY="${OUT_DIR}/${NAME}-server.key"
SRV_CSR="${OUT_DIR}/${NAME}-server.csr"
SRV_CRT="${OUT_DIR}/${NAME}-server.crt"

maybe_overwrite() {
	local path="$1"
	if [[ -f "$path" && $FORCE -ne 1 ]]; then
		echo "Refusing to overwrite existing file $path (use --force to override)" >&2
		exit 1
	fi
}

maybe_overwrite "$CA_KEY"
maybe_overwrite "$CA_CRT"
maybe_overwrite "$CA_SRL"
maybe_overwrite "$SRV_KEY"
maybe_overwrite "$SRV_CSR"
maybe_overwrite "$SRV_CRT"

echo "Generating CA key: $CA_KEY"
openssl genrsa -out "$CA_KEY" 4096 >/dev/null 2>&1

echo "Generating CA certificate: $CA_CRT"
openssl req -x509 -new -nodes -key "$CA_KEY" -sha256 -days "$DAYS" \
	-subj "/CN=${NAME}-ca" -out "$CA_CRT" >/dev/null 2>&1

echo "Generating server key: $SRV_KEY"
openssl genrsa -out "$SRV_KEY" 4096 >/dev/null 2>&1

SAN_CONFIG="$(mktemp)"
trap 'rm -f "$SAN_CONFIG"' EXIT

SAN_LIST=()
for entry in "${SAN_ITEMS[@]}"; do
	trimmed="$(echo "$entry" | xargs)"
	if [[ -z "$trimmed" ]]; then
		continue
	fi
	if [[ "$trimmed" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
		SAN_LIST+=("IP:${trimmed}")
	elif [[ "$trimmed" =~ ^\[?[0-9a-fA-F:]+\]?$ ]]; then
		SAN_LIST+=("IP:${trimmed#[}")
		SAN_LIST[-1]="${SAN_LIST[-1]%]}"
	else
		SAN_LIST+=("DNS:${trimmed}")
	fi
done

if [[ "${#SAN_LIST[@]}" -eq 0 ]]; then
	echo "Failed to parse any SAN entries" >&2
	exit 1
fi

{
	echo "[req]"
	echo "distinguished_name = req"
	echo "req_extensions = req_ext"
	echo "prompt = no"
	echo "[req_ext]"
	echo "subjectAltName = $(IFS=','; echo "${SAN_LIST[*]}")"
} >"$SAN_CONFIG"

echo "Generating server CSR: $SRV_CSR"
openssl req -new -key "$SRV_KEY" -out "$SRV_CSR" \
	-subj "/CN=${NAME}-server" -config "$SAN_CONFIG" >/dev/null 2>&1

echo "Signing server certificate: $SRV_CRT"
openssl x509 -req -in "$SRV_CSR" -CA "$CA_CRT" -CAkey "$CA_KEY" \
	-CAcreateserial -CAserial "$CA_SRL" -out "$SRV_CRT" -days "$DAYS" \
	-sha256 -extensions req_ext -extfile "$SAN_CONFIG" >/dev/null 2>&1

echo "Certificates created in $OUT_DIR:"
printf "  CA key:        %s\n" "$CA_KEY"
printf "  CA cert:       %s\n" "$CA_CRT"
printf "  Server key:    %s\n" "$SRV_KEY"
printf "  Server CSR:    %s\n" "$SRV_CSR"
printf "  Server cert:   %s\n" "$SRV_CRT"
