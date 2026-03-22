#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="${TOOLS_DIR:-$HOME/.grover-tools}"
BIN_DIR="$TOOLS_DIR/bin"
GO_VERSION="${GO_VERSION:-1.25.0}"
PROTOC_VERSION="${PROTOC_VERSION:-27.2}"
PROTOC_GEN_GO_VERSION="${PROTOC_GEN_GO_VERSION:-v1.36.1}"
PROTOC_GEN_GO_GRPC_VERSION="${PROTOC_GEN_GO_GRPC_VERSION:-v1.5.1}"

mkdir -p "$BIN_DIR"
export PATH="$BIN_DIR:$PATH"

log() {
	echo "[grover] $*"
}

fail() {
	echo "[grover][error] $*" >&2
	exit 1
}

require_tool() {
	if ! command -v "$1" >/dev/null 2>&1; then
		fail "missing required tool: $1"
	fi
}

require_tool curl
require_tool tar
require_tool unzip

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$OS" in
darwin) GO_OS="darwin" ;;
linux) GO_OS="linux" ;;
*) fail "unsupported OS: $OS" ;;
esac

case "$ARCH" in
x86_64 | amd64)
	GO_ARCH="amd64"
	PROTOC_PKG_OS="$OS-x86_64"
	;;
arm64 | aarch64)
	GO_ARCH="arm64"
	if [[ "$OS" == "linux" ]]; then
		PROTOC_PKG_OS="linux-aarch_64"
	else
		PROTOC_PKG_OS="osx-aarch_64"
	fi
	;;
*)
	fail "unsupported architecture: $ARCH"
	;;
esac

if [[ "$OS" == "darwin" && "$ARCH" == "x86_64" ]]; then
	PROTOC_PKG_OS="osx-x86_64"
elif [[ "$OS" == "darwin" && "$ARCH" == "arm64" ]]; then
	PROTOC_PKG_OS="osx-aarch_64"
fi

GO_INSTALL_DIR="$TOOLS_DIR/go${GO_VERSION}"
PROTOC_INSTALL_DIR="$TOOLS_DIR/protoc-${PROTOC_VERSION}"
GO_CMD=""

install_go() {
	if command -v go >/dev/null 2>&1; then
		local current
		current="$(go version 2>/dev/null | awk '{print $3}')"
		if [[ "$current" == "go${GO_VERSION}" ]]; then
			GO_CMD="$(command -v go)"
			log "found go ${GO_VERSION} at ${GO_CMD}"
			return
		fi
	fi

	log "installing go ${GO_VERSION} to ${GO_INSTALL_DIR}"
	local archive="go${GO_VERSION}.${GO_OS}-${GO_ARCH}.tar.gz"
	local url="https://go.dev/dl/${archive}"
	local tmpdir
	tmpdir="$(mktemp -d)"
	trap 'rm -rf "$tmpdir"' EXIT
	curl -fsSL "$url" -o "${tmpdir}/${archive}"
	tar -C "$tmpdir" -xzf "${tmpdir}/${archive}"
	rm -rf "$GO_INSTALL_DIR"
	mv "${tmpdir}/go" "$GO_INSTALL_DIR"
	ln -sf "$GO_INSTALL_DIR/bin/go" "$BIN_DIR/go"
	ln -sf "$GO_INSTALL_DIR/bin/gofmt" "$BIN_DIR/gofmt"
	GO_CMD="$BIN_DIR/go"
	log "go installed at ${GO_CMD}"
	rm -rf "$tmpdir"
	trap - EXIT
}

install_protoc() {
	if command -v protoc >/dev/null 2>&1; then
		local current
		current="$(protoc --version 2>/dev/null | awk '{print $2}')"
		if [[ "$current" == "$PROTOC_VERSION" ]]; then
			log "found protoc ${PROTOC_VERSION} at $(command -v protoc)"
			return
		fi
	fi

	log "installing protoc ${PROTOC_VERSION} to ${PROTOC_INSTALL_DIR}"
	local zip="protoc-${PROTOC_VERSION}-${PROTOC_PKG_OS}.zip"
	local url="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${zip}"
	local tmpdir
	tmpdir="$(mktemp -d)"
	trap 'rm -rf "$tmpdir"' EXIT
	curl -fsSL "$url" -o "${tmpdir}/${zip}"
	rm -rf "$PROTOC_INSTALL_DIR"
	unzip -q "${tmpdir}/${zip}" -d "$PROTOC_INSTALL_DIR"
	ln -sf "$PROTOC_INSTALL_DIR/bin/protoc" "$BIN_DIR/protoc"
	log "protoc installed at ${BIN_DIR}/protoc"
	rm -rf "$tmpdir"
	trap - EXIT
}

install_protoc_plugins() {
	if [[ -z "$GO_CMD" ]]; then
		GO_CMD="$(command -v go)" || fail "go binary not found"
	fi
	log "installing protoc plugins to ${BIN_DIR}"
	GOBIN="$BIN_DIR" "$GO_CMD" install "google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}"
	GOBIN="$BIN_DIR" "$GO_CMD" install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}"
}

build_project() {
	log "building grover binaries"
	( cd "$ROOT_DIR" && PATH="$BIN_DIR:$PATH" make all )
	log "build complete; binaries are in ${ROOT_DIR}/bin"
}

install_go
install_protoc
install_protoc_plugins
build_project

log "done. add ${BIN_DIR} to your PATH to use the installed tools."
