#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="${TOOLS_DIR:-$HOME/.grover-tools}"
BIN_DIR="$TOOLS_DIR/bin"
GO_VERSION="${GO_VERSION:-1.25.0}"
PROTOC_VERSION="${PROTOC_VERSION:-27.2}"
PROTOC_GEN_GO_VERSION="${PROTOC_GEN_GO_VERSION:-v1.36.1}"
PROTOC_GEN_GO_GRPC_VERSION="${PROTOC_GEN_GO_GRPC_VERSION:-v1.5.1}"
GROVER_CONTROL_PORT="${GROVER_CONTROL_PORT:-22444}"
GROVER_DATA_PORT_MIN="${GROVER_DATA_PORT_MIN:-30000}"
GROVER_DATA_PORT_MAX="${GROVER_DATA_PORT_MAX:-30199}"
GROVER_DISABLE_RP_FILTER="${GROVER_DISABLE_RP_FILTER:-1}"
GROVER_CPU_BENCHMARK_TIMEOUT="${GROVER_CPU_BENCHMARK_TIMEOUT:-60s}"

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

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

install_system_deps() {
  case "$OS" in
  linux)
    if command -v apt-get >/dev/null 2>&1; then
      log "installing system dependencies with apt"
      if ! sudo apt-get update; then
        log "apt update failed; clearing package lists and retrying"
        sudo apt-get clean
        sudo rm -rf /var/lib/apt/lists/*
        sudo apt-get update
      fi
      sudo apt-get install -y ca-certificates curl tar unzip make
      sudo apt-get install -y stress-ng cpufrequtils linux-tools-common "linux-tools-$(uname -r)" 2>/dev/null ||
        sudo apt-get install -y stress-ng cpufrequtils 2>/dev/null ||
        log "optional CPU benchmark/governor tools could not be installed"
      return
    fi
    ;;
  darwin)
    if command -v brew >/dev/null 2>&1; then
      log "installing system dependencies with brew"
      brew install curl unzip make || true
      return
    fi
    ;;
  esac
}

install_system_deps
require_tool curl
require_tool tar
require_tool unzip
require_tool make

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

sync_vendor() {
  log "refreshing vendored Go dependencies"
  (cd "$ROOT_DIR" && PATH="$BIN_DIR:$PATH" "$GO_CMD" mod vendor)
}

build_project() {
  log "building grover binaries"
  (cd "$ROOT_DIR" && PATH="$BIN_DIR:$PATH" make all)
  log "build complete; binaries are in ${ROOT_DIR}/bin"
}

setup_firewall_linux_ufw() {
  if ! command -v ufw >/dev/null 2>&1; then
    return 1
  fi
  local status
  status="$(sudo ufw status 2>/dev/null | head -n 1 || true)"
  if [[ "$status" != "Status: active" ]]; then
    return 1
  fi
  log "configuring active ufw firewall for Grover ports"
  sudo ufw allow "${GROVER_CONTROL_PORT}/tcp" comment "grover control" >/dev/null || true
  sudo ufw allow "${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX}/tcp" comment "grover data tcp" >/dev/null || true
  sudo ufw allow "${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX}/udp" comment "grover data udp" >/dev/null || true
  return 0
}

setup_firewall_linux_firewalld() {
  if ! command -v firewall-cmd >/dev/null 2>&1; then
    return 1
  fi
  if ! sudo firewall-cmd --state >/dev/null 2>&1; then
    return 1
  fi
  local zone
  zone="${GROVER_FIREWALLD_ZONE:-$(sudo firewall-cmd --get-default-zone 2>/dev/null || true)}"
  if [[ -z "$zone" ]]; then
    zone="public"
  fi
  log "configuring active firewalld zone ${zone} for Grover ports"
  sudo firewall-cmd --zone="$zone" --add-port="${GROVER_CONTROL_PORT}/tcp" >/dev/null || true
  sudo firewall-cmd --zone="$zone" --add-port="${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}/tcp" >/dev/null || true
  sudo firewall-cmd --zone="$zone" --add-port="${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}/udp" >/dev/null || true
  sudo firewall-cmd --permanent --zone="$zone" --add-port="${GROVER_CONTROL_PORT}/tcp" >/dev/null || true
  sudo firewall-cmd --permanent --zone="$zone" --add-port="${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}/tcp" >/dev/null || true
  sudo firewall-cmd --permanent --zone="$zone" --add-port="${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}/udp" >/dev/null || true
  return 0
}

setup_firewall_linux_nftables() {
  if ! command -v nft >/dev/null 2>&1; then
    return 1
  fi
  if ! sudo nft list ruleset >/dev/null 2>&1; then
    return 1
  fi
  log "configuring nftables firewall for Grover ports"
  local inserted=0
  local family table chain hook
  while read -r family table chain hook; do
    [[ -n "$family" && -n "$table" && -n "$chain" && -n "$hook" ]] || continue
    if [[ "$hook" == "input" ]]; then
      nft_insert_once "$family" "$table" "$chain" "grover control tcp" tcp dport "$GROVER_CONTROL_PORT" accept
      nft_insert_once "$family" "$table" "$chain" "grover data tcp" tcp dport "${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}" accept
      nft_insert_once "$family" "$table" "$chain" "grover data udp" udp dport "${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}" accept
      inserted=1
    elif [[ "$hook" == "output" ]]; then
      nft_insert_once "$family" "$table" "$chain" "grover control tcp out" tcp sport "$GROVER_CONTROL_PORT" accept
      nft_insert_once "$family" "$table" "$chain" "grover data tcp out" tcp sport "${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}" accept
      nft_insert_once "$family" "$table" "$chain" "grover data udp out" udp sport "${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}" accept
      inserted=1
    fi
  done < <(nft_hook_chains)

  if [[ "$inserted" == "0" ]]; then
    sudo nft list table inet grover >/dev/null 2>&1 || sudo nft add table inet grover
    sudo nft list chain inet grover input >/dev/null 2>&1 ||
      sudo nft 'add chain inet grover input { type filter hook input priority -300; policy accept; }'
    sudo nft list chain inet grover output >/dev/null 2>&1 ||
      sudo nft 'add chain inet grover output { type filter hook output priority -300; policy accept; }'
    nft_insert_once inet grover input "grover control tcp" tcp dport "$GROVER_CONTROL_PORT" accept
    nft_insert_once inet grover input "grover data tcp" tcp dport "${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}" accept
    nft_insert_once inet grover input "grover data udp" udp dport "${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}" accept
    nft_insert_once inet grover output "grover control tcp out" tcp sport "$GROVER_CONTROL_PORT" accept
    nft_insert_once inet grover output "grover data tcp out" tcp sport "${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}" accept
    nft_insert_once inet grover output "grover data udp out" udp sport "${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}" accept
  fi
  return 0
}

nft_hook_chains() {
  sudo nft list ruleset 2>/dev/null | awk '
    $1 == "table" { family=$2; table=$3 }
    $1 == "chain" { chain=$2 }
    /hook input/ { print family, table, chain, "input" }
    /hook output/ { print family, table, chain, "output" }
  '
}

nft_insert_once() {
  local family="$1"
  local table="$2"
  local chain="$3"
  local comment="$4"
  shift 4
  if sudo nft list chain "$family" "$table" "$chain" 2>/dev/null | grep -Fq "comment \"$comment\""; then
    return 0
  fi
  sudo nft insert rule "$family" "$table" "$chain" "$@" comment "$comment" 2>/dev/null || true
}

setup_firewall_linux_iptables() {
  if ! command -v iptables >/dev/null 2>&1; then
    return 1
  fi
  log "configuring iptables firewall for Grover ports"
  sudo iptables -C INPUT -p tcp --dport "$GROVER_CONTROL_PORT" -j ACCEPT 2>/dev/null ||
    sudo iptables -I INPUT -p tcp --dport "$GROVER_CONTROL_PORT" -j ACCEPT
  sudo iptables -C INPUT -p tcp --dport "${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX}" -j ACCEPT 2>/dev/null ||
    sudo iptables -I INPUT -p tcp --dport "${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX}" -j ACCEPT
  sudo iptables -C INPUT -p udp --dport "${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX}" -j ACCEPT 2>/dev/null ||
    sudo iptables -I INPUT -p udp --dport "${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX}" -j ACCEPT

  sudo iptables -C OUTPUT -p tcp --sport "$GROVER_CONTROL_PORT" -j ACCEPT 2>/dev/null ||
    sudo iptables -I OUTPUT -p tcp --sport "$GROVER_CONTROL_PORT" -j ACCEPT
  sudo iptables -C OUTPUT -p tcp --sport "${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX}" -j ACCEPT 2>/dev/null ||
    sudo iptables -I OUTPUT -p tcp --sport "${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX}" -j ACCEPT
  sudo iptables -C OUTPUT -p udp --sport "${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX}" -j ACCEPT 2>/dev/null ||
    sudo iptables -I OUTPUT -p udp --sport "${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX}" -j ACCEPT
  return 0
}

setup_firewall_macos_pf() {
  if ! command -v pfctl >/dev/null 2>&1; then
    return 1
  fi
  local anchor_file="/etc/pf.anchors/grover"
  local anchor_line='anchor "grover"'
  local load_line='load anchor "grover" from "/etc/pf.anchors/grover"'
  log "configuring macOS pf firewall for Grover ports"
  sudo mkdir -p /etc/pf.anchors
  printf '%s\n' \
    "# Grover control and data-plane ports. Managed by bin/install_deps.sh." \
    "pass in proto tcp from any to any port ${GROVER_CONTROL_PORT} keep state" \
    "pass in proto tcp from any to any port ${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX} keep state" \
    "pass in proto udp from any to any port ${GROVER_DATA_PORT_MIN}:${GROVER_DATA_PORT_MAX} keep state" |
    sudo tee "$anchor_file" >/dev/null

  if ! grep -Fqx "$anchor_line" /etc/pf.conf; then
    printf '\n%s\n' "$anchor_line" | sudo tee -a /etc/pf.conf >/dev/null
  fi
  if ! grep -Fqx "$load_line" /etc/pf.conf; then
    printf '%s\n' "$load_line" | sudo tee -a /etc/pf.conf >/dev/null
  fi
  sudo pfctl -f /etc/pf.conf
  sudo pfctl -e 2>/dev/null || true
  return 0
}

setup_firewall() {
  if [[ "${GROVER_SETUP_FIREWALL:-1}" == "0" ]]; then
    log "skipping firewall setup because GROVER_SETUP_FIREWALL=0"
    return
  fi
  if ! [[ "$GROVER_CONTROL_PORT" =~ ^[0-9]+$ && "$GROVER_DATA_PORT_MIN" =~ ^[0-9]+$ && "$GROVER_DATA_PORT_MAX" =~ ^[0-9]+$ ]]; then
    log "firewall setup skipped; Grover port values must be numeric"
    return
  fi
  if ((GROVER_CONTROL_PORT < 1 || GROVER_CONTROL_PORT > 65535 ||
    GROVER_DATA_PORT_MIN < 1 || GROVER_DATA_PORT_MAX > 65535 ||
    GROVER_DATA_PORT_MIN > GROVER_DATA_PORT_MAX)); then
    log "firewall setup skipped; invalid Grover ports control=${GROVER_CONTROL_PORT} data=${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}"
    return
  fi

  log "opening Grover firewall ports: control tcp/${GROVER_CONTROL_PORT}, data tcp+udp/${GROVER_DATA_PORT_MIN}-${GROVER_DATA_PORT_MAX}"
  case "$OS" in
  linux)
    local configured=0
    if setup_firewall_linux_ufw; then
      configured=1
    fi
    if setup_firewall_linux_firewalld; then
      configured=1
    fi
    if setup_firewall_linux_nftables; then
      configured=1
    fi
    if setup_firewall_linux_iptables; then
      configured=1
    fi
    if [[ "$configured" == "0" ]]; then
      log "firewall setup skipped; no supported Linux firewall tool found"
    fi
    ;;
  darwin)
    setup_firewall_macos_pf || log "firewall setup skipped; pfctl not found"
    ;;
  *)
    log "firewall setup skipped; unsupported OS ${OS}"
    ;;
  esac
}

setup_rp_filter() {
  if [[ "$OS" != "linux" ]]; then
    return
  fi
  if [[ "${GROVER_DISABLE_RP_FILTER:-1}" == "0" ]]; then
    log "skipping reverse-path filter setup because GROVER_DISABLE_RP_FILTER=0"
    return
  fi
  if ! command -v sysctl >/dev/null 2>&1; then
    log "reverse-path filter setup skipped; sysctl not found"
    return
  fi

  log "disabling Linux reverse-path filtering for floating-IP/asymmetric-route Grover nodes"
  sudo sysctl -w net.ipv4.conf.all.rp_filter=0 >/dev/null || true
  sudo sysctl -w net.ipv4.conf.default.rp_filter=0 >/dev/null || true

  local iface
  for iface_path in /proc/sys/net/ipv4/conf/*/rp_filter; do
    [[ -e "$iface_path" ]] || continue
    iface="$(basename "$(dirname "$iface_path")")"
    if [[ "$iface" == "all" || "$iface" == "default" || "$iface" == "lo" ]]; then
      continue
    fi
    sudo sysctl -w "net.ipv4.conf.${iface}.rp_filter=0" >/dev/null || true
  done

  local sysctl_file="/etc/sysctl.d/99-grover-network.conf"
  log "installing persistent reverse-path filter config at ${sysctl_file}"
  {
    echo "# Grover: floating IPs, tunnels, overlays, and multi-NIC experiments can use asymmetric routing."
    echo "# Strict rp_filter may drop valid inbound Grover control/data traffic before groverd can reply."
    echo "net.ipv4.conf.all.rp_filter=0"
    echo "net.ipv4.conf.default.rp_filter=0"
  } | sudo tee "$sysctl_file" >/dev/null

  for iface_path in /proc/sys/net/ipv4/conf/*/rp_filter; do
    [[ -e "$iface_path" ]] || continue
    iface="$(basename "$(dirname "$iface_path")")"
    if [[ "$iface" == "all" || "$iface" == "default" || "$iface" == "lo" ]]; then
      continue
    fi
    echo "net.ipv4.conf.${iface}.rp_filter=0"
  done | sudo tee -a "$sysctl_file" >/dev/null

  sudo sysctl --system >/dev/null || true
}

setup_cpu_governor() {
  if [[ "$OS" != "linux" ]]; then
    return
  fi
  if [[ "${GROVER_SETUP_CPU_GOVERNOR:-1}" == "0" ]]; then
    log "skipping CPU governor setup because GROVER_SETUP_CPU_GOVERNOR=0"
    return
  fi

  local governors=()
  local governor_file
  shopt -s nullglob
  for governor_file in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    governors+=("$governor_file")
  done
  shopt -u nullglob

  if [[ "${#governors[@]}" -eq 0 ]]; then
    log "CPU governor setup skipped; cpufreq scaling_governor files are not present"
    return
  fi

  log "current CPU governors:"
  grep -h . "${governors[@]}" 2>/dev/null | sort | uniq -c | sed 's/^/[grover]   /' || true

  log "setting CPU governors to performance"
  if command -v cpupower >/dev/null 2>&1; then
    sudo cpupower frequency-set -g performance >/dev/null 2>&1 || true
  fi

  for governor_file in "${governors[@]}"; do
    echo performance | sudo tee "$governor_file" >/dev/null 2>&1 || true
  done

  log "CPU governors after setup:"
  grep -h . "${governors[@]}" 2>/dev/null | sort | uniq -c | sed 's/^/[grover]   /' || true
}

run_cpu_benchmark() {
  if [[ "$OS" != "linux" ]]; then
    return
  fi
  if [[ "${GROVER_RUN_CPU_BENCHMARK:-1}" == "0" ]]; then
    log "skipping CPU benchmark because GROVER_RUN_CPU_BENCHMARK=0"
    return
  fi
  if ! command -v stress-ng >/dev/null 2>&1; then
    log "CPU benchmark skipped; stress-ng is not installed"
    return
  fi

  log "running CPU baseline: stress-ng --matrix 0 --matrix-method prod --timeout ${GROVER_CPU_BENCHMARK_TIMEOUT} --metrics-brief"
  stress-ng --matrix 0 --matrix-method prod --timeout "$GROVER_CPU_BENCHMARK_TIMEOUT" --metrics-brief 2>&1 |
    while IFS= read -r line; do
      echo "[grover][stress-ng] ${line}"
    done
}

setup_rapl_permissions() {
  if [[ "$OS" != "linux" ]]; then
    return
  fi
  if [[ "${GROVER_SETUP_RAPL:-1}" == "0" ]]; then
    log "skipping RAPL permission setup because GROVER_SETUP_RAPL=0"
    return
  fi
  if [[ ! -d /sys/class/powercap ]]; then
    log "RAPL permission setup skipped; /sys/class/powercap is not present"
    return
  fi

  local energy_files=()
  local file
  shopt -s nullglob
  for file in /sys/class/powercap/intel-rapl*/energy_uj; do
    energy_files+=("$file")
  done
  shopt -u nullglob

  if [[ "${#energy_files[@]}" -eq 0 ]]; then
    log "RAPL permission setup skipped; no energy_uj counters found"
    return
  fi

  local target_user="${GROVER_RAPL_USER:-}"
  if [[ -z "$target_user" ]]; then
    if [[ -n "${SUDO_USER:-}" && "${SUDO_USER:-}" != "root" ]]; then
      target_user="$SUDO_USER"
    elif command -v logname >/dev/null 2>&1; then
      target_user="$(logname 2>/dev/null || true)"
    fi
  fi
  if [[ -z "$target_user" || "$target_user" == "root" ]]; then
    target_user="$(id -un 2>/dev/null || true)"
  fi
  if [[ -z "$target_user" || "$target_user" == "root" ]]; then
    log "RAPL permission setup skipped; set GROVER_RAPL_USER=<user> when running as root"
    return
  fi
  if ! id "$target_user" >/dev/null 2>&1; then
    log "RAPL permission setup skipped; user ${target_user} does not exist"
    return
  fi

  log "configuring RAPL energy counter read access for user ${target_user}"
  sudo groupadd -f rapl
  sudo usermod -aG rapl "$target_user"

  for file in "${energy_files[@]}"; do
    sudo chgrp rapl "$file" || true
    sudo chmod 0440 "$file" || true
  done

  if command -v systemd-tmpfiles >/dev/null 2>&1; then
    local tmpfiles_rule="/etc/tmpfiles.d/grover-rapl.conf"
    log "installing persistent RAPL tmpfiles rule at ${tmpfiles_rule}"
    printf '%s\n' \
      '# Allow grover users in group rapl to read Intel RAPL energy counters.' \
      'z /sys/class/powercap/intel-rapl*/energy_uj 0440 root rapl - -' |
      sudo tee "$tmpfiles_rule" >/dev/null
    sudo systemd-tmpfiles --create "$tmpfiles_rule" || true
  else
    log "systemd-tmpfiles not found; RAPL permissions may need to be reapplied after reboot"
  fi

  if id -nG "$target_user" | tr ' ' '\n' | grep -qx rapl; then
    log "user ${target_user} is already in group rapl"
  else
    log "user ${target_user} was added to group rapl; run 'newgrp rapl' or log out/in before starting groverd"
  fi
}

persist_path() {
  local shell_path="${SHELL:-}"
  local profile=""
  case "$shell_path" in
  */zsh) profile="$HOME/.zshrc" ;;
  */bash) profile="$HOME/.bashrc" ;;
  *) profile="$HOME/.profile" ;;
  esac
  local export_line="export PATH=\"${BIN_DIR}:\$PATH\""
  if [[ -f "$profile" ]] && grep -Fqx "$export_line" "$profile"; then
    log "PATH entry already present in ${profile}"
    return
  fi
  log "appending PATH export to ${profile}"
  {
    echo ""
    echo "# Added by grover install_deps on $(date)"
    echo "$export_line"
  } >>"$profile"
  log "run 'source ${profile}' (or restart your shell) to pick up the new PATH"
}

install_go
install_protoc
install_protoc_plugins
sync_vendor
build_project
setup_firewall
setup_rp_filter
setup_cpu_governor
run_cpu_benchmark
setup_rapl_permissions
persist_path

log "installation complete."
