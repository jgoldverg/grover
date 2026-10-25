#!/usr/bin/env python3
"""Cross-platform energy sampler.
- Linux: reads Intel RAPL counters (energy_uj_*)
- macOS: parses powermetrics output for CPU/DRAM/GPU power.
Outputs CSV with job metadata.
"""

import argparse, csv, os, sys, time, socket, platform, subprocess, re
from pathlib import Path

POWERCAP_ROOT = Path("/sys/class/powercap")


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


# ------------------ Linux Sampler ------------------
def discover_powercap_domains():
    pairs = []
    if not POWERCAP_ROOT.exists():
        return pairs

    def classify(domain_path: Path) -> str:
        """Produce a friendly, lower-case name for the RAPL domain."""
        name_file = domain_path / "name"
        try:
            name = name_file.read_text().strip().lower()
        except Exception:
            return "pkg"
        if "dram" in name:
            return "dram"
        if "core" in name:
            return "cores"
        if "pkg" in name or "package" in name:
            return "pkg"
        return name

    def visit(domain_path: Path, seen: dict):
        energy_file = domain_path / "energy_uj"
        if energy_file.is_file():
            base_name = classify(domain_path)
            count = seen.get(base_name, 0)
            seen[base_name] = count + 1
            if count:
                domain_key = f"{base_name}_{count}"
            else:
                domain_key = base_name
            pairs.append((domain_key, energy_file))

        # Recurse into nested intel-rapl domains for per-component counters.
        for child in sorted(domain_path.iterdir()):
            if child.is_dir() and child.name.startswith("intel-rapl"):
                visit(child, seen)

    seen_names = {}
    for top in sorted(POWERCAP_ROOT.glob("intel-rapl:*")):
        if top.is_dir():
            visit(top, seen_names)

    return pairs


def linux_sample(pid: int, out: Path, interval_ms: float, meta: dict):
    domains = discover_powercap_domains()
    if not domains:
        print("No RAPL domains found", file=sys.stderr)
        return 2

    # ------------------------------
    # "Total" energy guidance
    # ------------------------------
    # RAPL exposes multiple counters that can overlap (e.g., "cores"/"uncore"
    # are often subdomains of "pkg"). Many users still want a single number.
    # We provide TWO totals:
    #   - energy_uj_sum_all: sum of *all* discovered counters (may double-count)
    #   - energy_uj_total:   best-effort "total" (prefers psys, else pkg+dram)
    # Open all counters defensively (permissions may differ per-domain).
    fps = []
    kept_domains = []
    for d, p in domains:
        try:
            fps.append(open(p, "r"))
            kept_domains.append((d, p))
        except PermissionError:
            print(f"[RAPL] permission denied: {p} (domain {d})", file=sys.stderr)
    domains = kept_domains
    if not domains:
        print("Found RAPL domains, but none are readable (permission denied)", file=sys.stderr)
        return 2

    # Decide which counters contribute to the "best-effort" total.
    # Prefer psys if present; else pkg + dram across sockets.
    domain_names = [d for d, _ in domains]
    psys_names = [d for d in domain_names if d == "psys" or d.startswith("psys_")]
    if psys_names:
        total_domain_set = set(psys_names)
    else:
        pkg_names = [d for d in domain_names if d == "pkg" or d.startswith("pkg_")]
        dram_names = [d for d in domain_names if d == "dram" or d.startswith("dram_")]
        total_domain_set = set(pkg_names + dram_names)
    total_indices = [i for i, d in enumerate(domain_names) if d in total_domain_set]

    out.parent.mkdir(parents=True, exist_ok=True)
    host = socket.gethostname()
    new_file = not out.exists() or out.stat().st_size == 0
    header = [
        "timestamp_ns",
        "tick",
        "job_name",
        "job_file",
        "job_count",
        "mode",
        "nodes_config",
        "associations_path",
        "route_key",
        "pid",
        "host",
        "scheduler_name",
    ] + [f"energy_uj_{d}" for d, _ in domains] + [
        "energy_uj_sum_all",
        "energy_uj_total",
    ]
    with out.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(header)
        tick = 0
        interval = interval_ms / 1000
        while pid_alive(pid):
            ts = time.time_ns()
            row = [
                ts,
                tick,
                meta["job_name"],
                meta["job_file"],
                meta.get("job_count", ""),
                meta["mode"],
                meta["nodes_config"],
                meta["associations_path"],
                meta["route_key"],
                pid,
                host,
                meta["scheduler_name"],
            ]

            energies = []
            for fp in fps:
                fp.seek(0)
                raw = fp.read().strip()
                try:
                    val = int(raw)
                except Exception:
                    val = 0
                energies.append(val)
                row.append(val)

            sum_all = sum(energies)
            total = sum(energies[i] for i in total_indices) if total_indices else ""
            row.append(sum_all)
            row.append(total)
            w.writerow(row)
            tick += 1
            time.sleep(interval)
    for fp in fps:
        fp.close()
    return 0


# ------------------ macOS Sampler ------------------
PM_PATTERNS = {
    "cpu_w": re.compile(r"CPU Power:\s*([0-9.]+)\s*([umk]?W)", re.I),
    "dram_w": re.compile(r"DRAM Power:\s*([0-9.]+)\s*([umk]?W)", re.I),
    "gpu_w": re.compile(r"GPU Power:\s*([0-9.]+)\s*([umk]?W)", re.I),
}


def _to_watts(value: str, unit: str):
    try:
        v = float(value)
    except Exception:
        return None
    unit = (unit or "W").strip().lower()
    if unit == "w":
        return v
    if unit == "mw":
        return v / 1000.0
    if unit == "uw":
        return v / 1_000_000.0
    if unit == "kw":
        return v * 1000.0
    return None


def mac_sample(pid: int, out: Path, interval_ms: float, meta: dict):
    out.parent.mkdir(parents=True, exist_ok=True)
    host = socket.gethostname()
    new_file = not out.exists() or out.stat().st_size == 0
    header = [
        "timestamp_ns",
        "tick",
        "job_name",
        "job_file",
        "job_count",
        "mode",
        "nodes_config",
        "associations_path",
        "route_key",
        "pid",
        "host",
        "dt_s",
        "cpu_w",
        "dram_w",
        "gpu_w",
        "cpu_j",
        "dram_j",
        "gpu_j",
        "total_j",
        "cum_total_j",
        "scheduler_name",
    ]
    cmd = [
        "sudo",
        "-n",
        "powermetrics",
        "-i",
        str(int(max(interval_ms, 100))),
        "--samplers",
        "cpu_power",
    ]
    with out.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(header)
        tick = 0
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
        cur = {"cpu_w": None, "dram_w": None, "gpu_w": None}
        prev_ts = None
        cum_total_j = 0.0
        for line in proc.stdout:
            if not pid_alive(pid):
                break
            for k, r in PM_PATTERNS.items():
                m = r.search(line)
                if m:
                    cur[k] = _to_watts(m.group(1), m.group(2))
            if "Sampled system activity" in line:
                ts = time.time_ns()
                dt_s = 0.0 if prev_ts is None else max(0.0, (ts - prev_ts) / 1_000_000_000.0)
                prev_ts = ts
                cpu_j = (cur["cpu_w"] * dt_s) if cur["cpu_w"] is not None else None
                dram_j = (cur["dram_w"] * dt_s) if cur["dram_w"] is not None else None
                gpu_j = (cur["gpu_w"] * dt_s) if cur["gpu_w"] is not None else None
                total_j = sum(v for v in (cpu_j, dram_j, gpu_j) if v is not None)
                cum_total_j += total_j
                row = [
                    ts,
                    tick,
                    meta["job_name"],
                    meta["job_file"],
                    meta.get("job_count", ""),
                    meta["mode"],
                    meta["nodes_config"],
                    meta["associations_path"],
                    meta["route_key"],
                    pid,
                    host,
                    dt_s,
                    cur["cpu_w"],
                    cur["dram_w"],
                    cur["gpu_w"],
                    cpu_j,
                    dram_j,
                    gpu_j,
                    total_j,
                    cum_total_j,
                    meta["scheduler_name"],
                ]
                w.writerow(row)
                tick += 1
                cur = {"cpu_w": None, "dram_w": None, "gpu_w": None}
        proc.terminate()
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pid", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval-ms", type=float, default=5.0)
    ap.add_argument("--job-name", dest="job_name", default="")
    ap.add_argument("--job-file", dest="job_file", default="")
    ap.add_argument("--job-count", type=int, default=0)
    ap.add_argument("--mode", default="")
    ap.add_argument("--nodes-config", default="")
    ap.add_argument("--associations-path", default="")
    ap.add_argument("--route-key", default="")
    ap.add_argument("--scheduler-name", default="")
    a = ap.parse_args()
    meta = vars(a)
    sys_os = platform.system()
    out = Path(a.out)
    if sys_os == "Linux":
        return linux_sample(a.pid, out, a.interval_ms, meta)
    elif sys_os == "Darwin":
        return mac_sample(a.pid, out, max(a.interval_ms, 100), meta)
    else:
        print("Unsupported OS", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
