#!/usr/bin/env bash
#
# Differential conformance run: the same operations against a reference etcd and
# against rudurru, diffed per check.
#
#   ./scripts/conformance.sh                 # run; fail on unknown deviations
#   ./scripts/conformance.sh --update-baseline
#
# Everything runs on loopback with throwaway data dirs. It never talks to a
# running cluster: this script starts its own etcd and its own rudurru.
#
# Requirements: curl, tar, cargo, python3.
set -euo pipefail

ETCD_VERSION="${ETCD_VERSION:-v3.5.30}"
RUDURRU_PROFILE="${RUDURRU_PROFILE:-release}"   # release ships with panic=abort
RUDURRU_PORT="${RUDURRU_PORT:-12390}"
ETCD_PORT="${ETCD_PORT:-12399}"
ETCD_PEER_PORT="${ETCD_PEER_PORT:-12398}"
BASELINE="tests/conformance/known-deviations.txt"
OUT="target/conformance"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# rustup installs cargo outside the default PATH for non-interactive shells
if [[ -x "${CARGO_HOME:-$HOME/.cargo}/bin/cargo" ]]; then
    export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
fi
command -v cargo >/dev/null || die "cargo not found in PATH"

UPDATE_BASELINE=0
[[ "${1:-}" == "--update-baseline" ]] && UPDATE_BASELINE=1

mkdir -p "$OUT"

log() { printf '\033[1m==>\033[0m %s\n' "$*"; }
die() { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

port_free() { ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
for p in "$RUDURRU_PORT" "$ETCD_PORT" "$ETCD_PEER_PORT"; do
    port_free "$p" || die "port $p is already in use (override with RUDURRU_PORT=/ETCD_PORT=/ETCD_PEER_PORT=)"
done

# ── reference etcd ────────────────────────────────────────────────────
ETCD_DIR="$OUT/etcd-$ETCD_VERSION-linux-amd64"
ETCD="$ETCD_DIR/etcd"
ETCDCTL="$ETCD_DIR/etcdctl"
if [[ ! -x "$ETCD" ]]; then
    log "downloading etcd $ETCD_VERSION"
    mkdir -p "$OUT/bin"
    url="https://github.com/etcd-io/etcd/releases/download/$ETCD_VERSION/etcd-$ETCD_VERSION-linux-amd64.tar.gz"
    curl -sSL -o "$OUT/bin/etcd.tar.gz" "$url" || die "download failed: $url"
    tar xzf "$OUT/bin/etcd.tar.gz" -C "$OUT"
    [[ -x "$ETCD" ]] || die "etcd binary not found after extract"
fi

WORK="$(mktemp -d)"
ETCD_PID=""
RUDURRU_PID=""
cleanup() {
    [[ -n "$RUDURRU_PID" ]] && kill "$RUDURRU_PID" 2>/dev/null || true
    [[ -n "$ETCD_PID" ]] && kill "$ETCD_PID" 2>/dev/null || true
    rm -rf "$WORK"
}
trap cleanup EXIT

log "starting reference etcd on 127.0.0.1:$ETCD_PORT"
"$ETCD" \
    --data-dir "$WORK/etcd-data" \
    --listen-client-urls "http://127.0.0.1:$ETCD_PORT" \
    --advertise-client-urls "http://127.0.0.1:$ETCD_PORT" \
    --listen-peer-urls "http://127.0.0.1:$ETCD_PEER_PORT" \
    --initial-advertise-peer-urls "http://127.0.0.1:$ETCD_PEER_PORT" \
    --initial-cluster "default=http://127.0.0.1:$ETCD_PEER_PORT" \
    --log-level error >"$WORK/etcd.log" 2>&1 &
ETCD_PID=$!

log "building rudurru ($RUDURRU_PROFILE)"
cargo build --quiet $([[ "$RUDURRU_PROFILE" == "release" ]] && echo --release)

log "starting rudurru on 127.0.0.1:$RUDURRU_PORT"
RUDURRU_LISTEN="127.0.0.1:$RUDURRU_PORT" \
RUDURRU_WAL="$WORK/rudurru.wal" \
RUST_LOG=rudurru=warn \
    "./target/$RUDURRU_PROFILE/rudurru" >"$WORK/rudurru.log" 2>&1 &
RUDURRU_PID=$!

wait_ready() {
    local port=$1 name=$2
    for _ in $(seq 1 100); do
        if "$ETCDCTL" --endpoints="http://127.0.0.1:$port" --command-timeout=1s endpoint health >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    die "$name did not become ready"
}
wait_ready "$ETCD_PORT" "reference etcd"
wait_ready "$RUDURRU_PORT" "rudurru"

# ── run the same suite against both ───────────────────────────────────
run_suite() {
    local port=$1 outfile=$2
    ETCD_ENDPOINT="http://127.0.0.1:$port" \
        cargo test --quiet --test conformance -- --ignored --nocapture --test-threads=1 2>/dev/null \
        | grep '^CHECK|' | sort >"$outfile" || true
    [[ -s "$outfile" ]] || die "no CHECK lines produced for port $port (server crashed or test failed)"
}

log "running conformance suite against etcd"
run_suite "$ETCD_PORT" "$OUT/etcd.txt"
kill -0 "$ETCD_PID" 2>/dev/null && echo "CHECK|server.process_alive|alive=true" >>"$OUT/etcd.txt" \
    || echo "CHECK|server.process_alive|alive=false" >>"$OUT/etcd.txt"

log "running conformance suite against rudurru"
run_suite "$RUDURRU_PORT" "$OUT/rudurru.txt"
kill -0 "$RUDURRU_PID" 2>/dev/null && echo "CHECK|server.process_alive|alive=true" >>"$OUT/rudurru.txt" \
    || echo "CHECK|server.process_alive|alive=false" >>"$OUT/rudurru.txt"
sort -o "$OUT/etcd.txt" "$OUT/etcd.txt"
sort -o "$OUT/rudurru.txt" "$OUT/rudurru.txt"

# ── diff ──────────────────────────────────────────────────────────────
python3 - "$BASELINE" "$UPDATE_BASELINE" "$OUT/etcd.txt" "$OUT/rudurru.txt" "$OUT/deviations.txt" <<'PY'
import sys

baseline_path, update, etcd_path, rud_path, out_path = sys.argv[1:6]

def load(path):
    d = {}
    for line in open(path):
        line = line.rstrip("\n")
        if not line.startswith("CHECK|"):
            continue
        _, name, value = line.split("|", 2)
        d[name] = value
    return d

etcd = load(etcd_path)
rud = load(rud_path)

deviations = {}
for name in sorted(set(etcd) | set(rud)):
    e = etcd.get(name, "<missing>")
    r = rud.get(name, "<missing>")
    if e != r:
        deviations[name] = (e, r)

known = set()
try:
    for line in open(baseline_path):
        line = line.split("#", 1)[0].strip()
        if line:
            known.add(line)
except FileNotFoundError:
    pass

if update == "1":
    with open(baseline_path, "w") as f:
        f.write("# Checks that currently deviate from etcd. Regenerate with:\n")
        f.write("#   ./scripts/conformance.sh --update-baseline\n")
        f.write("# Each entry is a CHECK name; see prd/etcd-conformance-audit.md.\n")
        for name in sorted(deviations):
            f.write(name + "\n")
    print(f"baseline updated: {len(deviations)} known deviation(s)")
    sys.exit(0)

with open(out_path, "w") as f:
    for name, (e, r) in deviations.items():
        f.write(f"{name}\n    etcd    : {e}\n    rudurru : {r}\n")

new = sorted(set(deviations) - known)
fixed = sorted(known - set(deviations))

print(f"checks: {len(etcd)} etcd, {len(rud)} rudurru; deviations: {len(deviations)} ({len(known)} known)")

if deviations:
    print("\ndeviations:")
    for name in sorted(deviations):
        e, r = deviations[name]
        mark = "new" if name in new else "known"
        print(f"  [{mark}] {name}\n      etcd    : {e}\n      rudurru : {r}")

if new:
    print(f"\n{len(new)} NEW deviation(s) not in {baseline_path}:")
    for name in new:
        print(f"  {name}")
if fixed:
    print(f"\n{len(fixed)} baseline entr(ies) no longer deviate — update the baseline:")
    for name in fixed:
        print(f"  {name}")

if new or fixed:
    print(f"\nfull diff: {out_path}")
    sys.exit(1)

print("\nconformance: no unknown deviations")
PY
