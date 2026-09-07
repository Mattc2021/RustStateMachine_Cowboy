#!/usr/bin/env bash
set -euo pipefail

workspace="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
expected_apps="${EXPECTED_APPS:-3}"
timeout_seconds="${AUTO_TIMEOUT_SECONDS:-20}"
hold_milliseconds="${AUTO_HOLD_MS:-1000}"
fault_after_milliseconds="${FAULT_AFTER_MS:-5000}"
power_off_milliseconds="${POWER_OFF_MS:-2000}"
client_pids=()
sam_pid=""

cleanup() {
    if [[ -n "$sam_pid" ]]; then
        kill "$sam_pid" 2>/dev/null || true
    fi
    if ((${#client_pids[@]})); then
        kill "${client_pids[@]}" 2>/dev/null || true
        wait "${client_pids[@]}" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

cd "$workspace"
echo "Building the complete SAM workspace..."
cargo build --workspace

echo
echo "============================================================"
echo " SAM RESILIENCE SHOWCASE"
echo "============================================================"
echo "Starting SAM, navigation, guidance, and telemetry..."
./target/debug/sam \
    --auto-showcase \
    --expected-apps "$expected_apps" \
    --auto-timeout-secs "$timeout_seconds" \
    --auto-hold-ms "$hold_milliseconds" &
sam_pid=$!

./target/debug/navigation & client_pids+=("$!")
./target/debug/guidance & client_pids+=("$!")
./target/debug/telemetry --exit-after-ms "$fault_after_milliseconds" &
telemetry_pid=$!
client_pids+=("$telemetry_pid")

set +e
wait "$telemetry_pid"
set -e

echo
echo "[LAUNCHER] Telemetry process stopped; power is OFF for $power_off_milliseconds ms..."
sleep "$((power_off_milliseconds / 1000)).$((power_off_milliseconds % 1000))"
echo "[LAUNCHER] Restoring telemetry power with one temporary safety interlock..."
./target/debug/telemetry --reject-working-once &
client_pids+=("$!")

wait "$sam_pid"
echo
echo "SHOWCASE PASSED"
