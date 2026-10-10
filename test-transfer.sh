#!/usr/bin/env bash
# Local development aid: test this directory's sender with a fresh proxy and receiver.
# Usage: bash test-transfer.sh <input_file> [proxy options...]
# Examples:
#   bash test-transfer.sh short.bin --delay-ms 15
#   MAX_WINDOW=2 bash test-transfer.sh medium.bin --delay-ms 15 --drop-data-once 1
set -euo pipefail

if (( $# < 1 )); then
    echo "Usage: bash test-transfer.sh <input_file> [proxy options...]" >&2
    exit 2
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
sender_source=${SENDER_SOURCE:-"$script_dir/sender.java"}
input=$(realpath -- "$1")
shift
if [[ ! -f "$input" ]]; then
    echo "Not a regular input file: $input" >&2
    exit 2
fi

max_window=${MAX_WINDOW:-8}
deadline=${DEADLINE:-40}
receiver_port=${RECEIVER_PORT:-$((20000 + ($$ * 97) % 20000))}
proxy_port=${PROXY_PORT:-$((receiver_port + 1))}
transfer_id="local-$$"
workdir=$(mktemp -d "${TMPDIR:-/tmp}/p2-transfer.XXXXXXXX")
mkdir -p "$workdir/output"
receiver_pid=
proxy_pid=

cleanup() {
    if [[ -n "$proxy_pid" ]]; then
        kill "$proxy_pid" 2>/dev/null || true
        wait "$proxy_pid" 2>/dev/null || true
    fi
    if [[ -n "$receiver_pid" ]]; then
        kill "$receiver_pid" 2>/dev/null || true
        wait "$receiver_pid" 2>/dev/null || true
    fi
    echo "Logs and received file: $workdir"
}
trap cleanup EXIT

wait_ready() {
    local pid=$1 log=$2 marker=$3
    for (( n=0; n<100; n++ )); do
        if grep -Fxq -- "$marker" "$log"; then return 0; fi
        if ! kill -0 "$pid" 2>/dev/null; then return 1; fi
        sleep 0.1
    done
    return 1
}

java --source 21 "$script_dir/receiver.java" "$receiver_port" "$workdir/output" \
    >"$workdir/receiver.log" 2>"$workdir/receiver.err" &
receiver_pid=$!
if ! wait_ready "$receiver_pid" "$workdir/receiver.log" "RECEIVER_READY port=$receiver_port"; then
    echo "Receiver did not become ready; see $workdir/receiver.err" >&2
    exit 1
fi

java --source 21 "$script_dir/proxy.java" \
    --listen-port "$proxy_port" --receiver-host 127.0.0.1 \
    --receiver-port "$receiver_port" --seed 260919 "$@" --verbose \
    >"$workdir/proxy.log" 2>"$workdir/proxy.err" &
proxy_pid=$!
if ! wait_ready "$proxy_pid" "$workdir/proxy.log" \
    "PROXY_READY port=$proxy_port receiver_port=$receiver_port"; then
    echo "Proxy did not become ready; see $workdir/proxy.err" >&2
    exit 1
fi

if ! timeout "$deadline" java --source 21 "$sender_source" \
    127.0.0.1 "$proxy_port" "$input" --transfer-id "$transfer_id" \
    --max-window "$max_window" >"$workdir/sender.log" 2>"$workdir/sender.err"; then
    echo "Sender failed or timed out; see $workdir/sender.log and $workdir/sender.err" >&2
    exit 1
fi

received="$workdir/output/$transfer_id.received"
for (( n=0; n<30; n++ )); do
    if grep -Eq -- "^RECEIVER_COMPLETE transfer=$transfer_id .* status=OK file=" \
        "$workdir/receiver.log" && [[ -f "$received" ]]; then
        break
    fi
    if ! kill -0 "$receiver_pid" 2>/dev/null; then break; fi
    sleep 0.1
done

if [[ ! -f "$received" ]] || ! grep -Eq -- \
    "^RECEIVER_COMPLETE transfer=$transfer_id .* status=OK file=" "$workdir/receiver.log"; then
    echo "Receiver did not complete successfully; see $workdir/receiver.log" >&2
    exit 1
fi
if ! cmp -s -- "$input" "$received"; then
    echo "Received file differs from input: $received" >&2
    exit 1
fi

echo "PASS: received bytes match $input"
echo "Inspect sender.log and proxy.err to explain the transfer."
