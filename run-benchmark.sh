#!/usr/bin/env bash
# Interactive launcher for the Kafka filesystem benchmark.
#
# Usage:
#   ./run-benchmark.sh          # show the menu
#   ./run-benchmark.sh 3        # run menu item 3 immediately
#   ./run-benchmark.sh --list   # print menu items only
#
# Optional environment variables:
#   BENCH_STORAGE=cns           # 0=fixed DM, 1=dynamic DM, or cns (default: 1)
#   BENCH_CNS_DEVICE=/dev/nvme0n2  # required when BENCH_STORAGE=cns
#   BENCH_ROUNDS=1              # repetitions (default: 1)
#   BENCH_SCENARIO_GROUP=baseline  # baseline, dynamic, or all
# Other BENCH_* variables documented in modify.md are passed through sudo -E.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
STORAGE="${BENCH_STORAGE:-${BENCH_DM_IMPL:-1}}"
ROUNDS="${BENCH_ROUNDS:-1}"
SCENARIO_GROUP="${BENCH_SCENARIO_GROUP:-baseline}"

print_menu() {
    cat <<'EOF'
Kafka filesystem benchmark

  0) Run all             - run the five useful suites below
  1) Fresh latency       - normal-load latency, reset before every scenario
  2) Occupancy latency   - per record size: reset, then 20/40/60/80%
  3) Fresh saturation    - maximum-load throughput on a reset device
  4) 80% endurance       - 80% fixed prefill + bounded Kafka retention cycling
  5) Kafka steady-state  - no prefill; Kafka grows to retention equilibrium
EOF
}

usage() {
    print_menu
    cat <<'EOF'

Usage: ./run-benchmark.sh [0-5|--list|--help]

Examples:
  ./run-benchmark.sh
  ./run-benchmark.sh 0
  ./run-benchmark.sh 2
  BENCH_ROUNDS=3 ./run-benchmark.sh 1
  BENCH_LONG_DURATION_SECONDS=600 BENCH_LONG_WARMUP_SECONDS=60 ./run-benchmark.sh 4
  BENCH_STORAGE=cns BENCH_CNS_DEVICE=/dev/nvme0n2 ./run-benchmark.sh 1
  BENCH_FAIL_FAST_STALL_SECONDS=60 ./run-benchmark.sh 4
EOF
}

selection="${1:-}"
if [[ $# -gt 1 ]]; then
    usage >&2
    exit 2
fi

case "$selection" in
    --help|-h)
        usage
        exit 0
        ;;
    --list)
        print_menu
        exit 0
        ;;
    "")
        print_menu
        printf '\nSelect a test [0-5]: '
        read -r selection
        ;;
esac

case "$selection" in
    0) profile="all";        workload_mode="all" ;;
    1) profile="latency";    workload_mode="fresh" ;;
    2) profile="latency";    workload_mode="occupancy" ;;
    3) profile="saturation"; workload_mode="fresh" ;;
    4) profile="latency";    workload_mode="endurance" ;;
    5) profile="latency";    workload_mode="steady" ;;
    *)
        printf 'Invalid selection: %s (expected 0-5)\n' "$selection" >&2
        exit 2
        ;;
esac

if [[ "$STORAGE" != "0" && "$STORAGE" != "1" && "$STORAGE" != "cns" ]]; then
    printf 'BENCH_STORAGE must be 0 (fixed), 1 (dynamic), or cns.\n' >&2
    exit 2
fi
if [[ "$STORAGE" == "cns" && -z "${BENCH_CNS_DEVICE:-}" ]]; then
    printf 'BENCH_CNS_DEVICE is required when BENCH_STORAGE=cns.\n' >&2
    exit 2
fi
if [[ ! "$ROUNDS" =~ ^[1-9][0-9]*$ ]]; then
    printf 'BENCH_ROUNDS must be a positive integer.\n' >&2
    exit 2
fi
case "$SCENARIO_GROUP" in
    baseline|dynamic|all) ;;
    *)
        printf 'BENCH_SCENARIO_GROUP must be baseline, dynamic, or all.\n' >&2
        exit 2
        ;;
esac

required_commands=(python3 sudo fio iostat vmstat findmnt)
if [[ "$STORAGE" != "cns" ]]; then
    required_commands+=(dmsetup blkzone)
fi
for command in "${required_commands[@]}"; do
    if ! command -v "$command" >/dev/null 2>&1; then
        printf 'Required command not found: %s\n' "$command" >&2
        exit 1
    fi
done

if [[ ! -f "$SCRIPT_DIR/bench_final.py" ]]; then
    printf 'bench_final.py not found below %s\n' "$SCRIPT_DIR" >&2
    exit 1
fi

printf '\nSelected benchmark\n'
printf '  Storage backend   : %s\n' "$STORAGE"
if [[ "$STORAGE" == "cns" ]]; then
    printf '  CNS device        : %s\n' "$BENCH_CNS_DEVICE"
fi
printf '  Rounds            : %s\n' "$ROUNDS"
printf '  Profile           : %s\n' "$profile"
printf '  Scenario group    : %s\n' "$SCENARIO_GROUP"
printf '  Workload mode     : %s\n' "$workload_mode"
if [[ "$workload_mode" == "occupancy" || "$workload_mode" == "endurance" ]]; then
    printf '  Occupancy points  : %s\n' "${BENCH_OCCUPANCY_POINTS:-20,40,60,80}"
fi
if [[ "$workload_mode" == "endurance" || "$workload_mode" == "steady" ]]; then
    printf '  Long measurement  : %ss (warmup %ss, record %sB, %s)\n' \
        "${BENCH_LONG_DURATION_SECONDS:-3600}" \
        "${BENCH_LONG_WARMUP_SECONDS:-300}" \
        "${BENCH_LONG_RECORD_SIZE:-1024}" \
        "${BENCH_LONG_SCENARIO:-scenario_b}"
fi
if [[ "$workload_mode" == "endurance" ]]; then
    printf '  Kafka retention   : %s bytes total\n' \
        "${BENCH_ENDURANCE_RETENTION_TOTAL_BYTES:-2147483648}"
elif [[ "$workload_mode" == "steady" ]]; then
    printf '  Kafka retention   : %s bytes total\n' \
        "${BENCH_STEADY_RETENTION_TOTAL_BYTES:-25769803776}"
    printf '  Steady warmup     : %ss\n' "${BENCH_STEADY_WARMUP_SECONDS:-1800}"
fi
if [[ "$selection" == "0" ]]; then
    printf '  Suites            : fresh latency, occupancy latency, fresh saturation, endurance, steady-state\n'
    printf '  Note              : each suite starts from its own reset device\n'
fi
if [[ "$STORAGE" == "cns" ]]; then
    printf '\nWARNING: this formats %s and destroys all data on it.\n\n' "$BENCH_CNS_DEVICE"
else
    printf '\nWARNING: this resets the configured FEMU ZNS device and destroys its data.\n\n'
fi

sudo -v
cd "$SCRIPT_DIR"

run_suite() {
    local suite_profile="$1"
    local suite_mode="$2"
    printf '\n======================================================================\n'
    printf 'Starting suite: profile=%s, mode=%s\n' "$suite_profile" "$suite_mode"
    printf '======================================================================\n\n'
    sudo -E python3 "$SCRIPT_DIR/bench_final.py" \
        "$STORAGE" "$ROUNDS" "$suite_profile" "$SCENARIO_GROUP" "$suite_mode"
}

if [[ "$selection" == "0" ]]; then
    run_suite latency fresh
    run_suite latency occupancy
    run_suite saturation fresh
    run_suite latency endurance
    run_suite latency steady
    printf '\nAll benchmark suites completed successfully.\n'
else
    run_suite "$profile" "$workload_mode"
fi
