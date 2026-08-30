#!/usr/bin/env bash
# Interactive launcher for the FEMU ZNS Kafka filesystem benchmark.
#
# Usage:
#   ./run-benchmark.sh          # show the menu
#   ./run-benchmark.sh 3        # run menu item 3 immediately
#   ./run-benchmark.sh --list   # print menu items only
#
# Optional environment variables:
#   BENCH_DM_IMPL=1             # 0=fixed, 1=dynamic (default: 1)
#   BENCH_ROUNDS=1              # repetitions (default: 1)
#   BENCH_SCENARIO_GROUP=baseline  # baseline, dynamic, or all
# Other BENCH_* variables documented in modify.md are passed through sudo -E.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DM_IMPL="${BENCH_DM_IMPL:-1}"
ROUNDS="${BENCH_ROUNDS:-1}"
SCENARIO_GROUP="${BENCH_SCENARIO_GROUP:-baseline}"

print_menu() {
    cat <<'EOF'
Kafka FEMU/ZNS benchmark

  1) Fresh latency       - every scenario starts from a reset device
  2) Occupancy latency   - measure at 20/40/60/80%, filesystem by filesystem
  3) Long latency        - occupancy test plus the long run at 80%
  4) Fresh saturation    - saturation profile on a reset device
  5) Occupancy saturation- saturation profile at 20/40/60/80%
  6) Long saturation     - occupancy saturation plus the long run at 80%
EOF
}

usage() {
    print_menu
    cat <<'EOF'

Usage: ./run-benchmark.sh [1-6|--list|--help]

Examples:
  ./run-benchmark.sh
  ./run-benchmark.sh 2
  BENCH_ROUNDS=3 ./run-benchmark.sh 1
  BENCH_LONG_DURATION_SECONDS=600 BENCH_LONG_WARMUP_SECONDS=60 ./run-benchmark.sh 3
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
        printf '\nSelect a test [1-6]: '
        read -r selection
        ;;
esac

case "$selection" in
    1) profile="latency";    workload_mode="fresh" ;;
    2) profile="latency";    workload_mode="occupancy" ;;
    3) profile="latency";    workload_mode="long" ;;
    4) profile="saturation"; workload_mode="fresh" ;;
    5) profile="saturation"; workload_mode="occupancy" ;;
    6) profile="saturation"; workload_mode="long" ;;
    *)
        printf 'Invalid selection: %s (expected 1-6)\n' "$selection" >&2
        exit 2
        ;;
esac

if [[ "$DM_IMPL" != "0" && "$DM_IMPL" != "1" ]]; then
    printf 'BENCH_DM_IMPL must be 0 (fixed) or 1 (dynamic).\n' >&2
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

for command in python3 sudo fio iostat vmstat dmsetup blkzone; do
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
printf '  DM implementation : %s\n' "$DM_IMPL"
printf '  Rounds            : %s\n' "$ROUNDS"
printf '  Profile           : %s\n' "$profile"
printf '  Scenario group    : %s\n' "$SCENARIO_GROUP"
printf '  Workload mode     : %s\n' "$workload_mode"
if [[ "$workload_mode" != "fresh" ]]; then
    printf '  Occupancy points  : %s\n' "${BENCH_OCCUPANCY_POINTS:-20,40,60,80}"
fi
if [[ "$workload_mode" == "long" ]]; then
    printf '  Long measurement  : %ss (warmup %ss, record %sB, %s)\n' \
        "${BENCH_LONG_DURATION_SECONDS:-3600}" \
        "${BENCH_LONG_WARMUP_SECONDS:-300}" \
        "${BENCH_LONG_RECORD_SIZE:-1024}" \
        "${BENCH_LONG_SCENARIO:-scenario_b}"
fi
printf '\nWARNING: this resets the configured FEMU ZNS device and destroys its data.\n\n'

sudo -v
cd "$SCRIPT_DIR"
exec sudo -E python3 "$SCRIPT_DIR/bench_final.py" \
    "$DM_IMPL" "$ROUNDS" "$profile" "$SCENARIO_GROUP" "$workload_mode"
