#!/usr/bin/env bash
# Adaptive FEMU performance sweep.  This is intentionally separate from
# run-benchmark.sh so benchmark 4 and its historical interface stay unchanged.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/performance_evaluation.py" "$@"
