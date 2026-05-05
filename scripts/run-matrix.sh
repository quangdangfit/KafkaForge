#!/usr/bin/env bash
# Runs the publisher with each profile and prints a side-by-side summary.
# A consumer should already be running against the same topic; this script
# only stresses the producer side.
set -euo pipefail

PROFILES=("baseline" "batch-lz4" "batch-zstd")
COUNT="${COUNT:-200000}"
LOG_DIR="${LOG_DIR:-.matrix-logs}"
mkdir -p "$LOG_DIR"

for p in "${PROFILES[@]}"; do
  log="$LOG_DIR/${p}.log"
  echo "=== profile: $p (count=$COUNT) ==="
  go run ./cmd/publisher --profile "configs/${p}.yaml" --count "$COUNT" >"$log" 2>&1
  # Pull the final summary line (it embeds final p50/p95/p99/rps).
  grep -E '"summary":' "$log" | tail -1 | sed -E 's/.*"summary":"([^"]+)".*/  \1/'
done

echo
echo "full logs in $LOG_DIR/"
