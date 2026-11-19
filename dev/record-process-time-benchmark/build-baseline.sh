#!/usr/bin/env bash
# Step 2: copies the benchmark from this repo into the baseline worktree, builds it, and writes
# its test classpath to $BASELINE_DIR/core-test-classpath.txt, where the benchmark looks for it.
# Re-run it after changing the benchmark. The first build takes ~15-20 minutes.
#
#   BASELINE_DIR      ../spark-baseline (next to this repo) by default
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BASELINE_DIR="${BASELINE_DIR:-$(dirname "$REPO")/spark-baseline}"
BENCHMARK="core/src/test/scala/org/apache/spark/shuffle/ShuffleReadRecordProcessTimeBenchmark.scala"
LOG="$BASELINE_DIR/build-baseline.log"
OUT="$BASELINE_DIR/core-test-classpath.txt"

[ -d "$BASELINE_DIR" ] || { echo "No $BASELINE_DIR, run create-baseline-worktree.sh first"; exit 1; }
cp "$REPO/$BENCHMARK" "$BASELINE_DIR/$BENCHMARK"

echo "Building $BASELINE_DIR, log in $LOG"
(cd "$BASELINE_DIR" && build/sbt "export core/Test/fullClasspath") > "$LOG" 2>&1 || {
  echo "Build failed, see $LOG"; exit 1; }
grep -v '^\[' "$LOG" | grep 'test-classes' | tail -1 > "$OUT"
[ -s "$OUT" ] || { echo "No classpath found in $LOG"; exit 1; }
echo "Wrote $OUT"
