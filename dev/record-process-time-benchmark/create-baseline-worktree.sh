#!/usr/bin/env bash
# Step 1: checks out the baseline (the parent commit of "Add record processing times", which has
# no record process time code) as a git worktree next to this repo. Does nothing if it exists.
#
#   BASELINE_COMMIT   e6252155daa by default
#   BASELINE_DIR      ../spark-baseline (next to this repo) by default
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BASELINE_COMMIT="${BASELINE_COMMIT:-e6252155daa}"
BASELINE_DIR="${BASELINE_DIR:-$(dirname "$REPO")/spark-baseline}"

if [ -d "$BASELINE_DIR" ]; then
  echo "$BASELINE_DIR already exists, at $(git -C "$BASELINE_DIR" log --oneline -1)"
else
  git -C "$REPO" worktree add --detach "$BASELINE_DIR" "$BASELINE_COMMIT"
fi
