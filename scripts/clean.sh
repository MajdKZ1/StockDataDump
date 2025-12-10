#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/clean.sh [--all] [--help]

Removes generated artifacts:
  - dumps/raw, dumps/arrow, dumps/manifests/*.jsonl (keep repo-owned files only)
  - python/*.egg-info
  - rust-core/target (with --all)

Options:
  --all     Also remove rust-core/target (full rebuild next time)
  -h        Show this help
EOF
}

CLEAN_TARGET=false
LOG="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/clean.log"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --all) CLEAN_TARGET=true; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown option: $1" >&2; usage; exit 1;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Logging to $LOG"
{
echo "==== clean $(date -Iseconds) ===="
echo "-> removing dumps/raw dumps/arrow"
rm -rf "$ROOT_DIR/dumps/raw" "$ROOT_DIR/dumps/arrow"

echo "-> removing generated manifests (keeping repo-owned files)"
find "$ROOT_DIR/dumps/manifests" -maxdepth 1 -type f -name '*.jsonl' ! -name 'README*' -delete 2>/dev/null || true

echo "-> removing python egg-info"
rm -rf "$ROOT_DIR/python/"*.egg-info

if "$CLEAN_TARGET"; then
  echo "-> removing rust-core/target"
  rm -rf "$ROOT_DIR/rust-core/target"
fi

echo "clean complete"
} >>"$LOG" 2>&1 || { echo "clean failed. See $LOG" >&2; exit 1; }
echo "Clean complete. See log at $LOG"
