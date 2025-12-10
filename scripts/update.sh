#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/update.sh [--venv PATH] [--help]

Rebuilds project artifacts:
  - Ensures venv (default .venv) exists; creates if missing
  - Re-installs Python package (editable)
  - Builds Rust core (release)

Options:
  --venv PATH   Override venv directory (default: .venv)
  -h, --help    Show this help
EOF
}

VENV_DIR=".venv"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --venv) VENV_DIR="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown option: $1" >&2; usage; exit 1;;
  esac
done

OS="$(uname -s || true)"
case "$OS" in
  Linux|Darwin) ;;
  *) echo "Unsupported OS: $OS. Use Linux or macOS." >&2; exit 1;;
esac

need_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need_cmd python3; need_cmd pip; need_cmd cargo

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PATH="$ROOT_DIR/$VENV_DIR"
LOG="$ROOT_DIR/update.log"

echo "Logging to $LOG"
{
echo "==== update $(date -Iseconds) ===="
if [[ ! -d "$VENV_PATH" ]]; then
  echo "-> venv missing; creating at $VENV_PATH"
  python3 -m venv "$VENV_PATH"
fi

VENV_PYTHON="$VENV_PATH/bin/python"
VENV_PIP="$VENV_PATH/bin/pip"

echo "-> upgrading pip/setuptools/wheel"
"$VENV_PIP" install --upgrade pip setuptools wheel

echo "-> reinstalling Python package (editable)"
cd "$ROOT_DIR/python"
"$VENV_PIP" install -e .

echo "-> rebuilding Rust core (release)"
cd "$ROOT_DIR/rust-core"
cargo build --release

echo "update complete"
} >>"$LOG" 2>&1 || { echo "update failed. See $LOG" >&2; exit 1; }
echo "Update complete. See log at $LOG"
