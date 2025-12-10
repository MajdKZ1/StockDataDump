#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/install.sh [--venv PATH] [--help]

Sets up the project:
  - Detects Linux/macOS
  - Creates a Python venv (default: .venv)
  - Installs Python deps (editable) and builds Rust core (release)
  - Installs a global shim at ~/.local/bin/stockdatadump (or /usr/local/bin if writable)

Options:
  --venv PATH   Override venv directory (default: .venv)
  -h, --help    Show this help
EOF
}

VENV_DIR=".venv"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --venv)
      VENV_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
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

LOG="$ROOT_DIR/install.log"
echo "Logging to $LOG"
{
echo "==== install $(date -Iseconds) ===="

echo "-> creating venv at $VENV_PATH"
python3 -m venv "$VENV_PATH"
VENV_PYTHON="$VENV_PATH/bin/python"
VENV_PIP="$VENV_PATH/bin/pip"

echo "-> upgrading pip/setuptools/wheel"
"$VENV_PIP" install --upgrade pip setuptools wheel

echo "-> installing Python package (editable)"
cd "$ROOT_DIR/python"
"$VENV_PIP" install -e .

echo "-> building Rust core (release)"
cd "$ROOT_DIR/rust-core"
cargo build --release

BIN_CANDIDATES=("$HOME/.local/bin" "/usr/local/bin")
SHIM_PATH=""
for dir in "${BIN_CANDIDATES[@]}"; do
  if [[ -d "$dir" && -w "$dir" ]]; then
    SHIM_PATH="$dir/stockdatadump"
    break
  fi
done
if [[ -z "$SHIM_PATH" ]]; then
  echo "No writable bin dir found in ${BIN_CANDIDATES[*]}; not installing shim." >&2
  echo "You can run with: $VENV_PYTHON -m stockdatadump.app" >&2
  exit 0
fi

echo "-> writing shim to $SHIM_PATH"
cat > "$SHIM_PATH" <<EOF
#!/usr/bin/env bash
exec "$VENV_PYTHON" -m stockdatadump.app "\$@"
EOF
chmod +x "$SHIM_PATH"

if ! echo "$PATH" | tr ":" "\n" | grep -q "^$(dirname "$SHIM_PATH")\$"; then
  echo "Note: add $(dirname "$SHIM_PATH") to your PATH."
fi

echo "Done. Try: stockdatadump --help"
} >>"$LOG" 2>&1 || { echo "install failed. See $LOG" >&2; exit 1; }
echo "Install complete. See log at $LOG"
