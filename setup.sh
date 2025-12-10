#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$ROOT_DIR/setup.log"

install_script="$ROOT_DIR/scripts/install.sh"
clean_script="$ROOT_DIR/scripts/clean.sh"
update_script="$ROOT_DIR/scripts/update.sh"

if [[ ! -x "$install_script" || ! -x "$clean_script" || ! -x "$update_script" ]]; then
  chmod +x "$install_script" "$clean_script" "$update_script" >/dev/null 2>&1 || true
fi

echo "Logging to $LOG"
while true; do
  cat <<'MENU'
StockDataDump Setup
1) Install
2) Clean
3) Update
4) Quit
MENU
  read -rp "Choose option [1-4]: " choice
  case "$choice" in
    1)
      echo "[$(date -Iseconds)] install" >>"$LOG"
      if "$install_script"; then
        echo "install ok" >>"$LOG"
      else
        echo "install failed (see install.log)" | tee -a "$LOG"
      fi
      ;;
    2)
      echo "[$(date -Iseconds)] clean" >>"$LOG"
      if "$clean_script"; then
        echo "clean ok" >>"$LOG"
      else
        echo "clean failed (see clean.log)" | tee -a "$LOG"
      fi
      ;;
    3)
      echo "[$(date -Iseconds)] update" >>"$LOG"
      if "$update_script"; then
        echo "update ok" >>"$LOG"
      else
        echo "update failed (see update.log)" | tee -a "$LOG"
      fi
      ;;
    4) exit 0;;
    *) echo "Invalid choice";;
  esac
done
