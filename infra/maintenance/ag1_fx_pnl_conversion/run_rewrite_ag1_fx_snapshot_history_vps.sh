#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "ERROR: python3/python introuvable. Installe Python 3 ou definis PYTHON_BIN=/chemin/vers/python." >&2
    exit 127
  fi
fi

"$PYTHON_BIN" "$SCRIPT_DIR/rewrite_ag1_fx_snapshot_history.py" \
  --yfinance-api "${YFINANCE_API_URL:-http://yfinance-api:8080}" \
  "$@"
