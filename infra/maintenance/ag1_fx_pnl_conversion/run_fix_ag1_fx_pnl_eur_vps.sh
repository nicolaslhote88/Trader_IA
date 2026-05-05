#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python "$SCRIPT_DIR/fix_ag1_fx_pnl_eur.py" \
  --yfinance-api "${YFINANCE_API_URL:-http://yfinance-api:8080}" \
  "$@"
