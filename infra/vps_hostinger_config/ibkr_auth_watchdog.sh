#!/usr/bin/env bash
set -euo pipefail

BROKER_URL="${IBKR_BROKER_URL:-http://127.0.0.1:18080}"
GATEWAY_CONTAINER="${IBKR_GATEWAY_CONTAINER:-ibkr-gateway}"
BROKER_CONTAINER="${IBKR_BROKER_CONTAINER:-ibkr-broker}"
LOG_FILE="${IBKR_AUTH_WATCHDOG_LOG:-/var/log/ibkr_auth_watchdog.log}"
STATE_DIR="${IBKR_AUTH_WATCHDOG_STATE_DIR:-/var/run/ibkr-auth-watchdog}"
RESTART_COOLDOWN_SECONDS="${IBKR_AUTH_RESTART_COOLDOWN_SECONDS:-1800}"
FAILED_LOGIN_COOLDOWN_SECONDS="${IBKR_FAILED_LOGIN_COOLDOWN_SECONDS:-21600}"
POST_RESTART_WAIT_SECONDS="${IBKR_POST_RESTART_WAIT_SECONDS:-150}"
IBEAM_OUTPUT_DIR="${IBEAM_OUTPUT_DIR:-/var/lib/docker/volumes/yfinance_ibeam_outputs/_data}"

mkdir -p "$STATE_DIR" "$(dirname "$LOG_FILE")"

log() {
  printf '%s %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" | tee -a "$LOG_FILE" >/dev/null
}

json_get() {
  python3 - "$1" "$2" <<'PY'
import json, sys
payload = json.loads(sys.argv[1] or "{}")
path = sys.argv[2].split(".")
cur = payload
for part in path:
    if isinstance(cur, dict):
        cur = cur.get(part)
    else:
        cur = None
        break
if isinstance(cur, (dict, list)):
    print(json.dumps(cur))
elif cur is None:
    print("")
else:
    print(str(cur).lower() if isinstance(cur, bool) else str(cur))
PY
}

now_epoch() {
  date +%s
}

age_ok() {
  local stamp_file="$1"
  local cooldown="$2"
  if [[ ! -f "$stamp_file" ]]; then
    return 0
  fi
  local last
  last="$(cat "$stamp_file" 2>/dev/null || echo 0)"
  [[ $(( $(now_epoch) - last )) -ge "$cooldown" ]]
}

recent_invalid_login() {
  [[ -d "$IBEAM_OUTPUT_DIR" ]] || return 1
  while IFS= read -r -d '' file; do
    if grep -iq 'Invalid username password combination' "$file"; then
      return 0
    fi
  done < <(find "$IBEAM_OUTPUT_DIR" -maxdepth 1 -type f -name 'ibeam_log__*.txt' -mmin "-$((FAILED_LOGIN_COOLDOWN_SECONDS / 60))" -print0 2>/dev/null)
  return 1
}

health="$(curl -fsS --max-time 12 "$BROKER_URL/health" || true)"
if [[ -z "$health" ]]; then
  log "broker_health_unreachable; restarting $BROKER_CONTAINER"
  docker restart "$BROKER_CONTAINER" >/dev/null
  exit 0
fi

authenticated="$(json_get "$health" "authenticated")"
connected="$(json_get "$health" "ibkr_status.connected")"
manual_required="$(json_get "$health" "session_monitor.manual_login_required")"
manual_reason="$(json_get "$health" "session_monitor.operator_action.reason")"

if [[ "$authenticated" == "true" ]]; then
  curl -fsS --max-time 12 -X POST "$BROKER_URL/auth/tickle" >/dev/null || true
  log "ok authenticated; tickle sent"
  exit 0
fi

if [[ "$connected" == "true" ]]; then
  log "connected_not_authenticated; calling auth/recover"
  curl -fsS --max-time 25 -X POST "$BROKER_URL/auth/recover" >/dev/null || true
  exit 0
fi

restart_stamp="$STATE_DIR/last_gateway_restart"
if ! age_ok "$restart_stamp" "$RESTART_COOLDOWN_SECONDS"; then
  log "gateway_disconnected; restart skipped by cooldown reason=${manual_reason:-unknown} manual=${manual_required:-unknown}"
  exit 0
fi

if recent_invalid_login; then
  log "gateway_disconnected; restart skipped because recent IBeam invalid-credentials errors were detected"
  exit 0
fi

date +%s > "$restart_stamp"
log "gateway_disconnected; restarting $GATEWAY_CONTAINER reason=${manual_reason:-unknown} manual=${manual_required:-unknown}"
docker restart "$GATEWAY_CONTAINER" >/dev/null
sleep "$POST_RESTART_WAIT_SECONDS"
curl -fsS --max-time 25 -X POST "$BROKER_URL/auth/recover" >/dev/null || true
post_health="$(curl -fsS --max-time 12 "$BROKER_URL/health" || true)"
post_authenticated="$(json_get "$post_health" "authenticated")"
post_connected="$(json_get "$post_health" "ibkr_status.connected")"
log "post_restart authenticated=${post_authenticated:-unknown} connected=${post_connected:-unknown}"

if [[ "$post_authenticated" == "true" ]]; then
  docker restart "$BROKER_CONTAINER" >/dev/null || true
  log "broker restarted after successful gateway auth to clear stale monitor state"
fi
