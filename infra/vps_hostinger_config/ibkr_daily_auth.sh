#!/usr/bin/env bash
set -euo pipefail

BROKER_URL="${IBKR_BROKER_URL:-http://127.0.0.1:18080}"
GATEWAY_CONTAINER="${IBKR_GATEWAY_CONTAINER:-ibkr-gateway}"
TIMEZONE="${IBKR_AUTH_TIMEZONE:-Europe/Paris}"
WINDOW_START_MINUTE="${IBKR_AUTH_WINDOW_START_MINUTE:-420}"
WINDOW_END_MINUTE="${IBKR_AUTH_WINDOW_END_MINUTE:-450}"
ATTEMPT_OFFSETS_MINUTES="${IBKR_AUTH_ATTEMPT_OFFSETS_MINUTES:-0 10 20}"
ATTEMPT_TIMEOUT_SECONDS="${IBKR_AUTH_ATTEMPT_TIMEOUT_SECONDS:-510}"
POST_AUTH_POLL_SECONDS="${IBKR_AUTH_POST_AUTH_POLL_SECONDS:-90}"
LOG_FILE="${IBKR_DAILY_AUTH_LOG:-/var/log/ibkr_daily_auth.log}"
IBEAM_LOG_FILE="${IBKR_DAILY_AUTH_IBEAM_LOG:-/var/log/ibkr_daily_auth_ibeam.log}"
STATE_DIR="${IBKR_DAILY_AUTH_STATE_DIR:-/var/lib/ibkr-daily-auth}"
DRY_RUN="${IBKR_AUTH_DRY_RUN:-false}"
NOW_OVERRIDE="${IBKR_AUTH_NOW_PARIS:-}"

mkdir -p "$STATE_DIR" "$(dirname "$LOG_FILE")" "$(dirname "$IBEAM_LOG_FILE")"
touch "$LOG_FILE" "$IBEAM_LOG_FILE"
chmod 600 "$LOG_FILE" "$IBEAM_LOG_FILE"

exec 9>"$STATE_DIR/run.lock"
if ! flock -n 9; then
  exit 0
fi

log() {
  printf '%s %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >>"$LOG_FILE"
}

paris_value() {
  local format="$1"
  if [[ -n "$NOW_OVERRIDE" ]]; then
    case "$format" in
      +%H%M) printf '%s\n' "$NOW_OVERRIDE" ;;
      +%F) printf '%s\n' "${IBKR_AUTH_DATE_PARIS:-$(TZ="$TIMEZONE" date +%F)}" ;;
      *) TZ="$TIMEZONE" date "$format" ;;
    esac
  else
    TZ="$TIMEZONE" date "$format"
  fi
}

minute_of_day() {
  local hhmm hour minute
  hhmm="$(paris_value +%H%M)"
  hour=$((10#${hhmm:0:2}))
  minute=$((10#${hhmm:2:2}))
  printf '%s\n' $((hour * 60 + minute))
}

json_authenticated() {
  python3 -c 'import json,sys; print("true" if json.load(sys.stdin).get("authenticated") is True else "false")'
}

is_authenticated() {
  local health authenticated
  health="$(curl -fsS --max-time 12 "$BROKER_URL/health" 2>/dev/null || true)"
  [[ -n "$health" ]] || return 1
  authenticated="$(printf '%s' "$health" | json_authenticated 2>/dev/null || printf 'false')"
  [[ "$authenticated" == "true" ]]
}

wait_until_offset() {
  local offset="$1" target current sleep_seconds
  target=$((WINDOW_START_MINUTE + offset))
  current="$(minute_of_day)"
  if (( current > target )); then
    return 1
  fi
  sleep_seconds=$(((target - current) * 60))
  if (( sleep_seconds > 0 )); then
    sleep "$sleep_seconds"
  fi
}

poll_after_auth() {
  local deadline
  curl -fsS --max-time 25 -X POST "$BROKER_URL/auth/recover" >/dev/null 2>&1 || true
  deadline=$((SECONDS + POST_AUTH_POLL_SECONDS))
  while (( SECONDS < deadline )); do
    if is_authenticated; then
      return 0
    fi
    sleep 5
  done
  return 1
}

current_minute="$(minute_of_day)"
if (( current_minute < WINDOW_START_MINUTE || current_minute >= WINDOW_END_MINUTE )); then
  log "blocked_outside_window timezone=$TIMEZONE minute=$current_minute"
  exit 0
fi

paris_date="$(paris_value +%F)"
run_stamp="$STATE_DIR/$paris_date.started"
if [[ -e "$run_stamp" ]]; then
  log "skipped_already_started date=$paris_date"
  exit 0
fi
touch "$run_stamp"
chmod 600 "$run_stamp"

if is_authenticated; then
  log "already_authenticated date=$paris_date"
  exit 0
fi

if [[ "$DRY_RUN" == "true" ]]; then
  log "dry_run date=$paris_date offsets=${ATTEMPT_OFFSETS_MINUTES// /,}"
  rm -f "$run_stamp"
  exit 0
fi

attempt=0
for offset in $ATTEMPT_OFFSETS_MINUTES; do
  if ! wait_until_offset "$offset"; then
    continue
  fi

  current_minute="$(minute_of_day)"
  if (( current_minute >= WINDOW_END_MINUTE )); then
    break
  fi

  if is_authenticated; then
    log "authenticated_before_attempt date=$paris_date attempt=$attempt"
    exit 0
  fi

  attempt=$((attempt + 1))
  log "ibeam_attempt_started date=$paris_date attempt=$attempt offset_min=$offset"

  docker exec "$GATEWAY_CONTAINER" mkdir -p /tmp/ibeam-empty-inputs >/dev/null 2>&1 || true

  docker exec \
    -e IBEAM_LOG_LEVEL=WARNING \
    -e IBEAM_LOG_TO_FILE=True \
    -e IBEAM_ERROR_SCREENSHOTS=True \
    -e IBEAM_INPUTS_DIR=/tmp/ibeam-empty-inputs \
    -e IBEAM_HEALTH_SERVER_PORT=5101 \
    -e IBEAM_OAUTH_TIMEOUT=480 \
    -e IBEAM_MAX_IMMEDIATE_ATTEMPTS=1 \
    -e IBEAM_REQUEST_RETRIES=1 \
    "$GATEWAY_CONTAINER" \
    timeout --signal=TERM --kill-after=15s "$ATTEMPT_TIMEOUT_SECONDS" \
    python /srv/ibeam/ibeam_starter.py --authenticate \
    >>"$IBEAM_LOG_FILE" 2>&1 || true

  if poll_after_auth; then
    log "authentication_succeeded date=$paris_date attempt=$attempt"
    exit 0
  fi

  log "authentication_not_confirmed date=$paris_date attempt=$attempt"
done

log "window_finished_without_authentication date=$paris_date attempts=$attempt"
exit 0
