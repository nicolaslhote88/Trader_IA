#!/usr/bin/env bash
set -euo pipefail

H="${HERMES_HOME:-}"
if [[ -z "$H" || ! -f "$H/ssh/config" ]]; then
  for candidate in "${HOME:-}/.hermes" /home/hermeswebui/.hermes /home/hermes/.hermes; do
    if [[ -f "$candidate/ssh/config" ]]; then
      H="$candidate"
      break
    fi
  done
fi

if [[ -z "$H" || ! -f "$H/ssh/config" ]]; then
  echo "Hermes SSH config not found" >&2
  exit 2
fi

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
sed "s|~/.hermes|$H|g" "$H/ssh/config" > "$TMP"
chmod 600 "$TMP"

exec ssh -F "$TMP" "$@"
