#!/usr/bin/env bash
# Orchestre une sauvegarde complete unique avec redemarrage garanti des writers.
set -Eeuo pipefail

BACKUP_SCRIPT="${BACKUP_SCRIPT:-/root/backup_vps_to_nas.sh}"
LOG_DIR="${LOG_DIR:-/var/log/vps-backup-to-nas}"
TS="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
export TS

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/$TS.log"
exec > >(tee -a "$LOG_FILE") 2>&1

writers_stopped=0

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [ "$writers_stopped" -eq 1 ]; then
    echo "[SECURITE] Echec/interruption : redemarrage des writers"
    bash "$BACKUP_SCRIPT" start || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

echo "[$(date -u +%FT%TZ)] Debut sauvegarde $TS"
bash "$BACKUP_SCRIPT" preflight
bash "$BACKUP_SCRIPT" stop
writers_stopped=1
bash "$BACKUP_SCRIPT" dump
bash "$BACKUP_SCRIPT" start
writers_stopped=0
bash "$BACKUP_SCRIPT" push

stage="/var/backups/vps-snapshot/$TS"
if ! tar -tzf "$stage/06_docker_volumes.tar.gz" \
       'volumes/n8n_data/_data/config' >/dev/null 2>&1 \
   && ! tar -tzf "$stage/06_docker_volumes.tar.gz" \
       'volumes/root_n8n_data/_data/config' >/dev/null 2>&1; then
  echo '[ERREUR] Cle de chiffrement n8n absente de 06_docker_volumes.tar.gz' >&2
  exit 1
fi

docker inspect --format '{{.State.Status}}' root-n8n-1 | grep -qx running
docker inspect --format '{{.State.Health.Status}}' ibkr-broker | grep -qx healthy
docker inspect --format '{{.State.Health.Status}}' ibkr-gateway | grep -qx healthy

echo "[$(date -u +%FT%TZ)] Sauvegarde $TS terminee et verifiee"
echo "Staging conserve : $stage"
echo "Journal : $LOG_FILE"
