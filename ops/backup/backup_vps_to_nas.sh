#!/usr/bin/env bash
# =============================================================================
# backup_vps_to_nas.sh
# Snapshot applicatif complet du VPS srv961978 -> NAS Synology
# Transport : montage SMB/CIFS du partage "VPS Sauvegarde" via Tailscale,
#             avec le compte DSM codex_nas (deja autorise en Lecture/Ecriture).
#
# Usage (sur le VPS, en root) :
#   export TS=$(date -u +%Y%m%dT%H%M%SZ)   # fige l'horodatage pour toutes les etapes
#   bash backup_vps_to_nas.sh preflight    # 1) controles, ne touche a rien
#   bash backup_vps_to_nas.sh stop         # 2) arret propre des writers
#   bash backup_vps_to_nas.sh dump         # 3) archives + sha256 en staging local
#   bash backup_vps_to_nas.sh start        # 4) redemarrage (le push n'en depend pas)
#   bash backup_vps_to_nas.sh push         # 5) copie vers le NAS + verification
#
# Chaque etape est independante et rejouable.
# Le mot de passe SMB n'est jamais lu ni affiche par ce script : il vit dans
# /root/.smb-nas-credentials (chmod 600), cree manuellement par Nicolas.
# =============================================================================
set -euo pipefail

# --- Cible NAS (surchargeable par variables d'environnement) ---
NAS_ADDR="${NAS_ADDR:-100.110.120.73}"              # nico-synology, IP Tailscale
NAS_SHARE_NAME="${NAS_SHARE_NAME:-VPS Sauvegarde}"  # ATTENTION : espace dans le nom
NAS_CREDS="${NAS_CREDS:-/root/.smb-nas-credentials}"
NAS_MNT="${NAS_MNT:-/mnt/nas-vps-sauvegarde}"
NAS_SUBDIR="${NAS_SUBDIR:-srv961978}"
CIFS_VERS="${CIFS_VERS:-3.0}"                       # DSM 6.x : 3.0 ; fallback 2.1 puis 1.0

STAGE_ROOT="${STAGE_ROOT:-/var/backups/vps-snapshot}"
TS="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
STAGE="$STAGE_ROOT/$TS"
DEST="$NAS_MNT/$NAS_SUBDIR/$TS"

# Conteneurs a arreter pour figer /local-files, /opt et les volumes Docker
# archives. NOTE: ibkr-gateway et ibkr-broker sont volontairement EXCLUS : les
# arreter casse la session IBKR et impose un relogin manuel LIVE + 2FA.
STOP_PATTERNS='^(root-n8n-1|root-task-runners-.*|root-trading-dashboard-1|macro-data-api|global-context-synthesizer|ag5ag9-.*|siga-dashboard|yf-enrichment|yfinance-api|hermes-webui-.*|visual-studio-code-server-.*|portainer|root-traefik-1)$'

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() { printf '[ERREUR] %s\n' "$*" >&2; exit 1; }

# -----------------------------------------------------------------------------
nas_mount() {
  if mountpoint -q "$NAS_MNT"; then log "NAS deja monte sur $NAS_MNT"; return 0; fi
  mkdir -p "$NAS_MNT"
  [ -f "$NAS_CREDS" ] || die "fichier d'identifiants absent : $NAS_CREDS (voir §1bis du runbook)"
  local perms; perms="$(stat -c '%a' "$NAS_CREDS")"
  [ "$perms" = "600" ] || die "$NAS_CREDS doit etre en 600 (actuel : $perms) — chmod 600 $NAS_CREDS"

  log "montage //$NAS_ADDR/$NAS_SHARE_NAME (SMB $CIFS_VERS)"
  mount -t cifs "//$NAS_ADDR/$NAS_SHARE_NAME" "$NAS_MNT" \
    -o "credentials=$NAS_CREDS,vers=$CIFS_VERS,uid=0,gid=0,file_mode=0600,dir_mode=0700,iocharset=utf8,noserverino,soft" \
    || die "montage CIFS KO. Pistes : partage chiffre DEMONTE cote DSM ; essayer CIFS_VERS=2.1 ou 1.0 ; verifier SMB active dans DSM > Services de fichiers."
  mountpoint -q "$NAS_MNT" || die "montage silencieusement echoue"
  log "monte OK"
}

nas_umount() {
  if mountpoint -q "$NAS_MNT"; then umount "$NAS_MNT" && log "NAS demonte"; fi
}

# -----------------------------------------------------------------------------
preflight() {
  log "== PREFLIGHT =="
  [ "$(id -u)" -eq 0 ] || die "doit tourner en root"

  log "-- Espace disque VPS (besoin ~= 2x la taille des donnees) --"
  df -h /
  echo
  log "-- Taille des sources --"
  for p in /docker /opt /root /local-files /var/lib/docker/volumes; do
    [ -e "$p" ] && du -sh "$p" 2>/dev/null || echo "  (absent) $p"
  done
  echo
  log "-- Volumes Docker --"
  docker volume ls
  echo
  log "-- Conteneurs --"
  docker ps --format 'table {{.Names}}\t{{.Status}}'
  echo
  log "-- Tailscale --"
  tailscale status 2>/dev/null || die "tailscale non installe / non connecte sur le VPS"
  ping -c 2 -W 3 "$NAS_ADDR" || die "NAS injoignable via Tailscale"
  echo
  log "-- Client CIFS --"
  command -v mount.cifs >/dev/null \
    || die "cifs-utils absent : apt-get update && apt-get install -y cifs-utils"
  echo
  log "-- Montage du partage NAS --"
  nas_mount
  log "-- Test ecriture --"
  mkdir -p "$NAS_MNT/$NAS_SUBDIR" || die "creation de $NAS_SUBDIR impossible sur le partage"
  local probe="$NAS_MNT/$NAS_SUBDIR/.write_test_$$"
  echo ok > "$probe" && rm -f "$probe" && log "ECRITURE OK"
  df -h "$NAS_MNT"
  nas_umount
  log "PREFLIGHT OK"
}

# -----------------------------------------------------------------------------
stop_writers() {
  log "== ARRET DES WRITERS =="
  mkdir -p "$STAGE"
  docker ps --format '{{.Names}}' | grep -E "$STOP_PATTERNS" > "$STAGE/stopped_containers.txt" || true
  if [ -s "$STAGE/stopped_containers.txt" ]; then
    log "arret de : $(tr '\n' ' ' < "$STAGE/stopped_containers.txt")"
    xargs -r docker stop -t 60 < "$STAGE/stopped_containers.txt"
  else
    log "aucun conteneur correspondant en cours d'execution"
  fi
  sleep 5
  log "-- Fichiers WAL residuels (doivent etre stables) --"
  ls -lh /var/lib/docker/volumes/n8n_data/_data/database.sqlite* 2>/dev/null || true
  ls -lh /local-files/duckdb/*.wal 2>/dev/null || echo "  aucun .wal DuckDB (bon signe)"
  log "ARRET OK"
}

# -----------------------------------------------------------------------------
dump() {
  log "== DUMP EN STAGING : $STAGE =="
  mkdir -p "$STAGE"

  # --- Metadonnees systeme (indispensables pour reconstruire) ---
  local M="$STAGE/metadata"; mkdir -p "$M"
  crontab -l                                > "$M/crontab_root.txt" 2>/dev/null || true
  docker ps -a                              > "$M/docker_ps_a.txt"
  docker images                             > "$M/docker_images.txt"
  docker volume ls                          > "$M/docker_volumes.txt"
  docker network ls                         > "$M/docker_networks.txt"
  docker inspect $(docker ps -aq)           > "$M/docker_inspect_all.json" 2>/dev/null || true
  dpkg -l                                   > "$M/dpkg_list.txt" 2>/dev/null || true
  systemctl list-unit-files --state=enabled > "$M/systemd_enabled.txt" 2>/dev/null || true
  ip a                                      > "$M/ip_a.txt"
  ufw status verbose                        > "$M/ufw.txt" 2>/dev/null || true
  df -h                                     > "$M/df.txt"
  uname -a                                  > "$M/uname.txt"
  cp -a /etc/fstab "$M/fstab" 2>/dev/null || true
  cp -a /etc/hosts "$M/hosts" 2>/dev/null || true
  tar -czf "$STAGE/00_metadata.tar.gz" -C "$STAGE" metadata && rm -rf "$M"

  # --- Lots applicatifs ---
  # tar preserve owner/permissions, que CIFS ne sait pas restituer.
  archive() {  # archive <nom> <chemin_parent> <cible...>
    local name="$1"; shift
    local parent="$1"; shift
    if [ ! -e "$parent/$1" ]; then log "  (absent, ignore) $parent/$1"; return 0; fi
    log "  -> $name"
    tar --numeric-owner -czf "$STAGE/$name.tar.gz" -C "$parent" "$@"
  }

  archive 01_docker_configs   /        docker          # compose + .env de tous les stacks
  archive 02_opt              /        opt             # services ibkr-gateway / ibkr-broker
  archive 03_root             /        root            # scripts, historique
  archive 04_local_files      /        local-files     # bases DuckDB
  archive 05_etc              /        etc             # confs systeme (nginx, systemd, tailscale)
  # Le volume yfinance_ibeam_outputs appartient a ibkr-gateway, maintenu actif
  # pour ne pas casser la session LIVE. Il ne contient que son runtime de
  # session, recree apres relogin, et n'est donc pas archive. Les sockets Unix
  # code-server sont egalement ephemeres et non restaurables.
  log "  -> 06_docker_volumes"
  tar --numeric-owner \
    --exclude='volumes/yfinance_ibeam_outputs' \
    --exclude='*.sock' \
    -czf "$STAGE/06_docker_volumes.tar.gz" \
    -C /var/lib/docker volumes

  # --- Images Docker construites localement (non repullables depuis un registre) ---
  local LOCAL_IMGS
  LOCAL_IMGS="$(docker images --format '{{.Repository}}:{{.Tag}}' \
                 | grep -Ei 'ibkr|trading|yfinance|trader' | grep -v '<none>' || true)"
  if [ -n "$LOCAL_IMGS" ]; then
    log "  -> 07_docker_images ($(echo "$LOCAL_IMGS" | tr '\n' ' '))"
    # shellcheck disable=SC2086
    docker save $LOCAL_IMGS | gzip > "$STAGE/07_docker_images.tar.gz"
  fi

  # --- Empreintes + manifeste ---
  ( cd "$STAGE" && sha256sum ./*.tar.gz > SHA256SUMS )
  { echo "snapshot   : $TS"; echo "hote       : $(hostname)"; date -u; echo;
    du -sh "$STAGE"/*.tar.gz; } > "$STAGE/MANIFEST.txt"
  log "DUMP OK — taille totale : $(du -sh "$STAGE" | cut -f1)"
  cat "$STAGE/MANIFEST.txt"
}

# -----------------------------------------------------------------------------
push() {
  log "== COPIE VERS LE NAS =="
  [ -d "$STAGE" ] || die "staging introuvable : $STAGE (exporter le meme TS que pour 'dump')"
  nas_mount
  trap nas_umount EXIT

  mkdir -p "$DEST"
  # Pas de -a : CIFS ne restitue ni owner ni permissions POSIX. Sans importance,
  # les droits reels sont conserves DANS les archives tar.
  # --partial : reprise apres coupure, il suffit de relancer 'push'.
  rsync -rvh --progress --partial --size-only \
    --exclude 'stopped_containers.txt' \
    "$STAGE"/ "$DEST"/ \
    || die "copie interrompue — relancer 'push' (reprise automatique)"

  log "-- Verification des empreintes (relecture depuis le NAS) --"
  # Verification de bout en bout : les fichiers sont relus a travers le reseau.
  ( cd "$DEST" && sha256sum -c SHA256SUMS ) \
    || die "CHECKSUMS KO — NE PAS supprimer le staging local, relancer 'push'"

  log "-- Contenu depose --"
  ls -lh "$DEST"
  log "COPIE VERIFIEE OK -> $NAS_SHARE_NAME/$NAS_SUBDIR/$TS"
}

# -----------------------------------------------------------------------------
start_writers() {
  log "== REDEMARRAGE =="
  if [ -s "$STAGE/stopped_containers.txt" ]; then
    xargs -r docker start < "$STAGE/stopped_containers.txt"
  else
    log "aucune liste de conteneurs arretes trouvee dans $STAGE"
  fi
  sleep 10
  docker ps --format 'table {{.Names}}\t{{.Status}}'
  log "Verifier ensuite : sante n8n, prochain run planifie, /health broker IBKR"
}

# -----------------------------------------------------------------------------
case "${1:-}" in
  preflight) preflight ;;
  stop)      stop_writers ;;
  dump)      dump ;;
  push)      push ;;
  start)     start_writers ;;
  mount)     nas_mount ;;
  umount)    nas_umount ;;
  *) die "usage: $0 {preflight|stop|dump|start|push|mount|umount}   (exporter TS=$TS pour rejouer une etape sur le meme snapshot)" ;;
esac
