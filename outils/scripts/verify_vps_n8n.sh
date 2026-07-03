#!/usr/bin/env bash
# =============================================================================
# verify_vps_n8n.sh — Vérifie sur le VPS que le runbook d'investigation n8n
# (docs/operations/runbook_n8n_investigation.md) et AGENTS.md collent à la réalité.
#
# Lance ce script SUR le VPS, ou depuis ton poste Windows (PowerShell) avec :
#     Get-Content "D:\N8N\Assistant IA complet\Trader_IA\outils\scripts\verify_vps_n8n.sh" -Raw | ssh vps "bash -s"
# ou depuis un shell POSIX :
#     ssh vps 'bash -s' < outils/scripts/verify_vps_n8n.sh
#
# Il ne MODIFIE rien (lectures seules). Copie-colle toute la sortie à Claude.
# =============================================================================
set -uo pipefail

PASS=0; FAIL=0; WARN=0
ok()   { echo "  [PASS] $*"; PASS=$((PASS+1)); }
ko()   { echo "  [FAIL] $*"; FAIL=$((FAIL+1)); }
warn() { echo "  [WARN] $*"; WARN=$((WARN+1)); }
hr()   { echo "-------------------------------------------------------------------"; }

echo "==================================================================="
echo " VERIFICATION VPS / n8n — $(date -u '+%Y-%m-%d %H:%M:%SZ')"
echo " host: $(hostname)  user: $(whoami)"
echo "==================================================================="

# --- 1. Docker dispo ---------------------------------------------------------
hr; echo "[1] Docker"
if command -v docker >/dev/null 2>&1; then ok "docker présent ($(docker --version))"; else ko "docker absent"; echo "Stop."; exit 1; fi

# --- 2. Découverte des containers -------------------------------------------
hr; echo "[2] Containers en cours"
docker ps --format '  {{.Names}}\t{{.Status}}\t{{.Ports}}'

N8N=$(docker ps --format '{{.Names}}' | grep -iE 'n8n' | grep -viE 'task-runner|runner' | head -1)
RUNNERS=$(docker ps --format '{{.Names}}' | grep -iE 'task-runner|runner' | sort)
DASH=$(docker ps --format '{{.Names}}' | grep -iE 'dashboard' | head -1)
BROKER=$(docker ps --format '{{.Names}}' | grep -iE 'ibkr-broker|broker' | head -1)
GATEWAY=$(docker ps --format '{{.Names}}' | grep -iE 'ibkr-gateway|gateway|ibeam' | head -1)

echo
[ -n "$N8N" ]     && ok "container n8n détecté : $N8N"            || ko "aucun container n8n détecté"
[ -n "$RUNNERS" ] && ok "task-runners : $(echo $RUNNERS | tr '\n' ' ')" || warn "aucun task-runner détecté"
[ -n "$DASH" ]    && ok "dashboard : $DASH"                       || warn "aucun container dashboard détecté"
[ -n "$BROKER" ]  && ok "ibkr-broker : $BROKER"                   || warn "aucun ibkr-broker détecté"
[ -n "$GATEWAY" ] && ok "ibkr-gateway/ibeam : $GATEWAY"           || warn "aucun ibkr-gateway/ibeam détecté"

# Comparaison aux noms attendus dans la doc (runners en 3/4/5 sur le VPS courant)
for expected in root-n8n-1 root-task-runners-3 root-trading-dashboard-1; do
  docker ps --format '{{.Names}}' | grep -qx "$expected" \
    && ok "nom attendu présent : $expected" \
    || warn "nom attendu ABSENT (doc à corriger ?) : $expected"
done

# --- 3. Dossier compose réel (lève le doute /docker/root vs /opt/trader-ia) --
hr; echo "[3] Emplacement réel du compose"
if [ -n "$N8N" ]; then
  WD=$(docker inspect "$N8N" --format '{{ index .Config.Labels "com.docker.compose.project.working_dir" }}' 2>/dev/null)
  PROJ=$(docker inspect "$N8N" --format '{{ index .Config.Labels "com.docker.compose.project" }}' 2>/dev/null)
  CF=$(docker inspect "$N8N" --format '{{ index .Config.Labels "com.docker.compose.project.config_files" }}' 2>/dev/null)
  echo "  project        = ${PROJ:-?}"
  echo "  working_dir    = ${WD:-?}"
  echo "  config_files   = ${CF:-?}"
  [ "$WD" = "/docker/root" ] && ok "working_dir == /docker/root (conforme doc)" \
    || warn "working_dir = ${WD:-?} (doc dit /docker/root — à ajuster)"
fi

# --- 4. n8n : python3 + sqlite3 + base d'exécutions -------------------------
hr; echo "[4] n8n : python3 + module sqlite3 + base exécutions"
if [ -n "$N8N" ]; then
  docker exec "$N8N" python3 -c 'import sqlite3,sys; print("python3 OK, sqlite3 module OK", sys.version.split()[0])' 2>/dev/null \
    && ok "python3 + sqlite3 disponibles dans $N8N" \
    || ko "python3/sqlite3 indisponibles dans $N8N (revoir §4 du runbook)"

  docker exec "$N8N" sh -lc 'test -f /home/node/.n8n/database.sqlite' 2>/dev/null \
    && ok "base présente : /home/node/.n8n/database.sqlite" \
    || ko "database.sqlite introuvable au chemin attendu"

  echo "  -- colonnes de workflow_entity (attendu: active, versionId, activeVersionId) --"
  docker exec "$N8N" python3 - <<'PY' 2>/dev/null || ko "lecture schéma workflow_entity échouée"
import sqlite3
c=sqlite3.connect('file:/home/node/.n8n/database.sqlite?mode=ro',uri=True)
cols=[r[1] for r in c.execute("PRAGMA table_info(workflow_entity)")]
print("    colonnes:", ", ".join(cols))
for need in ("active","versionId","activeVersionId"):
    print(f"    {'[PASS]' if need in cols else '[FAIL]'} colonne {need}")
PY
else
  ko "n8n introuvable — section sautée"
fi

# --- 5. Workflows réels + version publiée -----------------------------------
hr; echo "[5] Workflows n8n (id, name, active, versionId vs activeVersionId)"
if [ -n "$N8N" ]; then
  docker exec "$N8N" python3 - <<'PY' 2>/dev/null || warn "lecture workflows échouée"
import sqlite3
c=sqlite3.connect('file:/home/node/.n8n/database.sqlite?mode=ro',uri=True)
try:
    rows=list(c.execute("SELECT id,name,active,versionId,activeVersionId FROM workflow_entity ORDER BY active DESC,name"))
except Exception as e:
    print("    ERREUR:",e); rows=[]
print(f"    {len(rows)} workflow(s)")
for wid,name,active,vid,avid in rows:
    flag="" if (avid is None or vid==avid) else "  <-- versionId != activeVersionId (édition non publiée)"
    print(f"    [{'ON ' if active else 'off'}] {name!s:<48} id={wid} v={str(vid)[:8]} active_v={str(avid)[:8]}{flag}")
PY
fi

# --- 6. Dernières exécutions -------------------------------------------------
hr; echo "[6] 10 dernières exécutions"
if [ -n "$N8N" ]; then
  docker exec "$N8N" python3 - <<'PY' 2>/dev/null || warn "lecture exécutions échouée"
import sqlite3
c=sqlite3.connect('file:/home/node/.n8n/database.sqlite?mode=ro',uri=True)
q="""SELECT e.id,COALESCE(w.name,'?'),e.status,e.startedAt
     FROM execution_entity e LEFT JOIN workflow_entity w ON w.id=e.workflowId
     ORDER BY e.startedAt DESC LIMIT 10"""
for r in c.execute(q): print("   ",r)
PY
fi

# --- 7. DuckDB sur l'hôte ----------------------------------------------------
hr; echo "[7] Bases DuckDB hôte (chemin /local-files/duckdb)"
for p in /local-files/duckdb /opt/local-files/duckdb; do
  if [ -d "$p" ]; then
    ok "répertoire présent : $p"
    echo "  -- fichiers .duckdb --"
    ls -1 "$p"/*.duckdb 2>/dev/null | sed 's/^/    /' || warn "    aucun .duckdb dans $p"
    for must in ag1_v4_consensus.duckdb ag1_fx_v1_chatgpt52.duckdb ag2_fx_v1.duckdb ag4_fx_v1.duckdb; do
      [ -f "$p/$must" ] && ok "présent : $must" || warn "attendu mais absent : $must"
    done
  else
    warn "répertoire absent : $p"
  fi
done
# Confirme le mapping volume côté container
if [ -n "$N8N" ]; then
  echo "  -- mapping volume du container n8n (attendu /local-files -> /files) --"
  docker inspect "$N8N" --format '{{range .Mounts}}    {{.Source}} -> {{.Destination}}{{"\n"}}{{end}}' 2>/dev/null | grep -iE 'local-files|/files|\.n8n' || warn "mapping non trouvé"
fi

# --- 8. Santé IBKR broker ----------------------------------------------------
hr; echo "[8] Santé IBKR broker"
if [ -n "$BROKER" ]; then
  docker exec "$BROKER" python - <<'PY' 2>/dev/null || warn "health broker injoignable (interne 8080)"
import json,urllib.request
try:
    d=json.load(urllib.request.urlopen('http://localhost:8080/health',timeout=8))
    print("    health:",json.dumps(d))
except Exception as e:
    print("    ERREUR:",e)
PY
else
  warn "ibkr-broker non détecté — santé non testée"
fi

# --- 9. Dashboard ------------------------------------------------------------
hr; echo "[9] Dashboard Streamlit"
if [ -n "$DASH" ]; then
  docker exec "$DASH" python -c "import urllib.request;print('    http',urllib.request.urlopen('http://127.0.0.1:8501',timeout=8).status)" 2>/dev/null \
    && ok "dashboard répond sur 8501 (interne)" \
    || warn "dashboard ne répond pas sur 8501 (port peut être non publié)"
fi

# --- Bilan -------------------------------------------------------------------
hr
echo "BILAN : PASS=$PASS  WARN=$WARN  FAIL=$FAIL"
echo "(WARN = écart à clarifier ; FAIL = la doc affirme qqch de faux à corriger)"
echo "==> Copie TOUTE cette sortie à Claude pour figer AGENTS.md."
