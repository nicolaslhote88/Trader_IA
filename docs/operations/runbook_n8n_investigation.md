# Runbook — Investiguer les exécutions n8n sur le VPS

> But : aller **directement au bon endroit** sur le VPS Hostinger pour diagnostiquer le déroulement
> des workflows n8n, sans re-découvrir l'infra à chaque fois.
>
> Convention : `[FAIT✓]` = **vérifié sur le VPS le 2026-06-15** · `[FAIT]` = vérifié dans le repo · `[⚠]` = piège.
>
> **MAJ 2026-06-16** : (1) AG1 V4 = GPT-5.5 / Grok 4.3 / **Claude Sonnet 4.6** (Gemini retiré).
> (2) **Deux stacks compose** : n8n sous `/docker/root`, **IBKR/yfinance/broker sous `/docker/yfinance`**
> (pour le broker, utiliser `docker exec ibkr-broker …`, pas `docker compose` depuis `/docker/root`).
> (3) **Système d'approbation des ordres LIVE** (Telegram @CYROLAS_BOT → groupe) — voir `order_approval_deploy_notes.md`.
> (4) Bug ouvert : `core.runs.strategy_version`/`prompt_version`/`n8n_execution_id` restent NULL (mapping writer 08/09).

---

## 0. TL;DR — triage en 60 secondes

```bash
ssh vps   # = root@82.112.242.251 (clé .ssh/codex_vps_tailscale_ed25519)

# Containers + santé
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

# Dernières erreurs n8n (6h)
docker logs --since 6h root-n8n-1 2>&1 | grep -iE 'error|failed|timeout|exception' | tail -50

# 15 dernières exécutions Trader_IA (le container n8n a python3 + sqlite3)
docker exec root-n8n-1 python3 - <<'PY'
import sqlite3
c=sqlite3.connect('file:/home/node/.n8n/database.sqlite?mode=ro', uri=True)
for r in c.execute("""SELECT e.id,w.name,e.status,e.startedAt
  FROM execution_entity e JOIN workflow_entity w ON w.id=e.workflowId
  WHERE w.name LIKE 'AG%' OR w.name LIKE 'YF%'
  ORDER BY e.startedAt DESC LIMIT 15"""): print(r)
PY
```

`[⚠]` **n8n est une instance PARTAGÉE** (167 workflows, 23 actifs : Trader_IA + SIGA + templates).
Toujours **filtrer sur `AG*` / `YF*`** pour isoler Trader_IA.
Pour la vérité métier (décisions, ordres, fills) → DuckDB `/local-files/duckdb/` (§5), pas n8n.
Un script tout-en-un existe : `outils/scripts/verify_vps_n8n.sh` (lecture seule).

---

## 1. Accès SSH `[FAIT✓]`

| Élément | Valeur |
|---|---|
| Alias SSH | `ssh vps` (config `.ssh/config`) → `root@82.112.242.251` |
| Hostname | `srv961978` · User `root` · Ubuntu 24.04 |
| Clé | `.ssh/codex_vps_tailscale_ed25519` (dossier `.ssh/` **local, gitignoré**) |
| Tailscale | `100.104.236.78` (`ssh vps-tailscale`) |

```bash
ssh vps "hostname; whoami; uptime"
ssh vps "docker ps --format '{{.Names}}'"   # non-interactif (préféré)
```

`[⚠]` Ne jamais afficher/copier la clé privée. `.ssh/id_ed25519` (sans suffixe) vise `atelier-pi`, pas ce VPS.

---

## 2. Topologie Docker `[FAIT✓]`

Sur le VPS courant, la stack tourne depuis **`/docker/root`** (`com.docker.compose.project=root`,
`config=/docker/root/docker-compose.yml`).

| Container | Rôle |
|---|---|
| `root-n8n-1` | Orchestrateur n8n (UI + exécutions SQLite) — écoute `127.0.0.1:5678` |
| `root-task-runners-3`, `-4`, `-5` | Runners externes (Code nodes Python/JS). ⚠️ **indices 3/4/5, pas 1/2/3** |
| `root-traefik-1` | Reverse-proxy TLS |
| `root-trading-dashboard-1` | Dashboard Streamlit (port `8501`, interne) |
| `root-toolbox-1` | Utilitaires |
| `ibkr-gateway` / `ibkr-broker` | Exécution IBKR (gateway clientportal, broker FastAPI) |
| `macro-data-api` | API macro 3/4 piliers |
| `yfinance-api` / `yf-enrichment` | Prix yfinance + enrichissement |

`[⚠]` Le même hôte fait tourner d'autres projets (hermes, portainer, qdrant, siga-dashboard, voice-gateway) :
filtrer sur les noms ci-dessus.

```bash
# Confirmer le dossier compose réellement utilisé :
docker inspect root-n8n-1 --format '{{ index .Config.Labels "com.docker.compose.project.working_dir" }}'
# Détecter dynamiquement les runners (les indices peuvent changer après scale) :
docker ps --format '{{.Names}}' | grep -i task-runner
```

Volumes du container `root-n8n-1` `[FAIT✓]` : `/local-files → /files` (les `.duckdb` sont dans
`/local-files/duckdb/`), et `/var/lib/docker/volumes/n8n_data/_data → /home/node/.n8n` (SQLite).

---

## 3. Logs n8n & runners `[FAIT✓]`

```bash
docker logs --since 2h root-n8n-1 2>&1 | tail -100
docker logs --since 24h root-n8n-1 2>&1 | grep -iE 'error|fail|timeout|exception|stack' | tail -80
docker logs -f --tail 50 root-n8n-1                       # suivre un run en direct
for r in 3 4 5; do echo "== runner $r =="; docker logs --since 2h root-task-runners-$r 2>&1 | tail -40; done
```

`[⚠]` Les erreurs des **Code nodes** (sandbox Python/JS) remontent souvent côté **task-runners**, pas n8n.
`[⚠]` Pièges sandbox runner : `requests` indisponible (allow-list `duckdb,pandas,numpy,datetime,math,json`),
`process.env` interdit en JS (`$env.X`), builtins `eval/exec/getattr` bloqués.

---

## 4. Base d'exécutions n8n (SQLite) `[FAIT✓]`

`/home/node/.n8n/database.sqlite`. Rétention (compose) : `EXECUTIONS_DATA_MAX_AGE=72` h,
`SAVE_ON_SUCCESS=all`, `PRUNE_MAX_COUNT=5000`, VACUUM au boot. → fenêtre ≈ 72 h / 5000 ; historique long ⇒ DuckDB (§5).

`[FAIT✓]` Le container `root-n8n-1` embarque **python3 3.12 + module sqlite3** : voie d'accès recommandée.
Colonnes confirmées de `workflow_entity` : `id, name, active, nodes, …, versionId, …, activeVersionId`.

```bash
docker exec root-n8n-1 python3 - <<'PY'
import sqlite3
c=sqlite3.connect('file:/home/node/.n8n/database.sqlite?mode=ro', uri=True)

# Workflows Trader_IA + version publiée
print("== workflows AG*/YF* ==")
for r in c.execute("""SELECT id,name,active,versionId,activeVersionId
  FROM workflow_entity WHERE name LIKE 'AG%' OR name LIKE 'YF%'
  ORDER BY active DESC,name"""): print(r)

# Échecs récents (tous projets confondus -> filtrer si besoin)
print("== échecs ==")
for r in c.execute("""SELECT e.id,w.name,e.status,e.startedAt
  FROM execution_entity e LEFT JOIN workflow_entity w ON w.id=e.workflowId
  WHERE e.status IN ('error','crashed','failed')
  ORDER BY e.startedAt DESC LIMIT 20"""): print(r)
PY

# Détail I/O d'une exécution (gros JSON -> fichier)
docker exec root-n8n-1 python3 - "$EXEC_ID" <<'PY' > /tmp/exec.json
import sqlite3,sys
c=sqlite3.connect('file:/home/node/.n8n/database.sqlite?mode=ro', uri=True)
print(c.execute("SELECT data FROM execution_data WHERE executionId=?",(sys.argv[1],)).fetchone()[0])
PY
```

### Workflows Trader_IA actifs au 2026-06-15 `[FAIT✓]`
`AG1V4CONSENSUS` (AG1 V4 consensus), `AG1-PF-V1` (MTM V4), `AG2-V3`, `AG3-V2`, `AG4-V3`, `AG4_Spé-V2`, `YF-ENRICH-V1`.
**Tous les workflows FX sont `active=0`** (AG1-FX-V1 ×3, AG1-FX-PF-V1, AG2-FX-V1, AG3-FX-V1, AG4-FX-V1,
AG4-Forex, AG2-V3 FX-only, AG5/AG6/AG7/AG8-FX). La stack Forex est **parquée**.

---

## 4 bis. Quelle version a réellement tourné ? `[FAIT]` `[⚠]`

Sur **n8n 2.x**, un workflow actif exécute la **version publiée** (`workflow_entity.activeVersionId`),
**pas** `workflow_entity.nodes`. Si `versionId != activeVersionId`, l'édition n'est pas celle exécutée.
(Au 2026-06-15, aucune édition active non publiée.)

```bash
docker exec root-n8n-1 python3 - <<'PY'
import sqlite3
c=sqlite3.connect('file:/home/node/.n8n/database.sqlite?mode=ro', uri=True)
print(c.execute("SELECT id,name,active,versionId,activeVersionId,updatedAt "
                "FROM workflow_entity WHERE name LIKE 'AG1 V4%'").fetchall())
PY
# Republier après import/injection :
docker exec root-n8n-1 n8n publish:workflow --id=<id>
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

`[⚠]` Désactivation par SQL : `active=0` ne suffit pas, il faut aussi `activeVersionId=NULL` + redémarrer. (Source `deploy.md` §4.a.)

---

## 5. Vérité métier : DuckDB `[FAIT✓]`

Bases sur l'hôte dans **`/local-files/duckdb/`** (= `/files/duckdb/`). Présentes au 2026-06-15 :
`ag1_v4_consensus.duckdb`, `ag1_v3_*.duckdb`, `ag1_fx_v1_*.duckdb` (+ backups), `ag2_v3`, `ag2_fx_v1`,
`ag3_v2`, `ag3_fx_v1`, `ag4_v3`, `ag4_fx_v1`, `ag4_forex_v1`, `ag4_spe_v2`, `macro_data`, `yf_enrichment_v1`, `siga_v1`.

| Workflow actif | Base | Tables clés |
|---|---|---|
| **AG1 V4 consensus** | `ag1_v4_consensus.duckdb` | `core.runs`, `core.orders`, `core.fills`, `core.consensus_decisions`, `core.consensus_votes`, `core.model_proposals`, `core.alerts`, `core.portfolio_*_mtm_*`, `cfg`/`core.portfolio_config` |
| AG2-V3 (actions) | `ag2_v3.duckdb` | `run_log`, signaux |
| AG4-V3 (news) | `ag4_v3.duckdb` | `run_log`, news taguées |
| (FX, **inactif**) | `ag1_fx_v1_chatgpt52.duckdb`, `ag2_fx_v1`, `ag4_fx_v1`, `ag4_forex_v1` | `core.*` / `run_log` (figés) |

`[⚠]` **L'image `python:3.11-slim` n'a ni pandas, ni numpy, ni pytz.** Donc :
- utiliser `.fetchall()` (pas `.fetchdf()`),
- **caster les TIMESTAMP en VARCHAR** (`CAST(col AS VARCHAR)`) sinon erreur `pytz`.
- `[⚠]` Les noms de colonnes temporelles varient : `core.runs` utilise `created_at`/`ts_start`/`ts_end`,
  `core.orders` utilise `ts_created`. Inspecter le schéma avant de requêter.

```bash
DB=ag1_v4_consensus.duckdb
docker run --rm -i -v /local-files/duckdb:/db:ro python:3.11-slim \
  bash -lc "pip install -q duckdb 2>/dev/null && python3 -" <<'PY'
import duckdb
c=duckdb.connect('/db/ag1_v4_consensus.duckdb', read_only=True)
# lister les tables/colonnes si besoin :
# print([t[0] for t in c.execute("SELECT table_name FROM information_schema.tables").fetchall()])
for r in c.execute("SELECT run_id, CAST(created_at AS VARCHAR), strategy_version "
                   "FROM core.runs ORDER BY created_at DESC LIMIT 5").fetchall():
    print(r)
PY
```

Croisement type : run n8n `success` mais `core.orders` vide ⇒ rejet Risk Manager (Validate & Enforce Safety)
ou garde consensus (vote < 2/3) ⇒ voir `core.consensus_votes` / `core.alerts`.

---

## 6. Santé des services `[FAIT✓]`

```bash
cd /docker/root && docker compose ps
docker ps -a --filter 'status=exited' --format 'table {{.Names}}\t{{.Status}}'   # crashes
docker stats --no-stream; df -h /; free -h

# IBKR broker — santé + auth + alignement compte (interne 8080)
echo 'import json,urllib.request;print(json.dumps(json.load(urllib.request.urlopen("http://localhost:8080/health",timeout=8)),indent=2))' \
  | docker exec -i ibkr-broker python -

# Dashboard
docker exec root-trading-dashboard-1 python -c \
 "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:8501',timeout=8).status)"
```

État vérifié 2026-06-15 du broker : `dry_run=false`, compte **`U25651155` (live)**, `gateway_is_paper=false`,
`authenticated=true`, `fx_orders_enabled=false`, `assisted_login.enabled=false` (`mode=manual_gateway_login`).

`[⚠]` **Le trading actions est en LIVE réel.** `manual_login_required=true` dans `/health` ⇒ relogin
navigateur requis (tunnel `ssh -L 5000:localhost:5000 vps` puis `https://localhost:5000` + 2FA).
`auto_reauth_enabled=true` gère les sessions courtes. Cf. `docs/operations/ibkr_execution.md`.

---

## 7. Arbre de décision

```
Workflow n'a pas tourné            -> §6 (container up ? cron ? workflow active=1 ?) + §3 logs "Schedule"
Édition sans effet / mauvais code  -> §4 bis (versionId vs activeVersionId, republier)
Workflow a tourné mais a échoué    -> §3 logs n8n + runners 3/4/5, puis §4 execution_data de l'ID
Workflow OK mais résultat absurde  -> §5 DuckDB (core.runs / consensus_votes / alerts)
Ordres non envoyés à IBKR          -> §5 core.orders/alerts + §6 /health (authenticated, dry_run, fx_orders_enabled)
Erreur Code node (Python/JS)       -> §3 logs task-runners + pièges sandbox
```

---

## Sources
- Vérifié en direct sur le VPS le 2026-06-15 (script `outils/scripts/verify_vps_n8n.sh`).
- `docs/operations/vps-access.md`, `deploy.md`, `ibkr_execution.md`, `env_vars.md`
- `infra/vps_hostinger_config/docker-compose.yml`
