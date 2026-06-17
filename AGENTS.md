# AGENTS.md — Trader_IA

Instructions chargées automatiquement par Codex. Garder ce fichier **court** : les procédures
détaillées vivent dans `docs/`, on ne fait que pointer dessus.

## Langue & style
Répondre en français. Sortie structurée et opérationnelle : chemins de fichiers, commandes exactes,
distinction explicite **faits validés / hypothèses / actions restantes**. Ne pas inventer d'info
technique : signaler les incertitudes.

## État du projet — VÉRIFIÉ sur le VPS le 2026-06-15
- **Actions/ETF : AG1 V4 consensus** est le Portfolio Manager **actif** (consensus 2/3 GPT/Grok/Claude Sonnet).
  Workflow n8n `AG1V4CONSENSUS` (« AG1 V4 - Consensus Portfolio Manager »), tourne sur cron
  (dernier run vérifié 2026-06-15 14:00 Paris). Base `ag1_v4_consensus.duckdb`, ledger v4
  (tables `core.runs`, `core.orders`, `core.consensus_decisions`, `core.consensus_votes`,
  `core.model_proposals`, `core.fills`, `core.*_mtm_*`…). **Dashboard Streamlit = V4-only** (répond sur 8501).
- Autres workflows Trader_IA **actifs** : `AG1-PF-V1` (MTM horaire DuckDB pour V4), `AG2-V3`,
  `AG3-V2`, `AG4-V3`, `AG4_Spé-V2`, `YF-ENRICH-V1`.
- **⚠️ IBKR est en mode RÉEL (live), pas paper.** Broker `/health` : `dry_run=false`,
  compte **`U25651155` (type `live`)**, `gateway_is_paper=false`, `authenticated=true`, `aligned=true`.
  Gardes côté n8n : `AG1_ACTIONS_LIVE_ORDERS_ENABLED=true`, `IBKR_REQUIRE_PAPER_ACCOUNT=false`,
  `AG1_V4_ACTIONS_IBKR_ENABLED_MODELS=ag1_v4_consensus`. **⇒ AG1 V4 peut envoyer des ordres actions réels.**
- **Forex : entièrement désactivé (parqué).** Tous les workflows FX sont `active=0`
  (AG1-FX-V1 ×3 variants, AG1-FX-PF-V1, AG2-FX-V1, AG3-FX-V1, AG4-FX-V1, AG4-Forex, AG2-V3 FX-only,
  et les piliers AG5/AG6/AG7/AG8-FX). Au broker : `fx_orders_enabled=false`. Les bases FX existent
  encore dans `/local-files/duckdb/` mais ne sont plus alimentées.
- **Login IBKR : manuel** (`mode=manual_gateway_login`, navigateur + 2FA). L'assisted login (IBeam)
  est **désactivé** actuellement (`assisted_login.enabled=false`), `auto_reauth_enabled=true`.
- **n8n 2.x** : un workflow actif exécute sa **version publiée** (`activeVersionId`), pas forcément
  `workflow_entity.nodes`. (Au 2026-06-15, aucune édition active non publiée.) Vérifier avant de conclure.

## VPS / infra — VÉRIFIÉ
- VPS Hostinger `srv961978` (alias `ssh vps` → `root@82.112.242.251`, Ubuntu 24.04).
  Clé : `.ssh/codex_vps_tailscale_ed25519` (dossier `.ssh/` **local, gitignoré, jamais sur GitHub**).
- Stack Docker Compose **projet `root`**, déployée sous **`/docker/root`** (`/docker/root/docker-compose.yml`).
- Containers Trader_IA : `root-n8n-1`, `root-task-runners-3` / `-4` / `-5` (⚠️ pas 1/2/3),
  `root-trading-dashboard-1`, `root-traefik-1`, `root-toolbox-1`, `ibkr-gateway`, `ibkr-broker`,
  `macro-data-api`, `yfinance-api`, `yf-enrichment`.
- **n8n est une instance PARTAGÉE multi-projets** (167 workflows, 23 actifs) : Trader_IA **+ SIGA**
  + une grosse bibliothèque de templates inactifs. Toujours **filtrer sur les noms `AG*` / `YF*`**
  pour isoler Trader_IA.
- Exécutions n8n : **SQLite** `/home/node/.n8n/database.sqlite` (le container `root-n8n-1` a
  `python3` 3.12 + module `sqlite3`). Volume hôte `n8n_data` → `/var/lib/docker/volumes/n8n_data/_data`.
- **DuckDB** (vérité métier) sur l'hôte dans **`/local-files/duckdb/`** (= `/files/duckdb/` côté containers).

> ⚠️ Arborescence GitHub ≠ arborescence VPS. Source du compose : `infra/vps_hostinger_config/`.

### 🔎 Investiguer le déroulement d'un workflow n8n
**Avant toute investigation n8n/VPS, ouvrir et suivre :** `docs/operations/runbook_n8n_investigation.md`
(triage 60 s, accès SSH, containers, logs n8n+runners, exécutions SQLite, `run_log`/`core.*` DuckDB, santé).

### Docs de référence
- Accès SSH : `docs/operations/vps-access.md`
- Déploiement, publication workflows (`activeVersionId`), V4, IBKR : `docs/operations/deploy.md`
- Exécution / santé IBKR : `docs/operations/ibkr_execution.md`
- Variables d'env : `docs/operations/env_vars.md`
- Compose VPS (source de vérité services/volumes) : `infra/vps_hostinger_config/docker-compose.yml`
- Script de vérif rapide (lecture seule) : `scripts/verify_vps_n8n.sh`

## Garde-fous
- Ne jamais afficher/copier de clé privée ni de secret. `.ssh/` reste local (gitignoré).
- Lectures DuckDB **toujours** en `read_only=True` ; ne pas écrire en base directement.
- **⚠️ Le trading actions est en LIVE réel et ASSUMÉ (compte `U25651155`, positions réelles détenues).**
  C'est un choix **volontaire** de Nicolas (cutover V4), pas une anomalie de config. Ne PAS « corriger »,
  ni modifier les gardes d'exécution (`IBKR_DRY_RUN`, `AG1_ACTIONS_LIVE_ORDERS_ENABLED`,
  `IBKR_REQUIRE_PAPER_ACCOUNT`, `AG1_V4_ACTIONS_IBKR_ENABLED_MODELS`), ni réactiver un workflow FX,
  sans décision explicite de Nicolas. En cas de modif touchant l'exécution d'ordres : signaler et demander avant.
- Ne jamais exécuter / placer d'ordre soi-même : laisser les workflows n8n et le broker faire.
