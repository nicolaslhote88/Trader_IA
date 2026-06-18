# AGENTS.md — Trader_IA

Instructions chargées automatiquement par Codex. Garder ce fichier **court** : les procédures
détaillées vivent dans `docs/`, on ne fait que pointer dessus.

## Langue & style
Répondre en français. Sortie structurée et opérationnelle : chemins de fichiers, commandes exactes,
distinction explicite **faits validés / hypothèses / actions restantes**. Ne pas inventer d'info
technique : signaler les incertitudes.

## 🧭 Récupération de session (LIRE EN PREMIER)
Ce fichier est le point d'entrée durable du projet (la mémoire interne peut ne pas être à jour). Pour reprendre
à moindre coût : (1) lire ce fichier en entier ; (2) pour l'état live réel, se connecter au VPS (§ VPS/infra) et
lire broker `/health` + `/orders/approvals/pending` + DuckDB `core.runs`. Détail par sujet dans `docs/`.
État vérifié sur le VPS au **2026-06-18**. Branche repo : `claude/ag4-v3-dualbranch-calib-20260617`.
⚠️ Travaux récents **déployés sur le VPS mais à committer** (cf `docs/operations/HANDOFF_codex_PR_ag4_spe_v3_20260618.md`).
⚠️ Le working tree a ~200 fichiers en **bruit CRLF** : ne stager QUE les fichiers réellement modifiés (liste dans le handoff).

## État du projet — VÉRIFIÉ sur le VPS le 2026-06-18
- **Actions/ETF : AG1 V4 consensus** est le Portfolio Manager **actif** (consensus 2/3).
  3 modèles : **GPT-5.5** (`gpt-5.5-2026-04-23`), **Grok 4.3** (`grok-4.3`), **Claude Sonnet 4.6** (`claude-sonnet-4-6`).
  `model_keys` persistés : `chatgpt52`, `grok41_reasoning`, **`claude_sonnet46`**. Workflow n8n `AG1V4CONSENSUS`.
  Base `ag1_v4_consensus.duckdb` (ledger v4 : `core.runs/orders/consensus_*/model_proposals/fills/*_mtm_*`). Dashboard Streamlit V4-only (8501).
- Autres workflows actifs : `AG1-PF-V1` (MTM horaire V4), `AG2-V3`, `AG3-V2`, `AG4-V3`, `AG4_Spé-V2`,
  **`AG4_Spé-IBKR-V1`** (news IBKR portfolio, nouveau), **`AG4_Spé — Health Alert`** (nouveau), `YF-ENRICH-V1`.
- **AG4-V3 : dual-branch.** Node `20CFG - Analysis Mode` (`analysisMode`, défaut `reduced`) → Switch `20H_MODE`.
  `reduced` = Actions via Grok grok-4.3 ; `full` = ancien (gpt-5-mini, réactive le Forex). Détails : `docs/audits/20260617_ag4_v3_news_watcher_audit.md`.
- **⚠️ IBKR mode RÉEL (live).** Compte **`U25651155`**, `dry_run=false`, `AG1_ACTIONS_LIVE_ORDERS_ENABLED=true`. Ordres réels.
- **Forex : entièrement désactivé** (workflows FX `active=0`, `fx_orders_enabled=false`).
- **Login IBKR : manuel** (navigateur + 2FA, IBeam désactivé), `auto_reauth_enabled=true`. ⚠️ délai de propagation après 2FA (la session brokerage peut mettre quelques min à répondre `authenticated:true`).

## 📰 Pipeline NEWS single-stock (AG4_Spé V2/V3 → AG1 V4) — MAJ 2026-06-18
Base **`ag4_spe_v2.duckdb`** (`news_history` + vue **`news_analyzed`** = summary∧is_relevant). Détails : `docs/audits/20260617_ag4_spe_v2_analysis.md`, `…_remediation_plan.md`, `docs/specs/ag4_spe_v3_ibkr_news.md`, `docs/specs/ag1_v4_d2_news_digest.md`.
- **AG4_Spé-V2** (Boursorama, cron 09/12/15h05 UTC sem.) : B1 dates corrigées (`07_parse_article.js`), A1 anti-zombies (`02_start_run.py`), C1/C3 univers actions/ETF (FX exclus, ~385) + rotation priorisée portefeuille + retry S04/S14 (`01_build_symbol_queue.py`). Historique nettoyé (19,5k→3,3k).
- **AG4_Spé-IBKR-V1** (cron 10/13/16h UTC sem.) : news IBKR **portfolio** (positions détenues) via broker `GET /news/portfolio` → même chaîne LLM que V2 → `news_history` (`source='ibkr'`, `provider`, `news_article_id`, `ibkr_sentiment`). L'endpoint IBKR **par-contrat** (`/iserver/news?conid=`) n'est PAS servi (503) → held-only.
- **D2 (dans AG1 V4)** : node **`20K — News Digest (Pack+Held)`** (Calcul Matrice → 20K → Merge7[1]) enrichit `opportunity_pack` : `rows[].news[]` (≤3, 14j) + `held_news` + `news_legend`. Le PM lit `opportunity_pack` (PAS `opportunity_brief`). Budget ~+1,4k tokens.
- **AG4_Spé — Health Alert** (cron 16h30 UTC sem.) : alerte Telegram si pipeline stale / zombies / dates KO.

## 🆕 Approbation des ordres hors-bande (LIVE)
Ordre dont le prix limite s'écarte trop → parqué → Nicolas valide via Telegram (bot **@CYROLAS_BOT**, `chat_id -4887456379`).
Bandes (limit vs quote) : **≤5 %** auto-confirmé · **5–15 %** parqué + notif boutons Approuver/Rejeter (TTL 10 min) · **>15 %** rejeté.
Cas **prix non vérifiable** (`QUOTE_TOO_OLD`/`NO_REFERENCE_PRICE`) aussi parqués. `IBKR_APPROVAL_ENABLED=true`.
- **FIX 2026-06-18** (`services/ibkr-broker/app.py` `approvals_approve`) : à l'approbation on **re-soumet l'ordre frais** (`place_orders`) + auto-confirm de la NOUVELLE chaîne, sous try/except (plus de **500** dû au rejeu d'un `reply_id` IBKR périmé). Double-tap / déjà décidé → **200 idempotent** (helper `_approval_decision_error`). Cf `docs/operations/order_approval_deploy_notes.md`.
- ⚠️ Hors séance, un ordre limite ne fille pas → l'approbation finit `FAILED` proprement (et non crash). Approuver **en séance** pour un fill réel.
Code : `services/ibkr-broker/approval.py` + endpoints/guard `app.py`. Workflows : `AG1 V4 — Order Approval Request` / `… Decide`.

## VPS / infra — VÉRIFIÉ
- VPS Hostinger `srv961978` (`ssh vps` → `root@82.112.242.251`). Clé `.ssh/codex_vps_tailscale_ed25519` (local, gitignoré).
- **Deux stacks compose** : **n8n** projet `root` `/docker/root` (`root-n8n-1`, `root-task-runners-3/4/5`, `root-trading-dashboard-1`, `root-traefik-1`) ;
  **IBKR/yfinance** projet `yfinance` `/docker/yfinance` (`ibkr-broker`, `ibkr-gateway`, `yfinance-api`, `yf-enrichment`, `macro-data-api`). `.env`/approval ici.
- **Broker = image baked** : source host `/opt/trader-ia/services/ibkr-broker/`. Déploy = éditer source → `cd /docker/yfinance && docker compose build ibkr-broker && docker compose up -d ibkr-broker`. ⚠️ `/opt/trader-ia` pas un clone git → **committer dans ce repo**.
- **n8n PARTAGÉE** (Trader_IA + SIGA). Filtrer `AG*`/`YF*`. Exécutions SQLite `/home/node/.n8n/database.sqlite`.
  **Déployer un workflow** : `n8n import:workflow --input=...` (inclure l'`id` pour MAJ in-place) → `n8n publish:workflow --id=...` → `docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5`. La version **publiée** (`workflow_history` via `activeVersionId`) est celle exécutée, pas `workflow_entity.nodes`.
- **DuckDB** (vérité métier) : hôte `/local-files/duckdb/` (= `/files/duckdb/` côté containers). Lecture via container `yf-enrichment` (duckdb 1.4.4) ou `root-n8n-1` (1.4.3).
- **Telegram** : 1 credential n8n (label « Jarvis », bot **@CYROLAS_BOT**, id cred `pVqYKOVuJrq3njUz`). Ne pas ajouter de 2ᵉ Telegram Trigger (boutons URL + Webhook).

## 🧰 Pièges dev (vérifiés 2026-06-18)
- **Sandbox Python n8n (task-runner)** : imports autorisés = `duckdb, json, time, datetime, math, numpy, pandas`. **`hashlib` INTERDIT** ; imports combinés rejetés → **un import par ligne**, jamais hashlib (sinon « Security violations »).
- **Outils d'édition tronquent les gros fichiers** (>~160 lignes : `app.py`, `01_build_symbol_queue.py`…). Éditer ces fichiers via **patch python/heredoc shell**, jamais l'éditeur direct ; vérifier `py_compile` + équilibre accolades après.
- **Vue DuckDB `SELECT *`** cassée par tout `ALTER TABLE ADD COLUMN` → la **recréer** (`CREATE OR REPLACE VIEW`) après migration.

## ⚠️ Bugs / issues ouverts
- **Writer DuckDB ne mappe pas 3 champs** : `core.runs.strategy_version`/`prompt_version`/`n8n_execution_id` restent NULL (mismatch camelCase↔snake_case dans `08_build_duckdb_bundle.code.js` / `09_upsert_run_bundle_duckdb.code.py`). À corriger.
- **Session IBKR peut sauter la nuit** (`CPAPI HTTP 401`). Relogin requis (`ssh -L 5000:localhost:5000 vps` + `https://localhost:5000` + 2FA).
- **Store des approbations = en mémoire** (perdu au restart broker). Audit DuckDB = évolution future.
- Couverture scraping AG4_Spé : certains symboles rendent 0 article par run (502/503 transitoires Boursorama atténués par retry). Surveiller via Health Alert.

### Docs de référence
- Handoff PR (commit) : `docs/operations/HANDOFF_codex_PR_ag4_spe_v3_20260618.md`
- Audit/plan news AG4_Spé : `docs/audits/20260617_ag4_spe_v2_analysis.md` · `…_remediation_plan.md` · déploiement `docs/operations/ag4_spe_sprint1_deploy.md`
- Specs news : `docs/specs/ag4_spe_v3_ibkr_news.md` (V3 IBKR) · `docs/specs/ag1_v4_d2_news_digest.md` (D2)
- Investigation n8n : `docs/operations/runbook_n8n_investigation.md` · Approbation : `docs/operations/order_approval_deploy_notes.md`
- Accès SSH : `docs/operations/vps-access.md` · Déploiement : `docs/operations/deploy.md` · Env : `docs/operations/env_vars.md` · IBKR : `docs/operations/ibkr_execution.md`
- Vérif rapide (lecture seule) : `scripts/verify_vps_n8n.sh`

## Garde-fous
- Ne jamais afficher/copier de clé privée ni de secret. `.ssh/` reste local (gitignoré).
- Lectures DuckDB **toujours** en `read_only=True` ; ne pas écrire en base directement (sauf script de maintenance dédié).
- **Trading actions en LIVE réel (`U25651155`).** Ne pas modifier les gardes d'exécution (`IBKR_DRY_RUN`, `AG1_ACTIONS_LIVE_ORDERS_ENABLED`, `IBKR_REQUIRE_PAPER_ACCOUNT`, `IBKR_PRICE_GUARD_MAX_DEVIATION_PCT`, `IBKR_APPROVAL_*`) ni réactiver un workflow FX sans décision explicite de Nicolas.
- Ne jamais placer/confirmer un ordre soi-même : laisser n8n + broker + approbation Telegram faire.
- Toute nouvelle version d'un workflow live : valider en shadow/replay avant publication. Déploiement chirurgical (patch ciblé + diff blast-radius), backups dans `.codex-tmp/`.
