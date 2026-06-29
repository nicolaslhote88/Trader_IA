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
État vérifié sur le VPS au **2026-06-18** (AG3 : **2026-06-22** ; univers : **2026-06-24**). Branche repo : `claude/ag4-v3-dualbranch-calib-20260617`.
⚠️ Travaux récents **déployés sur le VPS mais à committer** : (1) `docs/operations/HANDOFF_codex_PR_ag4_spe_v3_20260618.md` ; (2) **AG2→AG1 hybride** (2026-06-19) `docs/operations/20260619_ag2_hybrid_deploy_notes.md` ; (3) **AG3 split + STALE_FUNDA** (2026-06-22) `docs/operations/20260622_ag3_split_stale_funda_deploy_notes.md` ; (4) **Expansion univers +100 + classification** (2026-06-24) `docs/operations/20260624_universe_expansion_100_global_deploy_notes.md` + `scripts/{seed_universe_100_global,classify_universe_100_segments,pin_core_manual_18}.py`.
⚠️ **Session 2026-06-26→28 déployée live, à committer** : (5) **gate liquidité AG1 V4** (preflight warm-up bid/ask IBKR + `SPREAD_UNQUOTED`, safety node) + alignement dashboard ; (6) **AG2 `07_hydrate_ai_cache`** (ne reporte plus un REJECT de cache périmé → SKIP si frais ET non-REJECT) ; (7) **AG3 Held+Core** lit désormais `CORE_MANUAL` (orphelin corrigé) ; (8) **R8 `data_age`** = `max(stocké, âge réel now−bar)` + idem dashboard ; (9) **déconfliction crons anti-contention DuckDB** (cf. `docs/operations/SCHEDULING_AND_LOAD.md`). Détail des fixes : mémoire interne `14_LIQUIDITY_GATE_FIX` / `15_PIPELINE_FRESHNESS_AUDIT` / `16_DUCKDB_CONCURRENCY`.
⚠️ Le working tree a ~200 fichiers en **bruit CRLF** : ne stager QUE les fichiers réellement modifiés (liste dans le handoff).

## État du projet — VÉRIFIÉ sur le VPS le 2026-06-18
- **Actions/ETF : AG1 V4 consensus** est le Portfolio Manager **actif** (consensus 2/3).
  3 modèles : **GPT-5.5** (`gpt-5.5-2026-04-23`), **Grok 4.3** (`grok-4.3`), **Claude Sonnet 4.6** (`claude-sonnet-4-6`).
  `model_keys` persistés : `chatgpt52`, `grok41_reasoning`, **`claude_sonnet46`**. Workflow n8n `AG1V4CONSENSUS`.
  Base `ag1_v4_consensus.duckdb` (ledger v4 : `core.runs/orders/consensus_*/model_proposals/fills/*_mtm_*`). Dashboard Streamlit V4-only (8501).
- Autres workflows actifs : `AG1-PF-V1` (MTM horaire V4), `AG2-V3` (split Held+Core / Watchlist), **AG3 split** (Held+Core / Watchlist, voir ci-dessous), `AG4-V3`, `AG4_Spé-V2`,
  **`AG4_Spé-IBKR-V1`** (news IBKR portfolio, nouveau), **`AG4_Spé — Health Alert`** (nouveau), `YF-ENRICH-V1`.
- **AG2-V3 → AG1 V4 : analyse technique utile au PM (hybride, MAJ 2026-06-19).** Suite à l'audit `docs/audits/20260619_ag2_v3_analyse_pertinence_efficience.md` (le LLM AG2 n'apportait rien de consommé par AG1).
  AG2 (`Extract AI + Write`) persiste désormais `ai_rr_theoretical` (était 100 % NULL). AG1 (`R8` + `Calcul Matrice`) lit `ai_decision`/`ai_quality` :
  **REJECT exclu de « Entrer/Renforcer »** (filtre dur, n'affecte pas les sorties) ; **APPROVE/WATCH pondérés** par qualité (WATCH éligible, poids réduit) ; SKIP/inconnu = neutre.
  Univers AG1 nettoyé (whitelist `EQUITY/ETF/CRYPTO` → retire 78 paires FX legacy). Le SELL reste scanné (REJECT alimente le filtre dur). Détails/rollback : `docs/operations/20260619_ag2_hybrid_deploy_notes.md`.
- **Universe Quarantine (live 2026-06-19).** Workflow `AG2 — Universe Health Quarantine` (`AG2UHQ20260619`, cron 18:35 UTC sem.) audite `ag2_v3.duckdb.universe` et maintient `universe_quarantine`.
  AG2/AG4_Spé excluent les symboles actifs de leur rotation non détenue ; AG1 exclut les entrées/renforts non détenus ; les positions détenues restent surveillées. Run initial : 385 évalués, 131 quarantaines actives. Notes/rollback : `docs/operations/20260619_universe_quarantine_deploy_notes.md`.
- **🗂️ Classification de l'univers — RÈGLE À CONNAÎTRE.** Ajouter un symbole à `ag2_v3.duckdb.universe` **ne suffit pas** : la rotation AG2/AG3 est pilotée par la table **`universe_segments`** (PK `(symbol, segment)`). Segments : `HELD` (détenu, auto), `CORE_AUTO` (top 50, score **dominé par le volume**, exige données YF `quote_ok`+`volume≥20000`), `CORE_MANUAL` (épinglé `source='manual'`, **préservé** des refresh), `WATCHLIST` (défaut entrée neuve). **Qui voit quoi** : `yf-enrichment` + AG1 (R8) lisent **tout `universe`** (R8 exclut juste la quarantaine active) ; **AG2/AG3 ne traitent QUE les symboles présents dans `universe_segments`** (workflows `…Held+Core` = HELD+CORE, `…Watchlist Nightly` = WATCHLIST). Un symbole sans segment ⇒ aucune rotation ⇒ jamais enrichi technique/fondamental. Le refresh `AG2UHQ` (18:35 UTC sem.) reclasse les nouvelles entrées en `WATCHLIST` au run suivant ; pour les voir immédiatement ou les épingler CORE, **classer manuellement**. ⚠️ **Écrire `ag2_v3.duckdb` avec `duckdb==1.4.4`** (lecteur le plus bas = `yf-enrichment` ; ≥1.5 upgrade le format et casse la lecture). Base souvent lockée (dashboard) → retry sur lock.
- **🌍 Expansion univers +100 (live 2026-06-24).** `universe` 463→**563** (US 30 / Europe hors-FR 25 / Asie dév. 23 / Émergents 14 / Canada 8, ADR/US privilégiés). Classés : **18 `CORE_MANUAL`** (ASML, TSM, Samsung, SAP, AMD, NVS, NVO, AZN, UNH, NESN.SW, UL, TM, SIE.DE, RHM.DE, SHEL, RIO, BHP, BABA) + 82 `WATCHLIST`. Backup `ag2_v3.duckdb.bak_20260624_preseed`. Scripts idempotents `scripts/{seed_universe_100_global,classify_universe_100_segments,pin_core_manual_18}.py`. ⚠️ Permissions IBKR places non-US à activer avant ordre réel. Détails/rollback : `docs/operations/20260624_universe_expansion_100_global_deploy_notes.md`.
- **AG2 split rotation (live 2026-06-19).** AG2 généraliste `lUsgEdJODpYh5vt0dQdb2` conservé en rollback mais désactivé. Actifs : `AG2-V3 — Technical Held+Core` (`AG2V3HELDCORE20260619`, 08/12/14h10 UTC sem., held + 18 CORE) et `AG2-V3 — Technical Watchlist Nightly` (`AG2V3WATCHNIGHT20260619`, 02h20 UTC mar-sam., 40 watchlist). Segments dans `ag2_v3.duckdb.universe_segments`.
  **Durcissement Held+Core live 2026-06-24** : checkpoints DuckDB retirés des nœuds par symbole et centralisés dans `Finalize Run`, retries de lock renforcés, cache en safe-SKIP si DuckDB reste indisponible. Shadow sur copie 2,7 Go : init 24–62s→9,4s, hydrate 20–33s→0,45s. Notes : `docs/operations/20260624_ag2_held_core_duckdb_hardening.md`.
- **AG3 split + STALE_FUNDA (live 2026-06-22).** Pilier fondamental (source **unique yfinance**, **zéro LLM**, base `ag3_v2.duckdb`). Suite à l'audit `docs/audits/20260622_ag3_v2_analysis.md` (AG3 pèse 0,30-0,34 du scoring AG1 mais fraîcheur 🔴 15,7j moy / 90% WATCH / 40% sans analystes = small caps FR structurel). Ancien `AG3-V2` (`WZToJYQbJKJBWpUmv9wvc`) **désactivé** (rollback). Actifs, pilotés par `universe_segments` (quarantaine exclue de fait) :
  - `AG3-V2 — Fundamental Held+Core` (`AG3V2HELDCORE20260622`, cron **0 1 * * * UTC quotidien**, segments HELD+CORE_AUTO ≈56, batch 80, SLA ≤24h).
  - `AG3-V2 — Fundamental Watchlist Nightly` (`AG3V2WATCHNIGHT20260622`, cron **0 2 * * * UTC quotidien**, segment WATCHLIST ≈196, batch 60 → cycle ~4j, SLA <5j).
  Builder `agents/trading-actions/AG3-V2/build_split_workflows.py` (part du live export ; `build_workflow.py` est **périmé**=GoogleSheets). **Gate STALE_FUNDA** dans AG1V4 node `R8 — Data Prep for Matrix` : au-delà de `MAX_FUNDA_AGE_HOURS` (env `AG1_ACTIONS_MAX_FUNDA_AGE_HOURS`, défaut 168h) le fondamental est **neutralisé** (Score/Risk→50, Upside/Target→0, `Funda_Usable=False`) + flag `STALE_FUNDA` ; **PAS** un reject dur du risk manager (un funda périmé ne gèle pas le trading). Détails/rollback : `docs/operations/20260622_ag3_split_stale_funda_deploy_notes.md`. **IBKR ne peut pas alimenter les fondamentaux** (Client Portal API : tags fondamentaux dépréciés + notes analystes indisponibles).
- **AG4-V3 : dual-branch.** Node `20CFG - Analysis Mode` (`analysisMode`, défaut `reduced`) → Switch `20H_MODE`.
  `reduced` = Actions via Grok grok-4.3 ; `full` = ancien (gpt-5-mini, réactive le Forex). Détails : `docs/audits/20260617_ag4_v3_news_watcher_audit.md`.
- **⚠️ IBKR mode RÉEL (live).** Compte **`U25651155`**, `dry_run=false`, `AG1_ACTIONS_LIVE_ORDERS_ENABLED=true`. Ordres réels.
- **Forex : entièrement désactivé** (workflows FX `active=0`, `fx_orders_enabled=false`).
- **Login IBKR : assisté quotidiennement à 07:00 Europe/Paris.** Timer systemd `ibkr-daily-auth.timer` → IBeam saisit les credentials et déclenche IB Key ; Nicolas valide sur téléphone dans la fenêtre **07:00–07:30** (tentatives max 07:00/07:10/07:20). Ancien `ibkr-auth-watchdog.timer` désactivé. Login navigateur manuel conservé en fallback. `auto_reauth_enabled=true`. Détails : `docs/operations/20260624_ibkr_daily_assisted_auth.md`.

## 📰 Pipeline NEWS single-stock (AG4_Spé V2/V3 → AG1 V4) — MAJ 2026-06-18
Base **`ag4_spe_v2.duckdb`** (`news_history` + vue **`news_analyzed`** = summary∧is_relevant). Détails : `docs/audits/20260617_ag4_spe_v2_analysis.md`, `…_remediation_plan.md`, `docs/specs/ag4_spe_v3_ibkr_news.md`, `docs/specs/ag1_v4_d2_news_digest.md`.
- **AG4_Spé-V2** (Boursorama, cron 09/12/15h05 UTC sem.) : B1 dates corrigées (`07_parse_article.js`), A1 anti-zombies (`02_start_run.py`), C1/C3 univers actions/ETF (FX exclus, ~385) + rotation priorisée portefeuille + retry S04/S14 (`01_build_symbol_queue.py`). Historique nettoyé (19,5k→3,3k).
- **AG4_Spé-IBKR-V1** (cron 10/13/16h UTC sem.) : news IBKR **portfolio** (positions détenues) via broker `GET /news/portfolio` → même chaîne LLM que V2 → `news_history` (`source='ibkr'`, `provider`, `news_article_id`, `ibkr_sentiment`). L'endpoint IBKR **par-contrat** (`/iserver/news?conid=`) n'est PAS servi (503) → held-only.
- **D2 (dans AG1 V4)** : node **`20K — News Digest (Pack+Held)`** (Calcul Matrice → 20K → Merge7[1]) enrichit `opportunity_pack` : `rows[].news[]` (≤3, 14j) + `held_news` + `news_legend`. Le PM lit `opportunity_pack` (PAS `opportunity_brief`). Budget ~+1,4k tokens.
- **AG4_Spé — Health Alert** (cron 16h30 UTC sem.) : alerte Telegram si pipeline stale / zombies / dates KO.
- **⚠️ News couverture univers global (analyse 2026-06-24, NON déployé).** Boursorama (FR) est insuffisant pour l'extension +100 (US/Asie/EM). **IBKR per-contrat écarté** : `/iserver/news?conid=` répond 503 même contrat armé (gateway 10.46.1m ne sert que `/iserver/news/portfolio`, held-only, provider Benzinga). **Solution retenue = Finnhub** (`company-news`, clé gratuite 60/min) : validé empiriquement **~95/100 à coût nul** via un mapping `symbole local → ticker ADR/OTC US` (NESN.SW→NSRGY, 0700.HK→TCEHY, 005930.KS→SSNLF…). Résidu 5 (ABB, 6861.T, CBA.AX, MQG.AX, O39.SI). **DÉPLOYÉ LIVE 2026-06-24.** (1) **Collecteur** persistant `/opt/trader-ia/finnhub/` (script + venv duckdb **1.4.3** + `run_collector.sh` lit `FINNHUB_TOKEN` de `/docker/yfinance/.env`), **crontab `0 9,12,15 * * 1-5`** → table `news_finnhub_staging` (segments CORE_MANUAL,CORE_AUTO — **HELD exclu** car couvert par IBKR-portfolio ; **cap 12 art./symbole** `--max-per-symbol` pour borner le LLM, 2j). (2) **Workflow n8n `AG4_Spé-Finnhub-V1`** (`id=AG4SPEFINNHUBV1`, **active=1**, schedule `0 0 10,13,16 * * 1-5` UTC) : Load staging → IF → OpenAI gpt-5-mini → Merge → Parse → Write `news_history` (`source='finnhub'`) → vue `news_analyzed` → AG1. Source repo : `agents/trading-actions/AG4-SPE-V2/AG4-SPE-FINNHUB-V1-workflow.json`. ⚠️ **Tout write ag4_spe_v2 doit être en duckdb ≤1.4.3** (lecteur n8n) — venv `/opt/trader-ia/finnhub/venv`, JAMAIS le 1.4.4. ⚠️ Volume par méga-cap élevé (NVDA ~222 art./3j) → coût LLM à surveiller, cap par-symbole = optim future. 5 non couverts : ABB, 6861.T, CBA.AX, MQG.AX, O39.SI. Déploiement/rollback : `docs/operations/20260624_ag4_spe_finnhub_v1_deploy_notes.md`. Scripts probe : `finnhub_news_probe.py`, `finnhub_gap_adr_probe.py`.

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
  **Déployer un workflow** : `n8n import:workflow --input=...` (inclure l'`id` pour MAJ in-place) → `n8n publish:workflow --id=...` → `docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5`. La version **publiée** (`workflow_history` via `activeVersionId`) est celle exécutée, pas `workflow_entity.nodes`. (n8n 2.3.5 : `update:workflow` est **déprécié** ; activer/désactiver via `publish:workflow` / `unpublish:workflow`.)
  - ⚠️ **`import:workflow` DÉSACTIVE le workflow** (« Remember to activate later ») → **toujours republier après import**, sinon un workflow live (ex. AG1V4CONSENSUS) reste OFF. Vérifier `active=1` en base après restart.
  - ⚠️ **Perms `docker cp`** : un fichier `scp` arrive en `600 root` → l'user `node` ne peut pas le lire (`EACCES` à l'import). Faire `chmod 644` sur l'hôte (ou `docker exec -u root … chmod 644`) avant `import:workflow`.
  - ⚠️ **`n8n execute` CLI inutilisable** quand n8n tourne (port task-broker 5679 occupé) → valider un workflow via son cron, pas en CLI.
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
- **⏱️ Ordonnancement & charge système (crons + durées) — SOURCE DE VÉRITÉ** : `docs/operations/SCHEDULING_AND_LOAD.md` (+ frise Gantt 24h `docs/operations/system_load_gantt.html`). **Consulter AVANT toute modif de cron** : tous les crons actifs, durées moyennes observées, base DuckDB par workflow, et la stratégie de déconfliction anti-contention (déployée 2026-06-28). ⚠️ Les horaires de cron cités ailleurs dans ce fichier sont **antérieurs** au déconflictage du 2026-06-28 — se fier à ce doc.
- **AG2→AG1 hybride (2026-06-19)** : audit `docs/audits/20260619_ag2_v3_analyse_pertinence_efficience.md` · déploiement/rollback `docs/operations/20260619_ag2_hybrid_deploy_notes.md` · handoff commit `docs/operations/HANDOFF_codex_PR_ag2_hybrid_20260619.md`
- **Universe Quarantine (2026-06-19)** : déploiement/rollback `docs/operations/20260619_universe_quarantine_deploy_notes.md`
- **AG2 split rotation (2026-06-19)** : déploiement/rollback `docs/operations/20260619_ag2_split_rotation_deploy_notes.md`
- **AG3 audit + split + STALE_FUNDA (2026-06-22)** : audit `docs/audits/20260622_ag3_v2_analysis.md` · déploiement/rollback `docs/operations/20260622_ag3_split_stale_funda_deploy_notes.md`
- **Expansion univers +100 + classification segments (2026-06-24)** : `docs/operations/20260624_universe_expansion_100_global_deploy_notes.md`
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
