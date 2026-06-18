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
État vérifié sur le VPS au **2026-06-17**. Repo non commité : branche `claude/order-approval-system-20260616`,
PR prête dans `docs/PR_order_approval_20260616.md` (à committer côté Windows).

## État du projet — VÉRIFIÉ sur le VPS le 2026-06-16
- **Actions/ETF : AG1 V4 consensus** est le Portfolio Manager **actif** (consensus 2/3).
  Les 3 modèles réels : **GPT-5.5** (`gpt-5.5-2026-04-23`, Responses API), **Grok 4.3** (`grok-4.3`),
  **Claude Sonnet 4.6** (`claude-sonnet-4-6`). **Gemini a été retiré** et remplacé par Claude Sonnet 4.6.
  `model_keys` persistés (historiques) : `chatgpt52`, `grok41_reasoning`, **`claude_sonnet46`**.
  Workflow n8n `AG1V4CONSENSUS`. Base `ag1_v4_consensus.duckdb` (ledger v4 : `core.runs`, `core.orders`,
  `core.consensus_decisions`, `core.consensus_votes`, `core.model_proposals`, `core.fills`, `core.*_mtm_*`).
  Dashboard Streamlit = V4-only (8501).
- Autres workflows actifs : `AG1-PF-V1` (MTM horaire V4), `AG2-V3`, `AG3-V2`, `AG4-V3`, `AG4_Spé-V2`, `YF-ENRICH-V1`.
- **AG4-V3 (MAJ 2026-06-17) : dual-branch.** Node Set `20CFG - Analysis Mode` (`analysisMode`, défaut
  **`reduced`**) → Switch `20H_MODE` : `reduced` = analyse Actions via **Grok grok-4.3** (`20H1R`, HTTP
  `api.x.ai`, cred `xAiApi`, schéma 9 champs, calibré) ; `full` = ancienne analyse Actions+Forex (gpt-5-mini,
  16 champs, inchangée) — basculer `analysisMode=full` pour réactiver le Forex. `impact_score` calibré
  (cap par magnitude + confiance), pré-filtre LLM dans `20G2`, `source` redérivée de l'URL. Détails +
  rollback : `docs/audits/20260617_ag4_v3_news_watcher_audit.md` §8. Maintenance base :
  `scripts/ag4_duckdb_maintenance.py`. Backup pré-modif : `.codex-tmp/backups/ag4v3_live_20260617.json`.
- **⚠️ IBKR est en mode RÉEL (live) — ASSUMÉ.** Compte **`U25651155`**, `dry_run=false`,
  `AG1_ACTIONS_LIVE_ORDERS_ENABLED=true`, `IBKR_REQUIRE_PAPER_ACCOUNT=false`. AG1 V4 envoie des ordres réels.
- **Forex : entièrement désactivé** (tous workflows FX `active=0`, `fx_orders_enabled=false`). Bases FX figées.
- **Login IBKR : manuel** (navigateur + 2FA, IBeam désactivé), `auto_reauth_enabled=true`.

## 🆕 Système d'approbation des ordres hors-bande (LIVE depuis 2026-06-16)
But : un ordre dont le prix limite s'écarte trop du marché n'est plus rejeté sec → Nicolas valide depuis Telegram.
Bandes de déviation (limit vs quote yfinance) :
- **≤ 5 %** → auto-confirmé (price-guard ; seuil relevé de 3 à 5 %, âge quote 8 h→1 h).
- **5 %–15 %** → ordre **parqué**, notif **Telegram** (bot **@CYROLAS_BOT**) dans le groupe « Gestion outils atelier »
  (`chat_id -4887456379`), boutons **Approuver/Rejeter** → re-price + re-soumission, ou annulation ; TTL 10 min.
- **> 15 %** → rejeté.
- **v2 (2026-06-16)** : les cas **prix non vérifiable** (`QUOTE_TOO_OLD`/`NO_REFERENCE_PRICE`, ex. action US hors
  séance) sont **aussi parqués pour approbation** (plus de rejet sec) ; le guard tente une **réf. IBKR de secours**
  (`marketdata_snapshot`) quand yfinance est périmé. `IBKR_APPROVAL_ENABLED=true`.
Détails complets : `docs/operations/order_approval_deploy_notes.md`. Code : `services/ibkr-broker/approval.py`
+ hook/endpoints/guard dans `app.py`. Workflows n8n : `AG1 V4 — Order Approval Request` / `… Decide` (actifs).

## VPS / infra — VÉRIFIÉ
- VPS Hostinger `srv961978` (alias `ssh vps` → `root@82.112.242.251`, Ubuntu 24.04).
  Clé : `.ssh/codex_vps_tailscale_ed25519` (dossier `.ssh/` **local, gitignoré**).
- **Deux stacks compose distinctes** :
  - **n8n** : projet `root`, **`/docker/root`**. Containers `root-n8n-1`, `root-task-runners-3/4/5`,
    `root-trading-dashboard-1`, `root-traefik-1`, `root-toolbox-1`.
  - **IBKR/yfinance** : projet `yfinance`, **`/docker/yfinance`**. Containers `ibkr-broker`, `ibkr-gateway`,
    `yfinance-api`, `yf-enrichment`, `macro-data-api`. ⚠️ `.env` + price-guard + approval ici, **pas** dans `/docker/root`.
- **Broker = image baked** (pas de bind-mount) : source host `/opt/trader-ia/services/ibkr-broker/`.
  Déployer une modif broker = éditer la source → `cd /docker/yfinance && docker compose build ibkr-broker && docker compose up -d ibkr-broker`.
  ⚠️ `/opt/trader-ia` n'est **pas** un clone git → **committer les changements broker dans ce repo** (sinon perdus au prochain build).
- **n8n = instance PARTAGÉE** (Trader_IA + SIGA + templates). Filtrer sur `AG*`/`YF*`.
  Exécutions n8n en **SQLite** `/home/node/.n8n/database.sqlite` (`root-n8n-1` a python3 3.12 + sqlite3).
  Activer/modifier un workflow via CLI nécessite un **`docker restart root-n8n-1`** pour (ré)enregistrer les webhooks (~60 s).
- **DuckDB** (vérité métier) sur l'hôte dans **`/local-files/duckdb/`** (= `/files/duckdb/` côté containers).
- **Telegram** : 1 seul credential n8n (label « Jarvis » mais bot réel = **@CYROLAS_BOT**). Consommateur d'updates
  actif = `SIGA - Telegram Collector Buffer v2` → ne pas ajouter de 2ᵉ Telegram Trigger (utiliser des boutons URL + Webhook).

## ⚠️ Bugs / issues ouverts
- **Writer DuckDB ne mappe pas 3 champs** : sur le run 18961, `core.runs.strategy_version`, `prompt_version`
  et `n8n_execution_id` restent **NULL** alors que les données sont présentes en amont (`run.strategyVersion`,
  `run.promptVersion`, `run.executionId`). `config_version` est écrit correctement. ⇒ mismatch camelCase↔snake_case
  dans `nodes/post_agent/08_build_duckdb_bundle.code.js` / `09_upsert_run_bundle_duckdb.code.py`. À corriger.
- **Session IBKR peut sauter la nuit** (`CPAPI HTTP 401`, ex. 2026-06-17 matin). Relogin requis
  (`ssh -L 5000:localhost:5000 vps` + `https://localhost:5000` + 2FA) sinon aucun ordre ne part.
- Approbation : le chemin **approve → re-price → fill** n'a pas encore été exercé sur un vrai ordre (échoue « fermé »).
- Store des ordres en attente = **en mémoire** (perdu au restart broker). Audit DuckDB = évolution future.

### Docs de référence
- Investigation n8n : `docs/operations/runbook_n8n_investigation.md`
- Approbation ordres (déploiement + activation) : `docs/operations/order_approval_deploy_notes.md`
- Audit du brief LLM AG1 V4 : `docs/audits/20260615_ag1_v4_prompt_audit.md`
- Accès SSH : `docs/operations/vps-access.md` · Déploiement : `docs/operations/deploy.md`
- Variables d'env : `docs/operations/env_vars.md` · Santé/auth IBKR : `docs/operations/ibkr_execution.md`
- Vérif rapide (lecture seule) : `scripts/verify_vps_n8n.sh`

## Garde-fous
- Ne jamais afficher/copier de clé privée ni de secret. `.ssh/` reste local (gitignoré).
- Lectures DuckDB **toujours** en `read_only=True` ; ne pas écrire en base directement.
- **Trading actions en LIVE réel et assumé (`U25651155`).** Ne pas modifier les gardes d'exécution
  (`IBKR_DRY_RUN`, `AG1_ACTIONS_LIVE_ORDERS_ENABLED`, `IBKR_REQUIRE_PAPER_ACCOUNT`, `IBKR_PRICE_GUARD_MAX_DEVIATION_PCT`,
  `IBKR_APPROVAL_*`) ni réactiver un workflow FX sans décision explicite de Nicolas.
- Ne jamais placer/confirmer un ordre soi-même : laisser les workflows n8n, le broker, et l'approbation Telegram faire.
- Toute nouvelle version d'un workflow live : valider en shadow/replay avant publication.
