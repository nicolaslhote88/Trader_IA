# HANDOFF Codex — commit de la session audit + fixes 2026-07-02

**Objectif :** committer sur `codex/live-trading-sync-20260629` tout ce qui a été déployé live le 2026-07-02 (et les restes non committés du 29-30/06), SANS embarquer le bruit CRLF (~230 fichiers).

**Références :** audit `docs/audits/20260702_audit_complet_projet.md` · déploiement/rollback `docs/operations/20260702_fixes_deploy_notes.md`.

## ⚠️ Règles de staging
1. Ne stager QUE les fichiers listés ci-dessous (chemins exacts, attention aux espaces dans les noms).
2. Vérifier avant chaque commit : `git diff --cached --ignore-cr-at-eol --stat` — tout fichier apparaissant avec 0 insertion/0 délétion réelle est du bruit CRLF à retirer du stage.
3. Ne PAS lancer de `git add -A` / `git add .`.
4. Le bruit CRLF résiduel fera l'objet d'un traitement séparé (proposition : `.gitattributes` `* text=auto` + `git add --renormalize .` dans un commit dédié, décision Nicolas).

## Commit 1 — sync live 29-30/06 (risk-score V2 + stop-fallback ATR)
```
git add "agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/nodes/pre_agent/calcul_matrice_briefing.code.py"
git add services/dashboard/app.py
```
Message proposé :
```
fix(ag1/dashboard): risk score V2 (renorm composantes observees) + stop fallback >= plancher ATR

Sync du live deploye 2026-06-29/30 (memoire 19/22) : risk_score V2 renormalise
sur composantes observees + reponderation tactique ; fallback stop support*0.998
borne au plancher ATR (fin des micro-stops 0,2-1%). Dashboard resynchronise
depuis /opt/trading-dashboard/app/app.py (source de verite live, 20 729 lignes).
```

## Commit 2 — broker F1 + F5
```
git add services/ibkr-broker/app.py
```
Message proposé :
```
fix(broker): auto-confirm du prompt IBKR "without market data" borne par le price-guard (F1) + approbations idempotentes EXPIRED/NOT_FOUND (F5)

F1 : sans souscription market data US, chaque ordre US finissait parque puis
expire (TTL 600s) -> 1 seul fill US depuis le 18/06. Le prompt qualifie
(hors margin/short/restricted) passe desormais par la meme verification prix
yfinance : ecart limit<->ref <= 5% -> auto-confirm ; sinon parcage inchange.
Flag IBKR_AUTO_CONFIRM_NO_MARKET_DATA_PROMPT (true en prod, defaut code false).
F5 : tap Telegram apres TTL (EXPIRED) ou restart broker (NOT_FOUND) -> 200
idempotent au lieu de 409 (le workflow Decide ne part plus en erreur).
Deploye live 2026-07-02 (build ibkr-broker), /health verifie.
```

## Commit 3 — AG4_Spé F2 (dates)
```
git add agents/trading-actions/AG4 - Les news/AG4-SPE-V2/nodes/04_normalize_articles.js
git add agents/trading-actions/AG4 - Les news/AG4-SPE-V2/nodes/07_parse_article.js
git add agents/trading-actions/AG4 - Les news/AG4-SPE-V2/nodes/12_write_news_duckdb.py
```
Message proposé :
```
fix(ag4-spe): parseListingDate mutilait les dates ISO -> published_at 2029/2030 (F2)

La regex FR non ancree matchait "2026-06-29" comme 26/06/29 -> 2029-06-26 ;
le garde B1 nullifiait mais S16 reinstaurait via `|| j.publishedAt`.
Fix : ISO-first + regex \b..\b + clamp [now-2ans;now+7j] (S07), suppression
du bypass (S16), clamp ecriture published_at>now+24h -> NULL (S22).
Donnees : 235 lignes reparees par transformation inverse, backup
news_history_date_repair_20260702. Deploye live (import+publish) 2026-07-02.
```

## Commit 4 — docs, ops, AGENTS.md
```
git add AGENTS.md
git add docs/audits/20260702_audit_complet_projet.md
git add docs/architecture/etat_des_lieux.md
git add docs/operations/SYSTEM_LINKS_AND_PARITY.md
git add docs/operations/SCHEDULING_AND_LOAD.md
git add docs/operations/20260702_fixes_deploy_notes.md
git add docs/operations/HANDOFF_codex_PR_fixes_20260702.md
```
Message proposé :
```
docs: audit complet 2026-07-02 + analyse fonctionnelle reecrite + MAJ operations

- Rapport d'audit F1-F10 (etat live verifie : broker, n8n, DuckDB, portefeuille)
- docs/architecture/etat_des_lieux.md reecrit integralement (ere AG1 V4 / IBKR live,
  encodage UTF-8 repare) ; ancien etat des lieux 2026-03-02 remplace
- SYSTEM_LINKS_AND_PARITY.md enfin track (source de verite parite dashboard<->AG1)
- SCHEDULING_AND_LOAD.md : MTM H+15 (F4), timeout runner 1200s cote task-runners (F3)
- AGENTS.md : etat 2026-07-02, fixes F1-F5 live, bug "3 champs NULL" clos,
  regle 78 paires FX:* sans segment (volontaire), nouveaux pieges dev
```

## Hors repo (déjà déployé, rien à committer, pour info)
- `/docker/yfinance/.env` : + `IBKR_AUTO_CONFIRM_NO_MARKET_DATA_PROMPT=true`
- `/docker/root/docker-compose.yml` : + `N8N_RUNNERS_TASK_TIMEOUT=1200` (task-runners) — ⚠️ si `infra/vps_hostinger_config/docker-compose.yml` doit rester un miroir, reporter la ligne (le miroir repo est de toute façon daté ; à trancher séparément).
- Workflows n8n publiés : `AG4_Spé-V2` (S07/S16/S22), `AG1-PF-V1` (cron `0 15 9-17 * * 1-5`).
- Table de backup `ag4_spe_v2.news_history_date_repair_20260702`.

## Optionnel (si Codex veut être exhaustif)
- Rafraîchir l'export repo `agents/trading-actions/AG4 - Les news/AG4-SPE-V2/AG4-SPE-V2-workflow.json` depuis le live (`n8n export:workflow --id=H0cfY1coMx8dvMuXScMc_`) pour que le JSON complet reflète S07/S16/S22 — les fichiers nodes/ ci-dessus sont déjà le miroir canonique.
- Idem `agents/trading-actions/AG1 - Portfolio manager/AG1-PF-V1/AG1-PF-V1-workflow.json` (cron H+15).

## Vérifications post-commit
1. `git log --oneline -5` : 4 commits au-dessus de `06ab868`.
2. `git status --porcelain | grep -v "^ M"` : plus d'untracked pertinent.
3. Le diff cumulé ne doit contenir AUCUN fichier purement CRLF : `git diff 06ab868..HEAD --ignore-cr-at-eol --stat` ≈ 13 fichiers.
