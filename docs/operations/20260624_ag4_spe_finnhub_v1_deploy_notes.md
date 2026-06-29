# AG4_Spé-Finnhub-V1 — News globale (déploiement)

Date : 2026-06-24. **Statut : DÉPLOYÉ LIVE.** Workflow `AG4SPEFINNHUBV1` active=1 (schedule 10/13/16h UTC), collecteur cron `/opt/trader-ia/finnhub/` (09/12/15h UTC). 1er run auto = 16:00 UTC sur 408 articles seedés.

## Pourquoi
Boursorama (FR) insuffisant pour l'extension +100 (US/Asie/EM). IBKR per-contrat écarté (503).
Finnhub `company-news` (gratuit, 60/min) couvre ~95/100 via mapping `symbole → ticker ADR/OTC US`.

## Architecture (contrainte sandbox : pas de réseau/urllib dans les nodes Python n8n)
1. **Collecte hors n8n** : `scripts/finnhub_news_collector.py` (cron VPS) → table **`news_finnhub_staging`**
   dans `ag4_spe_v2.duckdb`. Mapping ticker validé inclus (29/34 cotations locales via ADR/OTC).
2. **Analyse n8n** : workflow **`AG4_Spé-Finnhub-V1`** (`agents/trading-actions/AG4-SPE-V2/AG4-SPE-FINNHUB-V1-workflow.json`),
   clone de IBKR-V1. Node `Load+Normalize Finnhub Staging` lit le staging (dédup vs `news_history`) →
   `S18 IF → S19 OpenAI (gpt-5-mini, schéma specific_stock_news_v2) → S19M Merge → S20 Parse → Write Finnhub DuckDB`
   → `news_history` (`source='finnhub'`, status ANALYZED/SKIPPED) → vue `news_analyzed` → AG1.

## ⚠️ Piège VERSION duckdb (critique)
`ag4_spe_v2.duckdb` est lu par n8n/task-runners en **duckdb 1.4.3**. Le collecteur cron doit écrire
le staging avec **duckdb ≤ 1.4.3** (créer `/tmp/ddb143` : `python3 -m venv /tmp/ddb143 && /tmp/ddb143/bin/pip install duckdb==1.4.3`).
**Ne PAS** utiliser le venv `/tmp/ddb144` (1.4.4) sur la base live ag4 → risque d'upgrade de format cassant n8n.
(Le shadow test a utilisé 1.4.4 sur une COPIE jetable `/tmp/ag4_shadow.duckdb` — sans risque.)

## Clé Finnhub
Poser dans `/docker/yfinance/.env` : `FINNHUB_TOKEN=...` (clé gratuite finnhub.io). Le collecteur lit l'env.

## Déploiement (à exécuter après validation)
### 1. Collecteur (cron)
```bash
# venv 1.4.3 pour ag4
python3 -m venv /tmp/ddb143 && /tmp/ddb143/bin/pip install duckdb==1.4.3
# 1er run borné (3 jours) pour limiter le volume LLM initial
FINNHUB_TOKEN=xxx /tmp/ddb143/bin/python /tmp/finnhub_news_collector.py \
  --segments CORE_MANUAL,CORE_AUTO,HELD --days 3 --target staging
# puis cron (ex. 08/12/15h UTC, avant l'analyse n8n)
```
### 2. Workflow n8n
```bash
scp AG4-SPE-FINNHUB-V1-workflow.json -> VPS ; chmod 644
docker cp ... root-n8n-1:/tmp/ ; docker exec root-n8n-1 n8n import:workflow --input=/tmp/AG4-SPE-FINNHUB-V1-workflow.json
docker exec root-n8n-1 n8n publish:workflow --id=AG4SPEFINNHUBV1   # import DESACTIVE -> republier
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
# verifier active=1
```
### 3. 1er run contrôlé
Trigger manuel, surveiller : volume articles, coût OpenAI, lignes `news_history` `source='finnhub'` status ANALYZED,
apparition dans la vue `news_analyzed`, consommation par AG1 (R8/20K).

## Shadow validé (2026-06-24)
Copie `/tmp/ag4_shadow.duckdb` : collecteur → 187 articles staging (15/18 CORE_MANUAL, 3j) ;
node Load → 187 items émis, format conforme (symbol, source=finnhub, llmInput, _runAI), dédup OK.

## Rollback
- Workflow : `n8n unpublish:workflow --id=AG4SPEFINNHUBV1` (jamais publié = rien à faire).
- Données : `DELETE FROM news_history WHERE source='finnhub'` ; `DROP TABLE news_finnhub_staging`.
- Boursorama + IBKR-portfolio restent inchangés (sources additives).

## Décisions ouvertes
- Périmètre rotation (CORE+HELD fréquent / WATCHLIST nightly) + fréquence → dimensionne le coût OpenAI.
- Garder Boursorama pour FR (oui, additif) ; Finnhub pour le reste.
- Résidu 5 non couverts (ABB, 6861.T, CBA.AX, MQG.AX, O39.SI) : tickers OTC alternatifs ou source d'appoint.
