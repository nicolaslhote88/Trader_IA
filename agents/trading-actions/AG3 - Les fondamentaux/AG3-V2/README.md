# AG3-V2 - Fundamental Analyst (DuckDB-first)

## ⚠️ Workflows actifs depuis 2026-06-22 — SPLIT en 2 (Sprint 1)

Le workflow unique `AG3-V2` (id `WZToJYQbJKJBWpUmv9wvc`) est **désactivé** (gardé en rollback).
Il est remplacé par **deux** workflows pilotés par `ag2_v3.duckdb.universe_segments` (la quarantaine
est donc exclue de fait), sur le modèle du split AG2 :

| Workflow | id n8n | Cron (UTC) | Segments | Batch | SLA fraîcheur | batch_state key |
|---|---|---|---|---|---|---|
| `AG3-V2 — Fundamental Held+Core` | `AG3V2HELDCORE20260622` | `0 1 * * *` (quotidien) | HELD + CORE_AUTO (~56) | 80 | ≤ 24 h | `ag3_v2_held_core_last_index` |
| `AG3-V2 — Fundamental Watchlist Nightly` | `AG3V2WATCHNIGHT20260622` | `0 2 * * *` (quotidien) | WATCHLIST (~196) | 60 | < 5 j (cycle ~4 j) | `ag3_v2_watchlist_last_index` |

- Les deux écrivent dans la **même** base `ag3_v2.duckdb` (historique continu ; AG1/AG2 lisent `v_latest_triage` inchangé).
- Générateur : **`build_split_workflows.py`** (part de l'export live `AG3-V2-workflow.json`).
  ⚠️ `build_workflow.py` est **périmé** (il génère un node Google Sheets) : ne plus l'utiliser.
- Le node `AG3V2.01 - Read Universe` filtre via sous-requête
  `symbol IN (SELECT symbol FROM universe_segments WHERE active AND segment IN (...))`.
- Côté consommateur, AG1 V4 (`R8 — Data Prep for Matrix`) applique une **gate STALE_FUNDA**
  (`MAX_FUNDA_AGE_HOURS`, défaut 168 h) : un fondamental périmé est neutralisé (Score/Risk→50,
  Upside/Target→0) pour ne pas piloter une décision (poids 0,30-0,34).
- Audit : `docs/audits/20260622_ag3_v2_analysis.md`. Déploiement/rollback :
  `docs/operations/20260622_ag3_split_stale_funda_deploy_notes.md`.
- **Données** : IBKR ne peut PAS alimenter les fondamentaux (Client Portal API : tags fondamentaux
  dépréciés, notes analystes indisponibles). Source reste yfinance ; le trou analystes sur les small
  caps FR est structurel (aucun sell-side).

## Goal
Provide a reliable fundamental workflow for equities, with persistent outputs in DuckDB:
- `fundamentals_snapshot` (raw yfinance payload, normalized per symbol/run)
- `fundamentals_triage_history` (score, risks, thesis, horizon)
- `analyst_consensus_history` (target prices and recommendation proxy)
- `fundamental_metrics_history` (normalized metric rows)
- `run_log` (run lifecycle and counters)

## Why this V2
The previous Boursorama HTML parsing approach is fragile (layout changes, anti-bot walls, intermittent blocks).
V2 is API-first (yfinance) and DuckDB-first for reliability, traceability, and query performance.

## Flow
1. Load `Universe` from DuckDB (`/files/duckdb/ag2_v3.duckdb`, table `universe`).
2. Build symbol queue (`Symbol`, optional `BoursoramaRef`).
3. Fetch `/fundamentals?symbol=...` from `yfinance-api`.
4. Compute:
   - `Score` (0-100)
   - `risk_score` (0-100)
   - bull/bear thesis
   - valuation scenarios (`Bear/Base/Bull`)
   - horizon (`SWING` / `LONG_TERM` / `WATCH`)
5. Upsert rows into DuckDB.
6. On loop `done`, finalize run statistics in `run_log`.

## Files
- `AG3-V2/build_workflow.py`: workflow generator.
- `AG3-V2/nodes/`: JS logic for context, queue, scoring and row preparation.

## Universe Runtime Source

Since 2026-06-14, the active n8n workflow no longer reads the Google Sheets
`Universe` tab. The runtime source of truth is `ag2_v3.duckdb.main.universe`,
shared with AG2 and AG4_Spe.

Since 2026-06-22 (split), each workflow additionally **filters by
`ag2_v3.duckdb.main.universe_segments`** (segment HELD/CORE_AUTO vs WATCHLIST),
so quarantined symbols are excluded by construction. Freshness therefore depends
on `universe_segments` being kept up to date by the `AG2 — Universe Health
Quarantine` workflow.
- `AG3-V2/AG3-V2-workflow.json`: legacy single-workflow export (base for the split builder; now inactive).
- `AG3-V2/AG3-V2-Fundamental-Held-Core.workflow.json` / `…-Watchlist-Nightly.workflow.json`: active split workflows.

## Generate workflow JSON
From `AG3-V2/`, régénérer les **deux** workflows actifs :

```bash
python build_split_workflows.py
# -> AG3-V2-Fundamental-Held-Core.workflow.json
# -> AG3-V2-Fundamental-Watchlist-Nightly.workflow.json
```

⚠️ `build_workflow.py` (ancien générateur mono-workflow) est **périmé** : il émet un node Google
Sheets alors que le live lit DuckDB. Ne pas l'utiliser ; il est conservé pour référence historique.

## Runtime requirements
- `yfinance-api` service reachable (default: `http://yfinance-api:8080`).
- `duckdb` available in Python runner.
- Google Sheets credential only for reading `Universe`.
- DuckDB volume mounted (default path: `/files/duckdb/ag3_v2.duckdb`).

## Notes
- This V2 intentionally avoids hard dependency on Boursorama page parsing.
- No AG3 write-back to Google Sheets anymore.
- `Split In Batches` wiring is explicit:
  - output `main[0]` = done branch (`Finalize Run`)
  - output `main[1]` = loop branch (fetch/process/write)
