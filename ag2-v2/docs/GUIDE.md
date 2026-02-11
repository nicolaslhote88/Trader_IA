# AG2-V2 — Technical Analyst Pipeline

## Vue d'ensemble

Un seul workflow n8n remplaçant les 2 workflows V1 (étape 1 + étape 2).
Stockage DuckDB au lieu de Google Sheets (sauf lecture Universe).

### V1 → V2 : ce qui change

| Aspect | V1 | V2 |
|--------|----|----|
| Workflows | 2 (étape 1 + étape 2) | **1 seul unifié** |
| Nodes | 36 | **20** |
| Stockage output | Google Sheets (85 cols) | **DuckDB** (SQL, indexé, rapide) |
| Stockage cache | Google Sheets `ag2_ai_cache` | **DuckDB** `ai_dedup_cache` |
| Traçabilité runs | Aucune (Run_ID vide) | **Table `run_log`** complète |
| Port yfinance-api | 8080 OU 8000 (bug C1) | **8080 fixe** |
| RSI | Simple average | **Wilder smoothing** |
| EMA init | values[0] | **SMA warmup** |
| Scoring | Asymétrique (BUY≥2, SELL≤-1) | **Symétrique (±2)** |
| Support/Résistance | max(highs[-50:]) naïf | **Pivot-based** (swing H/L) |
| Indicateurs | 15 | **23** (+Bollinger, Stoch, ADX, OBV) |
| Vectorisation | Insert-only (doublons) | **Upsert** via ID |
| Vectorisation bug | Seul le dernier marqué DONE | **Tous marqués** |
| Code Python | Dupliqué H1/D1 | **Fonction unique** |
| Cooldown | Global | Per-symbol (via yfinance-api v2) |
| Fetch HTTP | 6 nodes (Set+HTTP+Merge ×2) | **1 node** (JS `$http.request`) |

## Architecture du workflow

```
Cron (9h10-17h10 lun-ven) ──┐
Manual Trigger ──────────────┤
                             ▼
              Read Universe (Google Sheets)
                             │
              Init Config + Batch (25 symboles, round-robin)
                             │
              DuckDB Init Schema
                             │
              ╔══════════════╧══════════════╗
              ║     Loop Symbols (1×1)      ║
              ║                             ║
              ║  Fetch H1+D1 (JS, parallel) ║
              ║         │                   ║
              ║  Compute+Filter+Dedup+Write ║
              ║  (Python, DuckDB)           ║
              ║         │                   ║
              ║    ┌────┴────┐              ║
              ║    │ IF AI?  │              ║
              ║    ▼         └──→ Loop      ║
              ║  Snapshot Context            ║
              ║    │                         ║
              ║  AI Validation (GPT-4o-mini) ║
              ║    │                         ║
              ║  Extract AI + Write DuckDB   ║
              ║    │                         ║
              ║    ┌────┴────┐              ║
              ║    │IF Vector│              ║
              ║    ▼         └──→ Loop      ║
              ║  Prep Text                  ║
              ║    │                         ║
              ║  Qdrant Upsert (embeddings) ║
              ║    │                         ║
              ║  Mark Vectorized (DuckDB)   ║
              ║    └──→ Loop                ║
              ╚═══════════╤═════════════════╝
                          │ (done)
              Finalize Run (DuckDB run_log)
```

## Fichiers

```
ag2-v2/
├── build_workflow.py          # Générateur du JSON n8n
├── AG2-V2-workflow.json       # JSON importable dans n8n
├── sql/
│   └── schema.sql             # Schéma DuckDB complet (avec vues)
├── python/
│   ├── indicators.py          # Moteur d'indicateurs (référence, testable)
│   └── duckdb_ops.py          # Opérations DuckDB (référence)
├── nodes/                     # Code source de chaque node n8n
│   ├── 01_init_config.js
│   ├── 02_duckdb_init.py
│   ├── 03_fetch_data.js
│   ├── 04_compute.py          # Moteur complet + filter + dedup + write
│   ├── 05_snapshot.js
│   ├── 06_extract_ai.py
│   ├── 08_prep_vector.js
│   ├── 09_mark_vector.py
│   └── 10_finalize.py
└── docs/
    └── GUIDE.md               # Ce fichier
```

## Déploiement

### 1. Préparer le VPS

```bash
# Créer le dossier DuckDB (sera accessible par task-runners via /files)
sudo mkdir -p /local-files/duckdb
sudo chown 1000:1000 /local-files/duckdb
```

### 2. Importer le workflow dans n8n

1. Ouvrir n8n → Menu → Import from File
2. Sélectionner `AG2-V2-workflow.json`
3. Vérifier que les credentials sont associés :
   - Google Sheets OAuth2
   - OpenAI API
   - Qdrant API
4. Désactiver les workflows V1 (AG2 étape 1 + AG2 étape 2)
5. Activer le workflow V2

### 3. Premier lancement

1. Cliquer "Execute Workflow" (trigger manuel)
2. Vérifier les logs dans l'exécution n8n
3. Vérifier DuckDB :
   ```bash
   docker exec -it task-runners python3 -c "
   import duckdb
   con = duckdb.connect('/files/duckdb/ag2_v2.duckdb')
   print(con.execute('SELECT count(*) FROM technical_signals').fetchone())
   print(con.execute('SELECT * FROM run_log ORDER BY started_at DESC LIMIT 1').fetchone())
   "
   ```

### 4. Vérifier Qdrant

```bash
curl -s http://localhost:6333/collections/financial_tech_v1 | jq .result.points_count
```

## Indicateurs techniques V2

| Indicateur | V1 | V2 | Correction |
|---|---|---|---|
| RSI(14) | Simple avg | **Wilder smoothing** | Fix M1 |
| EMA(12,26) | Init=values[0] | **SMA warmup** | Fix M3 |
| MACD/Signal/Hist | Via EMA buggées | **Via EMA corrigées** | Fix M3 |
| SMA(20,50,200) | OK | OK | — |
| ATR(14) | Simple avg | **Wilder smoothing** | Amélioration |
| Volatilité ann. | bpd=7 hardcodé | **8h/day (Europe)** | Fix D8 |
| Support | — | **Pivot swing lows** | Fix M5 |
| Résistance | max(highs[-50:]) | **Pivot swing highs** | Fix M5 |
| Bollinger Bands | — | **SMA20 ± 2σ** | Nouveau |
| Stochastic(14,3) | — | **%K et %D** | Nouveau |
| ADX(14) | — | **Wilder smoothed** | Nouveau |
| OBV Slope(20) | — | **Régression normalisée** | Nouveau |

## Scoring V2 (symétrique)

| Condition | Score |
|---|---|
| Prix > SMA50 | +1 |
| Prix < SMA50 | -1 |
| SMA50 > SMA200 | +1 |
| SMA50 < SMA200 | -1 |
| MACD Hist > 0 | +1 |
| MACD Hist < 0 | -1 |
| RSI < 30 | +1 |
| RSI > 70 | -1 |
| Stoch %K < 20 | +1 |
| Stoch %K > 80 | -1 |
| Prix sur BB basse | +1 |
| Prix sur BB haute | -1 |

**Score ≥ +2** → BUY | **Score ≤ -2** → SELL | sinon → NEUTRAL
**Plage** : -6 à +6 (vs -4 à +4 en V1)

## Vues SQL utiles

```sql
-- Derniers signaux par symbole (pour AG1)
SELECT * FROM v_latest_signals;

-- Signaux prêts pour vectorisation
SELECT * FROM v_pending_vectors;

-- Résumé pour Portfolio Manager
SELECT * FROM v_ag1_summary;

-- Historique des runs
SELECT * FROM run_log ORDER BY started_at DESC LIMIT 10;
```

## Rollback vers V1

Si problème avec la V2 :
1. Désactiver le workflow V2 dans n8n
2. Réactiver les workflows V1 (étape 1 + étape 2)
3. Les données V1 (Google Sheets) sont intactes — la V2 écrit dans DuckDB uniquement
