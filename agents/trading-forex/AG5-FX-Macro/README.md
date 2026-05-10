# AG5-FX-Macro — Pilier 1 : Analyse Macro/Flows

## Objectif
Calcule le score macro/flows pour chaque devise G10, basé sur :
- Croissance PIB (QoQ, momentum)
- Politique monétaire (taux directeur vs. neutre)
- Inflation (CPI YoY vs. cible 2%)
- Balance du compte courant (excédent = positif)

## Sources de données
- FRED API (PIB, CPI, CA, taux directeurs)
- Orchestré via `macro-data-api` service

## Schedule n8n
- 1x/jour à 06:00 UTC (après publication données éco matinales)

## Output
- `pillars.currency_scores` dans `macro_data.duckdb` :
  - `macro_growth_score`, `macro_inflation_score`, `macro_policy_score`, `macro_ca_score`
  - `macro_score` composite ∈ [-1, +1]

## Thèse de trading (Philippine Oato)
USD doit scorer très négatif (déficit ~6% PIB, taux en baisse)
JPY, EUR doivent scorer positif (excédents courants, repositionnement)

## Variables d'environnement
- `MACRO_DATA_API_URL` (défaut: http://macro-data-api:8081)
- `MACRO_DUCKDB_PATH` (défaut: /files/duckdb/macro_data.duckdb)
