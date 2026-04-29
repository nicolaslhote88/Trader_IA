# Agents

Les workflows n8n sont rangés par domaine pour éviter de mélanger les agents transverses, le système actions et le système Forex.

## `common/`

Agents et workflows partagés :

- `AG4-V3/` : news macro globales, geo-tagging et dual-write FX.
- `yf-enrichment-v1/` : enrichissement Yahoo Finance quotidien.

## `trading-actions/`

Agents spécifiques au système actions/ETF/crypto existant :

- `AG1-V3-Portfolio manager/`
- `AG1-PF-V1/`
- `AG2-V3/`
- `AG3-V2/`
- `AG4-SPE-V2/`

## `trading-forex/`

Agents spécifiques au système Forex isolé :

- `AG1-FX-V1-Portfolio manager/`
- `AG2-FX-V1/`
- `AG4-FX-V1/`
- `AG4-Forex/`

Les services Docker restent dans `services/` pour l’instant, car `dashboard`, `yfinance-api` et `yf-enrichment-service` sont transverses ou déjà câblés par le `docker-compose`.
