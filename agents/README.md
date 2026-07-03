# Agents

Les workflows n8n sont rangés par domaine pour éviter de mélanger les agents transverses, le système actions et le système Forex.

## `common/`

Agents et workflows partagés :

- `AG4-V3/` : news macro globales, geo-tagging et dual-write FX.
- `yf-enrichment-v1/` : enrichissement Yahoo Finance quotidien.

## `trading-actions/`

Agents spécifiques au système actions/ETF/crypto existant :

- `AG1 - Portfolio manager/`
  - `AG1-V4-Consensus Portfolio manager/` : Portfolio Manager actions actif.
  - `AG1-PF-V1/` : mark-to-market portefeuille V4.
  - `AG1-V3-Portfolio manager/` : ancienne génération conservée en historique.
- `AG2 - La technique/`
  - `AG2-V3/` : signaux techniques Held+Core, Watchlist et Universe Quarantine.
- `AG3 - Les fondamentaux/`
  - `AG3-V2/` : fondamentaux Held+Core et Watchlist.
- `AG4 - Les news/`
  - `AG4-SPE-V2/` : news single-stock Boursorama, IBKR, Finnhub et Health Alert.

## `trading-forex/`

Agents spécifiques au système Forex isolé :

- `AG1-FX-V1-Portfolio manager/`
- `AG2-FX-V1/`
- `AG4-FX-V1/`
- `AG4-Forex/`

Les services Docker restent dans `services/` pour l’instant, car `dashboard`, `yfinance-api` et `yf-enrichment-service` sont transverses ou déjà câblés par le `docker-compose`.
