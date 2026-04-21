# outils/ — Workflows n8n ponctuels / inactifs

Ce dossier regroupe les workflows n8n qui **ne tournent pas en production** sur l'ordonnancement standard et qui servent à exécuter des actions spécifiques et ponctuelles (import / bootstrap d'univers, tests manuels, utilitaires, diagnostics).

À l'inverse de ce qu'on trouve à la racine (`AG1-V3-Portfolio manager/`, `AG2-V3/`, `AG3-V2/`, `AG4-V3/`, `AG4-SPE-V2/`, `AG1-PF-V1/`, `yf-enrichment-v1/`), les workflows listés ici :

- ne sont pas déclenchés par un cron actif ;
- ne sont pas essentiels au cycle quotidien de trading ;
- peuvent être importés à la demande dans n8n pour régénérer un snapshot, tester un pipeline, ou rejouer une extraction.

## Contenu

### `AG0-V1 - extraction universe/`

Workflow d'extraction de l'univers d'investissement depuis des sources externes (tickers, métadonnées, secteurs). Historiquement utilisé pour bootstrapper la Google Sheets d'univers qui alimente AG2/AG3/AG4 ; aujourd'hui désactivé dans n8n — l'univers est maintenu directement via la feuille Google Sheets.

## Convention

- Un workflow ajouté à `outils/` implique qu'il n'est plus référencé par `docker-compose.yml`, par un cron n8n actif, ni par un pipeline de production.
- Si un workflow sort de `outils/` pour revenir en production, pensez à le re-documenter dans `docs/architecture/etat_des_lieux.md`.
