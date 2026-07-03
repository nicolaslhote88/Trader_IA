# outils/ — Outils ponctuels, diagnostics et scripts

Ce dossier regroupe les éléments qui ne sont pas des agents de production :
workflows n8n ponctuels, scripts d'administration, probes, diagnostics et aides
de maintenance.

À l'inverse des agents rangés dans `agents/`, les éléments listés ici :

- ne sont pas déclenchés par un cron actif ;
- ne portent pas seuls un pilier fonctionnel AG1/AG2/AG3/AG4 ;
- peuvent être exécutés à la demande pour régénérer un snapshot, tester un pipeline, rejouer une extraction ou diagnostiquer le VPS.

## Contenu

### `AG0-V1 - extraction universe/`

Workflow d'extraction de l'univers d'investissement depuis des sources externes (tickers, métadonnées, secteurs). Historiquement utilisé pour bootstrapper la Google Sheets d'univers qui alimente AG2/AG3/AG4 ; aujourd'hui désactivé dans n8n — l'univers est maintenu directement via la feuille Google Sheets.

### `scripts/`

Scripts de maintenance et d'exploitation lancés manuellement ou copiés sur le
VPS selon les runbooks :

- diagnostics VPS/n8n ;
- probes Finnhub et maintenance AG4 ;
- seed/classification de l'univers ;
- rapprochement IBKR/ledger ;
- helpers ponctuels Google Drive / SIGA.

## Convention

- Un workflow ajouté à `outils/` implique qu'il n'est plus référencé par `docker-compose.yml`, par un cron n8n actif, ni par un pipeline de production.
- Un script ajouté à `outils/scripts/` doit rester idempotent ou documenter clairement ses effets.
- Si un workflow ou un script sort de `outils/` pour revenir en production, pensez à le re-documenter dans `docs/architecture/etat_des_lieux.md`.
