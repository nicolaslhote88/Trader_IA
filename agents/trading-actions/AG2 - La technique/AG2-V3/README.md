# AG2-V3 - Analyse technique actions

## Workflows actifs en n8n

Les trois artefacts publiables sont générés depuis `nodes/` par les builders :

- `AG2-V3-Technical-Held-Core.workflow.json` — segments HELD + CORE, 09/13/15 h Paris ;
- `AG2-V3-Technical-Watchlist-Nightly.workflow.json` — WATCHLIST, 22/02 h Paris ;
- `AG2-Universe-Health-Quarantine.workflow.json` — audit de l'univers, 20 h Paris.

Le code des nœuds est la source canonique. Les builders utilisent des UUID v5 et doivent
reproduire des JSON déterministes avant tout import n8n.

## Agent technique

Le pipeline charge l'univers depuis DuckDB (`/files/duckdb/ag2_v3.duckdb`,
table `universe`), recupere les donnees Yahoo Finance H1/D1, calcule les
indicateurs, applique le prefiltre technique puis valide les candidats via le
prompt ACTIONS/ETF. Il ne doit plus dependre de l'onglet Google Sheets
`Universe`.

Contrat temporel v3.1 : les requêtes `/history` imposent `closed_only=true` et
`validated_only=true`. Une D1 n'est admissible qu'après la clôture régulière de
sa place + 10 minutes ; une H1 après la fin de sa fenêtre + 10 minutes. Les
demi-séances sont volontairement traitées au close régulier (retard conservateur,
jamais d'admission anticipée). Les OHLC non positifs/incohérents et volumes
négatifs sont rejetés avant calcul. La volatilité annualisée utilise la durée de
session de la place ; crypto utilise 365 jours et 24 h/jour.

Le schema DuckDB conserve les sorties utiles a AG1 :

- `universe`
- `technical_signals`
- `v_latest_signals`
- `v_ag1_summary`
- `ai_dedup_cache`
- `run_log`
- `batch_state`
- `schema_migrations`

## Scripts noeuds

Ne pas éditer un JSON workflow à la main. En cas de divergence, exporter d'abord
la version **publiée** live, la sauvegarder, réconcilier le nœud canonique, puis
reconstruire les trois artefacts.
