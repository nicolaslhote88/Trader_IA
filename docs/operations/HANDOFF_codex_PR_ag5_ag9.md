# Handoff draft PR — AG5 à AG9

> Mise à jour 2026-08-05 : AG5–AG8, synthèse, dashboard et AG1 consultatif sont
> déployés live ; AG9 est dormant. Les anciennes « actions restantes avant
> publication » ci-dessous décrivent le garde-fou shadow initial. État courant :
> [`20260805_ag5_ag8_global_context_live_deploy.md`](20260805_ag5_ag8_global_context_live_deploy.md).

## Branche et portée

Branche : `codex/ag5-ag9-global-context-20260805`.

Inclure uniquement :

- `agents/common/global-context/**` ;
- `agents/trading-forex/AG5-FX-Macro/**`, `AG6-FX-Valuation/**`,
  `AG7-FX-Positioning/**`, `AG8-FX-Rates/**`,
  `agents/trading-forex/build_three_pillars_workflows.py` ;
- fichiers AG1 V4 explicitement modifiés par le contexte global et son dossier
  `workflow/tests/test_global_context_contract.py` ;
- `services/macro-data-api/**` hors `.venv`/`__pycache__` ;
- `services/worldmonitor-adapter/**` ;
- `services/global-context-synthesizer/**` ;
- `services/dashboard/app.py`, `global_context_tab.py`, `tests/**` ;
- `outils/scripts/replay_ag1_global_context.py`, `tests/test_global_context_*` ;
- `infra/vps_hostinger_config/docker-compose.yml` et
  `global-context.env.example` ;
- documentation AG5–AG9 et hunks ciblés README/AGENTS/état/issues/env/scheduling.

Exclure les changements préexistants AG2/AG4 et les notes du 2026-07-30, sauf
s'ils sont livrés par une PR distincte. Ne jamais inclure `.codex-tmp`, `.venv`,
`__pycache__`, `.duckdb`, `.env` réel, archive ou payload World Monitor brut.

## Validation

Exécuter les quatre commandes pytest du runbook, `compileall`, parsing JSON,
`git diff --check` et régénérer les workflows avec :

```powershell
python agents/trading-forex/build_three_pillars_workflows.py
python 'agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/build_v4_workflow.py'
```

Puis vérifier que `git diff` ne change plus. Hashes consensus/safety/broker
attendus dans `test_global_context_contract.py`.

## Shadow déployé

- sources/images : `/opt/trader-ia-shadow/ag5-ag9-20260805` ;
- sauvegardes n8n : sous `backups/n8n-before-import/` ;
- quatre containers `ag5ag9-*-shadow` ;
- bases shadow dédiées sous `/local-files/duckdb/` ;
- sept workflows importés, tous `active=false`, `activeVersionId=null` ;
- AG1 live toujours version `e5b3f226-...` active ;
- aucun workflow producteur publié, aucun ordre, Forex désactivé.

## Actions restantes avant publication

1. Configurer un credential World Monitor hors repo et confirmer licence/tier.
2. Tester `describe_tool` puis appels réels, mappings, quota/coût.
3. Alimenter AG5 avec sources fraîches et observer plusieurs cycles producteurs.
4. Exécuter le workflow AG1 shadow manual-only, capturer trois propositions et
   consensus, puis fournir l'export au replay.
5. Mesurer tokens/latence, pertinence/faux positifs et comportement fallback.
6. Refaire backup live et rollback drill.
7. Seulement ensuite décider d'activer les producteurs, puis le candidat AG1.

Ne pas publier `AG1V4CONSENSUS` tant qu'un de ces contrôles reste absent.
