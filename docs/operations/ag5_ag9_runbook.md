# Runbook AG5–AG9 et contexte global

État live depuis le 2026-08-05 : AG5–AG8, synthèse et intégration AG1 sont
actifs avec `GLOBAL_CONTEXT_ENABLED_COMPONENTS=AG5,AG6,AG7,AG8`. AG9 reste
dormant ; l'absence de service World Monitor est donc attendue.

## Santé rapide (lecture seule)

```bash
curl -fsS http://macro-data-api:8081/health
curl -fsS http://global-context-synthesizer:8083/health
```

Ne pas attendre de réponse de `worldmonitor-adapter` en production actuelle :
son conteneur est volontairement arrêté. Le sonder uniquement pendant une
qualification AG9 explicitement autorisée.

Les états `DISABLED`, `DEGRADED`, `STALE`, `MISSING` sont des diagnostics, pas
des données neutres. AG1 doit continuer en fail-open avec un pack warning.

## Vérifier les bases

Utiliser une image DuckDB compatible et toujours `read_only=True` :

```bash
docker exec root-n8n-1 python - <<'PY'
import duckdb
for path, view in [
    ('/files/duckdb/macro_data.duckdb','main.v_latest_ag5_macro'),
    ('/files/duckdb/worldmonitor_v1.duckdb','main.v_latest_ag9_global_risk'),
    ('/files/duckdb/global_context_v1.duckdb','main.v_latest_global_context'),
]:
    try:
        con=duckdb.connect(path, read_only=True)
        print(path, con.execute(f'SELECT count(*) FROM {view}').fetchone()[0])
        con.close()
    except Exception as exc:
        print(path, type(exc).__name__, str(exc)[:300])
PY
```

Ne jamais lancer `CHECKPOINT` dans n8n. La compaction reste l'opération offline
existante.

## Exécution manuelle sûre

Avec des chemins shadow dédiés et sans broker :

```bash
curl -fsS -X POST http://macro-data-api-shadow:8081/components/ag5/refresh
curl -fsS -X POST http://macro-data-api-shadow:8081/components/ag6/compute
curl -fsS -X POST http://macro-data-api-shadow:8081/components/ag7/refresh
curl -fsS -X POST http://macro-data-api-shadow:8081/components/ag8/compute
curl -fsS -X POST http://worldmonitor-adapter-shadow:8082/admin/discover
curl -fsS -X POST http://worldmonitor-adapter-shadow:8082/ag9/refresh
curl -fsS -X POST http://global-context-synthesizer-shadow:8083/synthesize
```

La collecte AG9 nécessite un credential externe hors repo. Ne jamais le mettre
en argument de commande, JSON n8n ou log. Utiliser un `.env` protégé (`chmod
600`) et vérifier seulement `credential_configured=true`.
Sans credential, seule la découverte sûre est autorisée :
`POST /admin/discover?catalog_only=true`.

## Diagnostics

| Symptôme | Vérification | Action sûre |
|---|---|---|
| `WORLD_MONITOR_DISABLED` | variable + `/health` | conserver disabled ou configurer hors repo |
| `AUTH_OR_TIER` | abonnement/clé côté fournisseur | ne pas réessayer en boucle; corriger hors repo |
| `QUOTA_OR_RATE_LIMIT` | `core.source_health`, limites fournisseur | réduire fréquence/capacités |
| `AG9_ZERO_VALID_EVENTS` | raw statuses, dates, schémas | corriger adaptateur; ancien snapshot conservé |
| `GLOBAL_CONTEXT_ZERO_AVAILABLE_COMPONENTS` | vues AG5–AG9 | restaurer producteurs, ne pas publier de vide |
| `GLOBAL_CONTEXT_STALE` | `component_ages_json` | relancer composants; AG1 continue avec warning |
| lock DuckDB | PID/containers écrivains | attendre/retry; jamais tuer AG1 arbitrairement |
| pack V1 > budget | `AG1_GLOBAL_CONTEXT_MAX_CHARS` | corriger mapping/limites; ne pas augmenter sans replay |
| payload LLM V2 > budget | `AG1_GLOBAL_CONTEXT_LLM_MAX_CHARS` | filtrer davantage; ne pas augmenter sans replay |

## Tests

Sous Windows avec le venv existant :

```powershell
& 'services/macro-data-api/.venv/Scripts/python.exe' -m pytest -q services/macro-data-api/tests
& 'services/macro-data-api/.venv/Scripts/python.exe' -m pytest -q services/worldmonitor-adapter/tests
& 'services/macro-data-api/.venv/Scripts/python.exe' -m pytest -q services/global-context-synthesizer/tests
& 'services/macro-data-api/.venv/Scripts/python.exe' -m pytest -q 'agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/tests/test_global_context_contract.py' services/dashboard/tests tests
```

Le replay est read-only, sans réseau, LLM ni ordre :

```bash
python outils/scripts/replay_ag1_global_context.py \
  --ag1-db /files/duckdb/ag1_v4_consensus.duckdb \
  --global-db /files/duckdb/global_context_v1.duckdb --runs 20
```

Les variations LLM/consensus/latence ne sont mesurables qu'avec un export de
runs shadow fourni à `--shadow-results`; le script n'invente pas ces résultats.

## Alertes recommandées avant activation

- aucune collecte à zéro ligne déclarée `OK` ;
- couverture AG9 et source health par capacité ;
- âge de chaque composant, pas seulement âge de synthèse ;
- dérive du hash de contrat World Monitor ;
- taille/tokens du pack et latence HTTP AG1 ;
- comparaison baseline/shadow et absence totale de nœud broker dans shadow.
