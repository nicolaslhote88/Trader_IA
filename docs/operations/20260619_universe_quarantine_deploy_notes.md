# Universe Quarantine — audit récurrent AG2/AG1/AG4_Spé

Date : 2026-06-19

## Objectif

Réduire la bande passante perdue sur les symboles de l'univers qui n'ont pas assez de données exploitables, sans les supprimer de la source `universe`.

La quarantaine est une couche réversible :
- AG2 ne les inclut plus dans la rotation technique.
- AG4_Spé ne les inclut plus dans la rotation news Boursorama.
- AG1 ne les propose plus en opportunité d'entrée/renfort.
- Les symboles détenus restent surveillés, même s'ils remplissent un critère de quarantaine.

## Tables DuckDB

Base : `/files/duckdb/ag2_v3.duckdb`

- `universe_quarantine` : état courant par symbole.
- `universe_quarantine_audit_runs` : résumé de chaque run.
- `universe_quarantine_audit_history` : trace par symbole et par run.

Colonnes clés :
- `active = TRUE` : symbole exclu des rotations non détenues.
- `manual_override = TRUE` : la routine n'écrase pas l'état automatiquement.
- `reason` : `TECH_DATA_UNUSABLE_30D`, `QUOTE_UNUSABLE_30D`, `LOW_VOLUME_30D`, `HELD_POSITION`, `RECOVERED_DATA`.
- `metrics_json` : métriques au moment de la décision.

## Règles V1

Fenêtre : 30 jours.

Mise en quarantaine automatique si le symbole n'est pas détenu et vérifie au moins une règle :
- `TECH_DATA_UNUSABLE_30D` : au moins 5 runs AG2, zéro run avec H1 et D1 exploitables.
- `QUOTE_UNUSABLE_30D` : au moins 3 runs YF, zéro quote exploitable.
- `LOW_VOLUME_30D` : au moins 3 runs YF, volume moyen < 5 000 et volume max < 20 000.

Sortie automatique :
- symbole détenu (`HELD_POSITION`) ;
- ou récupération des données techniques : au moins 3 runs AG2 récents, dont au moins 2 exploitables.

## Workflow n8n

Fichier repo : `agents/trading-actions/AG2 - La technique/AG2-V3/AG2-Universe-Health-Quarantine.workflow.json`

Nom : `AG2 — Universe Health Quarantine`

ID : `AG2UHQ20260619`

Planification : jours ouvrés, `18:35 UTC`.

Le workflow contient :
- un trigger manuel ;
- un schedule trigger ;
- un node Code Python `Audit + Quarantine`.

## Déploiement

Importer/publier le workflow dédié :

```bash
n8n import:workflow --input=/tmp/AG2-Universe-Health-Quarantine.workflow.json
n8n publish:workflow --id=AG2UHQ20260619
```

Importer/publier aussi les workflows consommateurs après modification :

- `AG2-V3 - Analyse technique actions ETF crypto.json`
- `AG1_workflow_v4_consensus.json`
- `AG4-SPE-V2-workflow.json`

Puis redémarrer n8n et task runners :

```bash
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

## Vérification

Lecture seule :

```bash
docker exec yf-enrichment python3 - <<'PY'
import duckdb
con = duckdb.connect('/files/duckdb/ag2_v3.duckdb', read_only=True)
print(con.execute("SELECT COUNT(*) FROM universe_quarantine WHERE active").fetchall())
print(con.execute("SELECT reason, COUNT(*) FROM universe_quarantine WHERE active GROUP BY 1 ORDER BY 2 DESC").fetchall())
print(con.execute("SELECT * FROM universe_quarantine_audit_runs ORDER BY started_at DESC LIMIT 3").fetchall())
con.close()
PY
```

## Rollback

Rollback logique immédiat :

```sql
UPDATE universe_quarantine
   SET active = FALSE,
       reason = 'ROLLBACK',
       reason_detail = 'Rollback manuel quarantaine',
       last_released_at = CURRENT_TIMESTAMP,
       updated_at = CURRENT_TIMESTAMP
 WHERE active = TRUE;
```

Rollback code :
- réimporter les versions précédentes des workflows AG2, AG1 et AG4_Spé ;
- republier ;
- redémarrer n8n/task-runners.

## Garde-fous

- Ne jamais supprimer de lignes dans `universe` pour nettoyer la rotation.
- Les positions détenues restent éligibles à la surveillance.
- Ne pas utiliser `spread_pct` comme critère V1 : en pré-marché, il est souvent NULL et trop bruité.
- Une expansion de l'univers US doit être faite dans une PR séparée après stabilisation des règles V1.
