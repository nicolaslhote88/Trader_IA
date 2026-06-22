# AG2 Split Rotation — Held/Core + Watchlist Nightly

Date : 2026-06-19

## Objectif

Remplacer la rotation AG2 généraliste horaire par deux rythmes séparés :

- `AG2-V3 — Technical Held+Core` (`AG2V3HELDCORE20260619`) : surveillance fraîche des positions détenues et du noyau investissable.
- `AG2-V3 — Technical Watchlist Nightly` (`AG2V3WATCHNIGHT20260619`) : exploration lente de la watchlist hors heures critiques.

Le workflow historique `AG2-V3 - Analyse technique Actions` (`lUsgEdJODpYh5vt0dQdb2`) est conservé comme rollback mais désactivé en production.

## Segments

Base : `/files/duckdb/ag2_v3.duckdb`

Table : `universe_segments`

- `HELD` : positions détenues, calculées depuis `ag1_v4_consensus.portfolio_positions_mtm_latest`.
- `CORE_AUTO` : top 50 non quarantainés, score composite liquidité + fondamentaux + risque.
- `CORE_MANUAL` : override manuel, non effacé par l'audit automatique.
- `WATCHLIST` : disponible, non détenu, non CORE, non quarantainé.

La routine `AG2 — Universe Health Quarantine` reconstruit les segments après chaque audit.

## Schedules

- Held/Core : `10 8,12,14 * * 1-5` UTC.
- Watchlist : `20 2 * * 2-6` UTC.
- Quarantaine/segmentation : `35 18 * * 1-5` UTC.

## Batch sizing

- Held/Core : tous les `HELD` à chaque run + `18` CORE en rotation.
- Watchlist : `40` symboles par nuit.

Avec les chiffres initiaux du 2026-06-19 :

- `HELD` : 6
- `CORE_AUTO` : cible 50
- `WATCHLIST` : environ 198
- couverture CORE complète : 1 jour ouvré
- couverture watchlist complète : environ 5 nuits

## Vérification

```bash
docker exec yf-enrichment python3 - <<'PY'
import duckdb
con = duckdb.connect('/files/duckdb/ag2_v3.duckdb', read_only=True)
print(con.execute("SELECT segment, COUNT(*) FROM universe_segments WHERE active GROUP BY 1 ORDER BY 1").fetchall())
print(con.execute("SELECT reason, COUNT(*) FROM universe_quarantine WHERE active GROUP BY 1 ORDER BY 2 DESC").fetchall())
con.close()
PY
```

Vérifier les workflows actifs :

```bash
docker exec root-n8n-1 python3 - <<'PY'
import sqlite3
ids = ('AG2V3HELDCORE20260619','AG2V3WATCHNIGHT20260619','lUsgEdJODpYh5vt0dQdb2')
con = sqlite3.connect('/home/node/.n8n/database.sqlite')
print(con.execute("SELECT id,name,active FROM workflow_entity WHERE id IN (?,?,?) ORDER BY name", ids).fetchall())
con.close()
PY
```

Attendu :

- `AG2V3HELDCORE20260619 active=1`
- `AG2V3WATCHNIGHT20260619 active=1`
- `lUsgEdJODpYh5vt0dQdb2 active=0`

## Rollback

```bash
n8n update:workflow --id=AG2V3HELDCORE20260619 --active=false
n8n update:workflow --id=AG2V3WATCHNIGHT20260619 --active=false
n8n update:workflow --id=lUsgEdJODpYh5vt0dQdb2 --active=true
n8n publish:workflow --id=lUsgEdJODpYh5vt0dQdb2
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

## Garde-fous

- Les positions détenues sont incluses à chaque run Held/Core.
- La watchlist ne tourne pas en journée.
- Les symboles quarantainés restent exclus, sauf positions détenues.
- `CORE_MANUAL` permet de forcer un symbole dans le noyau sans modifier `universe`.
