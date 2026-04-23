# Fix PK perdues — 2026-04-23

## Contexte
Le script de compaction DuckDB du 2026-04-22 (task #28) n'a pas préservé les contraintes
PRIMARY KEY lors de la recréation des tables. Conséquence : tous les nœuds `duckdbinit`
des workflows n8n qui utilisent `INSERT ... ON CONFLICT (...)` échouent avec :

```
Binder Error: There are no UNIQUE/PRIMARY KEY constraints that refer to this table,
specify ON CONFLICT columns manually
```

## Diagnostic
Scan comparatif des snapshots originaux (`duckdb_20260422/`) vs compactés (`duckdb_20260422_compacted/`)
: **74 contraintes PRIMARY KEY perdues** sur 8 DB. Détail :

- ag1_v3_chatgpt52.duckdb       — 17 PK (core.* + cfg.* + main.*)
- ag1_v3_gemini30_pro.duckdb    — 17 PK
- ag1_v3_grok41_reasoning.duckdb — 17 PK
- ag2_v3.duckdb                 — 5 PK
- ag3_v2.duckdb                 — 6 PK
- ag4_spe_v2.duckdb             — 5 PK
- ag4_v3.duckdb                 — 5 PK
- yf_enrichment_v1.duckdb       — 2 PK

**Faisabilité validée** : zéro doublon, zéro null sur les colonnes clés → les `ALTER TABLE
ADD PRIMARY KEY` passeront sans perte de données.

## Test réalisé
Appliqué sur une copie de `yf_enrichment_v1.duckdb` : PK restaurées, `INSERT ON CONFLICT`
fonctionne à nouveau. Taille +75% (1.0 → 1.8 MB) — overhead normal de l'index PK.

## Exécution
Pour chaque DB sur le VPS (remplacer le chemin si besoin) :

```bash
cd /path/to/db/dir
duckdb ag1_v3_chatgpt52.duckdb        < fix_pk_ag1_v3_chatgpt52.sql
duckdb ag1_v3_gemini30_pro.duckdb     < fix_pk_ag1_v3_gemini30_pro.sql
duckdb ag1_v3_grok41_reasoning.duckdb < fix_pk_ag1_v3_grok41_reasoning.sql
duckdb ag2_v3.duckdb                  < fix_pk_ag2_v3.sql
duckdb ag3_v2.duckdb                  < fix_pk_ag3_v2.sql
duckdb ag4_spe_v2.duckdb              < fix_pk_ag4_spe_v2.sql
duckdb ag4_v3.duckdb                  < fix_pk_ag4_v3.sql
duckdb yf_enrichment_v1.duckdb        < fix_pk_yf_enrichment_v1.sql
```

**Pré-requis** : arrêter n8n (`docker-compose stop n8n`) avant exécution pour éviter les
locks concurrents, puis relancer.

## Après le fix
- Relancer manuellement un workflow qui échouait sur `duckdbinit` pour valider.
- Vérifier que les nouveaux runs s'écrivent sans conflit.

## À faire en parallèle (task #31)
Patcher le script de compaction (`infra/maintenance/compact_duckdbs.py` ou équivalent)
pour préserver les PRIMARY KEY lors du rebuild des tab