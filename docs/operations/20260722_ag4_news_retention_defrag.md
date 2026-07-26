# Maintenance AG4 news — rétention, dates et défragmentation (2026-07-22)

## Périmètre

- Base live : `/local-files/duckdb/ag4_spe_v2.duckdb`.
- Consommateur vérifié : AG1 V4, node R8.
- Rétention appliquée : **30 jours sur la date effective AG1** :
  `published_at` seulement si plausible (`now-730j` à `now+2j`), sinon repli sur
  `first_seen_at`, `analyzed_at`, `fetched_at`, `updated_at`, `created_at`.
- DuckDB utilisé pour toutes les écritures/reconstructions : **1.4.3**.

## Audit avant maintenance

- Taille fichier : **633 090 048 octets / 603,7 MiB**.
- Blocs : 1 103 utilisés, 1 312 libres (fragmentation importante).
- `news_history` : **16 275** lignes.
- Lignes hors fenêtre AG1 de 30 jours : **5 317** au moment du nettoyage :
  Boursorama 5 184, IBKR 130, Finnhub 3.
- Dates : aucune date future `> now+2j`, aucune date effective NULL, aucune date
  effective future. Les écritures depuis le correctif du 13/07 sont plausibles
  sur Boursorama, Finnhub et IBKR.
- Un run zombie subsistait : `AG4SPEV2_20260626100508`, `RUNNING`, zéro symbole
  traité.

## Validation shadow

Deux reconstructions ont été testées sur copie avant le live. La première a
révélé que le défragmenteur restaurait PK/UNIQUE et vues, mais perdait les index
secondaires. `infra/maintenance/defrag_duckdb.py` a donc été durci pour :

1. recopier les tables sans `LIMIT/OFFSET` ;
2. restaurer PK/UNIQUE ;
3. restaurer les index explicites via `duckdb_indexes()` ;
4. recréer les vues ;
5. limiter le contrôle WAL aux DB ciblées par `--only` ;
6. préserver propriétaire, groupe et permissions du fichier source.

La seconde copie shadow a conservé les 4 index explicites, toutes les clés et la
vue `news_analyzed`. Taille projetée : **27,8 MiB**.

## Opération live

Les conteneurs `root-n8n-1` et `root-task-runners-3/4/5` ont été arrêtés environ
11 secondes, puis redémarrés automatiquement.

- Backup vérifié par SHA-256 :
  `/local-files/duckdb/backups/ag4_spe_v2.duckdb.bak_20260722_pre_retention_defrag`
- Rollback immédiat conservé par le swap :
  `/local-files/duckdb/ag4_spe_v2.duckdb.old`
- Backup du défragmenteur précédent :
  `/local-files/maintenance/defrag_duckdb.py.bak_20260722_pre_index_fix`
- Défragmenteur corrigé déployé :
  `/local-files/maintenance/defrag_duckdb.py`

Résultat :

- `news_history` : **16 275 → 10 958** lignes ;
- `news_analyzed` : **4 201** lignes ;
- taille : **603,8 → 27,5 MiB** lors du swap (puis ~27,8 MiB après la
  réconciliation du run zombie) ;
- blocs libres : **1** ;
- 4 index explicites, toutes les PK et la vue `news_analyzed` présents ;
- dates futures : **0** ; dates effectives NULL/futures : **0** ;
- run zombie reclassé `STALE`, `RUNNING` restant : **0** ;
- transaction RW depuis `root-n8n-1` : OK ; propriétaire/mode restaurés à
  `ubuntu:ubuntu 0777`, identiques à l'ancien fichier ;
- workflows AG1 V4 et AG4_Spé (Boursorama, IBKR, Finnhub, Health Alert) actifs
  avec une version publiée ; broker sain et aucune approbation en attente.

## Point d'attention découvert (non modifié)

AG1 utilise les valeurs par défaut `AG1_R8_NEWS_LOOKBACK_DAYS=30` et
`AG1_R8_MAX_SYMBOL_NEWS_ROWS=6000`. La base contient encore 10 958 lignes dans
la fenêtre de 30 jours : le `LIMIT 6000` global de R8 peut donc tronquer les
agrégats 30 jours des symboles les moins récents. Modifier ce plafond toucherait
le scoring live et doit faire l'objet d'un shadow/replay séparé.

## Rollback

En cas de régression dans les 48 h : arrêter n8n et les trois task-runners,
mettre le fichier courant de côté, recopier `ag4_spe_v2.duckdb.old` vers
`ag4_spe_v2.duckdb`, vérifier owner/mode et redémarrer les quatre conteneurs.
Ne pas supprimer `.old` ni le backup avant validation de plusieurs runs AG4.
