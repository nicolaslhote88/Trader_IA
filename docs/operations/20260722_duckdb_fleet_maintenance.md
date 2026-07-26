# Maintenance du parc DuckDB Trader_IA — 2026-07-22

## Périmètre et méthode

Audit des bases présentes dans `/local-files/duckdb/`, à l'exception explicite
de `siga_v1.duckdb` (application SIGA, hors périmètre). Toutes les inspections
ont été faites en lecture seule. Les corrections live ont utilisé DuckDB 1.4.3,
des sauvegardes vérifiées par SHA-256 et une reconstruction validée au préalable
sur copie shadow.

`broker_costs.duckdb` a été contrôlée mais non reconstruite : 21 lignes et
environ 2 MiB ne présentent aucun gain matériel. `ag4_spe_v2.duckdb` avait déjà
été traitée dans la même session ; voir
`docs/operations/20260722_ag4_news_retention_defrag.md`.

## Faits validés par l'audit

- Les données actives AG1 V4, AG2 V3, AG3 V2 et YF sont fraîches au 22/07/2026.
- Aucun timestamp système futur invalide n'a été trouvé dans les bases auditées.
- Les dates futures de `next_earnings_date` dans YF sont des dates prévisionnelles
  métier valides, pas des anomalies.
- Les bases Forex ne reçoivent plus de données depuis juin 2026 ou avant, ce qui
  est cohérent avec la désactivation volontaire de tous les workflows Forex.
- Douze anciens runs AG3 étaient restés en statut `RUNNING` après des crashes de
  février à mai 2026. Ils ont été reclassés `STALE`; aucun run AG3 zombie ne reste.
- `ag4_v3.duckdb` grossissait malgré la maintenance hebdomadaire : le cron ne
  faisait qu'une rétention suivie d'un `CHECKPOINT`, sans reconstruction physique.

## Durcissement du défragmenteur

`infra/maintenance/defrag_duckdb.py` reconstruit désormais chaque table avec son
DDL exact issu de `duckdb_tables().sql`, dans l'ordre des dépendances de clés
étrangères. Il préserve et vérifie avant swap :

- colonnes, types, valeurs par défaut et `NOT NULL`;
- clés primaires, contraintes `UNIQUE` et clés étrangères;
- index secondaires explicites;
- vues;
- propriétaire, groupe et mode du fichier.

La copie des lignes se fait en un seul `INSERT ... SELECT`, sans le dangereux
`LIMIT/OFFSET` non déterministe. L'option `--only` limite aussi le contrôle des
WAL aux seules bases demandées.

Tests shadow représentatifs : AG1 V4 a conservé ses 23 clés étrangères et est
passée de 440,3 à 31,3 MiB; AG3 V2 de 533,3 à environ 65 MiB; AG4 Forex de
1 423,3 à 13,3 MiB; Macro de 39,3 à 3,5 MiB. Un dry-run complet des 13 bases
libres a validé l'équivalence des catalogues et des nombres de lignes.

## Résultats live — phase 1

| Base | Avant (MiB) | Après (MiB) |
|---|---:|---:|
| `ag1_fx_v1_chatgpt52.duckdb` | 595,3 | 9,3 |
| `ag1_fx_v1_gemini30_pro.duckdb` | 83,3 | 3,3 |
| `ag1_fx_v1_grok41_reasoning.duckdb` | 83,0 | 3,3 |
| `ag1_v4_consensus.duckdb` | 440,3 | 30,8 |
| `ag2_fx_v1.duckdb` | 157,0 | 3,5 |
| `ag2_v3.duckdb` | 343,0 | 16,8 |
| `ag3_fx_v1.duckdb` | 197,3 | 10,0 |
| `ag3_v2.duckdb` | 533,3 | 65,5 |
| `ag4_forex_v1.duckdb` | 1 423,3 | 13,3 |
| `ag4_fx_v1.duckdb` | 132,5 | 11,0 |
| `macro_data.duckdb` | 39,3 | 3,5 |
| `yf_enrichment_v1.duckdb` | 48,8 | 8,8 |
| **Total** | **4 076,1** | **178,9** |

Gain disque de la phase 1 : environ **3,81 Gio**. Le dashboard n'a pas été
interrompu pour cette phase; seul `macro-data-api` a été arrêté brièvement pour
libérer sa base, puis redémarré sainement.

Avec AG4 V3, l'empreinte des fichiers actifs traités est passée d'environ
**6 877,6 à 195,7 MiB**, soit **~6,53 Gio** de fragmentation retirée. Les
sauvegardes et `.old` sont volontairement conservés pendant la validation : le
gain physique complet sur le filesystem ne sera réalisé qu'après leur rotation.

Chaque base possède une sauvegarde pré-opération dans
`/local-files/duckdb/backups/<base>.bak_20260722_pre_fleet_defrag`, vérifiée par
SHA-256, ainsi qu'un fichier `<base>.old` créé par le swap. Les anciens `.old`
AG2/AG3 ont été copiés dans `backups/` avant rotation.

## Vérifications après phase 1

- transaction temporaire écriture/rollback réussie depuis `root-n8n-1` sur les
  12 bases reconstruites;
- permissions et propriétaires identiques aux fichiers sources;
- nouveau run AG1-PF MTM écrit avec succès dans `ag1_v4_consensus.duckdb` après
  le swap (`RUN_RECON_IBKR_PF_PFMTM_20260722111500`);
- un run AG4_Spé a démarré après le swap AG2 sans erreur de lecture de l'univers.

## AG4 V3 et prévention de la récidive

La phase AG4 V3 est exécutée séparément, uniquement après la fin naturelle du
run n8n qui détenait son WAL. La maintenance conserve 60 jours de news, 30 jours
d'erreurs et clôt les runs `RUNNING` âgés de plus de 6 h, puis appelle le
défragmenteur commun. Le dashboard est le seul service arrêté brièvement pendant
le swap; le workflow n8n n'est ni interrompu ni désactivé.

Le script `outils/scripts/ag4_duckdb_maintenance.py` ne lance plus de
`CHECKPOINT` explicite et délègue `--rebuild` au défragmenteur sûr. Le cron AG4
hebdomadaire utilise désormais `--rebuild`. La défragmentation préventive du
dimanche à 07:30 UTC couvre les bases actives à croissance régulière : AG1 V4,
AG2 V3, AG3 V2, AG4_Spé V2 et YF enrichment.

Résultat AG4 V3 :

- rétention `news_history` : **13 959 → 13 057** lignes (902 supprimées);
- `news_errors` : 2 lignes, toutes dans la fenêtre de 30 jours;
- taille : **2 801,5 → 16,8 MiB** (gain ~2,72 Gio);
- 1 bloc libre, WAL nul, 8 index et 6 clés primaires préservés;
- aucun timestamp futur `> now+2j`;
- transaction écriture/rollback réussie depuis n8n;
- backup SHA-256 :
  `/local-files/duckdb/backups/ag4_v3.duckdb.bak_20260722_pre_fleet_defrag`;
- rollback immédiat : `/local-files/duckdb/ag4_v3.duckdb.old`.

Les exécutions n8n AG4 V3 `20343` et `20350` avaient toutes deux échoué au nœud
`20DBW - Upsert News DuckDB` sur un verrou concurrent, après le budget de retry
de 8 secondes. Leurs runs `AG4V2_20260722044509` et
`AG4V2_20260722084508` ont été réconciliés en `CRASHED`, avec l'heure d'arrêt n8n
et le motif; `RUNNING` restant = 0. Un backup additionnel de la base compacte a
été créé avant cette correction :
`backups/ag4_v3.duckdb.bak_20260722_pre_run_reconcile`.

La compaction ramène les lectures dashboard d'une base de 2,8 Gio à une base de
17 MiB et réduit donc fortement leur fenêtre de verrou. Le prochain run AG4 V3
reste à surveiller. Si un conflit réapparaît, appliquer le retry-hardening déjà
documenté dans `SCHEDULING_AND_LOAD.md` via shadow/replay; il n'a pas été déployé
implicitement pendant cette maintenance de données.

## Rollback

Pour une base donnée : arrêter uniquement ses writers/lecteurs persistants,
mettre le fichier courant de côté, recopier soit `<base>.old`, soit la sauvegarde
SHA-256 depuis `backups/`, restaurer propriétaire/mode, puis redémarrer les
services concernés. Ne pas supprimer les `.old` ni les sauvegardes avant
validation de plusieurs cycles de production.

## État de sortie

- 15 workflows actifs Trader_IA vérifiés avec une version publiée;
- n8n, les trois task-runners, dashboard, broker, yfinance, yf-enrichment et
  macro-data actifs; dashboard `/_stcore/health` = `ok`;
- broker IBKR authentifié, compte live aligné, Forex désactivé, aucune
  approbation en attente;
- AG1-PF post-swap en succès; AG4_Spé lancé avant la phase finale encore actif
  au moment du contrôle et écrivant dans une autre base;
- prochain run AG4 V3 à surveiller pour confirmer la disparition du conflit de
  verrou après compaction.
