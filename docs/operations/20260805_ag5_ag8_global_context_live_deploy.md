# Déploiement live AG5–AG8 et contexte global consultatif

Date : 2026-08-05
Décision : déployer toutes les modifications du chantier contexte global sauf AG9.

> Suivi 2026-08-06 : les causes de la qualité initialement dégradée ont été
> corrigées et déployées. Voir
> `docs/operations/20260806_ag5_ag8_data_quality_remediation.md`.

## Portée effectivement activée

- `AG5 — Macro & Flows V2` ;
- `AG6 — FX Relative Valuation V2` (analyse relative uniquement, aucun ordre Forex) ;
- `AG7 — Positioning V2` ;
- `AG8 — Rates & Liquidity V2` ;
- `Global Context — AG5-AG9 Synthesizer`, configuré en production avec
  `GLOBAL_CONTEXT_ENABLED_COMPONENTS=AG5,AG6,AG7,AG8` ;
- pack consultatif commun injecté dans les trois branches AG1 V4 ;
- audit ledger additif et page dashboard `Commun → Contexte global`.

AG9 reste hors périmètre : workflow inactif/non publié, conteneur shadow arrêté
avec `restart=no`, aucune instance World Monitor de production.

## Sauvegardes et release

- release immuable :
  `/opt/trader-ia/releases/ag5-ag8-global-context-20260805` ;
- sauvegardes :
  `/opt/trader-ia/backups/ag5-ag8-global-context-20260805T1741Z` ;
- inclus : compose précédent, `.env` protégé, backup SQLite n8n cohérent,
  exports avant/après des workflows, scripts AG1 externes et copies DuckDB
  `macro_data`/`ag1_v4_consensus` ;
- archive finale dans le répertoire de sauvegarde :
  `ag5-ag8-live-release-final.tar.gz`, SHA-256
  `2f337792538bc3213a50975639e34f0037416e5bc4655a8a5da87472ce52adca`.

## Premier cycle production

Exécuté directement sur les API internes, sans LLM, broker ou ordre :

| Composant | Snapshot | Lignes | Couverture | Statut |
|---|---|---:|---:|---|
| AG5 | `AG5_20260805T174622Z_fd3b9ad9` | 12 | 0,213333 | `DEGRADED` |
| AG6 | `AG6_20260805T174632Z_4e807b0a` | 12 | 0,175000 | `DEGRADED` |
| AG7 | `AG7_20260805T174637Z_e4aedfdf` | 9 | 1,000000 | `OK` |
| AG8 | `AG8_20260805T174637Z_938aa3a8` | 12 | 0,837500 | `DEGRADED` |

Snapshot atomique : `GC_20260805T174637Z_4bca39b9`, couverture `0,584444`,
confiance `0,400186`, taille du pack 8 440 caractères. AG9 apparaît
`DISABLED` avec warning `AG9_DORMANT` et ne participe ni aux poids, ni à la
fraîcheur, ni à la couverture. La fraîcheur globale reste `missing` à cause des
entrées AG5–AG8 réellement manquantes/périmées ; aucune neutralisation
artificielle n'est appliquée.

## Versions n8n publiées

| Workflow | `activeVersionId` | État |
|---|---|---|
| AG5 | `15cada4a-2197-4c11-b839-d189878621f3` | actif |
| AG6 | `495a7f3a-11e8-4111-afc4-a90d2fdca86a` | actif |
| AG7 | `042469b9-91ed-468a-9cfc-657918ab19b2` | actif |
| AG8 | `ed283643-cf7f-4d3e-a696-18422836782b` | actif |
| Synthèse | `95d4b6ff-9e9a-4c3a-a8d2-a55da829f20b` | actif |
| AG1 V4 | `0129c485-c6c6-436f-80a5-745fc29e199b` | actif, 43 nœuds |
| AG9 | — | inactif, `activeVersionId=null` |

Les hashes déployés des nœuds critiques restent :

```text
consensus  c39434c3ff5b484ba2615fa6a0ec7c722387b790c3f83c630070645d611d1316
safety     d658f005a41131e175792f5b5dea63e3445fb744f8979f347916dacc9722883d
broker     060d649426d7ad015e68734fe1cda4909ecdf89503d1158e26d77f3a7e8b5e41
```

## Contrôles post-déploiement

- 46 tests locaux passants, compilation Python et JSON valides ;
- `macro-data-api` et `global-context-synthesizer` healthy ;
- trois task-runners reconnectés au broker n8n ;
- dashboard HTTP 200 et `DASHBOARD_GLOBAL_CONTEXT_SMOKE_OK` ;
- pack `advisory_only=true`, même prompt pour les trois modèles, attach avant
  le preflight liquidité ;
- aucun workflow de trading Forex actif ; `fx_orders_enabled=false` ;
- broker authentifié, `dry_run=false`, zéro approbation pending ;
- aucun run AG1 et aucun ordre déclenché par le déploiement.

## Observation restante

Le premier run naturel AG1 enrichi est prévu le 2026-08-06 à 14:00 Paris. Après
ce run, contrôler les champs `global_context_*` dans `core.runs`, le statut des
trois propositions, la durée, le coût et l'absence de régression d'exécution.

## Hotfix éditeur n8n

Après un test manuel du shadow, les expressions d'URL basées sur `$env` ont été
remplacées par les URLs Docker internes explicites dans AG1, AG5–AG8 et la
synthèse. Détail, versions et rollback :
`docs/operations/20260805_ag1_shadow_env_access_hotfix.md`.

## Rollback

1. restaurer `/docker/root/docker-compose.yml` depuis
   `docker-compose.root.before.yml` ;
2. restaurer les exports `*.before.json` avec `n8n import:workflow` ;
3. republier `AG1V4CONSENSUS`, dépublier AG5–AG8 et la synthèse ;
4. restaurer `duckdb_writer.py` et `portfolio_ledger_schema_v4.sql` depuis
   `ag1-external/` ;
5. recréer n8n, les runners, macro-data-api et le dashboard puis arrêter
   `global-context-synthesizer` ;
6. vérifier broker, pending approvals, Forex et hashes critiques.

Les bases sauvegardées ne doivent être restaurées que si une écriture corrompue
est démontrée ; les colonnes ledger additives peuvent sinon rester en place.
