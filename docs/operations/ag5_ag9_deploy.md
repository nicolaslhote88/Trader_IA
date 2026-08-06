# Déploiement progressif AG5–AG9

> Statut au 2026-08-05 : cette procédure décrit la qualification initiale.
> Nicolas a ensuite autorisé la promotion hors AG9. L'état live et le rollback
> exact sont consignés dans
> [`20260805_ag5_ag8_global_context_live_deploy.md`](20260805_ag5_ag8_global_context_live_deploy.md).

## Préconditions

1. Branche dédiée propre pour les fichiers du chantier; ne pas inclure les
   changements AG2/AG4 préexistants.
2. Tests du runbook verts, JSON parseables, images Docker construites.
3. Sauvegarde des exports n8n publiés et fichiers compose dans un répertoire
   daté `.codex-tmp`/hors Git.
4. Vérifier broker `/health`, pending approvals, `fx_orders_enabled=false` et
   workflow AG1 actif avant/après chaque étape.

## Étape 1 — shadow isolé

Créer trois bases dédiées, jamais les bases de production :

```text
/local-files/duckdb/macro_data_ag5ag9_shadow.duckdb
/local-files/duckdb/worldmonitor_v1_shadow.duckdb
/local-files/duckdb/global_context_v1_shadow.duckdb
```

Construire les trois images depuis les dossiers `services/`, monter
`/local-files/duckdb:/files/duckdb`, et démarrer avec :

```text
WORLD_MONITOR_ENABLED=false
GLOBAL_CONTEXT_ENABLED=false
MACRO_DUCKDB_PATH=/files/duckdb/macro_data_ag5ag9_shadow.duckdb
WORLD_MONITOR_DUCKDB_PATH=/files/duckdb/worldmonitor_v1_shadow.duckdb
GLOBAL_CONTEXT_DUCKDB_PATH=/files/duckdb/global_context_v1_shadow.duckdb
```

Importer les sept workflows uniquement, sans publication : AG5, AG6, AG7,
AG8, AG9, synthétiseur et `AG1_workflow_v4_global_context_shadow.json`. Vérifier
dans SQLite `active=0` et `activeVersionId IS NULL`. Ne jamais importer à cette
étape le candidat `AG1V4CONSENSUS`, car `import:workflow` désactive le live.

## Étape 2 — qualification

Exécuter manuellement les collecteurs via HTTP, inspecter volumes, millésimes,
locks, erreurs, mapping et durée. Sans credential World Monitor, arrêter la
qualification au catalogue public et aux fixtures : l'acceptation AG9 réelle
reste bloquée.

Avec credential configuré hors repo, lancer découverte puis collecte. Observer
quota/coût et plusieurs cycles. Publier les workflows producteurs seulement
après cette observation. Ils n'ont aucune connexion AG1/broker.

## Étape 3 — AG1 shadow

Activer/exécuter uniquement le workflow manual-only shadow. Il ne contient ni
Schedule Trigger, ni preflight, ni Risk Manager, ni writer AG1, ni broker. Capturer
les trois propositions et le consensus terminal. Comparer :

- baseline sans contexte ;
- AG5–AG8 ;
- AG5–AG9 ;
- hash reçu par les trois branches ;
- tokens/latence ;
- événements pertinents/faux positifs.

## Étape 4 — publication AG1 candidate

Interdite tant que les cycles réels et shadow ne satisfont pas tous les critères
de la mission. Avant publication : exporter le live publié, vérifier les hashes
consensus/safety/broker, importer le candidat, republier immédiatement, restart
n8n+runners, puis vérifier `active=1` et `versionId=activeVersionId`.

Le défaut de production reste `GLOBAL_CONTEXT_ENABLED=false`. Le passage à
`true` est une décision distincte et réversible; `GLOBAL_CONTEXT_ADVISORY_ONLY`
et `GLOBAL_CONTEXT_FAIL_OPEN` restent `true`.

## Contrôles de sortie

- compte, `dry_run`, bandes d'approbation et variables IBKR inchangés ;
- tous les workflows Forex inactifs/non publiés et `fx_orders_enabled=false` ;
- aucun ordre/pending approval créé par le shadow ;
- fichiers `.duckdb`, `.env` et réponses brutes exclus de Git ;
- rollback testé ou démontré avant toute publication live.
