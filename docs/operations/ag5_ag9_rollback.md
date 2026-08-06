# Rollback AG5–AG9

## Niveau 0 — désactivation fonctionnelle immédiate

Mettre, sans toucher aux variables IBKR :

```text
GLOBAL_CONTEXT_ENABLED=false
WORLD_MONITOR_ENABLED=false
```

Redémarrer uniquement `global-context-synthesizer` et
`worldmonitor-adapter`. AG1 reçoit alors un pack `GLOBAL_CONTEXT_DISABLED` et
reproduit son chemin historique. Les snapshots restent auditables.

## Niveau 1 — workflows producteurs

Dépublier AG5, AG6, AG7, AG8, AG9 et la synthèse. Ne pas publier les workflows
Forex historiques. Les services peuvent rester arrêtés; AG1 est fail-open.

## Niveau 2 — AG1 V4

Si le candidat enrichi a été publié :

1. sauvegarder son export et ses logs ;
2. importer l'export AG1 live sauvegardé avant migration ;
3. republier `AG1V4CONSENSUS` ;
4. redémarrer n8n et runners ;
5. vérifier `active=1`, `versionId=activeVersionId`, crons 14:00/16:30 Paris ;
6. comparer hashes des nœuds consensus, safety et broker ;
7. vérifier `/health`, pending approvals et `fx_orders_enabled=false`.

La migration ledger est additive : les colonnes `global_context_*` peuvent
rester présentes et nulles. Ne pas reconstruire la base live pour les retirer.

## Niveau 3 — services et bases

Arrêter les deux nouveaux services, restaurer le compose sauvegardé, puis
recréer les seuls containers concernés. Les bases dédiées ne doivent pas être
supprimées immédiatement : les renommer hors chemin actif ou conserver en
archive à permissions strictes pour l'audit.

Pour le shadow, les cibles exactes sont uniquement les trois fichiers suffixés
`_shadow.duckdb`. Vérifier leur chemin absolu avant toute opération. Une
suppression éventuelle doit être explicitement autorisée et précédée d'une
sauvegarde; le rollback normal ne requiert aucune suppression.

## Preuve du fallback

Le test de contrat vérifie que le fetch absent construit un pack warning, et les
hashes source suivants doivent rester ceux du baseline audité :

```text
consensus  c39434c3ff5b484ba2615fa6a0ec7c722387b790c3f83c630070645d611d1316
safety     d658f005a41131e175792f5b5dea63e3445fb744f8979f347916dacc9722883d
broker     060d649426d7ad015e68734fe1cda4909ecdf89503d1158e26d77f3a7e8b5e41
```
