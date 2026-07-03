# Déploiement — AG3 split + gate STALE_FUNDA (Sprint 1)

**Date** : 2026-06-22 · **Statut** : DÉPLOYÉ LIVE, vérifié statiquement (run runtime = crons de nuit).
**Contexte** : audit `docs/audits/20260622_ag3_v2_analysis.md`. AG3 pèse 0,30-0,34 du scoring AG1 mais
fraîcheur 🔴 (âge moyen 15,7 j, max 112 j ; batch 50/run sur tout l'`enabled` quarantaine incluse).

## Ce qui a changé

### 1. Split d'AG3 en 2 workflows (modèle AG2)
Ancien `AG3-V2` (id `WZToJYQbJKJBWpUmv9wvc`) **désactivé** (rollback). Deux nouveaux workflows actifs,
pilotés par `ag2_v3.duckdb.universe_segments` (quarantaine exclue de fait), écrivant dans la **même**
base `ag3_v2.duckdb` :

| Workflow | id n8n | Cron (UTC) | Segments | Batch | SLA | batch_state key |
|---|---|---|---|---|---|---|
| AG3-V2 — Fundamental Held+Core | `AG3V2HELDCORE20260622` | `0 1 * * *` | HELD+CORE_AUTO (~56) | 80 | ≤24 h | `ag3_v2_held_core_last_index` |
| AG3-V2 — Fundamental Watchlist Nightly | `AG3V2WATCHNIGHT20260622` | `0 2 * * *` | WATCHLIST (~196) | 60 | <5 j (~4 j) | `ag3_v2_watchlist_last_index` |

Générateur : `agents/trading-actions/AG3 - Les fondamentaux/AG3-V2/build_split_workflows.py` (part de l'export live
`AG3-V2-workflow.json` ; `build_workflow.py` est périmé=GoogleSheets). Node `AG3V2.01 - Read Universe`
patché avec `symbol IN (SELECT symbol FROM universe_segments WHERE active AND segment IN (...))`.

### 2. Gate STALE_FUNDA (AG1 V4)
Node `R8 — Data Prep for Matrix (Fusion Filter)` de `AG1V4CONSENSUS` :
- nouvelle constante `MAX_FUNDA_AGE_HOURS` (env `AG1_ACTIONS_MAX_FUNDA_AGE_HOURS`, défaut **168 h**) ;
- si fondamental absent **ou** `funda_age > seuil` → flag `STALE_FUNDA` + **neutralisation**
  (`Funda_Score`/`Funda_Risk`→50, `Funda_Upside`/`Target_Price`→0, `Funda_Usable=False`) ;
- nouveaux champs émis : `Funda_Age_Hours`, `Funda_Usable` ;
- **PAS** ajouté à la liste de reject dur du risk manager (`07_validate_enforce_safety_v5`) : un
  fondamental périmé ne doit pas geler le trading (sinon, en transition, quasi tout l'univers > 168 h
  serait bloqué). La gate borne l'*influence*, pas l'ordre.

## Procédure de déploiement exécutée
```bash
# backups
n8n export:workflow --id=WZToJYQbJKJBWpUmv9wvc --output=/tmp/bk_ag3v2_old.json
n8n export:workflow --id=AG1V4CONSENSUS --output=/tmp/bk_ag1v4.json   # -> .codex-tmp/ag3_sprint1_20260622/
# patch R8 (script python sur l'export live, PAS l'éditeur : troncature des gros fichiers)
# import (⚠️ chmod 644 avant docker cp : scp arrive en 600 root, EACCES sinon)
chmod 644 *.workflow.json ; docker cp … root-n8n-1:/tmp/ ; docker exec -u root … chmod 644 …
n8n import:workflow --input=/tmp/wf_core.json
n8n import:workflow --input=/tmp/wf_watch.json
n8n import:workflow --input=/tmp/wf_ag1v4.json      # ⚠️ DÉSACTIVE AG1V4 -> republish obligatoire
n8n publish:workflow --id=AG3V2HELDCORE20260622
n8n publish:workflow --id=AG3V2WATCHNIGHT20260622
n8n publish:workflow --id=AG1V4CONSENSUS            # réactive AG1 (sinon PM OFF !)
n8n unpublish:workflow --id=WZToJYQbJKJBWpUmv9wvc   # désactive l'ancien AG3
docker restart root-n8n-1
```

## Vérifs post-déploiement (faites)
- États en base : `AG1V4CONSENSUS active=1` (STALE_FUNDA présent dans R8), 2 AG3 `active=1` (crons OK),
  ancien AG3 `active=0`.
- SQL segments sur base live : HELD+CORE = 56 (56 EQUITY), WATCHLIST = 198 (196 EQUITY), 0 doublon.
- ⚠️ Run **runtime non validé en CLI** (`n8n execute` échoue : port broker 5679 occupé pendant que n8n
  tourne) → validation aux crons de nuit (01:00/02:00 UTC). Tâche planifiée de vérif le lendemain matin.

## Pièges rencontrés (à retenir)
1. `import:workflow` **désactive** le workflow (« Remember to activate later ») → **toujours republier**.
2. `docker cp` d'un fichier `scp` (600 root) → `EACCES` pour l'user `node` → `chmod 644` avant import.
3. L'éditeur **tronque les gros fichiers** : le R8 (~750 lignes) a été tronqué à 735 (mi-ligne) puis
   réparé via shell. Patcher les gros fichiers via python/heredoc, jamais l'éditeur ; vérifier l'AST
   (en enveloppant le corps dans une fonction, car les nodes n8n ont un `return` top-level).

## Rollback
```bash
# AG3 : réactiver l'ancien, désactiver le split
n8n publish:workflow --id=WZToJYQbJKJBWpUmv9wvc
n8n unpublish:workflow --id=AG3V2HELDCORE20260622
n8n unpublish:workflow --id=AG3V2WATCHNIGHT20260622
# AG1 R8 : réimporter le backup pré-patch puis republier
n8n import:workflow --input=.codex-tmp/ag3_sprint1_20260622/ag1v4_PRE_patch_backup.json
n8n publish:workflow --id=AG1V4CONSENSUS
docker restart root-n8n-1
# (ou neutraliser la gate sans rollback : env AG1_ACTIONS_MAX_FUNDA_AGE_HOURS=999999)
```

## Dépendances / suivi
- La fraîcheur AG3 dépend de `universe_segments`, rafraîchi par `AG2 — Universe Health Quarantine`
  (vérifier que sa cadence suit ; dernier MAJ observé : 2026-06-19).
- Reste Sprint 2 (traiter 6 `ERR_SOURCE` + ticker mort `ALLEC.PA` ; POC source *financials reportés*
  type FMP — ne comble PAS le trou analystes) et Sprint 3 (seuils horizon, digest PM) — voir l'audit.

## À committer côté Windows
`AGENTS.md`, `agents/trading-actions/AG3 - Les fondamentaux/AG3-V2/build_split_workflows.py`, les 2
`AG3-V2-Fundamental-*.workflow.json`, `AG3-V2/README.md`, R8 patché
(`AG1-V4-Consensus Portfolio manager/workflow/nodes/pre_agent/R8_data_prep_matrix.code.py`),
`docs/audits/20260622_ag3_v2_analysis.md`, ce fichier.
