# Correctif identité PRX.AS / PRX.PA — 2026-08-13

## Faits validés

- Ordre AG1 V4 : `PRX.AS`, 26 titres, run `RUN_20260812_140010_21010`.
- Fill IBKR : conid `382625193`, `PROSUS NV`, place primaire `AEB` (Amsterdam).
- `/positions` IBKR renvoie le ticker brut `PRX`, la devise EUR et aucune place.
- L'ancien réconciliateur AG1-PF ajoutait `.PA` à toute action EUR sans place.
- Aucun second ordre PRX n'a été exécuté pendant l'incident.

## Correctifs live

1. `AG1-PF-V1` reconstruit une correspondance canonique `conid -> symbole interne`
   depuis les fills persistés joints aux ordres. Le conid PRX devient donc
   toujours `PRX.AS`. En l'absence de preuve, aucune place n'est déduite de la
   seule devise EUR.
2. Le broker ignore désormais les places de dérivés `WAR/OPT/CFD` dans une
   réponse `secdef` et interdit le fallback SMART pour un suffixe dont la place
   primaire est connue. `PRX.PA` est rejeté ; `PRX.AS` résout le conid attendu.
3. Les lignes opérationnelles et historiques produites sous `PRX.PA` ont été
   renommées en `PRX.AS` par `outils/scripts/repair_prx_symbol_identity.py`.
   Les chaînes narratives historiques ont été conservées ; seuls les champs
   JSON dont la valeur était exactement `PRX.PA` ont été normalisés.

## Preuves de déploiement

- Workflow `iKnGA9gCMUFZfKYCCsWVF` : `active=1` et
  `versionId == activeVersionId == 5997294c-a7c0-4ac5-aa4f-2dc0cd0b0008`.
- Broker healthy, authentifié, compte live aligné `U25651155`.
- Résolution read-only : `PRX.AS -> 382625193`; `PRX.PA -> 0 résultat`.
- Réparation : 16 prix, 16 snapshots, 13 lignes MTM historiques, deux lignes
  courantes et 5 votes normalisés ; postcondition `PRX.PA = 0` sur les tables
  opérationnelles ciblées.

## Backups et rollback

- DuckDB complet :
  `/local-files/duckdb/backups/ag1_v4_consensus.bak_20260813_pre_prx_symbol_fix`
  (`sha256 b6f4196a31956ffbd47f41a705e455a12ca8e7ec009eda76948e4d8b4a76738e`).
- Workflow :
  `/opt/trader-ia/backups/n8n/20260813_prx_symbol_fix/ag1_pf_live_pre_prx_fix.json`.
- Broker :
  `/opt/trader-ia/services/ibkr-broker/app.py.bak_prx_contract_guard_20260813T195500Z`.
- Neuf tables DuckDB `main.maintenance_prx_symbol_identity_20260813__*`
  conservent les lignes avant réparation.

Rollback workflow : importer le backup, republier l'ID puis redémarrer n8n et
les quatre runners. Rollback broker : restaurer le fichier `.bak`, reconstruire
et recréer `ibkr-broker`. Rollback DuckDB complet : arrêter préalablement tous
les écrivains AG1/dashboard concernés, restaurer le fichier sauvegardé, puis
redémarrer et vérifier `/health`, les positions, les approbations et les runs.
