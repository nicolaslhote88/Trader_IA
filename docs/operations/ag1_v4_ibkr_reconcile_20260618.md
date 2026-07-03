# AG1 V4 — IBKR live reconciliation 2026-06-18

## Faits validés

- Compte IBKR live : `U25651155`, broker authentifié, `dry_run=false`.
- Avant réparation, IBKR exposait 6 positions : `PEUG`, `DSY`, `ELEC`, `NVDA`, `VIRP`, `LR`.
- Avant réparation, DuckDB/dashboard AG1 V4 exposait seulement 4 positions : `DSY.PA`, `PEUG.PA`, `LR.PA`, `VIRP.PA`.
- Cause : les fills IBKR tardifs / hors-bande n'étaient pas réimportés dans `core.fills`, donc `core.position_lots` et `core.positions_snapshot` restaient incomplets.

## Réparation appliquée

Commande source utilisée depuis le repo local :

```bash
Get-Content outils/scripts/ag1_v4_reconcile_ibkr_live.py -Raw \
  | ssh vps 'docker exec -i yf-enrichment python - --db-path /files/duckdb/ag1_v4_consensus.duckdb --broker-url http://ibkr-broker:8080 --apply'
```

Script copié sur le VPS :

```text
/opt/trader-ia/outils/scripts/ag1_v4_reconcile_ibkr_live.py
```

Backup créé avant écriture :

```text
/files/duckdb/backups/ag1_v4_consensus.pre_ibkr_reconcile_20260618_211129.duckdb
```

Run de réconciliation créé :

```text
RUN_RECON_IBKR_20260618_211130
```

Fills importés :

- `NVDA` x2, execution `0000d7f3.6a34005c.01.01`, ordre `ORD_RUN_20260618_210140_19175_002`
- `PEUG.PA` x8, execution `00024d11.6a2ba329.01.01`, ordre `ORD_RUN_20260612_101651_ecfb91d350fd3197_003`
- `ELEC.PA` x2, execution `00024d11.6a2ba7a5.01.01`, ordre `ORD_RUN_20260612_101651_ecfb91d350fd3197_004`
- `ELEC.PA` x2, execution `00024d11.6a2ba7a6.01.01`, ordre `ORD_RUN_20260612_140018_9fd296c9fae6c2d2_003`

## État après réparation

Second dry-run :

```json
{
  "missing_fills": [],
  "unmatched_stock_fills": [],
  "position_diffs": []
}
```

Dernier snapshot DuckDB :

- `cash_eur = 6549.14`
- `equity_eur = 3347.27`
- `total_value_eur = 9896.41`
- Positions : `PEUG.PA` x16, `ELEC.PA` x4, `DSY.PA` x28, `LR.PA` x3, `NVDA` x2, `VIRP.PA` x1.

## Correctif durable déployé

Déploiement effectué le 2026-06-18 :

- Workflow n8n mis à jour : `AG1-PF-V1 - Portfolio MTM (DuckDB-only, AG1-V4)`, id `iKnGA9gCMUFZfKYCCsWVF`.
- Version publiée active vérifiée : `active=1`, `activeVersionId=df2d87e6-ef8f-4695-b3d3-627cc534c917`.
- Nouveaux nœuds publiés :
  - `PF.00B - Fetch IBKR State`
  - `PF.00C - Reconcile IBKR Ledger`
- Backup workflow avant import : `/local-files/tmp/AG1-PF-V1.before_.json`.

Comportement :

- À chaque run `AG1-PF-V1`, le workflow lit `/health`, `/positions`, `/fills`, `/account/ledger` sur `ibkr-broker`.
- Si IBKR est authentifié et aligné sur `U25651155`, il importe les fills stock manquants, reconstruit `core.position_lots`, puis écrit un snapshot `RUN_RECON_IBKR_PF_*` uniquement si un écart est détecté.
- Si IBKR n'est pas authentifié, le nœud n'écrit rien et laisse le MTM existant continuer.

Tests effectués :

- Test sur copie DuckDB alignée : `NO_DIFF`, aucune écriture.
- Test sur copie DuckDB avec fill `NVDA` supprimé artificiellement : fill réimporté, statut `WRITTEN`.
- Post-déploiement : version publiée active contient les deux nouveaux nœuds, dashboard HTTP `200`.

État IBKR au moment du dernier check post-déploiement :

- `authenticated=false`, `dry_run=false`, erreur `CPAPI HTTP 401`.
- Action opérationnelle : relogin manuel IBKR requis pour que la réconciliation automatique reprenne.

## Actions restantes

- Relogin manuel IBKR (`https://localhost:5000` via tunnel SSH + 2FA) : la session CPAPI est passée en `401` après le déploiement.
- Après relogin, le prochain run `AG1-PF-V1` réconciliera automatiquement si un nouvel écart apparaît.
