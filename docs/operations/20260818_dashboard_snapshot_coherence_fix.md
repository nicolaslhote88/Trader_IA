# 2026-08-18 — Cohérence temporelle du dashboard après exécution AG1

## Incident

Après le run AG1 V4 `21208` (`RUN_20260818_163006_21208`), le dashboard a affiché
une valeur totale de `11 151,05 EUR` : cash post-vente `1 633,30 EUR` additionné
aux positions IBKR pré-vente `9 517,75 EUR`.

La NAV IBKR réelle contrôlée directement après le run était `9 927,40 EUR`
(`1 637,68 EUR` de cash et `8 289,72 EUR` d'actions). Le run avait vendu
`2 AVGO` et `70 MAU.PA`; les deux ordres étaient `FILLED`.

## Cause

`_ag1_apply_ibkr_live_overlay()` considérait
`portfolio_positions_ibkr_latest` comme fraîche pendant 18 heures, sans vérifier :

- que son `updated_at` était au moins aussi récent que `core.positions_snapshot.ts`;
- que sa quantité correspondait à celle du snapshot affiché.

Le snapshot AG1 post-trade contenait les nouvelles quantités, mais l'overlay PF
de 16:15 réinjectait les anciennes valeurs de marché jusqu'au refresh PF suivant.
`_ag1_attach_fx_breakdown()` reprenait ensuite les mêmes montants périmés dans la
cascade de rendement.

## Correctif

Dans `services/dashboard/app.py` :

- ajout de `_ag1_ibkr_row_is_coherent_with_snapshot()`;
- application de l'overlay seulement si quantité identique et horodatage non antérieur;
- conservation des valeurs monétaires du snapshot le plus récent dans la décomposition FX;
- conservation possible de la devise et du taux de l'overlay, qui ne changent pas avec la vente.

Test ajouté :
`services/dashboard/tests/test_ibkr_snapshot_overlay_coherence.py`.

## Déploiement et rollback

- Cible :
  `/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/app.py`
- Backup :
  `app.py.bak_ag1_snapshot_coherence_20260818_145426`
- Rollback : recopier le backup sur `app.py`, puis redémarrer
  `root-trading-dashboard-1`.

## Preuves post-déploiement

- 3 tests de cohérence : `OK`;
- replay read-only sur le snapshot réel : actions `8 292,95 EUR` avant/après overlay;
- Streamlit AppTest complet : 0 exception;
- tuiles rendues : valeur totale `9 926,25 EUR`, cash `1 633,30 EUR`,
  investi `8 292,95 EUR`, ROI `-0,74 %`, cash `16,5 %`;
- SHA-256 local/live identique :
  `953c2d550fed37626ff2587b6ab75f6d75bd244c133390f38c115eaed1643074`;
- broker : compte live `U25651155` aligné, aucune approbation en attente.

L'écart de quelques euros entre la NAV IBKR interrogée (`9 927,40 EUR`) et le
snapshot du run (`9 926,25 EUR`) correspond au mouvement de marché entre les deux
horodatages, pas à une rupture comptable.
