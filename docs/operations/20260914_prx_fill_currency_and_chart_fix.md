# PRX — coût d'achat, traçabilité FX et graphique — 2026-09-14

## Faits validés

Le graphique révélait une erreur du ledger, pas une mauvaise exécution IBKR.
Le fill `00029609.6a7c0f38.01.01`, ordre `1184124543`, porte 26 PRX,
conid `382625193`, listing `AEB`, exécutés le 2026-08-12 à 12:07:38 UTC
(14:07:38 Paris) à **39,355 EUR**, commission **3 EUR**.

Le réconciliateur classait les fills sans devise comme USD sauf quelques places
françaises. AEB était donc converti avec USD/EUR = 0,8661522 : prix stocké
34,087419831 EUR, frais arrondis 2,60 EUR. Le normaliseur complétait ensuite
`price_native` avec ce prix déjà erroné, car le symbole comportait un point.
Le lot ouvert et la ligne horizontale du graphique reprenaient ce coût faux.

Le coût des titres était minoré de 136,957084394 EUR, plus 0,40 EUR de frais
arrondis dans le ledger. Coût corrigé : 1 023,23 + 3 = **1 026,23 EUR** ;
PRU avec frais : **39,470384615 EUR**, conforme à IBKR (39,4703846).

La valorisation courante et le contexte portefeuille AG1 lisent les snapshots
et le MTM réconciliés avec IBKR (`4B_build_portfolio_context.code.py`). Les
snapshots PRX vérifiés utilisaient déjà ce PRU correct. Comparaison complète
avant/après : `core.positions_snapshot`, `core.orders`,
`portfolio_positions_ibkr_latest` et `portfolio_positions_mtm_latest` identiques.
Le dernier snapshot disponible pendant le contrôle était celui du vendredi
11 septembre à 15:15:09 UTC : NAV 9 904,49 EUR, inchangée par la réparation.
La valorisation IBKR interrogée le lundi est à un autre horodatage.

Sur 97 fills, 32 métadonnées natives/taux ont été corrigées : PRX et 31 fills
USD importés. Pour les USD, le prix natif vient désormais du fill brut IBKR ;
le taux est le taux implicite `prix_EUR_déjà_stocké / prix_natif_exécuté`.
Les montants EUR, frais et quantités de ces USD sont strictement inchangés.
Ce taux implicite préserve la conversion historique du ledger ; il ne constitue
pas une preuve d'une opération de change IBKR au même instant.

## Correctifs

- Réconciliateurs PF et manuel : devise explicite prioritaire, correspondance
  des places connues (dont AEB/EUR), erreur explicite si devise/taux inconnus ;
  écriture atomique de `currency`, `price_native`, `fx_rate_eur` avec le fill.
- Normaliseur FX : pour les fills importés, reprendre le prix natif brut IBKR
  et conserver la valeur EUR, sans reconstituer le natif avec un taux ultérieur.
- Maintenance `outils/scripts/repair_prx_fill_currency.py` : dry-run par défaut,
  transaction, garde conid/listing/prix/quantité et absence de vente PRX,
  backups de tables, contrôle d'idempotence. Prix/frais PRX réparés dans fills,
  lot ouvert et fill_costs ; cumul des frais des snapshots postérieurs +0,40 EUR.
  Les observations historiques du broker et décisions historiques sont conservées.
- Graphique : marqueur à l'heure UTC exacte et au prix EUR exécuté, quantité
  au survol ; plusieurs fills du même jour restent distincts. La courbe reste
  une série de clôtures quotidiennes, qui peut différer d'un prix intrajournalier.

## Déploiement et vérification

- Workflow PF `iKnGA9gCMUFZfKYCCsWVF` : `active=1`,
  `versionId=activeVersionId=a5893cea-990a-42bc-ab22-e60bc31f8711`.
  Seul le code de `PF.00C - Reconcile IBKR Ledger` a changé dans l'export live.
- Normaliseur : `/opt/trader-ia/fx-normalizer/fx_normalize_fills.py`.
- Graphique : `/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/app_modules/visualizations.py`.
- Réparation testée sur copie puis appliquée en DuckDB **1.4.3**, sans WAL,
  n8n/runners/dashboard et cron suspendus brièvement après preuve de zéro
  exécution active ; reprise effectuée. Broker et gateway conservés actifs.
- Rejeu synthétique du writer sur copie : fill EUR exact et frais 3 EUR ;
  insertion annulée par rollback. Deuxième passage maintenance : zéro correction.
- 6 tests PF/normaliseur, 25 tests dashboard, 16 tests AG2, 12 tests AG4,
  6 tests broker ; smoke JS AG1, tests devise/alertes et syntaxe n8n : OK.
- AppTest complet sur le dashboard déployé : aucune exception, figure PRX
  effectivement rendue avec x=`2026-08-12T12:07:38+00:00`, y=`39.355`, quantité 26.
- n8n et Streamlit répondent à leurs healthchecks. Aucun ordre déclenché par
  ces contrôles ; compte IBKR réel aligné et aucune approbation en attente.

## Sauvegardes et rollback

Dossier VPS : `/opt/trader-ia/backups/prx_chart_20260914/` :
`ag1_v4_before.duckdb`, `SHA256SUMS`, `pf_before.json`,
`visualizations_before.py`, `fx_normalize_before.py`, `live_repair.json`.
Tables avant réparation : `main.maintenance_fill_currency_20260914_{fills,position_lots,fill_costs,portfolio_snapshot}`.
Copies locales et preuves : `.codex-tmp/prx_chart_20260914/` (ignoré par Git).

Rollback applicatif : restaurer les deux fichiers sauvegardés vers les chemins
ci-dessus ; importer `pf_before.json`, republier l'ID PF puis redémarrer n8n,
les trois runners et le dashboard. Le rollback de base complet exige les mêmes
arrêts de writers et l'absence de nouvelles opérations depuis la sauvegarde ;
après de nouveaux trades, privilégier une restauration ciblée des champs
corrigés à partir des tables de maintenance pour conserver les nouveaux fills.
