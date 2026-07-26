# 2026-07-22 — Audit : que contient la barre « Ajustement réconciliation IBKR » (+105,41 €) ?

Question : pourquoi faut-il un poste de recollage entre le ledger local et la NAV IBKR, et pourquoi est-il **positif** ?
Réponse courte : ce n'est pas un poste économique, c'est un **poste de bouclage** (plug = `réalisé_net_IBKR − (réalisé_stocké − frais_broker)`) qui compense **quatre défauts de données du ledger local**. Décomposition exacte au centime ci-dessous (script `/tmp/decompose_plug.py`, lecture seule, taux BCE/Frankfurter).

## Décomposition du +105,41 € (au 22/07)

| # | Composante | EUR | Preuve |
|---|---|---|---|
| 1 | **Frais déjà déduits dans le réalisé stocké, re-déduits par la barre « Frais broker »** | **+86,68** | Lot par lot : `brut_reconstruit(close_events) − realized_pnl_eur` = frais open+close du lot, au centime (ex. NVDA 53,81−51,93 = 1,88 = 0,87+1,01). La cascade re-soustrait les 127,90 en totalité → 86,68 comptés 2×, le plug les rembourse. (Solde 41,22 = frais d'entrée des positions ouvertes, correctement déduits une seule fois.) |
| 2 | **Clôtures US comptées en devise incohérente** | **≈ −25** | Lots US clos valorisés en natif traité comme EUR (borne haute −25,03 si tous natifs ; certains lots sont déjà en EUR → vraie valeur entre −25 et −5). |
| 3 | **Contrepartie du FAUX « Impact change sur valeur latente » −144,38** | **≈ +144,40** | Le −144,38 = artefact de 3 lots ouverts dont `open_price` est en USD natif traité comme EUR par le loader (`cost_paid`, app.py ~l.8314) : AVGO −47,41 + NVDA −47,99 + TSM −49,00 = −144,40 ; les 5 autres positions USD (prix en EUR) s'annulent (+0,02). Le VRAI FX latent ≈ 0 (achats juillet à ~0,874-0,876, taux courant 0,8764). La cascade descend de −144,38 à tort, le plug remonte d'autant. |
| 4 | **Change réel des conversions cash EUR↔USD + frais FX + imprécisions** | **≈ −100,6** | Par différence. Non tracé localement : `cash_ledger` ne contient QUE le dépôt initial (10 000). Les ~78 trades FX « legacy » IBKR (conversions autour des achats/ventes US, frais min ~2 USD chacun) et l'écart taux exécution vs BCE vivent uniquement chez IBKR. |
|   | **Total** | **+105,41** | = plug constaté ✓ |

**Pourquoi positif** : les deux plus gros termes (1 et 3) corrigent des erreurs qui *sous-évaluent* le résultat dans la cascade (frais comptés deux fois, faux FX −144) → le plug « rend l'argent ». Le seul contenu réellement économique du poste (terme 4) est, lui, **négatif** (~−100 €, essentiellement coût de friction FX réel + erreurs résiduelles de devise).

## Cause racine (défaut de conception, pas un bug ponctuel)

Le ledger local n'a **aucune notion de devise** :
- `core.fills.price` : pas de colonne currency ni fx_rate. Les prix arrivent tantôt en USD (ordre local), tantôt en EUR (import/réconciliation IBKR) selon la source du fill — indétectable a posteriori sans comparer à l'avg IBKR.
- `core.instruments.currency` = 'EUR' pour TOUS les symboles (y compris NVDA, AVGO…).
- `core.position_lots.realized_pnl_eur` : mal nommé — net de frais, en devise du fill (mixte).
- `core.cash_ledger` : une seule ligne (DEPOSIT 10 000) — aucune conversion FX, dividende, frais.

Vérité devise constatée le 22/07 (comparaison `open_price` ledger vs avg IBKR) : **USD natif** = AVGO 390,80, NVDA 201,32, TSM 403,65 ; **EUR** = CRM, INFY, NFLX, PDD, BHP. Mélange au sein d'un même jour d'achat.

## Conséquences en chaîne (tous les affichages issus du couple prix-ledger/loader)

- « Impact FX −144,38 » (tableau positions, bandeau exécuteur, cascade) : artefact ~100 %.
- « P&L latent −37,16 » (bandeau exécuteur = prix 107,22 + FX −144,38) : faux ; vrai latent économique ≈ +107.
- « P&L réalisé net 126,23 » (= Total − Latent) : mécaniquement surestimé d'autant ; vrai réalisé net ≈ −18 € (cohérent avec : brut éco ~+210 − frais 127,90 − friction FX ~−100).
- Le pont bandeau→cascade déployé le 22/07 reste EXACT (il boucle sur la NAV, qui est juste) — ce sont les *étiquettes* prix/FX intermédiaires qui mentent.

## Fix de fond — DÉPLOYÉ le 22/07 (même jour)

**Convention établie : `core.fills.price` = EUR, toujours.** Nouvelles colonnes `currency`, `price_native`, `fx_rate_eur` (taux du jour du fill).

1. **Backfill** (`outils/scripts` via `/tmp/bf.py`, backup base `/tmp/ag1_v4_consensus.duckdb.bak_fxmig_20260722_133614`) : 68 fills classés (36 EUR locaux, 23 US importés déjà EUR [source `ibkr_pf_reconcile`], 9 US natifs convertis [source `confirmed_or_imported`]), 0 warning, garde-fou ratio vs `market_prices` ∈ [0,947 ; 1,012]. Taux BCE/Frankfurter par date. Lots rebuildés (45).
2. **Rebuilds prod inchangés** : les deux rebuilds (reconcile horaire + duckdb_writer) recalculent depuis fills → avec des prix tout-EUR ils sont devenus corrects sans patch. Vérifié : le reconcile de 16:15 a conservé les valeurs migrées.
3. **Self-healing** : `outils/scripts/fx_normalize_fills.py` déployé sur le VPS (`/opt/trader-ia/fx-normalizer/`, venv finnhub, cron `5,35 * * * *`, log `/var/log/fx_normalize.log`) — normalise tout fill `currency IS NULL` (les writers n8n écrivent encore du natif) puis rebuild. Idempotent.
4. **Cascade refondue** (`waterfall.py`) : barres « P&L réalisé net clos / partiel » (EUR économique, frais inclus), « P&L latent (prix, IBKR) », « **Impact change latent (réel)** » (fills : `remaining × prix_natif × (taux_courant − taux_achat)`), « **Change cash & conversions FX (non tracé)** » = bouclage NAV explicite. **Les barres « Frais broker (tous trades) » et « Ajustement réconciliation IBKR » sont supprimées** (double comptage éliminé, plug remplacé par le poste honnête).
5. **Loader** : `cost_eur = qty × PRU EUR IBKR` (frais inclus), `pnl_fx = total − prix` (vrai FX), `fx_rate_entry` = taux d'achat moyen pondéré des fills. L'artefact −144,38 disparaît du tableau Positions et du bandeau exécuteur.

**Valeurs constatées après migration (NAV 10 092,76)** : réalisé net clos +47,34 (avant : « 166,94 » mixte sans frais), partiel −11,74, latent prix +110,41, **FX latent réel +2,55** (avant : artefact −144,38), **change cash −55,80** (avant : plug +105,41). NAV raccord exact.

## MAJ 22/07 soir — bandeau Rendement = décomposition cascade + cash par devise

- **Barre « Impact change sur liquidité » supprimée** (toujours ~−2 €, bruit) : fusionnée dans « **Frais simulés de sortie (vente + change)** » — le résultat final de sortie est inchangé.
- **Bandeau onglet Rendement remplacé** par la décomposition exacte de la cascade en 6 tuiles (2×3) : réalisé net clos / réalisé net partiel / latent prix IBKR / impact change latent réel / change cash & FX / **= gain total (NAV − capital)** — la somme des 5 postes = gain total par construction. Ancien pont (caption + expander) supprimé, devenu redondant. Fallback : anciennes tuiles si cascade indisponible.
- **Cash par devise affiché sous le bandeau** : `read_cash_balances()` (waterfall.py) lit `broker_costs.duckdb.cash_snapshots` (collecteur quotidien 20h) — ex. « 2 350,12 EUR · 1 341,39 USD (≈ 1 176,50 EUR au taux 0,8771) — total ≈ 3 526,62 EUR (snapshot du 21/07) ».
- **Incident réparé** : le cron `fx_normalize_fills.py` plantait sur `pytz` manquant (venv finnhub) dès qu'un fill était en attente — `pip install pytz` fait, fill du run 14h normalisé (prix déjà EUR, réalisé inchangé), 0 pending.

## Backlog restant

- **Patch propre des nodes n8n** (duckdb_writer `_upsert_fills` + reconcile `insert_missing_fills`) pour écrire `currency`/`fx_rate_eur` dès l'insert (taux dispo : `rates` du snapshot ou ledger IBKR) → le cron normalizer deviendra un simple filet de sécurité.
- **P2 complet** : historique des conversions FX via IBKR **Flex Query** (l'API standard ne remonte plus les ~78 conversions passées ; `broker_costs.duckdb.broker_trades` sec_type CASH est vide) → `cash_ledger` type FX_CONV → décomposer les −55,80 en frais de conversion vs change pur. Cash USD réel au 21/07 : 1 341,39 USD (`cash_snapshots`).
- Committer `services/dashboard/`, `outils/scripts/fx_normalize_fills.py` et ce doc (working tree non commité).
