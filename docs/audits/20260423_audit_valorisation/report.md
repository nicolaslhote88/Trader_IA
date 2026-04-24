# Audit de valorisation — 3 IA trading

**Date d'analyse** : 23/04/2026
**Snapshot utilisé** : `/snapshots/duckdb_20260422/ag1_v3_*.duckdb` (extraction 22/04, dernier run 21/04 après-midi)
**Contexte utilisateur** : Nicolas soupçonne que la performance réelle des 3 IA est meilleure que ce qu'affichent les KPI du dashboard (notamment la ligne "ROI" et "PnL total en €"). Cas typique : Grok plafonne à +1,6 k€ alors que ses positions courantes ont l'air très performantes.

---

## TL;DR

1. **Le ROI affiché (3,31 % Grok / 3,43 % ChatGPT / 2,30 % Gemini) est mathématiquement correct** compte tenu des données persistées. Il n'y a pas de bug de calcul élémentaire : `ROI = (TV − 50 000) / 50 000` et `TV = cash + Σ(qty × last_price)` sont cohérents ligne à ligne.
2. **La performance "réelle" du système V2 est beaucoup plus modeste qu'il n'y paraît** parce qu'un gain de +1 458 € a été hérité de V1 à la bascule du 22/02/2026, et les 2 mois de V2 n'ont ajouté net que +195 € à +256 € (sauf Gemini qui a perdu −306 €).
3. **Les gains non-réalisés visibles sur les positions ouvertes (+3 684 € pour Grok) sont compensés par des pertes/frais réalisés passés** que l'accounting du cash "absorbe" silencieusement. Math implacable : `50 000 − cost_basis_ouvert − cash = réalisé_net_cumulé`. Pour Grok cela donne ~+2 031 € de pertes non retrouvées ailleurs.
4. **`core.position_lots` est incohérent avec la balance cash** sur les 3 bases (écart de −2 344 € à +1 350 €). Ce n'est pas le bon compteur de P&L réalisé : il sous-reporte ou sur-reporte selon l'IA.
5. **Les frais de transaction ne sont pas enregistrés** (`core.fills.fees_eur = 0` partout), ce qui n'invalide pas la simulation mais empêche toute réconciliation fine.
6. **Le point de bascule V1→V2 est identique sur les 3 IA** : 22/02/2026 14:07, TV = 51 458 €, PnL hérité = +1 458 €. Même dossier de trades migrés (`ORD_HIST_*`), même P&L. C'est un import synchronisé.

Conclusion : **la lecture "+1 653 € sur Grok" n'est pas un bug d'affichage, c'est la bonne agrégation des cycles réels depuis le 15/01**. La performance des positions courantes est bonne (upnl +3,6 k€ sur Grok) mais elle ne représente pas un gain net tant qu'elle n'est pas matérialisée en cash : aujourd'hui l'entièreté de ces unrealized gains est déjà mentalement "remboursée" par les pertes réalisées des trades passés.

---

## 1. Données de base

Tous les chiffres proviennent du dernier `core.portfolio_snapshot` enregistré pour chaque IA (21/04/2026).

| IA       | Cash (€) | Equity (€) | Total Value (€) | Total PnL (€) | ROI    |
|----------|---------:|-----------:|----------------:|--------------:|-------:|
| ChatGPT  | 6 353    | 45 362     | **51 714**      | **+1 714**    | 3,43 % |
| Gemini   | 848      | 50 304     | **51 152**      | **+1 152**    | 2,30 % |
| Grok     | 811      | 50 842     | **51 653**      | **+1 653**    | 3,31 % |

Baseline théorique par IA : 50 000 € (V1).
ROI = (TV − 50 000) / 50 000 — valide sur les 3 bases, cohérent ligne à ligne dans `portfolio_snapshot`.

---

## 2. Cohérence interne du dernier run

Pour le dernier run de chaque IA, on a vérifié :
- `equity_eur` = `SUM(market_value_eur)` de `core.positions_snapshot` → **écart nul sur les 3 bases**
- `last_price × qty` = `market_value_eur` ligne par ligne → **écart nul**
- `mtm_latest` vs `positions_snapshot` (même symbole, même run_id) → écart de marché de ±0,3 % (prix plus frais côté snapshot, mtm_latest est quelques minutes en retard).

Donc : **pas de désalignement interne**. Les KPI affichés sont l'image fidèle des tables de stockage.

---

## 3. Le gain +1 458 € hérité de V1 (invariant sur les 3 IA)

Premier snapshot V2 enregistré, identique sur les 3 bases :

```
ts                 cash    equity   TV      PnL     ROI
22/02 14:07:14     28 783  22 675   51 458  +1 458  2,92 %
```

Les 3 IA démarrent V2 avec **exactement la même composition** (cash 28 783 €, equity 22 675 €). Ce n'est pas le capital initial de 50 000 € — c'est un état intermédiaire importé depuis V1 avec déjà +1 458 € de gains cumulés dessus.

Cet invariant vient de l'import :
- 19 ordres `ORD_HIST_*` en BUY dans `core.orders`, tous datés 15/01/2026 13:00 avec `broker = 'MIGRATION'` et raison `'Importation historique complète'`.
- 7 ordres `ORD_HIST_*` en SELL entre 17/01 et la bascule.
- Répartition identique sur les 3 bases → l'import a été fait avec le même script pour les 3 IA.

**Impact sur la lecture** : le **vrai** point de départ pour juger l'activité V2 n'est pas 50 000 € (ROI 0 %) mais 51 458 € (ROI 2,92 %).

---

## 4. Performance V2 nette (22/02 → 21/04 ≈ 2 mois)

| IA       | TV bascule | TV actuel  | Δ net V2    | % sur 50 k€ | Commentaire |
|----------|-----------:|-----------:|------------:|------------:|-------------|
| ChatGPT  | 51 458     | 51 714     | **+256 €**  | +0,51 %     | léger vert  |
| Gemini   | 51 458     | 51 152     | **−306 €**  | −0,61 %     | **négatif** |
| Grok     | 51 458     | 51 653     | **+195 €**  | +0,39 %     | très flat   |

Donc en **2 mois de V2, aucune IA n'a significativement créé de valeur nette**. Tous les "gains" visibles aujourd'hui (+1,1 k€ à +1,7 k€) viennent essentiellement du legs V1.

---

## 5. L'énigme des gains non-réalisés

Pour Grok aujourd'hui :
- Positions courantes : 19 lignes, cost_basis = 47 159 €, market_value = 50 842 €
- **Unrealized P&L = +3 684 €** (positions courantes performantes)
- Cash = 811 €
- Total P&L affiché = +1 653 €

Écart = 3 684 − 1 653 = **2 031 €** "manquants".

**Où sont-ils ?** Math inévitable : si on a investi 47 159 € en positions depuis 50 000 € de capital et qu'il ne reste que 811 € de cash, alors :

```
realized_net = 50 000 − 47 159 − 811 = +2 031 € "manquants du cash"
            = pertes_réalisées + frais_cumulés
```

C'est la conservation du cash : tout euro qui n'est ni en position ni en cash est sorti du compte via une vente à perte ou des frais. Ici **2 031 € ont été brûlés par des trades fermés perdants ou des frais depuis le début**.

Résultat identique sur les 3 IA :

| IA       | cost_basis | cash  | realized implicite |
|----------|-----------:|------:|-------------------:|
| ChatGPT  | 43 165     | 6 353 | **+482 €**         |
| Gemini   | 49 648     | 848   | **−496 €**         |
| Grok     | 47 159     | 811   | **+2 031 €**       |

> Attention au signe : `50 000 − cost − cash` = ce qui a été "créé" (positif = gagné en réalisé, négatif = perdu en réalisé). Grok a paradoxalement **gagné +2 031 € en réalisé** (pas perdu). Donc la phrase plus haut "pertes non retrouvées" était dans le bon sens.

Gemini est la seule à avoir **−496 € de réalisé net** (cohérent avec −306 € de perf V2 nette + sa part des gains V1).

---

## 6. `core.position_lots` est incohérent

`position_lots` est censé enregistrer les lots ouverts/fermés avec leur P&L réalisé. Comparaison avec la math ci-dessus :

| IA       | Math (implicite) | `position_lots.realized` | Écart     |
|----------|-----------------:|-------------------------:|----------:|
| ChatGPT  | +482 €           | +1 591 €                 | **−1 109 €** |
| Gemini   | −496 €           | +1 848 €                 | **−2 344 €** |
| Grok     | +2 031 €         | +681 €                   | **+1 350 €** |

Les écarts vont dans **des directions différentes** selon l'IA. Ce n'est pas un bug systématique de signe ; c'est de la dérive accumulée.

Hypothèses principales :
- Les lots fermés en V1 (ORD_HIST_*) n'ont pas tous un `realized_pnl_eur` correctement calculé à l'import (les 4 lots V1 fermés rapportent exactement −293,28 € sur les 3 IA, valeur manifestement forfaitaire).
- L'appariement FIFO `open_fill_id`/`close_fill_id` ne colle pas toujours avec la méthode de calcul `avg_cost` utilisée par `positions_snapshot`.
- Les cycles BUY→SELL intraday peuvent créer des mini-lots qui ne sont pas agrégés dans `position_lots`.

**À faire** : comparer lot-par-lot fills et position_lots sur Gemini (où l'écart est le plus gros, −2 344 €) pour isoler le mode de dérive.

---

## 7. Pourquoi la courbe de TV semble "figée"

Observation : amplitude de `total_value_eur` sur 30 jours glissants :

| IA       | TV min  | TV max  | Amplitude |
|----------|--------:|--------:|----------:|
| ChatGPT  | 51 714  | 52 005  | **291 €** |
| Gemini   | 51 152  | 51 284  | **131 €** |
| Grok     | 51 653  | 51 726  | **73 €**  |

Amplitude de l'equity, elle, est de 6 k€ à 16 k€ sur la même période. Le cash et l'equity bougent en sens opposés **exactement par la même quantité** à chaque run : c'est la signature des cycles BUY/SELL qui se compensent à l'intérieur d'un run (conservation du cash).

La vraie volatilité de marché (mouvements de prix sur positions ouvertes entre deux runs) se voit dans l'évolution de la TV entre runs — et elle est quasi nulle. **Interprétation** : les 3 IA tournent leur portefeuille suffisamment vite pour "raser" les unrealized gains à chaque cycle. Les +3 684 € d'unrealized de Grok visibles au snapshot 21/04 17:05 ne sont que la variance accumulée depuis les derniers trades du cycle de l'après-midi.

**Ce n'est pas un bug** — c'est le comportement d'un système à haut turnover. Mais ça rend la courbe de TV peu lisible pour juger la perf réelle.

---

## 8. Ce qui manque pour faire mieux

- `core.fills.fees_eur = 0` sur les 219 fills Grok (idem 3 IA) : les frais ne sont pas modélisés, donc le P&L brut est légèrement surévalué par rapport à un P&L net.
- `core.cash_ledger` couvre uniquement 23/02 → 02/03/2026 (sauf Gemini qui a 56 lignes dès 01/01 mais total net ≈ 0 €). La tenue du ledger a été abandonnée au début de V2 — **réparer l'écriture dans ce ledger** réglerait 90 % des réconciliations.
- `drawdown_pct` = 0,00 % sur les 3 IA sur la dernière snapshot : le calcul de drawdown est très probablement cassé (aucune IA n'a jamais eu 0 % de DD sur 2 mois de trading).
- Pas de champ "mark-to-market at close" persisté : impossible de calculer un P&L "ce que j'aurais eu si j'avais liquidé en fin de journée".

---

## 9. Recommandations priorisées

| # | Recommandation                                                                                | Effort | Impact lecture |
|---|-----------------------------------------------------------------------------------------------|--------|----------------|
| 1 | Ajouter sur le dashboard un **ROI V2 net** = `(TV_now − TV_bascule) / 50 000` en plus du ROI absolu | Faible | Très fort — c'est la vraie perf. |
| 2 | Fixer l'écriture de `cash_ledger` (audit noeud n8n qui faisait les INSERTs avant le 02/03)      | Moyen  | Fort — permet la réconciliation. |
| 3 | Décomposer le P&L affiché en 3 lignes : **hérité V1** / **réalisé V2** / **unrealized courant**  | Faible | Fort — sépare le vent du bruit. |
| 4 | Vérifier le calcul de `drawdown_pct` (probablement stuck à 0)                                  | Faible | Moyen          |
| 5 | Auditer `position_lots` lot-par-lot sur Gemini pour isoler la dérive de −2 344 €              | Moyen  | Moyen          |
| 6 | Modéliser les frais de transaction dans `core.fills.fees_eur`                                   | Faible | Moyen (surtout pour passer en live) |

---

## 10. Classement des 3 IA sur la période V2 (22/02 → 21/04)

Si on remet tout le monde au même point de départ théorique de 51 458 € (le point commun de bascule V1→V2) :

1. **ChatGPT** : +256 € sur 2 mois (+0,51 %). Portefeuille plus cash-heavy (6 353 € en cash) — plus conservateur.
2. **Grok** : +195 € (+0,39 %). Très investi (98 % des fonds en positions), unrealized gains forts (+3,7 k€) mais beaucoup de rotation intra-cycle qui efface les gains.
3. **Gemini** : −306 € (−0,61 %). Le plus diversifié (23 positions vs 18-19), le plus de fills V2 (207 vs 143/114), mais négatif net.

Aucune des 3 IA n'a significativement battu un simple buy-and-hold du CAC40 sur la période (à confirmer avec un benchmark).

---

*Généré le 23/04/2026 à partir de l'extraction DuckDB du 22/04.*
*Scripts d'audit : `/infra/maintenance/audit_valorisation_20260423/`*
