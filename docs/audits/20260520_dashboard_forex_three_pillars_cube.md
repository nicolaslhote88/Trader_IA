# Dashboard Forex et Three Pillars Monitor - mise a jour 2026-05-20

## Changements UI

- Suppression de l'entree visible `Forex` dans l'univers Forex. Elle etait redondante avec `Dashboard Forex`.
- Ajout d'un onglet `Cube 3 piliers` dans `Dashboard Forex`.
- Enrichissement des explications dans `Three Pillars Monitor` pour la sante pipeline, la vue synthese, AG5, AG6, AG7, AG8 et l'historique.
- Amelioration de l'onglet `Historique Scores` : toutes les devises disponibles sont selectionnees par defaut, fenetre reglable `7j / 30j / 90j / all`, graphe plus haut et zone d'aide d'interpretation.
- Note complementaire : tous les tableaux du dashboard passent par un rendu HTML wrappé (`render_wrapped_dataframe`) afin d'eviter la troncature des textes longs et de laisser les colonnes courtes se compacter automatiquement.

## Cube 3 piliers

Le cube met en perspective le court terme et le long terme:

- Axe X: momentum technique AG2-FX.
- Axe Y: biais news/event AG4-Forex.
- Axe Z: impact structurel des 3 piliers, calcule par paire comme `(composite_score devise base - composite_score devise cotation) / 2`.

Les zones explicites sont:

- `Base favorisee multi-horizon`: technique, news et 3 piliers soutiennent la devise de base.
- `Cotation favorisee multi-horizon`: les trois axes favorisent la devise de cotation.
- `Divergence court terme / long terme`: les signaux tactiques contredisent le score structurel.
- `Zone neutre / information incomplete`: signal faible ou devise non couverte par les 3 piliers.

## Couverture devises

Le monitor 3 piliers reste limite au panier couvert par les agents macro actuels: `USD, EUR, JPY, GBP, CHF, CAD, AUD, NZD`.

Pour etendre proprement le perimetre a d'autres devises actives de l'univers Forex, il faut ajouter les mappings de donnees dans:

- `services/macro-data-api/fred_client.py` pour macro/taux directeurs,
- `services/macro-data-api/cot_client.py` pour COT ou proxy de positionnement,
- `services/macro-data-api/rates_client.py` pour courbes 2Y/10Y,
- `services/macro-data-api/scoring.py` pour elargir l'univers score.

Sans ces donnees, une paire peut rester visible dans le dashboard Forex, mais l'axe structurel du cube est marque incomplet.

## Extension hors G8 et AG1-FX-V2

Ajout de la premiere couche d'extension hors G8:

- `MXN` devient scorables des que macro/policy, valorisation, COT CFTC et courbe 2Y/10Y sont complets.
- `SEK` et `NOK` sont prepares, mais restent `data_incomplete` tant qu'un proxy de positionnement `OPTION_RR_25D` ou `CME_OI` n'est pas charge.
- `KRW` est conserve en macro-only: pas de score structurel tant qu'une source NDF/positionnement fiable n'est pas disponible.
- `cot.speculative_positions` porte maintenant `source` et `confidence`.
- `pillars.currency_scores` porte `data_completeness`, `score_status`, `confidence_floor` et `missing_inputs`.

AG1-FX-V2 consomme maintenant le cube dans le brief:

- `cube_summary.best_convergences` pour les ouvertures potentielles.
- `cube_summary.portfolio_positions_review` pour HOLD/REDUCE/CLOSE sur positions existantes.
- `cube_summary.pullback_reinforcement_candidates` pour les renforcements si Z reste intact.
- `cube_summary.short_term_hype_to_avoid` pour eviter de courir apres X/Y quand Z contredit.
- `cube_summary.missing_structural_data` pour signaler les paires non jugeables structurellement.
