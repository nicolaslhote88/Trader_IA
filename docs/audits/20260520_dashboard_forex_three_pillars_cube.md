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
