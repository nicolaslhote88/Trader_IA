# Méthodes de scoring du contexte global

## Règles transverses

Tous les scores directionnels sont bornés par `clamp(x,-1,1)`. Le composite
d'une famille est calculé seulement sur les composantes valides et non périmées :

```text
Wdisponible = somme(w_i disponibles)
w'_i = w_i / Wdisponible
score = clamp(somme(w'_i * s_i), -1, 1)
coverage = Wdisponible / somme(w_i configurés)
confidence = moyenne(confidence_i disponibles) * coverage
```

S'il ne reste aucun signal, le score vaut `null`. L'alignement est vrai seulement
avec couverture complète, aucun stale et tous les signes au-delà de ±0,20
identiques. L'âge effectif est `max(âge_stocké, now-observation_time)`.

## AG5

Poids : croissance 0,24; inflation/réponse 0,16; politique 0,18; taux réel 0,12;
balance courante 0,16; fiscal 0,07; emploi 0,07.

- croissance : `0,70*tanh(croissance/3) + 0,30*tanh(momentum/2)` ;
- inflation : stabilité autour de la cible combinée au taux réel ;
- stance : `tanh((policy-neutral)/(2*max(0,5,incertitude)))` ;
- balance courante : `tanh(%PIB/5)` ;
- fiscal : `tanh(%PIB/5)` ;
- emploi : `-tanh(delta_chômage_pp/1,5)`.

Avant : balance en milliards, taux neutres certains, manquants=0, biais USD.
Après : ratios comparables, neutral rates versionnés/incertains, nulls conservés,
aucun prior directionnel.

## AG6

`carry=tanh(carry_nominal/4)`, `real_carry=tanh(carry_réel/4)`. L'écart PPP vaut
`(fair-spot)/spot` seulement lorsque fair et spot existent et sont positifs.
`REER=clamp(gap_pct/25)`. Poids carry 0,15; réel 0,15; PPP 0,30; REER 0,30;
termes de l'échange 0,10.

Avant : inflation différentielle appelée PPP et carry majoritaire. Après : PPP
réellement confrontée au marché; REER/PPP/ToT absents restent null et abaissent
couverture/confiance. Ce score ne valorise aucune entreprise.

## AG7

`positioning_score=clamp(-z_CFTC/2)` : une position longue extrême devient un
risque contrarian négatif, pas un ordre. Le USD synthétique inverse une moyenne
pondérée des z-scores disponibles, publie les contributeurs, est marqué proxy et
a une confiance maximale de 0,60.

Avant : millésime hebdomadaire masqué par le run quotidien, proxy peu visible et
interprétation normative. Après : date CFTC réelle, proxy/confidence explicites.

## AG8

`slope=10Y-2Y`. Une variation ≥10 pb n'est qualifiée de steepening que si les
variations des deux jambes sont présentes : moyenne négative = bull steepening,
sinon bear steepening. Variation ≤-10 pb = flattening; pente négative = inverted.
La baseline J-30 est la plus proche dans une fenêtre 20–45 jours.

Avant : variation approximative et prescription long/short. Après : millésimes
contrôlés, régime descriptif, pression duration bornée, aucune recommandation.

## AG9 et synthèse

AG9 applique decay et agrégation bornée décrits dans
`worldmonitor_integration.md`. La synthèse ne moyenne pas des sens macro,
positionnement, taux, valorisation et géopolitique. Elle les conserve en
dimensions séparées. Seules sa couverture (poids AG5 0,25; AG6 0,10; AG7 0,20;
AG8 0,20; AG9 0,25) et sa confiance de qualité sont agrégées. Cela mesure la
qualité du contexte, pas une conviction d'achat.
