# AG3 : historique, probabilités et information transmise à AG1

Audit du 3 octobre 2026. Diagnostic en lecture seule de la production ; aucun
workflow publié, aucun score ni ordre modifié. Les changements ci-dessous sont
des propositions, les replays sont des expériences isolées.

## 1. Faits validés sur le VPS

- Dashboard monté depuis `/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard`.
  `app.py` identique au dépôt, SHA256 `E1E2B7CA9D53EF2B414EA81F535911CFD248411B12082E0CBB99115F40F99868`.
- AG3 Held+Core actif et publié : `9afbaccc-9b88-4b5b-a6e6-d4f6971476f5` ;
  Watchlist : `b9830b5a-99d7-4985-8a2f-3eef124a7abb`.
  Les deux codes de scoring publiés sont identiques.
- Exécutions AG3 récentes réussies : Held+Core `22716`, Watchlist `22720`.
- AG1 actif et publié : `447203d7-c110-473e-a5cc-1500107d20e5` ;
  exécution `22711` réussie le 2 octobre ; run métier
  `RUN_20261002_171007_22711` de 15:10:07 à 15:14:18 UTC.
- Broker authentifié, compte aligné, aucune approbation en attente à l'inspection.

## 2. Les courbes sont peu variables, pas figées

Le dashboard lit les lignes historiques réelles de `fundamentals_triage_history`.
Il ne répète pas la dernière valeur sur chaque date. Sur les 30 jours inspectés :

| Symbole | Relevés | Triage min–max | Risque min–max |
|---|---:|---:|---:|
| GOOGL | 30 | 76–78 | 13–14 |
| AAPL | 30 | 61–62 | 40–41 |
| MSFT | 30 | 74–76 | 29–29 |
| NVDA | 30 | 86–87 | 14–14 |
| ASML | 30 | 70–73 | 32–34 |
| DSY.PA | 30 | 65–69 | 23–26 |
| MC.PA | 26 | 56–58 | 38–39 |

Sur 364 symboles ayant au moins cinq relevés, 59 ont les deux scores constants,
190 ont une amplitude au plus égale à deux points pour chacun des deux scores.
Ce dénominateur inclut l'historique disponible, pas uniquement les actifs tradables.

GOOGL : qualité 98, croissance 56, santé 75 restent constants ; valorisation
varie de 57 à 64 et consensus de 88 à 93. Le cours varie de 330,65 à 354,97 USD.
Les agrégations, seuils saturants et arrondis à l'entier masquent les petites
variations. Le score de risque partage plusieurs composants du score de triage.

Interprétation : des informations comptables lentes et des scores compressés
expliquent cette stabilité. Elle ne prouve ni l'absence de risque de marché,
ni une stabilité des bénéfices sur plusieurs trimestres.

Les 960 métriques GOOGL du mois ont `period=NULL`. `as_of_date` est la date du
relevé, pas une période comptable. L'API assemble des instantanés `Ticker.info` /
`fast_info`, sans séries de comptes trimestriels dans ce chemin. Un relevé récent
ne prouve pas que les comptes sous-jacents ont été publiés récemment.

## 3. Les probabilités ne sont pas estimées statistiquement

`services/dashboard/app.py::_estimate_scenario_probabilities` mélange score,
risque et upside avec des coefficients fixes, puis normalise à 100 %.
Aucun apprentissage, événement probabilisé explicite, ni calibration hors
échantillon dans cette fonction. Ces pourcentages restent propres au dashboard.

Les cibles Bear/Base/Bull viennent principalement des objectifs bas/moyen/haut
des analystes. Ce sont des opinions dispersées, pas des quantiles prédictifs.
Les droites jusqu'à +365 jours ne sont pas des trajectoires prédites.
Le minimum des objectifs analystes ne borne pas les pertes possibles.

## 4. Deux défauts de calcul à traiter avant un modèle

**Valeurs absentes.** Le code publié fait `Number(null)`, soit zéro. Un P/E absent
peut ainsi devenir très favorable, une rentabilité absente défavorable. Le code
de pondération sait ignorer `null`, mais cette conversion l'en empêche.
378 des 527 derniers snapshots archivés contiennent au moins une valeur nulle
dans les blocs financiers inspectés ; ce chiffre n'est pas une mesure des seuls
symboles actuellement actifs, ni du nombre de scores effectivement changés.

Replay du snapshot ALCBX.PA du 1er octobre : préserver les valeurs absentes fait
passer le triage de 34 à 41 et la valorisation de 61 à 41. C'est une preuve
d'impact du traitement des absences, pas une validation économique du nouveau
score. Les sept grandes valeurs échantillonnées n'avaient pas de null dans ces
blocs et restent inchangées sous ce seul changement.

**Unités des pourcentages.** `pct()` multiplie par 100 seulement si la valeur
absolue est ≤1,5. Ainsi 1,50 devient 150 %, puis 1,51 devient 1,51 % :
discontinuité incompatible avec une unité de source constante.
GOOGL porte `earningsGrowth=2.94` et `earningsQuarterlyGrowth=2.979` dans le
snapshot brut. Avec un contrat de ratios décimaux appliqué uniformément,
le replay passe croissance 56→100 et triage 78→87, risque inchangé à 13.
Il faut formaliser les unités par champ et éprouver les valeurs extrêmes ;
ce replay ne justifie pas à lui seul un relèvement de l'appréciation du titre.

## 5. Ce qu'AG1 reçoit réellement

R8 publié lit score, risque, upside, recommandation, objectif moyen, horizon
et date depuis `v_latest_triage`. Il ne lit ni les cinq sous-scores ni les
métriques brutes, périodes comptables, révisions ou explications AG3.
Le fondamental pèse 34 % du `prob_score` matriciel ; ce score est lui-même
heuristique. Le pack porte déjà `score_calibrated=false` et
`score_semantics=heuristic_not_probability`.

Le `opportunity_pack.rows` construit par Calcul Matrice compresse encore ces
informations en risque/reward/score/TP et motifs : pas de fiche fondamentale
structurée. L'inspection des paramètres des autres nœuds publiés n'a pas révélé
d'autre lecture de `fundamentals_snapshot` ou `fundamental_metrics_history`.
Conclusion : la donnée influence la sélection, mais le PM dispose de peu de
matière pour comprendre ou contester cette influence.

## 6. Proposition de mise en œuvre

1. **Fiabiliser et clarifier.** Préserver les absences, fixer les unités,
   documenter couverture et dates ; remplacer les pourcentages non calibrés par
   des objectifs analystes explicitement nommés. Replay des deux workflows AG3
   et de la chaîne AG1/dashboard avant toute publication, avec comparaison des
   classements, gates et résultats ; conserver backups et rollback.
2. **Rendre le fondamental lisible et utile au PM.** Afficher qualité, croissance,
   valorisation, bilan et consensus séparément ; montrer les variations 30/90 j,
   révisions d'objectifs et changements des métriques. Ajouter ensuite les
   véritables séries comptables, dates de publication et comparaisons sectorielles.
   Distinguer risque de bilan et risque de baisse du cours. Produire une fiche
   commune au dashboard et à AG1 : chiffres sourcés, forces, fragilités,
   changements, couverture, fraîcheur et incertitudes. Un LLM peut rédiger la
   synthèse à partir de ces données ; ses hypothèses restent identifiées.
3. **Évaluer un modèle prédictif séparé.** Définir l'événement et l'horizon, par
   exemple surperformance à trois mois face à un indice approprié, ou perte
   supérieure à un seuil explicite. Comparer une baseline simple à un modèle
   tabulaire, puis calibrer sur des dates disjointes. Séparer cela des objectifs
   analystes à douze mois et de l'horizon tactique des ordres AG1.

Le registre AG3 contient 21 709 lignes, 527 symboles, du 16 février au 3 octobre
2026 : moins de huit mois, donc aucun résultat à douze mois entièrement observé
pour ces relevés. Les répétitions journalières ne sont pas autant d'observations
indépendantes. Un modèle à douze mois exige un historique antérieur fiable avec
les données réellement disponibles à chaque date. Ne pas reconstruire le passé
avec les seules valeurs actuelles ou les comptes révisés après la décision.

Validation proposée : découpage chronologique par date pour tous les titres,
purge des labels qui chevauchent les fenêtres, calibration séparée, comparaison
Brier/log-loss et courbes de fiabilité, robustesse par secteur et période,
coûts de transaction si usage tactique. Puis suivi en shadow avec version,
source et horizon visibles. Le gain prédictif reste une hypothèse à démontrer.

Références méthodologiques consultées :
[calibration](https://scikit-learn.org/stable/modules/calibration.html),
[validation temporelle](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html).
Les [API SEC](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
sont une piste de comptes et dépôts pour les émetteurs concernés, pas une
solution universelle ; l'accès depuis ce VPS n'a pas été retesté dans cet audit.

## Preuves locales et limites

Preuves et scripts de lecture/replay : `.codex-tmp/ag3-audit-20261003/` :
`published/`, `dashboard_live.py`, `evidence.jsonl`, `null_evidence.jsonl`,
`replay_null_affected.jsonl`, `replay_ratios.jsonl`, `stats.py`, `null_probe.py`.
Les DuckDB ont été ouverts avec `read_only=True` et SQLite en `mode=ro`.
Les replays évaluent seulement le nœud de score sur des snapshots, pas la chaîne
complète de décision ni le sandbox des runners. Aucun correctif de production
n'est donc déclaré validé ou déployé. Aucun entraînement n'a été effectué.
