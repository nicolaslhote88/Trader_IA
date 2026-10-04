# AG1 — fiche comptable historique et comparaison isolée

Intervention du 4 octobre 2026, autorisée pour enrichir les faits destinés à AG1 puis
mesurer leur apport en simulation avant toute intégration aux décisions.

## Résultat et décision

La fiche est construite, accessible dans le dashboard et consommée par les trois
modèles AG1 en replay isolé. **Aucun avantage décisionnel stable n'est démontré
dans cette expérience ; elle reste hors du workflow AG1 réel.** Aucun ordre envoyé.

| Variante | Achat hypothétique AZN | Achat hypothétique NRO.PA |
|---|---:|---:|
| Référence A1 | 8 titres | absent |
| Historique B1 | 8 titres | 28 titres |
| Historique B2 | 8 titres | absent |
| Référence A2 | 8 titres | 28 titres |

Ce tableau résulte du consensus **puis des règles safety publiées**, exécutés hors
réseau. Il ne décrit ni des ordres transmis au broker ni des fills simulés. Les
petites différences de poids AZN n'ont pas changé la quantité après arrondi.
NRO.PA varie une fois sur deux dans chaque variante ; il n'a lui-même pas
d'historique daté qualifié. On ne peut attribuer son apparition au nouvel apport.

Les justifications enrichies utilisent effectivement certains faits nouveaux :
GPT et Claude mentionnent l'amélioration des marges annuelles AZN ; GPT mentionne
l'amélioration du FCF approché TXN. Contrôle des données : marge nette AZN
2024→2025 **+4,400 points**, marge opérationnelle **+4,898 points** ; FCF approché
TXN **1 498→2 603 millions USD**. Cela prouve l'utilisation de ces faits dans
ces réponses, pas une amélioration de performance ni une validation de la totalité
de leur raisonnement.

## Protocole effectivement exécuté

- Export de la version publiée AG1 `87de669d-ae38-4c50-9247-492bbf11f0a3`.
- Contexte préflight capturé du run `RUN_20261002_171007_22711`, exécution `22711`,
  référence temporelle des nouveaux comptes **2 octobre 2026 à 17:10:07 Paris**.
- **18 titres, 11 couverts** : AMZN, AVGO, AZN, BRK-B, GOOGL, JPM, KO, LLY, TXN,
  UNH, UNP. Non couverts : 0700.HK, ELIS.PA, MAU.PA, NRO.PA, PDD, PRX.AS, TKO.PA.
- Ordre A1/B1/B2/A2, trois modèles dans chaque variante : `gpt-6-sol`,
  `deepseek-v4-pro`, `claude-opus-5-5`. **12 sorties valides**, parseurs réels.
- Prompts, transports et paramètres des modèles publiés conservés. Aucun ancien
  résultat LLM substitué à l'un des passages. Horloge consensus/safety figée.
- Vérification structurelle : retirer les seuls champs historiques ajoutés
  restitue exactement l'entrée de référence. Prix, portefeuille, news, gates,
  scores et contraintes d'exécution ne changent pas entre les deux entrées.
- Les différences HOLD/WATCH/absence de proposition sont séparées des véritables
  changements d'intention OPEN/INCREASE/DECREASE/CLOSE, pour ne pas confondre
  verbosité et décisions de trading.

Variabilité du consensus : une intention change entre A1/A2, une entre B1/B2,
une entre A1/B1 et une entre A2/B2. La différence observée entre variantes ne
dépasse donc pas la variabilité observée sans modification des données.

## Limites de l'interprétation

Un contexte et deux répétitions par variante ne constituent pas un test statistique
de performance. Aucun gain de rendement, drawdown, frais ou turnover futur n'est
établi. Les cours postérieurs nécessaires aux horizons de décision ne sont pas
encore disponibles pour ce contexte. Aucun fill hypothétique n'est présumé.

Le contexte capturé le 2 octobre **précède la fiche AG3_EVIDENCE_V1 déployée le
3 octobre** : celle-ci n'a pas été ajoutée rétroactivement avec des snapshots plus
récents. Le test utilise les branches LLM actuellement publiées avec cette entrée
historique figée ; il ne remplace pas une comparaison prospective du prochain
contexte complet. Les connaissances préentraînées des LLM peuvent aussi dépasser
la date rejouée : ce n'est pas un backtest financier point-in-time.

La disponibilité historique des comptes est reconstruite depuis les dépôts ;
les archives de collecte datent du 4 octobre. Les versions futures sont filtrées,
mais les limites du jeu courant, de sa couverture et de ses fournisseurs subsistent.

## Contenu et disponibilité de la fiche

Contrat `AG3_HISTORICAL_EVIDENCE_V1`, module
`services/ag3-predictive/historical_evidence.py`. Trois exercices comparables au
maximum, même source/devise par fiche, marges, cash-flow opérationnel, FCF approché,
capitaux propres/actifs, passifs/actifs, trésorerie/actifs ; différences annuelles
en points et en millions de devise comptable. Les passifs ne sont pas assimilés
à la dette financière. Les ratios financiers/bancaires portent une réserve sectorielle.

Horodatages SEC qualifiés ou publication ESEF prouvée exigés. Aucune publication
ESEF actuellement inconnue n'est utilisée. Les absences et comparatifs non disponibles
restent NULL. Référence du dépôt, concept, date de disponibilité et SHA256 source
sont conservés par métrique dans la fiche détaillée. Comptes de plus de 550 jours
signalés comme anciens ; aucun nouveau score ou gate n'est créé.

Route interne : `/evidence/{symbol}?as_of=<ISO avec fuseau>` ; sans paramètre,
informations connues à l'heure actuelle. Dates sans fuseau/futures rejetées.
Dans le dashboard : **Fiche historique destinée à AG1 — simulation uniquement**.
Aucun nœud AG1 live ne consomme cette route ou le nouveau bloc.

## Coût de contexte observé

| Modèle | Tokens d'entrée A | Tokens d'entrée B | Nature de la mesure |
|---|---:|---:|---|
| GPT-6 Sol | 35 429 | 46 571 | estimation n8n, pas facture API complète |
| DeepSeek V4 Pro | 39 373 | 51 556 | usage retourné |
| Claude Opus 5.5 | 62 613 | 78 831 | usage retourné |

Le supplément de contexte doit aussi être justifié avant promotion. Aucun coût
monétaire exact n'est déduit de ces seules valeurs. Chaque replay complet a pris
environ 162 à 182 secondes. Les appels de simulation sont ponctuels ; aucun cron
LLM supplémentaire installé.

## Tests, état live et preuves

- 8 tests nouveaux : disponibilité/révisions, unités/ratios, absences, années
  manquantes, entrée inchangée, graphe sans exécution, refus d'une URL inattendue.
- 10 tests prédictifs existants réussis. API testée sur snapshot SQLite isolé,
  comptes non datés exclus et dates invalides rejetées.
- AppTest du dashboard déployé : aucune exception, 13 métriques, 2 graphiques,
  9 tableaux. Fiche GOOGL trois périodes ; DSY.PA absence datée explicite.
- Sources déployées identiques aux fichiers locaux ; services sains, broker
  authentifié/aligné, aucune approbation en attente. Trois derniers runs métier
  inchangés. AG1 toujours actif sur la même version publiée.
- Instance n8n temporaire isolée du réseau `web`, aucun volume DuckDB métier,
  uniquement branches LLM et déclencheur manuel. Conteneurs temporaires et profil
  contenant les copies de credentials supprimés après les replays.

Résultats et empreintes : `20261004_ag1_historical_evidence_results.json`.
Archives complètes : `.codex-tmp/ag1-history-20261004/` localement et
`/local-files/.codex-tmp/ag1-history-20261004/` sur le VPS. Les résultats bruts
ne sont pas une nouvelle entrée pour les décisions live.

## Reproduction et suite avant promotion

Les trois outils `outils/scripts/{prepare_ag1_history_replay.py,
build_ag1_history_shadow.py,compare_ag1_history_replays.cjs}` produisent les entrées,
le graphe isolé et la comparaison. Réutiliser un nouvel export **publié**, un contexte
capturé et les credentials existants uniquement dans une instance n8n jetable.
Ne jamais importer le graphe de simulation dans l'instance de trading.

Pour envisager une intégration : élargir à plusieurs contextes complets capturés
après le 3 octobre, répéter les variantes avec budget plafonné, vérifier les faits
cités et les gates, puis observer prospectivement les rendements/risques après
frais avec des hypothèses de fill explicites. Fixer les critères et horizons avant
de regarder les résultats ; ne pas sélectionner seulement les exemples favorables.
Cette campagne prospective n'est pas automatiquement activée par cette intervention.

## Déploiement et rollback

Seuls l'API de recherche et son module dashboard ont été déployés.
Image recherche : `trader-ag3-predictive:20261004-history`. Sources :
`/opt/trader-ia/services/ag3-predictive` et montage dashboard vérifié
`/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard`.

Sauvegardes : `/local-files/.codex-tmp/ag1-history-20261004/backup/`.
Pour revenir en arrière, restaurer `compose.yml` et `api.py` dans le service de
recherche, puis `docker compose -f /opt/trader-ia/services/ag3-predictive/compose.yml
up -d --no-deps ag3-predictive` (image précédente conservée). Restaurer
`predictive_detail.py` dans le montage dashboard et redémarrer uniquement
`root-trading-dashboard-1`. Attendre la fin d'une collecte avant recréation.
Les données de recherche, crons, workflows n8n et bases métier ne sont pas restaurés.
