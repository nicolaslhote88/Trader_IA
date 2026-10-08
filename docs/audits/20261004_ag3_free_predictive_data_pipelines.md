# AG3 — Pipelines gratuits pour constituer le jeu prédictif

Recherche et sondes du 4 octobre 2026. Périmètre : identifier et tester les accès ; aucun collecteur permanent, entraînement, workflow, cron ou ordre déployé.

## Décision proposée

Constituer une base de recherche distincte avec quatre entrées : SEC pour les comptes des déclarants EDGAR ; ESEF et publications émetteurs pour l’Europe ; Yahoo pour les prix et opérations sur titres ; BCE et éventuellement ALFRED pour change et contexte macro daté. Les API publiques permettent de commencer sans abonnement de données. La couverture mondiale homogène, les données des sociétés radiées et les révisions historiques du consensus analystes ne sont pas démontrées.

Lancer deux volets : prototype statistique sur les émetteurs SEC suffisamment couverts, et qualification des titres français dès le départ. Ne pas présenter un modèle validé aux États-Unis comme validé sur les petites valeurs françaises. Les frais d’infrastructure et de maintenance ne deviennent pas nuls parce que les données sont gratuites.

## Faits validés sur le système

- Univers lu en `read_only=True` : 364 symboles distincts présents dans `universe_segments` et `universe`, dont 362 EQUITY et 2 ETF. Champs pays : 233 France, 47 United States, 12 Japan, 8 Canada, etc. Ce périmètre n’applique pas les exclusions de quarantaine ou les gates AG1 : ce n’est pas un décompte des titres tradables. Les segments se chevauchent.
- Broker authentifié, compte aligné, aucune approbation en attente. Dernier run AG1 décisionnel parmi les trois derniers runs : `RUN_20261002_171007_22711` ; deux runs PF ultérieurs au 2 octobre. Aucun ordre déclenché.
- Dépôt : `services/yfinance-api/main.py` utilise `Ticker.info` et `fast_info` pour `/fundamentals`. La sérialisation `/history` garde OHLCV et les drapeaux qualité/clôture, pas `Adj Close`, dividendes ou splits. Cette lecture du code local complète les sondes réseau ; aucune empreinte du code live n’a été comparée ici.
- Conteneur `yfinance-api` : version yfinance 1.3.0. Les sondes utilisent directement cette bibliothèque dans un processus séparé.

| Sonde depuis le VPS | Résultat observé |
|---|---|
| SEC Apple submissions | HTTP 200 ; 1 001 dépôts récents, un fichier historique supplémentaire référencé ; `acceptanceDateTime` présent |
| SEC Apple companyfacts | HTTP 200 ; 25 135 faits, taxonomies `dei` et `us-gaap` ; première période rencontrée 2006-09-30 |
| SEC ASML companyfacts | 11 381 faits ; `dei` et `us-gaap` ; formulaires 20-F et 20-F/A ; première période 2006-12-31 |
| SEC fichier global company_tickers | HTTP 403 : le mapping global reste un problème distinct ; ce blocage ne touche pas les deux endpoints Apple testés |
| Catalogue XBRL France | HTTP 200 ; accès aux métadonnées et liens JSON/ZIP |
| Dassault Systèmes ESEF | LEI `96950065LBWY0APQIM86` ; six rapports 2020–2025 ; JSON 2025 téléchargé, 537 faits incluant des blocs textuels |
| BCE EUR/USD | HTTP 200 ; quatre observations du 2 au 5 janvier 2024 |

Les premières périodes SEC incluent des comparatifs rapportés ultérieurement : elles ne prouvent pas que ces chiffres étaient accessibles en 2006, ni que toutes les métriques sont complètes depuis cette date. Les nombres de faits incluent répétitions, versions et contextes ; ce ne sont pas autant d’observations indépendantes d’apprentissage.

| Prix journaliers demandés depuis 2010 | Lignes | Dividendes non nuls | Splits non nuls |
|---|---:|---:|---:|
| AAPL | 4 213 | 57 | 2 |
| DSY.PA | 4 287 | 17 | 2 |
| ASML, cotation américaine | 4 213 | 34 | 0 |
| 9983.T | 4 116 | 34 | 1 |

Les quatre séries vont du 4 janvier 2010 au 2 octobre 2026 et ont `Adj Close` renseigné sur toutes les lignes retournées. C’est une preuve d’accès et de présence des champs, pas une certification des ajustements, des jours manquants ou des rendements.

## Pipe 1 — SEC / EDGAR : comptes historiques et dates des dépôts

Accès public sans clé. Endpoints :

```text
https://data.sec.gov/submissions/CIK0000320193.json
https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json
https://data.sec.gov/api/xbrl/companyfacts/CIK0000937966.json
```

Pipeline proposé : registre instrument/émetteur → CIK → submissions et fichiers historiques référencés → companyfacts → conservation de chaque accession/version → normalisation des concepts → disponibilité temporelle → features.

Données visées : chiffre d’affaires, résultat, cash-flow opérationnel, investissements, actifs, capitaux propres, dette, trésorerie, actions et EPS. Vérifier tags, unités et périmètres avant de dériver ratios et flux trimestriels ; un flux neuf mois n’est pas un trimestre. Recalculer les multiples avec les prix et nombres d’actions cohérents à la date considérée.

Conserver `accn`, `filed`, `start/end`, `form`, `fy/fp`, unité et taxonomie. Joindre `accn` à `accessionNumber` pour récupérer `acceptanceDateTime`. Utiliser une disponibilité conservatrice après acceptation/diffusion et traitement, à la séance admissible suivante. Si un communiqué antérieur est collecté et daté, il peut devenir une source distincte ; ne pas antidater automatiquement le dépôt au jour de clôture comptable.

Companyfacts ne retient que certains concepts standard et contextes consolidés. Les extensions propres aux émetteurs et données segmentées exigent parfois le dépôt XBRL d’origine. `frames` sélectionne des faits récemment déposés : ce n’est pas une table historique directement utilisable sans risque de fuite du futur. [Documentation SEC](https://www.sec.gov/search-filings/edgar-application-programming-interfaces).

Le fichier global de tickers est bloqué sur ce VPS. Pour le prototype : registre CIK limité et vérifié, puis contrôle des tickers/noms via submissions. Pour l’industrialisation : résoudre l’accès déclaré ou acquérir le fichier via un canal permis ; aucun contournement ni proxy rotatif. Mettre un User-Agent explicite avec vrai contact, cache, reprise incrémentale et débit proposé de 1 requête/s, sous le plafond SEC de 10/s. Les archives bulk sont documentées mais non testées ici. [Règles SEC](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data).

## Pipe 2 — ESEF / XBRL et publications des émetteurs : Europe

API gratuite sans clé dans les sondes :

```text
https://filings.xbrl.org/api/filings?filter[country]=FR&page[size]=200&include=entity
https://filings.xbrl.org/api/entities?filter[name]=DASSAULT%20SYSTEMES
https://filings.xbrl.org/api/entities/96950065LBWY0APQIM86/filings
```

Pipeline : instrument → LEI vérifié → dépôts → URL `json_url` ou `package_url` → concepts IFRS/contextes → normalisation des métriques. Les URL de fichier sont relatives au domaine. Pour les packages non convertis, Arelle constitue une piste de parsing : c’est également le processeur employé par le catalogue. [API](https://filings.xbrl.org/docs/api), [contenu et limites](https://filings.xbrl.org/docs/about).

ESEF fournit surtout des rapports annuels ; compléter avec les résultats semestriels/trimestriels datés des sites investisseurs. Le dispositif ESEF concerne les rapports financiers annuels ; sa présence ne garantit ni trimestriels structurés ni couverture de toutes les petites capitalisations. [ESMA](https://www.esma.europa.eu/issuer-disclosure/electronic-reporting), [AMF](https://www.amf-france.org/fr/actualites-publications/dossiers-thematiques/esef/esef-vos-questions-frequentes).

**Blocage à lever pour le backtest :** `period_end` est la fin de période, `date_added` l’entrée au catalogue et `processed` sa transformation. Aucune ne prouve la première publication au marché. Récupérer la date depuis le dépôt officiel ou la publication émetteur ; sinon garder une disponibilité conservatrice explicitement qualifiée, ou exclure du jeu strict. Ne pas déduire l’ordre des révisions de `/0/`, `/1/` : le catalogue dit ne pas garantir cet ordre. Il est incomplet et signale notamment des difficultés de collecte en Allemagne et en Irlande. [Limites déclarées](https://filings.xbrl.org/docs/about).

## Pipe 3 — Yahoo / yfinance : prix, dividendes, splits et benchmarks

Appel testé :

```python
yf.Ticker(symbol).history(
    start="2010-01-01", end="2026-10-04", interval="1d",
    auto_adjust=False, actions=True, repair=False, timeout=25,
)
```

Créer un collecteur de recherche distinct du contrat OHLCV AG2. Conserver `Close`, `Adj Close`, OHLCV, dividendes, splits, devise de cotation, fuseau, date de collecte et version du fournisseur. `auto_adjust=False` évite l’ajustement supplémentaire automatique de la bibliothèque ; cela ne garantit pas un cours historique brut avant tous splits. Contrôler la convention Yahoo avant tout rapprochement avec EPS et actions historiques. [API yfinance](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html).

Construire les labels de rendement total avec `Adj Close` après contrôle, sans rajouter une seconde fois les dividendes. Pour une prévision du cours hors dividendes, définir un autre label cohérent avec les splits. Les benchmarks doivent suivre la même convention de rendement, la même devise et les mêmes dates. Des ETF sectoriels peuvent servir de proxies, avec leur historique disponible, leurs frais et leur date de création ; ne pas injecter des classifications sectorielles actuelles dans le passé sans vérification.

Archiver les téléchargements avant correction. Qualifier les ruptures, valeurs aberrantes, absences et changements de devise ; une passe `repair=True` est un candidat séparé, jamais une correction silencieuse. La documentation signale erreurs d’ajustements et faux positifs possibles. [Réparation des prix](https://ranaroussi.github.io/yfinance/advanced/price_repair.html).

Accès sans abonnement, mais outil non officiel et sans garantie de service ; le projet rappelle l’usage personnel des API Yahoo et renvoie à leurs conditions. Ce n’est pas une licence de redistribution commerciale des données. [Projet yfinance](https://github.com/ranaroussi/yfinance).

## Pipe 4 — BCE / ALFRED : devises et macro disponible à la date

BCE testée sans clé : `https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?startPeriod=2024-01-01&endPeriod=2024-01-05&format=csvdata`. La série exprime des USD par EUR ; convertir dans le bon sens. Pour un signal avant publication du taux du jour, utiliser le dernier taux effectivement disponible. La conversion sert aussi à comparer titre et benchmark en EUR. [API BCE](https://data.ecb.europa.eu/help/getting-data-web-services-sdmx-0).

ALFRED/FRED : API documentée, non testée avec une clé dans cette recherche. Une clé est requise. Utiliser `fred/series/observations` avec `realtime_start/realtime_end` ou `vintage_dates`, pour récupérer les versions historiques disponibles ; le défaut renvoie l’état connu aujourd’hui. Complément utile pour taux, inflation ou activité, mais non requis pour amorcer le premier jeu de comptes/prix. [Versions historiques](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html), [paramètres](https://fred.stlouisfed.org/docs/api/fred/series_observations.html), [clé](https://fred.stlouisfed.org/docs/api/api_key.html).

## Extensions et solutions écartées du socle

- Japon, dont Fast Retailing : EDINET permet l’accès aux dépôts ; API V2 avec inscription et clé. Piste documentée, pas de clé créée ni téléchargement testé ici. Profondeur de rétention, formats et couverture à mesurer avant de promettre dix ans. [Portail officiel](https://disclosure2.edinet-fsa.go.jp/WEEK0020.aspx?lgKbn=1), [guides](https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/WEEK0060.html).
- Corée, dont Samsung : OpenDART fournit rapports, comptes et XBRL ; service gratuit en principe, clé à obtenir. Documentation vérifiée, accès authentifié non testé. [Introduction](https://engopendart.fss.or.kr/intro/main.do), [conditions et clé](https://engopendart.fss.or.kr/uss/umt/EgovMberInsertView.do).
- Autres places : SEC si l’émetteur dépose effectivement, sinon régulateur local/site investisseurs. Un ticker OTC/ADR ne prouve pas à lui seul l’existence de comptes XBRL SEC. Résoudre aussi ratio ADR, classe d’actions et devise.
- Alpha Vantage : offre habituelle 25 appels/jour, exemptions pour projets vérifiés ; `TIME_SERIES_DAILY_ADJUSTED` est premium. Pas le socle gratuit retenu pour plusieurs centaines de titres. [Quota](https://www.alphavantage.co/support/), [endpoint](https://www.alphavantage.co/documentation/#dailyadj).
- Consensus analystes : aucune source gratuite exhaustive avec toutes les versions historiques n’a été validée. Conserver les snapshots futurs, mais exclure les objectifs actuels des features historiques. Les comptes Yahoo courants ne remplacent pas les versions comptables connues à chaque date.

## Architecture proposée — non implémentée

```text
Registre instruments/émetteurs et identités historiques
    → Collecteurs SEC / ESEF / Yahoo / BCE / ALFRED
    → Archives sources immuables + empreintes + dates de collecte
    → Tables normalisées avec périodes ET dates de disponibilité
    → Jointures temporelles à chaque date de décision
    → Features + labels futurs + contrôles de couverture
    → Entraînement/calibration chronologiques → suivi en shadow
    → Publication éventuelle d’un AG3_PREDICTIVE_V1 consultatif
```

Tables proposées dans un stockage de recherche séparé :

- `issuer_instrument_map` : symboles, ISIN, LEI, CIK, place, devise, ratios ADR, validité temporelle ; distinguer société et titre.
- `filings` : identifiant, période, publication/dépôt, acceptation, `available_at`, `retrieved_at`, précision de date, amendement, source et empreinte.
- `financial_facts` : concept source, métrique normalisée, début/fin, valeur/unité, contexte consolidé/segment, accession et version. Préserver NULL.
- `prices_daily`, `corporate_actions`, `fx_daily`, `macro_vintages` : données et conventions sourcées.
- `features_asof`, `labels`, `model_runs`, `predictions` : date de décision, horizon, jeu/version, benchmark, couverture, performances et calibration.

Ne pas écraser AG3 live. Stocker les archives en fichiers et les tables de recherche hors des DuckDB de production ; pas de gros backfill ni CHECKPOINT dans les nodes n8n. Planifier les collectes incrémentales après lecture de `SCHEDULING_AND_LOAD.md`, sans définir de nouveau cron à ce stade.

## Hypothèses et actions restantes

1. Mesurer la couverture sur les 362 actions segmentées, après application séparée de la quarantaine : mapping, comptes, dates fiables, prix, opérations sur titres et nombre de périodes. Les deux ETF nécessitent un traitement distinct.
2. Prototype SEC + prix, et échantillon français ESEF incluant grandes et petites valeurs. Archiver les données d’origine et vérifier manuellement quelques comptes, amendements, splits et dividendes. L’accès technique testé ici ne valide pas encore la normalisation financière.
3. Objectif initial cohérent avec le plan AG3 : surperformance en rendement total à 90 jours calendaires contre benchmark défini à l’avance, valorisée à la première séance admissible à l’horizon. Ajouter éventuellement une distribution du rendement du titre et un événement de baisse ; ne pas confondre prix et rendement total.
4. Reconstruire les informations disponibles à chaque date. Séparer chronologiquement entraînement, calibration et test pour tous les titres ; purger les labels qui chevauchent les fenêtres. Comparer fréquence historique/logistique à un modèle tabulaire. Le gain reste une hypothèse.
5. Traiter le biais de survivance : les 364 titres actuels ne reconstituent pas l’univers passé. Les cours et rendements de radiation ne sont pas garantis par Yahoo. Sans résolution, qualifier le résultat comme exploratoire et ne pas revendiquer un backtest exhaustif.
6. Valider calibration, robustesse par marché/secteur et couverture avant tout usage AG1. L’intervalle de prix futur resterait conditionnel au modèle, pas une garantie ni un objectif analyste.

## Preuves et état local

Sondes et sorties : `.codex-tmp/ag3-sources-20261004/` (`probe.py`, `runtime.py`, `prices.py`, `coverage.py`, `details.py`, sorties JSONL et `details_summary.json`). Aucune clé d’API ajoutée. Les accès externes sont des GET publics et les bases sont lues en lecture seule. La bibliothèque Yahoo peut gérer son cache technique habituel.

Note ajoutée seulement ; aucune modification des sources applicatives, bases métier, workflows ou gardes broker. Pas de commit, push ou PR dans cette recherche. Les sondes sont ponctuelles, pas des pipelines déjà opérationnels.
