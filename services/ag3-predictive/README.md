# AG3 — collecte historique et prédictif en shadow

Service de recherche séparé, sans endpoint d’ordre et sans chemin de décision AG1.
Le Yahoo existant fournit les prix via `/research/history` ; aucun second collecteur
Yahoo n’est installé dans cette image. `/history`, `/fundamentals`, le scoring et les
gates du système de trading conservent leurs contrats.

## Données et sources

- SEC : `submissions` (fichiers anciens inclus), `companyfacts`, identité ticker/CIK
  vérifiée avant ingestion. Les CIK de `identity_seeds.json` ne sont que des candidats.
  `company_tickers.json` peut retourner 403 : la couverture le signale.
- Europe : catalogue ESEF paginé, correspondance exacte et unique des noms normalisés,
  GLEIF/ISIN avec contrôle du nom ; aucune correspondance floue automatique.
  Les dates d’ajout au catalogue ne deviennent jamais des dates de publication.
- Prix : historique journalier depuis 2010, puis chevauchement de 14 jours,
  rechargement complet mensuel ou après changement des cours ajustés dans le chevauchement.
  OHLC et événements d’origine restent dans les archives ; table de travail Close,
  Adj Close, volume, dividendes, splits et devise. Prix courant et rendement total distincts.
- BCE : USD par EUR depuis 2010. ALFRED : FEDFUNDS, CPIAUCSL et UNRATE avec vintages.
  Ces séries sont collectées ; elles ne sont pas encore des features du modèle initial.
- EDINET/OpenDART : adaptateurs optionnels, bloqués explicitement sans clé.
  EDINET archive actuellement les listes de dépôts des sept derniers jours.
  OpenDART archive les comptes annuels consolidés depuis 2015 des codes boursiers
  coréens identifiés. Leur normalisation historique par version n’est pas validée :
  ces adaptateurs n’alimentent pas le modèle strict, même après ajout d’une clé.

Références : [SEC](https://www.sec.gov/search-filings/edgar-application-programming-interfaces),
[ESEF](https://filings.xbrl.org/docs/api), [limites ESEF](https://filings.xbrl.org/docs/about),
[GLEIF](https://www.gleif.org/en/lei-data/gleif-api/),
[yfinance](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html),
[ALFRED](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html).

## Stockage et exploitation

- Image : `trader-ag3-predictive:20261004`, réseau Docker `web`, API interne 8084.
- Code VPS : `/opt/trader-ia/services/ag3-predictive`.
- Données : `/local-files/ag3-predictive` ; SQLite WAL dédié, archives gzip adressées
  par SHA256, journal des accès et des statuts. Aucun write dans une DuckDB live.
- Lecture brève de `/source/ag2_v3.duckdb`, `read_only=True`, un thread et retries.
- Le ledger d’ordres et les workflows n8n ne sont pas appelés par ce service.
- `collector.lock` : `flock` non bloquant ; aucune seconde collecte concurrente.
- Limites Docker : 1 CPU et 1 200 Mo. Collectes HTTP séquentielles, reprises limitées,
  pas de contournement de 403, URL des secrets non inscrites dans le journal des requêtes.
- `raw/` conserve la preuve source. Sauvegarder le répertoire ; ne pas supprimer des
  archives référencées par un jeu d’entraînement. Surveiller espace et croissance.
- Les lectures `/status` et `/research/{symbol}` utilisent SQLite en lecture seule.
  `/health` prouve la disponibilité API/base, pas le succès d’une collecte complète.

```bash
cd /opt/trader-ia/services/ag3-predictive
docker compose up -d --build
docker exec ag3-predictive python runner.py
docker exec ag3-predictive python runner.py --symbols AAPL,MSFT,DSY.PA
docker exec ag3-predictive python runner.py --train-only
docker exec ag3-predictive python -c 'from store import status; import json; print(json.dumps(status()))'
```

Planification : wrapper `run_scheduled.sh` à :45 de chaque heure UTC ; le runner
n’agit qu’à **04:45 Europe/Paris**, tous les jours. Le verrou empêche les chevauchements.
Un lancement manuel pendant une collecte retourne `COLLECTOR_ALREADY_RUNNING`.

## Configuration privée

`.env` sur le VPS, mode 600, hors dépôt et hors contexte Docker :

```dotenv
FRED_API_KEY=<clé du service macro existant>
SEC_USER_AGENT=TraderIA/1.0 <contact réel de l’exploitant si renseigné>
# EDINET_API_KEY=<clé obtenue personnellement>
# DART_API_KEY=<clé obtenue personnellement>
```

Ne jamais sourcer ce fichier comme un script shell. Après changement, recréer le
conteneur hors collecte en cours. La première clé FRED est réutilisée localement
depuis la configuration du service macro, sans être affichée.

Pour qualifier une publication européenne, ajouter dans le volume de données
`verified_publications.json`, avec le `fxo_id` exact comme clé :

```json
{
  "identifiant-du-rapport": {
    "available_at": "2026-04-08T07:00:00+00:00",
    "source_url": "https://site-officiel.example/publication-datee",
    "raw_hash": "empreinte-SHA256-exacte-du-JSON-source"
  }
}
```

Cet exemple est un format, pas une date réelle. Exiger une preuve du dépôt/rapport
exact et de sa version (empreinte identique, date avec fuseau obligatoire) ; ne pas dater les données révisées d’après un communiqué
antérieur. Sans preuve, les faits sont consultables mais exclus du jeu strict.

## Contrat du premier modèle

Modèle expérimental limité aux sociétés renseignées États-Unis, cotées en USD,
identifiées par la SEC, hors quarantaine ; une ligne par émetteur et mois.
La cible est la surperformance en **rendement total à 90 jours calendaires contre
SPY en USD**, et non la surperformance sectorielle ni un cours futur précis.

Features : marge, ROA, capitaux propres/actifs, cash/actifs, passifs/actifs,
cash-flow opérationnel/actifs, capex/actifs, croissance annuelle des revenus.
Les flux sont annuels (330–380 jours), jamais mélangés avec les cumuls intermédiaires.
Ratios de même période et unité ; les absences ne deviennent pas zéro.
Disponibilité SEC = acceptation + 10 min ; les lignes sans horodatage précis
restent hors du modèle strict. À chaque observation mensuelle, l’information
disponible avant minuit UTC est utilisée avec les cours de clôture de la séance.

Séparation chronologique par date 60/20/20, purge des labels franchissant les bornes.
Minimum 1 000 lignes, 10 émetteurs, 96 dates. Référence fréquence historique,
logistique régularisée et gradient boosting fixé à l’avance ; calibration logistique
sur le bloc séparé. Brier/log-loss pondérés à poids égal par date, fiabilité et métriques
annuelles sur le bloc final ; intervalle exploratoire par bootstrap de blocs de trois mois contre 50 %. Aucun choix automatique du meilleur modèle sur ce bloc.

Artefacts : `features_labels.csv.gz`, `model_artifacts.joblib`, `model_report.json`,
`predictions.json`. Le rapport contient empreintes du code, du jeu et de l’artefact.
Les fichiers joblib sont produits localement ; ne charger aucun artefact externe non fiable.

**Promotion toujours interdite dans cette version** : `decision_enabled=false`,
`validated=false`, pas de raccordement AG1. Restent à résoudre le biais de survivance,
les titres radiés, les références sectorielles datées, la robustesse hors États-Unis et
les résultats prospectifs en shadow. De bonnes métriques rétrospectives ne lèvent pas
ces limites. Le dashboard les affiche avec les probabilités expérimentales.

## Tests

```powershell
python -m unittest discover -s tests -p test_ag3_predictive.py -v
$env:PYTHONPATH='services/yfinance-api'
python -m unittest discover -s services/yfinance-api/tests -v
```

Installer les dépendances du service et, pour les tests Yahoo, `httpx` et `yfinance`.
Les tests portent sur les dates, révisions, périodes, absences, calibration/splits,
rechargement des ajustements et séparation du contrat Yahoo existant.
