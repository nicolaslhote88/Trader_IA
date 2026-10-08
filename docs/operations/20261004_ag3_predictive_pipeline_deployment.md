# AG3 — collectes historiques gratuites et évaluation prédictive

Intervention du 4 octobre 2026, autorisée par « lance la mise en œuvre complète avec les pipes » et demande de réutiliser le service Yahoo existant.

## Périmètre livré

- Extension additive du service `yfinance-api`, version 2.2.0 : `/research/history`, cours ajustés/dividendes/splits, dates explicites, cache et temporisation Yahoo existants. Les routes utilisées par le trading gardent leur contrat.
- Service `ag3-predictive`, API interne 8084, un CPU et 1 200 Mo maximum, stockage `/local-files/ag3-predictive` en SQLite WAL et archives gzip SHA256. Les DuckDB métier sont ouvertes seulement en lecture seule pour découvrir l’univers.
- Pipelines SEC (profils, anciens dépôts, facts), ESEF (catalogue paginé et JSON), GLEIF (identités contrôlées), BCE EUR/USD, ALFRED (FEDFUNDS, CPIAUCSL, UNRATE avec versions). Clé FRED existante réutilisée sans affichage.
- Collecte des prix depuis 2010, puis incrémentale avec 14 jours de chevauchement ; rechargement intégral mensuel ou si les ajustements changent. Les archives préservent les versions d’origine.
- Registre de couverture par titre et source : absence de mapping, données manquantes, date de publication inconnue et erreurs sont distinctes.
- Premier modèle expérimental : sociétés américaines identifiées SEC, comptes annuels, rendement total à 90 jours contre SPY en USD. Fréquence historique et constante 50 %, logistique et gradient boosting, calibration et test chronologiques séparés, purge des horizons chevauchants. Aucune sélection du modèle gagnant sur le test.
- Encadré du dashboard dans « Analyse fondamentale / Vue détaillée » : états des sources, vrais comptes historiques, métriques du test et probabilités explicitement expérimentales.
- Aucun raccordement des probabilités à AG1 ; `decision_enabled=false`, `validated=false`. Aucun poids, seuil, garde d’exécution, ordre ou workflow n8n modifié.

## Ce qui reste partiel ou non validé

- L’utilisateur confirme ne pas avoir de clés EDINET/OpenDART. États `MISSING_KEY` explicites. EDINET : découverte récente des dépôts préparée ; OpenDART : archive des comptes annuels préparée. Leur backfill documentaire et leur normalisation historique datée ne sont pas validés ni utilisés par le modèle ; fournir les clés ne suffira pas à déclarer cette validation acquise.
- ESEF : les comptes sont collectés, mais la première publication au marché n’est pas établie automatiquement. Aucun usage de `date_added` comme date comptable disponible. Les versions ne peuvent être qualifiées via `verified_publications.json` qu’avec horodatage à fuseau, preuve HTTPS et empreinte exacte du JSON concerné.
- SEC : le fichier global des tickers retourne 403 ; les CIK candidats sont validés individuellement sur les profils. Un nom d’entreprise ressemblant ne remplace pas ce contrôle. Exxon présente un profil sans ticker et reste non vérifié. Un ticker ADR radié, comme ABB, n’est pas remplacé silencieusement par une autre cotation/devise.
- Le premier modèle n’est pas une validation internationale ou sectorielle, ni une prévision d’un cours exact. Il ne consomme encore que les ratios comptables annuels. Macro et change sont collectés pour les extensions futures.
- Univers courant : biais de survivance non résolu, rendements des radiations non garantis. Absence de suivi prospectif achevé. Une amélioration rétrospective ne suffit pas à promouvoir les probabilités.

## Validation réalisée

- 10 tests ciblés : révision future sans fuite dans le passé, préservation des NULL, exclusion des flux intermédiaires, dates ESEF inconnues, purge chronologique des labels, contrat Yahoo/cache, rechargement des ajustements, cycle entraînement/calibration/artefacts avec promotion interdite.
- 7 tests existants du service Yahoo : clôtures des barres, fuseaux, DST, filtre des données invalides. Aucun changement de leurs résultats.
- Shadow réel sur AAPL, MSFT, DSY.PA et benchmark SPY dans un répertoire séparé : 16 926 lignes de prix, 1 965 faits SEC, 82 faits ESEF, 1 609 observations/vintages ALFRED, 4 290 observations BCE. Le modèle refuse cet échantillon insuffisant. Replay des versions successives et contrôle du cache réussis.
- AppTest sur les modules réellement déployés et les comptes GOOGL live lus en lecture seule : 13 métriques, 2 graphiques, 8 tableaux, aucune exception. Le second graphique et les métriques AG3 existants restent présents.
- `/health` Yahoo, `/health` recherche et santé Streamlit répondent HTTP 200. Le statut fonctionnel se lit via `/status`, pas via la seule santé HTTP.

## Couverture constatée après reprise

364 instruments segmentés, dont 362 actions et 2 ETF. Stockage : 1 414 553 prix,
50 377 faits comptables normalisés, 3 627 dépôts, 1 611 observations/versions macro
et 4 290 cours BCE. Le prix de référence SPY s'ajoute aux instruments de l'univers.

57 titres ont des comptes SEC collectés et leur ticker contrôlé ; 146 ont des
comptes ESEF encore sans publication prouvée. Les absences de mapping, 20 émetteurs
sans dépôt dans le catalogue et 3 sans faits utilisables restent visibles. Deux
ETF nécessitent un modèle distinct. Trois historiques Yahoo restent en erreur (ABB, PVL.PA, ROG.SW) ;
le premier chargement ACAN.PA a écarté 425 barres invalides, jamais remplacées par zéro.
Les prix archivés ne constituent pas une garantie d'exhaustivité des séances.

Run final `20261004T055940Z`, terminé le 4 octobre à 06:05:28 UTC : `PARTIAL`
à cause des trois erreurs Yahoo. Les 18 empreintes des sources déployées correspondent
aux fichiers locaux ; AG1 et les deux AG3 restent actifs sur leurs versions antérieures.

La reprise conserve le nombre de prix et complète le catalogue européen. La
consommation mémoire observée durant la réévaluation finale est de 270 Mio environ
(sous la limite de 1 200 Mo) ; ce relevé n'est pas une mesure du pic maximal.

## Premier résultat sur copie figée du jeu réel

44 titres, 7 406 observations mensuelles. Apprentissage : 2010-01 à 2019-07 (3 795 lignes), calibration : 2019-11 à 2022-12 (1 643), test : 2023-03 à 2026-06 (1 760 lignes, 40 dates). Les labels de train et calibration finissent avant le bloc suivant.

| Modèle | Brier test | Log-loss test |
|---|---:|---:|
| Fréquence de la période d’apprentissage | 0,258655 | 0,710584 |
| Constante 50 % | 0,250000 | 0,693147 |
| Logistique calibrée | 0,250419 | 0,693997 |
| Gradient boosting calibré | 0,251387 | 0,695944 |

Ni la logistique ni le gradient boosting ne battent ici la constante 50 %. Le gain contre la fréquence historique seule ne prouve donc pas l’utilité du prédictif. Les dates mensuelles/horizons à 90 jours se chevauchent ; les lignes ne sont pas indépendantes. La version finale ajoute un intervalle exploratoire par bootstrap en blocs de trois mois. Pas de promotion.

Les données et résultats finaux sont consignés dans `20261004_ag3_predictive_evidence.json` ; le snapshot ci-dessus est explicitement l’essai sur copie figée, pas une promesse de stabilité des scores futurs.

## Exploitation et déploiement

Code local : `services/ag3-predictive/` ; mode d’emploi complet : `services/ag3-predictive/README.md`.

Code VPS : `/opt/trader-ia/services/ag3-predictive/`, compose propre sur réseau `web`, fichier `.env` mode 600 non versionné. Yahoo conserve sa stack `/docker/yfinance` et son contexte `/docker/root/yfinance-api`. Dashboard monté depuis `/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard`, vérifié avant intervention.

Planification : **04:45 Europe/Paris, tous les jours**, après AG3 04:00 et avant YF-ENRICH 06:15. Crontab à :45 UTC chaque heure ; le wrapper vérifie l’heure Paris. Verrou `flock`, collecte séquentielle, pas de writer partagé DuckDB. Le premier cycle programmé reste à observer après installation ; un chargement manuel ne le prouve pas.

```bash
# État fonctionnel, avec lacunes et résultats du modèle
docker exec ag3-predictive python -c 'from store import status; import json; print(json.dumps(status()))'
# Collecte ou reprise idempotente ; refus si une collecte est déjà active
docker exec ag3-predictive python runner.py
# Réévaluation sans collecte, hors collecte active
docker exec ag3-predictive python runner.py --train-only
```

Artefacts : `research.sqlite`, `raw/`, `collector.log`, `status.json`, `features_labels.csv.gz`, `model_report.json`, `model_artifacts.joblib`, `predictions.json` dans `/local-files/ag3-predictive`. Empreintes code/jeu/modèle dans le rapport. Ne pas supprimer les archives utilisées pour un jeu de recherche.

## Sauvegardes et retour arrière

Sauvegardes : `/local-files/.codex-tmp/ag3-predictive-20261004/backup/`, source Yahoo complète et `dashboard_app.py`. Image Yahoo précédente : `yfinance-before-ag3:20261004`. Preuves/replays locaux : `.codex-tmp/ag3-predictive-20261004/`.

1. Retirer uniquement la ligne cron identifiée `AG3 predictive research`, puis arrêter le conteneur `ag3-predictive` après arrêt de sa collecte. Conserver ses données pour diagnostic.
2. Restaurer les fichiers Yahoo depuis `backup/yahoo/`, puis rebâtir et recréer uniquement `yfinance-api` dans `/docker/yfinance`. L’image sauvegardée permet aussi de revenir à l’image précédente avec `--no-build` après retag adéquat.
3. Restaurer `dashboard_app.py` vers le `app.py` du montage confirmé, puis redémarrer uniquement le dashboard. Le nouveau module inutilisé peut rester présent.
4. Relire les trois endpoints de santé, le broker et les approbations. Aucun rollback n8n/ledger requis puisqu’ils n’ont pas été modifiés.

Ne pas restaurer globalement des bases métier : cette intervention ne les écrit pas.
