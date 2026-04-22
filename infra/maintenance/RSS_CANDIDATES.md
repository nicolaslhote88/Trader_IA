# Candidats RSS pour AG4 (à valider sur VPS)

Liste de **30 candidats** RSS/Atom pour remplacer les 22 feeds morts identifiés dans
l'audit du 2026-04-22. URLs établies par recherche web — **pas validées techniquement**
depuis cette session (egress proxy bloque la majorité des domaines).

## Procédure de validation

```bash
# Sur le VPS, dans le repo
sudo apt-get install -y libxml2-utils  # une seule fois (xmllint)
cd /opt/trader-ia/infra/maintenance/
bash check_rss_feeds.sh > rss_check_$(date +%Y%m%d).tsv

# Inspecter le résultat
column -t -s $'\t' rss_check_*.tsv | less -S

# Garder uniquement les feeds OK_FRESH ou OK_NO_DATE pour Source_RSS
awk -F'\t' '$NF == "OK_FRESH" || $NF == "OK_NO_DATE"' rss_check_*.tsv
```

Le script retourne un TSV avec colonnes : `URL HTTP CONTENT_TYPE IS_XML ITEMS LATEST_PUB VERDICT`.
Verdicts : `OK_FRESH` (à garder), `OK_STALE` (à conserver mais surveiller), `OK_NO_DATE`
(parser limité, à inspecter à la main), `EMPTY` / `NOT_XML` / `HTTP_4xx` / `HTTP_5xx` / `TIMEOUT` (à rejeter).

## Liste candidate par famille

Format compatible `Source_RSS` (colonnes Sheet : `enabled`, `family`, `source`, `feedName`, `url`, `interest`, `sourceTier`).

### Banques centrales / Macro global (14)

| family | source | feedName | url | interest | sourceTier | language |
|---|---|---|---|---|---|---|
| Banque Centrale | BCE | Press Releases | https://www.ecb.europa.eu/rss/press.html | 5 | 1 | en |
| Banque Centrale | BCE | Speeches & Interviews | https://www.ecb.europa.eu/rss/fie.html | 4 | 1 | en |
| Banque Centrale | BCE Banking Supervision | News | https://www.bankingsupervision.europa.eu/rss/news.html | 3 | 1 | en |
| Banque Centrale | Federal Reserve | Press All | https://www.federalreserve.gov/feeds/press_all.xml | 5 | 1 | en |
| Banque Centrale | Federal Reserve | Press Monetary Policy | https://www.federalreserve.gov/feeds/press_monetary.xml | 5 | 1 | en |
| Banque Centrale | Federal Reserve | Speeches | https://www.federalreserve.gov/feeds/speeches.xml | 4 | 1 | en |
| Banque Centrale | Bank of England | News | https://www.bankofengland.co.uk/rss/news | 5 | 1 | en |
| Banque Centrale | Bank of England | Publications | https://www.bankofengland.co.uk/rss/publications | 3 | 1 | en |
| Banque Centrale | Bank of Japan | What's New | https://www.boj.or.jp/en/rss/whatsnew.xml | 4 | 1 | en |
| Banque Centrale | Swiss National Bank | News | https://www.snb.ch/public/en/rss/news | 3 | 1 | en |
| Banque Centrale | Bank of Canada | Press | https://www.bankofcanada.ca/rss-feeds/press/ | 3 | 1 | en |
| Banque Centrale | Bundesbank | Press Releases EN | https://www.bundesbank.de/service/rss/en/633286/feed.rss | 4 | 1 | en |
| Macro-Eco | BIS | All Publications | https://www.bis.org/list/all/index.rss | 4 | 1 | en |
| Macro-Eco | BIS | Central Bank Speeches | https://www.bis.org/list/cbspeeches/index.rss | 3 | 1 | en |

### Presse financière FR (8)

| family | source | feedName | url | interest | sourceTier | language |
|---|---|---|---|---|---|---|
| Presse Fin. | Challenges | Économie | https://www.challenges.fr/economie/rss.xml | 4 | 2 | fr |
| Presse Fin. | Challenges | Entreprise | https://www.challenges.fr/entreprise/rss.xml | 3 | 2 | fr |
| Presse Fin. | Capital | Entreprises & Marchés | https://feed.prismamediadigital.com/v1/cap/rss?sources=capital,polemik,xerfi,capital-avec-agence-france-presse,capital-avec-aof,capital-avec-reuters&categories=entreprises-marches | 4 | 2 | fr |
| Presse Fin. | Le Figaro | Flash Éco | https://www.lefigaro.fr/rss/figaro_flash-eco.xml | 4 | 2 | fr |
| Presse Fin. | Le Figaro | Économie | https://www.lefigaro.fr/rss/figaro_economie.xml | 3 | 2 | fr |
| Presse Fin. | Le Monde | Économie | https://www.lemonde.fr/economie/rss_full.xml | 4 | 2 | fr |
| Presse Fin. | Le Monde | Économie Mondiale | https://www.lemonde.fr/economie-mondiale/rss_full.xml | 3 | 2 | fr |
| Média Eco | BFM Business | Toutes actualités | https://bfmbusiness.bfmtv.com/rss/info/flux-rss/flux-toutes-les-actualites/ | 4 | 2 | fr |

### Forex / marchés (2)

| family | source | feedName | url | interest | sourceTier | language |
|---|---|---|---|---|---|---|
| Site Bourse | FXStreet | News | https://www.fxstreet.com/rss/news | 4 | 2 | en |
| Site Bourse | FXStreet | Analysis | https://www.fxstreet.com/rss/analysis | 3 | 2 | en |

### Sectoriels FR (6)

| family | source | feedName | url | interest | sourceTier | language |
|---|---|---|---|---|---|---|
| Secteur BTP | BatiActu | Toutes actualités | https://www.batiactu.com/rss/index.php | 3 | 2 | fr |
| Secteur Tech | Silicon.fr | Feed principal | https://feeds.feedburner.com/silicon/feed/rss | 2 | 2 | fr |
| Secteur Tech | LeMagIT | Toutes rubriques | https://www.lemagit.fr/rss/Toutes-les-rubriques.xml | 2 | 2 | fr |
| Secteur Tech | Journal du Net | Index général | https://www.journaldunet.com/rss/index.xml | 3 | 2 | fr |
| Secteur Tech | Numerama | Articles | https://www.numerama.com/feed/ | 2 | 2 | fr |
| Secteur Tech | Clubic | News | https://www.clubic.com/feed/news.rss | 2 | 2 | fr |

## Sources sans RSS public connu

À ne **pas** essayer de scraper depuis le pipeline RSS — bricolage = nouvelle dette technique.

- **Reuters** : a tué ses RSS publics en juin 2020. Workaround possible via Google News RSS (`https://news.google.com/rss/search?q=site:reuters.com&hl=en`) mais qualité variable et risque de blocage.
- **Bloomberg** : pas de RSS officiel. Idem Google News.
- **Financial Times / FT Alphaville** : pas de RSS officiel public.
- **MarketWatch** : avait des RSS, plus aucune doc officielle récente. À tester si curieux : `feeds.marketwatch.com/marketwatch/topstories/`.
- **CNBC** : avait `cnbc.com/id/100003114/device/rss/rss.html`, à valider, instable historiquement.
- **OECD** : page RSS retirée en 2023 lors de la refonte du site.

Si tu veux quand même une couverture Reuters/Bloomberg, la voie raisonnable est **Google News
RSS** ciblé par requête (`q=...&hl=fr&gl=FR&ceid=FR:fr`) — accepter que c'est un proxy de
recherche, pas un flux éditorial direct.

## Familles à compléter dans Source_RSS

Pour rester cohérent avec ce qui existe déjà côté schéma (vu dans les `raw_error` des `news_errors`) :

```
enabled    → true
family     → voir tableau ci-dessus
source     → nom court de l'éditeur
feedName   → nom du flux dans la nomenclature de l'éditeur
url        → URL complète
interest   → 1 (faible) à 5 (critique)
sourceTier → 1 (officiel/primaire) ou 2 (média/agrégateur)
sourceId   → AG4 le génère probablement (slug auto), à laisser vide si oui
```

## Workflow recommandé

1. `bash check_rss_feeds.sh > rss_check.tsv` sur le VPS
2. Filtrer les `OK_FRESH` (la majorité) + `OK_NO_DATE` (à inspecter au cas par cas)
3. Pour chaque ligne validée : ajouter dans `Source_RSS` avec `enabled=true`
4. Pour les 22 morts identifiés au 2026-04-22 : `enabled=false` (garder en trace)
5. Lancer un run AG4 manuel et vérifier `news_history` se remplit bien des nouvelles sources
6. Re-checker après 2-3 jours de runs : `news_errors` ne doit pas exploser sur les nouvelles
