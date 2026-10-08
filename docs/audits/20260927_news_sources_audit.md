# Audit des sources de news — Trader_IA — 27 septembre 2026

**Conclusion : compléter les sources est pertinent, mais le premier gain vient de la remise en état du canal macro et de la fraîcheur des sources déjà payées/intégrées.** Ajouter immédiatement un autre agrégateur généraliste augmenterait surtout le volume et les doublons. Priorité proposée : corriger les défauts observés, ajouter les publications officielles des émetteurs et SEC EDGAR, puis évaluer EODHD sur les lacunes réelles.

Audit en lecture seule sur le VPS, commencé à 15:29 UTC / 17:29 Paris. Aucun workflow exécuté manuellement, aucun ordre, aucune publication, aucun abonnement ni achat. Les seuls fichiers créés sont locaux. Preuves structurées : [20260927_news_sources_evidence.json](20260927_news_sources_evidence.json).

## 1. Méthode et portée des chiffres

- Vérification de `/health`, `/orders/approvals/pending`, `core.runs`, des versions **publiées** n8n, du cron hôte Finnhub et de son code réellement déployé.
- Lecture DuckDB avec `read_only=True`, SQLite avec `mode=ro`. Sources métier : `ag4_v3`, `ag4_spe_v2`, `ag2_v3`, `ag1_v4_consensus`, `global_context_v1`.
- Mesures glissantes sur 7/14/30 jours au moment de chaque requête ; les lectures successives ne forment pas un snapshot atomique. Audit un dimanche : la dernière activité du vendredi n'est pas en soi une panne.
- « Admissible » signifie `is_relevant=true` et résumé non vide ; c'est une qualification par le système, **pas une validation humaine de la pertinence**.
- Couverture : au moins une ligne dans `news_analyzed`, datée dans la fenêtre. L'absence d'article ne prouve pas qu'un événement a été manqué. Inversement, un article présent ne garantit pas une couverture exhaustive.
- La rétention n8n limite les exécutions disponibles. Boursorama ne sauvegarde pas ses exécutions réussies : leur absence de SQLite n'est pas un arrêt. Ses 80 runs métier SUCCESS sur 30 jours confirment l'activité.

## 2. Sources réellement actives

Les cinq workflows AG4 examinés sont actifs et leur `versionId` correspond à `activeVersionId` : Boursorama, IBKR, Finnhub, News Watcher macro et Health Alert.

| Canal | Périmètre / contenu réellement analysé | Entrées nouvelles 7 j | Admissibles 7 j | Symboles avec news admissibles publiées sur 7 j |
|---|---|---:|---:|---:|
| Boursorama | Rotation actions/ETF, priorité portefeuille ; texte extrait, médiane 1 505 caractères | 674 | 193 — 28,6 % | 72 |
| Finnhub | CORE_MANUAL et CORE_AUTO ; résumé fournisseur, médiane 152 caractères | 2 127 | 1 073 — 50,4 % | 59 |
| IBKR Portfolio | Positions détenues ; **titre seulement**, médiane 82 caractères | 221 | 94 — 42,5 % | 9 |
| RSS macro AG4-V3 | 22 flux configurés ; contexte sectoriel | 6 395 nouvelles lignes sur **30 j** | Aucun impact non nul parmi ces nouvelles lignes | Sans objet |

Le volume par symbole de la dernière colonne et les volumes d'ingestion n'ont pas la même horloge : date de publication contre `first_seen_at`.

Sur 30 jours, les trois canaux single-stock ont enregistré 11 893 lignes : 3 048 Boursorama, 7 601 Finnhub, 1 244 IBKR. Parmi elles, 4 892 passent le critère « admissible » défini ci-dessus. Les volumes stockés ne constituent pas un relevé exact des appels ou tokens facturés.

**Diversité réelle :** 6 407 des 7 601 lignes Finnhub sont étiquetées Yahoo, soit **84,3 %**. SeekingAlpha et Benzinga suivent. IBKR apporte notamment Reuters, Dow Jones, Trading Central, The Fly et des versions coréennes/anglaises de contenus. « Trois connecteurs » ne signifie donc pas trois ensembles éditoriaux indépendants. Les labels Yahoo/Finnhub ne documentent pas toujours l'auteur original.

**Les 22 RSS macro actuels** : AMF opérations et sanctions ; BCE ; Investing général et Forex ; BFM économie ; EasyBourse ; BoE news/publications ; BoJ ; Capital ; Le Monde économie/économie mondiale ; FXStreet news/analysis ; Journal du Net ; Numerama ; Clubic ; Fed press all/monetary/speeches ; BNS. Les banques centrales sont donc déjà intégrées. Ne pas les compter comme de nouvelles sources proposées.

AG5–AG8 sont des producteurs de contexte macro, valorisation, positionnement et taux, pas quatre nouvelles rédactions de presse. Leur dernière synthèse est OK, couverture 0,829 et confiance 0,558 au 25/09 14:05 UTC. AG9 est DISABLED ; WorldMonitor/GDELT ne constitue pas un canal géopolitique live vérifié ici.

## 3. Couverture mesurée de l'univers

Dénominateurs : symboles activés EQUITY/ETF/CRYPTO, quarantaine active exclue ; segments actuels. Les segments peuvent se recouper.

| Groupe | Symboles | Avec news admissibles 7 j | 14 j | 30 j |
|---|---:|---:|---:|---:|
| Positions détenues dans le ledger | 8 | 8 | 8 | 8 |
| CORE_MANUAL | 18 | 17 | 18 | 18 |
| CORE_AUTO | 50 | 42 | 44 | 46 |
| WATCHLIST | 289 | 69 | 115 | 170 |
| Univers hors quarantaine, dédupliqué | 364 | 135 — 37,1 % | 184 — 50,5 % | 241 — 66,2 % |

Les six CORE_AUTO sans news admissible sur 14 jours sont **6861.T, 8035.T, 9983.T, ABCA.PA, DSY.PA et PLNW.PA**. Sur CORE_MANUAL, seul RHM.DE n'a rien sur 7 jours, mais possède quatre articles sur 14 jours.

Les huit détenus sont VK.PA, TXN, UNP, PRX.AS, KO, TKO.PA, GOOGL et TSM. Leur couverture dépend souvent d'IBKR. L'exclusion du segment HELD dans le collecteur Finnhub ne signifie pas exclusion des titres détenus : un titre également CORE, par exemple TSM, reste collecté. Le collecteur actuel parcourt 68 symboles ; le dernier passage trouve des news pour 56.

## 4. Anomalies prioritaires — faits validés

### P0 — Analyse macro xAI refusée et erreur transformée en neutralité

Exécution `22482`, le 25/09 à 18:45 Paris : **34 appels sur 34** du nœud `20H1R - Analyze with Grok` retournent une erreur Forbidden. Le fournisseur indique crédit disponible épuisé **ou** plafond mensuel atteint ; ces deux causes ne peuvent pas être départagées sans consulter la facturation.

Le merge conserve l'erreur, mais `20H2R` utilise un JSON vide lorsqu'il ne trouve pas de réponse. Il produit alors `Noise`, impact 0, sans signal sectoriel. Le workflow termine néanmoins `success` dans n8n ; `run_log` est PARTIAL pour les erreurs RSS, pas pour cet échec LLM. Il faut représenter un fournisseur indisponible comme une donnée dégradée, et non comme une opinion neutre.

Les 6 395 lignes dont `first_seen_at` est dans les 30 derniers jours ont toutes `source='unknown'` et impact 0. La dernière première apparition conservée avec impact non nul remonte au **4 août**. Cela ne date pas avec certitude le début de la panne xAI : seule l'erreur du dernier run a été identifiée précisément.

Action proposée : rétablir l'accès xAI ou valider un remplacement en replay ; faire échouer explicitement l'analyse en cas d'erreur ; conserver la matière brute pour reprise ; alerter sur le taux d'échec fournisseur et l'absence prolongée de signal. Ne pas ajouter de flux à cette branche tant que ce contrat n'est pas corrigé.

### P1 — Provenance et actualité macro trompeuses

Sur le run `22482`, les 677 éléments normalisés sont déjà `unknown`, alors que leur URL et leur `feedUrl` existent. Le défaut est donc antérieur à l'écriture ; l'attribution peut être restaurée depuis le registre des feeds. La cause précise du défaut de `inferSource` dans le runner reste à reproduire.

Le run AG1 `22480` reçoit seulement **deux** lignes macro pour sa fenêtre de dix jours. Elles sont encore présentes lors de l'audit avec des premières observations de février/avril et des publications affichées en septembre, impact 3. Il faut vérifier les URL réutilisées, les révisions de flux et la conservation d'anciens scores : la date récente ne prouve pas une nouvelle analyse récente.

Les deux feeds FXStreet cumulent chacun **34 erreurs**, depuis le 10/09 ; les trois derniers runs macro n'ouvrent que 20 flux sur 22. Les messages persistés sont `unknown` : le diagnostic HTTP précis manque. Trois erreurs Fed press_all apparaissent aussi sur 30 jours.

### P1 — Finnhub : collecteur UTC et analyse Paris désalignés

| Étape | Heures UTC actuelles | Heures Paris au 27/09 |
|---|---|---|
| Collecteur hôte | 09:00, 12:00, 15:00 | 11:00, 14:00, 17:00 |
| Analyse n8n | 08:00, 11:00, 14:00 | 10:00, 13:00, 16:00 |
| AG1 | 15:10 | 17:10 |

Le lot collecté à 17:00 Paris arrive **après** la dernière analyse à 16:00 et ne peut donc pas nourrir AG1 à 17:10. Le vendredi, la prochaine analyse est prévue le lundi à 10:00 Paris. À l'audit, **121 articles encore absents de `news_history`** proviennent de la collecte du vendredi 15:00 UTC. Ce n'est pas une panne d'API, mais une attente induite par l'ordonnancement.

Latence mesurée publication → analyse, sur les lignes récemment ingérées : Finnhub médiane **18,77 h**, P90 **40,73 h** ; Boursorama 2,33 h / 61,32 h. Le P90 Boursorama reflète notamment la rotation, à approfondir par segment. Ces délais incluent collecte et traitement, et ne sont pas la latence propre des fournisseurs.

Action : définir explicitement le fuseau de chaque étape et une dépendance collecte terminée → analyse → AG1, après contrôle des écrivains DuckDB dans `SCHEDULING_AND_LOAD.md`. Les heures finales doivent être validées sur les durées réelles ; aucun cron modifié ici.

### P1 — Le passage Boursorama de 17:05 termine après AG1

Le 25/09, Boursorama démarre à 17:05:14 et termine à **17:33:55**, contre AG1 à 17:10. Le workflow possède un buffer et une écriture groupée en fin de parcours. Le créneau de 17:05 ne garantit donc pas des données disponibles pour la décision de 17:10. Le passage de 14:05 est le dernier lot complet garanti sur cet exemple. Vérifier la fin d'écriture, pas uniquement l'heure de déclenchement.

### P1 — IBKR apporte surtout des titres et une heure parfois non vérifiée

Le nœud de normalisation affecte `headline` à `snippet`, `text` et `llmInput.content` : les résumés générés sont donc des analyses de titres. Reuters/Dow Jones comme fournisseur ne garantit pas que le corps de la dépêche soit accessible au modèle.

Sur les sept nouveaux éléments du dernier run IBKR `22473`, quatre portent `INVALID_PROVIDER_TIME` et trois `UNVERIFIED_PROVIDER_TIME`. Vérification complémentaire pendant la remédiation : le contrat est bien conservé en JSON dans `published_at_raw`, malgré l’absence de colonnes dédiées. Il ne faut donc pas conclure à une perte de cette métadonnée. La médiane apparente publication → analyse de 0,1 h n'est donc **pas** une preuve de livraison quasi instantanée.

L'identification utilise le préfixe du titre puis le symbole sans suffixe. Risque de collisions de places : à durcir avec identité canonique/ISIN/conid lorsqu'ils sont disponibles. L'échantillon TKO.PA consulté contient bien des annonces de Tikehau ; aucune mauvaise attribution à TKO US n'a été démontrée ici.

### P2 — Déduplication et sélection à améliorer avant expansion

- Finnhub déduplique avec `SHA1('finnhub|' + article_id)` et conserve un seul `symbol`. Un même article renvoyé pour plusieurs entreprises perdrait les associations suivantes. Le défaut est établi dans le code live ; son volume réel n'a pas été mesuré.
- Sur les articles admissibles publiés sur 30 jours, les titres strictement identiques au sein d'une même source et d'un même symbole donnent **48 lignes supplémentaires** : 23 IBKR, 23 Finnhub, 2 Boursorama. Cette mesure sous-estime les mêmes événements reformulés ou traduits.
- Aucun doublon exact `(symbol, canonical_url)` trouvé dans cette vue. Cela ne prouve pas une absence de doublons éditoriaux : les URL Finnhub sont des redirections et les dépêches multilingues ont des identifiants distincts.
- Le digest live ne correspond plus à la description historique du nœud 20K : il est construit dans **R8 → Calcul Matrice**. Le code sélectionne les trois publications les plus récentes par symbole, sur 30 jours par défaut ; ni déduplication événementielle ni préférence fournisseur dans cette requête. Le seuil d'environnement effectif doit être explicité avant modification.
- L'agrégat quantitatif additionne les impacts. Multiplier les reprises du même événement peut changer le score sans information supplémentaire. Toute évolution de cette agrégation exige la parité AG1/dashboard documentée dans `SYSTEM_LINKS_AND_PARITY.md`.

### P2 — Supervision incomplète

Health Alert inspecte le dernier run Boursorama réussi, les zombies et les dates aberrantes de `ag4_spe_v2`. Il ne couvre pas la santé xAI macro, le retard du staging Finnhub, le rendement par fournisseur ni le contenu effectivement livré à AG1. Son dernier run n'a produit aucune alerte.

`core.runs.news_count=0` sur AG1 `22480` est également insuffisant comme indicateur : les entrées assemblées de cette même exécution contiennent **25 symboles, dont 23 avec news**, et les huit positions détenues ont chacune trois éléments. Le canal single-stock est donc bien consommé malgré ce compteur nul.

## 5. Compléments recherchés et essais réalisés

| Candidat | Gain attendu pour Trader_IA | Coût / accès vérifié | Conclusion |
|---|---|---|---|
| **SEC EDGAR** | Publications réglementaires US et émetteurs étrangers déclarants ; identité CIK, documents datés, catalyseurs vérifiables | API publique sans clé ; plafond officiel 10 requêtes/s | **Priorité 1**, filtrer 8-K/6-K et publications financières ; éviter de résumer intégralement tous les dépôts |
| **Relations investisseurs + communiqués émetteurs** | Couverture directe des six CORE_AUTO lacunaires et des détenus ; meilleure identité d'entreprise | Pages/flux publics lorsqu'offerts ; coût de maintenance du mapping | **Priorité 1**, commencer par les lacunes mesurées ; ne pas multiplier des centaines de scrapers |
| **GlobeNewswire / Euronext company news** | Communiqués et information réglementée européenne, utile aux petites/moyennes capitalisations | Catalogue RSS GlobeNewswire public ; page Euronext identifiée, accès API industrialisé non validé | **Priorité 1 ciblée**, sélectionner émetteurs et catégories avant LLM |
| **BLS / Eurostat** | Publications officielles emploi/inflation et indicateurs européens, complément textuel aux séries AG5 | Feeds officiels publiquement documentés | **Priorité 2**, après réparation macro ; éviter de doubler les mêmes événements Fed/BCE déjà ingérés |
| **EODHD News + Calendar** | Corps d'article, recherche par ticker/date, catalyseurs et calendrier | Offre affichée **19,99 USD/mois** ; page produit annonce 15–60 min de délai ; une requête news vaut 5 unités API | **Pilote conditionnel**, utile si gain prouvé sur Europe/Asie ou qualité du texte ; pas de souscription pendant l'audit |
| **FMP** | News/communiqués avec filtres et autres données financières | Ultimate global affiché **99 USD/mois, facturé annuellement** ; inclusion exacte des endpoints à confirmer pour le contrat choisi | Alternative si besoin transversal plus large ; peu justifié pour ajouter uniquement des news |
| **Alpha Vantage** | News sentiment et filtres entreprises/thèmes | Offre gratuite majoritaire : 25 requêtes/jour, éligibilité endpoint à vérifier | Petit essai possible ; ne résout pas d'emblée les lacunes locales identifiées |
| **Benzinga direct** | Flux spécialisé américain et données de catalyseurs | API officielle ; prix contractuel non établi ici | Reporter : Benzinga est déjà distribué par Finnhub/IBKR ; exiger preuve de contenu exclusif et droits d'usage |
| **GDELT** | Découverte multilingue et géopolitique | API DOC documentée ; qualité/latence VPS non testées | Futur canal consultatif, pas de scoring single-stock direct ; AG9 reste désactivé |
| **NewsAPI** | Presse généraliste | Gratuit limité au développement, délai 24 h ; Business 449 USD/mois ; pas de texte intégral fourni | **Non retenu** pour ce besoin |

Sources officielles consultées le 27/09 : [SEC API](https://www.sec.gov/search-filings/edgar-application-programming-interfaces), [SEC accès développeurs](https://www.sec.gov/about/developer-resources), [GlobeNewswire RSS](https://www.globenewswire.com/rss/list), [Euronext company news](https://live.euronext.com/en/products/equities/company-news), [BLS](https://www.bls.gov/feed/), [Eurostat](https://ec.europa.eu/eurostat/web/rss/), [EODHD produit/prix](https://eodhd.com/lp/calendar-and-news-api), [EODHD contrat API](https://eodhd.com/financial-apis/stock-market-financial-news-api), [FMP tarifs](https://site.financialmodelingprep.com/developer/docs/pricing), [Alpha Vantage support](https://www.alphavantage.co/support/), [Benzinga](https://www.benzinga.com/apis/), [GDELT DOC](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/), [NewsAPI tarifs](https://newsapi.org/pricing).

**Essais HTTP depuis le VPS, sans nouvelle clé :**

- SEC `https://data.sec.gov/submissions/CIK0000320193.json` : HTTP 200, Apple identifié, 1 000 dépôts récents dans la réponse, dernier dépôt daté du 24/09. Validation du transport/format, pas d'extraction complète des événements.
- Fast Retailing `https://www.fastretailing.com/eng/ir/news/index.xml` : HTTP 200, XML valide, 20 entrées, dernière le 18/09. Ce flux est [officiellement annoncé par l'émetteur](https://www.fastretailing.com/eng/termsofuse/). L'entrée du 18/09 est une annonce de date de conseil, **pas un résultat financier inédit démontré**. Elle prouve néanmoins un accès direct possible pour 9983.T, actuellement vide dans notre fenêtre 14 j.
- GlobeNewswire : catalogue HTTP 200 et catégories Earnings/European Regulatory News identifiées ; flux individuels et couverture des émetteurs non encore testés.
- EODHD, clé publique `demo`, AAPL.US du 20 au 27/09 : HTTP 200, **287 articles**, contenu médian **4 186 caractères**. Sur la sous-fenêtre commune arrêtée au 25/09 à 12:00 UTC : 251 articles, dont **242 URL Yahoo, soit 96,4 %**. Les 132 lignes AAPL du système sur cette période incluent 112 liens de redirection Finnhub et 20 Boursorama. Une comparaison brute d'URL ne mesure donc pas la nouveauté des événements. Aucun gain de diversité éditoriale ni de performance de trading n'est démontré par cet essai.

Les descriptions EODHD utilisent à la fois « real time » et « 15–60 min delay » : retenir le délai annoncé le plus prudent jusqu'à mesure. L'accès au texte n'établit pas à lui seul les droits de conservation, d'affichage ou de transmission à un fournisseur LLM : confirmer ces usages dans l'offre effectivement retenue. Les tarifs publics peuvent évoluer ; aucun devis commercial n'a été demandé.

## 6. Mise en œuvre recommandée, non déployée

**Étape A — Restaurer la valeur de l'existant.** Corriger l'erreur fournisseur macro, les statuts de qualité, la provenance, les deux flux FXStreet et l'ordre collecte/analyse. Vérifier les dates anciennes réutilisées. Conserver Boursorama et IBKR ; renforcer la fraîcheur et envisager Finnhub en secours de HELD lorsque le canal broker échoue, sans supposer que la seule présence d'un titre garantit sa couverture.

**Étape B — Ajouter une couche primaire ciblée.** Registre `issuer_id → ISIN/CIK/conid → tickers locaux/ADR → sources IR`. Commencer par les huit détenus et les six CORE_AUTO lacunaires. Sources SEC pour déclarants US/ADR ; IR/communiqués pour les autres. Enregistrer séparément `published_at`, `updated_at`, `observed_at`, `analyzed_at` et `timestamp_quality`.

**Étape C — Un seul pilote agrégateur, EODHD en premier.** Shadow sur 10 séances, comprenant détenus, CORE et une sélection WATCHLIST insuffisamment couverte. Même fenêtre temporelle et mêmes entités pour tous les fournisseurs. Résoudre les liens de redirection avec limites et dédupliquer les événements avant comptage ; conserver les associations article↔plusieurs entreprises.

Critères proposés pour conserver un ajout, à valider avant le pilote :

1. Au moins +10 points de couverture des symboles prioritaires sous-couverts, **ou** +20 % d'événements matériels distincts ; ne pas compter les traductions/reprises comme des événements.
2. Latence publication → disponibilité AG1 compatible avec 17:10 Paris ; cible de fin du lot au moins 15 minutes avant, sans nouvelle contention DuckDB.
3. Qualité vérifiée sur un échantillon humain équilibré : bonne entreprise ≥95 %, pertinence ≥80 % après filtrage ; mesures proposées, non résultats acquis.
4. Coût suivi par article utile et événement nouveau : requêtes, tokens d'entrée/sortie, erreurs et retries. Budget API initial proposé ≈20 USD/mois ; coût LLM additionnel **non chiffré** tant que l'usage réel et les tarifs du modèle retenu ne sont pas confrontés.
5. Digest borné à trois événements par symbole, classement matérialité × récence × provenance ; plafonds avant LLM. Les gros corps EODHD ne doivent pas tous être envoyés intégralement.

Une source sans événement peut être saine ; une source en erreur ne doit pas être assimilée à zéro événement. Une éventuelle publication nécessite replay/shadow puis preuve de version active conformément à `AGENTS.md`. Les poids/gates d'AG1 et du dashboard ne sont pas modifiés par cet audit.

## 7. Points annexes et limites restantes

- Contrôle broker : authentifié, configuration réelle, mais session gateway actuellement paper et `account_alignment.aligned=false` ; zéro approbation en attente. Anomalie distincte consignée, connexion non modifiée.
- Ni crédits xAI/plafond, ni droits IBKR au texte intégral, ni factures de données/LLM n'ont été modifiés ou exhaustivement audités. Pas d'appel payant de diagnostic au modèle.
- Couverture API EODHD des six symboles lacunaires non testée : la clé démo ne valide que le cas Apple. La couverture globale marketing ne remplace pas cette recette.
- L'identité et les publications sémantiques de tout l'univers n'ont pas été relues. Aucun backtest de rentabilité ni attribution causale aux sources.
- Le collecteur Finnhub live a une empreinte différente du fichier du dépôt. L'audit s'appuie sur le live ; une synchronisation future doit partir de celui-ci. Empreinte enregistrée dans les preuves.
- Aucune modification des sources de production ni synchronisation GitHub effectuée. Livrables locaux : ce rapport et son JSON de preuves.

## 8. Reproduction locale des sondes conservées

Les sondes ponctuelles se trouvent dans `.codex-tmp/news-audit-20260927/` : `probe.py`, `metrics.py`, `execution_probe.py`, `followup.py`, `http_probe.py`. Elles interrogent les bases en lecture seule ; aucune exécution des nodes exportés. Exemple PowerShell depuis la racine du dépôt :

```powershell
Get-Content -Raw .codex-tmp/news-audit-20260927/metrics.py | ssh vps 'docker exec -i root-n8n-1 python3 -'
Get-Content -Raw .codex-tmp/news-audit-20260927/http_probe.py | ssh vps 'python3 -'
```

Les identifiants d'exécution de `execution_probe.py` sont fixés à cet audit et disparaîtront avec la rétention n8n. Les résultats utiles et versions publiées sont conservés dans le JSON de preuves. Les fenêtres des sondes métriques sont glissantes ; leur répétition ne donnera pas nécessairement les mêmes décomptes.


## Suite autorisée le 27 septembre

Remédiation avec flux gratuits uniquement et migration Grok vers DeepSeek Flash : [déploiement et preuves](../operations/20260927_news_free_flash_remediation.md). Les chiffres et observations ci-dessus décrivent le snapshot **avant** modification.
