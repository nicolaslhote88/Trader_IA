# Corrections du contrat de performance — 14 septembre 2026

## Résultat déployé

L’audit a établi des pertes d’information dans le pack des modèles, une confusion entre score signé et confiance, des ambiguïtés BUY/SELL et des mesures de comparaison incomplètes. Le correctif rétablit ces contrats sans prétendre démontrer un avantage d’investissement. Le diagnostic PRX et son marqueur d’achat, déployés dans la PR précédente, sont conservés.

| Chaîne | Comportement corrigé |
|---|---|
| Positions → modèles | Une seule référence de portefeuille ; NAV du relevé conservée, écart cash + positions exposé séparément. Prix natifs/EUR, change daté, coût EUR des lots restants avec frais alloués, performances locale/EUR et provenance disponibles jusque dans les trois prompts. |
| Ancienneté et stop | Achats/ventes **exécutés** uniquement ; fermeture/réouverture réinitialise l’ancienneté. Dernière proposition distincte du dernier fill. Stop initial exprimé dans la devise native, avec provenance ; il reste indicatif, sans prétendre qu’un ordre protecteur est enregistré chez IBKR. |
| Technique | R8 transmet `d1_confidence` sur 0–100, séparément de `d1_score` sur −6–6. `tech_prob = 50 + direction × max(0, 8 + 0.2 × (confidence − 50))` avec confiance bornée. BUY/SELL sont symétriques, NEUTRAL vaut 50. Formule identique dans le dashboard ; pondérations conservées. |
| Interprétation des scores | Les clés historiques `p_win_pct`/`ev_r` restent compatibles, mais les textes les qualifient de scores heuristiques non calibrés. Un score favorable ne constitue pas une probabilité de succès mesurée. |
| Sélection | Jusqu’à 50 entrées sont examinées avant de sélectionner dix entrées faisables. Toutes les positions détenues restent dans le pack ; une courte liste de candidats non finançables explique les contraintes. |
| Exécution | `buy_feasibility` et `sell_feasibility` distincts. Aucun cash anticipé d’une vente proposée. Plafonds de concentration et nombre de lignes vérifiés sur le lot d’achats. Lot, ticket minimal, change et plafonds déterminent les quantités ; bornes de poids arrondies dans le sens conservateur. |
| Cotations | Horodatage du fournisseur conservé ; un temps de récupération ne rend plus une quote ancienne fraîche. Données IBKR différées/fixées distinguées. Un cross Yahoo **daté** peut compléter un taux non-G10 absent ; un taux ancien ou inconnu bloque l’achat. |
| Contrats IBKR | Résolution par place pour les suffixes asiatiques et autres places prises en charge. Les actions ordinaires japonaises identifiées utilisent 100 titres, les actions US la politique à l’unité. `sizeIncrement=100` de CPAPI n’est pas assimilé universellement à un lot minimal. Les instruments dont le lot reste inconnu sont bloqués à l’achat. |
| Comparaison | S&P 500 converti en EUR avec EURUSD daté ; CAC 40 et EURO STOXX 50 restent en EUR. Dernière clôture disponible au relevé, sans information future ni report illimité. Variantes **indices de prix**, dates et limites explicites. Écart de rendement séparé d’un alpha ajusté du risque. |
| Flux et coûts | Rendement chaîné corrigé des apports/retraits EUR **comptabilisés**, supposés en fin d’intervalle : estimation, pas TWR certifié. Les dividendes restent dans la performance. Courtage inclus dans la NAV ; facturation IA/infrastructure externe inconnue, montant IA comptabilisé distinct d’un coût exhaustif. |
| Risque | Drawdown observé depuis le sommet, rendement depuis le dernier relevé du jour UTC précédent et secteurs agrégés. Secteur incomplet ou VaR non estimée → valeur inconnue explicite. Le seuil de perte journalière configuré bloque les nouveaux achats, en laissant les sorties possibles ; valeur par défaut existante de 6 %. Il ne constitue pas un stop garanti entre deux passages. |
| News et logs | Dates IBKR futures remplacées par la première observation, valeur brute et qualité conservées. Aucun décalage uniforme de deux heures inventé. Les runs dépassant six heures sans finalisation sont réconciliés en ERROR avec motif explicite. Retrait des CHECKPOINT forcés dans les nœuds touchés. |
| Couverture et preuve | TEAM, PLTR et VEEV vérifiés par métadonnées Yahoo + contrat IBKR, puis ajoutés à WATCHLIST, sans promotion fondée sur leurs gains passés. Les entrées datées et tous les candidats du préflight sont conservés dans `core.runs.agent_output_json.decisionInput`. Rapport quotidien de couverture en lecture seule. |

Le seuil de ticket de 1 000 EUR, les plafonds d’allocation, les trois modèles, la règle de consensus et les variables de garde IBKR ne sont pas optimisés sur cet historique. La facture exacte et les flux externes doivent encore être rapprochés d’un relevé complet du courtier et de la facturation des fournisseurs.

## Horaires publiés

Heure de Paris, lundi à vendredi :

- AG2 Held+Core : **09:00, 13:00, 16:35**. Le dernier passage suit la première bougie H1 régulière US lors de l’ouverture usuelle à 15:30 Paris. Les fenêtres différentes de changement d’heure restent compatibles avec ce passage tardif.
- AG1 V4 : **17:10**, un passage quotidien comme dans la version live précédente (qui tournait à 16:30). La documentation locale à deux passages était obsolète.
- PF : **09:15 à 16:15**, puis **17:40** et **23:15**. Le dernier créneau enregistre un relevé même si la NAV n’a presque pas varié.
- Rapport de couverture : **23:45 UTC**, en lecture seule, vers `/local-files/monitoring/performance/latest.json`.

AG2 laisse 35 minutes avant AG1 : durées récentes observées 10–16 minutes, maximum historique documenté 27 minutes. AG1 dispose ensuite de vingt minutes avant la clôture Euronext usuelle. Le déplacement du PF évite son ancien créneau 17:15 au milieu du passage AG1. Les autres crons restent inchangés. Voir [ordonnancement](SCHEDULING_AND_LOAD.md).

## Validation et preuves

**Complément après les premiers crons PF :** les exécutions 22059/22060 ont révélé une dépendance implicite `pytz` absente des runners. Les rejeux initiaux utilisaient le Python du conteneur n8n et ne reproduisaient pas ce manque. Lecture temporelle corrigée et rejeu de l’entrée réelle validé dans le sandbox des trois runners : [incident et correctif](20260914_pf_runner_timezone_fix.md).

- 65 tests Python sur broker, dashboard, contrat de modèle, unités techniques, positions et réconciliation PF ; tests JavaScript de consensus, devises, extracteurs et contrat d’exécution.
- Rejeu avec données du VPS sur copies : PF, enrichissement des dix positions, R8 sur 361 lignes, matrice, préflight en GET uniquement, consensus synthétique sans action, validateur et écriture du journal sur copie. Aucun endpoint d’ordre appelé.
- Régression : prix USD constant et change variable, versement/retrait, absence de change, dates manquantes, précision microseconde/nanoseconde, réouverture, cash nul pour une vente, produits de ventes non exécutées, lot japonais, concentration cumulée, perte journalière et remplacement des dix premiers candidats infaisables.
- 634 snapshots historiques réparés en production, avec empreinte NAV/cash/valorisation identique avant et après. 2 305 dates IBKR incohérentes corrigées, aucune inversion restante sur le critère réparé ; quatre runs news et six runs AG3 anciens réconciliés.
- Publication des sept workflows : `active=1` et `versionId=activeVersionId`. Les 173 autres workflows de l’instance partagée restent inchangés. Une dernière publication chirurgicale d’AG1 ne modifie que le préflight (bornes d’arrondi et dates futures).
- Contrôle final : code des sept workflows publiés et des sept sources déployées identique au dépôt ; ordres, fills, lots et cash ledger inchangés par comparaison des empreintes avant/après.
- Broker authentifié et compte aligné après reconstruction ; contrats des places testées résolus. Dashboard testé via `Streamlit AppTest` dans son image de production, sans exception, en affichage standard et sur tout l’historique en pourcentage.

La configuration publiée et les rejeux sont validés. Le premier passage de trading avec ces corrections reste celui du prochain cron ; il n’a pas été déclenché manuellement. La collecte prospective des entrées commence avec ce passage. L’apport marginal des piliers, les alternatives de portefeuille et la performance après coûts restent à évaluer sur des observations futures : aucun alpha n’est déclaré sur la base de ces tests logiciels.

## Sauvegarde, déploiement et retour arrière

Sources et exports avant publication : `/opt/trader-ia/.codex-tmp/performance-remediation-20260914/`. Bases copiées sous verrou de lecture, WAL absent : `/local-files/.codex-tmp/performance-remediation-20260914/backup_live_migration/`. Rejeux et données financières restent privés ; ils ne sont pas versionnés sur GitHub.

Les scripts de maintenance versionnés sont `infra/maintenance/repair_performance_contract.py` et `infra/maintenance/seed_audit_watchlist.py`. Le premier conserve des sauvegardes de lignes et une réception idempotente ; il ne modifie ni NAV, ni cash, ni ordres, ni fills. Les écritures AG1/news utilisent DuckDB 1.4.3.

Retour arrière à exécuter uniquement après contrôle des exécutions et des approbations :

1. Attendre l’arrêt des écrivains et arrêter brièvement le dashboard. Restaurer les fichiers ciblés de `sources_before/` à partir de `source_manifest.json`. Les nouveaux modules peuvent rester présents et inutilisés.
2. Restaurer l’image `ibkr-broker:pre-performance-20260914` sous le nom compose `yfinance-ibkr-broker`, puis recréer seulement `ibkr-broker` avec `docker compose up -d --no-deps ibkr-broker` dans `/docker/yfinance`.
3. Extraire **seulement les sept IDs corrigés** de `workflows_before_publish.json` vers un export de rollback, importer, publier chacun et redémarrer n8n/runners. Ne pas réimporter les 180 workflows de la sauvegarde partagée.
4. Pour les mesures historiques, restaurer seulement les colonnes modifiées depuis `core.portfolio_snapshot_backup_pc20260914`, `core.risk_metrics_backup_pc20260914`, `news_timestamp_backup_pc20260914` et `run_log_backup_pc20260914`, via une maintenance transactionnelle. Ne pas remplacer la base courante par une ancienne copie contenant moins de transactions. Retirer ensuite la réception `performance_contract_20260914_v1` uniquement si une réapplication est prévue.
5. L’ajout à WATCHLIST est indépendant du retour arrière du code. Préserver tout instrument devenu détenu. Désinstaller le cron de couverture si nécessaire, puis vérifier les versions actives, le broker et le dashboard.

## Sources externes utilisées pour les contrats

- [JPX — unités de négociation des actions ordinaires](https://www.jpx.co.jp/english/equities/improvements/unit/index.html). Le principe des 100 titres ne s’étend pas aveuglément aux ETF, REIT et actions étrangères.
- [IBKR — champs de marché](https://ibkrcampus.com/docs/web-api/v1/endpoints/market-data/market-data-fields) : `D` signifie différé ; `Z`/`Y` désignent les données figées. Ce champ ne constitue pas à lui seul un calendrier de séance.
- [IBKR — snapshots et `_updated`](https://ibkrcampus.com/campus/ibkr-api-page/webapi-doc/). Les dates configurées de disponibilité des clôtures d’indices sont conservatrices ; elles ne remplacent pas un horodatage de place certifié.
