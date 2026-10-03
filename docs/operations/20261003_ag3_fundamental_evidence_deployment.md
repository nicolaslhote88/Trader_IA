# AG3 : calculs corrigés, lecture détaillée et fiche AG1

Autorisation du 3 octobre 2026 : « lance les corrections selon ce plan et déploie les mises à jour ».
Périmètre : deux workflows AG3, un nœud d'enrichissement AG1, dashboard et réparation dédiée des derniers relevés AG3.

## Changements

- Les valeurs absentes restent absentes ; les vrais zéros restent zéro. Les champs Yahoo de marge, rendement et croissance utilisent un contrat explicite de ratios décimaux, y compris au-delà de 150 %.
- Version de calcul : `ag3_v2_units_nulls_20261003`. Les objectifs manquants ne sont plus remplacés par des cibles inventées à ±15 %. L'unité dette/capitaux propres est `%`.
- Les 527 derniers relevés sont recalculés depuis leurs snapshots originaux. Triage, proxies du consensus et métriques du même run sont cohérents ; les signatures des métriques sont renouvelées. Les dates source, dates de collecte, identifiants et nombres de lignes sont préservés. Les anciens points gardent leur ancienne méthode et restent consultables.
- La vue détaillée présente cinq dimensions, dix métriques avec leurs absences, couverture utile, âge du relevé, variations d'objectifs à 30/90 jours et graphiques par méthode. Les objectifs analystes apparaissent comme des niveaux, sans trajectoire future ni probabilités non validées. Le risque fondamental est distingué du risque de baisse du cours.
- Une référence sectorielle de triage exige au moins huit autres titres récents, de même méthode et suffisamment couverts. Il s'agit d'un percentile de score dans l'univers disponible, pas d'une valorisation sectorielle économétrique.
- `20L — Fundamental Evidence` enrichit `opportunity_pack.rows[].fundamentals` avant Merge7. La fiche `AG3_EVIDENCE_V1` contient les métriques, sous-scores, forces/faiblesses selon les seuils, lacunes, dates, révisions et source. Le pack comporte une légende commune : pas de probabilités, pas de double comptage du fondamental déjà inclus dans la matrice.
- Source commune dashboard/nœud : `services/dashboard/fundamental_context.py`, embarquée dans le Python natif n8n par `outils/scripts/build_ag3_fundamental_update.py`. Les trois branches modèles reçoivent le pack existant, enrichi sans changer leur transport, modèles ou prompts.
- Aucun poids de matrice, seuil de fraîcheur, cron ni garde broker n'a été modifié. AG3 reste un calcul déterministe, sans nouvel appel LLM. Aucune invocation manuelle du workflow d'ordres.

## Validation

- 527 snapshots réels : ancien code reproduisant les scores stockés sans divergence. Les corrections changent triage ou risque pour 378 titres ; variation absolue maximale de triage 30 points. GOOGL 78→87, ALCBX.PA 34→41, MSFT 74→74. Ces différences ne constituent pas une preuve de rendement futur.
- Réparation complète sur copie physique DuckDB 1.4.3, puis contrôle des dates/identifiants/nombres de lignes ; version finale avec signatures de métriques également rejouée sur une seconde copie.
- 15 assertions JavaScript ciblent valeurs nulles, faux/chaînes vides, vrais zéros, unités extrêmes et objectifs absents. Six tests Python couvrent absence de données, dates, mélange de runs, changement de méthode et références sectorielles.
- Replay de la matrice sur les 364 lignes du contexte réel `22711`, avec les mêmes snapshots fondamentaux avant/après correction et les autres entrées fixées. Entrées : 13→13 ; surveillance : 262→263 ; sorties : 89→88. Les 22 symboles du pack restent les mêmes, leurs gates aussi. Les scores composites évoluent comme attendu. Ce replay ne simule pas des ordres ni les réponses des LLM.
- Fiche complète testée dans les **trois TaskExecutor réels**, avec cas nominal, absent, et pack réel de 22 titres. Le volume sérialisé de la sortie complète du nœud est 79 883 octets, incluant le briefing et les autres champs existants ; ce n'est pas un comptage exact de tokens des prompts.
- Streamlit AppTest dans le conteneur dashboard : 13 métriques, deux graphiques, quatre tableaux, changement de fenêtre 30→90 jours, aucune exception. Cinq tests de parité technique AG1/dashboard et trois tests des gates de données réussis.
- Attente de la fin d'AG2 Watchlist `22722`, réussie à 20:22:26 UTC, avant la maintenance/publication partagée n8n.

## Publication et preuves

Publication vérifiée le 3 octobre après redémarrage :

| Workflow | Version active |
|---|---|
| AG3 Held+Core | `c74b3727-5636-49d1-a663-d22d72b5b1ff` |
| AG3 Watchlist | `c2d6bcea-4d6a-48a0-acac-47b72a493e0a` |
| AG1 V4 | `87de669d-ae38-4c50-9247-492bbf11f0a3` |

177 autres workflows inchangés ; ledger inchangé : 783 runs, 173 ordres, 124 fills, 7 585 snapshots de positions, 783 snapshots de portefeuille, une ligne cash ledger, 63 lots. n8n HTTP 200 ; santé Streamlit interne `ok` ; broker authentifié et compte aligné ; aucune approbation en attente. Le dashboard n'expose pas le port 8501 sur localhost hôte : contrôle effectué à l'intérieur du conteneur. AppTest également rejoué sur les sources et données réellement déployées.

Les versions et empreintes sont consignées dans `20261003_ag3_publication_evidence.json`.
Sauvegardes VPS : `/local-files/.codex-tmp/ag3-remediation-20261003/backup/`.
Preuves locales : `.codex-tmp/ag3-remediation-20261003/`.

Le déploiement vérifie `active=1`, `versionId=activeVersionId`, puis l'égalité des nœuds/connexions publiés avec les candidats validés. Les autres workflows partagés sont comparés à leur état antérieur. Les sept tables du ledger AG1 sont contrôlées par nombre de lignes et empreinte agrégée.

## Retour arrière

Après vérification de l'absence d'exécutions actives : réimporter les trois exports sauvegardés, republier chaque ID, restaurer `dashboard_app.py`, puis redémarrer n8n, les runners 3/4/5 et le dashboard. Les modules ajoutés peuvent rester inutilisés.

La base précédente est `backup/ag3_before_production.duckdb`. Ne pas remplacer globalement la base si de nouveaux runs AG3 sont intervenus : restaurer uniquement les lignes concernées par `repair_plan.json`, depuis la sauvegarde, en gardant les nouveaux runs. Faire cette restauration avec DuckDB 1.4.3 et les writers arrêtés. Le ledger AG1 ne nécessite aucune restauration.

## Limites et suite prédictive

L'évaluation de faisabilité est exécutée, disponible dans `20261003_ag3_predictive_readiness.json` : 228 jours, 21 709 relevés, 527 symboles et aucune période renseignée dans 693 453 lignes de métriques. Cela représente au plus deux fenêtres temporelles non chevauchantes de 90 jours, zéro de 365 jours.

Le protocole est défini : surperformance en rendement total à 90 jours contre un indice sectoriel cohérent, baseline fréquence/logistique contre modèle tabulaire, séparation chronologique par date, purge des horizons chevauchants, calibration séparée et suivi en shadow.

**Aucun modèle prédictif n'est entraîné ni mis en décision.** Il manque les comptes avec périodes et dates de publication connues, leurs révisions, les séries de prix/dividendes ajustées et plusieurs périodes de marché indépendantes. La collecte de ces historiques reste un chantier distinct ; les dates comptables inconnues sont affichées comme telles. Les probabilités à douze mois ne peuvent pas être validées avec ce seul registre.

La fiche ajoutée est factuelle et déterministe, sans synthèse LLM supplémentaire. Son utilité décisionnelle et le coût en tokens devront être observés sur les prochains runs AG1 ; aucun gain de performance financière n'est revendiqué.

## Reproduction

```powershell
python -m unittest discover -s tests -p test_fundamental_context.py
node tests/test_ag3_fundamental_score.cjs
python outils/scripts/build_ag3_fundamental_update.py --exports <exports-publies> --output <candidats>
```

Le script de réparation n'écrit qu'avec `--apply` et une sauvegarde explicite. Son mode `--export` et l'évaluation prédictive ouvrent les bases en lecture seule. Ne pas exécuter le writer avec la version Python/DuckDB locale si elle n'est pas 1.4.3.
