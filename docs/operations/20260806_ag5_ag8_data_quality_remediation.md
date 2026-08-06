# Remédiation qualité des données AG5–AG8

Date : 2026-08-06

État : déployé live
Périmètre : AG5, AG6, AG7, AG8 et synthèse consultative AG1. AG9 reste dormant.

## Symptôme

Le snapshot `GC_20260806T063028Z_963de37c` était récent mais `DEGRADED` :
couverture `0,584`, confiance `0,400`, fraîcheur `missing`. AG5 et AG6 ne
fournissaient aucune ligne exploitable pour EUR/JPY/KRW/USD et AG8 seulement
une. Le workflow n8n restait vert parce que son validateur acceptait aussi
`DEGRADED`.

## Causes validées

1. AG5 lisait `value` dans les lignes de taux directeur alors que le contrat
   brut expose `rate_pct` : la politique monétaire était donc absente du score.
2. Plusieurs séries FRED de PIB sont des niveaux. Elles étaient traitées comme
   des taux de croissance, produisant par exemple des valeurs de plusieurs
   centaines de milliers au lieu de pourcentages.
3. La Banque mondiale n'alimentait que deux indicateurs, dont un identifiant
   fiscal vide. PPP, REER, termes de l'échange, chômage et fallbacks annuels
   n'étaient jamais construits.
4. Les mêmes seuils de fraîcheur courts étaient appliqués aux séries
   quotidiennes, mensuelles et annuelles. Une observation officielle mensuelle
   ou annuelle devenait artificiellement périmée.
5. AG8 déclarait la liquidité toujours absente et la moindre métrique optionnelle
   manquante imposait `missing` à toute la ligne.
6. La synthèse utilisait le pire statut de toutes les lignes, attribuait une
   couverture artificielle de `1,0` à AG7 et ne remontait pas les causes internes
   de dégradation.
7. Avec AG9 dormant, le pack annonçait à tort
   `NO_RELIABLE_EXPOSURE_MAPPING` pour chaque actif au lieu d'indiquer que cette
   évaluation n'était pas applicable.

## Corrections

- AG5 `AG5_MACRO_V3` : lecture explicite de `rate_pct`, conversion cohérente
  des niveaux PIB en croissance QoQ annualisée, contrôles de plausibilité et
  fallback annuel Banque mondiale avec provenance.
- Banque mondiale : PIB, inflation, chômage et variation du chômage pour 12/12
  devises ; PPP/REER/termes de l'échange dérivés avec direction documentée ;
  indicateur fiscal remplacé par `GC.NLD.TOTL.GD.ZS`. L'Allemagne est un proxy
  explicite uniquement pour les métriques structurelles EUR indisponibles au
  niveau agrégé EMU.
- AG6 `AG6_FX_VALUATION_V3` : PPP, REER et termes de l'échange réellement
  alimentés ; les données structurelles annuelles conservent une confiance
  inférieure aux observations rapides.
- AG8 `AG8_RATES_V3` : fraîcheur dépendante de la fréquence, courbes proxy
  explicitement marquées, inflation annuelle de secours et proxy global de
  liquidité via le NFCI FRED.
- Collecte : retries bornés FRED/World Bank ; les erreurs par série sont
  persistées comme warnings sans effacer les dernières observations valides.
- Qualité : agrégation par ratio de lignes utilisables et seuils explicites de
  couverture/confiance. Une ligne optionnelle absente ne détruit plus tout le
  composant.
- n8n : AG5–AG8 échouent maintenant explicitement si l'API retourne
  `DEGRADED`. La synthèse accepte encore ce statut afin de publier un contexte
  prudent et traçable.
- Synthèse `GLOBAL_CONTEXT_SYNTHESIS_V2`, pack
  `GLOBAL_CONTEXT_LLM_COMPACTION_V3` : couverture AG7 honnête (`9/12`),
  agrégation de fraîcheur robuste, seuil de détail `0,4`, exposition AG9
  marquée `AG9_DORMANT_EXPOSURE_MAPPING_NOT_EVALUATED`.

## Validation

### Tests locaux

`41 passed` sur les contrats macro, transformations, synthèse, sécurité des
workflows, replay AG1 et dashboard.

### Shadow VPS isolé

Le shadow a utilisé des copies de `macro_data.duckdb` et
`global_context_v1.duckdb`, des conteneurs distincts et aucun transport
d'ordre.

| Composant | Statut | Lignes | Couverture | Confiance | Lignes utilisables |
|---|---:|---:|---:|---:|---:|
| AG5 | OK | 12 | 0,975 | 0,585 | 1,000 |
| AG6 | OK | 12 | 0,975 | 0,584 | 1,000 |
| AG7 | OK | 9 | 1,000 producteur / 0,750 synthèse | 0,867 | 1,000 |
| AG8 | OK | 12 | 0,950 | 0,677 | 1,000 |

Synthèse shadow : couverture `0,908`, confiance `0,685`, statut `OK`,
fraîcheur `aging`. Un pack représentatif EUR/JPY/KRW/USD contient toutes les
lignes macro, valorisation et taux utilisables, mesure 3 775 caractères et
reste `CAUTION` parce que plusieurs sources sont structurelles/annuelles.

### Live

- snapshots composants :
  - AG5 `AG5_20260806T080209Z_a7d236d6` ;
  - AG6 `AG6_20260806T080220Z_4027fca1` ;
  - AG7 `AG7_20260806T080227Z_b9865314` ;
  - AG8 `AG8_20260806T080227Z_4d6f06d6` ;
- snapshot synthèse initial : `GC_20260806T080228Z_f8de89ab` ;
- synthèse naturelle n8n 10:05 Paris : exécution `20799`, succès, dernier
  snapshot `GC_20260806T080500Z_7a0ef250` ;
- pack live représentatif : statut `OK`, `use_policy=CAUTION`, couverture
  `0,908`, confiance `0,685`, tous les détails pertinents inclus ;
- services healthy, dashboard HTTP 200, broker authentifié et aligné, aucune
  approbation en attente, aucun ordre créé par le déploiement ;
- Forex trading reste désactivé et AG9 reste dormant.

## Versions n8n publiées

| Workflow | `activeVersionId` |
|---|---|
| AG5 | `cbfb4a7d-0847-4d0a-9843-2f5123e9bb0b` |
| AG6 | `97e8cc91-d751-4bbc-9d4f-e533ea54c1de` |
| AG7 | `4ee7261e-d662-461d-b08a-bbd74f07a2d7` |
| AG8 | `6f971c00-eee9-499c-a69a-bbd5fa6e018c` |
| Synthèse | `a6e22dce-804b-4624-ad68-0e3f5ec56a54` |

## Sauvegardes et rollback

Sauvegarde :
`/opt/trader-ia/backups/ag5-ag8-data-quality-20260806T0755Z`.
Archive de release : `ag5-ag8-data-quality-release.tar.gz`, SHA-256
`95b005444095e9245c9d0cc8537ef65ed7b2d6713f521adea55d5ff53b9a1371`.

Release :
`/opt/trader-ia/releases/ag5-ag8-data-quality-20260806`.

Rollback :

1. restaurer `docker-compose.before.yml` dans `/docker/root/docker-compose.yml` ;
2. reconstruire/recréer uniquement `macro-data-api` et
   `global-context-synthesizer` ;
3. réimporter les cinq fichiers `workflows/*.before.json`, republier leurs IDs
   puis redémarrer n8n et les trois runners ;
4. ne restaurer les bases sauvegardées que si une corruption est démontrée :
   les nouveaux snapshots sont additifs et les tables sources restent
   compatibles ;
5. recontrôler `/health`, approbations, AG9 dormant, Forex désactivé et absence
   d'ordre inattendu.

## Limites assumées

- `aging` est normal tant que des métriques annuelles officielles participent
  au contexte ; elles restent identifiées comme telles et n'ont pas la
  confiance d'une observation rapide.
- AG7 couvre neuf devises sur douze ; la synthèse publie désormais `0,75` au
  lieu de prétendre une couverture complète.
- AG9 et les expositions géopolitiques restent volontairement non évalués.
