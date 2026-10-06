# Audit de l’entonnoir de tradabilité — 6 octobre 2026

## Périmètre et méthode

Audit de l’état réel du VPS, avec lectures DuckDB `read_only=True`, versions
publiées n8n et santé broker. Dénominateur conservé : **364 actions/ETF activés,
hors quarantaine**, tous déjà segmentés, sur 566 symboles activés dont 488
compatibles avec les ordres actions/ETF. Aucun déplacement en quarantaine,
aucune suppression de titre pour améliorer artificiellement le pourcentage.

La pré-tradabilité signifie : segment actif, statuts H1/D1 OK, bougies closes,
âge effectif H1/D1 ≤96 h, quote positive/OK âgée de ≤72 h, spread observé ou
volume ≥5 000, et décision AG2 différente de REJECT. Ce n’est ni un signal
« acheter », ni une preuve de permissions ou de liquidité IBKR au moment d’un
ordre. Le scoring matrice, les événements, le cash, le sizing et le préflight
restent des étapes ultérieures. Un SKIP reste neutre selon le contrat existant.

## Faits validés avant intervention

À 07:57 Paris : technique réellement prête **39/364**, puis quote/liquidité
**35/364**, puis pré-tradables **23/364**. Le dashboard annonçait 217 : son calcul
reposait sur l’âge du workflow, alors que R8 utilise la date des bougies.
L’affichage de 217 était donc une surestimation, pas le point de départ réel.

Pertes non exclusives : 324 D1 dépassaient 96 h, 73 verdicts REJECT, 32 défauts
de liquidité, 13 H1 au-delà de 96 h, 5 quotes absentes. Les statuts de calcul
incluaient aussi 37 D1 STALE et 10 D1 INSUFFICIENT_DATA. Les 364 fondamentaux
avaient été collectés dans les 168 h ; cela ne garantit pas que tous leurs
champs soient présents ou de qualité égale.

## Causes corrigées

1. **Cadence insuffisante et couverture CORE incomplète.** L’ancienne rotation
   Watchlist de 2 × 40/j devait parcourir 288 titres, en plus d’une rotation
   Held+Core de petits lots. La nouvelle couverture parcourt tout le périmètre
   hors quarantaine par dernière tentative croissante : 5 × 80/j. Le contrôle
   sur copie a couvert 80, 160, 240, 320 puis 364 symboles distincts, sans FX ni
   quarantaine. Les revues Held+Core restent en place.
2. **Caches incomplets jamais reconstruits.** TEAM et PLTR restaient à 118 D1,
   BNP à 189. Une bougie récente faisait considérer le cache comme suffisant,
   puis les mises à jour incrémentales n’ajoutaient que les derniers jours.
   Le service recharge désormais la fenêtre demandée si les barres validées
   sont insuffisantes, au plus une tentative de backfill par jour. Validation
   réelle sur copie : TEAM 275, PLTR 275, BNP 278 barres valides.
3. **Tickers de données obsolètes.** Deux transferts de marché à ISIN constant
   ont été vérifiés : PVL.PA→ALPVL.PA et LHYFE.PA→ALHYF.PA. Alias au niveau du
   fournisseur, identités internes et broker conservées ; refresh producteur
   YF réussi pour les deux. Sources primaires dans la note de déploiement.
4. **Clôtures Yahoo manquantes en Europe et secours IBKR cassé.** Le premier
   rattrapage a donné 104 techniques prêtes et 74 pré-tradables, mais 257 statuts
   D1 STALE. Les appels directs ont confirmé l’absence de Close chez Yahoo,
   y compris hors cache. Le broker appelait `/hmds/history` (404) ; l’endpoint
   officiel `/iserver/marketdata/history` a fourni des bougies réelles complètes.
   Un repli D1 limité aux sept derniers jours a été ajouté : symbole exact,
   devise attendue, résolution stricte de place existante, deux séances communes
   au minimum et écart OHLC maximum de 2 % sur les séances communes. Une bougie
   Yahoo valide n’est jamais remplacée ; l’intégralité de la bougie de secours
   vient d’IBKR, séance régulière et clôture +10 min. Ni interpolation, ni
   récupération d’un simple dernier cours pour inventer une clôture.
5. **Mesure trompeuse.** Les deux entonnoirs dashboard utilisent désormais la
   date H1/D1, avec maximum de l’âge stocké et de l’âge réel, comme AG1.

## Résultat après rattrapage

Mesure finale **09:24 Paris** (07:24:28 UTC), après la fin des écritures de
rattrapage et du cron Held+Core de 09:00 :

| Étape, sur le même univers de 364 titres | Avant | Après |
|---|---:|---:|
| Segment actif hors quarantaine | 364 | 364 |
| Technique conforme | 39 | **324** |
| Technique + quote/liquidité | 35 | **302** |
| Pré-tradable, REJECT exclus | 23 | **272** |
| Taux de pré-tradabilité | 6,3 % | **74,7 %** |

Gain : **249 valeurs**, soit **×11,8** et **+68,4 points**. La conformité
technique atteint 89,0 %. Le stock d’univers, les segments et les quarantaines
ont été comparés aux sauvegardes : mêmes lignes et valeurs métier.

Les **92 exclusions restantes**, comptées une seule fois dans l’ordre du
funnel, sont : **40** défauts techniques/de données, puis **22** défauts de
quote/liquidité, puis **30** REJECT IA. Les motifs détaillés par symbole sont
dans la [preuve JSON](evidence/20261006_tradability.json). Les motifs individuels
se recouvrent : ils ne doivent pas être additionnés comme des pertes exclusives.

Le secours a complété 218 historiques lors du rattrapage ciblé. Il a refusé
25 divergences de prix et 8 contrats non résolus ; un historique est resté
indisponible. Les autres cas correspondent à une séance non encore close, un
historique déjà suffisant ou un périmètre non admissible.

Les deux calculs dashboard et le vrai code R8 publié ont été rejoués :
**zéro divergence**, sur 566 lignes dashboard et 364 titres du périmètre R8.
L’étape technique « sans rejet IA » vaut 294 avant le filtre quote/liquidité ;
elle ne doit pas être confondue avec les 272 pré-tradables du funnel complet.

## Limites conservées et décisions

- Les bougies Yahoo réellement invalides/manquantes ne sont pas reconstruites
  à partir de prix inventés. Le secours IBKR ne les remplace que si tous ses
  contrôles passent ; sinon la valeur conserve son exclusion et son motif.
- REJECT est un verdict d’analyse, pas une panne de collecte. Le modèle, le
  prompt, le cache, le poids et le filtre existants n’ont pas été assouplis.
- Une valeur dont le volume est trop faible sans spread exploitable reste
  exclue. Exemple PVL : réparer la quote ne suffit pas si le volume vaut 4 729.
- ROG.SW→ROP.SW correspond à un changement d’instrument/ISIN ; ABB→ABBNY à un
  changement de place, tandis qu’IBKR résout l’ancien ABB en un autre contrat
  EUR. Aucun alias de prix aveugle n’a été ajouté. Ces migrations nécessitent
  une opération d’identité complète avec historique, places et contrat.
- EIFF.PA : cotation indisponible et situation d’offre/retrait à clarifier ;
  aucun remplacement arbitraire. Les quarantaines/overrides existants sont
  préservés. Ces cas restent explicitement visibles, sans hausse artificielle
  du taux d’éligibilité.

## Hypothèses et contrôles à poursuivre

Le repli IBKR ajoute une dépendance à sa session authentifiée et à la disponibilité
des historiques. Une panne laisse une indisponibilité explicite ; le cache de secours
est séparé, limité à 15 min pour le téléchargement, et les dates de séance restent
revalidées à chaque réponse. Les prix et volumes peuvent différer entre places et
fournisseurs ; le contrôle d’historique commun limite ce risque sans l’annuler.

La capacité nominale de 400/j doit maintenir les 364 titres couverts si les
runs aboutissent. La durée estimée d’un lot n8n de 80 est de 35 à 60 min : 35 min à
partir des anciens lots de 40 en 17 min, marge supplémentaire pour le secours IBKR. Ni le coût IA quotidien futur,
ni la stabilité sur plusieurs séances, ni un gain de performance financière
ne sont démontrés par ce rattrapage. Observer le premier cron Coverage 10:10,
puis un cycle complet ; les preuves actuelles sont le replay, la publication
et le rattrapage réel, détaillés dans la note liée.

[Déploiement, tests, versions, preuves et rollback](../operations/20261006_tradability_deployment.md).
