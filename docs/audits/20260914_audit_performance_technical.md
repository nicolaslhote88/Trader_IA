# Audit technique du calcul de performance et des entrées AG1

> État avant corrections. Les défauts établis ont ensuite été corrigés et déployés : [résultat, validation et limites](../operations/20260914_performance_contract_remediation.md). Les propositions de recherche restent à évaluer.

Audit du 14 septembre 2026. Cette note décrit des contrats logiciels et leur validation. Les séries financières, positions, propositions détaillées et extractions de production restent dans les livrables locaux, hors de ce dépôt public. Aucun ordre ni changement de stratégie en production n’a été réalisé pour l’audit.

L’architecture permet de mettre en œuvre une stratégie multi-piliers, mais ses tests logiciels ne prouvent pas un avantage d’investissement après coûts. Les priorités sont la cohérence des données de décision, la qualité de la mesure et un protocole d’évaluation hors échantillon. Les pondérations et la fréquence d’appel des modèles ne doivent pas être optimisées sur les seuls gagnants observés après coup.

## Constats validés

### 1. Le pack compact perd des informations nécessaires aux consignes de gestion

Sources :

- [Assemblage des packs](../../agents/trading-actions/AG1%20-%20Portfolio%20manager/AG1-V4-Consensus%20Portfolio%20manager/workflow/nodes/agent_input/ag1_00_assemble_input_packs.code.js).
- [Enrichissement du portefeuille](../../agents/trading-actions/AG1%20-%20Portfolio%20manager/AG1-V4-Consensus%20Portfolio%20manager/workflow/nodes/pre_agent/4C_enrich_portfolio_with_market_prices.code.py).

`4C` produit notamment prix moyen, performance locale, ancienneté et cycle de vie. La projection `positions.map(...)` du pack compact omet ces informations, alors que les consignes des modèles demandent de vérifier la durée de détention et les exceptions liées au stop avant de réduire une position.

Le défaut a été confirmé dans une entrée réellement transmise aux modèles, et pas seulement par lecture d’un fichier local. La dernière action de décision n’est pas une date de dernière exécution et ne permet pas d’inférer l’ancienneté.

**Correction à préparer :** contrat explicite avec devise native, prix moyen natif/EUR, provenance du taux, date d’ouverture du lot encore détenu, dernier achat/vente exécuté, durée, performance locale/EUR et stop applicable. Distinguer stop initial et plan de risque d’une décision ultérieure ; ne pas recopier une valeur sans sa provenance.

**Réception :** cas d’achat, renforcement, vente partielle, fermeture/réouverture et données inconnues ; vérification jusqu’au prompt final. Aucune information utile ne doit exister seulement dans un objet qui n’est pas transmis au modèle.

### 2. Score signé AG2 utilisé comme confiance sur 100

Sources :

- [Calcul technique AG2](../../agents/trading-actions/AG2%20-%20La%20technique/AG2-V3/nodes/04_compute.py), fonction de signal.
- R8 et Calcul Matrice dans le [workflow AG1 V4](../../agents/trading-actions/AG1%20-%20Portfolio%20manager/AG1-V4-Consensus%20Portfolio%20manager/workflow/AG1_workflow_v4_consensus.json).

Le producteur définit `d1_score` sur −6 à +6 et `d1_confidence` sur 0 à 100, avec `abs(score) / 6 * 100`. R8 transmet `d1_score` sous `Tech_Confidence`. La matrice consomme ensuite `(tech_conf - 50) * 0.20`.

La confusion d’unités existe dans le workflow publié. Un rejeu hors production à entrées et horodatage constants a confirmé qu’une substitution de l’unité modifie des classements. Ce test mesure une sensibilité du calcul ; il ne démontre aucun gain de portefeuille.

**Correction à préparer :** champs distincts, domaines documentés et cohérence avec le sens du signal. Il faut aussi vérifier qu’une forte confiance baissière n’augmente pas indûment une estimation de hausse. Respecter la [parité dashboard/AG1](../operations/SYSTEM_LINKS_AND_PARITY.md) avant toute modification des formules.

**Réception :** valeurs négatives/positives extrêmes, zéro, confiance absente, signal vendeur fortement confirmé ; rejeux de plusieurs dates et diff des actions. Une modification de seuil relatif peut changer les candidats sans augmenter le nombre d’entrées.

### 3. La faisabilité d’achat est ambiguë sur les lignes de sortie

Le préflight enrichit aussi les lignes détenues « Réduire / Sortir » avec un `execution_constraints` relatif à un achat minimal : cash, ticket et plafond d’une position après achat. L’indicateur générique `feasible=false` peut ainsi être interprété comme une impossibilité de vendre.

Le validateur distingue déjà plusieurs contrôles BUY/SELL : le défaut d’information ne démontre pas que les ventes sont systématiquement bloquées à l’exécution.

**Correction à préparer :** `buy_feasibility` et `sell_feasibility`, avec des motifs propres à chaque sens. Une réallocation doit séparer vente proposée, produit attendu et cash confirmé après fill.

**Réception :** sortie détenue avec cash nul, vente partielle, absence de quantité disponible, limites d’achat et produit de vente non encore disponible.

### 4. Le benchmark américain n’est pas converti en EUR

Source : [dashboard](../../services/dashboard/app.py), `DEFAULT_BENCHMARKS`, `fetch_benchmarks_history` et rendu comparatif. L’indice `^GSPC` est rebasé à partir de sa clôture USD, sans conversion préalable vers la devise du portefeuille.

Les références actuelles sont des indices de prix. La date commune ne garantit pas non plus une heure de valorisation identique entre NAV et clôture d’indice.

**Correction à préparer :** contrat benchmark avec devise, variante price/net/gross, source et heure. Conversion datée, alignement temporel et distinction apports/retraits. Le rendement économique doit distinguer courtage et coûts IA/infrastructure.

**Réception :** prix USD constant et change variable, dates manquantes, jours fériés différents, apports/retraits, dividendes et provenance de chaque observation. Éviter tout recalcul de rendement qui transforme un flux externe en gain.

Référence méthodologique : [MSCI, Index Calculation Methodology, février 2026](https://www.msci.com/indexes/documents/methodology/0_MSCI_Index_Calculation_Methodology_20260210.pdf).

### 5. Plusieurs champs de risque PF sont des valeurs de remplacement

Source : [réconciliateur PF](../../agents/trading-actions/AG1%20-%20Portfolio%20manager/AG1-PF-V1/nodes/00c_reconcile_ibkr_ledger.py), écriture de `core.portfolio_snapshot` et `core.risk_metrics`.

Le réconciliateur écrit un drawdown et un coût IA nuls, un statut `BALANCED` constant, et utilise la plus grosse position comme `top1_sector_pct`. La VaR repose sur une volatilité fixe, sans estimation des covariances. Ces colonnes ne doivent pas être interprétées comme des mesures exhaustives du risque. Certains consommateurs recalculent leurs propres indicateurs : il faut tracer chaque usage avant correction.

Le validateur préordre applique un booléen de kill switch ; la présence du paramètre `max_daily_drawdown_pct` ne suffit pas à prouver un coupe-circuit automatique fondé sur ce seuil. Le writer distingue imparfaitement recul depuis un sommet et recul quotidien. Le plan de risque d’une décision n’est pas à lui seul une preuve de dépôt d’un ordre protecteur chez le courtier.

**Correction à préparer :** calcul ou valeur inconnue explicite, agrégation des secteurs, définition du drawdown, source/provenance des coûts et qualification de la VaR. Documenter séparément protection indicative, contrôle préordre, surveillance périodique et ordre conditionnel courtier. Toute évolution du comportement d’exécution exige une décision explicite.

### 6. Observabilité, fraîcheur et sources datées

La version publiée d’AG1 contient un déclenchement quotidien à 16 h 30 Paris, alors que le fichier local décrit encore deux créneaux. Une différence de configuration du merge du pack consultatif existe aussi. Aucune cause ni conséquence de cette seconde différence n’est présumée. `activeVersionId` doit rester la source de preuve du comportement réellement publié.

Le dernier passage AG2 Held+Core programmé à 15 h Paris précède l’ouverture régulière américaine. Un prix préflight récent ne garantit donc pas une analyse technique de la séance US en cours. Toute évolution de cron doit respecter [SCHEDULING_AND_LOAD](../operations/SCHEDULING_AND_LOAD.md), et être motivée par la stratégie.

Des news IBKR portent une analyse antérieure à leur publication enregistrée, avec un écart pouvant approcher deux heures. L’incohérence temporelle est confirmée ; l’origine précise du fuseau ne l’est pas. Conserver la valeur brute du fournisseur, publication/réception/analyse et règles de normalisation avant de réparer. Ne pas appliquer un décalage constant à toute la table sans preuve.

Des runs anciens restent `RUNNING` alors que les producteurs récents fonctionnent. Un statut `PARTIAL` d’enrichissement ne signifie pas absence de quotes sur tout le lot. Les taux de couverture et la fraîcheur doivent être mesurés par champ, segment et place.

## Limites de conception à évaluer

La matrice transforme un score heuristique en `p_win`, puis en espérance `EV_R`. Aucune calibration empirique de cette probabilité à un horizon et une cible définis n’a été identifiée. Un grade relatif n’est pas une probabilité de gain et une formule d’espérance ne constitue pas un résultat de backtest.

Les signaux de tendance et de retour à la moyenne peuvent s’opposer. La combinaison de supports techniques courts et de cibles d’analystes plus longues demande une définition d’horizon. Le classement des nouvelles entrées doit être distingué de la décision de conserver une position existante.

Le pack limite les entrées avant le préflight IBKR, sans remplacement systématique par les candidats suivants après rejet. L’univers, les segments, la quarantaine, la place autorisée, le lot, la devise et le coût minimal doivent être pris en compte avant les étapes d’analyse coûteuses. Un titre absent de l’univers ne peut pas être sélectionné par l’ajout d’un pilier.

## Validation et prochain lot

L’audit a comparé le code local à la version publiée, examiné les sorties intermédiaires disponibles et rejoué la matrice sur une entrée datée. Les extractions live sont en lecture seule. La rétention n8n limitée empêche de reconstituer tous les nœuds de chaque ancien run ; les journaux de décisions ne compensent pas entièrement cette limite.

Priorité proposée : contrat de position, unités techniques, faisabilité par sens, puis mesures de performance/risque et réconciliation comptable. Chaque changement doit être testé, rejoué et déployé séparément avec sauvegarde et contrôle de publication. Cette PR est documentaire et ne publie aucun correctif de stratégie.

Pour évaluer l’avantage d’investissement : définir à l’avance une référence principale, un horizon et un budget de risque ; comparer moteur actuel gelé, référence passive et challenger simple. Tester ensuite l’apport marginal de chaque pilier et du consensus. Inclure coûts, lots, spread, change, annonces hors séance, ordres partiels et données connues à la date du signal.

Les variantes ne doivent pas être sélectionnées sur le seul rendement maximal d’un passé réutilisé de nombreuses fois. Référence : [Bailey et al., The Probability of Backtest Overfitting, 2015](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf). Une phase shadow démontre d’abord un fonctionnement opérationnel ; elle ne prouve pas à elle seule un alpha durable.
