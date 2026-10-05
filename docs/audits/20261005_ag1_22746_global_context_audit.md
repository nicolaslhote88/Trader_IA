# Audit du run AG1 V4 22746 — contexte AG5–AG9

Audit du 5 octobre 2026, en lecture seule sur le VPS. Horaires exprimés en Europe/Paris.

## Conclusion

L'impression est confirmée pour ce run : AG5–AG8 n'ont fourni aucun détail utilisable aux modèles. La synthèse reçue était périmée et portait `use_policy=IGNORE`. Les trois modèles déclarent explicitement l'avoir ignorée. AG9 était volontairement désactivé.

La cause principale est un défaut de disponibilité du contexte pour les lancements matinaux : les producteurs ont tourné ce matin, mais la synthèse n'est programmée qu'à 10:05, 13:05 et 16:05. Le run manuel de 09:18 a donc récupéré la synthèse de vendredi. L'endpoint `/ag1-pack` lit le dernier snapshot publié sans recalculer la synthèse.

Il existe aussi une dégradation progressive de la couverture des sources et une remontée insuffisante de cet état dans le bilan du run. Ce constat ne permet pas de conclure que les agents sont durablement inutiles : 28 des 30 derniers runs AG1 persistés avaient un contexte `OK / CAUTION`.

## 1. Exécution auditée et résultat métier

- n8n : **22746**, `mode=manual`, début **09:18:18.304**, fin **09:23:05.384**, durée **287,080 s**, statut `success`.
- Ledger : `RUN_20261005_091823_22746`, modèle `ag1_v4_consensus`.
- Publication AG1 constatée pendant l'audit : `active=1`, `versionId=activeVersionId=1555b344-4673-4f1c-ad10-081c9bb345ca`. L'analyse des entrées/sorties repose sur la définition enregistrée avec l'exécution, pas sur la seule édition courante.
- Trois réponses valides : Claude Opus 5.5, DeepSeek V4 Pro, GPT-6 Sol ; extracteurs `OK_OBJECT`.
- Aucun rejet sécurité ou broker dans le bilan de ce run. `health_ok=true`, `mtm_sync_ok=true`.
- Broker authentifié sur le compte réel configuré ; aucune approbation en attente au contrôle.

| Décision | Votes | Résultat observé |
|---|---|---|
| Ouvrir TKO.PA | Claude + GPT, 2/3 | Achat de **66 titres à 15,74 EUR**, fill confirmé, **3 EUR** de frais |
| Fermer GOOGL | Claude + DeepSeek, 2/3 | Vente MARKET de **2 titres**, broker `PreSubmitted`, ledger `SUBMITTED`, aucun fill dans ce run au contrôle |
| Renforcer MAU.PA | DeepSeek seul | `NO_CONSENSUS` |
| Réduire TXN | DeepSeek seul | `NO_CONSENSUS` |

La vente GOOGL ne doit pas être assimilée à une vente exécutée. L'achat TKO.PA a été financé sur le cash confirmé ; les produits de vente non confirmés n'ont pas été utilisés. Aucun ordre n'a été créé, confirmé, annulé ou relancé par l'audit.

## 2. Preuve de l'exclusion AG5–AG8

Le nœud `AG1.GC — Fetch Advisory Pack` a retourné :

```json
{
  "snapshot_id": "GC_20261002T140500Z_5d53c8f9",
  "as_of": "2026-10-02T14:05:00.087915+00:00",
  "status": "GLOBAL_CONTEXT_STALE",
  "use_policy": "IGNORE",
  "quality": {
    "source_freshness": "stale",
    "coverage_ratio": 0.813,
    "confidence": 0.548,
    "snapshot_age_hours": 65.231
  }
}
```

- Limite effective du snapshot : **12 heures**.
- Macro, valorisation FX, positionnement et taux/liquidité : `details=OMITTED` pour les quatre composants ; absence de `currency_signals`.
- Devises pertinentes : EUR, JPY, USD. Les anciennes lignes correspondantes apparaissent encore `usable_rows=3` dans les résumés, mais cela ne lève pas l'exclusion globale pour ancienneté du snapshot.
- Identifiant et hash identiques retrouvés dans les entrées enregistrées GPT et DeepSeek ainsi que dans le corps HTTP préparé pour Claude.
- Hash du pack : `4fb6111b9f50a0e3211142c3b11ba215c4a8ea77b7012eed71635a0e0a733ff0`.
- Le prompt ordonne de traiter `IGNORE` comme un bloc absent et de signaler seulement son indisponibilité.
- Claude : « The global context AG5-AG9 is stale (use_policy IGNORE, AG9 dormant) and was not used. »
- DeepSeek : « Contexte global AG5-AG9 stale avec use_policy=IGNORE, donc ignore. »
- GPT : « Contexte mondial AG5-AG9 périmé, use_policy IGNORE : non utilisé dans les décisions. »

L'exclusion est donc attestée par le contrat transmis et les réponses finales. L'effet causal qu'aurait eu un contexte frais sur les ordres n'a pas été testé.

## 3. Producteurs de ce matin et reconstruction sans écriture

| Agent | n8n | Production | Lignes | Couverture | Confiance | Statut producteur |
|---|---:|---|---:|---:|---:|---|
| AG5 Macro | 22737 | 07:20 | 12 | 82,2 % | 50,4 % | OK |
| AG6 Valorisation FX | 22738 | 07:40 | 12 | 87,5 % | 51,9 % | OK |
| AG7 Positionnement | 22739 | 08:00 | 9 | 75,0 % de l'univers de 12 | 86,7 % | OK |
| AG8 Taux/liquidité | 22741 | 08:20 | 12 | 83,3 % | 53,2 % | OK |
| AG9 Risque global | — | Désactivé | 0 | Non applicable | Non applicable | DISABLED |

AG7 annonce 100 % dans son contrat producteur, puis la synthèse ajuste correctement à 9/12 = 75 %. Cela n'est pas une perte de données introduite par AG1.

Reconstruction effectuée avec `synthesizer.synthesize()` puis `advisory_pack_for_run()` dans le conteneur live, à l'instant simulé 09:18:51. Toutes les connexions source utilisent `read_only=True`. Aucun appel à `/synthesize`, aucun `publish`, aucun LLM et aucun transport d'ordre.

Le test porte sur les mêmes devises pertinentes EUR/JPY/USD, avec un portefeuille représentatif de trois lignes ; il ne rejoue pas les décisions ni le portefeuille exact. Résultat : **OK / CAUTION**, couverture **81,3 %**, confiance **61,0 %**, les quatre familles de détails incluses. Les snapshots producteurs utilisés sont exactement ceux des quatre exécutions matinales ci-dessus.

Cela prouve que des données exploitables existaient déjà avant le run. Cela ne prouve pas qu'elles auraient amélioré sa performance.

## 4. Problèmes validés, par priorité

**P1 — Synthèse non rafraîchie pour les runs matinaux.**
La dernière synthèse n8n était l'exécution **22707**, vendredi 2 octobre à **16:05**. Cron publié : `5 10,13,16 * * 1-5`, timezone `Europe/Paris`. Le lancement manuel de lundi 09:18 survient avant la première synthèse. Le même symptôme existe le 25 septembre à 08:15 : âge 16,184 h et `IGNORE`. Le cron AG1 habituel de 17:10 n'a pas ce décalage dans l'échantillon récent.

**P2 — Dégradation de qualité insuffisamment visible.**
Le run est `success`, `data_ok_for_trading=true`, `warnings_json=[]`, sans ligne `core.alerts`, et le post-run indique `health_ok=true`. Les limites sont pourtant présentes dans les `dataCaveats` des modèles et dans les colonnes dédiées au contexte. Le statut technique est cohérent avec un composant consultatif, mais ne résume pas la qualité effective de la décision.

**P2 — Couverture des sources en baisse.**
Dans les 30 derniers runs persistés, la couverture passe de **91,3 %** début septembre à **81,3 %** fin septembre/début octobre. Les composants continuent de passer leurs seuils minimaux ; `OK` ne signifie donc pas exhaustivité.

- AG5 : inflation/politique monétaire/taux réel absents ou périmés pour CHF, KRW, NZD et SEK ; compte courant EUR absent ; fiscal JPY absent. Plusieurs variables de croissance/inflation/chômage reposent sur des proxys annuels.
- AG6 : carry et real carry absents/périmés pour CHF, KRW, NZD et SEK ; REER également pour KRW.
- AG8 : taux directeur périmé pour ces mêmes quatre devises ; variation de pente absente pour 10/12 devises ; liquidité via proxy global et plusieurs inflations annuelles de secours.
- Table brute des taux : dernières observations CHF/NZD au **22 mai**, SEK au **13 mai**, KRW au **1er juin**. Les observations EUR et GBP sont récentes. Ces dates décrivent ce qui est stocké ; la cause amont de chaque série non rafraîchie reste à diagnostiquer.
- AG7 : neuf devises couvertes, USD explicitement proxy ; rapports utilisés datés du **29 septembre**, donc pas de panne générale du positionnement.

**État intentionnel — AG9 dormant.**
Workflow `AG9GLOBALRISK20260805` désactivé, aucune version publiée, exclu des composants actifs de la synthèse. Les expositions de 8 positions et 25 opportunités sont marquées `not_evaluated`, pas « risque nul ». L'absence d'apport géopolitique est donc structurelle et connue ; la réactivation demande un projet distinct de collecte, validation et qualification des expositions.

## 5. Autres observations sur le run

- Les modèles signalent des news générales âgées d'environ **65 h** et des données H1/D1 souvent âgées de **60–79 h**. Le prix d'entrée TKO.PA provient toutefois d'une cotation IBKR récente. Ancienneté des analyses et fraîcheur du prix d'exécution sont deux contrôles différents.
- Préflight : UNP porte `SPREAD_UNQUOTED|STALE_QUOTE`, TXN `LIQUIDITY_STRESS` avec spread d'environ 2,7 %. Aucun ordre n'a été retenu sur ces deux titres.
- La fiche historique AG3 a bien été exécutée : `AVAILABLE`, **8 émetteurs couverts sur 19**, aucun historique daté pour TKO.PA. Les absences sont explicites ; aucune probabilité prédictive n'a été fournie.
- Charge de prompt élevée : Claude **94 217 tokens d'entrée** et 2 603 de sortie ; DeepSeek **62 113 tokens d'entrée** et 8 237 de sortie. GPT n'expose ici qu'une estimation de 56 000 tokens, pas une consommation réelle confirmée. Le contexte AG5–AG9 ignoré ne représente qu'environ 1,5 k caractères : ce n'est pas la cause principale du volume.
- Le pack d'opportunités après préflight contient 19 lignes ; ses champs les plus volumineux sont fondamentaux, liquidité et news. Une revue de compaction pourrait réduire le coût, à valider sans perdre de données décisionnelles.
- `core.runs.ai_cost_eur=0.00` malgré les appels LLM réussis : la comptabilité du coût de ce run n'est pas renseignée de façon exploitable. Aucun coût fournisseur en euros n'a été estimé dans cet audit.

## 6. Portée historique et limites

Les **30 derniers runs AG1 persistés** contiennent **28 `OK / CAUTION`** et **2 `GLOBAL_CONTEXT_STALE / IGNORE`**, tous deux matinaux. Les six derniers runs de 17:10 ont un âge de contexte d'environ 1,09 h. Ce contrôle porte sur les runs présents dans le ledger ; il ne prouve pas l'absence de lancements échoués avant persistance.

La disponibilité historique est donc largement confirmée. L'utilité économique ne l'est pas : réception d'un pack, citation dans une réponse et amélioration mesurable de décision/performance sont trois niveaux différents. Aucun test comparatif avec/sans AG5–AG8 n'a été effectué ici.

## 7. Actions restantes proposées — non déployées

1. Rendre le rafraîchissement de synthèse disponible avant tout run AG1, y compris manuel : reconstruction bornée à partir des producteurs déjà publiés si la synthèse est périmée ou antérieure à leurs snapshots. Conserver le repli explicite si les sources elles-mêmes sont inutilisables.
2. Ajouter un avertissement structuré au bilan et au dashboard lorsque le contexte est `IGNORE` ou `CAVEAT_ONLY`, sans transformer automatiquement un contexte consultatif en gate de trading.
3. Auditer puis réparer les quatre séries monétaires périmées et les autres trous de couverture ; ne pas masquer le problème en allongeant globalement les seuils.
4. Mesurer séparément l'apport AG5–AG8 par replay isolé et comparaison des décisions. AG9 reste une décision de périmètre distincte.
5. Compléter la mesure des coûts LLM et évaluer la compaction des prompts.

Aucun workflow, cron, seuil, garde, service ni base métier n'a été modifié. Aucune nouvelle exécution AG1 n'a été déclenchée.

## Preuves locales et points de code

Dossier local gitignoré : `.codex-tmp/audit-ag1-22746/`.

- `executions.raw.json` : six exécutions n8n et leurs définitions enregistrées.
- `22746.json` : exécution décodée, entrées et sorties des nœuds.
- `ledger.json` : résultats métier, consensus, historique des 30 runs et publication constatée.
- `delivery-proof.json` : vérification de l'identifiant et du hash dans les trois branches.
- `shadow-synthesis.json` : configuration effective, snapshots producteurs, pack reconstruit et lignes qualité.
- `source-audit.txt` : dates des taux bruts et journaux producteurs du matin.
- `decode.py`, `focused.py` : extraction locale des preuves.

Points de code : `services/global-context-synthesizer/main.py` (`/ag1-pack`), `synthesizer.py` (`advisory_pack_for_run`, `_llm_use_policy`), `config/context.json`. Ordonnancement : `docs/operations/SCHEDULING_AND_LOAD.md`.
