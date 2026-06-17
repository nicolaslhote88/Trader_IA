# Audit du brief LLM — AG1 V4 Consensus (run de 14h, 2026-06-15)

> Version consolidée (révision croisée Claude × GPT-5.5). **Une seule conclusion fait autorité** :
> les anciennes prises de position contradictoires ont été retirées. Tout chiffre ci-dessous est vérifié
> sur le VPS depuis l'exécution n8n **18938** (`AG1V4CONSENSUS`, run `RUN_20260615_140007`, success, 14:00 Paris).

> Addendum 2026-06-16 : le run audité reste historique (`PM #3 = Gemini 3.5 Flash`).
> La version opérationnelle V4 a ensuite remplacé cette troisième branche par
> **Claude Sonnet 4.6** avec output parser structuré rétabli. Les nouveaux runs
> doivent donc être lus comme consensus GPT/Grok/Claude, pas GPT/Grok/Gemini.

## 1. Ce qui a réellement été envoyé aux 3 modèles

Les 3 agents PM partagent **exactement le même** System Message et User Message — aucune différenciation par modèle.

| Agent | Modèle réel | Tokens d'entrée (run 18938) |
|---|---|---|
| PM #1 | `gpt-5.5-2026-04-23` | 15 228 (compl. 3 014) |
| PM #2 | `grok-4.3` | 15 682 (compl. 1 508) |
| PM #3 | `models/gemini-3.5-flash` | **2 appels** : 18 262 (compl. 41, tour d'outil) + 18 785 (compl. 3 521) |

Tailles (vérifiées) : System **9 148** car. (dont **5 551** = schéma JSON, soit 61 %) · User template 8 756 → **rendu 38 861** car. ·
**message complet ≈ 48 025** car. · total entrée ≈ **69 700 tokens** (outil news inclus).
`opportunity_pack` : **17 804** car. compact / **25 311** car. tel qu'injecté (indenté `null,2`), **38 lignes** (18 Entrer / 8 Surveiller / 12 Réduire-Sortir).

Données injectées (réelles) : portefeuille 10 001,50 € / cash 9 168 € / 2 positions (PEUG.PA, VIRP.PA) ;
`sector_brief` (leaders Industrials/Technology/Financials, laggards Real Estate/Utilities/Energy) ;
matrice 38 candidats notés risk/reward/EV/grade/gates ; `config` = **uniquement des numéros de version** (aucune contrainte numérique).

Labels modèles persistés (`chatgpt52`, `grok41_reasoning`, `gemini30_pro`) : **obsolètes** vs modèles réels, mais conservés comme **identifiants historiques** (clés de jointure/lineage).

---

## 2. Réponses aux trois questions

**Pertinence — Oui.** Données fraîches et réelles, hiérarchie d'autorité claire (« la matrice fait foi »),
anti-hallucination opérationnel (prix proxy + `backfillRequests`), délégation AG2/AG3/AG4 bien posée, garde-fou périmètre FX.

**Raisonnement optimal — Partiellement.** Le fond est bon, la forme bride. Causes vérifiées : contraintes de risque
absentes, redondance System↔User, calculs déterministes confiés au LLM, pack surchargé de candidats inutiles,
schéma imposé en texte plutôt qu'en sortie structurée native, et un outil news qui désynchronise les faits entre modèles.

**Adéquation par modèle — Non, prompt unique « one-size-fits-all ».** Le **contenu métier doit rester commun** aux 3
(pour un vote comparable) ; l'adaptation doit porter sur les **paramètres fournisseur** (sortie structurée, budget de
raisonnement), pas sur 3 prompts métier différents. Aucun de ces réglages fournisseur n'est posé aujourd'hui.

---

## 3. Constats vérifiés en base (run 18938)

| Constat | Preuve |
|---|---|
| **2 ordres / 4 rejetés en LIVE pour liquidité** | `core.orders` : ALSOG.PA (pick 3/3) et THEP.PA `REJECTED` « limited liquidity » ; DSY.PA, LR.PA `FILLED` |
| Le pack masquait le risque liquidité | ALSOG.PA/THEP.PA avaient `gates=OK, spread_pct=0` dans `opportunity_pack` |
| Contraintes inventées par les modèles | `maxPositionPct` GPT/Gemini=10, Grok=6 ; `maxSingleNameRiskPct` 1→3 (rien dans `config`) |
| Déterminisme mal placé | timestamps supprimés par GPT/Grok ; `portfolioUpdatedAt` réécrit par Gemini ; `stopLossPct` **positifs** chez Gemini (négatifs ailleurs) |
| Outil news = faits désynchronisés | Gemini a appelé l'outil → 2ᵉ passage complet + faits absents chez GPT/Grok |
| Candidats inutiles | 12 « Réduire/Sortir » portent sur des symboles **non détenus** (GOOGL, AMZN…) ⇒ aucune vente possible |
| Bruit de mémoire historique | rationales périmées réinjectées ⇒ WATCH parasites (ex. FGR.PA chez Gemini) |

---

## 4. Plan d'action priorisé (consensus Claude × GPT-5.5)

**P0 parallèles**
1. **Liquidité** : ajouter au pack ADV, volume, spread horodaté, taille/ADV et un **preflight IBKR** ⇒ éviter les rejets live.
2. **Config réelle** : injecter `cfg.portfolio_config` (capital, `maxPositionPct`/`maxSectorPct`/`maxSingleNameRiskPct`, cash min, kill-switch).

**P1**
3. **Calculs déterministes hors LLM** : run_id, timestamps, `targetQty`, prix/limit, stops, `maxLossEUR` produits par le workflow et injectés *après* le LLM.
4. **Pack filtré et compact** : top 10-15 entrées, quelques WATCH, **aucune sortie sur non-détenu**, mémoire limitée aux idées encore présentes / rejets broker pertinents (≈ 25 k → ≈ 5 k car.).
5. **News mutualisées avant le fan-out** : retirer l'outil des 3 agents PM ; un enrichissement partagé en amont ⇒ faits identiques pour le vote.
6. **Dédupliquer System/User** + **sortie structurée** (OpenAI `json_schema`, Grok JSON mode, Claude via output parser structuré n8n / mode natif si exposé). Garder l'`Information Extractor` comme **télémétrie/garde** : accepter `OK_JSON`, **rejeter/retry** sur `OK_EXTRACTED_OBJECT`, **invalider le vote** sur `UNPARSED_TEXT`.

**P2**
7. **Réglages propres au fournisseur** : `reasoning_effort` Grok=medium ; reasoning/verbosity GPT-5.5 ; Claude Sonnet 4.6 → sortie stricte via parser structuré, consignes courtes et données compactes. Pas de température basse généralisée : la reproductibilité vient du schéma + déterminisme + évals.
8. **Ordre du prompt** : System = politique/rôle/hiérarchie/interdits d'abord ; User = données compactes puis question/checklist courte ; schéma porté par l'API, **non répété** en fin de prompt.
9. **Traçabilité modèles** : conserver les clés historiques et **ajouter** le modèle réel, ex. `model_key=chatgpt52` + `model_id=gpt-5.5-2026-04-23`.

**Gemini 3.5 Flash** : position historique de l'audit du 2026-06-15 : ne pas remplacer/pondérer sur un seul run. Décision opérationnelle ultérieure (2026-06-16) : remplacement par Claude Sonnet 4.6 pour restaurer la conformité de sortie structurée, à valider par évaluation comparative sur runs suivants.

> ⚠️ AG1 V4 pilote un compte **LIVE** : toute nouvelle version passe par **replay / shadow run** avant publication.

---

## 5. Alignement final — résolu

1. **Responses API (GPT)** : **vérifié.** Le snapshot réellement exécuté (`execution_data.workflowData`, versionId
   `2a9f6d7a…`) contient `responsesApiEnabled: true` (modèle `gpt-5.5-2026-04-23`). Nuance n8n : la colonne
   `workflow_entity.nodes` **élague** ce flag (valeur par défaut) pour le même versionId — d'où ma lecture initiale
   « absent ». Formulation retenue : *Responses API activée dans la config runtime n8n = vérifié ; l'endpoint HTTP
   réellement appelé n'a pas été capturé au niveau réseau.* Conséquence utile : GPT-5.5 tournant déjà via Responses API,
   la **sortie structurée native (`json_schema`)** est immédiatement disponible côté OpenAI (cf. plan P1.6).
2. **A priori Gemini Flash** : traité comme **hypothèse soumise aux évaluations** (cf. §4). Plus de divergence.

**Les deux analyses sont désormais entièrement alignées. Audit + plan d'action validés (Claude × GPT-5.5).**

---

## Annexe — sources (run 18938, vérifiées)
- System / User : node `Agent #1 - Portfolio manager` → `parameters.options.systemMessage` (9 148) / `parameters.text` (8 756).
- Pack résolu : `AG1.00 — Assemble Input Packs` (`run, config, portfolioBrief, sector_brief, opportunity_brief, opportunity_pack, …`).
- Ordres/consensus : `AG1.V4 — Build Consensus` (`decision=TRADE`, 4 actions, 3/3) ; `core.orders` (2 FILLED, 2 REJECTED).
- Fichiers source repo : `…/AG1-V4-Consensus Portfolio manager/workflow/nodes/agent_input/agent_1_portfolio_manager.node.json`,
  `…/nodes/agent_input/ag1_00_assemble_input_packs.code.js`, `…/workflow/build_v4_workflow.py`.
