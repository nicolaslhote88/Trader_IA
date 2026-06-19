# Audit AG2-V3 — Pertinence / Efficacité / Efficience pour AG1 V4

**Date** : 2026-06-19 · **Auteur** : analyse Claude · **Périmètre** : workflow `AG2-V3 — Analyse technique Actions` (univers EQUITY/ETF), 14 derniers jours d'exécutions réelles.
**Sources données** : VPS `srv961978`, base `ag2_v3.duckdb` (`/local-files/duckdb/`, lecture seule via `yf-enrichment`), tables `run_log`, `technical_signals`, `ai_dedup_cache`, `universe` ; requête de consommation extraite du nœud `R8 — Data Prep for Matrix` du workflow `AG1 V4 CONSENSUS` (`.codex-tmp/ag1v4_current_20260618.json`).

---

## 1. Verdict

**Le pilier AG2-V3 ne sert pas AG1 V4 à la hauteur de sa complexité.** Le préfiltre technique et les indicateurs D1 sont sains et pertinents. En revanche **l'étage LLM (gpt-5-mini) n'apporte aujourd'hui aucune information réellement consommée par AG1** : tous les champs IA qu'AG1 lit sont soit toujours NULL (`ai_rr_theoretical`), soit calculés de façon déterministe **avant** l'appel LLM (`ai_stop_loss`, `ai_alignment`, `ai_regime_d1`, `bias_sma200`) que le prompt se contente de recopier. Les vraies sorties génératives du LLM (`ai_decision`, `ai_quality`, `ai_reasoning`, `chart_pattern`) sont écrites en base mais **jamais sélectionnées par AG1**.

Le problème **n'est pas le coût** (≈ 0,5 $/mois, négligeable) : c'est un problème de **valeur** et de **câblage**. À cela s'ajoutent un bug de persistance (`ai_rr_theoretical` mort), une rotation qui neutralise le cache anti-doublon, et une pollution de l'univers par 78 paires FX désactivées.

---

## 2. Faits validés (chiffres réels, 14 j)

**Cadence & volumétrie**

- 84 runs (78 `SUCCESS`, 6 sans `finished_at` → incomplets/en cours), version unique `3.0.4`. Cron `5 9-17 * * 1-5` (horaire, 9h–17h UTC, jours ouvrés ≈ 6–9 runs/jour). Durée moyenne d'un run ≈ **19 min** (1128 s) pour ~30 symboles.
- Univers scanné : **385 EQUITY** (rotation par lot de ~30 → chaque symbole revu **4,1 fois** en moyenne sur 14 j, soit ~tous les 3,4 jours).
- 1 592 signaux écrits dans `technical_signals`.

**Entonnoir préfiltre → LLM**

| Étape | Volume | Part |
|---|---|---|
| Signaux calculés | 1 592 | 100 % |
| Éliminés par le préfiltre (`pass_ai=False`) | 1 304 | 82 % |
| Atteignent le LLM (`call_ai=True`) | **288** | **18 %** |

Détail préfiltre : `NEUTRAL` 1 161, `SELL_SIGNAL_SAFETY` 173, `BUY_IN_UPTREND` 93, `STALE_H1_DATA` 69, `NO_H1_DATA` 35, `WEAK_BUY_AGAINST_TREND` 34, `STRONG_REVERSAL_BUY` 22, `NO_DAILY_CONTEXT` 5.

**Décisions du LLM (288 appels)**

- `REJECT` 269 (**93,4 %**) · `APPROVE` 13 (**4,5 %**) · `WATCH` 5 (1,7 %) · erreur 1.
- Sur les 173 signaux `SELL` envoyés au LLM, **172 → REJECT** (1 erreur). Soit **60 % des appels LLM consacrés à des SELL** que le prompt rejette par règle (« stratégie long-only : si `h1.action`=SELL ⇒ REJECT »). Décision purement déterministe, sans valeur ajoutée du modèle.
- Sortie actionnable réelle pour AG1 : **18 signaux en 14 j** (13 APPROVE + 5 WATCH), soit ~1,3/jour.

**Cache anti-doublon (`ai_dedup_cache`) — inopérant**

- `pass_ai=True` = 288, `call_ai=True` = **288** : le cache n'a bloqué **aucun** appel.
- `dedup_reason` des 288 : `NO_CACHE` 218, `SIGNATURE_CHANGED` 69, `TTL_EXPIRED` 1, `UNCHANGED_WITHIN_TTL` **0**.
- Cause : TTL = 240 min (BUY) / 60 min (SELL), or chaque symbole n'est revu que tous les ~3,4 j → le TTL a toujours expiré entre deux visites. Le mécanisme est structurellement mort à cette cadence.

**Ce qu'AG1 V4 lit réellement** (nœud `R8`, `latest`-par-symbole, **sans filtre** sur `pass_ai`/`pass_pm`/`ai_decision` ni plancher de date) :
`d1_action, d1_score, d1_rsi14, d1_atr_pct, d1_resistance, d1_support, d1_dist_res_pct, d1_dist_sup_pct, ai_stop_loss, ai_rr_theoretical, ai_alignment, ai_regime_d1, last_close, data_age_h1/d1_hours`.

Couverture sur le snapshot lu par AG1 (463 lignes : 382 EQUITY + 78 FX + 3 ETF) :

| Champ IA lu par AG1 | Renseigné | Remarque |
|---|---|---|
| `ai_rr_theoretical` | **0 / 463 (0 %)** | Jamais persisté — absent du schéma de sortie LLM. **Colonne morte.** |
| `ai_stop_loss` | 78 / 463 (17 %) | Pré-calculé déterministe (`05_snapshot.js`), recopié par le LLM |
| `ai_alignment` | 461 / 463 (99,6 %) | Dérivé par règle (hydraté du cache) |
| `ai_regime_d1` | 461 / 463 (99,6 %) | **Recopié** d'un calcul déterministe pré-LLM |
| `ai_decision` (non lu par AG1) | 461 / 463 | présent mais jamais sélectionné |

**Preuve que le LLM n'invente rien d'utile à AG1** — `nodes/05_snapshot.js` calcule, **avant** l'appel modèle : `bias_sma200`, `regime_d1`, `stop_loss_suggested`, `stop_loss_basis`, `rr_theoretical`. Le prompt système ordonne ensuite : *« regime D1 … déjà calculé dans le contexte, tu le recopies »* et *« utilise stop_loss_suggested tel quel, ne l'invente pas »*. L'alignement est lui aussi une règle (BUY & bias=BULLISH ⇒ WITH_BIAS). **Tous les champs IA consommés par AG1 sont donc reproductibles sans LLM.**

**Qualité des données d'entrée**

- H1 : `OK` 1 172 (74 %), **`STALE` 375 (24 %)**, `NO_DATA` 35, `INSUFFICIENT_DATA` 10.
- D1 : `OK` 1 522 (96 %), `INSUFFICIENT_DATA` 37, `STALE` 28, `NO_DATA` 5.
- Le préfiltre bloque bien les H1 stale avant LLM (`STALE_H1_DATA` 69), mais le quart de données H1 périmées fragilise le déclencheur et le calcul de stop.

**Pollution de l'univers**

- `universe` contient encore **78 paires FX** (`asset_class='FX'`) + 3 ETF + 382 EQUITY. Le Forex est désactivé : ces 78 paires ont été scannées pour la **dernière fois le 2026-04-24** (~8 semaines). La requête d'AG1 (`MAX(workflow_date)` par symbole, sans plancher de date ni filtre `asset_class`) **ingère ces lignes d'avril** dans sa matrice.

**Coût LLM**

- gpt-5-mini : 0,125 $/M tokens entrée, 1,00 $/M sortie. Contexte ≈ 60 barres H1 + indicateurs bruts (~4–5k tokens entrée, ~300 sortie) ⇒ ≈ 0,0008 $/appel.
- 288 appels / 14 j ≈ **0,23 $** soit **~0,5 $/mois**. **Coût non significatif** — l'efficience n'est pas un enjeu monétaire ici.

---

## 3. Lecture pertinence / efficacité / efficience

**Pertinence (le pilier répond-il au besoin d'AG1 ?)** — Partiellement. La couche déterministe (indicateurs D1, S/R, ATR, préfiltre long-only, stop suggéré) est exactement ce dont un PM a besoin et elle est correcte. Mais la couche LLM, censée être la valeur ajoutée « analyse de courbe », ne transmet à AG1 aucune information qu'AG1 exploite : le PM travaille en réalité sur le signal déterministe brut + des champs IA redondants ou nuls.

**Efficacité (la qualité est-elle au rendez-vous ?)** — Moyenne. Sorties actionnables rares (18/14 j) — cohérent avec un marché et un univers donnés, mais 24 % de H1 stale et la colonne RR morte dégradent la fiabilité. Le LLM viole sa propre règle RR (13 APPROVE alors que `rr_theoretical` n'est jamais transmis dans sa sortie), signe que la « validation » n'est pas robuste.

**Efficience (efficacité au moindre coût LLM)** — Le coût absolu est négligeable, mais le **rapport valeur/coût est mauvais** : 60 % des appels servent à rejeter des SELL par règle, et 100 % des appels produisent des champs consommés par AG1 qui étaient déjà calculés sans IA. On paie (en latence et complexité, plus qu'en argent) un étage qui n'informe pas le décideur.

---

## 4. Anomalies / bugs concrets

1. **`ai_rr_theoretical` jamais persisté** (0/288) alors que `rr_theoretical` est calculé dans `05_snapshot.js`. Le `json_schema` de sortie du nœud LLM **ne contient pas** de champ RR ⇒ rien à mapper dans `06_extract_ai.py`. AG1 lit donc une colonne toujours NULL.
2. **APPROVE incohérents avec la règle RR** : le prompt impose `rr null ⇒ WATCH/REJECT`, or 13 APPROVE existent avec RR non transmis. La porte risque/rendement est de fait inactive.
3. **Cache dedup inopérant** (0 `UNCHANGED_WITHIN_TTL`) — TTL incompatible avec la cadence de rotation.
4. **Pollution FX** : 78 paires FX désactivées encore dans `universe`, réinjectées dans le snapshot AG1.
5. **`chart_pattern` incohérent** : valeurs `'None'`/`'NONE'`/`'none'`/`UNKNOWN`, ou simple recopie du *rationale* indicateur (« Prix < SMA50, … ») au lieu d'un motif chartiste. Champ non fiable (et non lu par AG1).
6. **6 runs sans `finished_at`** sur 14 j (incomplets) — à surveiller (timeouts ?).

---

## 5. Recommandations priorisées

**P0 — décider du rôle du LLM (le point central)**
Deux options exclusives, à trancher par Nicolas :
- **(A) Rendre le LLM utile** : câbler AG1 pour qu'il lise `ai_decision`/`ai_quality`/`ai_reasoning` comme filtre ou pondération de la matrice (p. ex. ne retenir que `APPROVE`/`WATCH`, ou pondérer par `quality_score`). Sans cela, l'étage ne sert à rien.
- **(B) Supprimer/contourner le LLM** : écrire directement `stop_loss`, `alignment`, `regime_d1`, `rr` depuis `05_snapshot.js` vers `technical_signals`, sans appel modèle. Gain : -100 % d'appels, -latence, -complexité. Coût d'opportunité quasi nul puisque AG1 n'exploite rien d'autre aujourd'hui.

**P0 — court-circuit SELL** (quelle que soit l'option) : ne pas appeler le LLM sur les `SELL` en stratégie long-only (REJECT déterministe dans le préfiltre). −60 % d'appels immédiatement.

**P0 — corriger `ai_rr_theoretical`** : soit l'ajouter au schéma de sortie LLM, soit (mieux) le mapper directement depuis `ai_context.rr_theoretical` dans `06_extract_ai.py`. Aujourd'hui AG1 lit une colonne morte.

**P1**
- **Nettoyer `universe`** : retirer/flag les 78 FX, et ajouter dans la requête `R8` d'AG1 un plancher de date (`workflow_date > now() - INTERVAL n DAY`) et/ou un filtre `asset_class IN ('EQUITY','ETF')`.
- **Cache dedup** : le retirer (mort à cette cadence) ou aligner TTL et fréquence si on veut le conserver.
- **H1 stale 24 %** : investiguer la source de fetch Yahoo H1 (le déclencheur et le stop en dépendent).

**P2**
- Fiabiliser `chart_pattern` (si l'option A est retenue) ou le retirer.
- Surveiller les 6 runs incomplets (durée moyenne 19 min ⇒ marge de timeout).

---

## 6. Hypothèses & limites de l'analyse

- **Hypothèse** : la requête `R8` du nœud `AG1 V4 CONSENSUS` est bien le **seul** point de consommation d'AG2 par AG1, et aucun nœud aval d'AG1 ne réintroduit `ai_decision`/`pass_pm`. Vérifié sur `.codex-tmp/ag1v4_current_20260618.json` (2 occurrences `technical_signals`, requête dupliquée) ; **non retracé** : la logique complète de la matrice AG1 en aval (pondération exacte de `d1_action`/`d1_score`).
- **Hypothèse** : l'estimation de tokens (≈4–5k entrée) est un ordre de grandeur, non une mesure ; le coût réel reste de toute façon négligeable.
- **Fait** : tous les chiffres des §2 proviennent de requêtes directes sur `ag2_v3.duckdb` (live, 2026-06-19).

## 7. Actions restantes

1. Trancher P0-A vs P0-B (rôle du LLM) — **décision Nicolas requise**.
2. Implémenter le court-circuit SELL + le fix `ai_rr_theoretical` (faible blast-radius, à valider en shadow/replay avant publication n8n).
3. Nettoyer `universe` (FX) + durcir la requête `R8` d'AG1.
4. Tracer la matrice AG1 aval pour confirmer l'hypothèse de consommation.
