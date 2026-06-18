# AG4_Spé-V2 — Plan de remédiation complet (restaurer l'utilité réelle)

**Date** : 2026-06-17 · **Auteur** : Nicolas × Claude · **Prérequis de lecture** : `docs/audits/20260617_ag4_spe_v2_analysis.md` (diagnostic).
**But** : passer d'une base de news *produite mais corrompue et quasi-inutilisée* à un **pilier fiable, frais et réellement consommé par AG1 V4**.

---

## 0. Reformulation du problème

Le diagnostic a montré que le coût LLM n'est **pas** le problème. Les vraies causes de la faible utilité sont, par ordre d'impact :

1. **L'analyse produite n'arrive pas au décideur.** AG1 V4 ignore 11 des 12 champs et ne garde qu'`impact_7d` (~10 % du score). → *plus gros levier d'utilité.*
2. **La seule donnée consommée est corrompue.** `published_at` (2016→2031) casse les fenêtres 7 j/30 j qui calculent justement `impact_7d`.
3. **La fraîcheur est insuffisante pour du live.** Cycle de rotation ~7 j (max 68 j observé) : la news d'une position ouverte peut avoir une semaine de retard.
4. **Le pipeline se dégrade en silence.** 6 jours sans analyse fraîche, 47 runs « zombies », et personne n'est alerté.
5. **Densité de signal faible.** 84 % de placeholders en base + 66 % des analyses jugées non pertinentes.

Le plan est organisé en **6 chantiers (A→F)**, chacun avec actions concrètes, critères d'acceptation et effort. La **séquence recommandée** est en §7.

---

## A. Fiabilité d'exécution & observabilité

> *Objectif : que le pipeline ne puisse plus mourir sans qu'on le sache, et que les stats de run soient justes.*

| # | Action | Détail technique | Acceptation |
|---|---|---|---|
| A1 | **Corriger la finalisation des runs (zombies)** | `S24` finalise par requête « dernier RUNNING » → fragile. Finaliser **par `run_id` explicite** (porté dans l'item), et au **début** de chaque run réconcilier les `RUNNING` orphelins en `STALE` (`02_start_run.py`). Brancher S24 sur **toutes** les sorties terminales, pas seulement la branche « done » de S03. | 0 run `RUNNING` > 1 h en base ; `run_log` reflète la réalité n8n. |
| A2 | **Tracer les symboles à 0 article** | Aujourd'hui `symbols_ok` chute (1/20, 10/20) sans trace. Logguer dans `news_errors` la raison (HTTP≠200, listing vide, ref manquante). | Chaque symbole « KO » a une ligne `news_errors` avec cause. |
| A3 | **Vue santé + alerte** | Petit script/vue : dernière analyse fraîche, runs zombies, couverture 7 j, % dates valides. Push Telegram (`@CYROLAS_BOT`, groupe existant) si « 0 analyse > 48 h » ou « >X% dates invalides ». | Alerte reçue le jour où le flux se tarit. |
| A4 | **Purge/retention `run_log`** | 391 lignes, 47 polluées. Marquer les zombies historiques `STALE` (one-shot) pour ne pas fausser les futurs agrégats. | Agrégats run_log fiables. |

---

## B. Qualité de données (le bloquant pour le signal consommé)

> *Objectif : des timestamps fiables et une table propre pour les consommateurs.*

| # | Action | Détail technique | Acceptation |
|---|---|---|---|
| B1 | **Réécrire `normalizeDate()`** (`nodes/07_parse_article.js`, S16) | Ne dater **que** depuis sources fiables : `<time datetime>`, meta `article:published_time`, JSON-LD `datePublished`. **Supprimer** la regex gloutonne sur texte libre, ou la borner : rejeter toute année hors `[now-2 ans ; now+7 j]`. Fallback explicite → `first_seen_at`. | Sur 30 articles test, `published_at` = vraie date ±1 j ; 0 date future. |
| B2 | **Backfill `published_at`** | Job idempotent : re-parser le HTML stocké si dispo, sinon poser `published_at = NULL` (pas une fausse date) et laisser AG1 retomber sur `first_seen_at`. | <2 % de dates hors plage plausible. |
| B3 | **Vue consommateur propre** | Créer `news_analyzed` (VIEW) = `summary IS NOT NULL AND is_relevant`. Évite le piège des 16 k placeholders `is_relevant=true` par défaut. AG1 et tout futur consommateur lisent la vue. | Vue exposée, documentée dans README. |
| B4 | **Hygiène placeholders** | Décider : soit ne **pas** écrire de ligne pour les skip « jamais analysés » (garder seulement le `news_id` de dédup ailleurs), soit les exclure via B3. Réduit la table de ~84 %. | `news_history` ne sert plus de signal trompeur. |
| B5 | **Nettoyer colonnes mortes** | `vector_status/vector_id/vectorized_at/chunk_total` (RAG abandonné, Qdrant retiré) : documenter comme dépréciées ou drop. | Schéma sans dette morte. |

---

## C. Fraîcheur & couverture

> *Objectif : que les symboles qui comptent pour AG1 soient frais (<24-48 h), pas en rotation uniforme à 7 j.*

| # | Action | Détail technique | Acceptation |
|---|---|---|---|
| C1 | **Rotation priorisée portefeuille/watchlist** | `01_build_symbol_queue` : faire passer **en priorité et à chaque run** les symboles détenus (positions AG1 ouvertes via `ag1_v4_consensus.duckdb`) + watchlist, puis compléter le batch avec la rotation du reste de l'univers. | Toute position ouverte a une news < 24 h en J ouvré. |
| C2 | **Augmenter le débit utile** | Soit batch 20→30-40, soit +1 run/jour, soit réduire l'univers scrappé aux symboles « actifs » (filtrer `universe.Enabled`/liquidité). 420 symboles à 7 j est trop lent. | Cycle complet ≤ 3 j ; positions ≤ 1 j. |
| C3 | **Robustesse scraping** | Vérifier le sélecteur listing Boursorama (anti-bot / pagination), back-off sur 429, retry par symbole. Cf. `symbols_ok` faible. | `symbols_ok`/`symbols_total` > 90 %. |

---

## D. Exploitation par AG1 V4 — **levier d'utilité n°1**

> *Objectif : faire arriver l'analyse qualitative au PM, pas juste un scalaire faussé.*

| # | Action | Détail technique | Acceptation |
|---|---|---|---|
| D1 | **Corriger la fenêtre temporelle côté AG1** | `R8_data_prep_matrix.code.py` (l.301-325) : remplacer le tri/filtre `COALESCE(published_at, …)` par `COALESCE(first_seen_at, analyzed_at)` tant que B1/B2 ne sont pas déployés. Sinon `impact_7d` reste faux. | `count_7d`/`impact_7d` reflètent les vraies 7 j ; `last_news_date` ≤ aujourd'hui. |
| D2 | **Passer un *digest news* qualitatif au PM** | Pour chaque symbole détenu/candidat, injecter dans le brief les **1-3 dernières news pertinentes** : `title + summary + suggested_signal + impact_score + horizon` (depuis la vue B3). Aujourd'hui le PM ne voit qu'un chiffre. C'est là que dort 84 % de la valeur déjà payée. | Le brief PM contient un bloc « news récentes » lisible par symbole. |
| D3 | **Recalibrer le poids/usage news** | Une fois la donnée fiable et qualitative exposée, réévaluer le poids 0,10 et la formule `news_risk/reward_catalyst/sentiment_prob` (`calcul_matrice_briefing`). Éventuellement utiliser `suggested_signal`/`confidence` plutôt que la seule somme d'`impact`. | Décision documentée (garder/ajuster), shadow-test avant prod. |
| D4 | **Garde-fou « news périmée »** | Si `last_news_date` d'un symbole > N jours, marquer la feature comme « stale » plutôt que 0 (un 0 = « pas de news » ≠ « pas regardé »). | Le PM distingue absence de news vs absence de couverture. |

---

## E. Efficacité & efficience LLM (densité de signal)

> *Objectif : moins de bruit analysé, même coût ou moindre, signal plus dense.*

| # | Action | Détail technique | Acceptation |
|---|---|---|---|
| E1 | **Pré-filtre anti-bruit avant LLM** | `08_prepare_llm_input` : sauter titres manifestement non-news (cotation brute, agenda, « séance du… », pubs) via règles/regex. Réduit les 66 % `is_relevant=false`. | Part de `is_relevant=false` < 40 %. |
| E2 | **Analyser N articles d'un symbole en 1 appel** (option) | Regrouper les articles neufs d'un même symbole en un seul appel structuré (array) → moins d'appels, contexte société partagé. | Appels LLM/run réduits sans perte de champs. |
| E3 | **Exploiter `needsFollowUp`/`confidence`** | Rien ne consomme `needsFollowUp`. Soit le brancher (re-analyse ciblée / signalement PM), soit le retirer du schéma. | Champ utilisé ou supprimé. |

---

## F. Architecture cible (moyen terme, à arbitrer)

> *Objectif : éviter la dette et la redondance avec les autres piliers.*

- **F1 — Convergence AG4-V3 (macro) / AG4_Spé (single-stock)** : deux workflows scrapent/analysent des news avec des schémas proches mais divergents. Évaluer un socle commun (parsing, dédup, finalize) pour ne maintenir qu'un moteur.
- **F2 — Contrat de données explicite** : figer le schéma que AG1 (et un futur PM Forex) consomme (vue B3 + champs), versionné, pour que producteur et consommateur n'aient plus de mismatch silencieux (cf. bug camelCase↔snake_case déjà connu sur AG1).
- **F3 — Décision « garder Boursorama ? »** : la source produit 84 % de bruit. Évaluer une source plus dense (RSS émetteurs, presse financière, API) au moins pour les symboles détenus.

---

## 7. Séquence recommandée (du plus utile au moins urgent)

**Sprint 1 — « rendre le signal vrai et visible » (P0)**
- D1 (fenêtre `first_seen_at` côté AG1) → *gain immédiat : `impact_7d` cesse d'être faux, sans attendre B1.*
- B1 + B2 (fix dates + backfill).
- A1 (finaliser les runs / zombies) + A4 (purge historique).

**Sprint 2 — « rendre le signal dense et frais » (P1)**
- B3/B4 (vue propre + hygiène placeholders).
- C1 + C2 (rotation priorisée portefeuille + débit).
- A2 + A3 (traçage KO + alerte santé).

**Sprint 3 — « rendre le signal exploité » (P1/P2)**
- D2 (digest qualitatif au PM) — *le gros levier d'utilité.*
- C3 (robustesse scraping), E1 (pré-filtre bruit).
- D3 + D4 (recalibrage + garde-fou stale).

**Backlog — moyen terme (P2/P3)**
- E2, E3, B5, F1, F2, F3.

> **Règle projet** : toute modif d'un workflow live se valide en **shadow/replay avant publication** (cf. AGENTS.md). D1/D2 touchent AG1 V4 qui est en **trading LIVE réel** → shadow obligatoire.

---

## 8. Quel ordre te donne le plus de valeur d'abord ?

Si tu veux un **quick win mesurable en 1 intervention** : **D1** (basculer AG1 sur `first_seen_at`) répare l'unique signal réellement utilisé, sans rien casser en amont. Ensuite **B1** (cause racine) puis **D2** (exploiter l'analyse déjà payée) sont les deux mouvements qui « rétablissent une vraie utilité ».
