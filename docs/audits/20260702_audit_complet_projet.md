# Audit complet du projet Trader_IA — 2026-07-02

**Auteur :** Claude (Cowork), à la demande de Nicolas.
**Méthode :** lecture d'AGENTS.md + docs de référence + mémoire projet, puis **vérification live sur le VPS** (lecture seule : broker `/health`, `/orders/approvals/pending`, SQLite n8n, DuckDB via `yf-enrichment`), puis audit du repo (git, divergences repo↔live).
**Horodatage des mesures live :** 2026-07-02 13:19 → 13:55 UTC.
**Convention :** chaque constat est marqué ✅ (sain), ⚠️ (vigilance) ou 🔴 (problème). Les faits sont vérifiés sauf mention « Hypothèse ».

---

## 1. Synthèse exécutive

Le système est **globalement sain et opérationnel** : 17 containers up, broker IBKR authentifié en LIVE sur le bon compte, 15 workflows n8n actifs conformes à AGENTS.md, fills Euronext réguliers, NAV 9 921,80 € (−0,78 % depuis le départ), disque à 34 %.

**Mais l'audit révèle un problème majeur** : **le circuit d'exécution des actions US est de facto bloqué** (F1). Depuis le 18/06, un seul ordre US a fillé ; tous les BUY US récents (NVDA, NFLX, AVGO, PDD, RIO) finissent soit REJECTED (« order without market data »), soit parqués en approbation puis **expirés** (TTL 10 min). L'expansion univers +100 (US/global, 24/06) et le 2ᵉ run 16:30 (29/06) ont été conçus pour trader les US — **la chaîne de décision fonctionne, la chaîne d'exécution n'aboutit pas**. Cause racine : absence de market data US temps réel côté IBKR → prompt de confirmation systématique.

Trois autres problèmes actifs : régression des dates Boursorama (235 news datées dans le futur, jusqu'en 2030) (F2), timeout du workflow Universe Quarantine 2 runs sur 3 (F3), contention DuckDB résiduelle sur `ag1_v4_consensus` (MTM en erreur ~4×/3j) (F4).

Côté repo : le code broker est **synchronisé repo↔live** (vérifié par md5), mais 4 fichiers réellement modifiés ne sont **pas committés** (dont les fix risk-score V2 et stop-fallback du 29-30/06), `SYSTEM_LINKS_AND_PARITY.md` est **untracked**, et 233 fichiers de bruit CRLF polluent le working tree.

---

## 2. État live vérifié (2026-07-02)

### 2.1 Infrastructure
| Élément | État | Détail |
|---|---|---|
| Containers | ✅ | 17 up dont `ibkr-broker` (healthy, 6j), `ibkr-gateway` (healthy, 2j), `root-n8n-1` + 3 task-runners (2j), dashboard (3h) |
| Disque | ✅ | 33 Go / 96 Go (34 %) — le nettoyage du 30/06 tient |
| Timer login IBKR | ✅ | `ibkr-daily-auth.timer` actif, dernier run 05:00 UTC, prochain 03/07 05:00 UTC |
| Crontabs hôte | ✅ | maintenance AG4 (dim. 11:00) + collecteur Finnhub (9/12/15h L-V) en place |
| Base n8n | ⚠️ | `database.sqlite` 1,1 Go + **WAL 407 Mo** (checkpoint en retard) ; 1 seul backup résiduel (315 Mo, 29/06) — rotation OK depuis le cleanup |

### 2.2 Broker / IBKR (`GET /health` 13:20 UTC)
- ✅ `authenticated=true`, `connected=true`, compte **U25651155 aligné** (`gateway_is_paper=false`), gateway Build 10.46.1p.
- ✅ Session monitor : tickle OK 13:20 UTC, dernier reauth auto réussi 30/06 23:28, `manual_login_required=false`.
- ✅ Garde-fous `.env` vérifiés : `IBKR_DRY_RUN=false`, `IBKR_REQUIRE_PAPER_ACCOUNT=false` (corrigé depuis le 30/06), `IBKR_APPROVAL_ENABLED=true`, `IBKR_PRICE_GUARD_MAX_DEVIATION_PCT=5.0`, `IBKR_FX_ORDERS_ENABLED=false`, `AG1_ACTIONS_LIVE_ORDERS_ENABLED=true` (dans `/docker/root/.env`).
- ⚠️ `assisted_login.enabled=false` / `credentials_configured=false` : le broker se repose sur le login quotidien assisté du timer + reauth auto. Cohérent avec `docs/operations/20260624_ibkr_daily_assisted_auth.md`, mais à savoir : pas de re-login autonome complet en cas de perte de session hors fenêtre 07:00.

### 2.3 Workflows n8n (base SQLite, 3 derniers jours)
- ✅ **15 workflows actifs**, liste strictement conforme à AGENTS.md (AG1V4CONSENSUS, Approval Request/Decide, AG1-PF MTM, AG2 HC/WL/UHQ, AG3 HC/WL, AG4-V3, AG4_Spé-V2/IBKR/Finnhub/Health, YF-ENRICH).
- ✅ Majorité de runs `success` ; AG1 V4 : 9 succès/3j (2 créneaux/j), AG3 HC/WL : 100 % succès, Finnhub/IBKR news : 9 succès chacun.
- 🔴 `AG2 — Universe Health Quarantine` : **2 échecs / 3 runs** (30/06 et 01/07 18:00 UTC) — « Task execution timed out after 60 seconds » (F3).
- 🔴 `AG1-PF-V1 MTM` : 4 erreurs/3j — « IO Error: Could not set lock on `ag1_v4_consensus.duckdb` » (F4).
- ⚠️ `Order Approval Decide` : 3 erreurs/3j au node « Broker approve/reject » (F5).
- ⚠️ AG4-V3 : 2 erreurs/3j (créneaux 16:45 le 29/06 et 08:45 le 02/07) ; AG4_Spé-V2 : 1 erreur. Transitoire, à surveiller via Health Alert.
- ℹ️ Les 4 `crashed` du 29/06 ~14:00 correspondent au restart n8n de cette date (containers Up 2 days) — pas un problème récurrent.

### 2.4 Portefeuille (ledger `ag1_v4_consensus.duckdb` + recon IBKR 15:00 Paris)
- **NAV 9 921,80 €** (net liquidation IBKR), cash 6 671,27 €, invested 3 249,44 € (~33 %), **P&L total −78,20 € (−0,78 %)**, unrealized −68,27 €.
- **7 positions** : DSY.PA ×28, ELEC.PA ×1, ENX.PA ×4, GTT.PA ×2, NVDA ×2, PEUG.PA ×16, VIRP.PA ×1.
- 99 runs en ledger ; ordres : 30 REJECTED / 13 FILLED / 10 PLANNED / 5 SUBMITTED ; 13 fills.
- Derniers fills : ENX.PA (01/07 ×2), ELEC.PA SELL (01/07), GTT.PA (30/06). **Un seul fill US depuis le 18/06 (NVDA ×2).**

### 2.5 Piliers de données (DuckDB)
| Pilier | Mesure live | Verdict |
|---|---|---|
| Univers (`ag2_v3.universe`) | 563 symboles ; segments : 7 HELD / 50 CORE_AUTO / 18 CORE_MANUAL / 282 WATCHLIST (=357) ; **206 sans segment** dont 128 en quarantaine active et **78 hors quarantaine** | ⚠️ F7 |
| Technique (`technical_signals`) | 320 symboles ≤96 h, 37 à 96-240 h, 206 >240 h (les non-segmentés) | ✅ cohérent avec la rotation |
| Fondamental (`ag3_v2.fundamentals_snapshot`) | 524 symboles ; âge moyen **14,2 j**, max 122 j ; **357 frais ≤7 j** (gate STALE_FUNDA à 168 h) | ⚠️ 167 symboles servis avec funda neutralisé |
| News (`ag4_spe_v2.news_history`, 7 j) | finnhub 1 457 · ibkr 350 · boursorama 69 valides — mais **235 articles boursorama datés dans le futur (max 2030-12-19)** | 🔴 F2 |
| Quarantaine | 128 actives ; dernière MAJ 01/07 18:00 (partielle, run en timeout) | ⚠️ F3 |

---

## 3. Constats détaillés (findings)

### 🔴 F1 — P0 · L'exécution des actions US n'aboutit quasi jamais
**Faits validés :**
- Tous les BUY US des 4 derniers jours ont échoué : NVDA REJECTED 3× (« IBKR_ORDER_NEEDS_CONFIRMATION: You are submitting an order without market data… »), NFLX/AVGO/PDD/RIO parqués en approbation `IBKR_PROMPT_WITHOUT_MARKET_DATA` puis **EXPIRED** (TTL 600 s) ou encore SUBMITTED sans fill.
- File d'approbations au 02/07 : 6 entrées, dont 3 EXPIRED (NFLX 30/06, AVGO 01/07 ×2) et 3 SUBMITTED du jour (PDD, RIO, AVGO).
- Un seul fill US au ledger depuis le 18/06 (NVDA ×2 le 18/06 21:05, marché US ouvert).
- Les fills Euronext passent normalement (pas de prompt market data sur `.PA`).

**Analyse :** le compte n'a pas de souscription market data US temps réel → IBKR affiche un prompt de confirmation sur chaque ordre US → le broker parque l'ordre pour approbation Telegram (comportement voulu, commit `8903551`) → si Nicolas ne valide pas dans les 10 min, l'ordre expire. Le 2ᵉ run 16:30 (fix timing US du 29/06) a bien rendu les US *décidables*, mais pas *exécutables*.

**Impact :** coût d'opportunité direct ; biais structurel du portefeuille vers Euronext ; l'expansion univers +100 (majoritairement US/ADR) ne produit aucun trade réel ; les CORE_MANUAL (AMD, TSM, NVO…) sont analysés à chaque run pour rien.

**Options (décision Nicolas requise) :**
1. **Souscrire un bundle market data US** sur U25651155 (quelques €/mois, souvent ~1,5-15 USD) → supprime le prompt à la racine. Option la plus propre.
2. Auto-confirmer `IBKR_PROMPT_WITHOUT_MARKET_DATA` quand la déviation limit↔référence yfinance est ≤ bande auto (5 %) — le price-guard joue déjà le rôle de garde-fou. Modif `services/ibkr-broker/approval.py` + `app.py`.
3. Allonger le TTL (600 → 3600 s) et/ou renvoyer un rappel Telegram avant expiration. Pallie sans corriger.

### 🔴 F2 — P0 · Régression des dates de publication Boursorama
**Faits validés :** 235 articles `source='boursorama'` avec `published_at` **dans le futur** (max 2030-12-19) dans `news_history` ; sur 7 j glissants, seuls 69 articles boursorama ont une date valide (dernier : 30/06).
**Analyse :** le fix B1 (`07_parse_article.js`, 18/06) est en régression ou un format de date non couvert est apparu. Les dates futures polluent la vue `news_analyzed`, la fenêtre 14 j du digest 20K d'AG1 et le tri par fraîcheur.
**Actions :** (1) re-diagnostiquer `07_parse_article.js` sur les articles fautifs ; (2) purge ciblée (`outils/scripts/ag4_spe_cleanup_history.py` à étendre) ; (3) **garde-fou à l'écriture** : rejeter/clamper tout `published_at > now() + 24 h` dans le node d'écriture DuckDB.

### 🔴 F3 — P1 · `AG2 Universe Health Quarantine` en timeout 2 runs sur 3
**Faits validés :** échecs 30/06 et 01/07 (18:00 UTC) — « Task execution timed out after 60 seconds » (timeout du Code node task-runner, distinct du timeout 1200 s par tâche). La quarantaine a néanmoins été partiellement mise à jour le 01/07 (MAX(updated_at) 18:00:28) : le timeout frappe un node aval.
**Hypothèse :** l'audit des 563 symboles (vs 385 au déploiement) a dépassé le budget du node.
**Actions :** identifier le node fautif (exécution 01/07 18:00), le découper en batches ou augmenter `N8N_RUNNERS_TASK_TIMEOUT`-équivalent du runner Python ; vérifier le run du 02/07 20:00 Paris.

### 🔴 F4 — P1 · Contention DuckDB résiduelle sur `ag1_v4_consensus`
**Faits validés :** AG1-PF MTM en erreur 4×/3j — « Could not set lock on `ag1_v4_consensus.duckdb` » ; collisions observées à 12:00 UTC (= run AG1 V4 14:00 Paris qui écrit la même base) et 11:00 UTC.
**Analyse :** la déconfliction du 28/06 a traité `ag2_v3`/`ag3_v2` mais pas les collisions MTM horaire ↔ AG1 V4 ↔ recon IBKR sur `ag1_v4`. Impact faible (le MTM suivant rattrape) mais bruit d'erreurs et trous ponctuels de MTM.
**Actions :** décaler le MTM à H+15 sur les créneaux 14:00/16:30 Paris, ou retry-backoff dans `00_read_portfolios_duckdb.py` / `01_write_positions_mtm_duckdb.py` (Option A du doc SCHEDULING, non déployée).

### ⚠️ F5 — P1 · `Order Approval Decide` en erreur sur les taps tardifs
**Faits validés :** 3 erreurs/3j au node « Broker approve/reject » (erreur HTTP renvoyée par le broker). Le fix idempotence du 18/06 couvre « déjà décidé » (200), mais un tap sur une approbation **EXPIRED** semble encore produire une erreur workflow.
**Hypothèse :** chemin `EXPIRED` non couvert par `_approval_decision_error`.
**Actions :** reproduire (tap sur approbation expirée), étendre l'idempotence à EXPIRED (réponse 200 + message Telegram « expiré »), committer.

### ⚠️ F6 — P1 · Hygiène git : travaux live non committés + bruit CRLF
**Faits validés :**
- Branche `codex/live-trading-sync-20260629`, dernier commit `06ab868`.
- **4 fichiers avec de vraies modifications non committées** : `AGENTS.md` (+2 blocs), `docs/operations/SCHEDULING_AND_LOAD.md` (2ᵉ créneau AG1), `agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/nodes/pre_agent/calcul_matrice_briefing.code.py` (+54/−… : risk-score V2 + stop-fallback ATR du 29-30/06), `services/dashboard/app.py` (+86/−12, miroir des mêmes fixes).
- **`docs/operations/SYSTEM_LINKS_AND_PARITY.md` est untracked** alors qu'AGENTS.md le désigne « SOURCE DE VÉRITÉ ».
- 233 autres fichiers modifiés = pur bruit CRLF (`git diff --ignore-cr-at-eol` ne montre que les 4 ci-dessus).
**Actions :** commit ciblé des 4 fichiers + add du doc parité ; puis traiter le bruit : `git add --renormalize .` après pose d'un `.gitattributes` (`* text=auto eol=lf`) dans un commit dédié.

### ✅ F7 — RÉSOLU (non-problème) · 78 symboles sans segment = paires FX legacy
**Faits validés (complément 02/07 après-midi) :** les 78 symboles ni segmentés ni quarantainés sont **à 100 % des paires `FX:*` legacy** (AUDCAD, CADCHF…) — précisément les 78 paires retirées de l'univers AG1 par la whitelist `EQUITY/ETF/CRYPTO` (hybride 19/06). Leur absence de `universe_segments` est **volontaire** (Forex gelé). Un classement WATCHLIST a été testé puis **entièrement rollbacké** (tag `reason='audit_20260702_orphan_backfill'`, 78 lignes supprimées, état vérifié identique 282/50/18/7).
**Action résiduelle (optionnelle, décision Nicolas) :** purge cosmétique des 78 lignes `FX:*` de `universe` pour clarifier les stats — sans urgence ni impact.

### ⚠️ F8 — P2 · Dashboard : divergence repo ↔ live confirmée
**Faits validés :** live `/opt/trading-dashboard/app/app.py` = 20 729 lignes ; repo `services/dashboard/app.py` = 20 454 lignes (md5 différents). Le broker, lui, est **parfaitement synchronisé** (md5 identiques sur `app.py`, `approval.py`, `cpapi_client.py`, `contract_cache.py`). Déjà documenté dans SYSTEM_LINKS_AND_PARITY.md ; confirmé toujours vrai.
**Action :** rapatrier la version live vers `services/dashboard/app.py` dans le commit F6 (le live est la source de vérité).

### ⚠️ F9 — P2 · Points AGENTS.md obsolètes (à mettre à jour)
- **Bug « 3 champs NULL » : corrigé en pratique.** `core.runs.strategy_version/prompt_version/n8n_execution_id` sont renseignés sur tous les runs récents (ex. run 19730) ; il ne reste que 15 anciens runs NULL sur 99. Retirer de la liste des bugs ouverts (ou noter « historique non backfillé »).
- L'entête « État vérifié 2026-06-18 » et la branche mentionnée (`claude/ag4-v3-dualbranch-calib-20260617`) sont périmés (branche réelle : `codex/live-trading-sync-20260629`).
- README.md : « État opérationnel au 2026-06-18 » à rafraîchir.

### ⚠️ F10 — P2 · Rappels de risque structurels (déjà connus, toujours vrais)
- **Store des approbations en mémoire** : la file (6 entrées) survit car le broker tourne depuis 6 j ; un restart la perd. Persistance DuckDB = évolution souhaitable, d'autant plus critique si F1 est traité par allongement de TTL.
- `AG2-V3 Held+Core` : durées du jour 26/32/29 min vs ~23 min de moyenne — dérive légère à surveiller (croissance HELD+CORE).
- WAL n8n 407 Mo : surveiller le checkpoint SQLite.

---

## 4. Récapitulatif priorisé

| # | Priorité | Constat | Action recommandée | Où |
|---|---|---|---|---|
| F1 | **P0** | Exécution US bloquée (prompt market data + TTL 10 min) | Décision : souscription market data US **ou** auto-confirm ≤5 % **ou** TTL allongé | IBKR account mgmt · `services/ibkr-broker/approval.py` |
| F2 | **P0** | 235 news Boursorama datées futur (régression B1) | Fix parseur + purge + clamp `published_at` à l'écriture | `agents/trading-actions/AG4 - Les news/AG4-SPE-V2/nodes/07_parse_article.js` |
| F3 | P1 | AG2UHQ timeout 60 s (2/3 échecs) | Batcher le node fautif | workflow `AG2UHQ20260619` |
| F4 | P1 | Locks `ag1_v4` (MTM 4 err/3j) | Décaler MTM à H+15 ou retry-backoff | `AG1-PF-V1` nodes 00/01 |
| F5 | P1 | Approval Decide erreur sur tap tardif | Idempotence étendue à EXPIRED | `services/ibkr-broker/app.py` |
| F6 | P1 | 4 fichiers réels non committés + doc parité untracked + 233 CRLF | Commit ciblé puis `.gitattributes` + renormalize | repo |
| F7 | ✅ résolu | 78 sans segment = paires FX legacy, exclusion volontaire | Aucune (purge cosmétique optionnelle) | `ag2_v3.universe` |
| F8 | P2 | Dashboard repo en retard sur live | Resync depuis `/opt/trading-dashboard/app/app.py` | `services/dashboard/app.py` |
| F9 | P2 | AGENTS.md/README périmés (bug NULL corrigé, branche, dates) | Mise à jour doc | `AGENTS.md`, `README.md` |
| F10 | P2 | Approvals en mémoire · dérive durée AG2HC · WAL 407 Mo | Persistance DuckDB · surveillance | broker · n8n |

**Ce qui va bien (à ne pas toucher) :** auth IBKR quotidienne + reauth auto, alignement compte live, consensus AG1 V4 2 créneaux, split AG2/AG3, pipeline news 3 sources (volumes sains), déconfliction nuit ag2/ag3, fills Euronext, garde-fous env, synchro broker repo↔live, discipline docs/audits.

---

## 5. Faits validés / hypothèses / actions restantes

**Faits validés :** tout le §2 et les mesures chiffrées du §3 (relevés live du 02/07 13:19-13:55 UTC, requêtes en lecture seule).
**Hypothèses (à confirmer) :** cause exacte du timeout AG2UHQ (volume 563) ; chemin EXPIRED non idempotent dans Approval Decide ; cause précise de la régression dates Boursorama (nouveau format vs rollback du fix).
**Décisions à prendre (Nicolas) :** option de déblocage F1 (market data / auto-confirm / TTL) ; sort des 78 symboles orphelins ; stratégie CRLF (.gitattributes).
**Actions restantes :** voir tableau §4 ; mise à jour AGENTS.md (F9) ; commit F6.

---

## 6. Annexe — commandes de vérification utilisées

```bash
# Broker (depuis le VPS)
curl -s http://127.0.0.1:18080/health
curl -s http://127.0.0.1:18080/orders/approvals/pending

# Workflows actifs + erreurs (SQLite n8n, lecture seule)
python3 -c 'import sqlite3; con=sqlite3.connect("file:/var/lib/docker/volumes/n8n_data/_data/database.sqlite?mode=ro",uri=True); ...'

# DuckDB (lecture seule via yf-enrichment, duckdb 1.4.4)
docker exec -i yf-enrichment python -c "import duckdb; con=duckdb.connect('/files/duckdb/ag1_v4_consensus.duckdb', read_only=True); ..."

# 78 orphelins segments (F7)
SELECT u.symbol FROM universe u
WHERE NOT EXISTS (SELECT 1 FROM universe_segments s WHERE s.symbol=u.symbol)
  AND NOT EXISTS (SELECT 1 FROM universe_quarantine q WHERE q.symbol=u.symbol AND q.active);

# News futures (F2)
SELECT source, COUNT(*) FROM news_history WHERE published_at > now() GROUP BY source;

# Vrais diffs hors bruit CRLF (F6)
git diff --ignore-cr-at-eol --stat
```
