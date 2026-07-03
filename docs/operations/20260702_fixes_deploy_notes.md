# Fixes audit 2026-07-02 — notes de déploiement & rollback

**Contexte :** suite à l'audit complet `docs/audits/20260702_audit_complet_projet.md` (findings F1-F10), Nicolas a validé : F1 = option « auto-confirm ≤5 % ». Tout ce qui suit a été **déployé live le 2026-07-02 entre 14:45 et 14:55 UTC** (vérifications post-déploiement incluses).

---

## F1 — Broker : auto-confirm du prompt IBKR « without market data » (≤5 %)

**Problème :** pas de souscription market data US → chaque ordre US déclenche le prompt IBKR « You are submitting an order without market data » → parcage approbation Telegram → expiration TTL 10 min quasi systématique. Un seul fill US depuis le 18/06 (NVDA), tous les autres BUY US perdus (NFLX, AVGO ×2, PDD, RIO).

**Fix :** `services/ibkr-broker/app.py`, `_price_confirmation_guard` : le prompt « without market data » (qualifié par `approval._is_without_market_data_prompt`, qui exclut margin/short/restricted) passe désormais par la **même vérification prix yfinance** que les prompts prix — écart limit↔référence ≤ `IBKR_PRICE_GUARD_MAX_DEVIATION_PCT` (5 %) → auto-confirm. Sinon (écart >5 %, quote trop vieille, pas de référence) : comportement inchangé (parcage/rejet). Trace : `guard.prompt_class="WITHOUT_MARKET_DATA"`.

**Flag :** `IBKR_AUTO_CONFIRM_NO_MARKET_DATA_PROMPT=true` ajouté à `/docker/yfinance/.env` (défaut code = `false`).

**Déploiement :** source `/opt/trader-ia/services/ibkr-broker/app.py` (= repo `services/ibkr-broker/app.py`, md5 identiques) → `cd /docker/yfinance && docker compose build ibkr-broker && docker compose up -d ibkr-broker`. Vérifié post-restart : `authenticated=true`, `aligned=true` (U25651155), `dry_run=false`.

**Rollback :** `IBKR_AUTO_CONFIRM_NO_MARKET_DATA_PROMPT=false` dans `.env` + `docker compose up -d ibkr-broker` (pas besoin de rebuild).

**Validation attendue :** prochain run AG1 V4 16:30 Paris avec BUY US → l'ordre doit passer `submitted_after_confirmation` (plus de parcage) tant que l'écart ≤5 %. Surveiller `core.orders` et les fills.

## F5 — Broker : idempotence des taps tardifs sur approbation

**Problème :** tap Telegram sur une approbation **EXPIRED** (après TTL) ou **NOT_FOUND** (store en mémoire vidé par un restart) → le broker renvoyait **409** → workflow `Order Approval Decide` en erreur (3 err/3j).

**Fix :** `_approval_decision_error` : `EXPIRED` et `NOT_FOUND` renvoient **200** `{status, idempotent: true}`. `BAD_TOKEN` reste 403 ; autres cas restent 409. Déployé dans le même build que F1.

**Rollback :** revert du bloc dans `app.py` + rebuild (peu probable : no-op strict).

## F2 — AG4_Spé-V2 : dates Boursorama corrompues (régression B1)

**Problème :** 235 lignes `news_history` avec `published_at` futur (2029/2030…). Cause racine trouvée : `parseListingDate` (node **S07**, repo `nodes/04_normalize_articles.js`) applique la regex FR **non ancrée** AVANT le parse ISO → une date déjà ISO « 2026-06-29 » est matchée `26/06/29` (le moteur regex prend « 26 » au milieu de « 2026 ») → `2029-06-26`. Le garde B1 de plausibilité (node S16) nullifiait bien ces dates, mais la ligne `publishedAt: publishedAt || j.publishedAt` **réinstaurait la valeur corrompue**. Vérifié sur les 4 variantes (2026-09-24←2024-09-26, 2027-06-26←2026-06-27, 2029-06-26←2026-06-29, 2030-12-19←2019-12-30).

**Fixes (live + miroir repo) :**
1. **S07** (`04_normalize_articles.js`) : parse **ISO d'abord** (si `^\d{4}-\d{2}-\d{2}`), regex FR **ancrée** (`\b…\b`), et clamp de plausibilité `[now-2ans ; now+7j]` → null (`clampPlausibleDate`).
2. **S16** (`07_parse_article.js`) : suppression du bypass — `publishedAt: publishedAt || null`.
3. **S22** (`12_write_news_duckdb.py`) : garde-fou à l'écriture — `published_at > now+24h` → NULL.

**Réparation données :** les 235 lignes ont été **récupérées** par la transformation inverse `vrai = (année=2000+jour_stocké, mois=idem, jour=année_stockée-2000)` avec garde `[2015 ; now+1j]`, sinon NULL. **Backup complet des valeurs d'origine dans la table `news_history_date_repair_20260702`** (même base). Post-fix : 0 date future ; boursorama 7 j valides 69→302.

**Déploiement workflow :** export live → patch (script `/tmp/deploy_20260702/patch_workflows_20260702.py`) → `n8n import:workflow` → `publish:workflow --id=H0cfY1coMx8dvMuXScMc_` → restart n8n+runners. Vérifié : `active=1`, version publiée contient les 3 patchs, déployé AVANT le run de 17:05 Paris.

**Rollback :** réimporter `/tmp/deploy_20260702/ag4spe.json` (export pré-patch) + republier ; données : `UPDATE news_history SET published_at = r.published_at_corrupt FROM news_history_date_repair_20260702 r WHERE news_history.news_id = r.news_id`.

## F3 — Task-runners : timeout 60 s → 1200 s

**Problème :** `AG2 — Universe Health Quarantine` échouait 2 runs/3 (« Task execution timed out after 60 seconds »). `N8N_RUNNERS_TASK_TIMEOUT=1200` était posé côté **n8n** mais PAS côté **task-runners** → le runner applique son défaut de 60 s. Le node unique « Audit + Quarantine » dépasse 60 s depuis l'expansion univers (563 symboles).

**Fix :** `/docker/root/docker-compose.yml`, service `task-runners` : ajout `N8N_RUNNERS_TASK_TIMEOUT=1200` (commentaire daté). Backup pré-patch : `/tmp/deploy_20260702/docker-compose.yml.bak`. Runners recréés (`docker compose up -d task-runners`), env vérifié dans les 3 replicas.

**Validation attendue :** run AG2UHQ ce soir 20:00 Paris → doit finir `success`.

**Rollback :** retirer la ligne + `docker compose up -d task-runners`.

## F4 — Cron AG1-PF MTM décalé à H+15

**Problème :** MTM horaire à H:00 → locks `ag1_v4_consensus.duckdb` avec le run AG1 V4 (14:00 Paris) et la recon (4 erreurs/3 j).

**Fix :** cron `0 0 9-17 * * 1-5` → **`0 15 9-17 * * 1-5`** (workflow `iKnGA9gCMUFZfKYCCsWVF`, import+publish, `active=1` vérifié). `SCHEDULING_AND_LOAD.md` mis à jour.

**Rollback :** réimporter `/tmp/deploy_20260702/ag1pf.json` + republier.

## F7 — (non déployé, enseignement) 78 « orphelins » = paires FX legacy

Le classement WATCHLIST des 78 symboles sans segment a été **testé puis intégralement rollbacké** : ce sont les 78 paires `FX:*` legacy, **volontairement** hors rotation (Forex gelé). Tag de rollback : `reason='audit_20260702_orphan_backfill'` (0 ligne restante, segments revérifiés 282/50/18/7). Règle ajoutée à AGENTS.md : ne pas les classer.

---

## Artefacts de déploiement (VPS)

`/tmp/deploy_20260702/` : `ag4spe.json` + `ag1pf.json` (exports pré-patch = rollback), `*_patched.json`, `patch_workflows_20260702.py`, `docker-compose.yml.bak`. ⚠️ `/tmp` non persistant — les rollbacks workflow peuvent aussi être reconstruits depuis le repo.

## Suivi post-déploiement (J+1)

1. Run AG1 V4 16:30 Paris : BUY US `submitted_after_confirmation` + fills dans `core.fills`.
2. AG4_Spé-V2 17:05 Paris + 08:05 demain : `SELECT COUNT(*) FROM news_history WHERE published_at > now()` doit rester 0.
3. AG2UHQ 20:00 Paris : statut `success`, quarantaine rafraîchie.
4. MTM demain 9:15-17:15 : plus d'erreurs de lock.
5. Prochain tap Telegram tardif : plus d'erreur workflow Decide.
