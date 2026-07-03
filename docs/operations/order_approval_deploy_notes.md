# Déploiement — Approbation humaine des ordres + seuil price-guard (2026-06-16)

Déployé en direct sur le VPS. ⚠️ La source broker est *baked* depuis `/opt/trader-ia/services/ibkr-broker`
(hors git) → **committer ces éléments** pour ne pas les perdre au prochain build.

## 1. Seuil price-guard (ACTIF en live)
`/docker/yfinance/.env` :
```
IBKR_PRICE_GUARD_MAX_DEVIATION_PCT=5.0      # était 3.0
IBKR_PRICE_GUARD_MAX_QUOTE_AGE_SECONDS=3600 # était 28800
```
Recreate : `cd /docker/yfinance && docker compose up -d ibkr-broker`. Vérif : `/health.price_guard.max_deviation_pct == 5.0`.
Backup : `.env.backup_priceguard_20260616T081332Z`. Rollback = remettre 3.0 / 28800.

## 2. Système d'approbation (DÉPLOYÉ, flag OFF)
- Nouveau fichier : `services/ibkr-broker/approval.py` (store PENDING in-memory + éligibilité bande + notify webhook + token + TTL).
- `services/ibkr-broker/app.py` patché :
  - `import approval` + `from fastapi import Body as ApprovalBody` (avant `app = FastAPI(...)`).
  - Hook dans `place_equity_orders`, juste avant le rejet catch-all `_reply_required_error` :
    ```python
    _appr_result = await approval.maybe_park_for_approval(
        confirmation=confirmation, order=order,
        client_order_id=client_order_id, ibkr_payload=ibkr_payload, run_id=req.run_id,
    )
    if _appr_result is not None:
        results.append(_appr_result); continue
    ```
  - 3 endpoints en fin de fichier : `GET /orders/approvals/pending`,
    `POST /orders/approvals/{order_id}/reject`, `POST /orders/approvals/{order_id}/approve`
    (approve = re-fetch prix + re-vérif déviation ≤ max + re-soumission fraîche + confirmation).
  - Backup : `app.py.backup_approval_20260616T082247Z`.
- `/docker/yfinance/docker-compose.yml` (service `ibkr-broker`) + `.env` — env (**ACTIVÉ le 2026-06-16**) :
  ```
  IBKR_APPROVAL_ENABLED=true
  IBKR_APPROVAL_MAX_DEVIATION_PCT=15.0
  IBKR_APPROVAL_TTL_SECONDS=600
  IBKR_APPROVAL_NOTIFY_WEBHOOK_URL=https://n8n.srv961978.hstgr.cloud/webhook/order-approval-request
  IBKR_APPROVAL_REPRICE_ON_APPROVE=true
  ```
  Backups : `docker-compose.yml.backup_approval_*`, `.env.backup_approval_*`, `.env.backup_enable_*`.
- Image rebâtie + déployée ; broker `healthy` ; `/orders/approvals/pending` → `enabled:true`.

### Bandes de déviation
- ≤ 5 % → auto-confirmé (price-guard).
- 5 % < dev ≤ 15 % → **parqué + notif Telegram** (si flag on).
- > 15 % → rejet.

## 3. Workflows n8n (ACTIFS)
- `AG1 V4 — Order Approval Request` (`l94gHBuugVE6c2aX`) : Webhook `POST /webhook/order-approval-request`
  ← broker → message Telegram (credential n8n « Jarvis » `pVqYKOVuJrq3njUz`, **bot réel = @CYROLAS_BOT**)
  vers le **groupe « Gestion outils atelier » `-4887456379`** (chatId = `$json.body.chat_id || "-4887456379"`),
  boutons URL Approuver/Rejeter.
- `AG1 V4 — Order Approval Decide` (`1uDcsNpDyXyAG616`) : Webhook `GET /webhook/order-approval-decide`
  ← tap → `POST http://ibkr-broker:8080/orders/approvals/{id}/{action}` (body `{token}`).
- Choix : **boutons URL** (pas de Telegram Trigger) pour ne pas entrer en conflit avec le consommateur
  d'updates actif du bot (`SIGA - Telegram Collector Buffer v2`).
- ⚠️ Modifier/activer ces workflows via CLI ⇒ `docker restart root-n8n-1` requis pour (ré)enregistrer le webhook.

## 4. Activation — RÉALISÉE le 2026-06-16
- chat_id ciblé = groupe `-4887456379` (« Gestion outils atelier » ; Nicolas seul membre → pas de souci de confidentialité).
- 2 workflows **activés** + `docker restart root-n8n-1` → webhook `order-approval-request` répond `HTTP 200`.
- Webhook branché côté broker (`IBKR_APPROVAL_NOTIFY_WEBHOOK_URL` posé) + `IBKR_APPROVAL_ENABLED=true`.
- **Test notif validé** : POST de démo → message Telegram livré dans le groupe **avec les boutons** (exec `success`).
- Prérequis Telegram réglé : Nicolas a ouvert une conversation avec **@CYROLAS_BOT** (un bot ne peut pas initier un DM).

### Reste à exercer (sur le 1er vrai ordre 5–15 %)
- Chemin **approve → re-price → re-soumission → fill** : non encore testé sur un ordre réel (échoue « fermé »).
  À la 1ʳᵉ occurrence : vérifier la transition `PENDING→FILLED` + `/orders/approvals/pending` sur le VPS.

### Rollback
`IBKR_APPROVAL_ENABLED=false` (ou seuil `IBKR_PRICE_GUARD_MAX_DEVIATION_PCT=3.0`) puis `docker compose up -d ibkr-broker`.

## 6. v2 (2026-06-16) — prix non vérifiable
Découverte sur le run 18985 : 2 ordres US (MSFT, NVDA) consensus 3/3 **rejetés** alors que la déviation
réelle était ~0,1 %. Cause : actions US évaluées **hors séance US** → quote yfinance trop vieux →
guard `QUOTE_TOO_OLD` → rejet, **sans** passer par l'approbation (qui ne gérait que la bande 5–15 %).

Deux correctifs déployés (image rebâtie, broker `healthy`) :
- **Fix 1 — `approval.py`** : parque aussi les cas « prix non vérifiable »
  (`UNVERIFIABLE_REASONS = {QUOTE_TOO_OLD, NO_REFERENCE_PRICE, QUOTE_FETCH_FAILED}`) → notif Telegram
  au lieu de rejet. `config()` expose `unverifiable_reasons`. La notif porte un champ `verification`.
- **Fix 2 — `app.py` (`_price_confirmation_guard`)** : **référence IBKR de secours** quand yfinance est
  périmé/absent (`marketdata_snapshot` warm-up + lecture, parse last/bid/ask via `_ibkr_reference_price`).
  Si IBKR donne un prix → déviation recalculée → auto-confirm/park/reject normal ; sinon → Fix 1 parque.

Comportement résultant sur action US pré-marché : yfinance stale → tentative IBKR → sinon **parqué pour
ton approbation** (plus de rejet silencieux).

⚠️ **Pré-requis** : la **session IBKR doit être authentifiée** (sinon `marketdata_snapshot` et l'envoi
d'ordres renvoient 401). Le 2026-06-17 au matin la session avait sauté (relogin gateway requis).

## 7. v3 (2026-06-17) — prompt IBKR « without market data »
Découverte sur le run manuel `19031` : AG1 V4 génère un consensus 3/3 MSFT avec prix IBKR snapshot frais,
le price-guard confirme correctement le prompt prix, puis IBKR renvoie une seconde confirmation :
`You are submitting an order without market data...`. Avant v3, cette confirmation non-prix tombait en rejet
sec (`IBKR_ORDER_NEEDS_CONFIRMATION`) et ne passait pas par l'approbation.

Correctif :
- `services/ibkr-broker/approval.py` reconnaît uniquement ce prompt explicite sous
  `IBKR_PROMPT_WITHOUT_MARKET_DATA`.
- Le parking n'est autorisé que si la chaîne de confirmation contient déjà une garde prix valide avec
  `deviation_pct <= IBKR_PRICE_GUARD_MAX_DEVIATION_PCT` (bande auto, 5 % en live).
- Les autres confirmations non-prix restent rejetées.
- `config()` expose `prompt_approval_reasons`.

Résultat attendu : si IBKR renvoie ce prompt après une validation prix fraîche, l'ordre est parqué et notifié
Telegram au lieu d'être perdu en rejet silencieux.

### v3.1 (2026-06-17) — approval Telegram répond au prompt IBKR existant
Découverte après clic Telegram sur le run `19035` : le webhook `approve` était bien appelé, mais l'endpoint
broker resoumettait l'ordre avec le même `cOID`. IBKR répondait alors :
`Local order ID=... is already registered`.

Correctif :
- l'entrée approval stocke maintenant la chaîne de confirmation IBKR (`confirmation.terminal_response`) ;
- au clic **Approuver**, le broker répond au `reply_id` IBKR déjà ouvert via `/iserver/reply/{id}` ;
- la resoumission complète via `/orders` reste seulement un fallback si aucun prompt stocké n'est disponible.

### v3.2 (2026-06-26) — resoumission approval avec `cOID` frais + réponse webhook réelle
Découverte sur le run manuel AG1 V4 `RUN_20260626_142628_19506` : clic Telegram reçu, mais l'endpoint
broker retournait `FAILED` car la resoumission utilisait encore le `cOID` de l'ordre initial parqué
(`Local order ID=... is already registered`). Le workflow n8n répondait en plus immédiatement
« Décision enregistrée », avant de connaître le statut broker.

Correctifs déployés :
- `services/ibkr-broker/app.py` génère un `cOID` frais `appr-{uuid}` au moment du clic **Approuver**,
  journalise l'ancien et le nouveau `cOID`, et marque l'approbation `SUBMITTED` au lieu de `FILLED`
  quand IBKR accepte la soumission.
- `AG1 V4 — Order Approval Decide` (`1uDcsNpDyXyAG616`) passe le webhook en `responseNode` et renvoie
  maintenant la réponse réelle du broker après le node HTTP (`APPROVED_SUBMITTED`, `FAILED`, etc.).

## 5. Limites connues (v1)
- Store PENDING **en mémoire** (perdu au restart broker ; OK pour TTL 10 min). Table DuckDB d'audit = évolution.
- Pas de watchdog in-broker : l'expiration se fait à la lecture (`get_for_decision`/`sweep_expired`).
  Option : workflow n8n planifié appelant `/orders/approvals/pending`.
- Token d'approbation passé en query URL (usage unique, TTL court).
