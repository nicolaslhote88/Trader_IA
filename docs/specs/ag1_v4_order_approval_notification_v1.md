# Spec — Approbation humaine des ordres hors-bande (Telegram) — AG1 V4

> Objectif : quand un ordre dépasse la bande d'auto-confirmation du price-guard, **ne plus rejeter
> sèchement** mais **notifier Nicolas sur Telegram** pour qu'il approuve/refuse depuis son téléphone,
> avec re-validation du prix au moment du tap. Compte **LIVE** → défaut = sûr (annulation).
>
> Source vérifiée le 2026-06-16 : `services/ibkr-broker/app.py` (`_confirm_price_prompt_chain` L679,
> `_price_confirmation_guard` L591, `_reply_required_error` L455, `_is_price_confirmation_prompt` L511),
> `cpapi_client.py` (`place_orders` L146, `reply_order`, `marketdata_snapshot` L135).
> Crédential n8n existant : **« Telegram Jarvis Bot »** (`telegramApi`). Hook libre : `IBKR_ALERT_WEBHOOK_URL` (vide).

## 1. Principe — 3 bandes de déviation

Déviation = `|limit_price − ref_price| / ref_price × 100`, `ref_price` = quote `yfinance-api` (côté achat/vente).

| Bande | Condition | Comportement |
|---|---|---|
| **AUTO** | déviation ≤ `IBKR_PRICE_GUARD_MAX_DEVIATION_PCT` | auto-confirme le prompt IBKR (comportement actuel) |
| **APPROBATION** | AUTO < déviation ≤ `IBKR_APPROVAL_MAX_DEVIATION_PCT` (nouveau) | **notification Telegram**, ordre parqué, attente du tap |
| **REJET** | déviation > `IBKR_APPROVAL_MAX_DEVIATION_PCT` | rejet franc (comportement actuel) |

Valeurs proposées (à valider par Nicolas) : `AUTO = 5`, `APPROBATION_MAX = 15`. Réduire aussi
`IBKR_PRICE_GUARD_MAX_QUOTE_AGE_SECONDS` de 28800 (8 h) à ~3600 (1 h) : une référence vieille fausse la déviation.

## 2. Nouveaux paramètres (env broker)

```
IBKR_APPROVAL_ENABLED=true
IBKR_APPROVAL_MAX_DEVIATION_PCT=15.0      # plafond au-delà duquel on rejette même avec humain
IBKR_APPROVAL_TTL_SECONDS=600             # 10 min : au-delà, auto-annulation
IBKR_APPROVAL_REPRICE_ON_APPROVE=true     # re-fetch prix + re-calage du limit à l'approbation
IBKR_APPROVAL_NOTIFY_WEBHOOK_URL=https://<n8n>/webhook/order-approval-request   # n8n
IBKR_PRICE_GUARD_MAX_DEVIATION_PCT=5.0    # bande AUTO (relevée depuis 3.0)
IBKR_PRICE_GUARD_MAX_QUOTE_AGE_SECONDS=3600
```

## 3. Changements côté broker (`ibkr-broker`)

### 3.1 Décision dans le flux d'ordre
Dans `_confirm_price_prompt_chain` / `_price_confirmation_guard` : quand le prompt est un prompt prix
(`_is_price_confirmation_prompt`) **et** que la déviation tombe dans la bande APPROBATION :
- **NE PAS** appeler `reply_order(...)` (laisser le prompt IBKR expirer → aucun ordre live créé — *à
  vérifier explicitement : un prompt non confirmé ne doit PAS laisser d'ordre pendant côté IBKR*).
- Créer une entrée **pending** (cf. 3.2), renvoyer au caller `status="needs_approval"` (au lieu de
  `needs_confirmation`/rejet), avec `order_id`, `client_order_id`, symbole, side, qty, `limit_price`,
  `ref_price`, `deviation_pct`, `expires_at`, `approval_token`.
- Émettre la **notification** : POST `IBKR_APPROVAL_NOTIFY_WEBHOOK_URL` avec ce payload (réutilise le
  pattern de `_send_manual_login_alert`).

### 3.2 Store des ordres en attente
Table mémoire + persistance DuckDB (`core.order_approvals`, cf. §6) :
`order_id, client_order_id, run_id, symbol, side, qty, limit_price, ref_price, deviation_pct,
status (PENDING|APPROVED|REJECTED|EXPIRED|FILLED|FAILED), approval_token, created_at, expires_at,
decided_at, decided_by`.

### 3.3 Nouveaux endpoints
- `GET  /orders/approvals/pending` → liste (debug/watchdog).
- `POST /orders/approvals/{order_id}/approve` (body : `{token}`) :
  1. vérifie token + statut PENDING + non expiré ;
  2. **re-fetch prix courant** (`marketdata_snapshot` ou `/quote`) ;
  3. recalcule la déviation ; si > `APPROVAL_MAX` **maintenant** → refuse (prix a trop bougé) ;
  4. si `REPRICE_ON_APPROVE` → recale `limit_price` sur le marché courant + petit buffer ;
  5. **re-soumet un ordre frais** (`place_orders`) avec `client_order_id` identique (idempotence) et
     auto-confirme son prompt prix (approbation explicite) ;
  6. marque `APPROVED`→`FILLED/FAILED`, renvoie le résultat.
- `POST /orders/approvals/{order_id}/reject` (body : `{token}`) → statut `REJECTED`, rien d'envoyé.
- **Watchdog TTL** : tâche de fond (réutiliser `_keepalive_loop`) qui passe les PENDING expirés en
  `EXPIRED` et notifie (édition du message Telegram).

> ⚠️ Ne **jamais** garder ouvert le `reply_id` IBKR initial pendant l'attente : il expire. L'approbation
> = **nouvelle soumission re-pricée**, pas une réponse au vieux prompt.

## 4. Séquence

```
AG1 V4 (07b) ──POST /orders/equity──▶ broker ──place_orders──▶ IBKR
                                         │  prompt prix, déviation dans bande APPROBATION
                                         │  parque PENDING + token, status=needs_approval
                07b reçoit needs_approval │  POST notify-webhook
                                         ▼
                                   n8n « Order Approval Request » ──▶ Telegram (Jarvis Bot)
                                         message : symbole, side, qty, limit, prix courant, déviation%,
                                         expire dans 10 min  [ ✅ Approuver ] [ ❌ Rejeter ]
   (run AG1 terminé ; ordre noté PENDING_APPROVAL dans core.runs/alerts)

   Nicolas tape ✅/❌ ──callback_query──▶ n8n « Order Approval Callback » (Telegram Trigger)
                                         │ parse order_id+action+token (callback_data)
                                         ├─ approve ▶ POST /orders/approvals/{id}/approve {token}
                                         │            broker re-price + re-soumet + confirme ▶ fill
                                         └─ reject  ▶ POST /orders/approvals/{id}/reject {token}
                                         édite le message Telegram avec le résultat
   (pas de tap en 10 min ▶ watchdog broker ▶ EXPIRED ▶ édition message « expiré »)
```

## 5. Côté n8n (2 workflows + 1 modif)

### 5.1 Modif `07b - IBKR Send Orders`
Si réponse broker `status="needs_approval"` : ne pas traiter comme erreur ; logguer l'ordre en
`PENDING_APPROVAL` (DuckDB) et laisser la notification au broker (webhook) **ou** l'émettre ici via le
node Telegram (au choix — recommandé : broker, pour découpler de la durée du run).

### 5.2 Workflow « Order Approval Request » (si la notif part de n8n plutôt que du broker)
Webhook ← broker → node **Telegram : Send Message** (Jarvis Bot) au `chat_id` de Nicolas, avec
`inline_keyboard` :
```
callback_data = "ordappr|<order_id>|approve|<token>"   et   "ordappr|<order_id>|reject|<token>"
```
Message : symbole, side, qty, **limit vs prix courant + déviation %**, run_id, « expire dans 10 min ».

### 5.3 Workflow « Order Approval Callback »
**Telegram Trigger** (updates `callback_query`) → filtre `callback_data` préfixe `ordappr|` (pour ne pas
collisionner avec les workflows SIGA du même bot) → vérifie `chat_id == NICOLAS_CHAT_ID` →
appelle le broker approve/reject avec le `token` → **answerCallbackQuery** + **editMessageText** avec le
résultat (« ✅ Exécuté à <prix> » / « ❌ Rejeté » / « ⏱ Expiré »).

### 5.4 (option) Watchdog n8n
Si le watchdog n'est pas dans le broker : workflow **Schedule** toutes les 2–3 min →
`GET /orders/approvals/pending` → expire ceux dépassant le TTL.

## 6. Persistance / audit (DuckDB)

Nouvelle table `core.order_approvals` (cf. champs §3.2) dans `ag1_v4_consensus.duckdb`, écrite par le
broker (ou via un node n8n). Tracer chaque transition (PENDING/APPROVED/REJECTED/EXPIRED/FILLED) avec
horodatage et `ref_price`/`deviation_pct` au moment de la demande **et** de la décision.

## 7. Sécurité

- **Token par ordre** (aléatoire, 1 usage) : un `order_id` fuité ne suffit pas à approuver.
- **chat_id en liste blanche** : seul le chat de Nicolas peut décider ; ignorer tout autre expéditeur.
- Webhook n8n d'approbation **non public/non devinable** (chemin + secret) ; le broker valide le token.
- **Idempotence** : `client_order_id` déterministe ; un double-tap ne crée pas 2 ordres (statut déjà décidé).
- **Jamais d'auto-approbation** ; l'action vient toujours du tap de Nicolas. Défaut = annulation au TTL.
- Anti-spam : `IBKR_ALERT_COOLDOWN_SECONDS` existant comme garde.

## 8. Cas limites

- **Prompt IBKR non confirmé laisse-t-il un ordre ?** À vérifier en test : sinon, annuler explicitement
  l'ordre côté IBKR au moment du parcage.
- **Prix bougé au-delà de la bande à l'approbation** → refus avec message clair (ne pas exécuter aveuglément).
- **Run suivant** : si un ordre du même symbole est re-proposé avant décision, ne pas empiler — détecter
  un PENDING existant (même `client_order_id`) et fusionner/ignorer.
- **Session IBKR expirée** au moment de l'approbation → message d'erreur + garder PENDING ou annuler.

## 9. Plan d'implémentation (validé shadow, pas de publication LIVE avant)

1. Broker : env + bande APPROBATION + store PENDING + endpoints approve/reject + watchdog + notify-webhook.
2. DuckDB : créer `core.order_approvals` (writer).
3. n8n : workflow callback (Telegram Trigger) + (option) workflow request + modif `07b`.
4. Test bout-en-bout en **DRY_RUN / paper** d'abord : forcer une déviation dans la bande APPROBATION,
   vérifier notif → tap → re-price → exécution → message édité → ligne `core.order_approvals`.
5. Vérif VPS (côté Claude) : extraction des transitions, conformité, latence tap→fill.
6. Bascule LIVE seulement après runs shadow concluants.

## Fichiers concernés
- `services/ibkr-broker/app.py` (flux ordre, guard, nouveaux endpoints, watchdog, notify)
- `services/ibkr-broker/cpapi_client.py` (`place_orders`, `reply_order`, `marketdata_snapshot` pour le re-price)
- `infra/vps_hostinger_config/docker-compose.yml` (env broker) + `.env`
- n8n : nouveau workflow « Order Approval Callback » (+ éventuel « Request ») ; modif node `07b - IBKR Send Orders`
- DuckDB `ag1_v4_consensus.duckdb` : table `core.order_approvals`
