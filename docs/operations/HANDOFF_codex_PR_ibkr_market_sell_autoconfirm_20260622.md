# HANDOFF Codex — Commit : auto-confirm SELL MARKET (ibkr-broker)

**Date** : 2026-06-22
**Auteur du patch** : session Cowork (Claude) — déjà **déployé en LIVE** sur le VPS, à committer dans le repo.
**Demandeur** : Nicolas.
**Repo** : `nicolaslhote88/Trader_IA` (défaut `main`). La branche `claude/ag4-v3-dualbranch-calib-20260617` est déjà mergée (PR #38) → **partir d'une nouvelle branche depuis `main`**.

---

## 1. Contexte (pourquoi ce patch)

Le run AG1 V4 `RUN_20260619_160005_19227` a généré **SELL LR.PA, order_type=MARKET, qty=3** (intent REBALANCE / action CLOSE).
L'ordre est resté `REJECTED` (`broker_order_id=NULL`), **sans fill et sans notification Telegram**.

**Cause racine** : toute la chaîne price-guard / auto-confirm / approbation Telegram (`services/ibkr-broker/app.py` + `approval.py`) est **LIMIT-only**.
- IBKR a répondu 200 avec 2 prompts : « Market Order Confirmation » + « Confirm Mandatory Cap Price ».
- `_price_confirmation_guard` rejette tout ordre non-LMT (`ORDER_TYPE_NOT_LIMIT`) → jamais auto-confirmé.
- `approval.maybe_park_for_approval` → `_eligible()` renvoie False (raison non éligible + `deviation_pct=None` car pas de prix limite) → **pas de parking, donc pas de notif Telegram**.
- → `_reply_required_error` → REJECTED silencieux.

C'est un **angle mort de conception**, pas un incident d'exécution. Seul ordre MARKET des 30 derniers jours (tous les autres = LIMIT).

**Décision Nicolas** : option (a) **pour les ventes uniquement** → auto-confirmer un SELL MARKET via la chaîne reply existante (sans price-guard de limite, qui n'existe pas pour un MARKET). MARKET BUY reste rejeté.

---

## 2. Modification à committer

**Un seul fichier** : `services/ibkr-broker/app.py` — **3 insertions ciblées, rien supprimé**.
Le fichier dans le repo (dossier de travail) est **déjà patché et byte-identique à la version déployée** :
`sha256 = 506e02e152a146f96041c06515e2845f0b1da8cc571423a5988abdff7d438157`.

Codex n'a donc qu'à **stager/committer le diff de ce fichier** (plus ses propres modifs en cours). ⚠️ Le working tree a du bruit CRLF → ne stager QUE `services/ibkr-broker/app.py` (et les fichiers réellement modifiés par Codex).

### Diff exact

```diff
@@ AUTO_CONFIRM_MAX_STEPS = _env_int("IBKR_AUTO_CONFIRM_MAX_STEPS", 4, minimum=1)
+AUTO_CONFIRM_MARKET_SELL = _env_bool("IBKR_AUTO_CONFIRM_MARKET_SELL", True)
```

```diff
+def _is_market_sell_confirmation_prompt(messages: list[str]) -> bool:
+    """Auto-confirmable IBKR prompts for a MARKET SELL (market-order / mandatory cap price).
+    Excludes any risk-bearing prompt (margin/short/restricted)."""
+    if not messages:
+        return False
+    joined = " | ".join(messages).lower()
+    danger_markers = (
+        "margin",
+        "insufficient",
+        "short sale",
+        "shortable",
+        "locate",
+        "restricted",
+        "not allowed",
+    )
+    if any(marker in joined for marker in danger_markers):
+        return False
+    return (
+        "market order" in joined
+        or "mandatory cap price" in joined
+        or ("fair and orderly market" in joined and "may set a cap" in joined)
+    )
+
+
 def _is_price_confirmation_prompt(messages: list[str]) -> bool:
```

```diff
     if not AUTO_CONFIRM_PRICE_WARNINGS:
         guard["reason"] = "AUTO_CONFIRM_PRICE_WARNINGS_DISABLED"
         return guard
+    _side = str(ibkr_payload.get("side") or "").upper()
+    _is_market = normalize_order_type(ibkr_payload.get("orderType")) == "MKT"
+    if _is_market:
+        if (
+            AUTO_CONFIRM_MARKET_SELL
+            and _side == "SELL"
+            and _is_market_sell_confirmation_prompt(messages)
+        ):
+            guard["ok"] = True
+            guard["reason"] = "MARKET_SELL_AUTO_CONFIRM"
+            return guard
+        guard["reason"] = "ORDER_TYPE_NOT_LIMIT"
+        return guard
     if not _is_price_confirmation_prompt(messages):
         guard["reason"] = "PROMPT_NOT_PRICE_CONFIRMATION"
         return guard
```

---

## 3. Comportement & garde-fous (à conserver / ne pas régresser)

- **SELL MARKET** déclenchant les prompts IBKR → auto-confirmé par `_confirm_price_prompt_chain` (boucle reply existante, ≤ `AUTO_CONFIRM_MAX_STEPS`) → envoyé. Statut résultat : `submitted_after_confirmation`.
- **MARKET BUY** → toujours rejeté (`ORDER_TYPE_NOT_LIMIT`), non couvert par la décision.
- **Exclusion dure** des prompts à risque (`margin/insufficient/short sale/shortable/locate/restricted/not allowed`) → pas d'auto-confirm.
- **Préflight SELL** déjà en place (`held_qty >= qty`) → jamais de vente à découvert ; l'auto-confirm ne fait que réduire/clôturer une position détenue.
- **Couplage** : la branche est placée **après** le check `AUTO_CONFIRM_PRICE_WARNINGS` → l'auto-confirm market-sell est gouverné par ce master switch (=`true` sur le VPS). Voulu (un seul interrupteur). Le rendre indépendant = remonter la branche avant ce check.
- **Kill-switch** : `IBKR_AUTO_CONFIRM_MARKET_SELL` (défaut `True`). Rollback = `false` + restart, ou restaurer le backup.

---

## 4. État de déploiement (déjà fait, LIVE)

- Source canonique VPS : `/opt/trader-ia/services/ibkr-broker/app.py` (patché). ⚠️ **pas un clone git** → d'où ce handoff.
- Build/up : `cd /docker/yfinance && docker compose build ibkr-broker && docker compose up -d ibkr-broker`. Container `Up (healthy)`.
- Vérifs OK : `app.AUTO_CONFIRM_MARKET_SELL == True`, helper présent, `/health` → `authenticated:true`, compte `U25651155` aligné, `dry_run:false`.
- **Backup VPS** : `/opt/trader-ia/services/ibkr-broker/app.py.bak_market_sell_autoconfirm_20260619_150210`.
- **Pas encore validé sur un vrai SELL MARKET en séance** → à surveiller au prochain ordre de ce type.

---

## 5. À faire par Codex

1. Nouvelle branche depuis `main` (ex. `fix/ibkr-broker-market-sell-autoconfirm-20260622`).
2. Committer le diff de `services/ibkr-broker/app.py` ci-dessus (+ ses propres modifs en cours, commits séparés et lisibles).
3. Documenter le nouvel env `IBKR_AUTO_CONFIRM_MARKET_SELL` dans `docs/operations/env_vars.md` (défaut True, couplé à `AUTO_CONFIRM_PRICE_WARNINGS`).
4. Ouvrir la PR vers `main`.

## 6. Optionnel (non fait, recommandé)

Patch séparé : **notifier (Telegram / AG4_Spé Health Alert) tout ordre REJECTED `IBKR_ORDER_NEEDS_CONFIRMATION`** (MARKET BUY, cas à risque) pour éliminer les rejets silencieux restants.

## 7. Point de vigilance hors-scope

`date -u` sur le VPS renvoyait **Jun 19** alors que la date réelle est **2026-06-22** (≈3 j d'écart apparent) — d'où le backup nommé `..._20260619_...`. À vérifier (horloge/tz VPS) car ça impacterait timestamps d'ordres, TTL d'approbation et session IBKR.
