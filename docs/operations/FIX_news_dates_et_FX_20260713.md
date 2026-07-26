# 2026-07-13 — Correctifs live : dates news Boursorama (S16) + conversion FX manquante (3 writers)

VPS `82.112.242.251`. Backups DB : `/local-files/duckdb/backups/`. Exports workflows patchés : `.codex-tmp/*_DEPLOYED_20260713.json` (+ `*_live_backup_*` pré-patch). Scripts backfill : `.codex-tmp/{fix_dates,fx_backfill2,mtm_backfill}.py`. **À committer.**

## 1) Dates news — node S16 (Parse Article) d'AG4_Spé-V2 (`H0cfY1coMx8dvMuXScMc_`)

**Cause.** La fonction `normalizeDate` de **S16** avait conservé l'ancienne regex FR non ancrée testée **avant** l'ISO et **sans garde-fou de plausibilité** :
`<time datetime="2026-07-10T09:15:05">` → la regex matche `26-07-10` → jour 26 / mois 07 / année 10 → `2010-07-26`. D'où des `published_at` aberrants (jour figé à 26, années 2001→2028 dont futures). Le correctif F2 du 02/07 avait réparé **S07 (listing)** mais **oublié S16 (article)**, qui écrit le `published_at` final. Bug **100 % Boursorama** (Finnhub et IBKR propres).

**Impact.** ~1006 lignes / 20 j mal datées → exclues de la fenêtre 14 j d'AG1 = news analysées pour rien + vraies news récentes perdues.

**Correctifs déployés.**
- Forward : `normalizeDate` remplacée (ISO d'abord + regex FR **ancrée** `\b…\b` 4 chiffres + `clampPlausibleDate` [now-2 ans ; now+7 j] → `null`). Import n8n + réactivation + restart, vérifié actif.
- Backfill : `ag4_spe_v2.duckdb` — **2679 dates Boursorama aberrantes → NULL** (audit conservé dans `news_history_date_repair_20260713`). `NULL` ⇒ repli sur `first_seen_at`. Résultat : 0 date aberrante, 4449 lignes récentes valides conservées, fenêtre 14 j réalimentée (boursorama 1328).
- Consommateur : node **R8** d'AG1 (`AG1V4CONSENSUS`) — le digest texte top-3 filtrait sur `published_at` brut ; patché pour utiliser la date effective plausible (COALESCE CASE plausible → `first_seen_at` …). Le calcul d'impact utilisait déjà ce repli (fix D1). Déployé.

## 2) Écart positions +574 € & dents de scie graphiques — conversion FX absente dans 3 writers

**Cause racine.** `core.instruments.currency` = **'EUR' pour les 31 instruments** (faux). Les valorisations font `market_value_eur = qty × prix_natif` sans conversion. Titres USD (NVDA, CRM, BHP, ADBE, NFLX, PFE, SAP, PDD, AVGO) gonflés de ≈ 1/0,8755 (+14 %). Somme ≈ **+574 €** = l'écart exact. Runs RECON IBKR corrects. Source autoritaire = `main.portfolio_positions_ibkr_latest` (`currency`, `fx_rate`, `last_price_eur` ; USD `fx_rate` = 0,8754804).

**Writers concernés & correctifs (tous déployés) :**
- **A — AG1 V4 Consensus** (`AG1V4CONSENSUS`), node 8 « Build DuckDB Bundle » alimenté par **4C** → écrit `core.positions_snapshot` (runs `RUN_2026*`). Fix dans **4C** : helper `load_fx_ref_map` + bloc gardé (try/except) convertissant `last_price/market_value/unrealized_pnl` des non-EUR via `ibkr_latest`, détection d'échelle anti double-conversion (`avg_price` déjà EUR). node 8 hérite l'EUR via priceMap.
- **B — AG1-PF-V1 MTM horaire** (`iKnGA9gCMUFZfKYCCsWVF`), node PF.07 (`qty*price` natif) → `portfolio_positions_mtm_latest/_history` (qui alimentent la **courbe de perf** du dashboard, source `pf_mtm_history` priorité 10). Fix dans **PF.08B** : helper `_apply_fx_eur(con, rows)` appelé après `ensure_schema` (même logique gardée).
- **C — RECON (PF.00C)** : déjà correct (EUR IBKR), non touché.

**Backfill historique** (conteneur éphémère `python:3.11-slim` + `duckdb==1.5.3`, montage rw ; idempotent) :
- `positions_snapshot` + `portfolio_snapshot` : 34 runs / 89 lignes USD converties (taux RECON le plus proche par run, marqueur `fx_backfill_20260713`, equity/total/pnl/roi recalculés).
- `mtm_history` (473) + `mtm_latest` (8) converties (garde-fou d'échelle vs prix EUR RECON).

**Vérifications finales.** Écart positions courant = **+0,00 €** (dernier run RECON). `mtm_history` par run tous ≈ 7000–7547 € (max légitime = jour à 17 positions), plus aucun run gonflé. Courbe de perf cohérente. 3 workflows actifs, conteneurs sains.

## 3) Réconciliation IBKR figée depuis vendredi (snapshot RECON stale)

**Symptôme.** Le KPI « P&L latent » (prix du jour, MTM yfinance) et le tableau Positions / la « Valeur » (snapshot réconcilié) divergeaient : aucun `RUN_RECON` dans `core.positions_snapshot` depuis vendredi 10/07 19:50, alors que le MTM tournait chaque heure.

**Cause.** Le week-end, la session IBKR est tombée et la gateway (Client Portal, IBeam) s'est retrouvée sur le **compte PAPER `DUQ816375`** au lieu du LIVE `U25651155` (le login LIVE exige la 2FA que l'auto-login ne fait pas). Le node **PF.00C** a un garde-fou (`EXPECTED_ACCOUNT=U25651155` → `raise BLOCKED_PAPER_GATEWAY/ACCOUNT_NOT_ALIGNED`) qui refuse (à raison) d'écrire un snapshot live contre du paper → plus de RECON. Le MTM continuait car il utilise yfinance. `.env` correct (`IBKR_ACCOUNT_ID=U25651155`, `REQUIRE_PAPER=false`).

**Angle mort alerte.** Le monitor du broker (`_maintain_ibkr_session`) ne déclenchait `manual_login_required`/Telegram que sur `reauth_failed`/`gateway_disconnected`/`keepalive_failed` — **jamais sur l'alignement de compte**. Session paper « authentifiée » + keepalive OK → aucune alerte pendant 3 jours.

**Corrections (13/07).**
- **Réalignement LIVE** : restart `ibkr-gateway` (aucun IBeam auto-login dans le conteneur — Java gateway seul) → login manuel via tunnel `ssh -L 5000:127.0.0.1:5000 vps` + `https://localhost:5000` + toggle LIVE + 2FA (Nicolas). → `aligned=true, selected=U25651155, paper=false`. Réconciliation forcée (bump cron `0 * * * * *` le temps d'un run, puis revert `0 15 9-17 * * 1-5`) → nouveau `RUN_RECON…20260713183500`, equity 7141,39 €, **écart positions = +0,00 €**.
- **Prévention (broker `ibkr-broker`, baké — rebuild)** : `_maintain_ibkr_session` vérifie désormais `_account_alignment_status` à chaque cycle ; si `gateway_is_paper=true` ou `aligned=false` → `_mark_manual_login_required("account_not_aligned")` → alerte Telegram. Vérifié : `last_account_alignment` présent dans `/health`, pas de fausse alerte en état aligné. Backup `app.py.bak_align_alert_*`, source synchro repo `services/ibkr-broker/app.py`.

**Rappel opérationnel (mémoire #21).** Reconnexion LIVE = restart gateway + login manuel toggle LIVE + 2FA rapide. Le cron RECON s'arrête à 17:15 Paris (`9-17`) → un décrochage vendredi soir/week-end n'est pas rattrapé avant lundi 09:15 sans intervention.

## 4) Décomposition P&L incohérente (Total ≠ Réalisé net + Latent)

**Symptôme.** KPI dashboard : P&L total net 53,14 € ≠ réalisé net −52,61 € + latent 64,81 € (trou ~41 €).

**Cause.** `net_realized_v4 = gross_realized_v4 − fees_v4` (app.py ~12329), où `fees_v4` = **toutes** les commissions (80,60 €). Or les frais d'entrée des positions **encore ouvertes** (~43 €, `position_lots` status OPEN) sont **déjà absorbés dans le coût de base du P&L latent IBKR** (avg cost IBKR inclut les commissions) → double comptage. Vérifié : IBKR `realizedpnl=0, dividends=0, interest=0`, `unrealizedpnl=62,9` ; identité comptable `Total(53,14) = Réalisé_net + Unrealized(62,9)` ⟹ réalisé net réel = **−9,76 €**, pas −52,61.

**Fix (dashboard `app.py`, backup `app.py.bak_pnl_reconcile_*`).** `net_realized_v4 = total_net_pnl_v4 − latent_pnl_v4` (identité comptable, boucle toujours) + caption expliquant que Fees IBKR (80,60) = ~40,9 € d'entrée sur positions ouvertes (déjà dans le latent) + ~39,7 € sur trades fermés. Vérifié live : −9,76 + 62,90 = 53,14 = total. Redémarrage `root-trading-dashboard-1`, sain.

## Suites recommandées
- **Durcir `core.instruments.currency`** (source unique de devise, ex. depuis IBKR) : c'est la cause de fond qui vivait dans 3 writers. Tant qu'elle reste 'EUR' partout, tout nouveau writer de valorisation ré-introduira le bug.
- **Garde-fou ordres** (proposé, non retenu ce jour) : ajouter le check d'alignement sur `/orders/equity` et `/orders/fx` pour ne jamais trader sur le paper par erreur (aujourd'hui seul le reconcile est protégé).
- Committer côté Windows les workflows patchés + `services/ibkr-broker/app.py` + ce document.
