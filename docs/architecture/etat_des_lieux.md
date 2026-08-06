# État des lieux fonctionnel — Trader_IA

**Dernière consolidation du socle live : 2026-08-06.**
**Méthode :** audit exhaustif du 2026-07-02, puis vérifications live ciblées les
2026-08-05/06 (broker `/health`, approbations, SQLite n8n, DuckDB en lecture
seule, replays AG1/AG2 et contexte global). Les métriques de portefeuille et de
volume explicitement datées du 02/07 restent des snapshots historiques.
**Document compagnon :** [`../audits/20260702_audit_complet_projet.md`](../audits/20260702_audit_complet_projet.md) (constats, priorités F1-F10).
**Pour les issues historiques :** [`historique_issues.md`](historique_issues.md). **Point d'entrée opérationnel du projet :** `AGENTS.md` (racine).

---

## 1. Résumé exécutif

Trader_IA est une **plateforme multi-agents de trading actions/ETF en LIVE réel** sur IBKR (compte `U25651155`), orchestrée par **n8n** sur un VPS Hostinger, avec **DuckDB** comme source de vérité, un **broker FastAPI** devant l'API IBKR Client Portal, une **approbation d'ordres via Telegram**, et un **dashboard Streamlit**.

Le cœur décisionnel est **AG1 V4 Consensus** : trois LLM (GPT-5.6 Sol,
DeepSeek V4 Pro, Claude Opus 4.8) proposent chacun des décisions de portefeuille ;
une règle de **consensus 2/3** filtre ce qui part à l'exécution, derrière un Risk
Manager déterministe et un preflight liquidité IBKR. Il tourne **2×/jour ouvré :
14:00 Paris (Euronext) et 16:30 Paris (US ouvert)**.

Les piliers d'analyse sont alimentés par des workflows autonomes : **AG2-V3** (technique, yfinance, split Held+Core / Watchlist), **AG3-V2** (fondamental, yfinance pur sans LLM, split Held+Core / Watchlist), **AG4-V3** (news macro), **AG4_Spé** (news par valeur, 3 sources : Boursorama, IBKR portfolio, Finnhub global).

**Le Forex est entièrement gelé** (workflows FX inactifs, `fx_orders_enabled=false`, bases conservées). L'ancien ensemble AG1-V3 (3 DuckDB parallèles) est décommissionné au profit du ledger unique V4.

**Contexte global validé les 2026-08-05/06.** AG5–AG8 réhabilités, synthèse atomique,
dashboard commun et pack AG1 strictement consultatif sont déployés live.
Production : `GLOBAL_CONTEXT_ENABLED_COMPONENTS=AG5,AG6,AG7,AG8`. AG9 World
Monitor reste en sommeil et ne participe ni aux poids ni à la fraîcheur. Les
hashes consensus/safety/broker sont inchangés. Après remédiation des sources,
le pack représentatif est `OK`, `use_policy=CAUTION`, couverture `0,908` et
confiance `0,685`. Voir
`global_context_architecture.md` et
`../operations/20260805_ag5_ag8_global_context_live_deploy.md`.

Snapshot historique au 2026-07-02 : NAV **9 921,80 €** (−0,78 % depuis le
départ à 10 000 €), 7 positions et 13 fills. Le blocage des prompts IBKR sans
market data a depuis été corrigé : auto-confirmation seulement si le garde prix
yfinance valide une déviation ≤5 % ; sinon l'approbation Telegram reste requise.

---

## 2. Architecture technique (VPS `srv961978`)

### 2.1 Deux stacks Docker Compose

**Stack n8n — projet `root`, `/docker/root` :**

| Service | Rôle | Exposition |
|---|---|---|
| `root-traefik-1` | Reverse proxy TLS | 80/443 publics |
| `root-n8n-1` | Orchestrateur workflows (n8n 2.3.5, runners externes) | via Traefik |
| `root-task-runners-3/4/5` | Runners Python/JS sandboxés | interne |
| `root-trading-dashboard-1` | Dashboard Streamlit (V4-only, port 8501) | via Traefik + BasicAuth |
| `root-toolbox-1` | Debug (curl/jq) | interne |

**Stack marché/broker — projet `yfinance`, `/docker/yfinance` :**

| Service | Rôle | Exposition |
|---|---|---|
| `ibkr-gateway` | IBKR Client Portal Gateway (IBeam bridé : maintien de session, pas de login auto LIVE) | 127.0.0.1:5000-5001 |
| `ibkr-broker` | **FastAPI broker maison** (`services/ibkr-broker/`) : ordres, price-guard, approbations, recon, news portfolio | 127.0.0.1:18080 → :8080 |
| `yfinance-api` | API marché Yahoo (history/quote/options/fundamentals, cache disque) | interne :8080 |
| `yf-enrichment` | FastAPI qui exécute `daily_enrichment.py` | interne :8081 |
| `macro-data-api` | FRED/COT/taux (héritage FX, sert AG4) | interne :8081 |
| `global-context-synthesizer` | synthèse atomique AG5–AG8 et pack consultatif AG1 | interne :8083 |
| `worldmonitor-adapter` | adaptateur AG9, conteneur arrêté tant qu'AG9 est dormant | non exposé en production |

Hors projet mais sur le même hôte : `hermes-*`, `siga-dashboard`, `voice-gateway`, `portainer`. La n8n est **partagée** avec SIGA — filtrer les workflows `AG*`/`YF*`.

### 2.2 Persistance et conventions critiques

- **DuckDB** : hôte `/local-files/duckdb/` = `/files/duckdb/` côté containers. **Un seul écrivain par fichier** ; lecteurs en `read_only=True` obligatoire.
- ⚠️ **Versions duckdb par base** : `ag2_v3` s'écrit en **duckdb 1.4.4 max** (lecteur le plus bas = yf-enrichment) ; `ag4_spe_v2` et `ag1_v4_consensus` s'écrivent en **≤1.4.3** (lecteur task-runner n8n). Ne jamais upgrader le format.
- **n8n** : crons en fuseau **Europe/Paris** ; base SQLite `/home/node/.n8n/database.sqlite` (volume `n8n_data`, 1,1 Go au 02/07). La version **publiée** d'un workflow (`activeVersionId`) est celle exécutée — déployer = `import:workflow` puis `publish:workflow` puis restart n8n + runners.
- **Broker = image baked** : source `/opt/trader-ia/services/ibkr-broker/` (pas un clone git) → éditer, `docker compose build ibkr-broker && up -d`, **puis committer dans le repo** (vérifié synchronisé au 02/07).
- **Login IBKR quotidien assisté** : timer systemd `ibkr-daily-auth.timer` à 07:00 Paris, validation IB Key par Nicolas 07:00-07:30 ; reauth auto (`auto_reauth_enabled=true`) en journée ; fallback login navigateur manuel (`ssh -L 5000:localhost:5000 vps`). Procédure de reconnexion fiable : mémoire projet `21_IBKR_RECONNECT_PROCEDURE`.

---

## 3. Chaîne fonctionnelle — les agents

### 3.0 Taxonomie
| Rôle métier | Implémentation actuelle |
|---|---|
| Univers | table `ag2_v3.universe` + `universe_segments` + quarantaine (`AG2UHQ`) ; outillage `outils/AG0-V1`, `outils/scripts/seed_universe_100_global.py` |
| Portfolio Manager | **AG1 V4 Consensus** (workflow `AG1V4CONSENSUS`) |
| Analyste technique | AG2-V3 (split Held+Core / Watchlist) |
| Analyste fondamental | AG3-V2 (split Held+Core / Watchlist) |
| Analyste news/sentiment | AG4-V3 (macro) + AG4_Spé-V2/IBKR/Finnhub (par valeur) |
| Risk Manager | nodes déterministes intégrés à AG1 V4 (safety node 7, gates R8/matrice) |
| Execution Trader | `ibkr-broker` + workflows Approval Request/Decide |

### 3.1 Univers et segmentation (règle structurante)
- `universe` : **563 symboles** (extension +100 mondiale du 24/06 : US/Europe/Asie/EM, ADR privilégiés).
- **La rotation AG2/AG3 est pilotée par `universe_segments`** (PK `(symbol, segment)`) : `HELD` (7), `CORE_AUTO` (50, score dominé par le volume), `CORE_MANUAL` (18 épinglés : ASML, TSM, AMD, NVO…), `WATCHLIST` (282). **Un symbole sans segment n'est jamais analysé** (au 02/07 : 206 sans segment = 128 en quarantaine + **78 paires `FX:*` legacy volontairement hors rotation**, Forex gelé — cf audit F7, résolu).
- **Quarantaine** (`AG2UHQ`, 20:00 Paris L-V) : audite la qualité de données et exclut les symboles malades de la rotation non détenue ; 128 actives au 02/07. AG1 exclut les entrées/renforts quarantainés, les positions détenues restent surveillées.
- AG1 (R8) et yf-enrichment lisent **tout** `universe` (hors quarantaine pour les décisions d'entrée).

### 3.2 AG2-V3 — pilier technique
- Source : `yfinance-api` (H1/D1), indicateurs + validation
  `deepseek-v4-pro` (verdict `ai_decision`/`ai_quality`/`ai_rr_theoretical`),
  base **`ag2_v3.duckdb`** — écrite par AG2, lue par AG3/AG4/AG1/dashboard.
- Split live : `AG2-V3 — Technical Held+Core` (9/13/15h Paris L-V, ~23-32 min) et `AG2-V3 — Technical Watchlist Nightly` (22h/02h, 40 symboles/slot → cycle ~3,5 j).
- **Hybride AG2→AG1 (19/06)** : AG1 consomme le verdict AG2 — REJECT = filtre dur sur « Entrer/Renforcer », APPROVE/WATCH pondérés par qualité, SKIP neutre.
- La perte de `batch_info` qui figeait les rotations en faux succès a été
  corrigée le 06/08. Le run manuel post-correction Held+Core `20812` a traité 27/27 symboles,
  avancé `0 → 18`, et 538/563 derniers signaux avaient alors des âges H1/D1
  stockés ≤96 h.

### 3.3 AG3-V2 — pilier fondamental
- **Source unique yfinance, zéro LLM.** Base `ag3_v2.duckdb` (3,4 Go) : `fundamentals_snapshot`, `analyst_consensus_history`, triage.
- Split live : Held+Core (01:00 UTC quotidien, ≈56 symboles, SLA ≤24 h) et Watchlist (02:00/04:00 UTC, ≈196 symboles, batch 60 → cycle ~4 j, SLA <5 j).
- **Gate STALE_FUNDA** (AG1 R8) : funda >168 h (`AG1_ACTIONS_MAX_FUNDA_AGE_HOURS`) → neutralisé (scores à 50, `Funda_Usable=False`), sans geler le trading.
- Au 02/07 : 524 symboles couverts, âge moyen 14,2 j, 357 frais ≤7 j. Limites structurelles : ~40 % des small caps FR sans analystes ; IBKR ne peut pas remplacer yfinance (tags fondamentaux CP API dépréciés).

### 3.4 AG4 — pilier news
- **AG4-V3 News Watcher** (macro, RSS + LLM dual-branch Grok/gpt-5-mini via node `20CFG`, mode `reduced` par défaut) → `ag4_v3.duckdb` ; fournit le régime macro à AG1.
- **News par valeur** → base commune **`ag4_spe_v2.duckdb`** (`news_history` + vue `news_analyzed` = summary ∧ is_relevant) :
  - `AG4_Spé-V2` : scraping **Boursorama** (FR), 4 créneaux L-V, rotation priorisée portefeuille ;
  - `AG4_Spé-IBKR-V1` : news **IBKR portfolio** (positions détenues, provider Benzinga) via broker `GET /news/portfolio` — l'endpoint per-contrat IBKR est indisponible (503) ;
  - `AG4_Spé-Finnhub-V1` : couverture **globale** CORE_MANUAL+CORE_AUTO via collecteur host `/opt/trader-ia/finnhub/` (cron 9/12/15h) + mapping ADR (NESN.SW→NSRGY…), cap 12 art./symbole ; ~95/100 couverts, 5 résiduels (ABB, 6861.T, CBA.AX, MQG.AX, O39.SI).
  - Volumes 7 j au 02/07 : finnhub 1 457, ibkr 350, boursorama 69 valides.
    Les 235 dates futures observées ce jour-là ont été réparées et les parseurs
    ISO/clamp corrigés le 02/07.
- Depuis le 30/07, les trois analyses par valeur Boursorama/IBKR/Finnhub
  utilisent `deepseek-v4-pro` avec chaîne LangChain et parseur structuré. Les
  collecteurs, crons, déduplication et schémas DuckDB sont inchangés.
- **D2** : node `20K — News Digest` injecte dans l'`opportunity_pack` d'AG1 les news ≤14 j (top 3/symbole + `held_news`). `AG4_Spé — Health Alert` (16:30 Paris) alerte Telegram si pipeline stale.

### 3.5 AG1 V4 Consensus — décision
- Workflow `AG1V4CONSENSUS`, **2 crons : 14:00 et 16:30 Paris L-V** (le 16:30 rend les US tradables : à 14:00 le NYSE est fermé → cotations figées → gate liquidité les bloque, comportement normal).
- Pipeline interne : R8 (préparation données, fraîcheurs H1≤96 h / D1≤96 h / YF≤72 h / funda≤168 h, `data_age = max(stocké, réel)`, exclusion quarantaine, verdicts AG2, STALE_FUNDA) → `Calcul Matrice & Briefing` (prob_score `0.36 tech + 0.34 funda + 0.20 news + 0.10 régime` ; **risk_score V2** renormalisé sur composantes observées, pondération tactique vol/liq/event ; grades A/B/C par quantiles ; règle `enter_core` ; stop-fallback ≥ plancher ATR) → 3 LLM en parallèle → **consensus 2/3** → safety node 7 (Risk Manager déterministe) → **preflight liquidité IBKR** (warm-up snapshot jusqu'au bid/ask, `SPREAD_UNQUOTED` toléré sur noms prouvés liquides) → envoi broker.
- Ledger **`ag1_v4_consensus.duckdb`** : `core.runs/orders/fills/consensus_*/model_proposals/positions_snapshot/portfolio_snapshot/…` (17 tables). 99 runs au 02/07 ; `strategy_version`/`prompt_version`/`n8n_execution_id` renseignés sur les runs récents.
- Modèles : `gpt-5.6-sol`, `deepseek-v4-pro`, `claude-opus-4-8`. Les
  `model_keys` persistés restent `chatgpt52`, `grok41_reasoning` et
  `claude_sonnet46` pour compatibilité historique ; `model_name`/`model_id`
  portent l'identité réelle.
- Le pack `AG1_GLOBAL_CONTEXT_LLM_V2` est attaché avant le fan-out, identique
  pour les trois branches, compacté à 4 000 caractères et strictement
  consultatif. Le run manuel `20808` a validé le chemin enrichi sans modifier
  consensus, safety ou broker.
- **AG1-PF-V1 MTM** : valorisation horaire (9-17h Paris) + runs de recon IBKR (`RUN_RECON_IBKR_PF_*`) — IBKR est la **source de vérité unique** du P&L (flag `ibkr_is_source_of_truth`).

### 3.6 Exécution — broker IBKR + approbation Telegram
1. AG1 V4 envoie les ordres au broker (`dry_run=false`, LIVE).
2. **Price-guard** (réf. yfinance, âge ≤1 h) : écart limit↔réf **≤5 %** auto-confirmé · **5-15 %** parqué pour approbation · **>15 %** rejeté. Un prix non vérifiable (`QUOTE_TOO_OLD`, `NO_REFERENCE_PRICE`) est parqué. Le prompt IBKR sans market data est auto-confirmé seulement si la référence indépendante respecte la bande ≤5 % ; sinon il suit le circuit d'approbation/rejet.
3. **Approbation** : bot Telegram `@CYROLAS_BOT` (boutons Approuver/Rejeter, **TTL 600 s**) → workflows `Order Approval Request` / `Decide` → à l'approbation, **re-soumission fraîche** de l'ordre (fix 18/06) ; `Update Ledger Status` met à jour `core.orders`. Double-tap → 200 idempotent. ⚠️ Store des approbations **en mémoire** (perdu si restart broker).
4. Limites connues : le store des approbations reste en mémoire et est perdu au redémarrage du broker ; hors séance une approbation finit `FAILED` proprement — approuver en séance. Les SELL MARKET disposent désormais de l'auto-confirmation dédiée `IBKR_AUTO_CONFIRM_MARKET_SELL`.
- **Constat historique au 02/07 (audit F1)** : l'absence de souscription market
  data US déclenchait un prompt IBKR et des expirations. Corrigé le même jour :
  le prompt « without market data » est auto-confirmé uniquement si la garde
  prix indépendante valide l'écart ≤5 % ; sinon l'ordre reste soumis à
  approbation.

### 3.7 Dashboard Streamlit (V4-only, 8501)
- Lit les DuckDB (+ yfinance pour certains graphes) ; **réimplémente** le scoring/gates d'AG1 (matrice « Vue consolidée », funnel « System Health ») — **parité obligatoire**, voir `docs/operations/SYSTEM_LINKS_AND_PARITY.md` (source de vérité, à consulter avant toute modif scoring/gates/seuils de fraîcheur).
- Le dashboard montre l'étape **décision matrice**, PAS le preflight IBKR (étape exécution, verdict au moment de l'ordre).
- La version live `/opt/trading-dashboard/app/app.py` a été resynchronisée dans
  `services/dashboard/app.py`. La règle de parité reste obligatoire pour tout
  changement de scoring, gate ou fraîcheur.

### 3.8 Forex — trading gelé
Les workflows de trading AG1-FX/AG2-FX/AG3-FX/AG4-FX restent inactifs et
`IBKR_FX_ORDERS_ENABLED=false`. AG5–AG8 ont été réactivés comme producteurs
analytiques communs sans nœud broker ni ordre Forex. Les bases `ag*_fx_v1.duckdb`
et `ag4_forex_v1.duckdb` restent figées.

---

## 4. Ordonnancement (source de vérité : `docs/operations/SCHEDULING_AND_LOAD.md`)

Résumé des crons actifs (heure Paris) après déconfliction anti-contention du 28/06 :

| Créneau | Workflow | Base écrite |
|---|---|---|
| 22:00 / 02:00 (7j/7) | AG2 Watchlist Nightly | ag2_v3 |
| 00:00 UTC / 01:00+04:00 UTC (7j/7) | AG3 Held+Core / Watchlist | ag3_v2 |
| 06:15 (7j/7) | YF-ENRICH | yf_enrichment |
| 06:45 / 10:45 / 18:45 (L-V) | AG4-V3 macro | ag4_v3 |
| 08:05 / 11:05 / 14:05 / 17:05 (L-V) | AG4_Spé-V2 Boursorama | ag4_spe |
| 09:00 / 13:00 / 15:00 (L-V) | AG2 Held+Core | ag2_v3 |
| 07:20 / 07:40 / 08:00 / 08:20 (L-V) | AG5 / AG6 / AG7 / AG8 | macro_data |
| 10:05 / 13:05 / 16:05 (L-V) | Global Context Synthesizer | global_context_v1 |
| 10:00 / 13:00 / 16:00 (L-V) | AG4_Spé Finnhub + IBKR news | ag4_spe |
| 9h-17h horaire (L-V) | AG1-PF MTM | ag1_v4 |
| **14:00 + 16:30 (L-V)** | **AG1 V4 Consensus** | ag1_v4 |
| 16:30 (L-V) | AG4_Spé Health Alert | — |
| 20:00 (L-V) | AG2 Universe Quarantine | ag2_v3 |

Règles : un seul écrivain par base ; écart ≥ durée_max + marge entre écrivains d'une même base ; nodes <20 min (timeout tâche 1200 s) ; ne pas déplacer AG1 V4 sans décision explicite. Contention résiduelle connue sur `ag1_v4` (MTM vs AG1 V4 14:00 / recon, audit F4) ; retry-hardening des `db_con` proposé, non déployé.

---

## 5. Bases DuckDB (au 2026-07-02)

| Base | Taille | Rôle | Écrivain |
|---|---|---|---|
| `ag1_v4_consensus.duckdb` | 188 Mo | Ledger V4 (runs, orders, fills, consensus, MTM) | AG1 V4 + MTM/recon (≤1.4.3) |
| `ag2_v3.duckdb` | 361 Mo au 06/08 après maintenance | Univers, segments, quarantaine, signaux techniques | AG2 (1.4.4) |
| `ag3_v2.duckdb` | 3,4 Go | Fondamentaux, consensus analystes | AG3 |
| `ag4_v3.duckdb` | 1,5 Go | News macro | AG4-V3 |
| `ag4_spe_v2.duckdb` | 587 Mo | News par valeur (3 sources) | AG4_Spé ×3 (≤1.4.3) |
| `yf_enrichment_v1.duckdb` | 38 Mo | Enrichissement quotidien | YF-ENRICH |
| `macro_data.duckdb` | 40 Mo | FRED/COT/taux | macro-data-api |
| `worldmonitor_v1.duckdb` | AG9 en sommeil, aucune base live requise | AG9 futur | — |
| `global_context_v1.duckdb` | créée le 2026-08-05 | snapshot canonique AG5–AG8 et pack AG1 | global-context-synthesizer (1.4.3) |
| `ag*_fx_*.duckdb`, `ag4_forex_v1` | ~1,2 Go cumulés | Forex gelé | — (figées) |

Pièges transverses : vue `SELECT *` cassée par tout `ALTER TABLE ADD COLUMN` (recréer la vue) ; base souvent lockée par le dashboard → retry sur lock ; sandbox Python n8n (imports whitelistés, un par ligne, `hashlib` interdit) ; éditeurs qui tronquent les gros fichiers (>~160 lignes) → patcher via shell.

---

## 6. Sécurité & garde-fous (revérifiés live le 06/08)

- `IBKR_DRY_RUN=false`, `AG1_ACTIONS_LIVE_ORDERS_ENABLED=true` → **ordres réels**. Ne modifier aucun garde (`IBKR_REQUIRE_PAPER_ACCOUNT=false`, `IBKR_PRICE_GUARD_MAX_DEVIATION_PCT=5.0`, `IBKR_APPROVAL_ENABLED=true`) ni réactiver le FX sans décision explicite de Nicolas.
- Jamais placer/confirmer un ordre à la main : chaîne n8n + broker + approbation Telegram uniquement.
- Lectures DuckDB en `read_only=True` ; pas d'écriture directe hors scripts de maintenance dédiés.
- Secrets : `.ssh/` local gitignoré ; credentials dans `/docker/*/.env` (jamais dans le repo) ; 1 seul credential Telegram n8n (« Jarvis », bot `@CYROLAS_BOT`) — ne pas ajouter de 2ᵉ Telegram Trigger.
- Workflows live : valider en shadow/replay avant publication ; déploiement chirurgical + backups `.codex-tmp/` ; ⚠️ `import:workflow` désactive le workflow → toujours republier et vérifier `active=1`.

---

## 7. État opérationnel & écarts (2026-08-06)

**Sain et validé :** broker authentifié et aligné sur le compte live, aucune
approbation en attente au contrôle ; AG1 V4 à trois modèles conforme ; AG5–AG8
et synthèse actifs avec pack consultatif `OK/CAUTION`; AG9 dormant ; Forex
inactif ; AG2 Held+Core naturel post-correction réussi avec avance du curseur ;
dashboard et sources repo synchronisés.

**Écarts historiques du 02/07 désormais corrigés :** prompt IBKR sans market
data sous garde prix, dates Boursorama, timeout runner AG2UHQ, décalage MTM,
idempotence des approbations expirées et divergence dashboard.

**Limites encore assumées :**

1. AG9 ne produit aucune donnée réelle sans accès World Monitor payant ; il est
   explicitement exclu de la couverture et des poids.
2. Le store des approbations reste en mémoire et est perdu au restart broker ;
   un clic tardif/perdu répond toutefois de façon idempotente.
3. Les séries macro annuelles peuvent légitimement donner une fraîcheur
   `aging`; leur fréquence, proxy et confiance restent visibles.
4. Le premier cycle Watchlist naturel après la correction de rotation est à
   observer ; son chemin exact a déjà passé le replay live 5/5.

---

## 8. Renvois documentaires

- Index : `../README.md`.
- Opérations : `SCHEDULING_AND_LOAD.md` (crons — source de vérité), `SYSTEM_LINKS_AND_PARITY.md` (parité dashboard↔AG1 — source de vérité), `deploy.md`, `env_vars.md`, `ibkr_execution.md`, `vps-access.md`, `runbook_n8n_investigation.md`, `order_approval_deploy_notes.md`, `20260806_ag5_ag8_data_quality_remediation.md`, `20260806_ag2_batch_rotation_cursor_fix.md`.
- Audits : `20260702_audit_complet_projet.md` (ce jour), `20260622_ag3_v2_analysis.md`, `20260619_ag2_v3_analyse_pertinence_efficience.md`, `20260617_ag4_spe_v2_analysis.md`, `20260617_ag4_v3_news_watcher_audit.md`.
- Specs : `ag1_v4_consensus_actions.md`, `ag1_v4_d2_news_digest.md`, `ag4_spe_v3_ibkr_news.md`, `ag1_v4_order_approval_notification_v1.md`.
- Déploiements 06/2026 : notes `20260619_*` (hybride AG2, quarantaine, split rotation), `20260622_*` (AG3 split + STALE_FUNDA), `20260624_*` (expansion +100, Finnhub, durcissement AG2HC, auth quotidienne assistée).
