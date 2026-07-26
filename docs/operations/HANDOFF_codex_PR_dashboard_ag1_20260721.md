# HANDOFF Codex — PR « Dashboard cascade + Sparkline FX + AG1 V4 règles & swaps modèles » (2026-07-21)

Brief pour Codex : commit + push + PR. **Tout est déjà déployé et vérifié en live** ; cette PR
resynchronise le repo sur l'état VPS et documente les changements.

## Contexte git
- Branche courante : `codex/repo-cleanup-20260703`.
- Dernière version poussée sur GitHub : `8eabf27` (merge PR #41, `codex/live-trading-sync-20260629`).
- ⚠️ **Working tree ~369 fichiers modifiés dont l'essentiel = bruit CRLF** (cf. AGENTS.md §20).
  **NE STAGER QUE LES 7 FICHIERS LISTÉS CI-DESSOUS.** Vérifier avec `git diff --cached --stat`
  qu'aucun autre fichier n'entre dans le commit.

## Fichiers à stager (exactement ceux-ci)
```
services/dashboard/app_modules/waterfall.py                 # NOUVEAU
services/dashboard/app_modules/visualizations.py            # sparkline EUR/USD
services/dashboard/app.py                                   # câblage cascade + resync prod
outils/scripts/broker_costs_collector.py                    # NOUVEAU (collecteur coûts broker)
agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/AG1_workflow_v4_consensus.json
agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/nodes/post_agent/07_validate_enforce_safety_v5.code.js
agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/nodes/pre_agent/4C_enrich_portfolio_with_market_prices.code.py
agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/nodes/agent_input/agent_1_portfolio_manager.node.json
```
Astuce anti-CRLF : `git add -- <chemins ci-dessus>` puis `git status --porcelain --cached`.

## Changements — faits validés

### A. Dashboard (déployé sur `root-trading-dashboard-1`, `/opt/trading-dashboard/app`, health 200)
1. **`app_modules/waterfall.py` (nouveau)** — « Cascade de valeur détaillée » de l'onglet Rendement (actif)
   → 1) Rendement Financier. 12 postes : Capital initial, P&L brut réalisé clos, Frais sur réalisé clos,
   P&L brut réalisé partiel, Frais de change (trades passés), Frais sur réalisé partiel, P&L brut latent
   (prix), Frais simulés sur vente latent, **Résultat intermédiaire**, Impact FX latent si liquidé EUR,
   **Conversion devises → EUR (sortie)**, **Résultat final (sortie totale EUR)**. Postes étiquetés
   réel / estimé (`· est.`) / simulé (`· sim.`). Barème frais IBKR (US 0,005 $/action min 1 $ ;
   Euronext 0,05 % min 1,25 €) ; frais de change ~0,2 bps min 2 $. Axe Y **tronqué** (zoom variations) +
   valeurs cumulées affichées sur les barres bleues.
2. **`app_modules/visualizations.py`** — sparkline dédiée **EUR/USD** en tête des Portfolio Sparklines :
   tirets verts = achat d'un titre USD (achat de dollars), rouges = vente d'un titre USD (retour euros).
   Helpers `_is_usd_symbol` / `_build_currency_map` / `_extract_usd_flow_events` / `_build_eurusd_flow_sparkline`.
3. **`app.py`** — remplacement du bloc « Cascade de valeur » par l'appel au module `waterfall`.
   ⚠️ Ce fichier a AUSSI été **resynchronisé sur la prod** : il inclut les correctifs prod antérieurs
   non commités (FIX 2026-07-16 latent économique EUR + snapshot totals + décomposition P&L V4). Le diff
   est donc large (~+249) et normal.

### B. AG1 V4 — règles de trading (déployé dans n8n `AG1V4CONSENSUS`, `active=1`, publié)
4. **`nodes/post_agent/07_validate_enforce_safety_v5.code.js`** — garde-fous durs, config-driven :
   `min_order_value_eur`=1000 (rejet `MIN_ORDER_VALUE_EUR` des ordres d'achat < 1000 €),
   `max_open_positions`=10 (rejet `MAX_OPEN_POSITIONS` d'une nouvelle ouverture au-delà de 10 lignes).
5. **`nodes/pre_agent/4C_enrich_portfolio_with_market_prices.code.py`** — expose au LLM `perfLocalPct`
   (perf PRIX en devise locale, captée **avant** la conversion FX), `devise=` et `pnlEUR=`.
   ⚠️ Fichier **resynchronisé sur la prod** : inclut le correctif prod **FIX 2026-07-13** (`load_fx_ref_map`,
   conversion FX→EUR des positions non-EUR) qui n'était pas dans le repo. Le patch a été appliqué SUR le
   live pour ne pas l'écraser.
6. **`nodes/agent_input/agent_1_portfolio_manager.node.json`** + **les 3 nœuds agents dans le JSON canonique** —
   règles prompt ajoutées (systemMessage) : concentration 6-10 lignes / `targetWeightPct` ≥ 10 % ;
   rotation (pas de DECREASE/CLOSE < 10 j sauf cassure du stop d'ouverture ou -10 % par défaut) ;
   change (juger sur `perfLocalPct`, ne pas couper pour un motif FX géré manuellement).

### C. Swaps de modèles (changement manuel utilisateur, dans le JSON canonique)
7. **`AG1_workflow_v4_consensus.json`** (export live complet, source de vérité du workflow) :
   - GPT : `gpt-5.5-2026-04-23` → **`gpt-5.6-sol`** (nœud `OpenAI Chat Model - GPT5.2` + info-extractor
     tag `chatgpt52`→`chatgpt56`).
   - Grok remplacé par **DeepSeek** : nœud `xAI Grok Chat Model` (`lmChatXAiGrok`, `grok-4.3`) →
     `DeepSeek Chat Model` (`lmChatDeepSeek`, **`deepseek-v4-pro`**) ; info-extractor tag
     `grok41_reasoning` → `DeepSeekV4 Pro`.
   - 3e modèle inchangé : `claude-opus-4-8` (Anthropic).

### D. Coûts broker réels — collecteur `broker_costs` (déployé VPS, hors git)
8. **`outils/scripts/broker_costs_collector.py` (nouveau)** — collecteur host qui poll le service
   `ibkr-broker` (`GET /fills` = trades 7 j avec commission RÉELLE ; `GET /account/ledger` = cash par
   devise) et persiste de façon idempotente dans une base **dédiée** `broker_costs.duckdb`
   (single-writer, séparée d'AG1 pour éviter la contention DuckDB). Tables `broker_trades`
   (execution_id PK) et `cash_snapshots` (ts_day, currency).
   - **Déployé sur le VPS** (hors dépôt git, comme le collecteur Finnhub) :
     `/opt/trader-ia/fx-costs/broker_costs_collector.py` + `run_collector.sh`, venv réutilisé
     `/opt/trader-ia/finnhub/venv` (duckdb **1.4.3**, lu par le dashboard 1.4.4).
     **Cron** `0 20 * * *` (quotidien 20:00 UTC). Base écrite `/local-files/duckdb/broker_costs.duckdb`.
   - **Dashboard `waterfall.py`** lit cette base (read-only, `read_broker_costs()`) : **cash USD réel**
     (877,71 $ → barre « Impact change sur liquidité » sur cash réel) et **frais de change réels**
     s'il y a des conversions FX captées (sinon estimation barème, étiquetée, se remplit au fil de l'eau).
   - ⚠️ **Limite** : `/iserver/account/trades` ne sert que **7 jours** → les conversions FX antérieures
     ne sont pas récupérables via CP API (backfill possible via **Flex Query** IBKR, chantier séparé).
     Les commissions actions restent lues depuis `fill_costs` (tout l'historique) ; `broker_trades`
     accumulera les commissions réelles par trade au fil du temps.

## Vérifications effectuées
- Dashboard : `py_compile` waterfall/visualizations OK, parse app.py dans le conteneur 3.12, health
  `/_stcore/health` = 200 après restart. Tests unitaires du calcul cascade (réconciliation, signes,
  barème) et de la détection USD.
- AG1 : `node --check` (07), AST Python wrappé n8n (4C), JSON re-parsé (40 nœuds), `active=1` re-vérifié.
  Marqueurs confirmés sur la version publiée : garde-fous, `perfLocalPct` (×3 agents), fix FX préservé.
- Patch AG1 **chirurgical sur l'export live** (pas le repo) pour préserver les dérives prod ; backup
  live avant modif : `/root/.codex-tmp/AG1V4CONSENSUS_backup_20260721_100415.json`.

## Rollback
- Dashboard : `.bak_<ts>` de `app.py` / `visualizations.py` / `waterfall.py` sur `/opt/trading-dashboard/app`.
- AG1 : ré-importer le backup `.codex-tmp/AG1V4CONSENSUS_backup_20260721_100415.json` →
  `n8n publish:workflow --id=AG1V4CONSENSUS` → restart `root-n8n-1` + `root-task-runners-3/4/5`.

## PR proposée
- **Titre** : `feat(ag1-v4+dashboard): cascade P&L détaillée, sparkline flux EUR/USD, garde-fous sizing & vue devise locale, swaps modèles (gpt-5.6, deepseek-v4-pro)`
- **Base** : dernière branche mergée (`main`/`master` selon le repo GitHub) ; **head** = branche courante
  ou une branche dédiée `codex/dashboard-ag1-20260721`.
- **Corps** : reprendre les sections A/B/C ci-dessus + « déployé live et vérifié le 2026-07-21 ».

## Points de vigilance / à trancher (NE PAS bloquer la PR, mais à vérifier)
1. **model_key DeepSeek** = `"DeepSeekV4 Pro"` (avec espaces/majuscules), au lieu d'un slug type
   `deepseek_v4_pro`. AGENTS.md §25 liste les model_keys persistés (`chatgpt52`, `grok41_reasoning`,
   `claude_sonnet46`). Vérifier que `duckdb_writer.py` / `06_build_consensus_v4` / le dashboard V4
   gèrent les **nouveaux** keys (`chatgpt56`, `DeepSeekV4 Pro`) sans casser la persistance
   `model_proposals` ni le consensus 2/3 (sinon les propositions du modèle DeepSeek peuvent être
   ignorées ou mal agrégées).
2. **Rotation** = consigne prompt uniquement (le nœud 07 n'a pas `heldDays`/stop). Backstop dur = plomberie
   à ajouter si souhaité.
3. **Waterfall poste « Impact FX liquidité USD »** non chiffré (cash USD non persisté en base) : à
   instrumenter (journaliser le cash par devise) pour l'activer.
4. Cette PR **ne couvre pas** tout le backlog « déployé mais à committer » listé dans AGENTS.md
   (sessions 2026-06-18 → 2026-07-05). À traiter séparément.

## Après merge
- Mettre à jour `AGENTS.md` (bloc « déployé live, à committer ») : ajouter l'entrée 2026-07-21.
- Le dépôt `/opt/trading-dashboard/app` n'est pas un clone git → garder le repo comme source.
