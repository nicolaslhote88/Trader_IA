# Trader_IA

Plateforme de trading assisté par IA, orchestrée par n8n sur un VPS Hostinger.
Le système combine un ensemble de **Portfolio Managers LLM** (GPT-5.2 / Grok-4.1 / Gemini-3), trois **analystes spécialisés** (technique, fondamental, sentiment/news), un **Risk Manager** déterministe et un **Execution Trader** (sandbox aujourd'hui, broker live en préparation).

---

## 1. Architecture fonctionnelle (6 agents)

| # | Agent | Rôle | Implémenté dans |
|---|---|---|---|
| 1 | **Univers** | Extraction et maintenance de l'univers d'investissement (tickers, métadonnées, secteurs) | `AG0-V1 - extraction universe/` |
| 2 | **Portfolio Manager** | Allocation, cibles de position, ordres théoriques — **ensemble de 3 LLM en parallèle** | `AG1-V3-Portfolio manager/` (+ variantes GPT-5.2 / Grok-4.1 / Gemini-3) |
| 3 | **Analyste Technique** | Indicateurs, patterns, signaux de prix | `AG2-V3/` |
| 4 | **Analyste Fondamental** | Financials, valorisation, earnings | `AG3-V2/` |
| 5 | **Analyste Sentiment / News** | Sentiment de marché, news, transcripts | `AG4-V3/`, `AG4-SPE-V2/` |
| 6 | **Risk Manager + Execution Trader** | Validation des ordres, garde-fous, exécution | `AG1-V3-Portfolio manager/nodes/post_agent/` (nodes 7→10) |

> État actuel : l'Execution Trader est en **sandbox interne** (fills fabriqués au prix théorique). Le branchement broker réel est la prochaine étape — voir `ANALYSE_SYSTEME_AVANT_AGENT6.md`.

## 2. Stack technique

- **n8n** : orchestration des workflows (13 workflows — 10 actifs, 3 inactifs)
- **DuckDB** : source of truth analytique + ledger d'exécution (`cfg.portfolio_config`, `core.orders`, `core.fills`, `core.lots`, snapshots, cash ledger)
- **Qdrant** : RAG pour recherche sémantique (technique / fundamental / news)
- **yfinance-api** : service maison autour de `yfinance` (cache, cooldown par symbole, endpoints `/history`, `/quote`, `/options`, `/calendar`, `/fundamentals`)
- **yf-enrichment** : enrichissement quotidien (volatilité, earnings, calendar)
- **Streamlit** : dashboard opérationnel (`dashboard/`, `trading-dashboard` service)
- **Traefik** : reverse proxy TLS (Let's Encrypt)

Tout tourne dans Docker Compose — voir `vps_hostinger_config/`.

## 3. Démarrage rapide

```bash
# Cloner
git clone https://github.com/nicolaslhote88/Trader_IA.git
cd Trader_IA

# Configurer l'environnement VPS
cd vps_hostinger_config
cp .env.example .env
# → éditer .env (voir docs/operations/env_vars.md)

# Lancer la stack principale
docker compose up -d

# Lancer Qdrant (stack séparée)
docker compose -f docker-compose.qdrant.yml up -d
```

## 4. Structure du repo

```
Trader_IA/
├── AG0-V1 - extraction universe/   # Agent 1 (Univers)
├── AG1-V3-Portfolio manager/        # Agent 2 (Portfolio Manager) + Risk + Execution
├── AG1-PF-V1/                       # Pipeline paper fusion (archivé)
├── AG2-V3/                          # Agent 3 (Analyste Technique)
├── AG3-V2/                          # Agent 4 (Analyste Fondamental)
├── AG4-V3/                          # Agent 5 (News - news réguliers)
├── AG4-SPE-V2/                      # Agent 5 (News - transcripts/earnings)
├── dashboard/                       # Streamlit (trading-dashboard)
├── docs/                            # Documentation consolidée (voir §6)
├── vps_hostinger_config/            # Docker Compose + env
├── yfinance-api/                    # Service Yahoo Finance
├── yf-enrichment-v1/                # Enrichment quotidien (script déployé)
└── yf-enrichment-service/           # Dockerfile scheduler pour enrichment
```

## 5. Flux de données (vue haute)

```
AG0 (univers) ──► AG2/AG3/AG4/AG4-SPE (analystes parallèles) ──► AG1 (Portfolio Manager)
                                                                    │
                                                                    ▼
                                    Validate & Enforce Safety ──► Build DuckDB Bundle ──► Upsert Run Bundle ──► Post-Run Health
                                    (Risk Manager)                 (Execution = SIM)    (ledger atomique)    (health check)
                                                                    │
                                                                    ▼
                                                          Streamlit Dashboard (lecture seule)
```

## 6. Documentation

| Thème | Emplacement |
|---|---|
| État des lieux fonctionnel complet | `docs/architecture/etat_des_lieux.md` |
| Historique des issues et décisions | `docs/architecture/historique_issues.md` |
| Analyse système avant branchement broker | `ANALYSE_SYSTEME_AVANT_AGENT6.md` (racine) |
| Comparatif brokers 2026 | `Etude_Comparative_Brokers_Trader_IA.docx` (racine) |
| Variables d'environnement | `docs/operations/env_vars.md` |
| Déploiement VPS | `docs/operations/deploy.md` |
| Reconstruction du pack AG1 | `docs/dev/rebuild_pack.md` |
| Historique des migrations | `docs/history/` |
| README par agent | `AG*/README.md` ou `AG*/docs/GUIDE.md` |

## 7. Conventions

- Ne jamais renommer les dossiers `AG*` ou `yfinance-api`, `yf-enrichment*` : les chemins sont en dur dans le docker-compose, dans `09_upsert_run_bundle_duckdb.code.py` (`STATIC_WRITER_PATHS`) et dans l'environnement VPS (`/opt/trader-ia/...`, `/local-files/...`).
- Les écritures DuckDB passent par `duckdb_writer.py::upsert_run_bundle()` (transaction atomique, idempotent via `ON CONFLICT DO UPDATE`).
- Tout nouvel ordre broker live doit passer par le chemin existant `core.orders` + `core.fills` (colonnes `broker`, `broker_order_id`, `client_order_id`).

## 8. Licence

MIT — voir `LICENSE`.
