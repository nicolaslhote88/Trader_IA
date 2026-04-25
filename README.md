# Trader_IA

Plateforme de trading assistÃ© par IA, orchestrÃ©e par n8n sur un VPS Hostinger.
Le systÃ¨me combine un ensemble de **Portfolio Managers LLM** (GPT-5.2 / Grok-4.1 / Gemini-3), trois **analystes spÃ©cialisÃ©s** (technique, fondamental, sentiment/news), un **Risk Manager** dÃ©terministe et un **Execution Trader** (sandbox aujourd'hui, broker live en prÃ©paration).

---

## 1. Architecture fonctionnelle (6 agents)

| # | Agent | RÃ´le | ImplÃ©mentÃ© dans |
|---|---|---|---|
| 1 | **Univers** | Extraction et maintenance de l'univers d'investissement (tickers, mÃ©tadonnÃ©es, secteurs) | `outils/AG0-V1 - extraction universe/` (workflow n8n inactif, utilitaire ponctuel) |
| 2 | **Portfolio Manager** | Allocation, cibles de position, ordres thÃ©oriques â€” **ensemble de 3 LLM en parallÃ¨le** | `AG1-V3-Portfolio manager/` (+ variantes GPT-5.2 / Grok-4.1 / Gemini-3) |
| 3 | **Analyste Technique** | Indicateurs, patterns, signaux de prix | `AG2-V3/` |
| 4 | **Analyste Fondamental** | Financials, valorisation, earnings | `AG3-V2/` |
| 5 | **Analyste Sentiment / News** | Sentiment de marchÃ©, news, transcripts | `AG4-V3/` (macro + geo-tagging), `AG4-SPE-V2/` (par valeur), `AG4-Forex/` (canaux FX dÃ©diÃ©s) |
| 6 | **Risk Manager + Execution Trader** | Validation des ordres, garde-fous, exÃ©cution | `AG1-V3-Portfolio manager/workflow/nodes/post_agent/` (nodes 7â†’10) |

> Ã‰tat actuel : l'Execution Trader est en **sandbox interne** (fills fabriquÃ©s au prix thÃ©orique). Le branchement broker rÃ©el est la prochaine Ã©tape â€” voir `ANALYSE_SYSTEME_AVANT_AGENT6.md`.

## 2. Stack technique

- **n8n** : orchestration des workflows (13 workflows â€” 10 actifs, 3 inactifs)
- **DuckDB** : source of truth analytique + ledger d'exÃ©cution (`cfg.portfolio_config`, `core.orders`, `core.fills`, `core.lots`, snapshots, cash ledger)
- **Qdrant** : RAG pour recherche sÃ©mantique (technique / fundamental / news)
- **yfinance-api** : service maison autour de `yfinance` (cache, cooldown par symbole, endpoints `/history`, `/quote`, `/options`, `/calendar`, `/fundamentals`)
- **yf-enrichment** : enrichissement quotidien (volatilitÃ©, earnings, calendar)
- **Streamlit** : dashboard opÃ©rationnel (`dashboard/`, `trading-dashboard` service)
- **Traefik** : reverse proxy TLS (Let's Encrypt)

Tout tourne dans Docker Compose â€” voir `vps_hostinger_config/`.

## 3. DÃ©marrage rapide

```bash
# Cloner
git clone https://github.com/nicolaslhote88/Trader_IA.git
cd Trader_IA

# Configurer l'environnement VPS
cd vps_hostinger_config
cp .env.example .env
# â†’ Ã©diter .env (voir docs/operations/env_vars.md)

# Lancer la stack principale
docker compose up -d

# Lancer Qdrant (stack sÃ©parÃ©e)
docker compose -f docker-compose.qdrant.yml up -d
```

## 4. Structure du repo

```
Trader_IA/
├── agents/                          # Workflows n8n des agents
│   ├── common/                      # Agents transverses
│   │   ├── AG4-V3/                  # News macro globales + geo-tagging + dual-write FX
│   │   └── yf-enrichment-v1/        # Enrichissement Yahoo Finance quotidien
│   ├── trading-actions/             # Agents du système actions/ETF/crypto
│   │   ├── AG1-PF-V1/
│   │   ├── AG1-V3-Portfolio manager/
│   │   ├── AG2-V3/
│   │   ├── AG3-V2/
│   │   └── AG4-SPE-V2/
│   └── trading-forex/               # Agents du système Forex isolé
│       ├── AG1-FX-V1-Portfolio manager/
│       ├── AG2-FX-V1/
│       ├── AG4-FX-V1/
│       └── AG4-Forex/
├── services/                        # Services Docker transverses
│   ├── dashboard/                   # Streamlit (trading-dashboard)
│   ├── yf-enrichment-service/       # Scheduler Dockerfile pour yf-enrichment-v1
│   └── yfinance-api/                # Service Yahoo Finance
├── infra/                           # Infra-as-code
│   └── vps_hostinger_config/        # Docker Compose + .env.example
├── docs/                            # Documentation consolidée (voir §6)
└── outils/                          # Workflows n8n inactifs / utilitaires ponctuels (AG0 univers)
```
> **Rappel** : cette arborescence GitHub peut diffÃ©rer de celle dÃ©ployÃ©e sur le VPS. Sur le VPS, `/opt/trader-ia/` a sa propre layout â€” les chemins de volumes et les `STATIC_WRITER_PATHS` dans le nÅ“ud 9 d'AG1-V3 sont pensÃ©s pour cette rÃ©alitÃ©.

## 5. Flux de donnÃ©es (vue haute)

```
AG0 (univers) â”€â”€â–º AG2/AG3/AG4/AG4-SPE (analystes parallÃ¨les) â”€â”€â–º AG1 (Portfolio Manager)
                                                                    â”‚
                                                                    â–¼
                                    Validate & Enforce Safety â”€â”€â–º Build DuckDB Bundle â”€â”€â–º Upsert Run Bundle â”€â”€â–º Post-Run Health
                                    (Risk Manager)                 (Execution = SIM)    (ledger atomique)    (health check)
                                                                    â”‚
                                                                    â–¼
                                                          Streamlit Dashboard (lecture seule)
```

## 6. Documentation

| ThÃ¨me | Emplacement |
|---|---|
| Ã‰tat des lieux fonctionnel complet | `docs/architecture/etat_des_lieux.md` |
| Historique des issues et dÃ©cisions | `docs/architecture/historique_issues.md` |
| Analyse systÃ¨me avant branchement broker | `ANALYSE_SYSTEME_AVANT_AGENT6.md` (racine) |
| Comparatif brokers 2026 | `Etude_Comparative_Brokers_Trader_IA.docx` (racine) |
| Audits (valorisation, segments marchÃ©) | `docs/audits/20260423_audit_valorisation/` |
| Spec AG4 geo-tagging + AG4_Forex | `docs/specs/ag4_geo_tagging_and_forex_base_v1.md` |
| Variables d'environnement | `docs/operations/env_vars.md` |
| DÃ©ploiement VPS | `docs/operations/deploy.md` |
| Reconstruction du pack AG1 | `docs/dev/rebuild_pack.md` |
| Historique des migrations | `docs/history/` |
| README par agent | `AG*/README.md` ou `AG*/docs/GUIDE.md` |

## 7. Conventions

- Ne jamais renommer les dossiers `AG*` ou `yfinance-api`, `yf-enrichment*` : les chemins sont en dur dans le docker-compose, dans `09_upsert_run_bundle_duckdb.code.py` (`STATIC_WRITER_PATHS`) et dans l'environnement VPS (`/opt/trader-ia/...`, `/local-files/...`).
- Les Ã©critures DuckDB passent par `duckdb_writer.py::upsert_run_bundle()` (transaction atomique, idempotent via `ON CONFLICT DO UPDATE`).
- Tout nouvel ordre broker live doit passer par le chemin existant `core.orders` + `core.fills` (colonnes `broker`, `broker_order_id`, `client_order_id`).

## 8. Licence

MIT â€” voir `LICENSE`.
