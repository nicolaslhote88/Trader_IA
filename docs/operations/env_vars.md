# Variables d'environnement

Cette page décrit les variables attendues côté VPS. Le fichier template est `infra/vps_hostinger_config/.env.example`.

## Traefik / TLS

| Variable | Rôle |
|---|---|
| `SSL_EMAIL` | Email utilisé par Let's Encrypt pour le challenge ACME. |
| `GENERIC_TIMEZONE` | Timezone appliquée aux conteneurs (défaut `Europe/Paris`). |

## n8n

| Variable | Rôle |
|---|---|
| `SUBDOMAIN` | Sous-domaine n8n (ex. `n8n`). |
| `DOMAIN_NAME` | Domaine racine (ex. `trader-ia.com`). |
| `N8N_RUNNERS_AUTH_TOKEN` | Token partagé entre `n8n` et `task-runners`. Doit être identique. Valeur forte obligatoire. |

## Services externes

| Variable | Rôle |
|---|---|
| `TRANSCRIPT_API_BASE` | URL de base du service de transcripts consommé par les analystes AG4. |

## Yahoo Finance / YF enrichment

| Variable | Rôle |
|---|---|
| `YF_MIN_SECONDS_BETWEEN_CALLS` | Delai global entre appels Yahoo dans `yfinance-api`. Defaut VPS recommande: `1`. Augmenter si Yahoo renvoie des 429 repetes. |
| `YF_ENRICH_METADATA_MAX_SYMBOLS` | Nombre maximal de symboles enrichis en metadata/options/calendar par run. Les quotes restent rafraichies pour tous les symboles charges. Defaut: `40`. |
| `YF_ENRICH_QUOTE_CHUNK` | Taille des chunks `/quote` pour le job YF. Defaut service: `40`. |
| `YF_ENRICH_TIMEOUT_SEC` | Timeout par appel au service `yfinance-api`. Defaut service: `90`. |

## IBKR / Execution live

| Variable | Rôle |
|---|---|
| `IBKR_DRY_RUN` | `true` : aucun ordre broker reel n'est envoye. `false` active l'envoi via IBKR. En production Forex actuelle, cette valeur est `false` uniquement sur le compte paper. |
| `IBKR_SEND_DRY_RUN_TO_BROKER` | `false` par defaut : les nodes n8n restent sandbox-only en dry-run. `true` appelle `ibkr-broker` en dry-run pour valider le chemin HTTP sans ordre live. |
| `AG1_ACTIONS_LIVE_ORDERS_ENABLED` | `false` par defaut : bloque les ordres actions AG1 meme si `IBKR_DRY_RUN=false` pour le Forex paper. Passer a `true` uniquement apres validation des runs actions, reconciliation broker et controle du ledger. |
| `AG1_ACTIONS_IBKR_ENABLED_MODELS` | Liste de tokens de modeles autorises a envoyer des ordres actions via IBKR quand le gate actions est ouvert. Exemple pre-live : `gemini`. |
| `AG1_V4_ACTIONS_IBKR_ENABLED_MODELS` | Token logique autorise pour AG1 V4 consensus quand le gate actions est ouvert. Defaut : `ag1_v4_consensus`. |
| `IBKR_ACCOUNT_ID` | Compte IBKR cible. Laisser vide pour auto-detection, mais le fixer est recommande avant le live. |
| `IBKR_BROKER_URL` | URL interne n8n/runners vers le broker. Definie dans compose : `http://ibkr-broker:8080`. |
| `IBKR_GATEWAY_URL` | URL interne du broker vers Client Portal Gateway. Definie dans compose : `https://ibkr-gateway:5000`. |
| `IBKR_REQUIRE_PAPER_ACCOUNT` | `true` en production Forex paper : bloque l'envoi si le compte detecte ne correspond pas a un compte paper attendu. |
| `IBKR_PAPER_ACCOUNT_PREFIXES` | Prefixes autorises pour le garde-fou paper, par defaut `DU`. |
| `IBKR_RECONCILE_CASH_BALANCES` | `true` par defaut. Compare les cash balances IBKR `/account/ledger` aux effets cash attendus des lots FX DuckDB. |
| `IBKR_BLOCK_ON_CASH_DIVERGENCE` | `false` par defaut. Si `true`, une divergence cash non-base au-dessus du seuil bloque les nouveaux ordres comme une divergence de position. |
| `IBKR_CASH_RECON_THRESHOLD_UNITS` | Seuil absolu par devise pour signaler une divergence de cash balance, par defaut `5` unites de devise. |
| `AG1_FX_PORTFOLIO_BASE_CCY` | Devise de base du portefeuille FX pour la reconciliation cash, par defaut `EUR`. |
| `AG1_FX_CASH_ONLY_BASE_CCY_MODE` | `true` par defaut en paper live CPAPI. Bloque les nouvelles ouvertures qui emprunteraient une devise non-base; avec `EUR`, seules les ouvertures `SELL_BASE` sur paires `EURxxx` et `BUY_BASE` sur paires `xxxEUR` passent. Les clotures restent autorisees. |
| `IBKR_FILL_CONFIRM_SECONDS` | Temps maximal de polling des fills apres soumission d'ordres FX, par defaut `6` secondes pour rester sous le timeout n8n de 60s. |
| `IBKR_FILL_POLL_INTERVAL_SECONDS` | Intervalle de polling `/fills` pendant la fenetre de confirmation, par defaut `2` secondes. |
| `IBKR_SEND_NODE_TIME_BUDGET_SECONDS` | Budget global du noeud AG1-FX `IBKR Send Orders FX`, par defaut `50` secondes. Le polling des fills est raccourci automatiquement si ce budget est presque consomme. |
| `IBKR_KEEPALIVE_INTERVAL_SECONDS` | Frequence du superviseur de session `ibkr-broker`, par defaut 55 secondes. |
| `IBKR_AUTO_REAUTH_ENABLED` | `true` par defaut : tente `/iserver/auth/ssodh/init` quand la session brokerage tombe mais que Gateway/SSO reste valide. |
| `IBKR_AUTO_REAUTH_COMPETE` | `false` par defaut. Si `true`, peut deconnecter une session concurrente du meme username IBKR; a reserver a un username dedie au robot. |
| `IBKR_ALERT_WEBHOOK_URL` | Webhook optionnel appele quand IBKR impose un relogin navigateur/2FA. Laisser vide si non branche. |
| `IBKR_ALERT_COOLDOWN_SECONDS` | Cooldown minimal entre deux alertes relogin, par defaut `900`. |
| `IBKR_LOGIN_URL` | URL affichee par `/auth/operator-action`, par defaut `https://localhost:5000`. |
| `IBKR_LOGIN_TUNNEL_COMMAND` | Commande de tunnel affichee par `/auth/operator-action`. |
| `IBKR_ASSISTED_LOGIN_ENABLED` | `false` par defaut. Indique qu'un flux credentials assistes est branche; ne contourne pas le 2FA IBKR. |
| `IBKR_USERNAME` / `IBKR_PASSWORD` | Credentials optionnels pour un flux assiste externe. Ne jamais versionner de vraies valeurs. |
| `IBEAM_ACCOUNT` / `IBEAM_PASSWORD` | Variante de credentials pour un wrapper type IBeam si active ulterieurement. |
| `IBEAM_LOG_LEVEL` | Niveau de logs IBeam, par defaut `INFO`. Passer a `DEBUG` uniquement pour diagnostic court. |
| `IBEAM_ERROR_SCREENSHOTS` | `True` par defaut. Capture les erreurs d'auth IBeam dans le volume de sorties. |
| `IBEAM_GATEWAY_BASE_URL` | Base URL interne IBeam vers son gateway, par defaut `https://localhost:5000`. |
| `IBEAM_MAINTENANCE_INTERVAL` | Frequence de maintenance IBeam, par defaut `60` secondes. |
| `IBEAM_MAX_FAILED_AUTH` | Garde-fou anti-lockout IBKR, par defaut `3` echecs consecutifs. |
| `IBEAM_RESTART_FAILED_SESSIONS` | `True` par defaut : IBeam redemarre le gateway si la session devient invalide. |
| `AG1_FX_REDUCED_SIZE_MAX_PAIR_PCT` | Cap d'exposition par ordre pour les ouvertures AG1-FX marquees `REDUCED_SIZE_ONLY`. Defaut `0.15`, toujours borne par `max_pair_pct`. |
| `AG1_FX_ESTIMATED_FEE_PER_FILL_EUR` | Estimation prudente du cout minimum par fill IBKR utilisee par le brief et le Risk Manager. Defaut `1.75`. |
| `AG1_FX_MIN_NEW_TRADE_NOTIONAL_EUR` | Notional minimum d'une nouvelle ouverture AG1-FX. Defaut `1200`, pour eviter les micro-trades detruits par les frais fixes. |
| `AG1_FX_TARGET_NEW_TRADE_NOTIONAL_EUR` | Taille cible indicative envoyee au LLM pour les setups valides. Defaut `1700`. |
| `AG1_FX_MIN_EXPECTED_GROSS_PROFIT_EUR` | Profit brut minimum attendu entre prix d'entree et take-profit avant frais. Defaut `8`. |
| `AG1_FX_MIN_REWARD_TO_FEE` | Ratio minimum profit brut attendu / frais estimes. Defaut `2.0`. |
| `AG1_FX_MIN_CLOSE_NET_PROFIT_EUR` | Profit net minimum estime apres frais de sortie pour autoriser une cloture discretionnaire. Defaut `2.0`. |
| `AG1_FX_MIN_DISCRETIONARY_HOLD_HOURS` | Duree minimale avant une cloture discretionnaire perdante nette, sauf stop/TP ou invalidation forte du cube. Defaut `24`. |
| `AG1_FX_LLM_TOP_NEWS_MAX` | Nombre maximum de news dans le brief LLM Forex. Defaut prod `4` pour limiter la taille du prompt et le risque de crash n8n. |
| `AG1_FX_LLM_MARKET_WATCH_MAX` | Nombre maximum de paires detaillees dans `market_watch`. Defaut prod `10`. |
| `AG1_FX_LLM_PAIR_DRIVERS_MAX` | Nombre maximum de drivers news par paire dans le brief compact. Defaut prod `1`. |
| `AG2_FX_IBKR_MARKETDATA_ENABLED` | `true` par defaut. Active l'enrichissement AG2-FX par snapshots FX IBKR bid/ask/mid/spread. |
| `AG4_FX_OFFICIAL_SOURCES_ENABLED` | `true` par defaut. Active les flux officiels banques centrales/BIS dans AG4-FX. |

Voir aussi `docs/operations/ibkr_execution.md`.

## AG1 V4 Consensus Actions

AG1 V4 actions utilise une base separee et ne reprend pas les historiques V3.
Le writer initialise `cfg.portfolio_config` et `core.cash_ledger` a 10 000 EUR
lors de la creation de la base.

| Variable | Rôle |
|---|---|
| `AG1_V4_DUCKDB_PATH` | Base ledger consensus actions. Defaut compose : `/files/duckdb/ag1_v4_consensus.duckdb`. |
| `AG1_V4_DUCKDB_WRITER_PATH` | Writer externe V4 monte dans n8n/runners. |
| `AG1_V4_LEDGER_SCHEMA_PATH` | Schema SQL V4 avec tables de propositions, votes et decisions consensus. |
| `AG1_V4_ACTIONS_IBKR_ENABLED_MODELS` | Token logique autorise par le node IBKR V4 quand les ordres actions sont ouverts. |

## Dashboard Streamlit

| Variable | Rôle |
|---|---|
| `DASHBOARD_DOMAIN` | Nom d'hôte public du dashboard, sans protocole `https://` (ex. `dashboard.trader-ia.com`). |
| `DASHBOARD_BASIC_AUTH` | Entrée Basic Auth au format `user:hash_apache`. Les `$` doivent être échappés en `$$` pour Docker Compose. |
| `AG2_DUCKDB_PATH` | Source de l'univers actions du dashboard. Par défaut `/files/duckdb/ag2_v3.duckdb`, table `universe`. |

Génération du hash :

```bash
htpasswd -nb admin 'motdepasse' | sed -e s/\\$/\\$\\$/g
```

## Variables internes au service `trading-dashboard`

Définies dans le docker-compose (pas dans le `.env`). Le volume `/local-files/duckdb:/files/duckdb:ro` les rend toutes lisibles :

- `AG1_CHATGPT52_DUCKDB_PATH=/files/duckdb/ag1_v3_chatgpt52.duckdb`
- `AG1_GROK41_REASONING_DUCKDB_PATH=/files/duckdb/ag1_v3_grok41_reasoning.duckdb`
- `AG1_GEMINI30_PRO_DUCKDB_PATH=/files/duckdb/ag1_v3_gemini30_pro.duckdb`
- `AG1_V4_DUCKDB_PATH=/files/duckdb/ag1_v4_consensus.duckdb`
- `AG2_DUCKDB_PATH=/files/duckdb/ag2_v3.duckdb`
- `AG3_DUCKDB_PATH=/files/duckdb/ag3_v2.duckdb`
- `AG4_DUCKDB_PATH=/files/duckdb/ag4_v3.duckdb`
- `AG4_SPE_DUCKDB_PATH=/files/duckdb/ag4_spe_v2.duckdb`
- `AG4_FOREX_DUCKDB_PATH=/files/duckdb/ag4_forex_v1.duckdb` *(alimente la page « Forex P&L (LLM x Paire) » — couverture news taguées FX)*
- `YF_ENRICH_DUCKDB_PATH=/files/duckdb/yf_enrichment_v1.duckdb`

## Variables internes au système Forex AG1-FX-V1

Définies dans `infra/vps_hostinger_config/docker-compose.yml` pour `n8n`, `task-runners` et `trading-dashboard` :

- `AG1_FX_V1_CHATGPT52_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_chatgpt52.duckdb`
- `AG1_FX_V1_GROK41_REASONING_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb`
- `AG1_FX_V1_GEMINI30_PRO_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb`
- `AG2_FX_V1_DUCKDB_PATH=/files/duckdb/ag2_fx_v1.duckdb`
- `AG3_FX_V1_DUCKDB_PATH=/files/duckdb/ag3_fx_v1.duckdb`
- `AG4_FX_V1_DUCKDB_PATH=/files/duckdb/ag4_fx_v1.duckdb`
- `AG1_FX_V1_WRITER_PATH=/files/AG1-FX-V1-EXPORT/nodes/post_agent/duckdb_writer.py`
- `AG1_FX_V1_LEDGER_SCHEMA_PATH=/files/AG1-FX-V1-EXPORT/sql/ag1_fx_v1_schema.sql`
- `AG1_FX_LOCK_PATH=/files/locks/ag1_fx_active.lock`
- `AG1_FX_LOCK_TTL_SECONDS=2700`

Production paper actuelle :

- un seul workflow PM AG1-FX actif par compte IBKR ;
- `chatgpt52` actif ;
- `grok41_reasoning` et `gemini30_pro` desactives ;
- `AG1-FX-PF-V1` actif pour la reconciliation horaire.

## Variables internes au service n8n

Ces variables sont déjà définies dans le docker-compose — elles ne sont **pas** dans le .env :

- `AG1_DUCKDB_PATH=/files/duckdb/ag1_v3.duckdb`
- `AG1_DUCKDB_WRITER_PATH=/files/AG1-V3-EXPORT/nodes/post_agent/duckdb_writer.py`
- `AG1_LEDGER_SCHEMA_PATH=/files/AG1-V3-EXPORT/sql/portfolio_ledger_schema_v2.sql`
- `AG1_V4_DUCKDB_PATH=/files/duckdb/ag1_v4_consensus.duckdb`
- `AG1_V4_DUCKDB_WRITER_PATH=/files/AG1-V4-EXPORT/nodes/post_agent/duckdb_writer.py`
- `AG1_V4_LEDGER_SCHEMA_PATH=/files/AG1-V4-EXPORT/sql/portfolio_ledger_schema_v4.sql`
- `EXECUTIONS_DATA_MAX_AGE=72`
- `EXECUTIONS_DATA_SAVE_ON_SUCCESS=all`
- `EXECUTIONS_DATA_SAVE_MANUAL_EXECUTIONS=true`
- `EXECUTIONS_DATA_PRUNE_MAX_COUNT=5000`
- `N8N_PROXY_HOPS=1`
- `DB_SQLITE_VACUUM_ON_STARTUP=true`

## Variables internes aux task-runners

- `N8N_RUNNERS_TASK_BROKER_URI=http://n8n:5679`
- `N8N_RUNNERS_MAX_CONCURRENCY=4`
- `N8N_RUNNERS_LAUNCHER_LOG_LEVEL=debug`
- `N8N_BLOCK_RUNNER_ENV_ACCESS=false` lorsque les Code nodes Python doivent lire
  les variables d'environnement IBKR/AG1-FX.

Les mêmes `AG1_DUCKDB_*` / `AG1_V4_*` sont répliquées côté runners pour accès
aux `.duckdb`.
Sur le VPS actuel, les runners externes utilisent aussi
`/opt/trader-ia/n8n-task-runners.clean.json`; les variables IBKR et AG1-FX
doivent etre presentes dans `allowed-env` pour etre visibles dans les workflows.
Pour AG1 V4, ajouter au minimum `AG1_V4_DUCKDB_PATH`,
`AG1_V4_DUCKDB_WRITER_PATH`, `AG1_V4_LEDGER_SCHEMA_PATH`,
`AG1_V4_ACTIONS_IBKR_ENABLED_MODELS` et `AG1_V4_INITIAL_CAPITAL_EUR`.
