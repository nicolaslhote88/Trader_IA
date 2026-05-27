# Execution IBKR

Ce document decrit le branchement IBKR Client Portal API ajoute au projet.

## Architecture

```text
n8n Code node 07b / 11b
  -> http://ibkr-broker:8080
  -> https://ibkr-gateway:5000/v1/api
  -> IBKR
```

Le `ibkr-broker` encapsule l'API IBKR. Cote Forex, le mode production actuel est
un environnement **IBKR paper** avec `IBKR_DRY_RUN=false`; cote actions/ETF, le
chemin reste dry-run par defaut tant qu'une validation separee n'a pas ete faite.

References IBKR utilisees :

- Web API orders: https://www.interactivebrokers.com/campus/ibkr-api-page/webapi-doc/
- Gateway / staging notes: https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-staging/

## Services Docker

- `services/ibkr-gateway/` telecharge et lance `clientportal.gw`.
- `services/ibkr-broker/` expose `/health`, `/orders/fx`, `/orders/equity`,
  `/fills`, `/positions`, `/account/summary`, `/account/ledger` et
  `/auth/tickle`, `/auth/initialize`.
- Le broker supervise la session CPAPI : `/tickle` toutes les 55 secondes,
  puis tentative `/iserver/auth/ssodh/init` si IBKR indique
  `connected=true/authenticated=false`. Si la session Gateway/SSO a expire,
  `/health` expose `session_monitor.manual_login_required=true` et
  `operator_action` avec la commande de tunnel, l'URL de login et les etapes
  de validation 2FA.
- Sur le VPS actuel, les deux services sont integres directement dans le stack
  `/docker/yfinance`. Le fichier de reference a copier comme compose principal
  est `infra/vps_hostinger_config/docker-compose.yfinance.yml`.
- `infra/vps_hostinger_config/docker-compose.yml` garde aussi la definition pour
  un deploiement complet du repo, mais ce n'est pas le mode utilise sur le VPS.
- Les nodes n8n lisent `IBKR_BROKER_URL`, `IBKR_DRY_RUN`,
  `IBKR_SEND_DRY_RUN_TO_BROKER`, `IBKR_REQUIRE_PAPER_ACCOUNT`,
  `IBKR_PAPER_ACCOUNT_PREFIXES`, `IBKR_RECONCILE_CASH_BALANCES`,
  `IBKR_BLOCK_ON_CASH_DIVERGENCE`, `IBKR_CASH_RECON_THRESHOLD_UNITS` et
  `AG1_FX_PORTFOLIO_BASE_CCY`. Sans variable explicite, ils restent en dry-run,
  utilisent `http://ibkr-broker:8080`, auditent les balances cash et ne bloquent
  que sur divergence de positions.

## Realite authentification IBKR

Le Client Portal Gateway ne permet pas d'automatiser le login complet pour un
client individuel. Le login navigateur + 2FA reste obligatoire au moins une fois
par jour selon IBKR, et plus tot en cas de maintenance serveur ou d'expiration
SSO. La solution de production retenue est donc :

- maintenir la session active avec `/tickle`;
- relancer automatiquement la session brokerage via `/iserver/auth/ssodh/init`
  quand la session Gateway/SSO est encore valide;
- signaler explicitement `manual_login_required=true` quand IBKR impose un
  relogin navigateur;
- envoyer une alerte webhook optionnelle quand un relogin/2FA est requis;
- exposer `/auth/recover` et `/auth/operator-action` pour lancer une
  recuperation non destructive et guider l'operateur;
- utiliser un username IBKR dedie au robot pour eviter les sessions concurrentes
  Client Portal/TWS/mobile.

`IBKR_AUTO_REAUTH_COMPETE=false` par defaut evite de deconnecter une autre
session ouverte avec le meme username. Si le robot utilise un username dedie,
`IBKR_AUTO_REAUTH_COMPETE=true` peut etre active pour reprendre la priorite sur
une session concurrente accidentelle.

### Credentials en environnement

Le broker accepte les variables `IBKR_USERNAME` / `IBKR_PASSWORD`, ainsi que
`IBEAM_ACCOUNT` / `IBEAM_PASSWORD`, uniquement comme signal de configuration
pour un flux d'auto-login assiste. Il ne les affiche jamais dans `/health` et ne
les journalise pas. Sur Client Portal Gateway, ces credentials ne remplacent pas
la validation Secure Login System / 2FA imposee par IBKR; ils servent surtout a
preparer un wrapper headless type IBeam ou Playwright, qui saisit login/password
et laisse le 2FA etre approuve si IBKR le demande.

Variables utiles :

```bash
IBKR_ALERT_WEBHOOK_URL=
IBKR_ALERT_COOLDOWN_SECONDS=900
IBKR_LOGIN_URL=https://localhost:5000
IBKR_LOGIN_TUNNEL_COMMAND="ssh -L 5000:127.0.0.1:5000 root@100.104.236.78"
IBKR_ASSISTED_LOGIN_ENABLED=false
IBKR_USERNAME=
IBKR_PASSWORD=
IBEAM_ACCOUNT=
IBEAM_PASSWORD=
```

Endpoints de controle :

```bash
curl -sS -X POST http://127.0.0.1:18080/auth/recover
curl -sS http://127.0.0.1:18080/auth/operator-action
```

`/auth/recover` est non destructif : il tente `tickle`, lit `auth/status`, puis
`ssodh/init` seulement si la session Gateway reste connectee. Si IBKR exige un
relogin, l'endpoint renvoie `operator_action` au lieu de masquer le probleme par
une erreur broker generique.

## Intention FX IBKR

Les ordres AG1-FX sont des **trades Forex spot speculatifs**, pas de simples
conversions de devise. Le broker envoie les tickets FX sur les contrats CASH
IBKR avec une quantite exprimee en devise de base :

- `open_short` / `close_long` deviennent `SELL` sur la devise de base.
- `open_long` / `close_short` deviennent `BUY` sur la devise de base.
- Le payload CPAPI contient explicitement `isCcyConv=false`.

Ainsi un short `EURCHF` est traite comme une vente d'EUR contre CHF, avec une
exposition short EUR / long CHF dans les balances reelles IBKR. Les controles de
reconciliation ne doivent donc pas se limiter aux lignes de positions FX
virtuelles : ils consultent aussi `/account/ledger`, qui expose les cash balances
par devise.

## Production Paper Forex (2026-05-06)

Etat deploye :

- `IBKR_DRY_RUN=false` pour le Forex paper.
- `IBKR_REQUIRE_PAPER_ACCOUNT=true`.
- `IBKR_PAPER_ACCOUNT_PREFIXES=DU`.
- Compte paper attendu : un compte dont l'identifiant commence par `DU`.
- Seul `AG1-FX-V1 Portfolio Manager - chatgpt52` est actif.
- `grok41_reasoning` et `gemini30_pro` sont desactives pour eviter que trois PMs
  ecrivent des ordres sur le meme compte IBKR.
- `AG1-FX-PF-V1 - Hourly Portfolio Valuation` est actif et reconcilie le ledger
  GPT toutes les heures.

Garanties ajoutees :

- Pre-run reconciliation dans `03_load_portfolio_state_fx.py` : compare les lots
  DuckDB ouverts avec `/positions` IBKR. Une divergence active le kill switch et
  bloque les ouvertures.
- Reconciliation des balances cash IBKR via `/account/ledger` dans le pre-run
  AG1-FX et dans `AG1-FX-PF-V1`. Pour les contrats spot-FX `CASH`, le ledger
  cash IBKR est la source autoritaire car CPAPI ne remonte pas toujours de
  lignes de positions FX exploitables. Par defaut, les ecarts de devises non
  base sont audites dans `core.reconciliation_log`. Pour transformer un ecart
  cash en blocage dur, definir `IBKR_BLOCK_ON_CASH_DIVERGENCE=true`.
- Global lock AG1-FX : empeche deux runs PM simultanes contre le meme compte.
- Le validateur `11 Validate Enforce Safety FX` bloque deterministiquement tout
  nouvel ordre dont le compact pack indique `trade_permission=NO_NEW_POSITION`.
- Le meme validateur limite les ouvertures `REDUCED_SIZE_ONLY` a
  `AG1_FX_REDUCED_SIZE_MAX_PAIR_PCT` de l'equity, par defaut `0.10`, avant
  l'envoi broker.
- En `IBKR_DRY_RUN=false`, `12_simulate_fills_fx.py` ne cree plus de fill simule.
  Il ne persiste que les fills confirmes par IBKR.
- Les frais ne sont plus modelises en paper/live : `core.fills.fees_eur` est
  alimente depuis les champs de commission IBKR quand ils sont fournis par
  `/fills`. La table `core.fill_costs` conserve en plus le montant brut, la
  devise, l'identifiant d'execution broker, la source du champ et le JSON brut.
  Si IBKR fournit une commission FX sans devise explicite, la devise est inferee
  depuis la devise de cotation du contrat CASH (`EUR.JPY` -> JPY,
  `EUR.CHF` -> CHF), puis convertie en EUR. La source est tracee comme
  `ibkr_commission_inferred_<CCY>_quote_no_ccy`. Un fallback EUR n'est utilise
  que si la paire ne peut pas etre identifiee.
- Les ordres envoyes mais sans fill immediat restent `submitted`; le workflow PF
  importe ensuite les fills confirmes depuis `/fills`.
- Les rejets IBKR explicites sont classes en `broker_error` et ne generent ni
  fill simule ni retry automatique.
- `core.reconciliation_log` conserve les controles IBKR/DuckDB.
- Depuis le 2026-05-19, `AG1_FX_PREFUND_NON_EUR_FX=true` complete
  `AG1_FX_CASH_ONLY_BASE_CCY_MODE=true`. Pour une nouvelle ouverture hors
  patterns EUR directs, AG1-FX cree une jambe de conversion cash avant l'ordre
  cible: `SELL_BASE` doit d'abord acheter la devise base avec EUR; `BUY_BASE`
  doit d'abord acheter la devise quote avec EUR. Ces jambes sont envoyees a
  IBKR avec `isCcyConv=true`, ne creent pas de lot speculatif dans DuckDB, et
  l'ordre cible n'est envoye que si la conversion est confirmee. Si
  `AG1_FX_PREFUND_NON_EUR_FX=false`, le comportement historique reste un
  blocage pre-broker avec `IBKR_CASH_ONLY_EUR_LEG_REQUIRED`.
- Les echecs de session CPAPI sont journalises comme `IBKR_MANUAL_LOGIN_REQUIRED`
  quand le broker indique qu'un relogin navigateur/2FA est necessaire, au lieu
  d'un simple `HTTP Error 502`.

## Deploiement VPS

Le VPS ne clone pas tout le repo dans `/opt/trader-ia`. Pour deploier depuis la
machine locale, copier seulement les services utiles puis reconstruire le stack
`yfinance`. Ne pas mettre IBKR dans un `docker-compose.override.yml` separe : le
compose principal doit rester la source de verite, sinon un redeploiement du
stack peut oublier `ibkr-gateway` et `ibkr-broker`.

```bash
# Sur la machine locale, depuis la racine du repo
scp -r services/ibkr-gateway services/ibkr-broker root@100.104.236.78:/opt/trader-ia/services/
scp infra/vps_hostinger_config/docker-compose.yfinance.yml root@100.104.236.78:/docker/yfinance/docker-compose.yml

# Sur le VPS
cd /docker/yfinance
grep -q '^IBKR_DRY_RUN=' .env || echo 'IBKR_DRY_RUN=true' >> .env
grep -q '^IBKR_SEND_DRY_RUN_TO_BROKER=' .env || echo 'IBKR_SEND_DRY_RUN_TO_BROKER=false' >> .env
grep -q '^IBKR_ACCOUNT_ID=' .env || echo 'IBKR_ACCOUNT_ID=' >> .env
grep -q '^IBKR_REQUIRE_PAPER_ACCOUNT=' .env || echo 'IBKR_REQUIRE_PAPER_ACCOUNT=true' >> .env
grep -q '^IBKR_PAPER_ACCOUNT_PREFIXES=' .env || echo 'IBKR_PAPER_ACCOUNT_PREFIXES=DU' >> .env
grep -q '^IBKR_RECONCILE_CASH_BALANCES=' .env || echo 'IBKR_RECONCILE_CASH_BALANCES=true' >> .env
grep -q '^IBKR_BLOCK_ON_CASH_DIVERGENCE=' .env || echo 'IBKR_BLOCK_ON_CASH_DIVERGENCE=false' >> .env
grep -q '^IBKR_CASH_RECON_THRESHOLD_UNITS=' .env || echo 'IBKR_CASH_RECON_THRESHOLD_UNITS=5' >> .env
grep -q '^AG1_FX_PORTFOLIO_BASE_CCY=' .env || echo 'AG1_FX_PORTFOLIO_BASE_CCY=EUR' >> .env
grep -q '^AG1_FX_PREFUND_NON_EUR_FX=' .env || echo 'AG1_FX_PREFUND_NON_EUR_FX=true' >> .env
grep -q '^AG1_FX_PREFUND_BUFFER_PCT=' .env || echo 'AG1_FX_PREFUND_BUFFER_PCT=0.005' >> .env
grep -q '^IBKR_KEEPALIVE_INTERVAL_SECONDS=' .env || echo 'IBKR_KEEPALIVE_INTERVAL_SECONDS=55' >> .env
grep -q '^IBKR_AUTO_REAUTH_ENABLED=' .env || echo 'IBKR_AUTO_REAUTH_ENABLED=true' >> .env
grep -q '^IBKR_AUTO_REAUTH_COMPETE=' .env || echo 'IBKR_AUTO_REAUTH_COMPETE=false' >> .env
grep -q '^IBKR_ALERT_WEBHOOK_URL=' .env || echo 'IBKR_ALERT_WEBHOOK_URL=' >> .env
grep -q '^IBKR_ALERT_COOLDOWN_SECONDS=' .env || echo 'IBKR_ALERT_COOLDOWN_SECONDS=900' >> .env
grep -q '^IBKR_LOGIN_URL=' .env || echo 'IBKR_LOGIN_URL=https://localhost:5000' >> .env
grep -q '^IBKR_LOGIN_TUNNEL_COMMAND=' .env || echo 'IBKR_LOGIN_TUNNEL_COMMAND=ssh -L 5000:127.0.0.1:5000 root@100.104.236.78' >> .env
grep -q '^IBKR_ASSISTED_LOGIN_ENABLED=' .env || echo 'IBKR_ASSISTED_LOGIN_ENABLED=false' >> .env
grep -q '^IBKR_USERNAME=' .env || echo 'IBKR_USERNAME=' >> .env
grep -q '^IBKR_PASSWORD=' .env || echo 'IBKR_PASSWORD=' >> .env
grep -q '^IBEAM_ACCOUNT=' .env || echo 'IBEAM_ACCOUNT=' >> .env
grep -q '^IBEAM_PASSWORD=' .env || echo 'IBEAM_PASSWORD=' >> .env
docker compose config --quiet
docker compose up -d --build yfinance-api yf-enrichment ibkr-gateway ibkr-broker
docker compose ps yfinance-api yf-enrichment ibkr-gateway ibkr-broker
curl -sS http://127.0.0.1:18080/health
```

Le gateway est publie uniquement sur `127.0.0.1:5000` du VPS. Pour le login IBKR
depuis la machine locale :

```bash
ssh -L 5000:127.0.0.1:5000 root@100.104.236.78
# puis ouvrir https://localhost:5000
```

Controle apres login :

```bash
curl -sS http://127.0.0.1:18080/health
curl -sS -X POST http://127.0.0.1:18080/auth/initialize
curl -sS -X POST http://127.0.0.1:18080/auth/recover
```

Le second appel doit repondre `ok=true` uniquement si la session Gateway/SSO est
encore valide. S'il echoue avec `manual_login_required`, ouvrir a nouveau
`https://localhost:5000` et valider IBKR/2FA.

Les task-runners n8n externes doivent exposer les variables IBKR aux Code nodes.
Sur le VPS actuel, cela passe par `/opt/trader-ia/n8n-task-runners.clean.json` :

- ajouter les variables IBKR et AG1-FX dans `allowed-env`;
- definir `N8N_BLOCK_RUNNER_ENV_ACCESS=false` dans `env-overrides`.

## Nodes n8n

Actions AG1-V3 :

- fichier code : `agents/trading-actions/AG1-V3-Portfolio manager/workflow/nodes/post_agent/07b_ibkr_send_orders.js`
- node importable : `agents/trading-actions/AG1-V3-Portfolio manager/workflow/nodes/post_agent/07b_ibkr_send_orders.node.json`
- deja cable dans le template et les variantes entre `7 - Validate & Enforce Safety`
  et `8 - Build DuckDB Bundle`.

Forex AG1-FX-V1 :

- fichier code : `agents/trading-forex/AG1-FX-V1-Portfolio manager/nodes/post_agent/11b_ibkr_send_orders_fx.py`
- node importable : `agents/trading-forex/AG1-FX-V1-Portfolio manager/nodes/post_agent/11b_ibkr_send_orders_fx.node.json`
- deja cable dans le template et les variantes entre `11 Validate Enforce Safety FX`
  et `12 Simulate Fills FX`.

## Sequence de Validation

Dry-run broker :

1. Garder `IBKR_DRY_RUN=true`.
2. Deployer `ibkr-gateway` et `ibkr-broker`.
3. Ouvrir le tunnel SSH vers le gateway et s'authentifier a IBKR.
4. Verifier `/health`.
5. Executer des runs n8n avec `IBKR_SEND_DRY_RUN_TO_BROKER=false`.
6. Optionnel : passer `IBKR_SEND_DRY_RUN_TO_BROKER=true` pour tester les appels
   broker sans ordre live.

Paper production Forex :

1. Verifier que le compte IBKR detecte commence par `DU`.
2. Activer `IBKR_DRY_RUN=false`, `IBKR_REQUIRE_PAPER_ACCOUNT=true` et
   `IBKR_PAPER_ACCOUNT_PREFIXES=DU`.
3. Garder un seul PM Forex actif par compte IBKR. En production actuelle : GPT
   actif, Grok/Gemini inactifs.
4. Lancer `AG1-FX-PF-V1` et verifier `core.reconciliation_log`.
5. Lancer un run AG1-FX GPT seulement si la reconciliation est `OK`.
6. Surveiller `core.orders.status`: `submitted` signifie ordre envoye en attente
   de fill IBKR; `filled` signifie fill confirme.
7. Controler les couts avec `core.fill_costs`: une ligne par fill, avec
   `commission_source` commencant par `ibkr_` en paper/live. `simulated_bps`
   doit uniquement apparaitre lorsque `IBKR_DRY_RUN=true`.
8. Controler `payload_json.cash_balances` dans `core.reconciliation_log` :
   `currency_deltas` doit rester vide hors petits arrondis de devise.

En paper/live, si un appel broker echoue, les nodes marquent l'ordre en erreur
et ne creent pas de fill simule pour cet ordre.
