# Analyse du système Trader_IA avant branchement Agent 6 (broker)

**Date :** 20 avril 2026
**Auteur :** Claude (Cowork), à la demande de Nicolas
**Objet :** Restituer, avant d'engager l'intégration broker, une lecture objective de ce qui existe déjà dans le repo, de ce qui est prêt pour le live, et de ce qui reste à traiter.
**Périmètre lu :** `vps_hostinger_config/docker-compose.yml`, `AG1-V3-Portfolio manager/**` (workflow, SQL, post_agent code), `AG0-AG4 workflows JSON`, `dashboard/app.py` (scans ciblés), `ETAT_DES_LIEUX_FONCTIONNEL.md`.

---

## 1. Cartographie : du découpage technique (AG0→AG4) vers la taxonomie 6 agents

| Agent (taxonomie) | Rôle | Où dans le code | État |
|---|---|---|---|
| **1. Portfolio Manager** | Décision, arbitrage, allocation | `AG1-V3-Portfolio manager/workflow/AG1_workflow_template_v3.json` (84 nodes) | **Ensemble 3 modèles en parallèle** : `gpt-5.2`, `grok-4-1-fast-reasoning`, `gemini-3` — chaque variante écrit dans sa propre DuckDB (`ag1_v3_chatgpt52.duckdb`, `ag1_v3_grok41_reasoning.duckdb`, `ag1_v3_gemini30_pro.duckdb`) |
| **2. Technical Analyst** | Tendance, momentum, supports | `AG2-V3/AG2-V3 - Analyse technique.json` + variantes FX/non-FX | 30 nodes, HTTP vers `yfinance-api`, LLM OpenAI |
| **3. Fundamental Analyst** | Qualité business, valorisation | `AG3-V2/AG3-V2-workflow.json` | Fichier avec BOM UTF-8 (déjà signalé dans l'état des lieux — ne casse pas n8n mais complique les tooling externes) |
| **4. Sentiment & Macro / News** | Breaking news, macro, sentiment | `AG4-V3/AG4-V3-workflow.json` (news générales, 30 nodes + RSS) + `AG4-SPE-V2/AG4-SPE-V2-workflow.json` (news par valeur, 45 nodes) | Deux workflows séparés, l'un macro, l'autre idiosyncratique |
| **5. Risk Manager** | Veto, sizing, exposure | `AG1-V3-Portfolio manager/nodes/post_agent/07_validate_enforce_safety_v5.code.js` (367 lignes) | Node JS intégré dans AG1-V3, non un workflow à part |
| **6. Execution Trader** | Broker, ordres réels | `AG1-V3-Portfolio manager/nodes/post_agent/08_build_duckdb_bundle.code.js` (362 lignes) | **Sandbox** : fills fabriqués in-memory au prix théorique, écrits tels quels en base |

**Observation importante :** les agents 5 et 6 ne sont pas des workflows n8n autonomes aujourd'hui. Ce sont des *Code nodes JS* chaînés en sortie de la décision du PM dans AG1-V3. Cela a deux implications :
- Le "droit de veto" du Risk Manager s'exécute dans le même process n8n que la décision → pas d'isolation fault-tolerant.
- Pour passer en live, il faut décider : on garde cette structure "tout dans AG1-V3" (plus simple, cohérent avec l'existant), ou on extrait un workflow dédié `AG5-Executor` (meilleure isolation, meilleure observabilité, mais double la complexité opérationnelle).

---

## 2. Ce qui a été **anticipé par design** et qui nous fait gagner du temps

### 2.1 Le schéma DuckDB était pensé pour le broker dès le jour 1

Dans `sql/portfolio_ledger_schema_v2.sql` :

```sql
CREATE TABLE core.orders (
  order_id VARCHAR PRIMARY KEY,
  ...
  broker VARCHAR,             -- ⟵ prêt
  broker_order_id VARCHAR,    -- ⟵ prêt
  ...
);

CREATE TABLE cfg.portfolio_config (
  config_version VARCHAR PRIMARY KEY,
  ...
  kill_switch_active BOOLEAN,         -- ⟵ prêt
  max_pos_pct DOUBLE,                 -- ⟵ prêt
  max_sector_pct DOUBLE,              -- ⟵ prêt
  max_daily_drawdown_pct DOUBLE,      -- ⟵ prêt
  ...
);
```

Et dans `duckdb_writer.py`, ligne 461, le champ `broker` est simplement défaulté à `"SIM"` quand absent :

```python
_clean_text(r.get("broker"), 64) or "SIM"
```

**Concrètement : pour tagger un ordre comme "réellement exécuté chez IBKR", il suffit de passer `broker: "IBKR_IBIE"` + `broker_order_id: "123456789"` dans le bundle. Zéro migration SQL.**

### 2.2 Le writer DuckDB est atomique ET idempotent

Dans `duckdb_writer.py`, `upsert_run_bundle()` :
- Transaction `BEGIN ... COMMIT / ROLLBACK` couvre tous les upserts (orders, fills, snapshots…)
- Toutes les tables ont des `ON CONFLICT DO UPDATE` → on peut ré-écrire un bundle sans corruption

C'est **la fondation exacte qu'il faut** pour un vrai broker : en cas d'erreur réseau après envoi de l'ordre, on peut relancer le workflow sans créer de doublons. Il manque juste un `client_order_id` côté broker pour fermer la boucle (voir §4).

### 2.3 Le Risk Manager référence déjà AG5 et AG6

Dans `07_validate_enforce_safety_v5.code.js` :

```js
if (!deps.includes("AG5_RISK_APPROVAL")) deps.push("AG5_RISK_APPROVAL");
if (!deps.includes("AG6_EXECUTION")) deps.push("AG6_EXECUTION");
```

Ces dépendances sont ajoutées aux actions FX quand `enable_fx=false`. C'est un squelette de "drapeau qualitatif" — pas un enforcement dur, mais la convention de nommage est posée.

### 2.4 L'architecture ensemble (GPT-5.2 / Grok-4.1 / Gemini) est un filet de sécurité

Chaque modèle écrit dans sa propre base. On peut donc :
- faire tourner le live sur *un seul* modèle au départ (ex : GPT-5.2, le plus stable d'après la config)
- ou exiger un consensus N-sur-3 avant toute exécution réelle (règle simple : on n'envoie un ordre que si au moins 2 des 3 PM le proposent dans la même direction, sur le même titre, dans la même fenêtre horaire)

**C'est un avantage différenciant versus un système mono-modèle. À exploiter explicitement dans la phase live.**

---

## 3. Le mécanisme de sandbox actuel — localisation exacte

Dans `08_build_duckdb_bundle.code.js`, autour des lignes 150-180 :

```js
orders: ordersIn.map((o, i) => ({
  order_id: `ORD_${run_id}_${i}`,
  status: 'FILLED',                // ⟵ statut fabriqué
  broker: 'SIM',                   // ⟵ broker fabriqué
  ...
})),
fills: ordersIn.map((o, i) => {
  const px = Number(o.limitPrice) || priceMap[o.symbol];
  return {
    fill_id: `FIL_${run_id}_${i}`,
    order_id: `ORD_${run_id}_${i}`,
    price: (Number.isFinite(px) && px > 0) ? px : 1.0,  // ⟵ prix théorique, zéro slippage
    qty: o.quantity,
    ts_fill: now_iso
  };
}),
```

**C'est précisément ce bloc qu'il faudra forker pour l'agent 6 réel.** Trois changements à faire (et uniquement ces trois) :

| Changement | Avant (sandbox) | Après (live) |
|---|---|---|
| **Statut initial** | `FILLED` direct | `PENDING` jusqu'à confirmation broker |
| **Prix du fill** | Fabriqué au prix théorique | Remonté depuis l'API broker (fill réel, multiples fills possibles par ordre) |
| **Origine** | Même node synchrone | Fill écrit dans un **2ème passage** quand le broker confirme — soit par callback webhook, soit par polling |

Autrement dit : on scinde `08_build_duckdb_bundle` en deux étapes logiques.
1. "Écrire les ordres en état `PENDING` avec `broker=IBKR`, pas encore de fill."
2. "Appeler le broker, attendre la confirmation, écrire le(s) fill(s) avec les vrais prix."

Structurellement, **c'est un ajout, pas une réécriture** — le reste du code (snapshots, lots, cash ledger) reste inchangé tant qu'on respecte le contrat de données existant.

---

## 4. Ce qui manque vraiment pour passer au live

Les 2 mois de sandbox valident la chaîne de décision et l'architecture de données. Le risque résiduel se concentre sur le "dernier kilomètre broker". Voici les gaps identifiés, classés par priorité.

### 4.1 Critique (bloquant pour le live)

**a) Idempotence côté broker — `client_order_id`**
- Aucun champ `client_order_id` n'existe pour l'instant (le `order_id` interne est réutilisable côté broker si on le passe comme `client_order_id`, mais ça doit être fait explicitement).
- **Sans cela :** un timeout réseau au moment d'envoyer l'ordre → on ne sait pas si l'ordre est parti → on le renvoie → double exécution possible.
- **Fix :** passer `order_id` (ex. `ORD_run123_1`) comme `client_order_id` à IBKR. L'API IBKR rejette les doublons.

**b) Kill-switch opérationnel**
- Le champ `cfg.portfolio_config.kill_switch_active` existe en base (défaut `TRUE` à l'init — paradoxalement sécuritaire).
- **Mais :** aucun code dans `07_validate_enforce_safety_v5.code.js` ne le *lit* pour rejeter les ordres.
- **Fix :** en tout début de Node 7, lire `kill_switch_active` depuis DuckDB (ou passer en paramètre via AG1.00) et si `TRUE` → bloquer tout ordre avec raison `KILL_SWITCH_ON`.
- **Dashboard :** aucun toggle UI aujourd'hui. À ajouter — voir §4.3.

**c) Enforcement réel des limites d'exposition**
- `max_pos_pct`, `max_sector_pct`, `max_daily_drawdown_pct` sont dans `cfg.portfolio_config` mais **jamais lus** dans le code Risk Manager. Aujourd'hui le Risk Manager fait uniquement :
  - Normalisation FX
  - Extraction actions OPEN/INCREASE/DECREASE/CLOSE
  - Conversion weight% → qty
  - Garde-fou cash (redimensionne un BUY si cash insuffisant)
- **Fix :** ajouter 3 vérifications explicites dans Node 7 AVANT que les ordres partent vers Node 8 :
  - *Position sizing :* `qty * price / total_equity <= max_pos_pct`
  - *Sector :* somme exposition secteur après trade `<= max_sector_pct` (nécessite de joindre `core.instruments` pour le secteur)
  - *Drawdown :* lire le PnL journalier courant depuis la dernière snapshot, si en dessous de `-max_daily_drawdown_pct` → bloquer tout nouvel ordre (freeze day)

### 4.2 Important (mais moins urgent)

**d) Contrainte Python allow-list**
- `docker-compose.yml` fixe : `N8N_RUNNERS_PYTHON_ALLOW_LIST=duckdb,pandas,numpy,datetime,math,json`
- **Conséquence :** pas de `requests` ni `httpx` côté Python. Les appels HTTP vers le broker doivent passer par :
  1. un node **HTTP Request** n8n natif, ou
  2. un node **Code JS** avec `fetch`, ou
  3. une extension de l'allow-list (risque sécurité) ou le choix d'un node communautaire broker (ex. `@l4z41/n8n-nodes-ibkr`).
- **Décision à prendre avant de coder.** La voie "HTTP Request natif + JS orchestrateur" est la plus sobre et n'introduit aucune dépendance tierce.

**e) Ordre placeholder "PENDING" dans `core.orders`**
- Aujourd'hui les ordres sont écrits en `status='FILLED'`. Pour le live, il faut :
  1. Autoriser `status='PENDING' / 'ACCEPTED' / 'PARTIAL' / 'FILLED' / 'CANCELLED' / 'REJECTED'`
  2. Ajouter un *second* pipeline (ou workflow séparé `AG5-Reconcile`) qui balaie les `PENDING` et interroge IBKR pour mise à jour.
- Le schéma SQL est agnostique, pas de migration. C'est purement un changement de code/process.

**f) Gestion des fills partiels**
- L'API IBKR peut renvoyer plusieurs fills pour un même ordre (exécution en tranches). `core.fills` est déjà modélisé 1-N avec `order_id`, donc **aucun problème côté données**. Mais le code de fill-writing actuel (Node 8) écrit 1 fill par ordre par défaut — à adapter.

### 4.3 Qualité de vie (à prévoir vite)

**g) Dashboard — pages manquantes pour le live**
- Page **"Risk Control"** : toggle kill-switch, affichage des limites actives, consommation courante (%sector, %pos, drawdown du jour).
- Page **"Broker & Ordres"** : liste des ordres par statut (PENDING / FILLED / REJECTED), reconciliation cash broker ↔ cash DuckDB, latences, taux de rejets.
- Page **"Équité live"** : valorisation broker (depuis `account_summary` IBKR) vs. valorisation DuckDB interne (snapshot) — divergences visibles immédiatement.

**h) Tests d'intégration vs. le sandbox IBKR (Paper Trading IBKR)**
- Avant de trader en réel, 2-4 semaines en parallèle sur **IBKR Paper Trading** avec le même workflow et le même code que le live final. Pas du "fake fill" — un vrai pipeline broker, juste sur un compte simulation. C'est la dernière assurance avant de câbler le compte réel.

---

## 5. Schéma de bascule recommandé (canary)

Proposition de séquence, validée contre la structure actuelle :

1. **Semaine 1-2 :** ajouter les 3 gardes critiques (§4.1 b, c, a) dans Node 7 + séparer PENDING / FILLED dans Node 8. Tout reste en `broker='SIM'`. **Aucune régression possible car le chemin de sortie ne change pas encore.**

2. **Semaine 3-4 :** brancher IBKR **en mode Paper** sur *un seul* PM (ex : GPT-5.2). Le bundle garde la sortie SIM en DuckDB ; en parallèle, un nouveau sous-pipeline écrit aussi en `broker='IBKR_PAPER'`. Dashboard page "Broker" ajoutée. Comparer distribution slippage paper vs. slippage théorique.

3. **Semaine 5-6 :** bascule IBKR en **Live** sur un *sous-ensemble réduit* (ex : 10 k€, uniquement ETF + 2 actions US majeures liquides, pas encore de FX). Quorum 2-sur-3 PM pour exécuter.

4. **Semaine 7+ :** élargir univers, puis FX, puis autres modèles du consensus. Kill-switch UI opérationnel. Enforcement drawdown journalier en prod.

Ce plan ne crée à aucun moment un point de rupture : la sandbox continue de tourner en parallèle (historique comparable), et chaque étape peut être revert sans toucher au schéma ni aux workflows amont.

---

## 6. Questions ouvertes à trancher avant de coder

1. **Topologie broker :** agent 6 reste dans AG1-V3 (simple, cohérent avec l'existant) ou devient un workflow séparé `AG5-Executor` (isolation, observabilité) ?
2. **Consensus requis pour exécution :** 1-sur-3 PM suffit, ou on exige 2-sur-3 ?
3. **Granularité de l'exécution :** ordres `MARKET` uniquement, ou introduire `LIMIT` avec règle de placement (mid ± X bps) ?
4. **Horizon kill-switch :** kill-switch global, ou par classe d'actif (ex : freeze forex sans freeze equity) ?
5. **Chemin HTTP broker :** node `@l4z41/n8n-nodes-ibkr` communautaire, ou HTTP Request natif n8n vers l'API Client Portal Web d'IBKR (nécessite Gateway local sur le VPS) ?

Sur ces 5 questions, les 3 premières sont des choix de gouvernance (à toi). Les 4 et 5 sont des choix techniques — on peut les discuter à partir de ce document.

---

## 7. TL;DR

- **Le système est prêt à 80 %.** Schéma DuckDB pensé pour le broker, writer atomique et idempotent, référence AG5/AG6 déjà dans le code, trois PM en ensemble qui donnent un filet naturel.
- **La sandbox se trouve dans 3 lignes de `08_build_duckdb_bundle.code.js`** (fabrication des fills). C'est le seul endroit à forker.
- **Les vrais gaps sont sécuritaires, pas architecturaux :** kill-switch non lu, limites d'exposition non enforced, pas de client_order_id. 3 à 5 jours de code.
- **Le plan de bascule peut être graduel et réversible** grâce à l'ensemble multi-modèles et au champ `broker` dans `core.orders`.

Prochaine étape que je recommande : trancher les questions §6 (surtout 1 et 5), puis produire un plan d'implémentation détaillé pour les trois gardes critiques §4.1.
