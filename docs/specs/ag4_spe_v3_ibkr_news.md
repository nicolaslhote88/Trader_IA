# AG4_Spé-V3 — Ajouter les news IBKR aux news single-stock

**Date** : 2026-06-18 · **Auteur** : Nicolas × Claude · **Statut** : spec / plan (à valider)
**But** : **compléter** (pas remplacer) le scraping Boursorama de AG4_Spé par les news spécifiques par symbole fournies par IBKR, via l'API Client Portal déjà utilisée par le broker.

---

## 0. RÉSULTATS Phase 0 — sonde live (2026-06-18, session authentifiée)

Sondé sur la gateway réelle (Build 10.46.1l) :
- ✅ **`/iserver/news/portfolio` fonctionne** : **50 titres**, multi-providers — `DJ-N` (Dow Jones News) 15, `RCG` 15, `DJ-GL` 6, `REUTERS` 6, `TGL`, `RTFL`, `PR`, `DJ-PR`, `ISSEVA-C`, `DJ-LKO`. **Couvre les valeurs FR détenues** (ex. « REUTERS LEGRAND SHARES UP 3% », news Dassault). Champs : `id, headline, source, provider, sentiment, receiptTime` (epoch fiable), `tickers`. **Titre seul (pas de texte intégral).**
- ✅ Résolution conid OK ; `/fyi/notifications` OK (notifs analystes FR).
- ❌ **`/iserver/news?conid=` (news par contrat, toute valeur)** : **503 « server id not found » / timeout** — NON fonctionnel sur ce build (sans doute besoin d'un *market-data server* par conid, non garanti / coûteux). ❌ `/iserver/news/article` = 404 (pas de texte intégral via REST).

**Conséquence sur le design** : la V3 s'appuie sur **`/iserver/news/portfolio`** (news premium pour les **valeurs détenues**) en **complément** de Boursorama (qui couvre tout l'univers en rotation). La couverture *univers complet* par IBKR par-contrat reste un spike futur (non disponible aujourd'hui). C'est en réalité un **excellent alignement** : les positions ouvertes — les plus importantes pour AG1 — reçoivent Reuters/Dow Jones + sentiment + dates fiables, en **1 seul appel API/run**.

---

## 1. Faisabilité — verdict

**Oui, confirmé en live** : faisable et à forte valeur pour les **valeurs détenues** (endpoint portfolio). La couverture par-contrat de tout l'univers n'est pas disponible via REST sur ce build (voir §0).

Faits validés (code/infra) :
- Le broker (`services/ibkr-broker/cpapi_client.py`) parle déjà au **Client Portal API** (`/v1/api/...`) avec session gérée, et sait **résoudre symbol → conid** (`/iserver/secdef/search`, `contract_cache.py`) + `marketdata/snapshot` + `hmds/history`. La brique d'identification d'instrument nécessaire aux news IBKR **existe déjà**.
- IBKR expose des **news par contrat** (par `conid`) dans ses API (providers, headlines, articles).

Conditions / caveats (à lever en étape 0, **session authentifiée requise**) :
1. **Abonnements news.** Les news IBKR sont *gated* par provider. Le « tape » IBKR de base est limité ; les providers utiles (Dow Jones, Reuters, Benzinga, etc.) sont des **abonnements payants** à activer dans Account Management, souvent avec **accord de diffusion**. Sans abonnement, l'endpoint renvoie peu/rien.
2. **Restrictions de diffusion (licensing).** Le **texte intégral** des providers payants est généralement *display-only / non redistribuable* → l'injecter dans un LLM et le **stocker** peut violer l'accord. Les **titres + métadonnées** (headline, provider, horodatage, conid) sont généralement OK. ⇒ design par défaut : analyser sur **titre/résumé**, ne persister le texte intégral que si le provider l'autorise.
3. **Couverture vs univers.** L'univers AG4_Spé est très **français (.PA)**, dont beaucoup de small/mid caps. La couverture IBKR/Dow Jones/Reuters y est **plus faible** que Boursorama (forte sur US/large caps). ⇒ IBKR **complète** surtout les valeurs US/grandes capis ; Boursorama reste le meilleur sur les petites valeurs FR. Attente réaliste : gain net surtout si le portefeuille contient des US/large caps.
4. **Fragilité de session.** La session CP API **tombe la nuit** (401, relogin 2FA manuel — cf AGENTS.md). Une source news IBKR hérite de cette fragilité : un run sans session = 0 news IBKR ce run. ⇒ IBKR en **complément** (Boursorama garde le pipeline vivant), et l'alerte santé A3 couvre les trous.

**Atout majeur** : les news IBKR ont des **horodatages fiables** → pas de bug de dates comme Boursorama (cf B1). Source de qualité, structurée, par `conid`.

---

## 2. Architecture cible (complément, source unifiée)

On garde le pipeline AG4_Spé-V2 (queue priorisée portefeuille, dedup, LLM, DuckDB) et on ajoute **une 2ᵉ source** qui se déverse dans la **même** `news_history` :

```
S01 queue (symbols actions, held-first)
   ├── [existant] S04 Boursorama listing → extract → articles (source=boursorama)
   └── [NOUVEAU] S04b Broker /news/symbol/{symbol} → articles IBKR (source=ibkr)
                         (broker: symbol→conid→ /iserver news endpoint)
   → merge articles (2 sources)
   → dedup (news_id) → route new/seen → S19 LLM (schéma inchangé)
   → write news_history (source, provider, conid, news_article_id)
```

- **Réutilise tel quel** : S01 (univers/priorité), S11 route new/seen, S16/parse (pour Boursorama), S17/S19 LLM (schéma `specific_stock_news_v2` inchangé), S20 parse, S22 write, S24 finalize, A1/A3.
- **`news_id` IBKR** : `sha1(symbol|provider|articleId)` (au lieu de `symbol|url`) → dedup propre, pas de collision avec Boursorama.
- **`published_at`** : depuis l'horodatage IBKR (fiable) → bypass du garde-fou de dates.

---

## 3. Changements de schéma (additif)

`ag4_spe_v2.duckdb.news_history` (la V3 peut rester sur la même base ; sinon `ag4_spe_v3.duckdb`) :
```sql
ALTER TABLE news_history ADD COLUMN IF NOT EXISTS provider        VARCHAR; -- ex: BRFG, DJNL, RSF...
ALTER TABLE news_history ADD COLUMN IF NOT EXISTS news_article_id VARCHAR; -- id article IBKR
ALTER TABLE news_history ADD COLUMN IF NOT EXISTS conid           BIGINT;  -- contrat IBKR
-- `source` existe déjà : 'boursorama' | 'ibkr'
```
`news_analyzed` (vue B3) : inchangée (filtre summary ∧ is_relevant) ; expose `source/provider` pour pondération aval.

---

## 4. Plan d'actions par phases

### Phase 0 — Spike / confirmation ✅ FAIT (2026-06-18, cf §0)
Résultat : endpoint **portfolio** OK (multi-providers, FR couvert, titre+sentiment+date fiable) ; endpoint **par-contrat** indisponible (503). Design recadré sur le portfolio.

### Phase 1 — Endpoint broker (extension `ibkr-broker`)
- `cpapi_client.py` : `get_portfolio_news()` → wrappe `GET /iserver/news/portfolio` (1 appel, renvoie les 50 derniers titres des valeurs détenues).
- `app.py` : route `GET /news/portfolio` → renvoie items normalisés `{news_article_id, provider, source, headline, datetime (depuis receiptTime_r), sentiment, symbol_guess}`. `symbol_guess` = préfixe du headline (« LR - … » → LR) recoupé avec les positions détenues. Gestion 401/erreurs → liste vide + trace (pas de crash).
- (Optionnel, spike futur) `get_contract_news(conid)` quand/si l'endpoint par-contrat redevient dispo (warm-up md server à étudier).
- ⚠️ Image broker *baked* → **committer** dans le repo + `docker compose build ibkr-broker` (cf AGENTS.md).

### Phase 2 — Workflow AG4_Spé-V3
- Dupliquer AG4_Spé-V2 → V3 (build script). Ajouter une branche **S04b** : **un seul** appel HTTP broker `GET /news/portfolio` par run (pas par symbole — économe), avec retry.
- Node de normalisation IBKR : exploser les titres par symbole détenu (préfixe headline / `tickers`), mapper vers le shape commun (symbol, news_id = `sha1(symbol|provider|id)`, title=headline, publishedAt depuis `receiptTime_r`, source='ibkr', provider, sentiment, conid si dispo). Ignorer les titres non rattachables à une valeur détenue.
- Brancher sur le **même** merge → dedup → LLM (titre) → write. `12_write_news_duckdb` : mapper `source/provider/news_article_id/sentiment`.
- Comme l'endpoint est *held-only*, ça renforce directement la priorité portefeuille (C1) : les positions reçoivent en plus le flux Reuters/DJ. Boursorama continue de couvrir l'univers en rotation.

### Phase 3 — Garde-fou licence/diffusion
- Table de config provider → `store_full_text` (bool). Par défaut **titre+résumé seulement** pour providers payants ; full-text seulement si autorisé. L'analyse LLM tourne sur ce qui est disponible.

### Phase 4 — Dédup inter-sources & exploitation
- Même histoire vue 2× (Boursorama + IBKR) : dédup souple optionnelle (titre normalisé + fenêtre temps) ; a minima `source` taggé pour qu'AG1 **pondère** (Reuters/DJ > scrape).
- Dashboard : répartition par `source`/`provider`.

### Phase 5 — Validation & rollout
- Shadow run V3 : comparer volume/couverture IBKR vs Boursorama, vérifier dates fiables, **mesurer l'augmentation du volume LLM** (lien direct avec la prudence D2 sur le volume injecté aux 3 LLM).
- Déploiement chirurgical (export live → patch → publish → restart), backups, comme pour V2.

---

## 5. Risques & points de vigilance
- **Licence/diffusion** des providers payants (Phase 3) — le point juridique le plus sensible.
- **Couverture FR faible** : sur un univers .PA, le gain peut être modeste hors US/large caps. Mesurer en Phase 0/5 avant d'investir.
- **Session IBKR fragile** : ne pas rendre AG4 dépendant d'IBKR ; complément seulement.
- **Coût LLM** : plus de news = plus d'appels. Quantifier (Phase 5) avant d'élargir, cohérent avec la décision de différer D2.
- **Rate limits CP API** : espacer les appels (réutiliser la logique S13 Wait / retry).

---

## 6. Prochaine étape immédiate
Reconnecter la session IBKR puis lancer la **Phase 0** (je fournis le script de sonde read-only providers + news par conid). Décision go/no-go sur la base des providers réellement disponibles et de la couverture observée.
