# Analyse AG4_Spé-V2 — pertinence, efficacité, efficience LLM

**Date** : 2026-06-17 · **Auteur** : Nicolas × Claude · **Périmètre** : workflow n8n `AG4_Spé-V2` (news single-stock Boursorama → `ag4_spe_v2.duckdb`) et sa consommation par `AG1 V4 Consensus`.
**Sources** : code repo (`agents/trading-actions/AG4-SPE-V2/`, `…/AG1-V4-Consensus…/nodes/pre_agent/`), DuckDB live VPS `/local-files/duckdb/ag4_spe_v2.duckdb` (614 Mo, lecture seule via `yf-enrichment`), `run_log` n8n.

---

## 0. TL;DR (verdict)

| Axe | Verdict | Détail |
|---|---|---|
| **Efficience LLM (coût)** | 🟢 Excellente | Le LLM (gpt-5-mini) ne tourne que sur les articles **nouveaux** : ~3 250 appels en 4 mois, 3,8/run en moyenne, 0 sur les derniers runs. Le coût est négligeable — c'est même le seul axe sain. |
| **Efficacité (qualité du signal produit)** | 🟠 Moyenne | Seules **3 254 lignes / 19 518 (16 %)** portent une vraie analyse LLM ; les 84 % restantes sont des placeholders « Noise / impact 0 / WATCH ». Parmi les analysées, 2/3 jugées non pertinentes par le LLM lui-même. Signal actionnable réel : ~470 BUY/SELL en 4 mois. |
| **Pertinence (fraîcheur + couverture)** | 🔴 Dégradée | **Bug de dates** (`published_at` 2016→2031, 63 % invraisemblables), **rotation trop lente** (max 68 j sans repasser sur un symbole, 33 symboles > 14 j), **47 runs zombies « RUNNING »** (12 %), **aucune nouvelle analyse depuis le 11/06** (6 jours). |
| **Service rendu à AG1 V4** | 🔴 Sous-exploité **et** corrompu | AG1 V4 **jette toute l'analyse riche** et ne garde qu'**un scalaire** : `Symbol_News_Impact_7d` (somme d'`impact_score` sur 7 j), pondéré ~10 %. Or ce scalaire est bâti sur la fenêtre 7 j, elle-même faussée par le bug de dates futures. |

**Conclusion** : le pilier « base de news » n'est **pas encore au niveau** pour servir efficacement AG1 V4 — non pas par excès de coût, mais par un défaut de **qualité de données (dates)**, de **fraîcheur (rotation/zombies)** et d'**exploitation aval (AG1 n'utilise qu'1 chiffre sur 12 champs produits)**.

---

## 1. Ce que fait le workflow (faits validés — code)

- **Déclenchement** : cron `0 5 9,12,15 * * 1-5` → 09:05, 12:05, 15:05 en semaine (3 runs/jour).
- **Univers** : lu dans `ag2_v3.duckdb` (`main.universe`). **420 symboles distincts** vus dans la base.
- **Queue rotative** : batch **20 symboles/run**, offset persisté (`workflow_state.ag4_spe_v2_last_symbol_index`). Débit théorique = 60 symboles/jour → **cycle complet ≈ 7 jours**.
- **Scraping** : page `cours/actualites/<ref>/` → extraction articles → **max 10 articles/symbole** (`04_normalize_articles.js`, `LIMIT=10`) → dédup `news_id = sha1(symbol|canonical_url)`.
- **Filtre fraîcheur** : `LOOKBACK_DAYS=120` sur `published_at` (`08_prepare_llm_input.js`).
- **LLM** : `S19` = **gpt-5-mini**, JSON-schema strict `specific_stock_news_v2` (12 champs : `isRelevant, relevanceReason, impactScore[-10..10], sentiment, category, summary, confidence[0..100], horizon, urgency, suggestedSignal, keyDrivers[2..5], needsFollowUp`). Prompt buy-side, anti-hallucination, `isRelevant=false` si doute.
- **Écriture** : `news_history` (analyse), `news_errors`, `run_log` (stats).

---

## 2. Efficience LLM — 🟢 (faits validés — données live)

- **Appels LLM = uniquement les articles nouveaux.** Tout article déjà vu (`news_id` connu) est routé `skip` sans appel LLM (`S11/S12`).
- Sur les **30 derniers runs** : 3 274 articles scrapés, **114 analysés (3,5 %)**, 3 160 skippés (doublons). Moyenne **3,8 analyses/run**.
- Total historique : **3 254 analyses LLM** depuis le 17/02/2026 (≈ 4 mois).
- **Coût** : gpt-5-mini × ~3 250 appels, contenu plafonné à 12 000 caractères → coût total de l'ordre de quelques € sur 4 mois. **Aucun gaspillage de tokens.**

> ⚠️ Nuance : l'efficience est si bonne qu'elle confine à la **sous-utilisation**. Sur les runs du 15→17/06, `items_analyzed = 0` : le LLM ne produit plus rien (voir §4, fraîcheur).

---

## 3. Efficacité — 🟠 (faits validés — données live)

**Composition de `news_history` (19 518 lignes) :**

- Lignes **réellement analysées** (`summary` non vide) : **3 254 (16 %)**.
- Les **16 264 autres (84 %)** sont des placeholders : `category='Noise'`, `impact_score=0`, `suggested_signal='WATCH'`, `summary`/`key_drivers` vides, `is_relevant=true` par défaut.

**Parmi les 3 254 analysées :**

| Champ | Distribution |
|---|---|
| `is_relevant` | **False 2 138 (66 %)** / True 1 116 (34 %) |
| `category` | Noise 2 179 · Earnings 431 · Contract/Product 201 · M&A 143 · Legal/Reg 119 · Management 106 · Macro/Sector 51 · Analyst Rating 24 |
| `suggested_signal` | WATCH 2 785 · **BUY 313 · SELL 109** · NEUTRAL 47 |
| `impact_score≠0` | 2 222 (68 %) |
| `confidence` (relevant) | moyenne **75,5** / 100 |

**Lecture** : quand le LLM tourne, sa sortie est structurée et plausible (confiance ~75, schéma respecté, 0 erreur de parsing). Mais :
1. **2/3 des articles analysés sont jugés non pertinents** par le LLM → on paie l'analyse d'un grand volume de bruit Boursorama (cotations, pubs, articles génériques).
2. **Le signal actionnable est rare** : ~470 BUY/SELL sur 4 mois, soit ~4/jour ouvré tous symboles confondus.
3. **Piège pour le consommateur** : un filtre naïf `is_relevant=true` ramène 17 361 lignes dont ~16 245 sont des **placeholders vides** (le défaut `true` des lignes non analysées). Le bon filtre est `summary IS NOT NULL` ou `impact_score≠0`.

---

## 4. Pertinence (fraîcheur + couverture) — 🔴

### 4.1 Bug de dates `published_at` (faits validés)
- Plage observée : **2016-05-31 → 2031-12-25**. **1 872 lignes datées dans le futur**, **10 392 avant 2024** (63 % du total invraisemblables).
- Exemples réels : « Société Générale 33e plan » → `published_at=2027-05-26` ; « Vinci rachat Fletcher » → `2029-05-26` ; Dassault Systèmes → `2028-05-26`. Le champ `published_at_raw` est **déjà** faux (`2027-05-26T00:00:00.000Z`).
- Âge médian `published_at`→`analyzed_at` = **-364 jours** (publication « après » l'analyse).
- **Cause racine probable** (hypothèse à confirmer) : `normalizeDate()` dans `nodes/07_parse_article.js` (node S16). La regex `(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})` capture le **premier motif type-date** rencontré — souvent une **année d'échéance/projection citée dans le corps ou le snippet** (« obligataire 2031 », « plan mondial 2030 ») — au lieu de la date de publication. Les jours/mois sont cohérents (26/05), seule l'**année** dérive → typique d'un faux positif sur du texte financier.
- **Impact en cascade** : casse (a) le filtre `LOOKBACK 120 j` d'AG4 lui-même, (b) les fenêtres 7 j/30 j d'AG1 V4 (§5), (c) tout tri chronologique.

### 4.2 Rotation / couverture (faits validés)
- Ancienneté du dernier passage par symbole : **132 symboles ≤ 2 j · 92 entre 3-7 j · 163 entre 8-14 j · 33 > 14 j**. **Max = 68 jours** sans repasser.
- Cause : 420 symboles / 60 slots/jour ≈ 7 j de cycle **nominal**, mais dégradé par les runs partiels (souvent `symbols_ok` ≪ 20 : ex. 15/06 07:05 → **1/20** ; 17/06 07:05 → 10/20).

### 4.3 Fiabilité d'exécution (faits validés)
- **47 runs `RUNNING` jamais finalisés / 391 (12 %)** → runs qui plantent en milieu de pipeline (timeout scraping, lock DuckDB, ou `S13 Wait`).
- `items_analyzed` chute : 131 (29/05) → 33 (03/06) → 9 (10/06) → **0 (15-17/06)**.
- **Dernière vraie analyse : 2026-06-11 13:21**. Soit **6 jours sans aucune news fraîche analysée** au 17/06.
- `news_errors` = 9 lignes seulement → les échecs ne sont **pas** tracés comme erreurs ; ils se traduisent en `symbols_ok` faible et runs zombies, donc **invisibles** sans inspection du `run_log`.

---

## 5. Service rendu à AG1 V4 — 🔴 (faits validés — code `R8_data_prep_matrix.code.py` + `calcul_matrice_briefing.code.py`)

**Comment AG1 V4 lit la base** (`R8`, l.301-325) : une agrégation par symbole sur 30 j produisant **5 features** :
`count_7d, count_30d, impact_7d, impact_30d, last_news_date`.
→ **Aucun** des champs qualitatifs (`summary, key_drivers, suggested_signal, sentiment, category, horizon, confidence, relevance_reason`) n'est lu.

**Comment ces features pèsent dans la décision** (`calcul_matrice_briefing`) : seul **`Symbol_News_Impact_7d`** entre réellement :
- `news_risk = clamp(max(0,-impact_7d)*8 + …)` ;
- `reward_catalyst = clamp(max(0, impact_7d)*6 + …)` ;
- `sentiment_prob = clamp(50 + impact_7d*4 + …)` ;
- poids `news` dans le score composite = **0,10 (10 %)** ; `last_news_date` affiché au PM pour contexte.

**Reproduction de la requête AG1 sur la base live (au 17/06)** :
- 242 symboles remontent des features ; 152 avec `count_7d>0` ; **97 avec `impact_30d≠0`**.
- **`last_news_date` corrompu** pour les tops : HO.PA → 2030-01-26, TTE.PA → **2031-03-26**, ALO.PA → 2031-03-26. Le PM reçoit des dates de news en **2030/2031**.
- Fenêtre 30 j : 2 222 lignes dont **1 981 (89 %) à impact 0** → `count_30d` mesure surtout du bruit.
- Les fenêtres 7 j/30 j sont **mécaniquement faussées** : un article mal-daté 2030 satisfait toujours `≥ now()-7d`, donc compte comme « news des 7 derniers jours ».

**Conséquence** : l'unique canal news→décision (`impact_7d`, 10 %) est alimenté par une fenêtre temporelle non fiable, et 90 % de l'effort d'analyse (les 12 champs riches) est produit puis ignoré.

---

## 6. Synthèse faits / hypothèses / actions

### Faits validés
1. Efficience LLM excellente (≈3 250 appels/4 mois, 0 gaspillage) — voire sous-utilisation actuelle (0 analyse depuis le 11/06).
2. 84 % de `news_history` = placeholders vides ; 16 % analysés, dont 66 % jugés non pertinents.
3. `published_at` corrompu sur 63 % des lignes (2016→2031).
4. 47 runs zombies (12 %), couverture symboles partielle, max 68 j de staleness.
5. AG1 V4 n'exploite qu'`impact_7d` (~10 % du score) + `last_news_date` ; ignore tout le reste ; et `impact_7d` repose sur une fenêtre faussée par les dates.

### Hypothèses (à confirmer)
- H1 : cause du bug dates = regex gloutonne dans `normalizeDate()` (S16) capturant une année du corps/snippet. *Confirmation : logguer `timeDt/metaPub/metaDate` vs valeur retenue sur 20 articles.*
- H2 : runs zombies = timeout HTTP Boursorama / rate-limit / lock DuckDB pendant `S22`. *Confirmation : exécutions n8n SQLite (`error`, `stoppedAt IS NULL`).*
- H3 : la chute `items_analyzed→0` = scraping qui ne renvoie plus d'articles neufs (sélecteur listing cassé ou anti-bot), pas une absence réelle de news. *Confirmation : run manuel sur 1 symbole + inspection HTML.*

### Actions recommandées (priorisées)
**P0 — fiabiliser la donnée (préalable à toute confiance d'AG1)**
1. **Corriger `normalizeDate()`** : ne dater que depuis `<time datetime>` / `article:published_time` ; rejeter toute année hors `[now-2ans, now+7j]` → fallback `first_seen_at`. Re-backfill `published_at` (ou basculer AG1 sur `first_seen_at` en attendant — voir P1.4).
2. **Tuer/auditer les runs zombies** : ajouter un timeout global + finalisation `PARTIAL` garantie, et tracer les symboles à 0 article dans `news_errors`.
3. **Diagnostiquer la chute à 0 analyse** (H3) : run manuel, vérifier le sélecteur listing.

**P1 — fiabiliser la fraîcheur + l'usage aval**
4. **AG1 V4 : remplacer le tri/fenêtre sur `published_at` par `COALESCE(first_seen_at, …)`** tant que `published_at` n'est pas corrigé (`first_seen_at` est fiable et reflète la date de collecte). Sinon `impact_7d` reste faux même après P0.
5. **Augmenter la fréquence de rotation** des symboles portés en portefeuille / watchlist AG1 (prioriser les positions ouvertes plutôt que rotation uniforme à 7 j).

**P2 — rentabiliser l'analyse déjà produite**
6. **Exploiter les champs riches dans AG1** : passer au PM, pour chaque symbole, les 1-3 dernières news `is_relevant=true AND summary IS NOT NULL` (titre + `summary` + `suggested_signal` + `impact_score`), pas seulement un scalaire. C'est là que 84 % de la valeur dort.
7. **Pré-filtre anti-bruit avant LLM** (optionnel, efficience) : sauter l'analyse des titres manifestement non-news (cotations, agenda) pour réduire les 66 % de `is_relevant=false`.

---

## 7. Requêtes de vérification (read-only)

```bash
# Accès base (read_only) via container yf-enrichment
docker exec yf-enrichment python3 - <<'PY'
import duckdb; c=duckdb.connect("/files/duckdb/ag4_spe_v2.duckdb", read_only=True)
print("analysées:", c.execute("SELECT count(*) FROM news_history WHERE summary IS NOT NULL AND summary<>''").fetchone())
print("dates futures:", c.execute("SELECT count(*) FROM news_history WHERE published_at>now()").fetchone())
print("zombies:", c.execute("SELECT count(*) FROM run_log WHERE status='RUNNING'").fetchone())
print("dernière analyse:", c.execute("SELECT max(first_seen_at) FROM news_history WHERE summary IS NOT NULL AND summary<>''").fetchone())
PY
```
