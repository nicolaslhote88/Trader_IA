# D2 — Exploitation des news single-stock par AG1 V4 (digest compact)

**Date** : 2026-06-18 · **Statut** : spec (validée sur les paramètres, à implémenter)
**Objectif** : faire que le PM AG1 V4 (GPT-5.5 / Grok 4.3 / Claude Sonnet 4.6) **lise réellement** les news par symbole (résumés/signaux), au-delà du seul scalaire `impact_7d` — **sans** exploser le contexte, le coût ni la latence.

---

## 1. Contrainte centrale & ce qui la résout

Le brief AG1 V4 **n'envoie pas** les 385 symboles : `calcul_matrice_briefing` produit un `opportunity_pack` **plafonné** = 10 Entrer + 4 Surveiller + 6 Réduire/Sortir ≈ **~20 symboles**, classés par le scoring (qui intègre déjà `Symbol_News_Impact_7d`). Le volume news est donc borné **par construction** : on n'enrichit que le pack + les positions détenues.

On **conserve** le canal quantitatif existant (`impact_7d` dans le scoring, corrigé par D1) et on **ajoute** un canal qualitatif compact (le digest) — pas de doublon de volume.

## 2. Paramètres validés (Nicolas, 2026-06-18)

- **Richesse** : Compact — **top 3 news/symbole**, 1 ligne chacune.
- **Périmètre** : **pack (~20) ∪ positions détenues** (toute position ouverte a ses news, même hors pack).
- **Fraîcheur** : **14 jours**.

## 3. Contrat de données

Source : vue `news_analyzed` de `ag4_spe_v2.duckdb` (= `summary IS NOT NULL AND is_relevant` ; contient Boursorama **et** IBKR).

Par symbole, `news` = liste de **≤3** objets :
```
{ date: "YYYY-MM-DD", source: "ibkr|boursorama", provider: "Reuters News|…",
  signal: "BUY|SELL|WATCH|NEUTRAL", impact: <int -10..10>, title: "<≤90 char>" }
```
- **Tri** : récence × |impact| (catalyseur récent et fort d'abord) sur la fenêtre 14 j.
- **Dédup** : par titre normalisé (lowercase, ~40 premiers car.) au sein du symbole ; en cas de doublon Boursorama/IBKR, **garder IBKR** (Reuters/DJ > scrape).
- **Recency** : `COALESCE(published_at plausible, first_seen_at)` (même logique que D1) ≥ now()-14j.

Rendu dans le brief (1 ligne/news, ultra-compact) :
```
LR.PA — news (3):
  2026-06-18 | ibkr/Reuters | WATCH | +3 | Legrand rises as data center concerns ease
  2026-06-17 | boursorama   | BUY   | +5 | Legrand relève ses objectifs annuels
  2026-06-12 | ibkr/DJ-GL   | WATCH |  0 | French stocks flat in morning trading
```

## 4. Budget (tient les 3 contraintes)

- **Contexte** : ~25 symboles × 3 news × ~30 tokens ≈ **+2 000 tokens** sur un brief déjà à plusieurs milliers. Marginal.
- **Coût** : +2k tokens × 3 modèles × 3 runs/j → ordre du centime/jour.
- **Perf** : **1 requête DuckDB** (sur ~25 symboles), pré-agrégée hors LLM. **0 appel LLM ajouté**, 0 latence LLM.

## 5. Implémentation (nodes AG1 V4)

1. **Nouveau node `20K - News Digest (Pack+Held)`** (Python/DuckDB), placé **après** `calcul_matrice_briefing` (quand les ~20 symboles du pack sont connus) et avant l'assemblage `ag1_00_assemble_input_packs` :
   - Entrées : `opportunity_pack.rows` (symboles) + positions détenues (`portfolio_positions_mtm_latest`).
   - Requête `news_analyzed` (top 3/symbole, 14 j, tri récence×|impact|, dédup, préférence IBKR).
   - Sortie : attache `news: [...]` à chaque `pack_row` + une section `held_news` pour les détenus hors pack.
2. **`ag1_00_assemble_input_packs.code.js`** : inclure le bloc `news` par symbole dans le texte du brief (format §3) + la section détenus.
3. **System prompt PM** (agent_input) : ajouter 1-2 phrases — *« Le bloc news par symbole donne les catalyseurs récents (signal/impact/source). Une news récente à fort impact peut justifier d'ajuster la conviction, la taille ou la sortie ; privilégier les sources premium (Reuters/Dow Jones). Ne pas sur-réagir à une news isolée à impact faible. »*
4. **Garde-fous volume** : caps en dur (3 news, 90 car. titre, 14 j) + exclusion des `impact=0/Noise` au-delà de la 1ʳᵉ ligne si plus de 3 candidats.

## 6. Déploiement & validation

- **AG1 V4 = trading LIVE.** Donc : implémenter en repo → **valider en shadow/replay** (vérifier le brief généré : volume tokens réel, format, pertinence) → diff chirurgical (node ajouté + assemble + prompt) → import/publish/restart → surveiller 1-2 runs.
- Mesurer le **delta tokens réel** du brief avant/après (objectif < +2,5k) avant de publier.
- Rollback : retirer le node `20K` du flux (le scalaire `impact_7d` reste, comportement V4 actuel inchangé).

## 7. Hors scope (à part)
- Pondération fine du scoring sur le signal qualitatif (pour l'instant le qualitatif est pour le **raisonnement** du PM ; le quantitatif reste `impact_7d`).
- Dédup inter-sources avancée (sémantique) — on s'en tient au titre normalisé + préférence IBKR.
