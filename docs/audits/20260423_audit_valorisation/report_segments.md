# Audit de performance par segment de marché — 3 IA trading

**Date d'analyse** : 23/04/2026
**Snapshot** : 22/04/2026 (dernier run 21/04)
**Période de référence** : V2 pure (22/02 → 21/04/2026, ~2 mois)
**Question posée** : l'une des 3 IA est-elle plus efficace sur le Forex ou les Actions ?

---

## TL;DR

| Segment       | Meilleure IA | Pire IA   | Verdict |
|---------------|--------------|-----------|---------|
| **Actions EU** | Gemini (+2 615 €, wr 62 %) — ChatGPT juste derrière (+2 125 €, wr 66 %) | **Grok** (−669 €, wr 39 %) | Gemini et ChatGPT sont comparables, Grok sous-performe nettement |
| **Actions US** | Aucune      | Toutes    | Échantillons non significatifs (2 à 13 trades), aucun edge identifié |
| **Forex**     | Gemini (seule active) | — | Activité anecdotique sur les 3 ; pas de preuve de rentabilité |

**Conclusion opérationnelle** :

1. **Gemini est la seule IA qui a diversifié son périmètre** (EU + US + FX). Son P&L V2 total est négatif (−306 €) mais c'est **entièrement à cause des Actions US et du Forex** — sur les Actions EU elle est la plus rentable (+2 615 €).
2. **Aucune IA n'a prouvé d'edge sur le Forex**. Gemini fait 75 % de winrate mais sur 4 trades — statistiquement non concluant, et les montants sont trop petits pour bouger l'aiguille.
3. **Grok a un problème spécifique sur les Actions EU** : malgré des positions courantes très performantes (+3 684 € unrealized, +7,8 %), son winrate historique n'est que de 39 %. C'est une inadéquation entre sélection (bonne) et timing de clôture (mauvais) — ou du sur-trading.

---

## 1. Périmètre par IA

Distribution du périmètre d'activité sur les 2 mois V2 :

| IA       | Symboles EU | Symboles US     | Paires FX | Focus                    |
|----------|------------:|:----------------|:----------|:-------------------------|
| ChatGPT  | 35 tickers  | 2 (V, NVDA)     | 1 (CHFCAD) | **Quasi-exclusivement Euronext Paris** |
| Gemini   | 56 tickers  | 8 (AMZN, GOOGL, LLY, META, MSFT, NVDA, V, XOM) | **13 paires** | **La seule multi-segments** |
| Grok     | 54 tickers  | 1 (AAPL)        | 1 (USDMXN) | **Quasi-exclusivement Euronext Paris** |

ChatGPT et Grok ont essentiellement tradé Paris. Seule Gemini a fait une vraie allocation transversale.

---

## 2. Segment Actions EU (performance V2 pure)

C'est le segment dominant en volume pour les 3 IA.

| IA       | n trades fermés | Wins | Losses | Win rate | P&L réalisé V2 | Avg / trade | Best    | Worst  |
|----------|----------------:|-----:|-------:|---------:|---------------:|------------:|--------:|-------:|
| Gemini   | 59              | 37   | 22     | **62 %** | **+2 615 €**   | +44 €       | +756 €  | −267 € |
| ChatGPT  | 29              | 20   | 9      | **66 %** | +2 125 €       | +55 €       | +1 236 €| −483 € |
| Grok     | 73              | 29   | 44     | **39 %** | **−669 €**     | −12 €       | +789 €  | −439 € |

### Positions encore ouvertes (upnl au 21/04)

| IA       | n positions | Cost basis | MV      | Unrealized  | % sur cost |
|----------|------------:|-----------:|--------:|------------:|-----------:|
| ChatGPT  | 17          | 41 924 €   | 44 114 €| **+2 190 €**| **+5,22 %**|
| Gemini   | 14          | 34 180 €   | 35 537 €| +1 357 €    | +3,97 %    |
| Grok     | 19          | 47 159 €   | 50 842 €| **+3 684 €**| **+7,81 %**|

### Ce qu'on lit

- **Gemini** : la plus productrice en réalisé (+2 615 €) avec 59 trades fermés, win rate sain à 62 %. Son portefeuille courant est modeste (+3,97 % unrealized) mais la stratégie globale est solide.
- **ChatGPT** : le meilleur win rate (66 %), moins actif (29 trades fermés), positions courantes en gain (+5,22 %). Style "moins mais mieux" — durée moyenne de détention **33,8 jours**, le plus long des 3.
- **Grok** : **anomalie frappante** — le meilleur portefeuille courant (+7,81 % unrealized) coexiste avec le pire historique réalisé (wr 39 %, −669 €). Durée moyenne de détention 15,1 j (2× plus court que ChatGPT). Hypothèses :
  - Sur-trading : Grok ferme trop tôt ses gagnants et laisse courir ses perdants (classique "coupe les fleurs, arrose les mauvaises herbes").
  - Ses choix de sélection sont bons (en témoigne le portefeuille courant), mais sa discipline de sortie est mauvaise.

### Durée moyenne de détention (lots fermés V2)

| IA       | avg (j) | min | max |
|----------|--------:|----:|----:|
| ChatGPT  | 33,8    | 0   | 76  |
| Gemini   | 21,5    | 0   | 85  |
| Grok     | 15,1    | 0   | 56  |

ChatGPT = style swing long. Grok = style swing court. Gemini au milieu.

---

## 3. Segment Actions US (faible volume)

Gemini est la seule à avoir un échantillon statistique. Les 2 autres ont essayé et arrêté.

| IA       | n fermés V2 | Wins | Losses | Win rate | P&L réalisé V2 | Positions ouvertes |
|----------|------------:|-----:|-------:|---------:|---------------:|:-------------------|
| Gemini   | 13          | 4    | 9      | 31 %     | **−435 €**     | 4 (AMZN, GOOGL, LLY, MSFT) upnl **−713 €** |
| ChatGPT  | 3           | 0    | 3      | **0 %**  | −241 €         | 0                   |
| Grok     | 2           | 2    | 0      | 100 %    | **+1 644 €**   | 0                   |

### Le "100 % wr de Grok" est du bruit

Les 2 seuls trades US de Grok sont deux allers-retours sur AAPL :

```
AAPL  open 11/03  close 13/03  qty=15  px=165.50  pnl=+1 374 €  (2 j)
AAPL  open 10/04  close 13/04  qty=3   px=170.50  pnl=  +270 €  (3 j)
```

Une IA qui touche AAPL deux fois et gagne les deux ne "prouve" rien — elle a saisi un rebond sur 2 jours de mars et un mini-pump en avril. Ce n'est pas une stratégie US reproductible.

### Ce qu'on lit

- **Gemini** est la seule à avoir un vrai pattern US (13 trades, 31 % wr, perte nette) → **son allocation US est clairement non rentable**. Recommandation : arrêter les US tant qu'aucun edge n'est identifié, ou contraindre à un sous-ensemble (GAFAM liquides uniquement).
- **ChatGPT** a fait 3 trades US, tous perdants, et a arrêté. Discipline correcte.
- **Grok** n'a pas de stratégie US à proprement parler.

---

## 4. Segment Forex

| IA       | Paires tradées | n fermés V2 | Wins | Losses | Win rate | P&L réalisé V2 | Positions ouvertes |
|----------|---------------:|------------:|-----:|-------:|---------:|---------------:|:-------------------|
| Gemini   | 13             | 4           | 3    | 1      | **75 %** | **−39 €**      | 2 (GBPUSD, AUDEUR) upnl +13 € |
| ChatGPT  | 1 (CHFCAD)     | 0           | —    | —      | —        | 0 €            | 1 (CHFCAD) upnl +6 € |
| Grok     | 1 (USDMXN, clôture V1 uniquement) | 0 | — | — | — | 0 € | 0 |

### Le détail Gemini (seule active)

```
FX:CADNZD   CLOSED  qty= 1 218   pnl= +22 €   durée 22 j
FX:USDEUR   CLOSED  qty=     9   pnl=  +0 €   durée  2 j
FX:JPYNZD   CLOSED  qty=86 889   pnl= −67 €   durée  0 j  (intraday perdant)
FX:GBPJPY   CLOSED  qty=     7   pnl=  +6 €   durée  1 j
FX:GBPUSD   OPEN    qty=   676   upnl  +0 €
FX:AUDEUR   OPEN    qty= 1 277   upnl  +0 €
```

### Ce qu'on lit

- **Le "75 % winrate" est sur 4 trades, dont le seul perdant pèse −67 € et les 3 gagnants pèsent +28 € au total → net négatif**. Asymétrie perte/gain très défavorable.
- Montants très faibles (MV totale FX Gemini ≈ 3,9 k€, soit 7,5 % du capital) → même un bon edge FX ne bougerait pas significativement le P&L total.
- Les 15 paires tradées sur 2 mois sont trop dispersées : aucune paire n'a plus de 2 trades → **pas de temps d'apprentissage / ajustement par paire**.
- ChatGPT a une position FX expérimentale (CHFCAD) ouverte mais jamais fermée depuis 2 mois : aucun signal.
- Grok n'a jamais ouvert de position FX en V2 (les 2 ventes USDMXN sont des clôtures d'un lot V1 importé).

**Conclusion segment FX** : aucune IA n'a démontré un edge sur le Forex. Gemini essaie mais se disperse. Le segment n'est pas prêt pour le live.

---

## 5. Synthèse comparée — qui fait quoi bien ?

```
         ┌────────────┬────────────┬────────────┐
         │ Actions EU │ Actions US │   Forex    │
┌────────┼────────────┼────────────┼────────────┤
│ ChatGPT│   🟢 +2,1k │    🔴 0/3  │ — (1 pos.) │
│ Gemini │   🟢 +2,6k │  🟠 −435 € │ 🟠 −39/4tr │
│ Grok   │   🔴 −0,7k │  ⚪ 2 tr.  │   — (nul)  │
└────────┴────────────┴────────────┴────────────┘
   P&L réalisé V2 pur, légende :
   🟢 positif & significatif     🟠 actif mais perdant
   🔴 négatif ou échec           ⚪ échantillon trop petit
```

### Classement par segment

- **Actions EU** : 1. Gemini (+2 615 €, wr 62 %) · 2. ChatGPT (+2 125 €, wr 66 %) · 3. Grok (−669 €, wr 39 %)
- **Actions US** : rien d'exploitable. Gemini perd, les autres n'en font pas.
- **Forex** : rien d'exploitable. Gemini essaie, les autres regardent.

### Pourquoi Gemini a un P&L V2 négatif (−306 €) alors qu'elle est la meilleure en actions EU

Décomposition du P&L réalisé V2 de Gemini :

```
  Actions EU  : +2 615 €
  Actions US  :   −435 €
  Forex       :    −39 €
  ───────────────────
  Total réalisé V2   : +2 141 €
  + Unrealized courant : +656 € (dont EU +1 357, US −713, FX +13)
  − "consommation" cash (frais/écarts non retracés) : …
  ───────────────────
  Net V2 apparent : −306 €
```

Il y a un écart non retracé entre le réalisé des lots (+2 141 €) et le Δ P&L V2 (−306 €) ≈ 2 450 €. Cela renforce la conclusion du rapport précédent : **`position_lots` n'est pas fiable comme compteur de réalisé** (divergence de −2 344 € vs la balance cash sur Gemini). Tant que ce compteur n'est pas fiabilisé, les chiffres de P&L par segment ci-dessus sont à interpréter en **relatif entre IA**, pas en absolu.

---

## 6. Recommandations

| # | Reco                                                                                 | Impact |
|---|---------------------------------------------------------------------------------------|--------|
| 1 | **Concentrer le périmètre agent 6 (broker live) sur les Actions EU**, là où ChatGPT et Gemini ont un edge mesurable | Fort |
| 2 | **Désactiver les Actions US pour Gemini** tant qu'aucune stratégie dédiée n'est validée (stop-loss au niveau du segment) | Fort |
| 3 | **Geler le périmètre Forex** pour toutes les IA : trop peu de trades, trop dispersé, trop petits montants pour conclure | Moyen |
| 4 | **Investiguer le cas Grok × Actions EU** : pourquoi un excellent stock-picker (upnl +7,8 %) a-t-il un wr historique de 39 % ? Hypothèse à tester : il ferme trop tôt. Comparer avg winner size vs avg loser size. | Moyen |
| 5 | Ajouter sur le dashboard une **décomposition P&L par segment × IA** (ratios affichés côte à côte) | Fort lecture |
| 6 | Reprendre l'expérimentation FX avec un **périmètre restreint** (2-3 paires majeures) et un nombre minimal de trades (20+) avant de conclure | Long terme |

---

## 7. Classement global pour passer en live (ordre proposé)

1. **ChatGPT** sur Actions EU uniquement — wr 66 %, +5,2 % unrealized actuel, style swing long cohérent (34 j en moyenne). Profil le plus stable.
2. **Gemini** sur Actions EU uniquement — wr 62 %, meilleur P&L réalisé absolu. Doit couper les US et le FX.
3. **Grok** à maintenir en sandbox — bonne sélection de titres mais discipline de clôture à corriger avant d'exposer du capital réel.

---

*Scripts d'analyse : `/infra/maintenance/audit_valorisation_20260423/audit_segments.py`*
