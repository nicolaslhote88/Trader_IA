# Trader IA — Design System

## Overview

**Trader_IA** is an AI-assisted trading platform built and operated by Nicolas Lhote. It runs as a fully automated pipeline on a Hostinger VPS, orchestrated via n8n. The system combines an ensemble of three LLM-based Portfolio Managers (GPT-5.2, Grok-4.1 Reasoning, Gemini-3.0 Pro) with specialized analyst agents (technical, fundamental, sentiment/news), a deterministic Risk Manager, and an Execution Trader (currently in sandbox mode, live broker integration in progress).

The sole user-facing product is a **Streamlit dashboard** called "AI Trading Executor" / "TradingSim AI". There is no public website, mobile app, or marketing surface — this is an internal operational tool.

### Sources
- **Codebase:** https://github.com/nicolaslhote88/Trader_IA (branch: `main`)
  - Primary UI source: `services/dashboard/app.py` (13,457 lines)
  - Modules: `services/dashboard/app_modules/core.py`, `visualizations.py`, `tables.py`
  - Architecture docs: `ANALYSE_SYSTEME_AVANT_AGENT6.md`, `docs/architecture/`
- No Figma file provided.
- No brand assets (logos, illustrations) found in the repository.

---

## Products

| Product | Description | Status |
|---|---|---|
| **Streamlit Dashboard** | Operational monitoring & decision support for the AI trading pipeline | Live (sandbox mode) |
| **n8n Workflows** | Internal agent orchestration — not user-facing | Live |
| **yfinance-api** | Internal microservice — not user-facing | Live |

The design system focuses on the Streamlit dashboard only.

---

## Dashboard Pages

The sidebar navigation exposes 6 pages:
1. **Dashboard Trading** — Portfolio overview, allocations, PnL, sparklines
2. **System Health (Monitoring)** — Pipeline status, DuckDB freshness, agent run logs
3. **Vue consolidée Multi-Agents** — Cross-model consensus view (GPT / Grok / Gemini)
4. **Analyse Technique V2** — AG2 signals: D1/H1 decisions, indicator bars, top BUY/SELL
5. **Analyse Fondamentale V2** — AG3 signals: valuation scenarios, earnings gates
6. **Macro & News (AG4)** — Macro barometer, sector sentiment, symbol momentum

---

## CONTENT FUNDAMENTALS

### Language
- Written entirely in **French**. All UI labels, headings, tooltips, status messages, and navigation items use French.
- Technical field names remain English (e.g. `symbol`, `BUY`, `SELL`, `APPROVE`, `REJECT`).

### Tone
- **Operational and direct.** No marketing fluff. No first-person narrative.
- Copy reads like a Bloomberg terminal or quantitative analyst dashboard — dense, precise, abbreviation-heavy.
- Examples from the codebase:
  - "Pas de données d'articles sur la fenêtre."
  - "Aucun historique de runs disponible."
  - "Data unavailable"
  - "Donnees Google Sheets indisponibles. Les vues basees DuckDB restent disponibles."
- Section headers use title case in French (e.g. "Vue consolidée Multi-Agents", "Analyse Technique V2").

### Casing
- Page/tab titles: Title Case in French (e.g. "Analyse Technique V2")
- Field labels: lowercase French (e.g. "prob. win", "seuil risque p60")
- Status badges: ALLCAPS English (BUY, SELL, APPROVE, REJECT, WATCH, SKIP, NEUTRAL)
- Agent names: proper nouns — ChatGPT 5.2, Grok 4.1 Reasoning, Gemini 3.0 Pro

### Emoji / Icons
- Emoji used as **data freshness indicators only** — never decorative:
  - ✅ fresh data (within threshold)
  - ⚠️ aging data (within 3× threshold)
  - 🛑 stale data (beyond 3× threshold)
  - ❌ missing / null
  - ❓ unparseable date
- No icon font or SVG icon system. No decorative emoji anywhere.

---

## VISUAL FOUNDATIONS

### Color System
The entire product is **dark-mode only**. There is no light mode.

**Backgrounds (dark hierarchy):**
- App background: `#0e1117` (Streamlit default)
- Surface / card: `#1e1e2e`
- Table header: `#262730`
- Subtle separator: `#333` / `#444`

**Signal colors (semantic):**
- Buy / Success / Approve: `#28a745` (green)
- Sell / Danger / Reject: `#dc3545` (red)
- Watch / Warning: `#fd7e14` (orange)
- Running / Action: `#0d6efd` (blue)
- Neutral / Muted: `#6c757d` (gray)

**Text:**
- Primary: `#ffffff`
- Secondary: `#cccccc`
- Tertiary / muted: `#9aa0a6`
- Disabled / range labels: `#666666`

**Agent accent colors:**
- GPT-5.2: `#10b981` (emerald)
- Grok-4.1 Reasoning: `#f59e0b` (amber)
- Gemini-3.0 Pro: `#60a5fa` (sky blue)

**Table row tints (8% opacity overlays):**
- Confluence BUY: `rgba(34, 197, 94, 0.08)`
- Confluence SELL: `rgba(239, 68, 68, 0.08)`
- Divergence: `rgba(245, 158, 11, 0.08)`
- Reject + high D1: `rgba(251, 191, 36, 0.10)`

**Badge cell background/text pairs:**
- BUY: bg `#14532d`, text `#dcfce7`
- SELL: bg `#7f1d1d`, text `#fee2e2`
- NEUTRAL: bg `#374151`, text `#e5e7eb`
- APPROVE: bg `#14532d`, text `#dcfce7`
- REJECT: bg `#7c2d12`, text `#ffedd5`
- WATCH: bg `#78350f`, text `#fef3c7`
- SKIP: bg `#1f2937`, text `#d1d5db`

**Chart color:**
- Line: `#4ea1ff`
- Positive fill: `rgba(40, 167, 69, 0.18)`
- Negative fill: `rgba(220, 53, 69, 0.16)`

### Typography
- **Font family:** System sans-serif (no custom font loaded). Streamlit uses its default stack.
- **Base size:** 0.9rem for tables and secondary content; 0.8rem for badges; 0.85rem for labels
- Font weight: 600 for labels, 700 for badges/values
- No display or serif type. No monospace (no terminal/code blocks in the UI).

### Layout
- **Full-width layout** (`st.layout="wide"`) — content stretches edge to edge.
- Multi-column grids via `st.columns()`.
- Heavy use of nested `st.tabs()` for content organization.
- Sidebar navigation with `st.sidebar.radio()`.
- No fixed headers or footers. No sticky elements.
- Dense information layout — minimal whitespace. Bloomberg-terminal energy.

### Cards
- Background: `#1e1e2e`
- Border: `1px solid #333`
- Border radius: `8px`
- Padding: `16px`
- No shadow. No hover state.

### Badges
- Border radius: `4px` (inline badges) or `999px` (pill-style severity levels)
- Padding: `2–3px 6–10px`
- Font weight: bold (700)
- Always white text on colored background
- No icon glyphs inside badges

### Borders & Separators
- Table cell borders: `1px solid #333`
- Table header bottom: `2px solid #444`
- Card border: `1px solid #333`
- No shadows on any element

### Progress / Indicator Bars
- Track: `#222`, `border-radius: 5px`, height: `10px`
- Fill segments colored by zone (green/yellow/red)
- Value cursor: 3px white bar
- Range labels: `#666`, `0.7em`

### Plotly Charts
- Background: transparent (`rgba(0,0,0,0)`)
- No grid lines, no tick labels on sparklines
- Hover mode: `x unified`
- Height: 190px for sparklines

### Animations
- None. No transitions, no hover animations, no entrance effects.

### Hover / Interaction States
- No custom hover states beyond Streamlit defaults.
- Buttons use Bootstrap-style colors (`#0d6efd` primary, `#198754` success).

### Imagery
- No photography, illustrations, or decorative graphics of any kind.
- Plotly charts and tables are the only visual elements beyond text and badges.

---

## ICONOGRAPHY

No icon system is used in this product. Key findings:
- No icon font (no Font Awesome, Material Icons, etc.)
- No SVG icon sprites
- No image-based icons
- Emoji used **only** as data-status indicators (see Content Fundamentals above)
- No logos found in the repository

The `assets/` folder is populated with placeholder files for future use.

---

## File Index

```
/
├── README.md                          ← This file
├── SKILL.md                           ← Agent skill definition
├── colors_and_type.css                ← CSS variables: colors, typography, spacing
├── assets/                            ← Brand assets (logos, icons — none in repo)
│   └── README.md                      ← Notes on missing assets
├── preview/                           ← Design System card previews
│   ├── colors-base.html               ← Dark background + surface palette
│   ├── colors-signal.html             ← Signal / semantic colors
│   ├── colors-agents.html             ← Per-agent accent colors
│   ├── colors-badges.html             ← Badge color pairs
│   ├── type-scale.html                ← Typography scale
│   ├── spacing-tokens.html            ← Spacing, radius, border tokens
│   ├── components-badges.html         ← Badge component states
│   ├── components-cards.html          ← Card component
│   ├── components-indicator-bars.html ← Indicator progress bars
│   └── components-table.html          ← Table styles
└── ui_kits/
    └── dashboard/
        ├── README.md                  ← Dashboard UI kit notes
        └── index.html                 ← Interactive dashboard prototype
```
