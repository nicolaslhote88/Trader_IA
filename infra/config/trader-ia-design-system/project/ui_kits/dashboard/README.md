# Dashboard UI Kit — Trader IA

## Overview
High-fidelity recreation of the "AI Trading Executor" Streamlit dashboard as a clickable HTML prototype.

## Source
- `services/dashboard/app.py` (13,457 lines)
- `services/dashboard/app_modules/`

## Design Notes
- Dark-mode only (`#0e1117` app bg, `#1e1e2e` surfaces)
- System sans-serif typography
- Streamlit's default wide layout — full-width, multi-column grids
- Dense data-heavy UI: tables, sparklines, badge indicators, metric cards, indicator bars
- No custom branding or logos in the source

## Pages Recreated
1. **Dashboard Trading** — portfolio metrics, allocation chart placeholder, sparkline grid, positions table
2. **Analyse Technique V2** — symbol matrix with D1/H1 signals, indicator bars, top BUY/SELL
3. **Vue consolidée Multi-Agents** — cross-model consensus view (GPT / Grok / Gemini)
4. **Macro & News** — sector barometer, impact badges, news feed

## Components
- `Sidebar` — navigation radio, title
- `MetricCard` — KPI tile with value, delta, sub-label
- `BadgeChip` — signal badge (inline and cell variants)
- `IndicatorBar` — technical indicator progress bar with colored zones
- `DataTable` — styled table with row tints and badge cells
- `SparklineCard` — mini price chart placeholder
- `AgentTab` — per-model panel with accent color
