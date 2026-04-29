# Trader IA Design System Handoff

This directory contains the Claude Design handoff package for future Trader IA dashboard evolution.

## Source Package

- Archive: `infra/config/Trader IA Design System-handoff.zip`
- Extracted bundle: `infra/config/trader-ia-design-system/`

Keep both:

- the `.zip` is the immutable original handoff artifact
- the extracted folder is the searchable, reviewable source used by coding agents and developers

## What To Read First

1. `infra/config/trader-ia-design-system/README.md`
2. `infra/config/trader-ia-design-system/project/SKILL.md`
3. `infra/config/trader-ia-design-system/project/README.md`
4. `infra/config/trader-ia-design-system/project/colors_and_type.css`
5. `infra/config/trader-ia-design-system/project/ui_kits/dashboard/index.html`

## Production Source Of Truth

The live dashboard remains:

- `services/dashboard/app.py`
- `services/dashboard/app_modules/`

The extracted file below is a design-reference snapshot only:

- `infra/config/trader-ia-design-system/project/services/dashboard/app.py`

Do not replace the production dashboard with that snapshot. Use it to understand the design handoff context, then apply changes intentionally to the live Streamlit files.

## Reuse Guidance

For future dashboard work:

- Use `colors_and_type.css` as the token reference for colors, spacing, radius, typography, and badges.
- Use `project/preview/*.html` for focused examples of palettes and components.
- Use `project/ui_kits/dashboard/index.html` as the high-fidelity dashboard prototype reference.
- Preserve the operational Streamlit style: dense, dark-mode only, French labels, no marketing layout, no decorative imagery.
- Keep agent accents consistent:
  - GPT: `#10b981`
  - Grok: `#f59e0b`
  - Gemini: `#60a5fa`

## Integrity

`infra/config/trader-ia-design-system/MANIFEST.sha256` records hashes for the extracted files so later changes to the handoff material are visible in Git review.
