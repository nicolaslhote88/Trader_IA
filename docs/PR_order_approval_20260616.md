# PR — Système d'approbation Telegram des ordres + price-guard + docs (2026-06-16)

Branche déjà créée : **`claude/order-approval-system-20260616`** (tu es dessus).

## Titre de PR
`AG1 V4 — Approbation Telegram des ordres hors-bande + price-guard 5% + mise à jour doc`

## Description (corps de PR)
Déployé en direct sur le VPS le 2026-06-16, ce commit met le repo en phase.

**Broker IBKR (`services/ibkr-broker/`)**
- `approval.py` (nouveau) : approbation humaine des ordres dont la déviation prix est dans la bande
  5 %–15 %. Store PENDING en mémoire, token à usage unique, TTL, notif webhook. Flag-gated par
  `IBKR_APPROVAL_ENABLED`.
- `app.py` : `import approval` + hook dans `place_equity_orders` (parcage + `needs_approval` au lieu du
  rejet) + 3 endpoints `GET /orders/approvals/pending`, `POST .../{id}/approve|reject`.

**Comportement (price-guard, env sur le VPS)**
- ≤ 5 % → auto-confirmé (`IBKR_PRICE_GUARD_MAX_DEVIATION_PCT` relevé 3→5, âge quote 8 h→1 h).
- 5 %–15 % → notif Telegram (@CYROLAS_BOT → groupe) + boutons Approuver/Rejeter.
- > 15 % → rejeté.

**Workflows n8n** : `AG1 V4 — Order Approval Request` + `… Decide` (créés/actifs sur le VPS ; à exporter
dans le repo séparément si tu veux les versionner).

**Documentation**
- `AGENTS.md` : état projet à jour (GPT-5.5 / Grok 4.3 / Claude Sonnet 4.6, système d'approbation,
  deux stacks compose, bug writer ouvert).
- `README.md`, `docs/operations/env_vars.md`, `docs/operations/runbook_n8n_investigation.md` : maj.
- `docs/operations/order_approval_deploy_notes.md` (nouveau) : déploiement + activation + rollback.
- `docs/audits/20260615_ag1_v4_prompt_audit.md` (nouveau) : audit du brief LLM AG1 V4.
- `docs/specs/ag1_v4_order_approval_notification_v1.md` (nouveau) : spec du système d'approbation.
- `scripts/verify_vps_n8n.sh` (nouveau) : vérif VPS lecture seule.
- `.gitignore` : exclusion de `.ssh/` (clé VPS locale).

**Issue connexe non résolue ici** : `core.runs.strategy_version`/`prompt_version`/`n8n_execution_id`
restent NULL (mapping writer `08`/`09` — données présentes en amont). À traiter dans une PR dédiée.

## Commandes (PowerShell, sur ton poste)
```powershell
cd "D:\N8N\Assistant IA complet\Trader_IA"
if (Test-Path .git\index.lock) { del .git\index.lock }   # lock résiduel de mon fetch

# tu es déjà sur la branche claude/order-approval-system-20260616
git add AGENTS.md README.md .gitignore `
  "services/ibkr-broker/approval.py" "services/ibkr-broker/app.py" `
  "docs/operations/order_approval_deploy_notes.md" `
  "docs/operations/env_vars.md" `
  "docs/operations/runbook_n8n_investigation.md" `
  "docs/audits/20260615_ag1_v4_prompt_audit.md" `
  "docs/specs/ag1_v4_order_approval_notification_v1.md" `
  "docs/PR_order_approval_20260616.md" `
  "scripts/verify_vps_n8n.sh"

git diff --cached --stat        # VÉRIFIE que seuls ces fichiers sont stagés
git commit -m "AG1 V4: approbation Telegram des ordres hors-bande + price-guard 5% + docs"
git push -u origin claude/order-approval-system-20260616

# PR via gh (ou ouvre l'URL affichée par GitHub) :
gh pr create --base main --head claude/order-approval-system-20260616 `
  --title "AG1 V4 — Approbation Telegram des ordres hors-bande + price-guard 5% + doc" `
  --body-file "docs/PR_order_approval_20260616.md"
```

## ⚠️ Important
- Le working tree contient **beaucoup d'autres modifs non-commitées** (travail Codex : AG4-V3,
  AG1-PF-V1, AG1-V3…). Le `git add` ci-dessus est **sélectif** pour ne committer QUE cette PR.
  Vérifie bien `git diff --cached --stat` avant de commit.
- **Ne pas inclure** `AG1_workflow_template_v4.json` (resync GPT 5.5) dans cette PR : il est traité à part.
- La source broker sur le VPS (`/opt/trader-ia/...`) est **hors git** : ce repo fait foi.
