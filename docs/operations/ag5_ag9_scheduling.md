# Scheduling AG5–AG9

Fuseau de tous les crons n8n : `Europe/Paris`.

| Workflow | Cron | Écriture | Justification |
|---|---|---|---|
| AG5 Macro | `20 7 * * 1-5` | `macro_data` | après enrichissement du matin |
| AG6 FX valuation | `40 7 * * 1-5` | `macro_data` | après AG5 |
| AG7 Positioning | `0 8 * * 1-5` | `macro_data` | COT hebdomadaire, check quotidien |
| AG8 Rates/Liquidity | `20 8 * * 1-5` | `macro_data` | sérialise le writer macro |
| AG9 Global Risk | `35 9,12,15 * * 1-5` | `worldmonitor_v1` | avant les synthèses; hors AG1 |
| Global Context | `5 10,13,16 * * 1-5` | `global_context_v1` | 30 min après AG9, avant AG1 14:00/16:30 |

Les quatre composants macro partagent un writer unique et sont espacés de 20
minutes. AG9 et la synthèse écrivent des bases distinctes. La synthèse lit les
bases sources après la fin attendue de leurs writers et laisse 55/25 minutes
avant AG1. Le run 16:05 se termine avant AG1 16:30. Aucun cron Forex ou ordre
n'est créé.

Ces horaires sont des candidats non actifs tant que les durées VPS shadow ne
sont pas mesurées. Si un p95 dépasse la marge, déplacer le producteur — jamais
AG1 — après consultation de `SCHEDULING_AND_LOAD.md`. Publier d'abord les
producteurs, observer plusieurs cycles, puis seulement le shadow AG1.
