# Expansion univers — 100 valeurs mondiales + classification segments

Date : 2026-06-24. Base : `ag2_v3.duckdb` (`/local-files/duckdb/` = `/files/duckdb/`).
Auteur de l'opération : session assistée (déploiement direct VPS).

## Objectif

Diversifier l'univers (très concentré France) avec 100 large/méga-caps mondiales,
puis les rendre **visibles par le système** via la classification `universe_segments`.

## Taxonomie de classification (RÈGLE À CONNAÎTRE)

Un symbole de `universe` n'est **pas** automatiquement traité par tous les agents. Deux couches :

1. **`universe_quarantine`** (`active=TRUE` exclut des rotations non détenues).
2. **`universe_segments`** (PK `(symbol, segment)`), segments :
   - `HELD` — position détenue (auto, lu depuis `ag1_v4_consensus.portfolio_positions_mtm_latest`), priorité 1000.
   - `CORE_AUTO` — top 50 par score composite **dominé par le volume** (jusqu'à 40 pts) + fondamental/qualité/santé/analystes − risque. Exige `quote_ok` + `volume ≥ 20000` (données YF). Recalculé à chaque refresh.
   - `CORE_MANUAL` — épinglage **manuel** (`source='manual'`). **Préservé** des refresh (le `DELETE` ne vise que `source='auto'`). Force la rotation prioritaire.
   - `WATCHLIST` — disponible, ni détenu ni core. État par défaut d'une entrée neuve.

### Qui lit quoi (visibilité réelle)
- **yf-enrichment** : lit **tout `universe`** (pas de filtre segment) → toute nouvelle entrée reçoit prix/volume.
- **AG1 V4 (node R8)** : lit **tout `universe`** sauf quarantaine active → la voit, avec flags `MISSING_TECH/YF/FUNDA` tant qu'aucune donnée.
- **AG2 (technique) & AG3 (fondamental)** : **rotation pilotée par `universe_segments`** :
  - workflows `…Held+Core` ⇒ traitent `HELD` + `CORE_AUTO` + `CORE_MANUAL` ;
  - workflows `…Watchlist Nightly` ⇒ traitent `WATCHLIST`.
  - **Un symbole absent de `universe_segments` n'entre dans aucune rotation AG2/AG3** ⇒ jamais enrichi technique/fondamental.

### Conséquence opérationnelle
Le refresh `AG2UHQ` (jours ouvrés 18:35 UTC) **reclasse automatiquement** toute nouvelle entrée `universe` en `WATCHLIST` au prochain run. Mais d'ici là, et pour tout pin `CORE`, il faut classer **manuellement**. Promotion `CORE_AUTO` automatique une fois le volume YF collecté (méga-caps liquides remontent seules, plafond 50 ⇒ déplacent des mid-caps).

## Ce qui a été déployé (2026-06-24)

1. `universe` : **463 → 563** (+100). Backup : `/local-files/duckdb/ag2_v3.duckdb.bak_20260624_preseed`.
   - Répartition : US 30, Europe hors-FR 25, Asie dév. 23, Émergents 14, Canada 8.
   - `symbol == symbol_yahoo`, `sector` = labels Yahoo, ADR/US privilégiés (mix pragmatique).
2. `universe_segments` : **+100** → 18 `CORE_MANUAL` (piliers) + 82 `WATCHLIST`. CORE_AUTO 50 / HELD 5 inchangés.
   - CORE_MANUAL épinglés : ASML, TSM, 005930.KS (Samsung), SAP, AMD, NVS, NVO, AZN, UNH, NESN.SW, UL, TM, SIE.DE, RHM.DE, SHEL, RIO, BHP, BABA.

## Scripts (repo `outils/scripts/`, idempotents)

- `outils/scripts/seed_universe_100_global.py` — insert des 100 (`ON CONFLICT(symbol) DO NOTHING`).
- `outils/scripts/classify_universe_100_segments.py` — classe en WATCHLIST (HELD si détenu).
- `outils/scripts/pin_core_manual_18.py` — épingle les 18 CORE_MANUAL + retire leur WATCHLIST auto.

### Contrainte d'exécution CRITIQUE
Écrire **avec `duckdb==1.4.4`** (venv VPS `/tmp/ddb144`, à recréer si besoin :
`python3 -m venv /tmp/ddb144 && /tmp/ddb144/bin/pip install duckdb==1.4.4`).
Le host a duckdb 1.5.2 et siga-dashboard 1.5.3 ; **écrire avec ≥1.5 risque d'upgrader le format**
et de casser la lecture côté `yf-enrichment` (1.4.4). La base est **fréquemment lockée** par un
process dashboard (python3.13) ⇒ les scripts ont un retry sur lock.

```bash
ssh vps
/tmp/ddb144/bin/python /tmp/seed_universe_100_global.py --db /local-files/duckdb/ag2_v3.duckdb
/tmp/ddb144/bin/python /tmp/classify_universe_100_segments.py --db /local-files/duckdb/ag2_v3.duckdb
/tmp/ddb144/bin/python /tmp/pin_core_manual_18.py --db /local-files/duckdb/ag2_v3.duckdb
```

## Vérification (lecture seule, retry lock)

```bash
/tmp/ddb144/bin/python - <<'PY'
import duckdb,time
for _ in range(15):
    try: c=duckdb.connect('/local-files/duckdb/ag2_v3.duckdb',read_only=True); break
    except Exception as e:
        if 'lock' in str(e).lower(): time.sleep(2); continue
        raise
print('universe', c.execute('select count(*) from universe').fetchone()[0])          # 563
print(c.execute("select segment,count(*) from universe_segments where active group by 1 order by 2 desc").fetchall())
PY
```

## Rollback

- Univers : restaurer `ag2_v3.duckdb.bak_20260624_preseed`, ou
  `DELETE FROM universe WHERE symbol IN (...)` (les 100).
- Segments : `DELETE FROM universe_segments WHERE reason IN ('seed_global_pillar_pin','seed_global_100_not_held_not_core')`.

## À faire / vigilance

- **Permissions IBKR** à activer pour le bloc non-US (Xetra, SIX, KRX, ASX, SEHK, SGX, BME, Euronext AMS, Tokyo) avant ordre réel. ADR/US (~70 dont la plupart des 18 CORE) négociables immédiatement.
- Garde-fou doc quarantaine respecté pour partie : expansion faite hors PR — **à committer** dans ce repo (3 scripts + cette note).
