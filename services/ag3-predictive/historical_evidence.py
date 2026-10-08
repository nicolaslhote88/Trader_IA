"""Dated accounting evidence for isolated AG1 comparisons; no score or forecast."""
from datetime import datetime, timezone
from copy import deepcopy
from normalization import TAGS, numeric
from store import db

SCHEMA = 'AG3_HISTORICAL_EVIDENCE_V1'
STRICT = {'SEC_ACCEPTED_PLUS_10M', 'VERIFIED_PUBLICATION'}
LEGEND = {
    'schema': SCHEMA,
    'use': 'Dated accounting facts only. No trading signal, probability, score or change to gates. Do not double-count the existing fundamental score.',
    'amounts': 'Millions of the stated reporting currency; not the share quotation currency.',
    'ratios': 'Percent; changes in percentage points. Liabilities include more than financial debt.',
    'cashflow': 'FCF proxy = operating cash flow minus reported capex; not a standardized IFRS total.',
    'time': 'Annual periods, latest eligible revisions known at as_of. Unknown publication dates excluded. Not quarterly/TTM data.',
    'missing': 'Null means unavailable, never zero. No extrapolation to another issuer or period.',
    'limitations': 'Accounting trends are not price-return forecasts; financial-sector balance-sheet ratios require sector interpretation.'
}


def timestamp(value):
    d = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if d.tzinfo is None:
        raise ValueError('AS_OF_REQUIRES_TIMEZONE')
    return d.astimezone(timezone.utc)


def card(symbol, facts, as_of, sector=None):
    cutoff = timestamp(as_of)
    eligible = []
    for f in facts:
        try:
            if f['date_quality'] in STRICT and f['available_at'] and timestamp(f['available_at']) <= cutoff and numeric(f['value']) is not None:
                eligible.append(f)
        except (ValueError, TypeError):
            continue
    result = {'schema': SCHEMA, 'symbol': symbol, 'as_of': cutoff.isoformat(),
              'status': 'NO_DATED_ACCOUNTS', 'periods': [], 'changes': None,
              'flags': [], 'advisory_only': True, 'predictive_probabilities_supplied': False}
    revenue = [f for f in eligible if f['metric'] == 'revenue']
    if not revenue:
        return result
    # Annual periods come from normalized revenue flows, not interim balance sheets.
    ends = sorted({f['period_end'] for f in revenue}, reverse=True)
    end = ends[0]
    newest = max((f for f in revenue if f['period_end'] == end), key=lambda f: (timestamp(f['available_at']), f['filing_id']))
    source, unit = newest['source'], newest['unit']
    chosen = []
    for candidate in ends:
        if not chosen or 330 <= (datetime.fromisoformat(chosen[-1])-datetime.fromisoformat(candidate)).days <= 400:
            chosen.append(candidate)
        if len(chosen) == 3:
            break
    for end in chosen:
        period = [f for f in eligible if f['period_end'] == end and f['source'] == source and f['unit'] == unit]
        values, refs = {}, {}
        for metric in TAGS:
            rows = [f for f in period if f['metric'] == metric]
            if not rows:
                continue
            latest = max(timestamp(f['available_at']) for f in rows)
            rows = [f for f in rows if timestamp(f['available_at']) == latest]
            rows.sort(key=lambda f: (TAGS[metric].index(f['concept'].split(':')[-1]), f['filing_id']))
            f = rows[0]
            values[metric] = float(f['value'])
            refs[metric] = {'filing_id': f['filing_id'], 'available_at': f['available_at'],
                            'raw_hash': f['raw_hash'], 'concept': f['concept']}
        def ratio(a,b):
            x,y = values.get(a),values.get(b)
            return round(100*x/y,3) if x is not None and y is not None and y>0 else None
        ocf, capex = values.get('operating_cashflow'), values.get('capex')
        fcf = ocf-capex if ocf is not None and capex is not None and capex>=0 else None
        result['periods'].append({'period_end': end, 'source': source, 'reporting_currency': unit,
            'available_at': max((v['available_at'] for v in refs.values()), key=timestamp) if refs else None,
            'amounts_million': {k: round(v/1e6,3) for k,v in values.items()},
            'fcf_proxy_million': round(fcf/1e6,3) if fcf is not None else None,
            'ratios_pct': {'net_margin': ratio('net_income','revenue'), 'operating_margin': ratio('operating_income','revenue'),
                'ocf_margin': ratio('operating_cashflow','revenue'), 'equity_assets': ratio('equity','assets'),
                'liabilities_assets': ratio('liabilities','assets'), 'cash_assets': ratio('cash','assets')},
            'provenance': refs})
    if len(result['periods']) >= 2:
        a,b = result['periods'][:2]
        if a['reporting_currency'] == b['reporting_currency']:
            delta = {k: round(v-b['ratios_pct'][k],3) if v is not None and b['ratios_pct'][k] is not None else None
                     for k,v in a['ratios_pct'].items()}
            result['changes'] = {'from_period': b['period_end'], 'to_period': a['period_end'], 'ratios_pp': delta,
                'fcf_proxy_delta_million': round(a['fcf_proxy_million']-b['fcf_proxy_million'],3)
                    if a['fcf_proxy_million'] is not None and b['fcf_proxy_million'] is not None else None}
    age = (cutoff.date()-datetime.fromisoformat(result['periods'][0]['period_end']).date()).days
    result['accounting_age_days'] = age
    result['status'] = 'DATED_ACCOUNTS' if age<=550 else 'STALE_ANNUAL_ACCOUNTS'
    if len(result['periods'])<2:
        result['flags'].append('NO_COMPARABLE_ANNUAL_BASELINE')
    if sector and any(x in sector.lower() for x in ['financial','bank','insurance']):
        result['flags'].append('FINANCIAL_SECTOR_RATIOS_NOT_INDUSTRIAL_LEVERAGE')
    return result


def load_cards(symbols, as_of):
    cutoff = timestamp(as_of).isoformat()
    symbols = sorted(set(str(s).upper() for s in symbols if s))
    result = {}
    with db(True) as c:
        for symbol in symbols:
            facts = [dict(r) for r in c.execute('SELECT * FROM facts WHERE symbol=? AND available_at IS NOT NULL',(symbol,))]
            instrument = c.execute('SELECT sector FROM instruments WHERE symbol=?',(symbol,)).fetchone()
            result[symbol] = card(symbol, facts, cutoff, instrument[0] if instrument else None)
    return result


def compact(value):
    value = deepcopy(value)
    for period in value['periods']:
        provenance = period.pop('provenance')
        period['filings'] = sorted({p['filing_id'] for p in provenance.values()})
    return value


def enrich_context(context, cards):
    """Pure candidate transformation. Production never calls this implicitly."""
    result = deepcopy(context)
    pack = result.setdefault('opportunity_pack', {})
    for row in pack.get('rows', []):
        symbol = str(row.get('symbol','')).upper()
        if symbol in cards:
            row.setdefault('fundamentals', {})['historical_accounts'] = compact(cards[symbol])
    held = result.get('portfolio_pack',{}).get('positions',[])
    pack['held_historical_accounts'] = {r['symbol']: compact(cards[r['symbol']]) for r in held if r.get('symbol') in cards}
    pack['historical_accounts_legend'] = LEGEND
    return result
