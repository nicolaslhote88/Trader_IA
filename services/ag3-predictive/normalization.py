"""Conservative accounting normalization. No date or zero imputation."""
from datetime import datetime, timedelta, timezone
import math

TAGS = {
 'revenue': ['RevenueFromContractWithCustomerExcludingAssessedTax','Revenues','SalesRevenueNet','Revenue'],
 'net_income': ['NetIncomeLoss','ProfitLoss'],
 'operating_income': ['OperatingIncomeLoss','ProfitLossFromOperatingActivities'],
 'operating_cashflow': ['NetCashProvidedByUsedInOperatingActivities','CashFlowsFromUsedInOperatingActivities'],
 'capex': ['PaymentsToAcquirePropertyPlantAndEquipment'],
 'assets': ['Assets'], 'equity': ['StockholdersEquity','EquityAttributableToOwnersOfParent','Equity'],
 'cash': ['CashAndCashEquivalentsAtCarryingValue','CashAndCashEquivalents'],
 'liabilities': ['Liabilities'],
}
METRICS = {tag:metric for metric,tags in TAGS.items() for tag in tags}
FLOWS = {'revenue','net_income','operating_income','operating_cashflow','capex'}


def numeric(value):
    if value is None or isinstance(value,bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError,ValueError):
        return None


def accepted_time(raw, filed):
    if raw:
        try:
            d=datetime.fromisoformat(raw.replace('Z','+00:00'))
            # SEC acceptanceDateTime supplies offset; don't guess one.
            if d.tzinfo:
                return (d.astimezone(timezone.utc)+timedelta(minutes=10)).isoformat(), 'SEC_ACCEPTED_PLUS_10M'
        except ValueError:
            pass
    # A filing date without time is conservatively available two calendar
    # days later. Kept separate and excluded from the strict model.
    if filed:
        return (datetime.fromisoformat(filed).replace(tzinfo=timezone.utc)+timedelta(days=2)).isoformat(), 'FILED_DATE_ONLY'
    return None, 'UNKNOWN'


def sec_facts(symbol, payload, submissions, digest):
    rows=[]
    for namespace, concepts in payload.get('facts',{}).items():
        if namespace not in ('us-gaap','ifrs-full'):
            continue
        for tag, concept in concepts.items():
            if tag not in METRICS:
                continue
            metric=METRICS[tag]
            for unit, facts in concept.get('units',{}).items():
                if len(unit)!=3 or not unit.isupper():
                    continue
                for fact in facts:
                    value=numeric(fact.get('val'))
                    accession=fact.get('accn')
                    end=fact.get('end')
                    start=fact.get('start','')
                    if value is None or not accession or not end:
                        continue
                    if fact.get('form') not in ('10-K','10-Q','10-K/A','10-Q/A','20-F','20-F/A','40-F','40-F/A'):
                        continue
                    if metric in FLOWS:
                        try:
                            duration=(datetime.fromisoformat(end)-datetime.fromisoformat(start)).days
                        except ValueError:
                            continue
                        # Annual features only: no mixing YTD/quarterly flows.
                        if not 330 <= duration <= 380:
                            continue
                    available, quality=accepted_time(submissions.get(accession),fact.get('filed'))
                    rows.append(('sec',symbol,accession,metric,namespace+':'+tag,start,end,available,quality,unit,value,digest))
    return rows


def esef_facts(symbol, filing, payload, digest, publication=None):
    rows=[]
    # Catalogue addition time cannot establish historical market availability.
    available=publication.get('available_at') if publication else None
    quality='VERIFIED_PUBLICATION' if available else 'UNKNOWN_PUBLICATION'
    for f in payload.get('facts',{}).values():
        dim=f.get('dimensions',{})
        concept=dim.get('concept','')
        metric=METRICS.get(concept.split(':')[-1])
        value=numeric(f.get('value'))
        if not metric or value is None:
            continue
        # Exclude dimensional segment facts, which are not consolidated totals.
        if set(dim)-{'concept','entity','period','unit','language'}:
            continue
        period=dim.get('period','')
        parts=period.split('/')
        try:
            # XBRL JSON instants are end-exclusive midnight boundaries.
            end=(datetime.fromisoformat(parts[-1].replace('Z','+00:00'))-timedelta(days=1)).date().isoformat()
            start=datetime.fromisoformat(parts[0].replace('Z','+00:00')).date().isoformat() if len(parts)==2 else ''
            if metric in FLOWS and (not start or not 330 <= (datetime.fromisoformat(end)-datetime.fromisoformat(start)).days <= 380):
                continue
        except ValueError:
            continue
        unit=dim.get('unit','').split(':')[-1]
        if len(unit)!=3 or not unit.isupper():
            continue
        rows.append(('esef',symbol,filing,metric,concept,start,end,available,quality,unit,value,digest))
    return rows
