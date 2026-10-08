"""Optional regulated sources. Archive original reports; do not invent PIT dates."""
import io
import os
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from store import coverage


def collect_dart(client, instruments):
    key=os.getenv('DART_API_KEY')
    if not key:
        coverage('*','opendart','MISSING_KEY','DART_API_KEY required; no account created automatically')
        return
    content,_=client.get('opendart','https://opendart.fss.or.kr/api/corpCode.xml',{'crtfc_key':key},as_json=False,ttl=7*86400)
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        xml=ET.fromstring(z.read('CORPCODE.xml'))
    codes={r.findtext('stock_code'):r.findtext('corp_code') for r in xml.findall('list')}
    for instrument in instruments:
        symbol=instrument['symbol']
        if not symbol.endswith(('.KS','.KQ')):
            continue
        corp=codes.get(symbol.split('.')[0])
        if not corp:
            coverage(symbol,'opendart','NO_MAPPING')
            continue
        reports=0
        for year in range(2015,date.today().year):
            payload,_=client.get('opendart','https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json',
                {'crtfc_key':key,'corp_code':corp,'bsns_year':str(year),'reprt_code':'11011','fs_div':'CFS'},ttl=7*86400)
            if payload.get('status')=='000':
                reports+=1
            elif payload.get('status') not in ('013',):
                raise RuntimeError('OPENDART_STATUS_'+str(payload.get('status')))
        coverage(symbol,'opendart','ARCHIVED_NOT_PIT',f'{reports} annual statements archived; original versions/date reconciliation required')


def collect_edinet(client):
    key=os.getenv('EDINET_API_KEY')
    if not key:
        coverage('*','edinet','MISSING_KEY','EDINET_API_KEY required; no account created automatically')
        return
    count=0
    # Incremental discovery archive. Full historical XBRL normalization is
    # deliberately not represented as validated data without a keyed replay.
    for offset in range(7):
        day=(date.today()-timedelta(days=offset)).isoformat()
        payload,_=client.get('edinet','https://api.edinet-fsa.go.jp/api/v2/documents.json',
            {'date':day,'type':2,'Subscription-Key':key})
        meta=payload.get('metadata',{})
        if str(meta.get('status'))!='200':
            raise RuntimeError('EDINET_STATUS_'+str(meta.get('status')))
        count+=len(payload.get('results',[]))
    coverage('*','edinet','DISCOVERY_ONLY',f'{count} filing metadata records archived; historical original-package normalization pending')
