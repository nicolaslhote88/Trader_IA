"""Experimental, annual-accounting model. No execution or AG1 decision path.

One monthly observation per issuer. Source availability and label end dates
are purged at every split. Current-universe bias always blocks promotion.
"""
from datetime import timedelta
import hashlib
import json
import joblib
import sklearn
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from normalization import TAGS
from store import ROOT, atomic_json, db, now

FEATURES=['margin','roa','equity_assets','cash_assets','liabilities_assets','ocf_assets','capex_assets','annual_revenue_growth']
STRICT={'SEC_ACCEPTED_PLUS_10M','VERIFIED_PUBLICATION'}


def accounting_at(facts, instant):
    subset=facts[(facts.available_at<=instant)&facts.date_quality.isin(STRICT)].copy()
    if subset.empty:
        return None
    # Require a common annual report period and currency for every ratio.
    revenue=subset[subset.metric=='revenue']
    if revenue.empty:
        return None
    end=revenue.period_end.max()
    if (instant.tz_localize(None)-pd.Timestamp(end)).days>550:
        return None
    same=subset[subset.period_end==end].sort_values('available_at')
    unit=same[same.metric=='revenue'].iloc[-1]['unit']
    same=same[same.unit==unit]
    values={}
    for metric, group in same.groupby('metric'):
        # Latest eligible revision, then preferred equivalent accounting tag.
        group=group[group.available_at==group.available_at.max()].copy()
        group['rank']=group.concept.map(lambda c:TAGS[metric].index(c.split(':')[-1]))
        values[metric]=float(group.sort_values('rank').iloc[0].value)
    def ratio(a,b):
        x,y=values.get(a),values.get(b)
        return x/y if x is not None and y is not None and y>0 else np.nan
    result=dict(margin=ratio('net_income','revenue'),roa=ratio('net_income','assets'),
                equity_assets=ratio('equity','assets'),cash_assets=ratio('cash','assets'),
                liabilities_assets=ratio('liabilities','assets'),ocf_assets=ratio('operating_cashflow','assets'),
                capex_assets=ratio('capex','assets'),annual_revenue_growth=np.nan)
    previous=revenue[(revenue.unit==unit)&(revenue.period_end<end)].copy()
    if not previous.empty:
        prior_end=previous.period_end.max()
        gap=(pd.Timestamp(end)-pd.Timestamp(prior_end)).days
        previous=previous[previous.period_end==prior_end].sort_values('available_at')
        old=float(previous.iloc[-1].value)
        if 330<=gap<=400 and old>0:
            result['annual_revenue_growth']=values['revenue']/old-1
    if sum(np.isfinite(x) for x in result.values())<5:
        return None
    return result


def build_dataset():
    with db(True) as c:
        facts=pd.read_sql_query("SELECT * FROM facts WHERE available_at IS NOT NULL",c)
        prices=pd.read_sql_query("SELECT symbol,day,adj_close,currency FROM prices WHERE symbol='SPY' OR symbol IN (SELECT symbol FROM instruments WHERE asset_class='EQUITY' AND quarantined=0 AND country='United States' AND cik IS NOT NULL) ORDER BY day",c)
        instruments=pd.read_sql_query("SELECT * FROM instruments WHERE asset_class='EQUITY' AND quarantined=0",c)
    if facts.empty or prices.empty:
        return pd.DataFrame(),pd.DataFrame()
    facts['available_at']=pd.to_datetime(facts.available_at,utc=True,errors='coerce')
    prices['day']=pd.to_datetime(prices.day,utc=True)
    # One fixed broad-US total-return proxy. No claim of sector outperformance
    # or portability to other markets. Current sectors do not enter features.
    benchmark=prices[(prices.symbol=='SPY')&(prices.currency=='USD')].set_index('day').adj_close
    if benchmark.empty:
        return pd.DataFrame(),pd.DataFrame()
    labeled=[]
    current=[]
    # Only US-domiciled USD instruments for this first measured experiment.
    issuers=set()
    for instrument in instruments.to_dict('records'):
        if instrument['country']!='United States' or not instrument['cik'] or instrument['cik'] in issuers:
            continue
        issuers.add(instrument['cik'])
        symbol=instrument['symbol']
        series=prices[(prices.symbol==symbol)&(prices.currency=='USD')].set_index('day').adj_close
        panel=pd.concat([series.rename('stock'),benchmark.rename('benchmark')],axis=1).dropna().sort_index()
        if len(panel)<800:
            continue
        ff=facts[facts.symbol==symbol]
        if ff.empty:
            continue
        dates=panel.index.to_series().groupby(panel.index.strftime('%Y-%m')).max().tolist()
        if panel.index[-1] not in dates:
            dates.append(panel.index[-1])
        for day in dates:
            # Information at prior UTC midnight, before that day's trading.
            features=accounting_at(ff,day)
            if features is None:
                continue
            row=dict(symbol=symbol,cik=instrument['cik'],day=day,**features)
            if day==panel.index[-1] and (pd.Timestamp.now(tz='UTC')-day).days<=7:
                current.append(row)
            target=day+timedelta(days=90)
            idx=panel.index.searchsorted(target)
            if idx==len(panel) or (panel.index[idx]-target).days>7:
                continue
            end=panel.index[idx]
            stock_return=panel.loc[end,'stock']/panel.loc[day,'stock']-1
            excess=stock_return-(panel.loc[end,'benchmark']/panel.loc[day,'benchmark']-1)
            # Extreme 90d moves warrant investigation, not silent wins/losses.
            if not np.isfinite(stock_return) or abs(stock_return)>5:
                continue
            labeled.append(dict(row,label_end=end,target=int(excess>0),stock_return=stock_return,excess_return=excess))
    return pd.DataFrame(labeled),pd.DataFrame(current)


def temporal_split(data):
    dates=sorted(data.day.unique())
    a=pd.Timestamp(dates[int(len(dates)*.60)])
    b=pd.Timestamp(dates[int(len(dates)*.80)])
    train=data[(data.day<a)&(data.label_end<a)]
    cal=data[(data.day>=a)&(data.day<b)&(data.label_end<b)]
    test=data[data.day>=b]
    return train,cal,test


def metrics(y,p,dates):
    p=np.clip(p,1e-6,1-1e-6)
    # Equal total weight for each decision date; stocks are not independent.
    counts=pd.Series(dates).value_counts()
    weights=np.array([1/counts[d] for d in dates])
    bins=[]
    for lo in np.arange(0,1,.1):
        mask=(p>=lo)&(p<lo+.1)
        if mask.any():
            bins.append({'lower':round(float(lo),1),'n':int(mask.sum()),
                         'predicted':float(np.average(p[mask],weights=weights[mask])),
                         'observed':float(np.average(np.array(y)[mask],weights=weights[mask]))})
    return {'n':len(y),'dates':len(counts),'brier':float(brier_score_loss(y,p,sample_weight=weights)),
            'log_loss':float(log_loss(y,p,sample_weight=weights,labels=[0,1])), 'reliability':bins}


def train():
    data,latest=build_dataset()
    code_hash=hashlib.sha256(b''.join(p.read_bytes() for p in sorted(Path(__file__).parent.glob('*.py')))).hexdigest()
    report={'source_sha256':code_hash,'sklearn_version':sklearn.__version__,'contract':'AG3_PREDICTIVE_EXPERIMENT_V1','generated_at':now(),'mode':'SHADOW',
            'decision_enabled':False,'validated':False,'target':'90_calendar_day_total_return_outperformance_vs_SPY_USD',
            'horizon_days':90,'scope':'US-domiciled SEC issuers, USD quotation, current universe',
            'features':FEATURES,'rows':len(data),'symbols':int(data.symbol.nunique()) if len(data) else 0,
            'limitations':['Current-universe survivorship bias; delisted returns not covered',
                'Broad-market benchmark SPY, not a point-in-time sector benchmark',
                'Annual consolidated accounting only; valuation and macro not yet model features',
                'No prospective shadow outcomes yet; probabilities not validated for decisions']}
    if len(data)<1000 or data.symbol.nunique()<10 or data.day.nunique()<96:
        report['status']='INSUFFICIENT_STRICT_DATA'
        atomic_json(ROOT/'model_report.json',report)
        atomic_json(ROOT/'predictions.json',{'generated_at':now(),'predictions':[],'decision_enabled':False})
        return report
    train_df,cal,test=temporal_split(data)
    if min(len(train_df),len(cal),len(test))<100 or any(d.target.nunique()<2 for d in [train_df,cal,test]):
        atomic_json(ROOT/'predictions.json',{'generated_at':now(),'predictions':[],'decision_enabled':False})
        report['status']='INSUFFICIENT_TEMPORAL_SPLITS'
        atomic_json(ROOT/'model_report.json',report)
        return report
    data.to_csv(ROOT/'features_labels.csv.gz',index=False,compression='gzip')
    report['dataset_sha256']=hashlib.sha256((ROOT/'features_labels.csv.gz').read_bytes()).hexdigest()
    baseline=float(train_df.target.mean())
    estimators={
        'logistic':make_pipeline(SimpleImputer(add_indicator=True),StandardScaler(),LogisticRegression(C=.1,max_iter=1000,random_state=17)),
        'gradient_boosting':HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=7,
            learning_rate=.05,l2_regularization=5,early_stopping=False,random_state=17)}
    score={'frequency':metrics(test.target,np.full(len(test),baseline),test.day.tolist()),
           'constant_50pct':metrics(test.target,np.full(len(test),.5),test.day.tolist())}
    probabilities={}
    artifacts={}
    for name,estimator in estimators.items():
        estimator.fit(train_df[FEATURES],train_df.target)
        pcal=np.clip(estimator.predict_proba(cal[FEATURES])[:,1],1e-6,1-1e-6)
        calibrator=LogisticRegression(C=1,max_iter=1000)
        calibrator.fit(np.log(pcal/(1-pcal)).reshape(-1,1),cal.target)
        def predict(frame):
            p=np.clip(estimator.predict_proba(frame[FEATURES])[:,1],1e-6,1-1e-6)
            return calibrator.predict_proba(np.log(p/(1-p)).reshape(-1,1))[:,1]
        artifacts[name]={'estimator':estimator,'calibrator':calibrator,'features':FEATURES}
        ptest=predict(test)
        score[name]=metrics(test.target,ptest,test.day.tolist())
        monthly=pd.DataFrame({'date':test.day.to_numpy(),'delta':(ptest-test.target.to_numpy())**2-.25}).groupby('date').delta.mean().to_numpy()
        rng=np.random.default_rng(17)
        differences=[]
        for _ in range(1000):
            starts=rng.integers(0,len(monthly),size=(len(monthly)+2)//3)
            indices=np.concatenate([(s+np.arange(3))%len(monthly) for s in starts])[:len(monthly)]
            differences.append(float(monthly[indices].mean()))
        score[name]['brier_minus_50pct_block_bootstrap_95pct']=np.quantile(differences,[.025,.975]).tolist()
        score[name]['by_year']={str(year):metrics(g.target,ptest[test.index.get_indexer(g.index)],g.day.tolist())
            for year,g in test.groupby(test.day.dt.year)}
        if not latest.empty:
            probabilities[name]=predict(latest).tolist()
    report.update(status='EXPERIMENT_EVALUATED_NOT_PROMOTED',metrics=score,
        splits={name:{'n':len(d),'first':str(d.day.min()),'last':str(d.day.max()),'max_label_end':str(d.label_end.max())}
                for name,d in [('train',train_df),('calibration',cal),('test',test)]},
        challenger_beats_frequency=score['gradient_boosting']['brier']<score['frequency']['brier'],
        challenger_beats_50pct=score['gradient_boosting']['brier']<score['constant_50pct']['brier'],
        challenger_beats_logistic=score['gradient_boosting']['brier']<score['logistic']['brier'])
    # Never select on the test score. Both fixed candidates remain visible.
    predictions=[]
    if not latest.empty:
        for i,row in enumerate(latest.to_dict('records')):
            predictions.append({'symbol':row['symbol'],'as_of':str(row['day']),
                'horizon_days':90,'benchmark':'SPY','currency':'USD','validated':False,
                'probabilities':{name:p[i] for name,p in probabilities.items()}})
    atomic_json(ROOT/'predictions.json',{'generated_at':now(),'decision_enabled':False,'predictions':predictions})
    artifact_path=ROOT/'model_artifacts.joblib'
    joblib.dump({'models':artifacts,'report':report},artifact_path)
    report['artifact_sha256']=hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    atomic_json(ROOT/'model_report.json',report)
    return report
