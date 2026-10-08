"""Read-only display of the isolated predictive experiment."""
import os
import pandas as pd
import requests
import streamlit as st


@st.cache_data(ttl=60,show_spinner=False)
def load_research(symbol):
    base=os.getenv('AG3_RESEARCH_URL','http://ag3-predictive:8084').rstrip('/')
    try:
        a=requests.get(base+'/research/'+symbol,timeout=4)
        a.raise_for_status()
        b=requests.get(base+'/status',timeout=4)
        b.raise_for_status()
        return a.json(),b.json()
    except (requests.RequestException,ValueError):
        return None,None


def render_predictive_detail(symbol):
    render_historical_candidate(symbol)
    with st.expander('Comptes historiques et modèle prédictif en évaluation',expanded=False):
        detail,status=load_research(symbol)
        if detail is None:
            st.info('Le service de recherche prédictive est indisponible. Aucun résultat ne peut être confirmé.')
            return
        st.caption('Recherche en shadow : aucun effet sur le score AG3, la décision AG1 ou les ordres.')
        if detail['coverage']:
            st.dataframe(pd.DataFrame(detail['coverage']).rename(columns={
                'pipe':'Source','status':'État','detail':'Détail','updated_at':'Dernier contrôle'}),hide_index=True)
        else:
            st.info('Ce titre ne dispose pas encore de collecte historique qualifiée.')
        accounts=pd.DataFrame(detail.get('accounts',[]))
        if not accounts.empty:
            st.markdown('**Comptes publiés — versions collectées**')
            st.caption('La période comptable diffère de la date de publication. Les révisions peuvent changer les comparatifs. Les dates inconnues excluent ces chiffres du modèle strict.')
            st.dataframe(accounts.rename(columns={'period_end':'Fin de période','metric':'Métrique',
                'unit':'Unité','value':'Valeur','source':'Source','date_quality':'Qualité de date',
                'available_at':'Disponible à partir de'}),hide_index=True)
        model=status.get('model',{})
        st.markdown('**Évaluation prédictive**')
        st.write('État :',model.get('status','NOT_TRAINED'))
        st.caption('Jeu initial : sociétés américaines, comptes annuels, rendement total à 90 jours comparé à SPY en USD. Ce résultat ne prédit pas un cours précis et ne valide pas les autres marchés.')
        st.write('Observations :',model.get('rows',0),'— Titres :',model.get('symbols',0))
        metrics=model.get('metrics',{})
        if metrics:
            st.dataframe(pd.DataFrame([{'Modèle':name,'Brier (plus bas = meilleur)':v['brier'],
                'Log loss':v['log_loss'],'Observations test':v['n'],'Dates test':v['dates']} for name,v in metrics.items()]),hide_index=True)
        if detail.get('predictions'):
            st.warning('Probabilités expérimentales, non validées pour décider. Le biais de survivance et le suivi prospectif restent à traiter.')
            for prediction in detail['predictions']:
                st.caption('Données au '+prediction['as_of']+' ; calcul du '+str(detail.get('predictions_generated_at')))
                st.dataframe(pd.DataFrame([{'Modèle':k,'P(surperformance SPY à 90 j), expérimentale':round(v,3)}
                    for k,v in prediction['probabilities'].items()]),hide_index=True)
        else:
            st.info('Aucune probabilité disponible pour ce titre : couverture, identité, marché ou historique insuffisant.')
        if status.get('runs'):
            st.caption('Dernière collecte : '+status['runs'][0]['status']+' — '+status['runs'][0]['started_at'])


@st.cache_data(ttl=60,show_spinner=False)
def load_historical_candidate(symbol):
    base=os.getenv('AG3_RESEARCH_URL','http://ag3-predictive:8084').rstrip('/')
    try:
        response=requests.get(base+'/evidence/'+symbol,timeout=4)
        response.raise_for_status()
        payload=response.json()
        return dict(payload['card'], live_decision_enabled=payload.get('live_decision_enabled',False))
    except (requests.RequestException,ValueError,KeyError):
        return None


def render_historical_candidate(symbol):
    with st.expander('Fiche historique datée pour AG1',expanded=False):
        card=load_historical_candidate(symbol)
        if card is None:
            st.info('Fiche historique indisponible.')
            return
        if card.get('live_decision_enabled'):
            st.caption('Intégrée aux entrées des trois modèles AG1 comme contexte factuel consultatif. Gain de performance non démontré.')
        else:
            st.caption('Fiche disponible ; raccordement aux décisions AG1 non activé.')
        st.write('État :',card['status'])
        st.caption('Informations disponibles au '+card['as_of']+' ; périodes annuelles et publications distinctes.')
        if not card['periods']:
            st.info('Aucun compte avec date de publication qualifiée : aucune tendance inventée.')
            return
        rows=[]
        for p in card['periods']:
            r=p['ratios_pct'];a=p['amounts_million']
            rows.append({'Période':p['period_end'],'Disponible au':p['available_at'],'Devise comptable':p['reporting_currency'],
                'Marge nette (%)':r['net_margin'],'Marge opérationnelle (%)':r['operating_margin'],
                'Cash-flow opérationnel (millions)':a.get('operating_cashflow'),'FCF approché (millions)':p['fcf_proxy_million'],
                'Capitaux propres / actifs (%)':r['equity_assets'],'Passifs / actifs (%)':r['liabilities_assets'],
                'Trésorerie / actifs (%)':r['cash_assets']})
        st.dataframe(pd.DataFrame(rows),hide_index=True)
        if card['changes']:
            delta=card['changes']
            st.caption('Comparaison '+delta['from_period']+' → '+delta['to_period']+' : variations des ratios en points de pourcentage.')
            st.json(delta,expanded=False)
        st.caption('Les passifs ne sont pas la dette financière. FCF approché = cash-flow opérationnel − investissements publiés. Aucune prévision de cours.')
        if card['flags']:
            st.warning('Qualité : '+', '.join(card['flags']))
