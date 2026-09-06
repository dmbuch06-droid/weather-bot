import hashlib, json, logging, math, os, re, statistics, time
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests
try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    psycopg2 = None
    Json = None

KALSHI_API_URL=os.environ.get('KALSHI_API_URL','https://external-api.kalshi.com/trade-api/v2').rstrip('/')
DATABASE_URL=os.environ.get('DATABASE_URL','').strip()
DISCORD_RELAY_URL=os.environ.get('DISCORD_RELAY_URL','').strip()
DISCORD_RELAY_SECRET=os.environ.get('DISCORD_RELAY_SECRET','').strip()
REQUEST_TIMEOUT=int(os.environ.get('REQUEST_TIMEOUT','20'))
FORECAST_DAYS=int(os.environ.get('FORECAST_DAYS','3'))
NWS_API_URL='https://api.weather.gov'
NWS_USER_AGENT=os.environ.get('NWS_USER_AGENT','WeatherKalshiResearchBot/7.0')
GEOCODING_API_URL='https://geocoding-api.open-meteo.com/v1/search'
ENSEMBLE_MODEL='gfs_seamless'; DETERMINISTIC_MODEL='gfs_seamless'
SCHEMA_VERSION=7; MEASUREMENT_VERSION='v7_clean_baseline'
MIN_FORECAST_PROBABILITY_CHANGE_POINTS=float(os.environ.get('MIN_FORECAST_PROBABILITY_CHANGE_POINTS','20'))
MIN_MARKET_LAG_POINTS=float(os.environ.get('MIN_MARKET_LAG_POINTS','10'))
MIN_PRELIMINARY_EDGE_POINTS=float(os.environ.get('MIN_PRELIMINARY_EDGE_POINTS','10'))
MIN_ENTRY_PRICE_CENTS=float(os.environ.get('MIN_ENTRY_PRICE_CENTS','5'))
MAX_ENTRY_PRICE_CENTS=float(os.environ.get('MAX_ENTRY_PRICE_CENTS','95'))
PAPER_RISK_DOLLARS=float(os.environ.get('PAPER_RISK_DOLLARS','10'))
RESEARCH_MIN_FORECAST_CHANGE_POINTS=float(os.environ.get('RESEARCH_MIN_FORECAST_CHANGE_POINTS','3'))
ALLOW_UNVERIFIED_LOCATION_SIGNALS=os.environ.get('ALLOW_UNVERIFIED_LOCATION_SIGNALS','false').lower() in {'1','true','yes'}
KNOWN={'NYC':('New York City',40.7789,-73.9692,'America/New_York',True),'CHI':('Chicago',41.9742,-87.9073,'America/Chicago',False),'MIA':('Miami',25.7959,-80.2870,'America/New_York',False),'AUS':('Austin',30.1975,-97.6663,'America/Chicago',False)}
logging.basicConfig(level=logging.INFO,format='%(asctime)s | %(levelname)s | %(message)s'); log=logging.getLogger('weather-kalshi-scanner'); _DB_CONN=None

def now(): return datetime.now(timezone.utc)
def f(v,d=None):
    try:return float(v) if v is not None else d
    except:return d
def h(x): return hashlib.sha256(json.dumps(x,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
def slug(s): return re.sub(r'[^A-Z0-9]+','_',s.upper()).strip('_')[:48] or 'UNKNOWN'
def local_date(ts,tz): return datetime.fromisoformat(str(ts).replace('Z','+00:00')).astimezone(ZoneInfo(tz)).date().isoformat()
def round_temp(v):
    v=f(v); return None if v is None else int(math.floor(v+0.5))

def db():
    if not DATABASE_URL: raise RuntimeError('DATABASE_URL is not configured')
    if psycopg2 is None: raise RuntimeError('psycopg2-binary is not installed')
    return psycopg2.connect(DATABASE_URL,connect_timeout=10)
def q(sql,p=(),fetch=False,one=False):
    global _DB_CONN
    if _DB_CONN is None or _DB_CONN.closed:_DB_CONN=db()
    try:
        with _DB_CONN.cursor() as c:
            c.execute(sql,p); r=c.fetchone() if one else (c.fetchall() if fetch else None)
        _DB_CONN.commit(); return r
    except Exception:
        _DB_CONN.rollback(); raise
def close_db():
    global _DB_CONN
    if _DB_CONN is not None and not _DB_CONN.closed:_DB_CONN.close()
    _DB_CONN=None

def http(url,params=None,headers=None,tries=2):
    last=None
    for i in range(tries):
        try:
            r=requests.get(url,params=params,headers=headers or {'User-Agent':'WeatherKalshiResearchBot/7.0','Accept':'application/json'},timeout=REQUEST_TIMEOUT)
            if r.status_code==200:return r.json()
            if r.status_code in {408,429,500,502,503,504} and i+1<tries:time.sleep(1);continue
            raise RuntimeError(f'HTTP {r.status_code}: {r.text[:800]}')
        except requests.RequestException as e:
            last=e
            if i+1<tries:time.sleep(1);continue
    raise RuntimeError(f'HTTP request failed: {last}')
def nws(url): return http(url,headers={'User-Agent':NWS_USER_AGENT,'Accept':'application/geo+json,application/json'})

def schema():
    for s in [
'''CREATE TABLE IF NOT EXISTS scan_runs(id BIGSERIAL PRIMARY KEY,started_at TIMESTAMPTZ NOT NULL,completed_at TIMESTAMPTZ,status TEXT NOT NULL,stats JSONB NOT NULL DEFAULT '{}'::jsonb,error TEXT)''',
'''CREATE TABLE IF NOT EXISTS series_registry(series_ticker TEXT PRIMARY KEY,title TEXT NOT NULL,category TEXT,tags JSONB NOT NULL,settlement_sources JSONB NOT NULL,contract_terms_url TEXT,updated_at TIMESTAMPTZ NOT NULL,raw_series JSONB NOT NULL)''',
'''CREATE TABLE IF NOT EXISTS weather_service_state(service TEXT NOT NULL,city TEXT NOT NULL,state_key TEXT NOT NULL,last_update TIMESTAMPTZ,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),PRIMARY KEY(service,city,state_key))''',
'''CREATE TABLE IF NOT EXISTS forecast_observations(id BIGSERIAL PRIMARY KEY,observed_at TIMESTAMPTZ NOT NULL,city TEXT NOT NULL,variable TEXT NOT NULL,model TEXT NOT NULL,forecast_date DATE NOT NULL,scalar_value DOUBLE PRECISION,payload JSONB NOT NULL,payload_hash TEXT NOT NULL,UNIQUE(city,variable,model,forecast_date,payload_hash))''',
'''CREATE TABLE IF NOT EXISTS market_snapshots(id BIGSERIAL PRIMARY KEY,observed_at TIMESTAMPTZ NOT NULL,ticker TEXT NOT NULL,event_ticker TEXT,series_ticker TEXT,market_date DATE,city TEXT,market_kind TEXT NOT NULL,strike_type TEXT,floor_strike DOUBLE PRECISION,cap_strike DOUBLE PRECISION,yes_bid_cents DOUBLE PRECISION,yes_ask_cents DOUBLE PRECISION,no_bid_cents DOUBLE PRECISION,no_ask_cents DOUBLE PRECISION,last_price_cents DOUBLE PRECISION,status TEXT,result TEXT)''',
'''CREATE TABLE IF NOT EXISTS paper_trades(id BIGSERIAL PRIMARY KEY,signal_fingerprint TEXT UNIQUE NOT NULL,created_at TIMESTAMPTZ NOT NULL,settled_at TIMESTAMPTZ,city TEXT NOT NULL,forecast_date DATE NOT NULL,market_ticker TEXT NOT NULL,market_kind TEXT NOT NULL,side TEXT NOT NULL,entry_price_cents DOUBLE PRECISION NOT NULL,stake_dollars DOUBLE PRECISION NOT NULL,contracts DOUBLE PRECISION NOT NULL,model_probability_proxy DOUBLE PRECISION NOT NULL,preliminary_edge_points DOUBLE PRECISION NOT NULL,forecast_probability_change_points DOUBLE PRECISION NOT NULL,market_price_change_points DOUBLE PRECISION NOT NULL,market_lag_points DOUBLE PRECISION NOT NULL,forecast_temperature_change_f DOUBLE PRECISION,reason JSONB NOT NULL,result TEXT,profit_loss_dollars DOUBLE PRECISION,status TEXT NOT NULL DEFAULT 'open')''',
'''CREATE TABLE IF NOT EXISTS alert_log(fingerprint TEXT PRIMARY KEY,sent_at TIMESTAMPTZ NOT NULL,payload JSONB NOT NULL)''',
'''CREATE TABLE IF NOT EXISTS forecast_research_events(id BIGSERIAL PRIMARY KEY,event_fingerprint TEXT UNIQUE NOT NULL,created_at TIMESTAMPTZ NOT NULL,city TEXT NOT NULL,forecast_date DATE NOT NULL,variable TEXT NOT NULL,market_ticker TEXT NOT NULL,side TEXT NOT NULL,previous_probability DOUBLE PRECISION NOT NULL,current_probability DOUBLE PRECISION NOT NULL,forecast_probability_change_points DOUBLE PRECISION NOT NULL,pre_forecast_ask_cents DOUBLE PRECISION,event_ask_cents DOUBLE PRECISION,initial_market_change_points DOUBLE PRECISION,initial_market_lag_points DOUBLE PRECISION,initial_preliminary_edge_points DOUBLE PRECISION,first_response_at TIMESTAMPTZ,milestone_25_at TIMESTAMPTZ,milestone_50_at TIMESTAMPTZ,milestone_75_at TIMESTAMPTZ,milestone_90_at TIMESTAMPTZ,latest_observation_at TIMESTAMPTZ,latest_ask_cents DOUBLE PRECISION,latest_market_move_points DOUBLE PRECISION,latest_lag_remaining_points DOUBLE PRECISION,max_market_move_points DOUBLE PRECISION DEFAULT 0,status TEXT NOT NULL DEFAULT 'open',closed_at TIMESTAMPTZ,settlement_result TEXT)''',
'''CREATE TABLE IF NOT EXISTS forecast_research_updates(id BIGSERIAL PRIMARY KEY,event_id BIGINT NOT NULL REFERENCES forecast_research_events(id) ON DELETE CASCADE,observed_at TIMESTAMPTZ NOT NULL,market_ask_cents DOUBLE PRECISION,market_move_points DOUBLE PRECISION,lag_remaining_points DOUBLE PRECISION,market_response_fraction DOUBLE PRECISION)''',
'''CREATE TABLE IF NOT EXISTS weather_locations(location_key TEXT PRIMARY KEY,city_name TEXT NOT NULL,latitude DOUBLE PRECISION NOT NULL,longitude DOUBLE PRECISION NOT NULL,timezone TEXT NOT NULL,settlement_verified BOOLEAN NOT NULL DEFAULT FALSE,signal_enabled BOOLEAN NOT NULL DEFAULT FALSE,mapping_method TEXT NOT NULL,nws_grid_url TEXT,source_series_tickers JSONB NOT NULL DEFAULT '[]'::jsonb,raw_geocode JSONB,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())''']:
        q(s)
    for s in [
        'ALTER TABLE scan_runs ADD COLUMN IF NOT EXISTS schema_version INTEGER',
        'ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS scan_id BIGINT',
        'ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS snapshot_phase TEXT',
        'ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS measurement_version TEXT',
        'ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS spread_yes_cents DOUBLE PRECISION',
        'ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS spread_no_cents DOUBLE PRECISION',
        'ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS volume DOUBLE PRECISION',
        'ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS open_interest DOUBLE PRECISION',
        'ALTER TABLE forecast_research_events ADD COLUMN IF NOT EXISTS scan_id BIGINT',
        'ALTER TABLE forecast_research_events ADD COLUMN IF NOT EXISTS forecast_observed_at TIMESTAMPTZ',
        'ALTER TABLE forecast_research_events ADD COLUMN IF NOT EXISTS measurement_version TEXT',
        'ALTER TABLE forecast_research_events ADD COLUMN IF NOT EXISTS settlement_verified BOOLEAN',
        'ALTER TABLE forecast_research_events ADD COLUMN IF NOT EXISTS location_key TEXT',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS scan_id BIGINT',
        'ALTER TABLE forecast_observations ADD COLUMN IF NOT EXISTS source_observed_at TIMESTAMPTZ',
        'ALTER TABLE forecast_observations ADD COLUMN IF NOT EXISTS measurement_version TEXT']:
        q(s)
    for s in ['CREATE INDEX IF NOT EXISTS idx_forecast_lookup ON forecast_observations(city,variable,model,forecast_date,observed_at DESC)','CREATE INDEX IF NOT EXISTS idx_market_lookup ON market_snapshots(ticker,observed_at DESC)','CREATE INDEX IF NOT EXISTS idx_market_scan ON market_snapshots(scan_id,snapshot_phase,observed_at DESC)','CREATE INDEX IF NOT EXISTS idx_research_open ON forecast_research_events(status,created_at DESC)','CREATE INDEX IF NOT EXISTS idx_updates_event ON forecast_research_updates(event_id,observed_at DESC)']:q(s)
    for k,(city,lat,lon,tz,verified) in KNOWN.items():
        q('''INSERT INTO weather_locations(location_key,city_name,latitude,longitude,timezone,settlement_verified,signal_enabled,mapping_method) VALUES(%s,%s,%s,%s,%s,%s,%s,'manual_existing') ON CONFLICT(location_key) DO NOTHING''',(k,city,lat,lon,tz,verified,verified))

def series_list():
    out=[];cur=None
    while True:
        p={'category':'Climate and Weather','limit':1000};
        if cur:p['cursor']=cur
        d=http(KALSHI_API_URL+'/series',p);out+=d.get('series',[]);cur=d.get('cursor')
        if not cur:return out

def save_series(xs):
    c=db()
    try:
        with c.cursor() as cur:
            cur.executemany('''INSERT INTO series_registry(series_ticker,title,category,tags,settlement_sources,contract_terms_url,updated_at,raw_series) VALUES(%s,%s,%s,%s,%s,%s,NOW(),%s) ON CONFLICT(series_ticker) DO UPDATE SET title=EXCLUDED.title,category=EXCLUDED.category,tags=EXCLUDED.tags,settlement_sources=EXCLUDED.settlement_sources,contract_terms_url=EXCLUDED.contract_terms_url,updated_at=NOW(),raw_series=EXCLUDED.raw_series''',[(x.get('ticker'),x.get('title',''),x.get('category'),Json(x.get('tags') or []),Json(x.get('settlement_sources') or []),x.get('contract_terms_url'),Json(x)) for x in xs if x.get('ticker')])
        c.commit()
    except: c.rollback();raise
    finally:c.close()

def is_temp(s):
    t=(s.get('title') or '').lower();x=(s.get('ticker') or '').upper();freq=(s.get('frequency') or '').lower()
    return (not freq or freq=='daily') and 'lowest temperature' not in t and (x.startswith('KXHIGH') or 'highest temperature' in t or 'high temperature' in t or 'maximum temperature' in t)
def is_rain(s):
    t=(s.get('title') or '').lower();x=(s.get('ticker') or '').upper();return x=='KXRAIN' or ((s.get('frequency') or '').lower()=='daily' and ('rain' in t or 'precipitation' in t))
def city_title(s):
    t=' '.join((s.get('title') or '').split())
    m=re.search(r'(?:temperature|rain|precipitation)\s+in\s+(.+?)(?:\s+today\??|\s+on\s+.+?\??$|\?$|$)',t,re.I)
    return m.group(1).strip(' ?.') if m else None

def geocode(city):
    d=http(GEOCODING_API_URL,{'name':city,'count':5,'language':'en','format':'json','countryCode':'US'})
    rs=[r for r in d.get('results',[]) if str(r.get('country_code','')).upper()=='US']
    if not rs:return None
    norm=re.sub(r'[^a-z0-9]','',city.lower());rs.sort(key=lambda r:0 if re.sub(r'[^a-z0-9]','',str(r.get('name','')).lower())==norm else 1);r=rs[0]
    return r if r.get('latitude') is not None and r.get('longitude') is not None and r.get('timezone') else None

def loc_for(s):
    x=(s.get('ticker') or '').upper();mp={'KXHIGHNY':'NYC','HIGHNY':'NYC','KXHIGHCHI':'CHI','HIGHCHI':'CHI','KXHIGHMIA':'MIA','HIGHMIA':'MIA','KXHIGHAUS':'AUS','HIGHAUS':'AUS'}
    for p,k in mp.items():
        if p in x:
            r=q('SELECT location_key,city_name,latitude,longitude,timezone,settlement_verified,signal_enabled,mapping_method,nws_grid_url FROM weather_locations WHERE location_key=%s',(k,),one=True);return rowloc(r) if r else None
    city=city_title(s)
    if not city:return None
    r=q('SELECT location_key,city_name,latitude,longitude,timezone,settlement_verified,signal_enabled,mapping_method,nws_grid_url FROM weather_locations WHERE lower(city_name)=lower(%s) LIMIT 1',(city,),one=True)
    if r:return rowloc(r)
    g=geocode(city)
    if not g:return None
    k=slug(g.get('name') or city)
    q('''INSERT INTO weather_locations(location_key,city_name,latitude,longitude,timezone,mapping_method,source_series_tickers,raw_geocode) VALUES(%s,%s,%s,%s,%s,'open_meteo_geocoding',jsonb_build_array(%s),%s) ON CONFLICT(location_key) DO NOTHING''',(k,g['name'],g['latitude'],g['longitude'],g['timezone'],s.get('ticker',''),Json(g)))
    return getloc(k)
def rowloc(r):
    return {'location_key':r[0],'city_name':r[1],'latitude':r[2],'longitude':r[3],'timezone':r[4],'settlement_verified':bool(r[5]),'signal_enabled':bool(r[6]),'mapping_method':r[7],'nws_grid_url':r[8]}
def getloc(k):
    r=q('SELECT location_key,city_name,latitude,longitude,timezone,settlement_verified,signal_enabled,mapping_method,nws_grid_url FROM weather_locations WHERE location_key=%s',(k,),one=True);return rowloc(r) if r else None

def discover(xs):
    temps=[];rains=[];seen_t=set();seen_r=set()
    for s in xs:
        if is_temp(s):
            l=loc_for(s)
            if l:temps.append((s,l))
        elif is_rain(s):
            l=loc_for(s)
            if l:rains.append((s,l))
    by={}
    for s,l in temps:
        by.setdefault(l['location_key'],[]).append((s,l))
    temps=[]
    for k,es in sorted(by.items()):
        es.sort(key=lambda z:(0 if (z[0].get('ticker') or '').upper().startswith('KXHIGH') else 1,z[0].get('ticker') or ''));temps.append(es[0])
    for e in rains:
        if e[0].get('ticker') not in seen_r:seen_r.add(e[0].get('ticker'));seen_t.add(e[0].get('ticker')); 
    rains=[e for e in rains if e[0].get('ticker') in seen_r]
    return temps,rains

def markets(ticker):
    out=[];cur=None
    while True:
        p={'series_ticker':ticker,'status':'open','limit':1000};
        if cur:p['cursor']=cur
        d=http(KALSHI_API_URL+'/markets',p);out+=d.get('markets',[]);cur=d.get('cursor')
        if not cur:return out

def date_market(m):
    for s in (m.get('event_ticker',''),m.get('ticker','')):
        for p in s.split('-'):
            try:return datetime.strptime(p,'%y%b%d').date().isoformat()
            except:pass
    return None

def cache(entries): return [(s,l,markets(s.get('ticker'))) for s,l in entries]

def snapshot(cache,scan_id,phase,stats):
    for kind,entries in [('temperature',cache['temperature']),('precipitation',cache['rain'])]:
        for s,l,ms in entries:
            for m in ms:
                yb=f(m.get('yes_bid_dollars'));ya=f(m.get('yes_ask_dollars'));nb=f(m.get('no_bid_dollars'));na=f(m.get('no_ask_dollars'))
                q('''INSERT INTO market_snapshots(observed_at,ticker,event_ticker,series_ticker,market_date,city,market_kind,strike_type,floor_strike,cap_strike,yes_bid_cents,yes_ask_cents,no_bid_cents,no_ask_cents,last_price_cents,status,result,spread_yes_cents,spread_no_cents,volume,open_interest,scan_id,snapshot_phase,measurement_version) VALUES(NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(m.get('ticker',''),m.get('event_ticker'),m.get('series_ticker'),date_market(m),l['city_name'],kind,m.get('strike_type'),f(m.get('floor_strike')),f(m.get('cap_strike')),None if yb is None else yb*100,None if ya is None else ya*100,None if nb is None else nb*100,None if na is None else na*100,None if f(m.get('last_price_dollars')) is None else f(m.get('last_price_dollars'))*100,m.get('status'),m.get('result'),None if yb is None or ya is None else max(0,ya*100-yb*100),None if nb is None or na is None else max(0,na*100-nb*100),f(m.get('volume')),f(m.get('open_interest')),scan_id,phase,MEASUREMENT_VERSION))
                stats['temperature_markets' if kind=='temperature' else 'rain_markets']+=1

def prior_market(ticker,before):return q('SELECT observed_at,yes_bid_cents,yes_ask_cents,no_bid_cents,no_ask_cents,last_price_cents FROM market_snapshots WHERE ticker=%s AND observed_at<%s AND measurement_version=%s ORDER BY observed_at DESC,id DESC LIMIT 1',(ticker,before,MEASUREMENT_VERSION),one=True)
def event_market(ticker,scan):return q('SELECT observed_at,yes_bid_cents,yes_ask_cents,no_bid_cents,no_ask_cents,last_price_cents FROM market_snapshots WHERE ticker=%s AND scan_id=%s AND snapshot_phase=\'forecast_event\' AND measurement_version=%s ORDER BY observed_at DESC,id DESC LIMIT 1',(ticker,scan,MEASUREMENT_VERSION),one=True)

def tprob(vals,m):
    vals=[round_temp(v) for v in vals if round_temp(v) is not None];
    if not vals:return None
    st=(m.get('strike_type') or '').lower();lo=f(m.get('floor_strike'));hi=f(m.get('cap_strike'))
    if st=='greater':hits=sum(v>lo for v in vals) if lo is not None else None
    elif st=='less':hits=sum(v<hi for v in vals) if hi is not None else None
    elif st=='between':hits=sum(lo<=v<=hi for v in vals) if lo is not None and hi is not None else None
    else:return None
    return None if hits is None else 100*hits/len(vals)
def rprob(vals):
    vals=[f(v) for v in vals if f(v) is not None];return None if not vals else 100*sum(v>0 for v in vals)/len(vals)

def forecast_prev(city,var,date,before):
    r=q('SELECT payload FROM forecast_observations WHERE city=%s AND variable=%s AND model=%s AND forecast_date=%s AND observed_at<%s AND measurement_version=%s ORDER BY observed_at DESC LIMIT 1',(city,var,ENSEMBLE_MODEL,date,before,MEASUREMENT_VERSION),one=True);return r[0] if r else None
def save_forecasts(det,ens,observed):
    for k,d in det.items():
        l=getloc(k);city=l['city_name']
        for date,x in d['daily'].items():
            for var,val,p in [('temperature_high',x['high'],x),('precipitation_sum',x['precipitation_sum'],x)]:
                p={**p,'model_run':d.get('model_run'),'location_key':k};q('INSERT INTO forecast_observations(observed_at,city,variable,model,forecast_date,scalar_value,payload,payload_hash,source_observed_at,measurement_version) VALUES(NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(city,variable,model,forecast_date,payload_hash) DO NOTHING',(city,var,DETERMINISTIC_MODEL,date,val,Json(p),h(p),observed,MEASUREMENT_VERSION))
    for k,d in ens.items():
        l=getloc(k);city=l['city_name']
        for date,x in d['daily'].items():
            p={'member_highs':x['member_highs'],'member_highs_rounded':[round_temp(v) for v in x['member_highs']],'member_precip_totals':x['member_precip_totals'],'temperature_mean':x['temperature_mean'],'temperature_median':x['temperature_median'],'temperature_member_count':d['temperature_member_count'],'precipitation_member_count':d['precipitation_member_count'],'model_run':d.get('model_run'),'location_key':k}
            q('INSERT INTO forecast_observations(observed_at,city,variable,model,forecast_date,scalar_value,payload,payload_hash,source_observed_at,measurement_version) VALUES(NOW(),%s,\'ensemble_temperature_distribution\',%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(city,variable,model,forecast_date,payload_hash) DO NOTHING',(city,ENSEMBLE_MODEL,date,x['temperature_mean'],Json(p),h(p),observed,MEASUREMENT_VERSION))
            if x['member_precip_totals']:
                p={'member_precip_totals':x['member_precip_totals'],'precipitation_member_count':d['precipitation_member_count'],'model_run':d.get('model_run'),'location_key':k}
                q('INSERT INTO forecast_observations(observed_at,city,variable,model,forecast_date,scalar_value,payload,payload_hash,source_observed_at,measurement_version) VALUES(NOW(),%s,\'ensemble_rain_distribution\',%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(city,variable,model,forecast_date,payload_hash) DO NOTHING',(city,ENSEMBLE_MODEL,date,statistics.mean(x['member_precip_totals']),Json(p),h(p),observed,MEASUREMENT_VERSION))

def nws_updates(locations):
    out={};changed={}
    for l in locations:
        try:
            url=l.get('nws_grid_url')
            if not url:
                p=nws(f"{NWS_API_URL}/points/{l['latitude']},{l['longitude']}");url=(p.get('properties') or {}).get('forecastGridData');
                if url:q('UPDATE weather_locations SET nws_grid_url=%s,updated_at=NOW() WHERE location_key=%s',(url,l['location_key']))
            if not url:continue
            d=nws(url);u=(d.get('properties') or {}).get('updateTime')
            if not u:continue
            dt=datetime.fromisoformat(u.replace('Z','+00:00'));out[l['location_key']]=dt
            r=q("SELECT last_update FROM weather_service_state WHERE service='NWS_GRID' AND city=%s AND state_key=%s",(l['city_name'],l['location_key']),one=True)
            if not r or not r[0] or dt>r[0]:changed[l['location_key']]=dt
        except Exception as e:log.warning('NWS update unavailable for %s: %s',l['city_name'],e)
    return out,changed
def save_nws(us,locs):
    for k,u in us.items():
        l=locs.get(k)
        if l:q('''INSERT INTO weather_service_state(service,city,state_key,last_update,updated_at) VALUES('NWS_GRID',%s,%s,%s,NOW()) ON CONFLICT(service,city,state_key) DO UPDATE SET last_update=EXCLUDED.last_update,updated_at=NOW()''',(l['city_name'],k,u))

def norm(data,locs):
    rows=data if isinstance(data,list) else [data]
    if len(rows)!=len(locs):raise RuntimeError(f'Forecast API returned {len(rows)} locations; expected {len(locs)}')
    return zip(locs,rows)
def hourly(loc,d):
    x=d.get('hourly') or {};ts=x.get('time') or [];temps=x.get('temperature_2m') or [];rain=x.get('precipitation') or [];g=defaultdict(lambda:{'t':[],'p':[]})
    for i,t in enumerate(ts):
        day=local_date(t,loc['timezone']);
        if i<len(temps) and f(temps[i]) is not None:g[day]['t'].append(f(temps[i]))
        if i<len(rain) and f(rain[i]) is not None:g[day]['p'].append(f(rain[i]))
    return {day:{'high':max(v['t']),'precipitation_sum':sum(v['p'])} for day,v in g.items() if v['t']}
def fetch_det(locs):
    p={'latitude':','.join(str(x['latitude']) for x in locs),'longitude':','.join(str(x['longitude']) for x in locs),'models':DETERMINISTIC_MODEL,'hourly':'temperature_2m,precipitation','temperature_unit':'fahrenheit','precipitation_unit':'inch','timezone':'UTC','forecast_days':FORECAST_DAYS};d=http('https://api.open-meteo.com/v1/gfs',p);return {l['location_key']:{'daily':hourly(l,x),'model_run':x.get('model_run') or x.get('model_run_id') or x.get('model_run_time')} for l,x in norm(d,locs)}
def fetch_ens(locs):
    p={'latitude':','.join(str(x['latitude']) for x in locs),'longitude':','.join(str(x['longitude']) for x in locs),'models':ENSEMBLE_MODEL,'hourly':'temperature_2m,precipitation','temperature_unit':'fahrenheit','precipitation_unit':'inch','timezone':'UTC','forecast_days':FORECAST_DAYS};data=http('https://ensemble-api.open-meteo.com/v1/ensemble',p);out={}
    for l,d in norm(data,locs):
        x=d.get('hourly') or {};ts=x.get('time') or [];tk=sorted(k for k in x if k.startswith('temperature_2m_member'));pk=sorted(k for k in x if k.startswith('precipitation_member'));days=defaultdict(lambda:{'t':defaultdict(list),'p':defaultdict(float)})
        if not tk:raise RuntimeError(f'No ensemble temperature members for {l["city_name"]}')
        for i,t in enumerate(ts):
            day=local_date(t,l['timezone'])
            for k in tk:
                a=x.get(k) or []
                if i<len(a) and f(a[i]) is not None:days[day]['t'][k].append(f(a[i]))
            for k in pk:
                a=x.get(k) or []
                if i<len(a) and f(a[i]) is not None:days[day]['p'][k]+=f(a[i])
        daily={}
        for day,v in days.items():
            highs=[max(a) for a in v['t'].values() if a];rain=list(v['p'].values())
            if highs:daily[day]={'member_highs':highs,'member_precip_totals':rain,'temperature_mean':statistics.mean(highs),'temperature_median':statistics.median(highs)}
        out[l['location_key']]={'daily':daily,'temperature_member_count':len(tk),'precipitation_member_count':len(pk),'model_run':d.get('model_run') or d.get('model_run_id') or d.get('model_run_time')}
    return out

def create_research(l,date,var,m,prevp,curp,preask,eventask,scan,observed,stats):
    ch=curp-prevp
    if abs(ch)<RESEARCH_MIN_FORECAST_CHANGE_POINTS:return
    side='YES' if ch>=0 else 'NO';prev_side=prevp if side=='YES' else 100-prevp;cur_side=curp if side=='YES' else 100-curp;side_ch=ch if side=='YES' else -ch;market_ch=eventask-preask;lag=side_ch-market_ch;edge=cur_side-eventask;fp=h({'city':l['city_name'],'date':date,'var':var,'ticker':m.get('ticker'),'forecast':curp,'previous':prevp})[:32]
    r=q('''INSERT INTO forecast_research_events(event_fingerprint,created_at,city,forecast_date,variable,market_ticker,side,previous_probability,current_probability,forecast_probability_change_points,pre_forecast_ask_cents,event_ask_cents,initial_market_change_points,initial_market_lag_points,initial_preliminary_edge_points,status,scan_id,forecast_observed_at,measurement_version,settlement_verified,location_key) VALUES(%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(event_fingerprint) DO NOTHING RETURNING id''',(fp,l['city_name'],date,var,m.get('ticker',''),side,prev_side,cur_side,side_ch,preask,eventask,market_ch,lag,edge,'open' if lag>0 else 'no_initial_lag',scan,observed,MEASUREMENT_VERSION,l['settlement_verified'],l['location_key']),one=True)
    if r:stats['research_events_created']+=1

def process_research(cache,ens,before,scan,observed,stats):
    for kind,keyvar,probfun in [('temperature','ensemble_temperature_distribution',tprob),('rain','ensemble_rain_distribution',rprob)]:
        for s,l,ms in cache[kind]:
            for m in ms:
                date=date_market(m)
                if not date:continue
                d=ens.get(l['location_key'],{}).get('daily',{}).get(date)
                if not d:continue
                curvals=d.get('member_highs' if kind=='temperature' else 'member_precip_totals') or []
                prev=forecast_prev(l['city_name'],keyvar,date,before)
                if not prev:stats['research_missing_previous_forecast']+=1;continue
                prevvals=prev.get('member_highs' if kind=='temperature' else 'member_precip_totals') or []
                cp=probfun(curvals,m) if kind=='temperature' else probfun(curvals);pp=probfun(prevvals,m) if kind=='temperature' else probfun(prevvals)
                if cp is None or pp is None:continue
                stats['research_current_forecasts']+=1;stats['research_previous_forecasts']+=1
                pm=prior_market(m.get('ticker',''),before);em=event_market(m.get('ticker',''),scan)
                if not pm or not em:stats['research_missing_previous_market']+=1;continue
                pa=pm[2] if cp>=pp else pm[4];ea=em[2] if cp>=pp else em[4]
                if pa is not None and ea is not None:create_research(l,date,'temperature' if kind=='temperature' else 'precipitation',m,pp,cp,pa,ea,scan,observed,stats)

def candidate(l,date,m,cp,pp,pm,em,kind='temperature'):
    if cp is None or pp is None or not pm or not em or abs(cp-pp)<MIN_FORECAST_PROBABILITY_CHANGE_POINTS:return None
    best=None;ch=cp-pp
    for side in ('YES','NO'):
        ask=em[2] if side=='YES' else em[4];prev=pm[2] if side=='YES' else pm[4]
        if ask is None or prev is None or not MIN_ENTRY_PRICE_CENTS<=ask<=MAX_ENTRY_PRICE_CENTS:continue
        sp=cp if side=='YES' else 100-cp;sch=ch if side=='YES' else -ch;mc=ask-prev;lag=sch-mc;edge=sp-ask
        if lag<MIN_MARKET_LAG_POINTS or edge<MIN_PRELIMINARY_EDGE_POINTS:continue
        z={'city':l['city_name'],'forecast_date':date,'market_ticker':m.get('ticker',''),'market_kind':kind,'side':side,'entry_price_cents':ask,'model_probability_proxy':sp,'preliminary_edge_points':edge,'forecast_probability_change_points':sch,'market_price_change_points':mc,'market_lag_points':lag,'forecast_temperature_change_f':None}
        if best is None or (z['market_lag_points'],z['preliminary_edge_points'])>(best['market_lag_points'],best['preliminary_edge_points']):best=z
    return best

def paper(signal,reason,stats):
    fp=h(signal)[:24]
    if q('SELECT 1 FROM paper_trades WHERE signal_fingerprint=%s',(fp,),one=True):return
    entry=signal['entry_price_cents']/100;contracts=max(1,int(PAPER_RISK_DOLLARS/entry));stake=contracts*entry
    q('''INSERT INTO paper_trades(signal_fingerprint,created_at,city,forecast_date,market_ticker,market_kind,side,entry_price_cents,stake_dollars,contracts,model_probability_proxy,preliminary_edge_points,forecast_probability_change_points,market_price_change_points,market_lag_points,forecast_temperature_change_f,reason,status) VALUES(%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open') ON CONFLICT(signal_fingerprint) DO NOTHING''',(fp,signal['city'],signal['forecast_date'],signal['market_ticker'],signal['market_kind'],signal['side'],signal['entry_price_cents'],stake,contracts,signal['model_probability_proxy'],signal['preliminary_edge_points'],signal['forecast_probability_change_points'],signal['market_price_change_points'],signal['market_lag_points'],None,Json(reason)));stats['paper_trades_created']+=1
    if not q('SELECT 1 FROM alert_log WHERE fingerprint=%s',(fp,),one=True):
        msg=f"🌦️ **{signal['market_kind'].upper()} FORECAST SHOCK — PAPER TRADE**\n\n**{signal['city']} — {signal['forecast_date']}**\nMarket: `{signal['market_ticker']}`\nSide: **{signal['side']}**\nEntry ask: **{signal['entry_price_cents']:.1f}¢**\n\nEnsemble probability proxy: **{signal['model_probability_proxy']:.1f}%**\nForecast change: **{signal['forecast_probability_change_points']:+.1f} pts**\nMarket change: **{signal['market_price_change_points']:+.1f} pts**\nEstimated lag: **{signal['market_lag_points']:+.1f} pts**\nPreliminary edge: **{signal['preliminary_edge_points']:+.1f} pts**\n\n⚠️ Research only; verify the individual Kalshi market rules before real trading."
        try:
            r=requests.post(DISCORD_RELAY_URL,json={'secret':DISCORD_RELAY_SECRET,'message':msg},headers={'User-Agent':'WeatherKalshiResearchBot/7.0'},timeout=REQUEST_TIMEOUT) if DISCORD_RELAY_URL and DISCORD_RELAY_SECRET else None
            if r is not None and 200<=r.status_code<300:q('INSERT INTO alert_log(fingerprint,sent_at,payload) VALUES(%s,NOW(),%s) ON CONFLICT DO NOTHING',(fp,Json(signal)));stats['discord_alerts']+=1
        except Exception as e:log.error('Discord relay failed: %s',e)

def settle(stats):
    for tid,ticker,side,stake,contracts in q("SELECT id,market_ticker,side,stake_dollars,contracts FROM paper_trades WHERE status='open' LIMIT 200",fetch=True) or []:
        try:
            m=http(f'{KALSHI_API_URL}/markets/{ticker}',tries=1).get('market',{});res=(m.get('result') or '').lower()
            if res not in {'yes','no'}:continue
            pnl=contracts-stake if res==side.lower() else -stake;q('UPDATE paper_trades SET settled_at=NOW(),result=%s,profit_loss_dollars=%s,status=\'settled\' WHERE id=%s',(res,pnl,tid));stats['settled_trades']+=1
        except Exception as e:log.warning('Could not settle %s: %s',ticker,e)

def observe(stats,before):
    rows=q("SELECT id,market_ticker,side,created_at,event_ask_cents,initial_market_lag_points,latest_observation_at,max_market_move_points FROM forecast_research_events WHERE measurement_version=%s AND status='open' AND created_at<%s LIMIT 1000",(MEASUREMENT_VERSION,before),fetch=True) or []
    for eid,ticker,side,created,event,lag,last,maxmove in rows:
        r=q("SELECT observed_at,CASE WHEN %s='YES' THEN yes_ask_cents ELSE no_ask_cents END FROM market_snapshots WHERE ticker=%s AND snapshot_phase='scan_start' AND measurement_version=%s AND observed_at>%s AND observed_at<%s ORDER BY observed_at LIMIT 1",(side,ticker,MEASUREMENT_VERSION,last or created,before),one=True)
        if not r or r[1] is None:continue
        move=r[1]-event;frac=move/lag if lag and lag>0 else 0;remaining=lag-move if lag is not None else None
        q('''UPDATE forecast_research_events SET latest_observation_at=%s,latest_ask_cents=%s,latest_market_move_points=%s,latest_lag_remaining_points=%s,max_market_move_points=%s,first_response_at=CASE WHEN first_response_at IS NULL AND %s>0 THEN %s ELSE first_response_at END,milestone_25_at=CASE WHEN milestone_25_at IS NULL AND %s>0 AND %s>=initial_market_lag_points*.25 THEN %s ELSE milestone_25_at END,milestone_50_at=CASE WHEN milestone_50_at IS NULL AND %s>0 AND %s>=initial_market_lag_points*.50 THEN %s ELSE milestone_50_at END,milestone_75_at=CASE WHEN milestone_75_at IS NULL AND %s>0 AND %s>=initial_market_lag_points*.75 THEN %s ELSE milestone_75_at END,milestone_90_at=CASE WHEN milestone_90_at IS NULL AND %s>0 AND %s>=initial_market_lag_points*.90 THEN %s ELSE milestone_90_at END WHERE id=%s''',(r[0],r[1],move,remaining,max(maxmove or 0,move),move,r[0],move,move,r[0],move,move,r[0],move,move,r[0],eid));q('INSERT INTO forecast_research_updates(event_id,observed_at,market_ask_cents,market_move_points,lag_remaining_points,market_response_fraction) VALUES(%s,%s,%s,%s,%s,%s)',(eid,r[0],r[1],move,remaining,frac));stats['research_events_observed']+=1

def run_scan():
    scan=None;stats={'schema_version':SCHEMA_VERSION,'measurement_version':MEASUREMENT_VERSION,'temperature_series':0,'rain_series':0,'temperature_markets':0,'rain_markets':0,'weather_refreshed':False,'forecast_shocks':0,'paper_trades_created':0,'discord_alerts':0,'settled_trades':0,'deterministic_gfs_ok':False,'ensemble_ok':False,'rain_forecast_shocks':0,'research_events_created':0,'research_events_observed':0,'research_markets_considered':0,'research_current_forecasts':0,'research_previous_forecasts':0,'research_missing_previous_forecast':0,'research_missing_previous_market':0,'nws_updates_detected':0}
    started=now();
    try:
        schema();scan=q("INSERT INTO scan_runs(started_at,status,stats,schema_version) VALUES(NOW(),'running','{}'::jsonb,%s) RETURNING id",(SCHEMA_VERSION,),one=True)[0];settle(stats)
        xs=series_list();save_series(xs);te,re=discover(xs);stats['temperature_series']=len(te);stats['rain_series']=len(re);stats['weather_locations_discovered']=q('SELECT COUNT(*) FROM weather_locations',one=True)[0]
        c={'temperature':cache(te),'rain':cache(re)};snapshot(c,scan,'scan_start',stats)
        locs={l['location_key']:l for _,l in te+re};us,ch=nws_updates(list(locs.values()));stats['nws_updates_detected']=len(ch)
        if ch:
            stats['weather_refreshed']=True
            try:det=fetch_det(list(locs.values()));stats['deterministic_gfs_ok']=True
            except Exception as e:log.warning('Deterministic GFS unavailable: %s',e);det={}
            ens=fetch_ens(list(locs.values()));stats['ensemble_ok']=True;observed=now();snapshot(c,scan,'forecast_event',stats)
            process_research(c,ens,started,scan,observed,stats)
            for kind,entries in [('temperature',c['temperature']),('rain',c['rain'])]:
                for s,l,ms in entries:
                    if not l['settlement_verified'] and not ALLOW_UNVERIFIED_LOCATION_SIGNALS:continue
                    for m in ms:
                        date=date_market(m);d=ens.get(l['location_key'],{}).get('daily',{}).get(date or '')
                        if not d:continue
                        cur=d.get('member_highs' if kind=='temperature' else 'member_precip_totals') or [];prev=forecast_prev(l['city_name'],'ensemble_temperature_distribution' if kind=='temperature' else 'ensemble_rain_distribution',date,started)
                        if not prev:continue
                        old=prev.get('member_highs' if kind=='temperature' else 'member_precip_totals') or [];cp=tprob(cur,m) if kind=='temperature' else rprob(cur);pp=tprob(old,m) if kind=='temperature' else rprob(old);pm=prior_market(m.get('ticker',''),started);em=event_market(m.get('ticker',''),scan);sig=candidate(l,date,m,cp,pp,pm,em,kind='temperature' if kind=='temperature' else 'precipitation')
                        if sig:paper(sig,{'measurement_version':MEASUREMENT_VERSION,'settlement_verified':l['settlement_verified'],'ensemble_model':ENSEMBLE_MODEL},stats)
            save_forecasts(det,ens,observed);save_nws(us,locs)
        observe(stats,started);settle(stats)
        q('UPDATE scan_runs SET completed_at=NOW(),status=\'success\',stats=%s,schema_version=%s WHERE id=%s',(Json(stats),SCHEMA_VERSION,scan));log.info('SCAN COMPLETE | %s | runtime=%.1fs',json.dumps(stats,default=str), (now()-started).total_seconds())
    except Exception as e:
        log.exception('SCAN FAILED')
        if scan:
            q('UPDATE scan_runs SET completed_at=NOW(),status=\'failed\',stats=%s,error=%s,schema_version=%s WHERE id=%s',(Json(stats),str(e),SCHEMA_VERSION,scan))
        raise
    finally:close_db()

if __name__=='__main__':run_scan()
