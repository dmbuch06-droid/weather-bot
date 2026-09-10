import hashlib, json, logging, os, re, statistics, time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo
import requests
try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    psycopg2 = None
    Json = None

def J(value):
    return Json(value, dumps=lambda obj: json.dumps(obj, default=str)) if Json is not None else value

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
SCHEMA_VERSION=10; MEASUREMENT_VERSION='v10_incremental_forecast_tracking'
MIN_FORECAST_PROBABILITY_CHANGE_POINTS=float(os.environ.get('MIN_FORECAST_PROBABILITY_CHANGE_POINTS','20'))
MIN_MARKET_LAG_POINTS=float(os.environ.get('MIN_MARKET_LAG_POINTS','10'))
MIN_PRELIMINARY_EDGE_POINTS=float(os.environ.get('MIN_PRELIMINARY_EDGE_POINTS','10'))
MIN_ENTRY_PRICE_CENTS=float(os.environ.get('MIN_ENTRY_PRICE_CENTS','5'))
MAX_ENTRY_PRICE_CENTS=float(os.environ.get('MAX_ENTRY_PRICE_CENTS','95'))
PAPER_RISK_DOLLARS=float(os.environ.get('PAPER_RISK_DOLLARS','10'))
RESEARCH_MIN_FORECAST_CHANGE_POINTS=float(os.environ.get('RESEARCH_MIN_FORECAST_CHANGE_POINTS','3'))
MIN_ENSEMBLE_MEMBERS=int(os.environ.get('MIN_ENSEMBLE_MEMBERS','20'))
ALLOW_UNVERIFIED_LOCATION_SIGNALS=os.environ.get('ALLOW_UNVERIFIED_LOCATION_SIGNALS','true').lower() in {'1','true','yes'}
ALLOW_RAIN_PAPER_SIGNALS=os.environ.get('ALLOW_RAIN_PAPER_SIGNALS','false').lower() in {'1','true','yes'}
REQUIRE_NWS_CONFIRMATION=os.environ.get('REQUIRE_NWS_CONFIRMATION','true').lower() in {'1','true','yes'}
KNOWN={
    'NYC':('New York City',40.7789,-73.9692,'America/New_York',True),
    'CHI':('Chicago',41.9742,-87.9073,'America/Chicago',False),
    'MIA':('Miami',25.7959,-80.2870,'America/New_York',False),
    'AUS':('Austin',30.1975,-97.6663,'America/Chicago',False),
    'DC':('Washington',38.9072,-77.0369,'America/New_York',False),
    'DEN':('Denver',39.7392,-104.9903,'America/Denver',False),
    'PHIL':('Philadelphia',39.9526,-75.1652,'America/New_York',False),
    'LAX':('Los Angeles',34.0522,-118.2437,'America/Los_Angeles',False),
    'SFO':('San Francisco',37.7749,-122.4194,'America/Los_Angeles',False),
    'SEA':('Seattle',47.6062,-122.3321,'America/Los_Angeles',False),
    'DAL':('Dallas',32.7767,-96.7970,'America/Chicago',False),
    'PHX':('Phoenix',33.4484,-112.0740,'America/Phoenix',False),
    'ATL':('Atlanta',33.7490,-84.3880,'America/New_York',False),
    'BOS':('Boston',42.3601,-71.0589,'America/New_York',False),
    'HOU':('Houston',29.7604,-95.3698,'America/Chicago',False),
}

logging.basicConfig(level=logging.INFO,format='%(asctime)s | %(levelname)s | %(message)s'); log=logging.getLogger('weather-kalshi-scanner'); _DB_CONN=None

def now(): return datetime.now(timezone.utc)
def f(v,d=None):
    try:return float(v) if v is not None else d
    except:return d
def h(x): return hashlib.sha256(json.dumps(x,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
def slug(s): return re.sub(r'[^A-Z0-9]+','_',s.upper()).strip('_')[:48] or 'UNKNOWN'
def local_date(ts,tz): return datetime.fromisoformat(str(ts).replace('Z','+00:00')).astimezone(ZoneInfo(tz)).date().isoformat()
def round_temp(v):
    v=f(v)
    if v is None:return None
    return int(Decimal(str(v)).quantize(Decimal('1'),rounding=ROUND_HALF_UP))

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
        # Read-only SELECTs do not need a commit. Avoiding a commit on every
        # lookup is important because discovery/research performs many small
        # reads per scan. Writes/DDL still commit immediately, preserving the
        # existing error/transaction behavior.
        if not sql.lstrip().upper().startswith('SELECT'):
            _DB_CONN.commit()
        return r
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
'''CREATE TABLE IF NOT EXISTS weather_service_state(city_code TEXT PRIMARY KEY,source TEXT NOT NULL,last_update_at TIMESTAMPTZ,checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW())''',
'''CREATE TABLE IF NOT EXISTS forecast_observations(id BIGSERIAL PRIMARY KEY,observed_at TIMESTAMPTZ NOT NULL,city TEXT NOT NULL,variable TEXT NOT NULL,model TEXT NOT NULL,forecast_date DATE NOT NULL,scalar_value DOUBLE PRECISION,payload JSONB NOT NULL,payload_hash TEXT NOT NULL,UNIQUE(city,variable,model,forecast_date,payload_hash))''',
'''CREATE TABLE IF NOT EXISTS market_snapshots(id BIGSERIAL PRIMARY KEY,observed_at TIMESTAMPTZ NOT NULL,ticker TEXT NOT NULL,event_ticker TEXT,series_ticker TEXT,market_date DATE,city TEXT,market_kind TEXT NOT NULL,strike_type TEXT,floor_strike DOUBLE PRECISION,cap_strike DOUBLE PRECISION,yes_bid_cents DOUBLE PRECISION,yes_ask_cents DOUBLE PRECISION,no_bid_cents DOUBLE PRECISION,no_ask_cents DOUBLE PRECISION,last_price_cents DOUBLE PRECISION,status TEXT,result TEXT)''',
'''CREATE TABLE IF NOT EXISTS paper_trades(id BIGSERIAL PRIMARY KEY,signal_fingerprint TEXT UNIQUE NOT NULL,created_at TIMESTAMPTZ NOT NULL,settled_at TIMESTAMPTZ,city TEXT NOT NULL,forecast_date DATE NOT NULL,market_ticker TEXT NOT NULL,market_kind TEXT NOT NULL,side TEXT NOT NULL,entry_price_cents DOUBLE PRECISION NOT NULL,stake_dollars DOUBLE PRECISION NOT NULL,contracts DOUBLE PRECISION NOT NULL,model_probability_proxy DOUBLE PRECISION NOT NULL,preliminary_edge_points DOUBLE PRECISION NOT NULL,forecast_probability_change_points DOUBLE PRECISION NOT NULL,market_price_change_points DOUBLE PRECISION NOT NULL,market_lag_points DOUBLE PRECISION NOT NULL,forecast_temperature_change_f DOUBLE PRECISION,reason JSONB NOT NULL,result TEXT,profit_loss_dollars DOUBLE PRECISION,status TEXT NOT NULL DEFAULT 'open')''',
'''CREATE TABLE IF NOT EXISTS alert_log(fingerprint TEXT PRIMARY KEY,sent_at TIMESTAMPTZ NOT NULL,payload JSONB NOT NULL)''',
'''CREATE TABLE IF NOT EXISTS forecast_research_events(id BIGSERIAL PRIMARY KEY,event_fingerprint TEXT UNIQUE NOT NULL,created_at TIMESTAMPTZ NOT NULL,city TEXT NOT NULL,forecast_date DATE NOT NULL,variable TEXT NOT NULL,market_ticker TEXT NOT NULL,side TEXT NOT NULL,previous_probability DOUBLE PRECISION NOT NULL,current_probability DOUBLE PRECISION NOT NULL,forecast_probability_change_points DOUBLE PRECISION NOT NULL,pre_forecast_ask_cents DOUBLE PRECISION,event_ask_cents DOUBLE PRECISION,initial_market_change_points DOUBLE PRECISION,initial_market_lag_points DOUBLE PRECISION,initial_preliminary_edge_points DOUBLE PRECISION,first_response_at TIMESTAMPTZ,milestone_25_at TIMESTAMPTZ,milestone_50_at TIMESTAMPTZ,milestone_75_at TIMESTAMPTZ,milestone_90_at TIMESTAMPTZ,latest_observation_at TIMESTAMPTZ,latest_ask_cents DOUBLE PRECISION,latest_market_move_points DOUBLE PRECISION,latest_lag_remaining_points DOUBLE PRECISION,max_market_move_points DOUBLE PRECISION DEFAULT 0,status TEXT NOT NULL DEFAULT 'open',closed_at TIMESTAMPTZ,settlement_result TEXT)''',
'''CREATE TABLE IF NOT EXISTS forecast_research_updates(id BIGSERIAL PRIMARY KEY,event_id BIGINT NOT NULL REFERENCES forecast_research_events(id) ON DELETE CASCADE,observed_at TIMESTAMPTZ NOT NULL,market_ask_cents DOUBLE PRECISION,market_move_points DOUBLE PRECISION,lag_remaining_points DOUBLE PRECISION,market_response_fraction DOUBLE PRECISION,scan_id BIGINT,ticker TEXT,event_ticker TEXT,series_ticker TEXT,yes_bid_cents DOUBLE PRECISION,yes_ask_cents DOUBLE PRECISION,no_bid_cents DOUBLE PRECISION,no_ask_cents DOUBLE PRECISION,last_price_cents DOUBLE PRECISION,spread_yes_cents DOUBLE PRECISION,spread_no_cents DOUBLE PRECISION,volume DOUBLE PRECISION,open_interest DOUBLE PRECISION)''',
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
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS ticker TEXT',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS event_ticker TEXT',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS series_ticker TEXT',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS yes_bid_cents DOUBLE PRECISION',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS yes_ask_cents DOUBLE PRECISION',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS no_bid_cents DOUBLE PRECISION',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS no_ask_cents DOUBLE PRECISION',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS last_price_cents DOUBLE PRECISION',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS spread_yes_cents DOUBLE PRECISION',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS spread_no_cents DOUBLE PRECISION',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS volume DOUBLE PRECISION',
        'ALTER TABLE forecast_research_updates ADD COLUMN IF NOT EXISTS open_interest DOUBLE PRECISION',
        'ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS measurement_version TEXT',
        'ALTER TABLE alert_log ADD COLUMN IF NOT EXISTS measurement_version TEXT',
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
            cur.executemany('''INSERT INTO series_registry(series_ticker,title,category,tags,settlement_sources,contract_terms_url,updated_at,raw_series) VALUES(%s,%s,%s,%s,%s,%s,NOW(),%s) ON CONFLICT(series_ticker) DO UPDATE SET title=EXCLUDED.title,category=EXCLUDED.category,tags=EXCLUDED.tags,settlement_sources=EXCLUDED.settlement_sources,contract_terms_url=EXCLUDED.contract_terms_url,updated_at=NOW(),raw_series=EXCLUDED.raw_series''',[(x.get('ticker'),x.get('title',''),x.get('category'),J(x.get('tags') or []),J(x.get('settlement_sources') or []),x.get('contract_terms_url'),J(x)) for x in xs if x.get('ticker')])
        c.commit()
    except: c.rollback();raise
    finally:c.close()

def is_temp(s):
    t=(s.get('title') or '').lower();x=(s.get('ticker') or '').upper();freq=(s.get('frequency') or '').lower()
    return (not freq or freq=='daily') and 'lowest temperature' not in t and (x.startswith('KXHIGH') or 'highest temperature' in t or 'high temperature' in t or 'maximum temperature' in t)
def is_rain(s):
    t=(s.get('title') or '').lower();x=(s.get('ticker') or '').upper();return x=='KXRAIN' or ((s.get('frequency') or '').lower()=='daily' and ('rain' in t or 'precipitation' in t))
_TITLE_BOILERPLATE={'daily','maximum','max','high','highest','temperature','temp','rain','precipitation','will','it','where','the','today','on','in','weather','daly'}
_CITY_ABBREV={'DC':'Washington','NYC':'New York City','SATX':'San Antonio','LV':'Las Vegas','SF':'San Francisco','LA':'Los Angeles'}
def city_title(s):
    t=' '.join((s.get('title') or '').split())
    if not t:return None
    # Drop parenthetical airport codes, e.g. "Newark, NJ (EWR) Daily Max Temp".
    t=' '.join(re.sub(r'\([^)]*\)','',t).split())
    # Prefer the classic "<temperature|rain|precipitation> in <city>" phrasing when present.
    m=re.search(r'(?:temperature|rain|precipitation)\s+in\s+(.+?)(?:\s+today\??|\s+on\s+.+?\??$|\?$|$)',t,re.I)
    if m:
        cand=m.group(1).strip(' ?.,')
        if cand:return _CITY_ABBREV.get(cand.upper(),cand)
    # Otherwise strip known boilerplate tokens from both ends of the title.
    # Kalshi's real weather-market titles are inconsistently ordered
    # ("Seattle Maximum Temperature Daily", "Daily high temp Tokyo", "NYC rain")
    # so requiring the word "in" (the old behavior) missed almost all of them.
    tokens=[tok for tok in t.replace('-',' ').replace('?','').split(' ') if tok]
    lo=0;hi=len(tokens)
    while lo<hi and tokens[lo].strip('.,').lower() in _TITLE_BOILERPLATE:lo+=1
    while hi>lo and tokens[hi-1].strip('.,').lower() in _TITLE_BOILERPLATE:hi-=1
    cand=' '.join(tokens[lo:hi]).strip(' ,.')
    # Drop a trailing ", ST" state abbreviation for geocoding, e.g. "Trenton, NJ" -> "Trenton".
    cand=re.split(r',',cand)[0].strip()
    if not cand:return None
    return _CITY_ABBREV.get(cand.upper(),cand)

def geocode(city):
    d=http(GEOCODING_API_URL,{'name':city,'count':5,'language':'en','format':'json'})
    rs=[r for r in d.get('results',[]) if r.get('latitude') is not None and r.get('longitude') is not None and r.get('timezone')]
    if not rs:return None
    norm=re.sub(r'[^a-z0-9]','',city.lower())
    # Prefer an exact name match, then the largest population among candidates
    # (disambiguates e.g. "Paris" the metropolis from small same-named towns).
    rs.sort(key=lambda r:(0 if re.sub(r'[^a-z0-9]','',str(r.get('name','')).lower())==norm else 1,-(r.get('population') or 0)))
    return rs[0]

# Kalshi's international weather tickers embed the ICAO airport code for the
# settlement station (e.g. KXHIGHTRJTT -> RJTT = Tokyo Haneda). This is exact
# and unambiguous, unlike free-text geocoding "Tokyo"/"Dubai"/"Hong Kong" -
# which either returns no US result (our geocoder was US-only) or the wrong
# same-named place. Coordinates are airport-level (fine for model queries).
ICAO_LOCATIONS={
    'RJTT':('Tokyo',35.5494,139.7798,'Asia/Tokyo'),
    'ZBAA':('Beijing',40.0801,116.5846,'Asia/Shanghai'),
    'VHHH':('Hong Kong',22.3080,113.9185,'Asia/Hong_Kong'),
    'ZSPD':('Shanghai',31.1443,121.8083,'Asia/Shanghai'),
    'RKSI':('Seoul',37.4602,126.4407,'Asia/Seoul'),
    'VABB':('Mumbai',19.0896,72.8656,'Asia/Kolkata'),
    'YSSY':('Sydney',-33.9399,151.1753,'Australia/Sydney'),
    'CYYZ':('Toronto',43.6777,-79.6248,'America/Toronto'),
    'EGLL':('London',51.4700,-0.4543,'Europe/London'),
    'LFPG':('Paris',49.0097,2.5479,'Europe/Paris'),
    'EDDB':('Berlin',52.3667,13.5033,'Europe/Berlin'),
    'EDDF':('Frankfurt',50.0379,8.5622,'Europe/Berlin'),
    'EHAM':('Amsterdam',52.3086,4.7639,'Europe/Amsterdam'),
    'EBBR':('Brussels',50.9014,4.4844,'Europe/Brussels'),
    'LSGG':('Geneva',46.2381,6.1089,'Europe/Zurich'),
    'LTFM':('Istanbul',41.2753,28.7519,'Europe/Istanbul'),
    'OMDB':('Dubai',25.2532,55.3657,'Asia/Dubai'),
    'MMMX':('Mexico City',19.4363,-99.0721,'America/Mexico_City'),
    'SBGR':('Sao Paulo',-23.4356,-46.4731,'America/Sao_Paulo'),
    'WSSS':('Singapore',1.3644,103.9915,'Asia/Singapore'),
}
def icao_loc_for(ticker):
    x=(ticker or '').upper()
    for code,(city,lat,lon,tz) in ICAO_LOCATIONS.items():
        if code in x:
            k=slug(city)
            r=getloc(k)
            if r:return r
            q('''INSERT INTO weather_locations(location_key,city_name,latitude,longitude,timezone,mapping_method,source_series_tickers) VALUES(%s,%s,%s,%s,%s,'icao_lookup',jsonb_build_array(%s)) ON CONFLICT(location_key) DO NOTHING''',(k,city,lat,lon,tz,ticker))
            return getloc(k)
    return None

def loc_for(s):
    """Allow only the fixed 15 major U.S. cities; never dynamically geocode others."""
    x=(s.get('ticker') or '').upper()
    title=(s.get('title') or s.get('subtitle') or '').strip().lower()
    ticker_map={
        'KXHIGHNY':'NYC','HIGHNY':'NYC','KXHIGHCHI':'CHI','HIGHCHI':'CHI',
        'KXHIGHMIA':'MIA','HIGHMIA':'MIA','KXHIGHAUS':'AUS','HIGHAUS':'AUS',
        'KXHIGHTDC':'DC','KXHIGHDC':'DC','HIGHTDC':'DC','HIGHDC':'DC',
        'KXHIGHDEN':'DEN','HIGHDEN':'DEN','KXHIGHPHIL':'PHIL','HIGHPHIL':'PHIL',
        'KXHIGHLAX':'LAX','HIGHLAX':'LAX','KXHIGHTSFO':'SFO','HIGHTSFO':'SFO',
        'KXHIGHSEA':'SEA','HIGHSEA':'SEA','KXHIGHDAL':'DAL','HIGHDAL':'DAL',
        'KXHIGHPHX':'PHX','HIGHPHX':'PHX','KXHIGHATL':'ATL','HIGHATL':'ATL',
        'KXHIGHBOS':'BOS','HIGHBOS':'BOS','KXHIGHHOU':'HOU','HIGHHOU':'HOU'}
    aliases={
        'new york city':'NYC','new york':'NYC','chicago':'CHI','miami':'MIA',
        'austin':'AUS','washington dc':'DC','washington, dc':'DC','washington':'DC',
        'denver':'DEN','philadelphia':'PHIL','los angeles':'LAX',
        'san francisco':'SFO','seattle':'SEA','dallas':'DAL','phoenix':'PHX',
        'atlanta':'ATL','boston':'BOS','houston':'HOU'}
    key=next((k for p,k in ticker_map.items() if x.startswith(p)),None)
    if key is None:
        for city in sorted(aliases,key=len,reverse=True):
            if re.search(r'(?<![a-z])'+re.escape(city)+r'(?![a-z])',title):
                key=aliases[city]
                break
    if key is None:
        return None
    r=q('SELECT location_key,city_name,latitude,longitude,timezone,settlement_verified,signal_enabled,mapping_method,nws_grid_url FROM weather_locations WHERE location_key=%s',(key,),one=True)
    return rowloc(r) if r else None



def _loc_for_inner(s):
    x=(s.get('ticker') or '').upper();mp={'KXHIGHNY':'NYC','HIGHNY':'NYC','KXHIGHCHI':'CHI','HIGHCHI':'CHI','KXHIGHMIA':'MIA','HIGHMIA':'MIA','KXHIGHAUS':'AUS','HIGHAUS':'AUS'}
    for p,k in mp.items():
        if p in x:
            r=q('SELECT location_key,city_name,latitude,longitude,timezone,settlement_verified,signal_enabled,mapping_method,nws_grid_url FROM weather_locations WHERE location_key=%s',(k,),one=True);return rowloc(r) if r else None
    icao=icao_loc_for(x)
    if icao:return icao
    city=city_title(s)
    if not city:
        log.warning('MAPPING SKIP | ticker=%s | reason=no_city_parsed_from_title | title=%r',s.get('ticker'),s.get('title'))
        return None
    r=q('SELECT location_key,city_name,latitude,longitude,timezone,settlement_verified,signal_enabled,mapping_method,nws_grid_url FROM weather_locations WHERE lower(city_name)=lower(%s) LIMIT 1',(city,),one=True)
    if r:return rowloc(r)
    try:
        g=geocode(city)
    except Exception as e:
        log.warning('MAPPING SKIP | ticker=%s | reason=geocode_request_failed | parsed_city=%r | error=%s',s.get('ticker'),city,e)
        return None
    if not g:
        log.warning('MAPPING SKIP | ticker=%s | reason=geocode_no_match | parsed_city=%r',s.get('ticker'),city)
        return None
    feature=str(g.get('feature_code') or '').upper()
    if not feature.startswith('PPL'):
        log.warning('MAPPING SKIP | ticker=%s | reason=non_populated_place | parsed_city=%r | geocode_name=%r | feature_code=%s', s.get('ticker'), city, g.get('name'), feature)
        return None
    k=slug('_'.join(x for x in [g.get('name'),g.get('admin1'),g.get('country_code')] if x))
    q('''INSERT INTO weather_locations(location_key,city_name,latitude,longitude,timezone,mapping_method,source_series_tickers,raw_geocode) VALUES(%s,%s,%s,%s,%s,'open_meteo_geocoding',jsonb_build_array(%s),%s) ON CONFLICT(location_key) DO NOTHING''',(k,g['name'],g['latitude'],g['longitude'],g['timezone'],s.get('ticker',''),J(g)))
    return getloc(k)
def rowloc(r):
    return {'location_key':r[0],'city_name':r[1],'latitude':r[2],'longitude':r[3],'timezone':r[4],'settlement_verified':bool(r[5]),'signal_enabled':bool(r[6]),'mapping_method':r[7],'nws_grid_url':r[8]}
def getloc(k):
    r=q('SELECT location_key,city_name,latitude,longitude,timezone,settlement_verified,signal_enabled,mapping_method,nws_grid_url FROM weather_locations WHERE location_key=%s',(k,),one=True);return rowloc(r) if r else None

def discover(xs):
    temps=[];rains=[];seen_t=set();seen_r=set();skipped_temp=0;skipped_rain=0
    for s in xs:
        if is_temp(s):
            l=loc_for(s)
            if l:temps.append((s,l))
            else:skipped_temp+=1
        elif is_rain(s):
            l=loc_for(s)
            if l:rains.append((s,l))
            else:skipped_rain+=1
    if skipped_temp or skipped_rain:
        log.warning('MAPPING SUMMARY | temperature_series_skipped=%d | rain_series_skipped=%d (see MAPPING SKIP lines above for which tickers and why)',skipped_temp,skipped_rain)
    by={}
    for s,l in temps:
        by.setdefault(l['location_key'],[]).append((s,l))
    temps=[]
    for k,es in sorted(by.items()):
        es.sort(key=lambda z:(0 if (z[0].get('ticker') or '').upper().startswith('KXHIGH') else 1,z[0].get('ticker') or ''));temps.append(es[0])
    unique_rains=[]
    for e in rains:
        ticker=e[0].get('ticker')
        if ticker and ticker not in seen_r:
            seen_r.add(ticker); unique_rains.append(e)
    rains=unique_rains
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
    log.warning('MAPPING SKIP | ticker=%s | event_ticker=%s | reason=unparseable_date_in_ticker',m.get('ticker'),m.get('event_ticker'))
    return None

def cache(entries): return [(s,l,markets(s.get('ticker'))) for s,l in entries]

def snapshot(cache,scan_id,phase,stats,count_markets=False):
    rows=[]
    for kind,entries in [('temperature',cache['temperature']),('precipitation',cache['rain'])]:
        for s,l,ms in entries:
            for m in ms:
                yb=f(m.get('yes_bid_dollars'));ya=f(m.get('yes_ask_dollars'));nb=f(m.get('no_bid_dollars'));na=f(m.get('no_ask_dollars'))
                rows.append((
                    m.get('ticker',''),m.get('event_ticker'),m.get('series_ticker'),
                    date_market(m),l['city_name'],kind,m.get('strike_type'),
                    f(m.get('floor_strike')),f(m.get('cap_strike')),
                    None if yb is None else yb*100,
                    None if ya is None else ya*100,
                    None if nb is None else nb*100,
                    None if na is None else na*100,
                    None if f(m.get('last_price_dollars')) is None else f(m.get('last_price_dollars'))*100,
                    m.get('status'),m.get('result'),
                    None if yb is None or ya is None else max(0,ya*100-yb*100),
                    None if nb is None or na is None else max(0,na*100-nb*100),
                    f(m.get('volume')),f(m.get('open_interest')),
                    scan_id,phase,MEASUREMENT_VERSION
                ))
                if count_markets:
                    stats['temperature_markets' if kind=='temperature' else 'rain_markets']+=1
    if not rows:
        return
    sql='''INSERT INTO market_snapshots(
        observed_at,ticker,event_ticker,series_ticker,market_date,city,
        market_kind,strike_type,floor_strike,cap_strike,yes_bid_cents,
        yes_ask_cents,no_bid_cents,no_ask_cents,last_price_cents,status,
        result,spread_yes_cents,spread_no_cents,volume,open_interest,
        scan_id,snapshot_phase,measurement_version
    ) VALUES(
        NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s
    )'''
    c=db()
    try:
        with c.cursor() as cur:
            cur.executemany(sql,rows)
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()

def prior_market(ticker,before):return q('SELECT observed_at,yes_bid_cents,yes_ask_cents,no_bid_cents,no_ask_cents,last_price_cents FROM market_snapshots WHERE ticker=%s AND observed_at<%s AND measurement_version=%s ORDER BY observed_at DESC,id DESC LIMIT 1',(ticker,before,MEASUREMENT_VERSION),one=True)
def event_market(ticker,scan):return q('SELECT observed_at,yes_bid_cents,yes_ask_cents,no_bid_cents,no_ask_cents,last_price_cents,spread_yes_cents,spread_no_cents,volume,open_interest,event_ticker,series_ticker FROM market_snapshots WHERE ticker=%s AND scan_id=%s AND snapshot_phase=\'forecast_event\' AND measurement_version=%s ORDER BY observed_at DESC,id DESC LIMIT 1',(ticker,scan,MEASUREMENT_VERSION),one=True)

def contract_label(m):
    st=(m.get('strike_type') or '').lower()
    lo=f(m.get('floor_strike'));hi=f(m.get('cap_strike'))
    if st=='between' and lo is not None and hi is not None:
        low=int(Decimal(str(lo)).to_integral_value(rounding=ROUND_HALF_UP))
        high=int(Decimal(str(hi)).to_integral_value(rounding=ROUND_HALF_UP))
        return f'{low}°–{high}°'
    if st=='greater' and lo is not None:
        threshold=int(Decimal(str(lo)).to_integral_value(rounding=ROUND_HALF_UP))
        return f'{threshold}° or higher'
    if st=='less' and hi is not None:
        threshold=int(Decimal(str(hi)).to_integral_value(rounding=ROUND_HALF_UP))-1
        return f'{threshold}° or lower'
    return 'Temperature contract (see ticker)'

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
    r=q('SELECT payload,observed_at FROM forecast_observations WHERE city=%s AND variable=%s AND model=%s AND forecast_date=%s AND observed_at<%s AND measurement_version=%s ORDER BY observed_at DESC,id DESC LIMIT 1',(city,var,ENSEMBLE_MODEL,date,before,MEASUREMENT_VERSION),one=True)
    return {'payload':r[0],'observed_at':r[1]} if r else None

def forecast_model_run(payload):
    if not isinstance(payload,dict):return None
    return payload.get('model_run') or payload.get('model_run_id') or payload.get('model_run_time')

def forecast_revision_fingerprint(variable,payload):
    if not isinstance(payload,dict):
        return None
    run=forecast_model_run(payload)
    if variable=='ensemble_temperature_distribution':
        vals=payload.get('member_highs_rounded')
        if vals is None:
            vals=[round_temp(v) for v in (payload.get('member_highs') or [])]
        vals=[int(v) for v in vals if v is not None]
        return h({'variable':variable,'model_run':run,'member_highs_rounded':vals})
    if variable=='ensemble_rain_distribution':
        vals=[1 if f(v) is not None and f(v)>0 else 0 for v in (payload.get('member_precip_totals') or [])]
        return h({'variable':variable,'model_run':run,'member_wet_flags':vals})
    return h({'variable':variable,'model_run':run})

def forecast_payload_fingerprint(payload,variable=None):
    if variable:
        return forecast_revision_fingerprint(variable,payload)
    return h(payload) if isinstance(payload,dict) else None

def signal_allowed(l,kind):
    if kind=='rain':
        return bool(ALLOW_RAIN_PAPER_SIGNALS and l.get('settlement_verified') and l.get('signal_enabled'))
    if ALLOW_UNVERIFIED_LOCATION_SIGNALS:
        return True
    return bool(l.get('settlement_verified') and l.get('signal_enabled'))

def save_forecasts(det,ens,observed):
    rows=[]
    for k,d in det.items():
        l=getloc(k)
        if not l:
            continue
        city=l['city_name']
        for date,x in d['daily'].items():
            for var,val,p0 in [('temperature_high',x['high'],x),('precipitation_sum',x['precipitation_sum'],x)]:
                p={**p0,'model_run':d.get('model_run'),'location_key':k}
                rows.append((city,var,DETERMINISTIC_MODEL,date,val,J(p),h(p),observed,MEASUREMENT_VERSION))

    for k,d in ens.items():
        l=getloc(k)
        if not l:
            continue
        city=l['city_name']
        for date,x in d['daily'].items():
            p={
                'member_highs':x['member_highs'],
                'member_highs_rounded':[round_temp(v) for v in x['member_highs']],
                'member_precip_totals':x['member_precip_totals'],
                'temperature_mean':x['temperature_mean'],
                'temperature_median':x['temperature_median'],
                'temperature_member_count':d['temperature_member_count'],
                'precipitation_member_count':d['precipitation_member_count'],
                'model_run':d.get('model_run'),
                'location_key':k
            }
            temp_fp=forecast_revision_fingerprint('ensemble_temperature_distribution',p)
            rows.append((city,'ensemble_temperature_distribution',ENSEMBLE_MODEL,date,x['temperature_mean'],J(p),temp_fp,observed,MEASUREMENT_VERSION))

            if x['member_precip_totals']:
                p={
                    'member_precip_totals':x['member_precip_totals'],
                    'precipitation_member_count':d['precipitation_member_count'],
                    'model_run':d.get('model_run'),
                    'location_key':k
                }
                rain_fp=forecast_revision_fingerprint('ensemble_rain_distribution',p)
                rows.append((city,'ensemble_rain_distribution',ENSEMBLE_MODEL,date,statistics.mean(x['member_precip_totals']),J(p),rain_fp,observed,MEASUREMENT_VERSION))

    if not rows:
        return

    sql = 'INSERT INTO forecast_observations(\n        observed_at,city,variable,model,forecast_date,scalar_value,\n        payload,payload_hash,source_observed_at,measurement_version\n    ) VALUES(NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s)\n    ON CONFLICT(city,variable,model,forecast_date,payload_hash) DO NOTHING'
    c=db()
    try:
        with c.cursor() as cur:
            cur.executemany(sql,rows)
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()

def nws_grid_snapshot(locations):
    """Fetch each location's NWS forecastGridData document exactly once per
    scan and extract everything we need from it: the updateTime (used to
    decide whether to also refresh the deterministic GFS pull) and the
    official temperature/precipitation-probability values (used by
    nws_confirms_direction()). Previously these were two separate HTTP
    fetches of the same URL per location; this merges them into one."""
    update_times={};changed={};grid_values={}
    for l in locations:
        try:
            url=l.get('nws_grid_url')
            if not url:
                p=nws(f"{NWS_API_URL}/points/{l['latitude']},{l['longitude']}");url=(p.get('properties') or {}).get('forecastGridData')
                if url:q('UPDATE weather_locations SET nws_grid_url=%s,updated_at=NOW() WHERE location_key=%s',(url,l['location_key']))
            if not url:continue
            d=nws(url);props=d.get('properties') or {}
            u=props.get('updateTime')
            if u:
                dt=datetime.fromisoformat(u.replace('Z','+00:00'));update_times[l['location_key']]=dt
                r=q("SELECT last_update_at FROM weather_service_state WHERE city_code=%s AND source='nws'",(l['location_key'],),one=True)
                if not r or not r[0] or dt>r[0]:changed[l['location_key']]=dt
            temp_block=(props.get('temperature') or {})
            pop_block=(props.get('probabilityOfPrecipitation') or {})
            temp_uom=str(temp_block.get('uom') or '').lower()
            highs=expand_grid_series(temp_block.get('values'),l['timezone'],'degc' in temp_uom or 'wmounit:degc' in temp_uom,agg='max')
            pops=expand_grid_series(pop_block.get('values'),l['timezone'],False,agg='max')
            if highs:
                grid_values[l['location_key']]={'daily_high_f':highs,'daily_pop_max':pops,'update_time':u}
        except Exception as e:log.warning('NWS grid unavailable for %s: %s',l['city_name'],e)
    return update_times,changed,grid_values
def save_nws(us,locs):
    rows=[]
    for k,u in us.items():
        l=locs.get(k)
        if l:rows.append((k,u))
    if not rows:return
    c=db()
    try:
        with c.cursor() as cur:
            cur.executemany(
                '''INSERT INTO weather_service_state(city_code,source,last_update_at,checked_at)
                   VALUES(%s,'nws',%s,NOW())
                   ON CONFLICT(city_code) DO UPDATE SET
                       source='nws',last_update_at=EXCLUDED.last_update_at,checked_at=NOW()''',
                rows)
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()

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
    p={'latitude':','.join(str(x['latitude']) for x in locs),'longitude':','.join(str(x['longitude']) for x in locs),'models':DETERMINISTIC_MODEL,'hourly':'temperature_2m,precipitation','temperature_unit':'fahrenheit','precipitation_unit':'inch','timezone':'UTC','forecast_days':FORECAST_DAYS}
    d=http('https://api.open-meteo.com/v1/gfs',p);out={}
    for l,x in norm(d,locs):
        daily=hourly(l,x)
        if not daily:raise RuntimeError(f'No deterministic daily temperature data for {l["city_name"]}')
        out[l['location_key']]={'daily':daily,'model_run':x.get('model_run') or x.get('model_run_id') or x.get('model_run_time')}
    return out
def fetch_ens(locs):
    p={'latitude':','.join(str(x['latitude']) for x in locs),'longitude':','.join(str(x['longitude']) for x in locs),'models':ENSEMBLE_MODEL,'hourly':'temperature_2m,precipitation','temperature_unit':'fahrenheit','precipitation_unit':'inch','timezone':'UTC','forecast_days':FORECAST_DAYS};data=http('https://ensemble-api.open-meteo.com/v1/ensemble',p);out={}
    for l,d in norm(data,locs):
        x=d.get('hourly') or {};ts=x.get('time') or [];tk=sorted(k for k in x if k.startswith('temperature_2m_member'));pk=sorted(k for k in x if k.startswith('precipitation_member'));days=defaultdict(lambda:{'t':defaultdict(list),'p':defaultdict(float)})
        if len(tk)<MIN_ENSEMBLE_MEMBERS:raise RuntimeError(f'Only {len(tk)} ensemble temperature members for {l["city_name"]}; expected at least {MIN_ENSEMBLE_MEMBERS}')
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
    missing=[l['city_name'] for l in locs if l['location_key'] not in out or not out[l['location_key']]['daily']]
    if missing:raise RuntimeError('Ensemble missing daily temperature data for: '+', '.join(missing))
    return out

# ---------------------------------------------------------------------------
# Real NWS gridpoint forecast values (not just an update-time trigger).
# forecastGridData returns time-series "values" blocks shaped like:
#   {"validTime": "2026-01-01T06:00:00+00:00/PT6H", "value": 5.0}
# where the second half of validTime is an ISO-8601 duration. We expand each
# block across the local calendar day(s) it covers and take the daily max,
# which is the NWS-forecaster-adjusted counterpart to the raw GFS ensemble
# "member_highs" used elsewhere in this file.
# ---------------------------------------------------------------------------
_ISO_DUR_RE=re.compile(r'^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?)?$')
def parse_iso8601_duration(s):
    m=_ISO_DUR_RE.match(s or '')
    if not m:return None
    days,hrs,mins=(int(g) if g else 0 for g in m.groups())
    from datetime import timedelta
    return timedelta(days=days,hours=hrs,minutes=mins)

def expand_grid_series(values,tz,unit_is_celsius,agg='max'):
    from datetime import timedelta
    by_day=defaultdict(list)
    for entry in values or []:
        vt=entry.get('validTime');val=f(entry.get('value'))
        if not vt or val is None or '/' not in vt:continue
        start_s,dur_s=vt.split('/',1)
        try:start=datetime.fromisoformat(start_s.replace('Z','+00:00'))
        except Exception:continue
        dur=parse_iso8601_duration(dur_s)
        if dur is None:continue
        total_hours=max(1,int(dur.total_seconds()//3600))
        v=val*9/5+32 if unit_is_celsius else val
        for hstep in range(0,total_hours,1):
            t=start+timedelta(hours=hstep)
            try:day=local_date(t.isoformat(),tz)
            except Exception:continue
            by_day[day].append(v)
    out={}
    for day,vals in by_day.items():
        out[day]=max(vals) if agg=='max' else sum(vals)/len(vals)
    return out

def save_nws_grid_values(nws_grid,observed,stats):
    # Batch all NWS grid observations into one transaction. The old version
    # called q() once per value, which committed every row and made this phase
    # take ~110 seconds on a large dynamically discovered city set.
    rows=[]
    for k,d in nws_grid.items():
        l=getloc(k)
        if not l:continue
        city=l['city_name']
        for date,high_f in (d.get('daily_high_f') or {}).items():
            payload={'value':high_f,'update_time':d.get('update_time'),'location_key':k}
            rows.append((city,'nws_temperature_high','nws_grid',date,high_f,J(payload),h(payload),observed,MEASUREMENT_VERSION))
        for date,pop in (d.get('daily_pop_max') or {}).items():
            payload={'value':pop,'update_time':d.get('update_time'),'location_key':k}
            rows.append((city,'nws_precip_probability_max','nws_grid',date,pop,J(payload),h(payload),observed,MEASUREMENT_VERSION))
    if rows:
        c=db()
        try:
            with c.cursor() as cur:
                cur.executemany(
                    '''INSERT INTO forecast_observations(
                        observed_at,city,variable,model,forecast_date,scalar_value,
                        payload,payload_hash,source_observed_at,measurement_version
                    ) VALUES(NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(city,variable,model,forecast_date,payload_hash)
                    DO NOTHING''',
                    rows)
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()
    stats['nws_grid_values_saved']=stats.get('nws_grid_values_saved',0)+len(rows)

def nws_latest_high(city,date,before):
    r=q("SELECT scalar_value FROM forecast_observations WHERE city=%s AND variable='nws_temperature_high' AND model='nws_grid' AND forecast_date=%s AND observed_at<%s AND measurement_version=%s ORDER BY observed_at DESC,id DESC LIMIT 1",(city,date,before,MEASUREMENT_VERSION),one=True)
    return r[0] if r else None

def nws_confirms_direction(location_key,date,gfs_change,before):
    """Compare the direction of a GFS-ensemble probability shift against the
    change in the official NWS gridpoint high-temp forecast over the same
    lookback window. Returns True/False/None (None = no comparable NWS data
    this run, e.g. NWS grid unavailable)."""
    l=getloc(location_key)
    if not l:return None
    city=l['city_name']
    current=q("SELECT scalar_value FROM forecast_observations WHERE city=%s AND variable='nws_temperature_high' AND model='nws_grid' AND forecast_date=%s AND measurement_version=%s ORDER BY observed_at DESC,id DESC LIMIT 1",(city,date,MEASUREMENT_VERSION),one=True)
    previous=q("SELECT scalar_value FROM forecast_observations WHERE city=%s AND variable='nws_temperature_high' AND model='nws_grid' AND forecast_date=%s AND observed_at<%s AND measurement_version=%s ORDER BY observed_at DESC,id DESC LIMIT 1",(city,date,before,MEASUREMENT_VERSION),one=True)
    if not current or not previous or current[0] is None or previous[0] is None:
        return None
    nws_change=current[0]-previous[0]
    if abs(nws_change)<0.5:
        return None  # NWS forecast essentially flat this window; not informative either way
    return (nws_change>0)==(gfs_change>0)

def create_research(l,date,var,m,prevp,curp,preask,eventask,scan,observed,stats):
    ch=curp-prevp
    if abs(ch)<RESEARCH_MIN_FORECAST_CHANGE_POINTS:return
    side='YES' if ch>=0 else 'NO'
    prev_side=prevp if side=='YES' else 100-prevp
    cur_side=curp if side=='YES' else 100-curp
    side_ch=abs(ch)
    market_ch=eventask-preask
    lag=side_ch-market_ch
    edge=cur_side-eventask
    fp=h({'city':l['city_name'],'date':date,'var':var,'ticker':m.get('ticker'),'side':side,'previous':round(prevp,6),'current':round(curp,6),'scan_id':scan})[:32]
    r=q("""INSERT INTO forecast_research_events(event_fingerprint,created_at,city,forecast_date,variable,market_ticker,side,previous_probability,current_probability,forecast_probability_change_points,pre_forecast_ask_cents,event_ask_cents,initial_market_change_points,initial_market_lag_points,initial_preliminary_edge_points,status,scan_id,forecast_observed_at,measurement_version,settlement_verified,location_key) VALUES(%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(event_fingerprint) DO NOTHING RETURNING id""",(fp,l['city_name'],date,var,m.get('ticker',''),side,prev_side,cur_side,side_ch,preask,eventask,market_ch,lag,edge,'open' if lag>0 else 'no_initial_lag',scan,observed,MEASUREMENT_VERSION,l['settlement_verified'],l['location_key']),one=True)
    if r:stats['research_events_created']+=1

def process_research(cache,ens,before,scan,observed,stats):
    # Cache repeated lookups. Each market in a city/date shares the same
    # previous ensemble forecast; each ticker shares the same market snapshots.
    # This avoids hundreds/thousands of redundant DB round trips on a scan.
    missing_prev_keys=set()
    forecast_cache={}
    prior_market_cache={}
    event_market_cache={}

    for kind,keyvar,probfun in [('temperature','ensemble_temperature_distribution',tprob),('rain','ensemble_rain_distribution',rprob)]:
        for s,l,ms in cache[kind]:
            for m in ms:
                stats['research_markets_considered']+=1
                date=date_market(m)
                if not date:continue
                d=ens.get(l['location_key'],{}).get('daily',{}).get(date)
                if not d:continue
                curvals=d.get('member_highs' if kind=='temperature' else 'member_precip_totals') or []

                fkey=(l['city_name'],keyvar,date)
                if fkey not in forecast_cache:
                    forecast_cache[fkey]=forecast_prev(l['city_name'],keyvar,date,before)
                prev=forecast_cache[fkey]

                if not prev:
                    mk=(l['location_key'],date,kind)
                    if mk not in missing_prev_keys:
                        missing_prev_keys.add(mk);stats['research_missing_previous_forecast']+=1
                    continue

                prev_payload=prev['payload'] or {}
                current_run=ens.get(l['location_key'],{}).get('model_run')
                if kind=='temperature':
                    current_payload={'member_highs':d.get('member_highs') or [],'member_highs_rounded':[round_temp(v) for v in (d.get('member_highs') or [])],'model_run':current_run}
                    variable_key='ensemble_temperature_distribution'
                else:
                    current_payload={'member_precip_totals':d.get('member_precip_totals') or [],'model_run':current_run}
                    variable_key='ensemble_rain_distribution'

                current_fp=forecast_revision_fingerprint(variable_key,current_payload)
                previous_fp=forecast_revision_fingerprint(variable_key,prev_payload)
                if not current_fp or not previous_fp or current_fp==previous_fp:
                    continue

                prevvals=prev_payload.get('member_highs' if kind=='temperature' else 'member_precip_totals') or []
                cp=probfun(curvals,m) if kind=='temperature' else probfun(curvals)
                pp=probfun(prevvals,m) if kind=='temperature' else probfun(prevvals)
                if cp is None or pp is None:continue
                stats['research_current_forecasts']+=1
                stats['research_previous_forecasts']+=1

                ticker=m.get('ticker','')
                if ticker not in prior_market_cache:
                    prior_market_cache[ticker]=prior_market(ticker,before)
                pm=prior_market_cache[ticker]
                if ticker not in event_market_cache:
                    event_market_cache[ticker]=event_market(ticker,scan)
                em=event_market_cache[ticker]
                if not pm or not em:
                    stats['research_missing_previous_market']+=1
                    continue

                side='YES' if cp>=pp else 'NO'
                pa=pm[2] if side=='YES' else pm[4]
                ea=em[2] if side=='YES' else em[4]
                if pa is not None and ea is not None:
                    create_research(l,date,'temperature' if kind=='temperature' else 'precipitation',m,pp,cp,pa,ea,scan,observed,stats)

def candidate(l,date,m,cp,pp,pm,em,kind='temperature',nws_confirms=None):
    if cp is None or pp is None or not pm or not em or abs(cp-pp)<MIN_FORECAST_PROBABILITY_CHANGE_POINTS:return None
    if not signal_allowed(l,'rain' if kind=='precipitation' else 'temperature'):return None
    # NWS confirmation gate: only fire temperature signals when the official
    # NWS gridpoint forecast (human/station-bias-adjusted) is also moving in
    # the same direction as the raw GFS ensemble shift. This cuts down on
    # signals that are just lag/noise in the Open-Meteo mirror rather than a
    # real forecast change. See fetch_nws_grid_values()/nws_confirms_direction().
    if kind=='temperature' and REQUIRE_NWS_CONFIRMATION and nws_confirms is False:
        return None
    best=None
    forecast_change=cp-pp
    sides=('YES',) if forecast_change>0 else ('NO',)
    for side in sides:
        ask=em[2] if side=='YES' else em[4]
        prev=pm[2] if side=='YES' else pm[4]
        if ask is None or prev is None or not MIN_ENTRY_PRICE_CENTS<=ask<=MAX_ENTRY_PRICE_CENTS:continue
        sp=cp if side=='YES' else 100-cp
        sch=abs(forecast_change)
        mc=ask-prev
        lag=sch-mc
        edge=sp-ask
        if lag<MIN_MARKET_LAG_POINTS or edge<MIN_PRELIMINARY_EDGE_POINTS:continue
        z={'city':l['city_name'],'forecast_date':date,'market_ticker':m.get('ticker',''),'market_kind':kind,'side':side,'entry_price_cents':ask,'model_probability_proxy':sp,'max_profitable_entry_cents':sp,'preliminary_edge_points':edge,'forecast_probability_change_points':sch,'market_price_change_points':mc,'market_lag_points':lag,'forecast_temperature_change_f':None,'forecast_previous_probability':pp,'forecast_current_probability':cp,'market_observed_at':em[0],'yes_bid_cents':em[1],'yes_ask_cents':em[2],'no_bid_cents':em[3],'no_ask_cents':em[4],'last_price_cents':em[5],'spread_yes_cents':em[6],'spread_no_cents':em[7],'volume':em[8],'open_interest':em[9],'event_ticker':em[10],'series_ticker':em[11],'contract_label':contract_label(m),'nws_confirmed':nws_confirms}
        if best is None or (z['market_lag_points'],z['preliminary_edge_points'])>(best['market_lag_points'],best['preliminary_edge_points']):best=z
    return best

def paper(signal,reason,stats):
    fp=h({'city':signal['city'],'forecast_date':signal['forecast_date'],'market_ticker':signal['market_ticker'],'side':signal['side'],'previous_probability':round(signal['forecast_previous_probability'],6),'current_probability':round(signal['forecast_current_probability'],6)})
    existing=q('SELECT id FROM paper_trades WHERE signal_fingerprint=%s',(fp,),one=True)
    if not existing:
        entry=signal['entry_price_cents']/100
        if entry<=0:return
        contracts=max(1,int(PAPER_RISK_DOLLARS/entry));stake=contracts*entry
        inserted=q("""INSERT INTO paper_trades(signal_fingerprint,created_at,city,forecast_date,market_ticker,market_kind,side,entry_price_cents,stake_dollars,contracts,model_probability_proxy,preliminary_edge_points,forecast_probability_change_points,market_price_change_points,market_lag_points,forecast_temperature_change_f,reason,status,measurement_version) VALUES(%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open',%s) ON CONFLICT(signal_fingerprint) DO NOTHING RETURNING id""",(fp,signal['city'],signal['forecast_date'],signal['market_ticker'],signal['market_kind'],signal['side'],signal['entry_price_cents'],stake,contracts,signal['model_probability_proxy'],signal['preliminary_edge_points'],signal['forecast_probability_change_points'],signal['market_price_change_points'],signal['market_lag_points'],None,J(reason),MEASUREMENT_VERSION),one=True)
        if not inserted: existing=True
        else: stats['paper_trades_created']+=1
    if q('SELECT 1 FROM alert_log WHERE fingerprint=%s',(fp,),one=True):return
    market_ticker=signal['market_ticker'];event_ticker=reason.get('event_ticker');series_ticker=reason.get('series_ticker');market_url=reason.get('market_url')
    if not market_url:
        market_url=(f"https://kalshi.com/markets/{series_ticker.lower()}/{event_ticker.lower()}" if series_ticker and event_ticker else 'https://kalshi.com/search?q='+requests.utils.quote(market_ticker,safe=''))
    observed_at=signal.get('market_observed_at');observed_text=observed_at.isoformat() if hasattr(observed_at,'isoformat') else str(observed_at or 'unknown')
    def fmt(v): return f'{v:.1f}c' if v is not None else 'n/a'
    nws_line=''
    if signal.get('nws_confirmed') is True:
        nws_line='NWS official forecast: **confirms** this move ✅\n'
    elif signal.get('nws_confirmed') is False:
        nws_line='NWS official forecast: **does not confirm** this move ⚠️\n'
    elif signal.get('nws_confirmed') is None:
        nws_line='NWS official forecast: no comparable data this run\n'
    msg=(f"🌦️ **{signal['market_kind'].upper()} FORECAST SHOCK — PAPER TRADE**\n\n**{signal['city']} — {signal['forecast_date']}**\n**CONTRACT: {signal.get('contract_label','Temperature contract')}**\n**ACTION: BUY {signal['side']}**\nMarket ticker: `{market_ticker}`\nEntry ask: **{signal['entry_price_cents']:.1f}¢**\nQuote observed (UTC): `{observed_text}`\nYES bid/ask: **{fmt(signal.get('yes_bid_cents'))} / {fmt(signal.get('yes_ask_cents'))}**\nNO bid/ask: **{fmt(signal.get('no_bid_cents'))} / {fmt(signal.get('no_ask_cents'))}**\nLast trade: **{fmt(signal.get('last_price_cents'))}**\n{nws_line}\nEnsemble probability proxy: **{signal['model_probability_proxy']:.1f}%**\n💰 **Model fair value / max entry before fees: {signal['max_profitable_entry_cents']:.1f}¢**\n\nForecast change: **{signal['forecast_probability_change_points']:+.1f} pts**\nMarket ask change: **{signal['market_price_change_points']:+.1f} pts**\nEstimated lag: **{signal['market_lag_points']:+.1f} pts**\nPreliminary edge: **{signal['preliminary_edge_points']:+.1f} pts**\n\nPaper risk: **${PAPER_RISK_DOLLARS:.2f}**\n\n🔗 **Kalshi market:** {market_url}\n\n⚠️ **PAPER TRADE ONLY** — the quote above is the exact market snapshot used by the scanner. The ensemble value is an uncalibrated frequency proxy; verify active Kalshi market rules before any real trade.")
    try:
        r=requests.post(DISCORD_RELAY_URL,json={'secret':DISCORD_RELAY_SECRET,'message':msg},headers={'User-Agent':'WeatherKalshiResearchBot/8.0'},timeout=REQUEST_TIMEOUT) if DISCORD_RELAY_URL and DISCORD_RELAY_SECRET else None
        if r is not None and 200<=r.status_code<300:
            q('INSERT INTO alert_log(fingerprint,sent_at,payload,measurement_version) VALUES(%s,NOW(),%s,%s) ON CONFLICT DO NOTHING',(fp,J({**signal,'alert_observed_at_utc':observed_text,'contract_label':signal.get('contract_label')}),MEASUREMENT_VERSION))
            stats['discord_alerts']+=1
    except Exception as e:log.error('Discord relay failed: %s',e)

def settle(stats):
    for tid,ticker,side,stake,contracts in q("SELECT id,market_ticker,side,stake_dollars,contracts FROM paper_trades WHERE status='open' LIMIT 200",fetch=True) or []:
        try:
            m=http(f'{KALSHI_API_URL}/markets/{ticker}',tries=1).get('market',{});res=(m.get('result') or '').lower()
            if res not in {'yes','no'}:continue
            pnl=contracts-stake if res==side.lower() else -stake;q('UPDATE paper_trades SET settled_at=NOW(),result=%s,profit_loss_dollars=%s,status=\'settled\' WHERE id=%s',(res,pnl,tid));stats['settled_trades']+=1
        except Exception as e:log.warning('Could not settle %s: %s',ticker,e)

def close_research_events(stats):
    rows=q("SELECT id,market_ticker FROM forecast_research_events WHERE measurement_version=%s AND status IN ('open','no_initial_lag') LIMIT 1000",(MEASUREMENT_VERSION,),fetch=True) or []
    for eid,ticker in rows:
        try:
            m=http(f'{KALSHI_API_URL}/markets/{ticker}',tries=1).get('market',{})
            res=(m.get('result') or '').lower()
            if res in {'yes','no'}:
                q("UPDATE forecast_research_events SET status='settled',closed_at=NOW(),settlement_result=%s WHERE id=%s AND status IN ('open','no_initial_lag')",(res,eid))
                stats['research_events_settled']=stats.get('research_events_settled',0)+1
        except Exception as e:
            log.warning('Could not settle research event %s/%s: %s',eid,ticker,e)

def observe(stats,before):
    rows=q("SELECT id,market_ticker,side,created_at,event_ask_cents,initial_market_lag_points,latest_observation_at,max_market_move_points FROM forecast_research_events WHERE measurement_version=%s AND status='open' AND created_at<%s LIMIT 1000",(MEASUREMENT_VERSION,before),fetch=True) or []
    for eid,ticker,side,created,event,lag,last,maxmove in rows:
        r=q("""
            SELECT observed_at,scan_id,ticker,event_ticker,series_ticker,
                   yes_bid_cents,yes_ask_cents,no_bid_cents,no_ask_cents,
                   last_price_cents,spread_yes_cents,spread_no_cents,volume,open_interest
            FROM market_snapshots
            WHERE ticker=%s AND snapshot_phase='scan_start'
              AND measurement_version=%s AND observed_at>%s AND observed_at<%s
            ORDER BY observed_at,id
            LIMIT 1
        """,(ticker,MEASUREMENT_VERSION,last or created,before),one=True)
        if not r or r[1] is None:
            continue
        (observed_at,scan_id,ticker_value,event_ticker,series_ticker,
         yes_bid,yes_ask,no_bid,no_ask,last_price,spread_yes,spread_no,volume,open_interest)=r
        current_ask=yes_ask if side=='YES' else no_ask
        if current_ask is None:
            continue
        move=current_ask-event
        frac=move/lag if lag and lag>0 else 0
        remaining=lag-move if lag is not None else None
        max_move_new=max(maxmove or 0,move)
        q("""
            UPDATE forecast_research_events
            SET latest_observation_at=%s,
                latest_ask_cents=%s,
                latest_market_move_points=%s,
                latest_lag_remaining_points=%s,
                max_market_move_points=%s,
                first_response_at=CASE WHEN first_response_at IS NULL AND %s>0 THEN %s ELSE first_response_at END,
                milestone_25_at=CASE WHEN milestone_25_at IS NULL AND %s>0 AND %s>=initial_market_lag_points*.25 THEN %s ELSE milestone_25_at END,
                milestone_50_at=CASE WHEN milestone_50_at IS NULL AND %s>0 AND %s>=initial_market_lag_points*.50 THEN %s ELSE milestone_50_at END,
                milestone_75_at=CASE WHEN milestone_75_at IS NULL AND %s>0 AND %s>=initial_market_lag_points*.75 THEN %s ELSE milestone_75_at END,
                milestone_90_at=CASE WHEN milestone_90_at IS NULL AND %s>0 AND %s>=initial_market_lag_points*.90 THEN %s ELSE milestone_90_at END
            WHERE id=%s
        """,(observed_at,current_ask,move,remaining,max_move_new,
             move,observed_at,
             move,move,observed_at,
             move,move,observed_at,
             move,move,observed_at,
             move,move,observed_at,eid))
        q("""
            INSERT INTO forecast_research_updates(
                event_id,observed_at,market_ask_cents,market_move_points,
                lag_remaining_points,market_response_fraction,scan_id,ticker,
                event_ticker,series_ticker,yes_bid_cents,yes_ask_cents,
                no_bid_cents,no_ask_cents,last_price_cents,spread_yes_cents,
                spread_no_cents,volume,open_interest
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,(eid,observed_at,current_ask,move,remaining,frac,scan_id,ticker_value,
              event_ticker,series_ticker,yes_bid,yes_ask,no_bid,no_ask,last_price,
              spread_yes,spread_no,volume,open_interest))
        stats['research_events_observed']+=1
        log.info(
            'RESEARCH MARKET | %s | side=%s | observed=%s | scan=%s | YES %.1f/%.1f | NO %.1f/%.1f | last=%s | volume=%s',
            ticker_value,side,observed_at.isoformat(),scan_id,
            yes_bid if yes_bid is not None else float('nan'),yes_ask if yes_ask is not None else float('nan'),
            no_bid if no_bid is not None else float('nan'),no_ask if no_ask is not None else float('nan'),
            f'{last_price:.1f}c' if last_price is not None else 'n/a',
            f'{volume:.0f}' if volume is not None else 'n/a')

def run_scan():
    stats={'schema_version':SCHEMA_VERSION,'measurement_version':MEASUREMENT_VERSION,'temperature_series':0,'rain_series':0,'temperature_markets':0,'rain_markets':0,'weather_refreshed':False,'forecast_shocks':0,'paper_trades_created':0,'discord_alerts':0,'settled_trades':0,'deterministic_gfs_ok':False,'ensemble_ok':False,'ensemble_fetch_attempted':False,'rain_forecast_shocks':0,'research_events_created':0,'research_events_observed':0,'research_markets_considered':0,'research_current_forecasts':0,'research_previous_forecasts':0,'research_missing_previous_forecast':0,'research_missing_previous_market':0,'research_missing_model_run':0,'research_events_settled':0,'nws_updates_detected':0,'nws_grid_values_saved':0,'phase_seconds':{}}
    scan=None
    started=now();
    def phase(name,t0):
        dt=time.time()-t0
        stats['phase_seconds'][name]=round(dt,1)
        log.info('PHASE %s | %.1fs',name,dt)
        return time.time()
    t=time.time()
    try:
        schema();scan=q("INSERT INTO scan_runs(started_at,status,stats,schema_version) VALUES(NOW(),'running','{}'::jsonb,%s) RETURNING id",(SCHEMA_VERSION,),one=True)[0];settle(stats)
        t=phase('schema_and_settle',t)
        xs=series_list();save_series(xs);te,re=discover(xs);stats['temperature_series']=len(te);stats['rain_series']=len(re);stats['weather_locations_discovered']=len({l['location_key'] for _,l in te+re})
        t=phase('series_discovery',t)
        c={'temperature':cache(te),'rain':cache(re)};snapshot(c,scan,'scan_start',stats,count_markets=True)
        t=phase('scan_start_snapshot',t)
        locs={l['location_key']:l for _,l in te+re}

        # Single pass over each location's NWS grid document: gets both the
        # updateTime (used below to decide whether to refresh deterministic
        # GFS) and the official temperature/PoP values, instead of fetching
        # the same URL twice.
        us,ch,nws_grid=nws_grid_snapshot(list(locs.values()));stats['nws_updates_detected']=len(ch)
        t=phase('nws_grid_snapshot',t)
        try:
            save_nws_grid_values(nws_grid,now(),stats)
        except Exception as e:
            log.warning('Could not save NWS grid values: %s',e)
        t=phase('nws_grid_save',t)

        det={}
        ens={}
        observed=now()

        if ch:
            try:
                det=fetch_det(list(locs.values()))
                stats['deterministic_gfs_ok']=True
            except Exception as e:
                log.warning('Deterministic GFS unavailable: %s',e)
        t=phase('fetch_det',t)

        try:
            stats['ensemble_fetch_attempted']=True
            ens=fetch_ens(list(locs.values()))
            stats['ensemble_ok']=True
        except Exception as e:
            log.warning('Ensemble forecast unavailable: %s',e)
            ens={}
        t=phase('fetch_ens',t)

        observed=now()

        if ens:
            stats['weather_refreshed']=True
            event_cache={'temperature':cache(te),'rain':cache(re)}
            snapshot(event_cache,scan,'forecast_event',stats,count_markets=False)
            t=phase('event_snapshot',t)
            process_research(event_cache,ens,started,scan,observed,stats)
            t=phase('process_research',t)

            # Cache repeated DB lookups. Every market in the same city/date
            # shares the previous forecast and NWS confirmation; every ticker
            # shares the same market snapshots.
            forecast_cache={}
            prior_cache={}
            event_cache_lookup={}
            nws_confirmation_cache={}

            for kind,entries in [('temperature',c['temperature']),('rain',c['rain'])]:
                if kind=='rain' and not ALLOW_RAIN_PAPER_SIGNALS:
                    continue
                for s,l,ms in entries:
                    if not l['settlement_verified'] and not ALLOW_UNVERIFIED_LOCATION_SIGNALS:
                        continue
                    variable_key='ensemble_temperature_distribution' if kind=='temperature' else 'ensemble_rain_distribution'
                    for m in ms:
                        date=date_market(m)
                        d=ens.get(l['location_key'],{}).get('daily',{}).get(date or '')
                        if not d:
                            continue
                        cur=d.get('member_highs' if kind=='temperature' else 'member_precip_totals') or []

                        fkey=(l['city_name'],variable_key,date)
                        if fkey not in forecast_cache:
                            forecast_cache[fkey]=forecast_prev(l['city_name'],variable_key,date,started)
                        prev=forecast_cache[fkey]
                        if not prev:
                            continue

                        old=prev['payload'].get('member_highs' if kind=='temperature' else 'member_precip_totals') or []
                        cp=tprob(cur,m) if kind=='temperature' else rprob(cur)
                        pp=tprob(old,m) if kind=='temperature' else rprob(old)
                        ticker=m.get('ticker','')

                        if ticker not in prior_cache:
                            prior_cache[ticker]=prior_market(ticker,started)
                        pm=prior_cache[ticker]

                        if ticker not in event_cache_lookup:
                            event_cache_lookup[ticker]=event_market(ticker,scan)
                        em=event_cache_lookup[ticker]

                        nws_confirms=None
                        if kind=='temperature' and cp is not None and pp is not None:
                            nkey=(l['location_key'],date)
                            if nkey not in nws_confirmation_cache:
                                nws_confirmation_cache[nkey]=nws_confirms_direction(l['location_key'],date,cp-pp,started)
                            nws_confirms=nws_confirmation_cache[nkey]

                        sig=candidate(l,date,m,cp,pp,pm,em,kind='temperature' if kind=='temperature' else 'precipitation',nws_confirms=nws_confirms)
                        if sig:
                            stats['forecast_shocks']+=1
                            if kind=='rain':
                                stats['rain_forecast_shocks']+=1
                            paper(sig,{'measurement_version':MEASUREMENT_VERSION,'settlement_verified':l['settlement_verified'],'signal_enabled':l['signal_enabled'],'ensemble_model':ENSEMBLE_MODEL,'event_ticker':m.get('event_ticker'),'series_ticker':m.get('series_ticker') or s.get('ticker'),'market_url':m.get('url'),'market_observed_at':sig.get('market_observed_at'),'yes_bid_cents':sig.get('yes_bid_cents'),'yes_ask_cents':sig.get('yes_ask_cents'),'no_bid_cents':sig.get('no_bid_cents'),'no_ask_cents':sig.get('no_ask_cents'),'last_price_cents':sig.get('last_price_cents'),'spread_yes_cents':sig.get('spread_yes_cents'),'spread_no_cents':sig.get('spread_no_cents'),'volume':sig.get('volume'),'open_interest':sig.get('open_interest')},stats)

            t=phase('candidate_loop',t)
            save_forecasts(det,ens,observed)
            t=phase('save_forecasts',t)

        if ch:
            save_nws(us,locs)

        observe(stats,started);close_research_events(stats);settle(stats)
        t=phase('observe_close_settle',t)
        q('UPDATE scan_runs SET completed_at=NOW(),status=\'success\',stats=%s,schema_version=%s WHERE id=%s',(J(stats),SCHEMA_VERSION,scan));log.info('SCAN COMPLETE | %s | runtime=%.1fs',json.dumps(stats,default=str), (now()-started).total_seconds())
    except Exception as e:
        log.exception('SCAN FAILED')
        if scan:
            q('UPDATE scan_runs SET completed_at=NOW(),status=\'failed\',stats=%s,error=%s,schema_version=%s WHERE id=%s',(J(stats),str(e),SCHEMA_VERSION,scan))
        raise
    finally:close_db()

if __name__=='__main__':run_scan()
