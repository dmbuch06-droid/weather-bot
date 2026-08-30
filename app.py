# Full replacement app.py
import os,time,json,threading,statistics,hashlib
from datetime import datetime,timezone,timedelta
from collections import defaultdict
from urllib.parse import urlparse
import requests
from flask import Flask,jsonify
app=Flask(__name__)
KALSHI_API_URL='https://api.elections.kalshi.com/trade-api/v2'
DISCORD_RELAY_URL=os.environ.get('DISCORD_RELAY_URL','').strip()
DISCORD_RELAY_SECRET=os.environ.get('DISCORD_RELAY_SECRET','').strip()
SCAN_INTERVAL_SECONDS=int(os.environ.get('SCAN_INTERVAL_SECONDS','300'))
POINT_REFRESH_SECONDS=int(os.environ.get('POINT_REFRESH_SECONDS','1800'))
ENSEMBLE_REFRESH_SECONDS=int(os.environ.get('ENSEMBLE_REFRESH_SECONDS','1800'))
MIN_FORECAST_CHANGE_F=float(os.environ.get('MIN_FORECAST_CHANGE_F','1.0'))
MIN_EDGE_POINTS=float(os.environ.get('MIN_EDGE_POINTS','5.0'))
MAX_POINT_ENSEMBLE_GAP_F=float(os.environ.get('MAX_POINT_ENSEMBLE_GAP_F','6.0'))
MAX_POINT_CACHE_AGE_SECONDS=int(os.environ.get('MAX_POINT_CACHE_AGE_SECONDS','21600'))
MAX_ENSEMBLE_CACHE_AGE_SECONDS=int(os.environ.get('MAX_ENSEMBLE_CACHE_AGE_SECONDS','21600'))
STATE_FILE=os.environ.get('STATE_FILE','forecast_state.json'); REQUEST_TIMEOUT=int(os.environ.get('REQUEST_TIMEOUT','20')); FORECAST_DAYS=int(os.environ.get('FORECAST_DAYS','7')); STATE_VERSION=3
CITIES={'KXHIGHNY':{'name':'New York','lat':40.7128,'lon':-74.0060,'timezone':'America/New_York'},'KXHIGHCHI':{'name':'Chicago','lat':41.8781,'lon':-87.6298,'timezone':'America/Chicago'},'KXHIGHMIA':{'name':'Miami','lat':25.7617,'lon':-80.1918,'timezone':'America/New_York'},'KXHIGHAUS':{'name':'Austin','lat':30.2672,'lon':-97.7431,'timezone':'America/Chicago'}}
bot_status={'last_scan_utc':None,'last_scan_success':None,'series_checked':0,'markets_checked':0,'forecast_changes':0,'positive_signals':0,'discord_alerts':0,'last_error':None,'state_persistence_warning':None,'discord_last_status':None,'discord_last_error':None}
state_lock=threading.RLock(); persistent_state={}
def now_utc(): return datetime.now(timezone.utc)
def utc_iso(): return now_utc().isoformat()
def safe_float(v,d=None):
 try:return float(v) if v is not None else d
 except (TypeError,ValueError):return d
def parse_iso_datetime(v):
 try:
  x=datetime.fromisoformat(str(v).replace('Z','+00:00')); return (x if x.tzinfo else x.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
 except:return None
def age_seconds(v):
 x=parse_iso_datetime(v); return max(0,(now_utc()-x).total_seconds()) if x else None
def today_for_city(c):
 try:
  from zoneinfo import ZoneInfo; return datetime.now(ZoneInfo(c['timezone'])).date().isoformat()
 except:return now_utc().date().isoformat()
def ensure_state_shape():
 global persistent_state
 if not isinstance(persistent_state,dict): persistent_state={}
 for k,d in [('version',STATE_VERSION),('forecasts',{}),('cache',{}),('cooldowns',{}),('alerts',{}),('paper_trades',[])]: persistent_state.setdefault(k,d)
 persistent_state['version']=STATE_VERSION
def load_state():
 global persistent_state
 try:
  if not os.path.exists(STATE_FILE): persistent_state={}; ensure_state_shape(); print('No previous state file found. Starting fresh.',flush=True); return
  with open(STATE_FILE,encoding='utf-8') as f:persistent_state=json.load(f)
  ensure_state_shape(); print(f"Loaded state: {len(persistent_state['forecasts'])} forecast entries, {len(persistent_state['cache'])} cache entries, {len(persistent_state['paper_trades'])} paper trades.",flush=True)
 except Exception as e: print(f'State load error: {e}',flush=True); persistent_state={}; ensure_state_shape()
def save_state():
 try:
  with state_lock:
   with open(STATE_FILE+'.tmp','w',encoding='utf-8') as f: json.dump(persistent_state,f,indent=2)
   os.replace(STATE_FILE+'.tmp',STATE_FILE); bot_status['state_persistence_warning']=None
 except Exception as e: print(f'State save error: {e}',flush=True); bot_status['state_persistence_warning']=str(e)
def ck(s,t):return f'{s}|{t}'
def get_cache(s,t):return persistent_state['cache'].get(ck(s,t))
def set_cache(s,t,data,metadata=None): persistent_state['cache'][ck(s,t)]={'retrieved_at':utc_iso(),'data':data,'metadata':metadata or {}}; save_state()
def cache_is_fresh(e,secs): return bool(e and age_seconds(e.get('retrieved_at')) is not None and age_seconds(e.get('retrieved_at'))<secs)
def cache_is_usable(e,secs): return bool(e and age_seconds(e.get('retrieved_at')) is not None and age_seconds(e.get('retrieved_at'))<=secs)
def cached_result(e,status): return (e.get('data',{}),{'status':status,'retrieved_at':e.get('retrieved_at'),'age_seconds':age_seconds(e.get('retrieved_at')),'metadata':e.get('metadata',{})}) if e else ({},{'status':'missing','retrieved_at':None,'age_seconds':None,'metadata':{}})
def validate_discord_relay():
 if not DISCORD_RELAY_URL:
  return False,'DISCORD_RELAY_URL is empty'
 if not DISCORD_RELAY_SECRET:
  return False,'DISCORD_RELAY_SECRET is empty'
 if not DISCORD_RELAY_URL.startswith('https://script.google.com/macros/s/') or not DISCORD_RELAY_URL.endswith('/exec'):
  return False,'DISCORD_RELAY_URL does not look like the Google Apps Script web-app URL'
 return True,None

def send_discord_alert(message):
 ok,err=validate_discord_relay()
 if not ok:
  print(f'Discord relay configuration error: {err}',flush=True)
  bot_status['discord_last_status']=None
  bot_status['discord_last_error']=err
  return False

 payload={'secret':DISCORD_RELAY_SECRET,'message':message}
 headers={
  'User-Agent':'WeatherKalshiPaperMonitor/1.0 (Render)',
  'Content-Type':'application/json',
  'Accept':'application/json',
 }

 for attempt in range(1,4):
  try:
   r=requests.post(DISCORD_RELAY_URL,json=payload,headers=headers,timeout=REQUEST_TIMEOUT)
   bot_status['discord_last_status']=r.status_code
   print(f'Discord relay response: {r.status_code} (attempt {attempt}/3)',flush=True)

   content_type=r.headers.get('content-type','').lower()
   data=None
   if 'application/json' in content_type:
    try:data=r.json()
    except Exception:data=None

   if r.status_code>=200 and r.status_code<300:
    if isinstance(data,dict) and data.get('success') is False:
     err=data.get('error') or data.get('discord_response') or 'Relay reported failure.'
     bot_status['discord_last_error']=err
     print(f'Discord relay reported failure: {err}',flush=True)
     return False
    bot_status['discord_last_error']=None
    print('Discord alert delivered through relay.',flush=True)
    return True

   if r.status_code==429 and attempt<3:
    retry_after=2.0
    if isinstance(data,dict):
     retry_after=safe_float(data.get('retry_after'),2.0) or 2.0
    retry_after=max(0.5,min(retry_after,30.0))
    print(f'Discord relay returned 429. Waiting {retry_after:.1f}s.',flush=True)
    time.sleep(retry_after+0.25)
    continue

   body=r.text[:1000]
   err=f'Discord relay HTTP {r.status_code}: {body}'
   bot_status['discord_last_error']=err
   print(err,flush=True)
   return False

  except requests.RequestException as e:
   err=f'Discord relay request exception: {e}'
   bot_status['discord_last_error']=err
   print(err,flush=True)
   if attempt<3:
    time.sleep(attempt*2)
    continue
   return False

  except Exception as e:
   err=f'Unexpected Discord relay error: {e}'
   bot_status['discord_last_error']=err
   print(err,flush=True)
   return False
 return False

def fetch_point_forecast(city):
 p={'latitude':city['lat'],'longitude':city['lon'],'daily':'temperature_2m_max,precipitation_sum','temperature_unit':'fahrenheit','precipitation_unit':'inch','timezone':city['timezone'],'forecast_days':FORECAST_DAYS}; r=requests.get('https://api.open-meteo.com/v1/forecast',params=p,timeout=REQUEST_TIMEOUT); print(f'Point forecast status: {r.status_code}',flush=True)
 if r.status_code!=200:raise RuntimeError(f'HTTP_{r.status_code}|{r.text[:500]}')
 d=r.json().get('daily',{}); out={}
 for i,date in enumerate(d.get('time',[])):
  high=safe_float(d.get('temperature_2m_max',[])[i]) if i<len(d.get('temperature_2m_max',[])) else None; precip=safe_float(d.get('precipitation_sum',[])[i]) if i<len(d.get('precipitation_sum',[])) else 0
  if high is not None:out[date]={'high':high,'precipitation':precip or 0}
 return out,{'api':'open-meteo-forecast','timezone':city['timezone']}
def get_point_forecast(t,city):
 e=get_cache('point',t)
 if cache_is_fresh(e,POINT_REFRESH_SECONDS):print(f"Point forecast: using scheduled cache ({age_seconds(e['retrieved_at']):.0f}s old).",flush=True); return cached_result(e,'fresh_cache')
 try:data,m=fetch_point_forecast(city); set_cache('point',t,data,m); return cached_result(get_cache('point',t),'fresh_api')
 except Exception as x:
  print(f'Point forecast error: {x}',flush=True)
  if cache_is_usable(e,MAX_POINT_CACHE_AGE_SECONDS):return cached_result(e,'stale_cache_after_error')
  return {},{'status':'failed_no_cache','retrieved_at':None,'age_seconds':None,'metadata':{},'error':str(x)}
def fetch_ensemble_forecast(city):
 p={'latitude':city['lat'],'longitude':city['lon'],'hourly':'temperature_2m','temperature_unit':'fahrenheit','timezone':city['timezone'],'forecast_days':FORECAST_DAYS}; r=requests.get('https://ensemble-api.open-meteo.com/v1/ensemble',params=p,timeout=REQUEST_TIMEOUT); print(f'Ensemble API status: {r.status_code}',flush=True)
 if r.status_code!=200:raise RuntimeError(f'HTTP_{r.status_code}|{r.text[:500]}')
 h=r.json().get('hourly',{}); ts=h.get('time',[]); keys=sorted(k for k in h if k.startswith('temperature_2m_member')); print(f'Ensemble member temperature keys found: {len(keys)}',flush=True); md=defaultdict(lambda:defaultdict(list))
 for k in keys:
  vals=h.get(k,[])
  for i,x in enumerate(ts):
   if i<len(vals) and safe_float(vals[i]) is not None:md[str(x)[:10]][k].append(float(vals[i]))
 out={}
 for date,members in md.items():
  highs=[max(members[k]) for k in keys if members.get(k)]
  if highs:out[date]={'member_highs':highs,'member_count':len(highs),'mean':statistics.mean(highs),'median':statistics.median(highs),'minimum':min(highs),'maximum':max(highs)}
 print(f'Ensemble dates available: {len(out)}',flush=True); return out,{'api':'open-meteo-ensemble','timezone':city['timezone'],'member_keys_found':len(keys),'retrieved_utc':utc_iso()}
def get_ensemble_forecast(t,city):
 e=get_cache('ensemble',t)
 if cache_is_fresh(e,ENSEMBLE_REFRESH_SECONDS):print(f"Ensemble forecast: using scheduled cache ({age_seconds(e['retrieved_at']):.0f}s old).",flush=True); return cached_result(e,'fresh_cache')
 try:data,m=fetch_ensemble_forecast(city); set_cache('ensemble',t,data,m); return cached_result(get_cache('ensemble',t),'fresh_api')
 except Exception as x:
  print(f'Ensemble forecast error: {x}',flush=True)
  if cache_is_usable(e,MAX_ENSEMBLE_CACHE_AGE_SECONDS):return cached_result(e,'stale_cache_after_error')
  return {},{'status':'failed_no_cache','retrieved_at':None,'age_seconds':None,'metadata':{},'error':str(x)}
def validate_weather_alignment(ph,ed,pi,ei):
 hs=ed.get('member_highs',[]) if ed else []
 if ph is None:return {'valid':False,'reason':'missing point forecast','ensemble_mean':None,'gap_f':None}
 if len(hs)<5:return {'valid':False,'reason':'too few ensemble members','ensemble_mean':None,'gap_f':None}
 mean=statistics.mean(hs); gap=abs(ph-mean)
 if pi.get('status') not in ('fresh_api','fresh_cache'):return {'valid':False,'reason':f"point forecast not fresh ({pi.get('status')})",'ensemble_mean':mean,'gap_f':gap}
 if ei.get('status') not in ('fresh_api','fresh_cache'):return {'valid':False,'reason':f"ensemble forecast not fresh ({ei.get('status')})",'ensemble_mean':mean,'gap_f':gap}
 if gap>MAX_POINT_ENSEMBLE_GAP_F:return {'valid':False,'reason':f'point/ensemble mean gap {gap:.2f}F exceeds {MAX_POINT_ENSEMBLE_GAP_F:.2f}F','ensemble_mean':mean,'gap_f':gap}
 return {'valid':True,'reason':'passed sanity check','ensemble_mean':mean,'gap_f':gap}
def get_kalshi_markets(t):
 try:
  r=requests.get(f'{KALSHI_API_URL}/markets',params={'series_ticker':t,'status':'open','limit':200},timeout=REQUEST_TIMEOUT); print(f'Kalshi {t} status: {r.status_code}',flush=True)
  if r.status_code!=200:print(r.text[:500],flush=True); return []
  x=r.json().get('markets',[]); print(f'Markets found: {len(x)}',flush=True); return x
 except Exception as e:print(f'Kalshi API error for {t}: {e}',flush=True); return []
def cents_from_market(m):
 for f in ('yes_ask_dollars','yes_ask_price_dollars'):
  v=safe_float(m.get(f));
  if v is not None and 0<=v<=1:return v*100
 for f in ('yes_ask','yes_ask_price'):
  v=safe_float(m.get(f));
  if v is not None:return v*100 if 0<=v<=1 else v
 return None
def parse_market_date(t):
 try:return datetime.strptime(t.split('-')[1],'%y%b%d').strftime('%Y-%m-%d')
 except:return None
def get_market_strike(m):
 floor=safe_float(m.get('floor_strike')); cap=safe_float(m.get('cap_strike')); ticker=m.get('ticker') or ''; title=(m.get('title') or '').lower()
 if floor is not None and cap is None:return {'type':'greater','floor':floor,'cap':None,'label':f'>{floor:g}°F'}
 if cap is not None and floor is None:return {'type':'less','floor':None,'cap':cap,'label':f'<{cap:g}°F'}
 if floor is not None and cap is not None:return {'type':'between','floor':floor,'cap':cap,'label':f'{floor:g}°F to {cap:g}°F'}
 if '-T' in ticker:
  try:
   s=float(ticker.split('-T')[-1]);
   if '>' in title:return {'type':'greater','floor':s,'cap':None,'label':f'>{s:g}°F'}
   if '<' in title:return {'type':'less','floor':None,'cap':s,'label':f'<{s:g}°F'}
  except:pass
 return None
def calculate_probability(hs,s):
 if not hs:return 0
 n=sum((x>s['floor'] if s['type']=='greater' else x<s['cap'] if s['type']=='less' else s['floor']<=x<=s['cap']) for x in hs); return n/len(hs)*100
def check_forecast_change(series,date,ph,prec,ed,pi,ei):
 key=f'{series}|{date}'; mean=safe_float(ed.get('mean')); current={'point_high':ph,'precipitation':prec,'ensemble_mean':mean,'ensemble_member_count':len(ed.get('member_highs',[])),'point_retrieved_at':pi.get('retrieved_at'),'ensemble_retrieved_at':ei.get('retrieved_at'),'point_status':pi.get('status'),'ensemble_status':ei.get('status'),'updated_at':utc_iso()}
 previous=persistent_state['forecasts'].get(key); persistent_state['forecasts'][key]=current; save_state()
 if previous is None:return {'first':True,'temperature_change':0,'precipitation_change':0,'ensemble_mean_change':0,'previous':None}
 return {'first':False,'temperature_change':ph-safe_float(previous.get('point_high'),ph),'precipitation_change':prec-safe_float(previous.get('precipitation'),prec),'ensemble_mean_change':(mean-safe_float(previous.get('ensemble_mean'),mean)) if mean is not None else 0,'previous':previous}
def fingerprint(series,date,o,ph,mean):return hashlib.sha256('|'.join([series,date,o['ticker'],f'{ph:.2f}',f'{mean:.2f}',f"{o['ask']:.2f}",f"{o['probability']:.2f}"]).encode()).hexdigest()[:20]
def build_discord_message(city,date,ph,prec,change,o,val,series):
 d=f"up {change['temperature_change']:.1f}°F" if change['temperature_change']>0 else f"down {abs(change['temperature_change']):.1f}°F" if change['temperature_change']<0 else 'unchanged'
 return f"🌦️ **WEATHER FORECAST CHANGE — PAPER SIGNAL**\n\n**{city} — {date}**\nPoint forecast high: **{ph:.1f}°F**\nChange since previous observation: **{d}**\nPrecipitation forecast: **{prec:.2f} in**\nEnsemble mean: **{val['ensemble_mean']:.1f}°F**\nPoint/ensemble gap: **{val['gap_f']:.1f}°F**\n\n🎯 **Potential opportunity**\nContract: **{o['label']}**\nTicker: `{o['ticker']}`\nRaw ensemble frequency: **{o['probability']:.1f}%**\nYES ask: **{o['ask']:.1f}¢**\nPreliminary edge: **+{o['edge']:.1f} points**\n\nKalshi: https://kalshi.com/markets/{series.lower()}\n\n⚠️ Paper-trading only. Raw ensemble frequency is not a calibrated probability and contract settlement rules must be validated."
def analyze_city(series,city):
 print('\n--------------------------------------------------',flush=True); print(f'SERIES: {series}',flush=True); print(f"CITY: {city['name']}",flush=True); pf,pi=get_point_forecast(series,city); ef,ei=get_ensemble_forecast(series,city); markets=get_kalshi_markets(series); print(f"POINT SOURCE STATUS: {pi.get('status')} | age={pi.get('age_seconds')}",flush=True); print(f"ENSEMBLE SOURCE STATUS: {ei.get('status')} | age={ei.get('age_seconds')}",flush=True)
 if not markets or not pf or not ef:return
 by=defaultdict(list); today=today_for_city(city)
 for m in markets:
  d=parse_market_date(m.get('ticker') or '')
  if d and d>=today and d in pf and d in ef:by[d].append(m)
 for d in sorted(by):
  pd,ed=pf[d],ef[d]; ph=pd.get('high'); prec=pd.get('precipitation',0); hs=ed.get('member_highs',[])
  if ph is None or not hs:continue
  print('\n..................................................',flush=True); print(f'DATE: {d}',flush=True); print(f'POINT FORECAST HIGH: {ph:.2f}°F',flush=True); print(f'POINT FORECAST PRECIPITATION: {prec:.2f} inches',flush=True); print(f'ENSEMBLE MEMBERS: {len(hs)}',flush=True); print(f'ENSEMBLE MINIMUM: {min(hs):.2f}°F',flush=True); print(f'ENSEMBLE MAXIMUM: {max(hs):.2f}°F',flush=True); print(f'ENSEMBLE MEAN: {statistics.mean(hs):.2f}°F',flush=True); print(f'ENSEMBLE MEDIAN: {statistics.median(hs):.2f}°F',flush=True)
  val=validate_weather_alignment(ph,ed,pi,ei); print(f"POINT/ENSEMBLE GAP: {val['gap_f']:.2f}°F" if val['gap_f'] is not None else '',flush=True); print(f"WEATHER DATA VALIDATION: {val['reason']}",flush=True); change=check_forecast_change(series,d,ph,prec,ed,pi,ei)
  if change['first']:print('FORECAST STATUS: FIRST OBSERVATION',flush=True); print('Baseline stored.',flush=True)
  else:
   print(f"POINT FORECAST CHANGE: {change['temperature_change']:+.2f}°F",flush=True); print(f"ENSEMBLE MEAN CHANGE: {change['ensemble_mean_change']:+.2f}°F",flush=True); print(f"PRECIPITATION CHANGE: {change['precipitation_change']:+.2f} in",flush=True)
   if abs(change['temperature_change'])>=MIN_FORECAST_CHANGE_F:bot_status['forecast_changes']+=1
  opp=[]
  for m in by[d]:
   bot_status['markets_checked']+=1; s=get_market_strike(m); ask=cents_from_market(m)
   if not s or ask is None:continue
   prob=calculate_probability(hs,s); opp.append({'ticker':m.get('ticker') or '','label':s['label'],'probability':prob,'ask':ask,'edge':prob-ask})
  opp.sort(key=lambda x:x['edge'],reverse=True); print(f"\nTOP OPPORTUNITIES: {city['name']} | {d}",flush=True)
  for i,o in enumerate(opp[:5],1):print(f"{i}. {o['label']} | Raw ensemble: {o['probability']:.1f}% | Ask: {o['ask']:.1f}¢ | Edge: {o['edge']:+.1f} points | {o['ticker']}",flush=True)
  positive=[o for o in opp if o['edge']>=MIN_EDGE_POINTS]; bot_status['positive_signals']+=len(positive)
  if change['first']:print('No Discord alert: first observation.',flush=True); continue
  if abs(change['temperature_change'])<MIN_FORECAST_CHANGE_F:print('No Discord alert: point forecast change below threshold.',flush=True); continue
  if not val['valid']:print('No Discord alert: weather validation/freshness failed.',flush=True); continue
  if not positive:print('No Discord alert: no opportunity meets minimum edge.',flush=True); continue
  best=positive[0]; fp=fingerprint(series,d,best,ph,val['ensemble_mean'])
  if fp in persistent_state['alerts']:print('No Discord alert: duplicate opportunity already alerted.',flush=True); continue
  msg=build_discord_message(city['name'],d,ph,prec,change,best,val,series); print('\nDISCORD PAPER SIGNAL:',flush=True); print(msg,flush=True)
  if send_discord_alert(msg):bot_status['discord_alerts']+=1; persistent_state['alerts'][fp]={'sent_at':utc_iso()}; persistent_state['paper_trades'].append({'timestamp_utc':utc_iso(),'city':city['name'],'series':series,'forecast_date':d,'contract_ticker':best['ticker'],'contract_interpretation':best['label'],'forecast_before':(change.get('previous') or {}).get('point_high'),'forecast_after':ph,'precipitation':prec,'ensemble_mean':ed.get('mean'),'ensemble_median':ed.get('median'),'ensemble_member_count':len(hs),'yes_ask_cents':best['ask'],'raw_ensemble_probability':best['probability'],'preliminary_edge_points':best['edge'],'settlement_result':None,'profit_loss_cents':None}); save_state()
def run_weather_scan():
 for k in ('series_checked','markets_checked','forecast_changes','positive_signals','discord_alerts'):bot_status[k]=0
 bot_status['last_scan_utc']=utc_iso(); bot_status['last_error']=None; print('\n==================================================\nSTARTING WEATHER MARKET SCAN',flush=True); print(f'UTC: {utc_iso()}',flush=True); print('==================================================',flush=True)
 for s,c in CITIES.items():
  try:bot_status['series_checked']+=1; analyze_city(s,c)
  except Exception as e:print(f'ERROR ANALYZING {s}: {e}',flush=True); bot_status['last_error']=str(e)
 bot_status['last_scan_success']=utc_iso(); print('\n==================================================\nSCAN COMPLETE',flush=True); [print(f"{label}: {bot_status[k]}",flush=True) for label,k in [('Series checked','series_checked'),('Markets checked','markets_checked'),('Forecast changes','forecast_changes'),('Positive preliminary signals','positive_signals'),('Discord alerts sent','discord_alerts')]]; print('==================================================',flush=True)
def background_scanner():
 print('Background scanner started.',flush=True)
 while True:
  try:run_weather_scan()
  except Exception as e:print(f'Background scanner error: {e}',flush=True); bot_status['last_error']=str(e)
  print(f'Waiting {SCAN_INTERVAL_SECONDS} seconds...',flush=True); time.sleep(SCAN_INTERVAL_SECONDS)
@app.route('/')
def home():return 'Weather + Kalshi paper-trading monitor is running. Use /health, /status, /paper-trades, /network-test, or /test-alert.'
@app.route('/health')
def health():
 ok,err=validate_discord_relay()
 return jsonify({
  'status':'ok',
  'bot':bot_status,
  'discord_relay_configured':bool(DISCORD_RELAY_URL),
  'discord_relay_secret_configured':bool(DISCORD_RELAY_SECRET),
  'discord_relay_valid':ok,
  'discord_relay_error':err,
  'cities':list(CITIES),
  'state_file':STATE_FILE,
  'state_version':STATE_VERSION,
  'cache_entries':len(persistent_state.get('cache',{})),
  'forecast_entries':len(persistent_state.get('forecasts',{})),
  'paper_trade_count':len(persistent_state.get('paper_trades',[])),
 })

@app.route('/status')
def status():return jsonify(bot_status)
@app.route('/paper-trades')
def paper_trades():return jsonify(persistent_state.get('paper_trades',[])[-100:])
@app.route('/debug-state')
def debug_state():return jsonify({'cache':persistent_state.get('cache',{}),'forecasts':persistent_state.get('forecasts',{}),'alerts':persistent_state.get('alerts',{})})
@app.route('/network-test')
def network_test():
 result={
  'relay_url_configured':bool(DISCORD_RELAY_URL),
  'relay_secret_configured':bool(DISCORD_RELAY_SECRET),
  'relay_url_valid':False,
  'relay_get_status':None,
  'relay_get_content_type':None,
  'relay_get_response':None,
  'error':None,
 }
 ok,err=validate_discord_relay()
 result['relay_url_valid']=ok
 if not ok:
  result['error']=err
  return jsonify(result),500
 try:
  r=requests.get(
   DISCORD_RELAY_URL,
   headers={'User-Agent':'WeatherKalshiPaperMonitor/1.0 (Render)','Accept':'application/json'},
   timeout=REQUEST_TIMEOUT,
  )
  result['relay_get_status']=r.status_code
  result['relay_get_content_type']=r.headers.get('content-type')
  try:
   data=r.json()
   if isinstance(data,dict):
    result['relay_get_response']={
     'status':data.get('status'),
     'service':data.get('service'),
     'discord_configured':data.get('discord_configured'),
     'secret_configured':data.get('secret_configured'),
    }
   else:
    result['relay_get_response']=str(data)[:500]
  except Exception:
   result['relay_get_response']=r.text[:500]
 except Exception as e:
  result['error']=str(e)
  return jsonify(result),500
 return jsonify(result)

@app.route('/test-alert')
def test_alert():
 ok,err=validate_discord_relay()
 if not ok:
  return jsonify({'success':False,'error':err}),500
 success=send_discord_alert(
  '🧪 **WEATHER BOT RELAY TEST**\n\n'
  'Render successfully reached the Google Apps Script relay.\n'
  'The relay attempted to deliver this message to Discord.\n\n'
  'This is a test message only.'
 )
 if success:
  return jsonify({'success':True,'message':'Test Discord alert sent successfully through the relay.'})
 return jsonify({
  'success':False,
  'relay_status':bot_status.get('discord_last_status'),
  'error':bot_status.get('discord_last_error'),
 }),500

load_state(); threading.Thread(target=background_scanner,daemon=True,name='weather-market-scanner').start()
if __name__=='__main__':
 port=int(os.environ.get('PORT','10000')); print(f'Starting server on port {port}',flush=True); app.run(host='0.0.0.0',port=port,debug=False)
