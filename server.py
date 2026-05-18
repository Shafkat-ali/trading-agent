from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import yfinance as yf
import requests
import smtplib
import threading
import csv
import io
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv(override=False)

app = FastAPI(title="Payda x UyghurKid Trading Agent")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

POLYGON_API_KEY    = os.getenv('POLYGON_API_KEY',    'W7T9tMZzRCsHUhJfPvL7SZOReXow4q8L')
FINNHUB_KEY        = os.getenv('FINNHUB_API_KEY',    'd812vi9r01qler4gpnmgd812vi9r01qler4gpnn0')
ALPACA_API_KEY     = os.getenv('ALPACA_API_KEY',     'PKS5O37RJFFB5S4BJMWYOE65XE')
ALPACA_SECRET_KEY  = os.getenv('ALPACA_SECRET_KEY',  'F8tASs56AzAorfKqYkNYEVgEH78pFFFUYmR5kvtnW3f9')
EMAIL_ADDRESS      = os.getenv('EMAIL_ADDRESS', '')
EMAIL_PASSWORD     = os.getenv('EMAIL_PASSWORD', '')
EMAIL_TO           = os.getenv('EMAIL_TO', '')

# ── User auth — passwords stored as SHA-256 hashes, never in plaintext ──
import hashlib
USER_PASSWORDS = {
    'shafkat': os.getenv('SHAFKAT_PW_HASH', '00c19523772ea30a3755c6fa1c5642a93e4e56e8dbcf977420ad2d4395448f5c'),
    'irfan':   os.getenv('IRFAN_PW_HASH',   '6f596603a921a50a83c118911b9774b95a5fb081600b14429a34f3e66ee84176'),
}

# Tradier — Shafkat's keys (production real-time + sandbox paper trading)
TRADIER_TOKEN         = os.getenv('TRADIER_TOKEN',        'TJuXpKVavQJ6ad8GMiot6JWISGQG')
TRADIER_SANDBOX_TOKEN = os.getenv('TRADIER_SANDBOX_TOKEN','uz4ojbfLroPKXmSrJOA1NffOXkiA')
TRADIER_SANDBOX_ACCT  = os.getenv('TRADIER_SANDBOX_ACCT', 'VA47327054')

# Tradier — Irfan's keys (separate account = separate rate limits)
TRADIER_TOKEN_IRFAN         = os.getenv('TRADIER_TOKEN_IRFAN',        'lpiaEmzUA8B7sMuqwf5vUS0Ivp8T')
TRADIER_SANDBOX_TOKEN_IRFAN = os.getenv('TRADIER_SANDBOX_TOKEN_IRFAN','AGAdfEQ5MLiAfAUNZvPg6lbuOG1q')
TRADIER_SANDBOX_ACCT_IRFAN  = os.getenv('TRADIER_SANDBOX_ACCT_IRFAN', 'VA42488972')

TRADIER_API_URL     = 'https://api.tradier.com/v1'
TRADIER_SANDBOX_URL = 'https://sandbox.tradier.com/v1'

# Headers per user — each gets their own Tradier account
TRADIER_HEADERS       = {'Authorization': f'Bearer {TRADIER_TOKEN}',              'Accept': 'application/json'}
TRADIER_SANDBOX_HDR   = {'Authorization': f'Bearer {TRADIER_SANDBOX_TOKEN}',       'Accept': 'application/json'}
TRADIER_HEADERS_IRFAN     = {'Authorization': f'Bearer {TRADIER_TOKEN_IRFAN}',         'Accept': 'application/json'}
TRADIER_SANDBOX_HDR_IRFAN = {'Authorization': f'Bearer {TRADIER_SANDBOX_TOKEN_IRFAN}', 'Accept': 'application/json'}

def get_tradier_headers(user_id: str):
    """Return the correct Tradier production headers for this user."""
    return TRADIER_HEADERS_IRFAN if user_id == 'irfan' else TRADIER_HEADERS

def get_tradier_sandbox_headers(user_id: str):
    """Return the correct Tradier sandbox headers for this user."""
    return TRADIER_SANDBOX_HDR_IRFAN if user_id == 'irfan' else TRADIER_SANDBOX_HDR

def get_tradier_sandbox_acct(user_id: str):
    """Return the correct Tradier sandbox account number for this user."""
    return TRADIER_SANDBOX_ACCT_IRFAN if user_id == 'irfan' else TRADIER_SANDBOX_ACCT

ALPACA_HEADERS = {
    'APCA-API-KEY-ID':     ALPACA_API_KEY,
    'APCA-API-SECRET-KEY': ALPACA_SECRET_KEY,
    'accept':              'application/json',
}
ALPACA_DATA_URL  = 'https://data.alpaca.markets/v2'
ALPACA_PAPER_URL = 'https://paper-api.alpaca.markets/v2'

# ── PER-USER STATE ──
user_state = {
    'shafkat': {'scan_results':[], 'alerted':set(), 'mode':'morning_gap', 'running':False,
                'filters':{'min_price':0.10,'max_price':50.00,'min_gap_pct':5.0,
                           'min_dollar_vol':250000,'min_volume':50000,'min_rvol':0,
                           'max_float_m':20.0,'max_market_cap_m':0,'require_news':False}},
    'irfan':   {'scan_results':[], 'alerted':set(), 'mode':'morning_gap', 'running':False,
                'filters':{'min_price':0.10,'max_price':50.00,'min_gap_pct':5.0,
                           'min_dollar_vol':250000,'min_volume':50000,'min_rvol':0,
                           'max_float_m':20.0,'max_market_cap_m':0,'require_news':False}},
}
user_clients = {'shafkat':[], 'irfan':[]}
alert_log    = []

# NYSE halt cache
halted_tickers = {}
halt_cache_ts  = None

# Company info cache
_company_cache = {}

SCAN_MODES = {
    'morning_gap': {
        'label':'🌅 MorningGap', 'desc':'Price $0.10–$50, Gap 5%+, $Vol $250K+, Float ≤20M',
        'min_price':0.10,'max_price':50.00,'min_gap':5.0,'min_dvol':250_000,
        'min_volume':50_000,'min_rvol':0,'max_float_m':20.0,'max_mktcap_m':0,'require_news':False,
    },
    'scanopp': {
        'label':'💡 ScanOpp', 'desc':'Price $0.50–$15, Gap 9%+, $Vol $3M+, Trades 3K+',
        'min_price':0.50,'max_price':15.00,'min_gap':9.0,'min_dvol':3_000_000,
        'min_volume':300_000,'min_rvol':0,'max_float_m':0,'max_mktcap_m':0,'require_news':False,
    },
    'standard': {
        'label':'🔍 Standard', 'desc':'Gap 10%+, $0.20–$20, $100K vol, RVOL 1.5x+',
        'min_price':0.20,'max_price':20.00,'min_gap':10.0,'min_dvol':100_000,
        'min_volume':100_000,'min_rvol':1.5,'max_float_m':50.0,'max_mktcap_m':1000.0,'require_news':False,
    },
    'supernova': {
        'label':'🚀 Supernovas', 'desc':'Gap 50%+ only — massive movers',
        'min_price':0.10,'max_price':50.00,'min_gap':50.0,'min_dvol':250_000,
        'min_volume':50_000,'min_rvol':0,'max_float_m':0,'max_mktcap_m':0,'require_news':False,
    },
    'biotech': {
        'label':'🧬 Biotech', 'desc':'Biotech/FDA, gap 15%+, catalyst required',
        'min_price':0.50,'max_price':20.00,'min_gap':15.0,'min_dvol':500_000,
        'min_volume':100_000,'min_rvol':0,'max_float_m':50.0,'max_mktcap_m':500.0,'require_news':True,
    },
    'low_float': {
        'label':'⚡ Low Float', 'desc':'Float under 10M, gap 15%+',
        'min_price':0.10,'max_price':20.00,'min_gap':15.0,'min_dvol':250_000,
        'min_volume':50_000,'min_rvol':0,'max_float_m':10.0,'max_mktcap_m':0,'require_news':False,
    },
    'premarket': {
        'label':'🌄 Pre-Market', 'desc':'Gap 10%+, vol 100K+',
        'min_price':0.10,'max_price':50.00,'min_gap':10.0,'min_dvol':100_000,
        'min_volume':100_000,'min_rvol':0,'max_float_m':0,'max_mktcap_m':0,'require_news':False,
    },
    'squeeze': {
        'label':'💎 Squeeze', 'desc':'High dollar vol $3M+, gap 9%+',
        'min_price':0.50,'max_price':15.00,'min_gap':9.0,'min_dvol':3_000_000,
        'min_volume':300_000,'min_rvol':0,'max_float_m':0,'max_mktcap_m':0,'require_news':False,
    },
    'small_cap': {
        'label':'📊 Small Cap', 'desc':'$1–$20, gap 15%+',
        'min_price':1.00,'max_price':20.00,'min_gap':15.0,'min_dvol':250_000,
        'min_volume':50_000,'min_rvol':0,'max_float_m':0,'max_mktcap_m':500.0,'require_news':False,
    },
    'afternoon': {
        'label':'🌆 Afternoon', 'desc':'HOD breakouts, gap 5%+',
        'min_price':0.10,'max_price':50.00,'min_gap':5.0,'min_dvol':250_000,
        'min_volume':50_000,'min_rvol':0,'max_float_m':0,'max_mktcap_m':0,'require_news':False,
    },
    'penny': {
        'label':'🪙 Penny', 'desc':'Under $1, gap 20%+',
        'min_price':0.01,'max_price':1.00,'min_gap':20.0,'min_dvol':100_000,
        'min_volume':50_000,'min_rvol':0,'max_float_m':0,'max_mktcap_m':0,'require_news':False,
    },
    'custom': {
        'label':'⚙️ Custom', 'desc':'Your custom filter settings',
        'min_price':0.10,'max_price':50.00,'min_gap':5.0,'min_dvol':250_000,
        'min_volume':50_000,'min_rvol':0,'max_float_m':0,'max_mktcap_m':0,'require_news':False,
    },
}

STRONG_KEYWORDS  = ['fda','approval','approved','breakthrough','contract','partnership','merger',
    'acquisition','earnings','beat','guidance','revenue','clinical','trial','phase','results',
    'patent','exclusive','launch','deal','awarded','wins','secures','signs','robot','ai',
    'technology','crypto','bitcoin']
DANGER_KEYWORDS  = ['offering','dilut','shelf','warrant','investigation','lawsuit','sec',
    'subpoena','delay','failed','withdrawn','suspended','reverse split','compliance']
BIOTECH_KEYWORDS = ['fda','trial','phase','clinical','drug','therapy','biotech',
    'pharmaceutical','approval','nda','bla','cancer','oncology']
SEC_DANGER_KEYWORDS = ['s-1','s-3','424b','offering','dilut','warrant','reverse split',
    'sec filing','8-k','going concern','non-compliance']
VALID_EXCHANGES  = {'NASDAQ','NYSE','AMEX','NYSE ARCA','NYSE MKT','BATS','CBOE'}

# ============================================================
# HELPERS
# ============================================================

def send_email(subject, body):
    def _send():
        try:
            msg = MIMEMultipart()
            msg['From'] = EMAIL_ADDRESS; msg['To'] = EMAIL_TO; msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            s = smtplib.SMTP('smtp.gmail.com', 587); s.starttls()
            s.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            s.sendmail(EMAIL_ADDRESS, EMAIL_TO, msg.as_string()); s.quit()
        except Exception as e:
            log_alert(f"⚠️ Email failed: {e}")
    threading.Thread(target=_send, daemon=True).start()

def log_alert(message):
    alert_log.insert(0, {'time': datetime.now().strftime('%H:%M:%S'), 'message': message})
    if len(alert_log) > 100: alert_log.pop()

async def broadcast_to_user(user_id, data):
    clients = user_clients.get(user_id, [])
    disconnected = []
    for client in clients:
        try:
            await client.send_json(data)
        except:
            disconnected.append(client)
    for c in disconnected:
        if c in user_clients.get(user_id, []):
            user_clients[user_id].remove(c)

# ============================================================
# ALPACA — QUOTE (replaces Finnhub quote)
# ============================================================

def alpaca_get_quote(ticker):
    """
    Returns real bid/ask/last/high/low/volume from Alpaca.
    Falls back to Finnhub if Alpaca fails.
    """
    try:
        # Latest quote (bid/ask)
        q_url = f"{ALPACA_DATA_URL}/stocks/{ticker}/quotes/latest"
        q_r   = requests.get(q_url, headers=ALPACA_HEADERS, timeout=6)

        # Latest trade (last price)
        t_url = f"{ALPACA_DATA_URL}/stocks/{ticker}/trades/latest"
        t_r   = requests.get(t_url, headers=ALPACA_HEADERS, timeout=6)

        # Latest bar (open/high/low/close/volume)
        b_url = f"{ALPACA_DATA_URL}/stocks/{ticker}/bars/latest?timeframe=1Day"
        b_r   = requests.get(b_url, headers=ALPACA_HEADERS, timeout=6)

        result = {}

        if q_r.status_code == 200:
            q = q_r.json().get('quote', {})
            result['bid']      = q.get('bp', 0)
            result['ask']      = q.get('ap', 0)
            result['bid_size'] = q.get('bs', 0)
            result['ask_size'] = q.get('as', 0)

        if t_r.status_code == 200:
            t = t_r.json().get('trade', {})
            result['last']  = t.get('p', 0)
            result['size']  = t.get('s', 0)

        if b_r.status_code == 200:
            b = b_r.json().get('bar', {})
            result['open']   = b.get('o', 0)
            result['high']   = b.get('h', 0)
            result['low']    = b.get('l', 0)
            result['close']  = b.get('c', 0)
            result['volume'] = b.get('v', 0)
            result['vwap']   = b.get('vw', 0)

        if result:
            result['source'] = 'alpaca'
            return result

    except Exception as e:
        log_alert(f"⚠️ Alpaca quote error {ticker}: {e}")

    # Fallback to Finnhub
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=8
        )
        if r.status_code == 200:
            d = r.json()
            return {
                'last':   d.get('c', 0),
                'open':   d.get('o', 0),
                'high':   d.get('h', 0),
                'low':    d.get('l', 0),
                'bid':    0,
                'ask':    0,
                'volume': 0,
                'vwap':   0,
                'source': 'finnhub',
            }
    except:
        pass

    return {}

def alpaca_get_snapshot(ticker):
    """
    Snapshot = quote + trade + daily bar in one call.
    Gives us price, RVOL, VWAP, bid/ask all at once.
    """
    try:
        url = f"{ALPACA_DATA_URL}/stocks/{ticker}/snapshot"
        r   = requests.get(url, headers=ALPACA_HEADERS, timeout=8)
        if r.status_code == 200:
            d             = r.json()
            daily_bar     = d.get('dailyBar', {})
            prev_daily    = d.get('prevDailyBar', {})
            latest_trade  = d.get('latestTrade', {})
            latest_quote  = d.get('latestQuote', {})

            price      = latest_trade.get('p', 0) or daily_bar.get('c', 0)
            prev_close = prev_daily.get('c', 0)
            volume     = daily_bar.get('v', 0)
            prev_vol   = prev_daily.get('v', 1) or 1
            pct_change = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0
            dollar_vol = price * volume
            rvol       = round(volume / prev_vol, 1) if prev_vol > 0 else 0
            vwap       = daily_bar.get('vw', 0)

            return {
                'price':      round(price, 4),
                'prev_close': round(prev_close, 4),
                'pct_change': round(pct_change, 2),
                'open':       daily_bar.get('o', 0),
                'high':       daily_bar.get('h', 0),
                'low':        daily_bar.get('l', 0),
                'volume':     volume,
                'dollar_vol': round(dollar_vol, 0),
                'rvol':       rvol,
                'vwap':       round(vwap, 4),
                'bid':        latest_quote.get('bp', 0),
                'ask':        latest_quote.get('ap', 0),
                'bid_size':   latest_quote.get('bs', 0),
                'ask_size':   latest_quote.get('as', 0),
                'source':     'alpaca',
            }
    except Exception as e:
        log_alert(f"⚠️ Alpaca snapshot error {ticker}: {e}")
    return {}

def alpaca_get_gainers():
    """
    Alpaca doesn't have a built-in gainers endpoint,
    so we use Polygon as primary and Alpaca snapshots for enrichment.
    """
    return []

# ============================================================
# NYSE HALT DETECTION
# ============================================================

def refresh_halt_cache():
    global halted_tickers, halt_cache_ts
    try:
        r = requests.get(
            'https://www.nyse.com/api/trade-halts/current/download',
            timeout=10, headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200 and r.text:
            halted_tickers = {}
            reader = csv.DictReader(io.StringIO(r.text))
            for row in reader:
                sym       = (row.get('Symbol') or row.get('symbol') or '').strip().upper()
                reason    = (row.get('Reason Halted') or row.get('reasonHalted') or 'Unknown').strip()
                halt_time = (row.get('Halt Time') or row.get('haltTime') or '').strip()
                if sym:
                    halted_tickers[sym] = {'reason': reason, 'halt_time': halt_time}
            halt_cache_ts = datetime.now()
            log_alert(f"🛑 NYSE halts: {len(halted_tickers)} stocks")
    except Exception as e:
        log_alert(f"⚠️ NYSE halt fetch: {e}")

def get_halt_info(ticker):
    global halt_cache_ts
    if halt_cache_ts is None or (datetime.now() - halt_cache_ts).seconds > 120:
        refresh_halt_cache()
    return halted_tickers.get(ticker.upper(), None)

# ============================================================
# COMPANY INFO (Finnhub — free, reliable)
# ============================================================

def get_company_info(ticker):
    if ticker in _company_cache:
        return _company_cache[ticker]
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/stock/profile2",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=6
        )
        if r.status_code == 200:
            d = r.json()
            info = {
                'company_name': d.get('name', ''),
                'exchange':     d.get('exchange', ''),
                'industry':     d.get('finnhubIndustry', ''),
                'market_cap_m': d.get('marketCapitalization', 0),
            }
            _company_cache[ticker] = info
            return info
    except:
        pass
    return {'company_name':'','exchange':'','industry':'','market_cap_m':0}

# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/api/debug")
async def debug():
    # Test Alpaca connection
    try:
        r = requests.get(f"{ALPACA_PAPER_URL}/account", headers=ALPACA_HEADERS, timeout=5)
        alpaca_ok = r.status_code == 200
        alpaca_status = r.json().get('status','unknown') if alpaca_ok else f"Error {r.status_code}"
    except Exception as e:
        alpaca_ok = False; alpaca_status = str(e)

    return {
        'alpaca_connected': alpaca_ok,
        'alpaca_status':    alpaca_status,
        'finnhub_key_set':  bool(FINNHUB_KEY),
        'polygon_key_set':  bool(POLYGON_API_KEY),
        'halted_count':     len(halted_tickers),
        'users': {uid: {'mode':s['mode'],'running':s['running'],'results':len(s['scan_results'])}
                  for uid, s in user_state.items()}
    }

@app.get("/api/scanner_diag")
async def scanner_diag():
    """Diagnose scanner data sources — call this to see exactly what's working."""
    result = {}

    # 1. Test Alpaca most_actives
    try:
        r = requests.get(f"{ALPACA_DATA_URL}/stocks/most_actives",
            headers=ALPACA_HEADERS, params={'by':'volume','top':10}, timeout=10)
        tickers = [s.get('symbol') for s in r.json().get('most_actives',[])]
        result['alpaca_most_actives'] = {'status': r.status_code, 'count': len(tickers), 'sample': tickers[:5]}
    except Exception as e:
        result['alpaca_most_actives'] = {'status': 'error', 'error': str(e)}

    # 2. Test Polygon gainers
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
            f"?apiKey={POLYGON_API_KEY}&include_otc=false", timeout=10)
        tickers = [t.get('ticker') for t in r.json().get('tickers',[])]
        result['polygon_gainers'] = {'status': r.status_code, 'count': len(tickers), 'sample': tickers[:5]}
    except Exception as e:
        result['polygon_gainers'] = {'status': 'error', 'error': str(e)}

    # 3. Test Yahoo gainers
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
            "?formatted=false&scrIds=day_gainers&count=10",
            headers={'User-Agent':'Mozilla/5.0'}, timeout=10)
        quotes = r.json().get('finance',{}).get('result',[{}])[0].get('quotes',[]) if r.status_code==200 else []
        tickers = [q.get('symbol') for q in quotes]
        result['yahoo_gainers'] = {'status': r.status_code, 'count': len(tickers), 'sample': tickers[:5]}
    except Exception as e:
        result['yahoo_gainers'] = {'status': 'error', 'error': str(e)}

    # 4. Test Tradier quotes on sample tickers
    test_syms = ['AAPL','NVDA','TSLA','SPY','QQQ']
    try:
        r = requests.get(f"{TRADIER_API_URL}/markets/quotes",
            headers=TRADIER_HEADERS,
            params={'symbols': ','.join(test_syms), 'greeks':'false'}, timeout=10)
        if r.status_code == 200:
            quotes = r.json().get('quotes',{}).get('quote',[])
            if isinstance(quotes, dict): quotes = [quotes]
            sample = [{'sym':q.get('symbol'),'last':q.get('last'),'prev':q.get('prevclose')} for q in quotes[:3]]
            result['tradier_quotes_shafkat'] = {'status': r.status_code, 'count': len(quotes), 'sample': sample}
        else:
            result['tradier_quotes_shafkat'] = {'status': r.status_code, 'body': r.text[:200]}
    except Exception as e:
        result['tradier_quotes_shafkat'] = {'status': 'error', 'error': str(e)}

    # 5. Test Tradier quotes for Irfan
    try:
        r = requests.get(f"{TRADIER_API_URL}/markets/quotes",
            headers=TRADIER_HEADERS_IRFAN,
            params={'symbols': ','.join(test_syms), 'greeks':'false'}, timeout=10)
        if r.status_code == 200:
            quotes = r.json().get('quotes',{}).get('quote',[])
            if isinstance(quotes, dict): quotes = [quotes]
            result['tradier_quotes_irfan'] = {'status': r.status_code, 'count': len(quotes)}
        else:
            result['tradier_quotes_irfan'] = {'status': r.status_code, 'body': r.text[:200]}
    except Exception as e:
        result['tradier_quotes_irfan'] = {'status': 'error', 'error': str(e)}

    # 6. Simulate full universe build
    universe, errors = get_dynamic_universe()
    result['universe'] = {'total': len(universe), 'errors': errors, 'sample': universe[:10]}

    return result

@app.get("/api/scan_modes")
async def get_scan_modes():
    return {'modes': SCAN_MODES}

@app.get("/api/halts")
async def get_halts():
    refresh_halt_cache()
    return {'halted': halted_tickers, 'count': len(halted_tickers),
            'as_of': datetime.now().strftime('%H:%M:%S')}

# ── PER USER ──

@app.post("/api/scanner/mode/{user_id}/{mode}")
async def set_scan_mode(user_id: str, mode: str):
    if user_id not in user_state: return {'error':'Unknown user'}
    if mode not in SCAN_MODES:   return {'error':f'Unknown mode: {mode}'}
    m = SCAN_MODES[mode]
    user_state[user_id]['mode'] = mode
    user_state[user_id]['filters'] = {
        'min_price':        m['min_price'],
        'max_price':        m['max_price'],
        'min_gap_pct':      m['min_gap'],
        'min_dollar_vol':   m['min_dvol'],
        'min_volume':       m['min_volume'],
        'min_rvol':         m['min_rvol'],
        'max_float_m':      m['max_float_m'],
        'max_market_cap_m': m['max_mktcap_m'],
        'require_news':     m['require_news'],
    }
    log_alert(f"🔄 [{user_id}] Mode: {m['label']}")
    return {'status':'ok','mode':mode,'filters':user_state[user_id]['filters']}

@app.post("/api/scanner/filters/{user_id}")
async def set_filters(user_id: str, filters: dict):
    if user_id not in user_state: return {'error':'Unknown user'}
    user_state[user_id]['mode'] = 'custom'
    f = user_state[user_id]['filters']
    for key in ['min_price','max_price','min_gap_pct','min_dollar_vol',
                'min_volume','min_rvol','max_float_m','max_market_cap_m']:
        if key in filters: f[key] = float(filters[key])
    if 'require_news' in filters: f['require_news'] = bool(filters['require_news'])
    log_alert(f"⚙️ [{user_id}] Custom filters applied")
    return {'status':'ok','filters':f,'mode':'custom'}

@app.get("/api/status/{user_id}")
async def get_user_status(user_id: str):
    if user_id not in user_state: return {'error':'Unknown user'}
    s = user_state[user_id]
    return {'running':s['running'],'results':len(s['scan_results']),
            'time':datetime.now().strftime('%H:%M:%S'),'mode':s['mode'],'filters':s['filters']}

@app.get("/api/results/{user_id}")
async def get_user_results(user_id: str):
    if user_id not in user_state: return {'data':[],'alerts':alert_log[:20]}
    s = user_state[user_id]
    return {'data':s['scan_results'],'alerts':alert_log[:20],
            'time':datetime.now().strftime('%H:%M:%S'),'mode':s['mode']}

@app.post("/api/scanner/start/{user_id}")
async def start_scanner(user_id: str):
    if user_id not in user_state: return {'error':'Unknown user'}
    s = user_state[user_id]
    if not s['running']:
        s['running'] = True; s['scan_results'] = []
        asyncio.create_task(scanner_loop(user_id))
        log_alert(f"✅ [{user_id}] Scanner started")
        return {'status':'started','mode':s['mode']}
    return {'status':'already running','mode':s['mode']}

@app.post("/api/scanner/stop/{user_id}")
async def stop_scanner(user_id: str):
    if user_id not in user_state: return {'error':'Unknown user'}
    user_state[user_id]['running'] = False
    log_alert(f"⏹️ [{user_id}] Scanner stopped")
    return {'status':'stopped'}

@app.post("/api/clear-alerts/{user_id}")
async def clear_alerts_route(user_id: str):
    if user_id in user_state: user_state[user_id]['alerted'].clear()
    return {'status':'cleared'}

# ── STOCK DATA — Alpaca primary, Finnhub fallback ──

@app.get("/api/stock/quote/{ticker}")
async def get_stock_quote(ticker: str):
    """Real bid/ask from Alpaca, fallback to Finnhub"""
    snap = alpaca_get_snapshot(ticker)
    if snap and snap.get('price', 0) > 0:
        # Format to match what dashboard expects (c, pc, h, l, o fields)
        return {
            'c':      snap['price'],
            'pc':     snap['prev_close'],
            'h':      snap['high'],
            'l':      snap['low'],
            'o':      snap['open'],
            'v':      snap['volume'],
            'bid':    snap['bid'],
            'ask':    snap['ask'],
            'bid_size': snap.get('bid_size', 0),
            'ask_size': snap.get('ask_size', 0),
            'vwap':   snap['vwap'],
            'rvol':   snap['rvol'],
            'source': 'alpaca',
        }
    # Finnhub fallback
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=8
        )
        if r.status_code == 200:
            d = r.json(); d['source'] = 'finnhub'; return d
    except:
        pass
    return {'error': 'Quote unavailable'}

@app.get("/api/stock/snapshot/{ticker}")
async def get_stock_snapshot(ticker: str):
    """Full Alpaca snapshot — price, RVOL, VWAP, bid/ask"""
    snap = alpaca_get_snapshot(ticker)
    return snap if snap else {'error': 'Snapshot unavailable'}

@app.get("/api/stock/bars/{ticker}")
async def get_stock_bars(ticker: str, timeframe: str = "5Min", days: int = 1, start: str = "", range: str = "1D", user: str = "shafkat"):
    """
    OHLCV bars — Tradier primary with full interval aggregation.
    Tradier native: 1min, 5min, 15min, 30min, daily
    Aggregated:     2min, 3min, 10min, 1h, 2h, 4h (built from 1min or 5min base)
    """
    from datetime import datetime, timedelta
    import math

    ticker = ticker.upper().strip()

    # ── Tradier native intervals ONLY ──
    NATIVE_MAP = {
        '1Min': '1min', '5Min': '5min', '15Min': '15min', '1Day': 'daily',
        '1m':   '1min', '5m':   '5min', '15m':   '15min', '1D':   'daily',
    }
    tradier_interval  = NATIVE_MAP.get(timeframe, '5min')
    needs_aggregation = False
    agg_n             = 1

    # ── Date range — add buffer for weekends/holidays ──
    now = datetime.now()
    if start:
        # Frontend sent a start date — push back 3 extra days as buffer for weekends
        start_dt  = datetime.strptime(start, '%Y-%m-%d') - timedelta(days=3)
        start_str = start_dt.strftime('%Y-%m-%d') + ' 04:00'
    elif days > 1:
        start_dt  = now - timedelta(days=int(days) + 3)  # +3 day buffer
        start_str = start_dt.strftime('%Y-%m-%d') + ' 04:00'
    else:
        # 1D — go back 5 days to ensure we always get data regardless of weekends
        start_dt  = now - timedelta(days=5)
        start_str = start_dt.strftime('%Y-%m-%d') + ' 04:00'
    end_str = now.strftime('%Y-%m-%d') + ' 20:00'

    def aggregate_bars(raw_bars, n):
        """Aggregate n consecutive 1/5/15min bars into one candle."""
        result = []
        for i in range(0, len(raw_bars), n):
            chunk = raw_bars[i:i+n]
            if not chunk: continue
            result.append({
                'time':   chunk[0]['time'],
                'open':   chunk[0]['open'],
                'high':   max(b['high'] for b in chunk),
                'low':    min(b['low']  for b in chunk),
                'close':  chunk[-1]['close'],
                'volume': sum(b['volume'] for b in chunk),
                'vwap':   round(sum(b['vwap']*b['volume'] for b in chunk if b['vwap']>0)
                                / max(sum(b['volume'] for b in chunk if b['vwap']>0), 1), 4),
            })
        return result

    # ── Try Tradier ──
    try:
        params = {
            'symbol':         ticker,
            'interval':       tradier_interval,
            'start':          start_str,
            'end':            end_str,
            'session_filter': 'all',
        }
        # Daily bars use /markets/history not /markets/timesales
        if tradier_interval == 'daily':
            params = {'symbol': ticker, 'start': start_str[:10], 'end': end_str[:10], 'interval': 'daily'}
            r = requests.get(f"{TRADIER_API_URL}/markets/history", headers=get_tradier_headers(user), params=params, timeout=12)
        else:
            r = requests.get(f"{TRADIER_API_URL}/markets/timesales", headers=get_tradier_headers(user), params=params, timeout=12)

        log_alert(f"📊 Tradier [{ticker}] {tradier_interval} agg={agg_n} HTTP {r.status_code}")

        if r.status_code == 200:
            data = r.json()
            if tradier_interval == 'daily':
                raw = data.get('history', {}).get('day', []) or []
                if isinstance(raw, dict): raw = [raw]
                bars_raw = [{'time': b.get('date',''), 'open': float(b.get('open',0)),
                             'high': float(b.get('high',0)), 'low': float(b.get('low',0)),
                             'close': float(b.get('close',0)), 'volume': int(b.get('volume',0)), 'vwap': 0}
                            for b in raw]
            else:
                series = data.get('series') or {}
                raw    = series.get('data', []) if series else []
                if isinstance(raw, dict): raw = [raw]
                bars_raw = [{'time': b.get('time',''), 'open': float(b.get('open',0)),
                             'high': float(b.get('high',0)), 'low': float(b.get('low',0)),
                             'close': float(b.get('close',0)), 'volume': int(b.get('volume',0)),
                             'vwap': float(b.get('vwap',0)) if b.get('vwap') else 0}
                            for b in raw]

            if bars_raw:
                bars = aggregate_bars(bars_raw, agg_n) if needs_aggregation else bars_raw
                log_alert(f"📊 Tradier [{ticker}]: {len(bars_raw)} raw → {len(bars)} {timeframe} bars")
                return {'ticker': ticker, 'timeframe': timeframe, 'bars': bars,
                        'source': f'tradier({tradier_interval}{"→agg×"+str(agg_n) if needs_aggregation else ""})',
                        'count': len(bars)}

        log_alert(f"⚠️ Tradier empty for {ticker}, trying Alpaca...")
    except Exception as e:
        log_alert(f"⚠️ Tradier error [{ticker}]: {e}")

    # ── Fallback: Alpaca ──
    try:
        now   = datetime.now()
        # Use same start_str calculated above (already has buffer)
        s_dt  = datetime.strptime(start_str[:10], '%Y-%m-%d').replace(hour=4, minute=0, second=0, microsecond=0)
        e_dt  = now.replace(hour=20, minute=0, second=0, microsecond=0)

        alpaca_tf = {'1Min':'1Min','5Min':'5Min','15Min':'15Min','30Min':'30Min',
                     '1Hour':'1Hour','1Day':'1Day'}.get(timeframe, '5Min')
        url = (f"{ALPACA_DATA_URL}/stocks/{ticker}/bars"
               f"?timeframe={alpaca_tf}"
               f"&start={s_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
               f"&end={e_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
               f"&feed=sip&limit=2000")
        r = requests.get(url, headers=ALPACA_HEADERS, timeout=10)
        if r.status_code == 200:
            raw_bars = r.json().get('bars', [])
            bars_raw = [{'time': b.get('t',''), 'open': round(b.get('o',0),4),
                         'high': round(b.get('h',0),4), 'low': round(b.get('l',0),4),
                         'close': round(b.get('c',0),4), 'volume': b.get('v',0),
                         'vwap': round(b.get('vw',0),4)} for b in raw_bars]
            bars = aggregate_bars(bars_raw, agg_n) if needs_aggregation else bars_raw
            log_alert(f"📊 Alpaca [{ticker}]: {len(bars)} {timeframe} bars")
            return {'ticker': ticker, 'timeframe': timeframe, 'bars': bars,
                    'source': 'alpaca', 'count': len(bars)}
        log_alert(f"⚠️ Alpaca HTTP {r.status_code} for {ticker}")
    except Exception as e:
        log_alert(f"⚠️ Alpaca error [{ticker}]: {e}")

    return {'ticker': ticker, 'timeframe': timeframe, 'bars': [],
            'source': 'none', 'count': 0,
            'error': 'No data from Tradier or Alpaca'}

    # ── Try Tradier first ──
    try:
        from datetime import datetime, timedelta
        now = datetime.now()

        # Determine date range from query params
        if start:  # explicit start date passed from frontend
            start_str = start + ' 04:00'
        elif days > 1:
            start_dt = now - timedelta(days=days)
            start_str = start_dt.strftime('%Y-%m-%d') + ' 04:00'
        else:  # default: today from 4am
            start_str = now.strftime('%Y-%m-%d') + ' 04:00'

        end_str = now.strftime('%Y-%m-%d') + ' 20:00'

        params = {
            'symbol':         ticker,
            'interval':       tradier_interval,
            'start':          start_str,
            'end':            end_str,
            'session_filter': 'all',
        }
        r = requests.get(
            f"{TRADIER_API_URL}/markets/timesales",
            headers=TRADIER_HEADERS,
            params=params,
            timeout=10
        )
        log_alert(f"📊 Tradier bars [{ticker}] HTTP {r.status_code}")

        if r.status_code == 200:
            data  = r.json()
            series = data.get('series') or {}
            raw   = series.get('data', []) if series else []
            if isinstance(raw, dict): raw = [raw]  # single candle returns dict not list

            if raw:
                bars = []
                for b in raw:
                    bars.append({
                        'time':   b.get('time', ''),
                        'open':   float(b.get('open',  0)),
                        'high':   float(b.get('high',  0)),
                        'low':    float(b.get('low',   0)),
                        'close':  float(b.get('close', 0)),
                        'volume': int(b.get('volume',  0)),
                        'vwap':   float(b.get('vwap',  0)) if b.get('vwap') else 0,
                    })
                log_alert(f"📊 Tradier bars [{ticker}]: {len(bars)} candles")
                return {'ticker': ticker, 'timeframe': timeframe, 'bars': bars,
                        'source': 'tradier', 'count': len(bars)}

        log_alert(f"⚠️ Tradier bars empty for {ticker}, trying Alpaca...")
    except Exception as e:
        log_alert(f"⚠️ Tradier bars error [{ticker}]: {e}")

    # ── Fallback: Alpaca ──
    try:
        from datetime import datetime, timedelta
        now   = datetime.now()
        start = now.replace(hour=4,  minute=0, second=0, microsecond=0)
        end   = now.replace(hour=20, minute=0, second=0, microsecond=0)
        if now.hour < 4:
            start = (now - timedelta(days=1)).replace(hour=4,  minute=0, second=0, microsecond=0)
            end   = (now - timedelta(days=1)).replace(hour=20, minute=0, second=0, microsecond=0)

        alpaca_tf = {'1Min':'1Min','5Min':'5Min','15Min':'15Min','1Hour':'1Hour','1D':'1Day'}.get(timeframe,'5Min')
        url = (f"{ALPACA_DATA_URL}/stocks/{ticker}/bars"
               f"?timeframe={alpaca_tf}"
               f"&start={start.strftime('%Y-%m-%dT%H:%M:%SZ')}"
               f"&end={end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
               f"&feed=sip&limit=1000")
        r = requests.get(url, headers=ALPACA_HEADERS, timeout=10)

        if r.status_code == 200:
            raw_bars = r.json().get('bars', [])
            bars = [{'time': b.get('t',''), 'open': round(b.get('o',0),4),
                     'high': round(b.get('h',0),4), 'low': round(b.get('l',0),4),
                     'close': round(b.get('c',0),4), 'volume': b.get('v',0),
                     'vwap': round(b.get('vw',0),4)} for b in raw_bars]
            log_alert(f"📊 Alpaca bars [{ticker}]: {len(bars)} candles")
            return {'ticker': ticker, 'timeframe': timeframe, 'bars': bars,
                    'source': 'alpaca', 'count': len(bars)}

        log_alert(f"⚠️ Alpaca bars HTTP {r.status_code} for {ticker}")
    except Exception as e:
        log_alert(f"⚠️ Alpaca bars error [{ticker}]: {e}")

    return {'ticker': ticker, 'timeframe': timeframe, 'bars': [],
            'source': 'none', 'count': 0,
            'error': 'No data from Tradier or Alpaca — market may be closed or ticker invalid'}

@app.get("/api/stock/profile/{ticker}")
async def get_stock_profile(ticker: str):
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/stock/profile2",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=8
        )
        return r.json() if r.status_code == 200 else {'error': f'Status {r.status_code}'}
    except Exception as e:
        return {'error': str(e)}

# ── Original profile endpoint ──
# PAPER TRADING — Tradier Sandbox (VA47327054)
# 15-min delayed data, $100K virtual funds
# ══════════════════════════════════════════════════════════

@app.get("/api/paper/account")
async def paper_account():
    """Get paper trading account balance and positions"""
    try:
        r = requests.get(f"{TRADIER_SANDBOX_URL}/accounts/{TRADIER_SANDBOX_ACCT}/balances",
                         headers=TRADIER_SANDBOX_HDR, timeout=8)
        if r.status_code == 200:
            return r.json()
        return {'error': f'Tradier sandbox HTTP {r.status_code}'}
    except Exception as e:
        return {'error': str(e)}

@app.get("/api/paper/positions")
async def paper_positions():
    """Get open paper trading positions"""
    try:
        r = requests.get(f"{TRADIER_SANDBOX_URL}/accounts/{TRADIER_SANDBOX_ACCT}/positions",
                         headers=TRADIER_SANDBOX_HDR, timeout=8)
        if r.status_code == 200:
            return r.json()
        return {'error': f'HTTP {r.status_code}'}
    except Exception as e:
        return {'error': str(e)}

@app.get("/api/paper/orders")
async def paper_orders():
    """Get paper trading order history"""
    try:
        r = requests.get(f"{TRADIER_SANDBOX_URL}/accounts/{TRADIER_SANDBOX_ACCT}/orders",
                         headers=TRADIER_SANDBOX_HDR, timeout=8)
        if r.status_code == 200:
            return r.json()
        return {'error': f'HTTP {r.status_code}'}
    except Exception as e:
        return {'error': str(e)}

@app.delete("/api/paper/order/{order_id}")
async def cancel_paper_order(order_id: str, user: str = 'shafkat'):
    """Cancel a pending paper trading order"""
    try:
        r = requests.delete(
            f"{TRADIER_SANDBOX_URL}/accounts/{get_tradier_sandbox_acct(user)}/orders/{order_id}",
            headers=get_tradier_sandbox_headers(user), timeout=8
        )
        log_alert(f"🗑 Cancel order {order_id} — HTTP {r.status_code}")
        if r.status_code == 200:
            return r.json()
        return {'error': f'HTTP {r.status_code}: {r.text[:200]}'}
    except Exception as e:
        return {'error': str(e)}

class LoginRequest(BaseModel):
    user_id:  str
    password: str

@app.post("/api/auth/login")
async def auth_login(req: LoginRequest):
    uid = req.user_id.lower().strip()
    if uid not in USER_PASSWORDS:
        return {'success': False, 'error': 'Unknown user'}
    pw_hash = hashlib.sha256(req.password.encode()).hexdigest()
    if pw_hash == USER_PASSWORDS[uid]:
        log_alert(f"🔐 Login: {uid}")
        return {'success': True, 'user_id': uid}
    return {'success': False, 'error': 'Incorrect password'}

class PaperOrderRequest(BaseModel):
    ticker:   str
    side:     str   # 'buy' or 'sell'
    qty:      float
    order_type: str = 'market'   # 'market' or 'limit'
    limit_price: float = 0.0
    duration:   str = 'day'
    user_id:  str = 'shafkat'

@app.post("/api/paper/order")
async def paper_order(req: PaperOrderRequest):
    """Place a paper trade on Tradier sandbox"""
    try:
        ticker = req.ticker.upper().strip()
        if not ticker:
            return {'error': 'Ticker required'}
        if req.side not in ('buy','sell'):
            return {'error': 'Side must be buy or sell'}
        if req.qty <= 0:
            return {'error': 'Quantity must be positive'}

        # Tradier extended hours rules:
        # pre/post duration MUST use limit orders only
        duration = req.duration
        order_type = req.order_type

        if duration in ('pre', 'post') and order_type != 'limit':
            return {'error': f'Pre/Post market orders must be Limit orders — market orders are not allowed in extended hours'}

        if duration in ('pre', 'post') and req.limit_price <= 0:
            return {'error': f'Pre/Post market orders require a limit price'}

        payload = {
            'class':    'equity',
            'symbol':   ticker,
            'side':     req.side,
            'quantity': str(int(req.qty)),
            'type':     order_type,
            'duration': duration,
        }
        if order_type in ('limit', 'stop_limit') and req.limit_price > 0:
            payload['price'] = str(req.limit_price)
        if order_type in ('stop', 'stop_limit') and req.limit_price > 0:
            payload['stop'] = str(req.limit_price)

        r = requests.post(
            f"{TRADIER_SANDBOX_URL}/accounts/{get_tradier_sandbox_acct(req.user_id if req.user_id else 'shafkat')}/orders",
            headers={**get_tradier_sandbox_headers(req.user_id), 'Content-Type':'application/x-www-form-urlencoded'},
            data=payload, timeout=10
        )

        # Log full response so we can debug any 403/400
        log_alert(f"📝 Paper {req.side.upper()} {int(req.qty)}x {ticker} [{order_type}/{duration}] — HTTP {r.status_code} — {r.text[:300]}")

        if r.status_code == 403:
            return {'error': f'Tradier rejected order (403) — Note: Tradier Sandbox does NOT support pre/post market extended hours orders. Use a funded live account for extended hours trading. Response: {r.text[:200]}'}

        if r.status_code in (200, 201):
            return r.json()

        return {'error': f'HTTP {r.status_code}: {r.text[:300]}'}

    except Exception as e:
        return {'error': str(e)}
        return r.json() if r.status_code in (200,201) else {'error': f'HTTP {r.status_code}: {r.text[:200]}'}
    except Exception as e:
        return {'error': str(e)}

@app.get("/api/paper/quote/{ticker}")
async def paper_quote(ticker: str):
    """Get real-time Tradier quote (production key = real-time)"""
    try:
        r = requests.get(f"{TRADIER_API_URL}/markets/quotes",
                         headers=TRADIER_HEADERS,
                         params={'symbols': ticker.upper(), 'greeks': 'false'},
                         timeout=8)
        if r.status_code == 200:
            return r.json()
        return {'error': f'HTTP {r.status_code}'}
    except Exception as e:
        return {'error': str(e)}

@app.get("/api/paper/history/{ticker}")
async def paper_history(ticker: str, interval: str = '5min'):
    """Get Tradier historical bars for paper trading chart"""
    try:
        r = requests.get(f"{TRADIER_API_URL}/markets/timesales",
                         headers=TRADIER_HEADERS,
                         params={'symbol': ticker.upper(), 'interval': interval,
                                 'session_filter': 'all'},
                         timeout=10)
        if r.status_code == 200:
            return r.json()
        return {'error': f'HTTP {r.status_code}'}
    except Exception as e:
        return {'error': str(e)}

# ── Original profile endpoint ──
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/stock/profile2",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=8
        )
        return r.json() if r.status_code == 200 else {'error': f'Status {r.status_code}'}
    except Exception as e:
        return {'error': str(e)}

@app.get("/api/stock/metrics/{ticker}")
async def get_stock_metrics(ticker: str):
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/stock/metric",
            params={'symbol': ticker, 'metric': 'all', 'token': FINNHUB_KEY},
            timeout=8
        )
        finnhub_data = r.json() if r.status_code == 200 else {}
        m = finnhub_data.get('metric', {})
        # yfinance fallback for float + short
        if not m.get('float') or not m.get('shortInterestRatio'):
            try:
                info = yf.Ticker(ticker).info
                if not m.get('float'):
                    yf_float = info.get('floatShares', 0)
                    if yf_float: m['float'] = round(yf_float / 1_000_000, 2)
                if not m.get('shortInterestRatio'):
                    yf_short = info.get('shortRatio', 0)
                    if yf_short: m['shortInterestRatio'] = round(yf_short, 2)
                if not m.get('shortPercentFloat'):
                    yf_spf = info.get('shortPercentOfFloat', 0)
                    if yf_spf: m['shortPercentFloat'] = round(yf_spf * 100, 2)
                finnhub_data['metric'] = m
            except:
                pass
        return finnhub_data
    except Exception as e:
        return {'error': str(e)}

@app.get("/api/stock/news/{ticker}")
async def get_stock_news(ticker: str):
    try:
        today     = datetime.now()
        from_date = (today - timedelta(days=5)).strftime('%Y-%m-%d')
        to_date   = today.strftime('%Y-%m-%d')
        r = requests.get(
            "https://finnhub.io/api/v1/company-news",
            params={'symbol': ticker, 'from': from_date, 'to': to_date, 'token': FINNHUB_KEY},
            timeout=8
        )
        return {'news': r.json()} if r.status_code == 200 else {'news': []}
    except Exception as e:
        return {'news': [], 'error': str(e)}

@app.get("/api/stock/sec/{ticker}")
async def get_sec_filings(ticker: str):
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/stock/filings",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=8
        )
        filings = r.json() if r.status_code == 200 else []
        danger  = []
        for f in filings[:20]:
            form = (f.get('form') or '').upper()
            desc = (f.get('description') or '').lower()
            if any(d in form for d in ['S-1','S-3','424B','8-K']) or \
               any(kw in desc for kw in SEC_DANGER_KEYWORDS):
                f['danger'] = True
            danger.append(f)
        return {'filings': danger}
    except Exception as e:
        return {'filings': [], 'error': str(e)}

@app.get("/api/news/market")
async def get_market_news():
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/news",
            params={'category': 'general', 'token': FINNHUB_KEY},
            timeout=8
        )
        return {'news': r.json()[:30] if r.status_code == 200 else []}
    except Exception as e:
        return {'news': [], 'error': str(e)}

@app.get("/api/news/sec")
async def get_sec_news():
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/news",
            params={'category': 'merger', 'token': FINNHUB_KEY},
            timeout=8
        )
        news = r.json() if r.status_code == 200 else []
        tagged = []
        for n in news[:30]:
            headline   = (n.get('headline') or '').lower()
            n['danger'] = any(kw in headline for kw in SEC_DANGER_KEYWORDS)
            tagged.append(n)
        return {'news': tagged}
    except Exception as e:
        return {'news': [], 'error': str(e)}

@app.get("/api/stock/search/{ticker}")
async def search_ticker(ticker: str):
    try:
        ticker = ticker.upper().strip()

        # Use Alpaca snapshot as primary
        snap = alpaca_get_snapshot(ticker)

        if snap and snap.get('price', 0) > 0:
            price      = snap['price']
            prev_close = snap['prev_close']
            pct_change = snap['pct_change']
            high       = snap['high']
            low        = snap['low']
            open_price = snap['open']
            volume     = snap['volume']
            dollar_vol = snap['dollar_vol']
            rvol       = snap['rvol']
            vwap       = snap['vwap']
        else:
            # Fallback to Finnhub + yfinance
            quote_r = requests.get(
                "https://finnhub.io/api/v1/quote",
                params={'symbol': ticker, 'token': FINNHUB_KEY},
                timeout=8
            )
            quote      = quote_r.json() if quote_r.status_code == 200 else {}
            price      = quote.get('c', 0)
            prev_close = quote.get('pc', 0)
            high       = quote.get('h', 0)
            low        = quote.get('l', 0)
            open_price = quote.get('o', 0)
            if not price or price == 0:
                info       = yf.Ticker(ticker).fast_info
                price      = getattr(info, 'last_price', 0) or 0
                prev_close = getattr(info, 'previous_close', 0) or 0
            pct_change = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0
            stock      = yf.Ticker(ticker)
            hist_1m    = stock.history(period="1d",  interval="1m")
            hist_10d   = stock.history(period="10d", interval="1d")
            volume     = float(hist_1m['Volume'].sum()) if not hist_1m.empty else 0
            avg_vol    = float(hist_10d['Volume'].mean()) if not hist_10d.empty else 0
            dollar_vol = price * volume
            rvol       = round(volume / avg_vol, 1) if avg_vol > 0 else 0
            vwap       = 0

        if not price or price == 0:
            return {'error': f'Could not find ticker {ticker}'}

        # Company info from Finnhub
        info         = get_company_info(ticker)
        company_name = info.get('company_name', '')
        exchange     = info.get('exchange', '')
        industry     = info.get('industry', '')
        market_cap_m = info.get('market_cap_m', 0)

        # Historical closes for pattern detection
        try:
            hist   = yf.Ticker(ticker).history(period="10d", interval="1d")
            closes = hist['Close'].values.tolist() if not hist.empty else []
        except:
            closes = []

        # Spread from real bid/ask
        bid       = snap.get('bid', 0) if snap else 0
        ask       = snap.get('ask', 0) if snap else 0
        spread_pct = round(((ask - bid) / ((ask + bid) / 2)) * 100, 2) if bid > 0 and ask > 0 else 0

        halt_info                              = get_halt_info(ticker)
        pattern, pattern_desc, pattern_criteria = detect_sykes_pattern(
            ticker, price, prev_close, pct_change, closes, rvol, dollar_vol)
        grade, notes                            = grade_setup(pct_change, dollar_vol, rvol)
        strength, news_count, headlines, warning = check_catalyst(ticker)

        final_grade = grade
        if warning:                                  final_grade = "D"
        elif strength == 'STRONG' and grade == 'A': final_grade = "A+"
        elif strength == 'NONE':
            if grade == 'A+':  final_grade = 'A'
            elif grade == 'A': final_grade = 'B'

        catalyst_label = (
            f"☠️ Danger — {news_count} article(s)" if warning else
            f"🔥 Strong Catalyst — {news_count}" if strength == 'STRONG' else
            f"✅ Moderate — {news_count}" if strength == 'MODERATE' else
            f"📰 {news_count} article(s) in 5 days" if news_count > 0 else
            "📰 0 articles in past 5 days"
        )

        entry_low  = round(price * 0.99, 2)
        entry_high = round(price * 1.02, 2)
        stop_loss  = round(entry_low * 0.95, 2)
        target1    = round(entry_high * 1.10, 2)
        target2    = round(entry_high * 1.20, 2)
        target3    = round(entry_high * 1.30, 2)

        # Above VWAP flag
        above_vwap = price > vwap if vwap > 0 else None

        return {
            'ticker':           ticker,
            'company_name':     company_name,
            'exchange':         exchange,
            'industry':         industry,
            'market_cap_m':     round(market_cap_m, 2),
            'price':            round(price, 2),
            'prev_close':       round(prev_close, 2),
            'gap_pct':          round(pct_change, 1),
            'high':             round(high, 2),
            'low':              round(low, 2),
            'open':             round(open_price, 2),
            'dollar_vol':       round(dollar_vol, 0),
            'volume':           round(volume, 0),
            'rvol':             rvol,
            'vwap':             round(vwap, 2),
            'above_vwap':       above_vwap,
            'bid':              bid,
            'ask':              ask,
            'spread_pct':       spread_pct,
            'grade':            final_grade,
            'notes':            notes,
            'catalyst':         catalyst_label,
            'news_count':       news_count,
            'headlines':        headlines[:2],
            'warning':          warning,
            'halt_info':        halt_info,
            'pattern':          pattern,
            'pattern_desc':     pattern_desc,
            'pattern_criteria': pattern_criteria,
            'entry_low':        entry_low,
            'entry_high':       entry_high,
            'stop_loss':        stop_loss,
            'target1':          target1,
            'target2':          target2,
            'target3':          target3,
            'source':           'alpaca' if snap else 'finnhub',
            'time':             datetime.now().strftime('%H:%M:%S'),
        }
    except Exception as e:
        return {'error': str(e)}

# ============================================================
# PATTERN DETECTION
# ============================================================

def detect_sykes_pattern(ticker, price, prev_close, pct_change, closes, vol_ratio, dollar_vol):
    n = len(closes)

    if pct_change >= 50:
        return ("🚀 Supernova",
            "Stock exploded 50%+ in one session — classic Sykes supernova.",
            [f"✅ Up {pct_change:.1f}% today",
             f"✅ RVOL {vol_ratio:.1f}x" if vol_ratio>=3 else f"⚠️ RVOL {vol_ratio:.1f}x (want 3x+)",
             "📌 Buy momentum OR wait for first red day to short",
             "📌 Exit: Sell into strength — supernovas crash fast",
             "⚠️ Can crash 90% in one day — always use stop loss"])

    if n >= 5:
        recent_high   = max(closes[-5:])
        recent_run    = ((recent_high - closes[-5]) / closes[-5] * 100) if closes[-5] > 0 else 0
        dip_from_high = ((price - recent_high) / recent_high * 100) if recent_high > 0 else 0
        if recent_run >= 50 and dip_from_high <= -20 and pct_change > 0:
            return ("🎯 Morning Panic Dip Buy",
                "Sykes #1 favorite. Stock ran 50%+ recently then panicked hard at open.",
                [f"✅ Ran {recent_run:.0f}% recently",
                 f"✅ Down {abs(dip_from_high):.0f}% from high",
                 f"✅ Bouncing {pct_change:.1f}% today",
                 "📌 Wait for selling to STOP before buying",
                 "📌 Double bottom = stronger bounce",
                 "📌 Exit: Quick scalp — sell into bounce"])

    if n >= 4:
        prev_days_red = all(closes[i] < closes[i-1] for i in range(-3, -1))
        if prev_days_red and pct_change > 5:
            return ("🟢 First Green Day",
                "First green candle after multiple red days. Jack Kellogg's favorite ($22M+).",
                [f"✅ First green day up {pct_change:.1f}%",
                 f"✅ RVOL {vol_ratio:.1f}x" if vol_ratio>=2 else f"⚠️ RVOL {vol_ratio:.1f}x (want 2x+)",
                 "📌 Buy dips on run-up — never chase the spike",
                 "📌 If closing near HOD → overnight hold for gap up"])

    if n >= 6:
        green_days = sum(1 for i in range(-5, 0) if closes[i] > closes[i-1])
        total_run  = ((closes[-1] - closes[-6]) / closes[-6] * 100) if closes[-6] > 0 else 0
        if green_days >= 3 and pct_change > 0 and total_run >= 30:
            return ("📈 Multi-Day Breakout",
                "Running consecutively multiple days. Buy dips along the run.",
                [f"✅ {green_days} of last 5 days green",
                 f"✅ Total {total_run:.0f}% run",
                 "📌 Buy 10-20% dips off morning highs",
                 "📌 Exit: Sell into strength"])

    if pct_change >= 15 and vol_ratio >= 3:
        return ("⚡ Gap and Go",
            "Gapped up on catalyst with RVOL 3x+.",
            [f"✅ Gapped up {pct_change:.1f}%",
             f"✅ RVOL {vol_ratio:.1f}x",
             "📌 Only trade with strong catalyst",
             "📌 Entry: Buy first pullback after open",
             "📌 Exit: T1 +10%, T2 +20%, T3 +30%"])

    if n >= 5 and pct_change >= 5:
        recent_max = max(closes[-5:])
        flagpole   = ((recent_max - closes[-5]) / closes[-5] * 100) if closes[-5] > 0 else 0
        flag_range = ((recent_max - min(closes[-3:])) / recent_max * 100) if recent_max > 0 else 0
        if flagpole >= 15 and flag_range <= 15 and pct_change < flagpole * 0.5:
            return ("🏳️ Bull Flag / Pennant",
                "Spike + tight consolidation + breakout.",
                [f"✅ Flagpole: {flagpole:.0f}% spike",
                 f"✅ Consolidation: {flag_range:.1f}% range",
                 "📌 Wait for confirmed breakout above flag top",
                 "⚠️ Failed breakouts look identical — cut fast"])

    if n >= 6 and pct_change >= 3:
        step_pattern = all(closes[i] > closes[i-1] * 0.93 for i in range(-4, -1))
        total_climb  = ((closes[-1] - closes[-6]) / closes[-6] * 100) if closes[-6] > 0 else 0
        if step_pattern and total_climb >= 20:
            return ("🪜 Stair Stepper",
                "Rises progressively with brief pullbacks.",
                [f"✅ Progressive {total_climb:.0f}% uptrend",
                 "📌 Buy pullbacks, sell into spikes"])

    if n >= 5:
        recent_high_5d = max(closes[-5:])
        if price >= recent_high_5d * 0.97 and vol_ratio >= 2 and pct_change > 5:
            return ("💥 Breakout",
                "Breaking above recent resistance with volume confirmation.",
                [f"✅ Price near 5-day high (${recent_high_5d:.2f})",
                 f"✅ RVOL {vol_ratio:.1f}x",
                 "📌 Only buy with volume — no volume = fake breakout"])

    if n >= 7:
        mid        = n // 2
        left_high  = max(closes[:mid]) if mid > 0 else 0
        cup_bottom = min(closes[mid-2:mid+2]) if mid > 2 else 0
        right_high = max(closes[mid:]) if mid < n else 0
        cup_depth  = ((left_high - cup_bottom) / left_high * 100) if left_high > 0 else 0
        recovery   = ((right_high - cup_bottom) / cup_bottom * 100) if cup_bottom > 0 else 0
        if cup_depth >= 15 and cup_depth <= 60 and recovery >= 50 and pct_change > 0:
            return ("☕ Cup and Handle",
                "U-shaped recovery + handle consolidation + breakout.",
                [f"✅ Cup depth: {cup_depth:.0f}%",
                 f"✅ Recovery: {recovery:.0f}%",
                 "📌 Wait for handle breakout with volume"])

    now_hour = datetime.now().hour
    if 13 <= now_hour <= 16 and pct_change >= 5 and vol_ratio >= 2:
        return ("🌆 Afternoon Breakout",
            "Breaking above morning HOD in afternoon.",
            [f"✅ Up {pct_change:.1f}% in afternoon",
             f"✅ RVOL {vol_ratio:.1f}x",
             "📌 Draw line at morning HOD — if it breaks, buy it"])

    if 15 <= now_hour <= 16 and n >= 3:
        prev_red = closes[-2] < closes[-3] if n >= 3 else False
        if pct_change >= 10 and prev_red:
            return ("🌙 Overnight Hold Setup",
                "Closing strong near HOD on first green day.",
                [f"✅ Up {pct_change:.1f}% near HOD",
                 "📌 Only hold overnight if within 10% of HOD",
                 "⚠️ Bad news overnight = gap DOWN"])

    if n >= 3 and pct_change >= 3:
        dip1 = ((closes[-2] - closes[-3]) / closes[-3] * 100) if closes[-3] > 0 else 0
        if dip1 <= -10:
            return ("📉 Double Bottom Morning Panic",
                "Two panic lows forming support.",
                [f"✅ First panic: {dip1:.0f}%",
                 f"✅ Bouncing {pct_change:.1f}%",
                 "📌 Buy the SECOND bottom, not the first",
                 "⚠️ If second bottom breaks, exit immediately"])

    if n >= 3 and pct_change < -10:
        prev_was_big_up = closes[-2] > closes[-3] * 1.2 if n >= 3 else False
        if prev_was_big_up:
            return ("🔴 Supernova Fade (Short)",
                "First red day after big run.",
                [f"✅ Down {abs(pct_change):.1f}% after big run",
                 "📌 Short the first red day",
                 "⚠️ Short squeezes brutal — keep position small"])

    if n >= 5 and pct_change < -5:
        consec_red = sum(1 for i in range(-4, 0) if closes[i] < closes[i-1])
        if consec_red >= 3:
            return ("🦅 The Crow — AVOID",
                "Continuous selling pressure.",
                [f"⚠️ Down {abs(pct_change):.1f}% — downtrend",
                 "🚨 DO NOT trade The Crow on the long side"])

    if pct_change >= 10:
        return ("📊 Momentum Play",
            "Decent move but no clean Sykes pattern.",
            [f"⚠️ Up {pct_change:.1f}% — no clean pattern",
             "📌 Wait for cleaner setup"])

    return ("🔍 No Clear Pattern",
        "Does not match any defined Sykes setup.",
        ["⚠️ Move too small for Sykes patterns",
         "🚨 SKIP — wait for better opportunity"])

# ============================================================
# CATALYST CHECK
# ============================================================

def check_catalyst(ticker):
    try:
        today     = datetime.now()
        from_date = (today - timedelta(days=5)).strftime('%Y-%m-%d')
        to_date   = today.strftime('%Y-%m-%d')
        r = requests.get(
            'https://finnhub.io/api/v1/company-news',
            params={'symbol': ticker, 'from': from_date, 'to': to_date, 'token': FINNHUB_KEY},
            timeout=8
        )
        news_items = r.json()[:15] if r.status_code == 200 else []
        if not news_items:
            yf_news = yf.Ticker(ticker).news or []
            cutoff  = datetime.now() - timedelta(days=5)
            for n in yf_news[:10]:
                pub = n.get('providerPublishTime', 0)
                if pub and datetime.fromtimestamp(pub) >= cutoff:
                    news_items.append({'headline': n.get('title', '')})

        news_count  = len(news_items)
        headlines   = []
        strong_hits = 0
        danger_hits = 0
        warning     = False

        for item in news_items[:5]:
            title = (item.get('headline') or item.get('title') or '').lower()
            headlines.append(item.get('headline') or item.get('title') or '')
            if any(kw in title for kw in STRONG_KEYWORDS): strong_hits += 1
            if any(kw in title for kw in DANGER_KEYWORDS): danger_hits += 1; warning = True

        if not news_items:                         return 'NONE',    0,          [],           False
        if warning and danger_hits > strong_hits:  return 'DANGER',  news_count, headlines[:2], True
        elif strong_hits >= 2:                     return 'STRONG',  news_count, headlines[:2], False
        elif strong_hits == 1:                     return 'MODERATE',news_count, headlines[:2], False
        else:                                      return 'WEAK',    news_count, headlines[:2], False
    except:
        return 'NONE', 0, [], False

def is_biotech(headlines):
    return any(kw in ' '.join(headlines).lower() for kw in BIOTECH_KEYWORDS)

# ============================================================
# GRADING
# ============================================================

def grade_setup(gap_pct, dollar_vol, vol_ratio=0):
    grade, notes = "B", []
    if gap_pct >= 50:   grade = "A+"; notes.append(f"Supernova +{gap_pct:.1f}% 🔥🔥🔥")
    elif gap_pct >= 30: grade = "A+"; notes.append(f"Huge gap +{gap_pct:.1f}% 🔥🔥")
    elif gap_pct >= 20: grade = "A";  notes.append(f"Strong gap +{gap_pct:.1f}% 🔥")
    elif gap_pct >= 15: grade = "A";  notes.append(f"Good gap +{gap_pct:.1f}%")
    elif gap_pct >= 10: grade = "B";  notes.append(f"Moderate gap +{gap_pct:.1f}%")
    elif gap_pct > 0:   grade = "C";  notes.append(f"Weak gap +{gap_pct:.1f}%")
    else:               grade = "D";  notes.append(f"Down {gap_pct:.1f}%")

    if dollar_vol >= 5_000_000:
        notes.append(f"Monster $vol ${dollar_vol/1e6:.1f}M 🔥")
        if grade == "A": grade = "A+"
    elif dollar_vol >= 1_000_000:
        notes.append(f"Strong $vol ${dollar_vol/1e6:.1f}M ✅")
    elif dollar_vol >= 500_000:
        notes.append(f"$Vol ${dollar_vol/1000:.0f}K ✅")
    else:
        notes.append(f"$Vol ${dollar_vol:,.0f}")

    if vol_ratio >= 5:   notes.append(f"RVOL {vol_ratio:.1f}x 🔥🔥")
    elif vol_ratio >= 3: notes.append(f"RVOL {vol_ratio:.1f}x ✅")
    elif vol_ratio > 0:  notes.append(f"RVOL {vol_ratio:.1f}x ⚠️")

    return grade, notes

# ============================================================
# DATA SOURCES — Polygon gainers + Alpaca snapshot enrichment
# ============================================================

def get_dynamic_universe():
    """
    Build a fully dynamic universe of tickers from live sources only.
    No hardcoded stocks — everything is discovered dynamically.
    Returns (tickers, errors) so caller can report failures clearly.
    """
    tickers = set()
    errors  = []

    # Source 1: Alpaca most actives by volume
    try:
        r = requests.get(
            f"{ALPACA_DATA_URL}/stocks/most_actives",
            headers=ALPACA_HEADERS,
            params={'by': 'volume', 'top': 100},
            timeout=10
        )
        if r.status_code == 200:
            before = len(tickers)
            for s in r.json().get('most_actives', []):
                sym = s.get('symbol', '')
                if sym and sym.isalpha() and len(sym) <= 5:
                    tickers.add(sym)
            log_alert(f"📡 Alpaca volume actives: +{len(tickers)-before}")
        else:
            msg = f"Alpaca most_actives HTTP {r.status_code}"
            errors.append(msg); log_alert(f"⚠️ {msg}")
    except Exception as e:
        msg = f"Alpaca most_actives: {e}"
        errors.append(msg); log_alert(f"⚠️ {msg}")

    # Source 2: Alpaca most actives by trades
    try:
        r = requests.get(
            f"{ALPACA_DATA_URL}/stocks/most_actives",
            headers=ALPACA_HEADERS,
            params={'by': 'trades', 'top': 100},
            timeout=10
        )
        if r.status_code == 200:
            before = len(tickers)
            for s in r.json().get('most_actives', []):
                sym = s.get('symbol', '')
                if sym and sym.isalpha() and len(sym) <= 5:
                    tickers.add(sym)
            log_alert(f"📡 Alpaca trade actives: +{len(tickers)-before}")
        else:
            msg = f"Alpaca trade_actives HTTP {r.status_code}"
            errors.append(msg); log_alert(f"⚠️ {msg}")
    except Exception as e:
        msg = f"Alpaca trade actives: {e}"
        errors.append(msg); log_alert(f"⚠️ {msg}")

    # Source 3: Polygon gainers
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
            f"?apiKey={POLYGON_API_KEY}&include_otc=false",
            timeout=10
        )
        if r.status_code == 200:
            before = len(tickers)
            for t in r.json().get('tickers', []):
                sym = t.get('ticker', '')
                if sym and sym.isalpha() and len(sym) <= 5:
                    tickers.add(sym)
            log_alert(f"📡 Polygon gainers: +{len(tickers)-before}")
        else:
            msg = f"Polygon gainers HTTP {r.status_code}"
            errors.append(msg); log_alert(f"⚠️ {msg}")
    except Exception as e:
        msg = f"Polygon gainers: {e}"
        errors.append(msg); log_alert(f"⚠️ {msg}")

    # Source 4: Yahoo gainers
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
            "?formatted=false&scrIds=day_gainers,small_cap_gainers&count=100",
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=10
        )
        if r.status_code == 200:
            before = len(tickers)
            for result in r.json().get('finance', {}).get('result', []):
                for q in result.get('quotes', []):
                    sym = q.get('symbol', '')
                    if sym and sym.isalpha() and len(sym) <= 5:
                        tickers.add(sym)
            log_alert(f"📡 Yahoo gainers: +{len(tickers)-before}")
        else:
            msg = f"Yahoo gainers HTTP {r.status_code}"
            errors.append(msg); log_alert(f"⚠️ {msg}")
    except Exception as e:
        msg = f"Yahoo gainers: {e}"
        errors.append(msg); log_alert(f"⚠️ {msg}")

    log_alert(f"📊 Universe: {len(tickers)} tickers | errors: {errors or 'none'}")
    return list(tickers), errors


def get_tradier_quotes(symbols, user_id='shafkat'):
    """
    Batch quote all symbols via the user's own Tradier production API.
    Returns (quote_map, error_msg).
    """
    results = {}
    if not symbols:
        return results, "No symbols to quote"

    hdrs = get_tradier_headers(user_id)

    for i in range(0, len(symbols), 200):
        chunk = symbols[i:i+200]
        try:
            r = requests.get(
                f"{TRADIER_API_URL}/markets/quotes",
                headers=hdrs,
                params={'symbols': ','.join(chunk), 'greeks': 'false'},
                timeout=15
            )
            if r.status_code == 200:
                quotes = r.json().get('quotes', {}).get('quote', [])
                if isinstance(quotes, dict): quotes = [quotes]
                for q in quotes:
                    sym = q.get('symbol', '')
                    if sym: results[sym] = q
            elif r.status_code == 403:
                msg = f"Tradier quotes HTTP 403 — check API key for user '{user_id}'"
                log_alert(f"⚠️ {msg}")
                return results, msg
            else:
                msg = f"Tradier quotes HTTP {r.status_code}: {r.text[:100]}"
                log_alert(f"⚠️ {msg}")
                return results, msg
        except Exception as e:
            msg = f"Tradier quotes exception: {e}"
            log_alert(f"⚠️ {msg}")
            return results, msg

    log_alert(f"📡 Tradier quotes [{user_id}]: {len(results)}/{len(symbols)} returned")
    return results, None


def get_tradier_movers(filters, user_id='shafkat'):
    """
    Full dynamic scanner:
    1. Build universe from live sources
    2. Get real-time Tradier quotes
    3. Filter by scan criteria
    Returns (candidates, error_message)
    """
    # Step 1: Universe
    universe, universe_errors = get_dynamic_universe()

    if not universe:
        err = f"No tickers found from any source. Errors: {', '.join(universe_errors)}"
        log_alert(f"❌ {err}")
        return [], err

    # Step 2: Tradier quotes
    quote_map, quote_error = get_tradier_quotes(universe, user_id)

    if not quote_map:
        err = quote_error or "Tradier returned 0 quotes"
        if universe_errors:
            err += f" | Universe errors: {', '.join(universe_errors)}"
        log_alert(f"❌ {err}")
        return [], err

    # Step 3: Filter
    candidates = []
    for sym, q in quote_map.items():
        try:
            price      = float(q.get('last') or q.get('bid') or 0)
            prev_close = float(q.get('prevclose') or 0)
            volume     = float(q.get('volume') or 0)
            avg_vol    = float(q.get('average_volume') or 1) or 1

            if price <= 0 or prev_close <= 0: continue
            pct        = ((price - prev_close) / prev_close) * 100
            dollar_vol = price * volume
            rvol       = round(volume / avg_vol, 1) if avg_vol > 0 else 0

            if not (filters['min_price'] <= price <= filters['max_price']): continue
            if pct < filters['min_gap_pct']:                                continue
            if dollar_vol < filters['min_dollar_vol']:                      continue

            candidates.append({
                'ticker':     sym,
                'price':      price,
                'prev_close': prev_close,
                'gap_pct':    round(pct, 1),
                'volume':     volume,
                'dollar_vol': dollar_vol,
                'rvol':       rvol,
                'source':     'tradier',
            })
        except:
            continue

    candidates.sort(key=lambda x: x['gap_pct'], reverse=True)
    summary = f"Universe: {len(universe)} → Quoted: {len(quote_map)} → Passed: {len(candidates)}"
    log_alert(f"📊 {summary}")
    return candidates, None

    # Source 1: Alpaca most actives by volume
    try:
        r = requests.get(
            f"{ALPACA_DATA_URL}/stocks/most_actives",
            headers=ALPACA_HEADERS,
            params={'by': 'volume', 'top': 100},
            timeout=10
        )
        if r.status_code == 200:
            before = len(tickers)
            for s in r.json().get('most_actives', []):
                sym = s.get('symbol', '')
                if sym and sym.isalpha() and len(sym) <= 5:
                    tickers.add(sym)
            log_alert(f"📡 Alpaca volume actives: +{len(tickers)-before}")
        else:
            log_alert(f"⚠️ Alpaca most_actives: {r.status_code}")
    except Exception as e:
        log_alert(f"⚠️ Alpaca most_actives error: {e}")

    # Source 2: Alpaca movers by trades
    try:
        r = requests.get(
            f"{ALPACA_DATA_URL}/stocks/most_actives",
            headers=ALPACA_HEADERS,
            params={'by': 'trades', 'top': 100},
            timeout=10
        )
        if r.status_code == 200:
            before = len(tickers)
            for s in r.json().get('most_actives', []):
                sym = s.get('symbol', '')
                if sym and sym.isalpha() and len(sym) <= 5:
                    tickers.add(sym)
            log_alert(f"📡 Alpaca trade actives: +{len(tickers)-before}")
    except Exception as e:
        log_alert(f"⚠️ Alpaca trade actives error: {e}")

    # Source 3: Polygon gainers
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
            f"?apiKey={POLYGON_API_KEY}&include_otc=false",
            timeout=10
        )
        if r.status_code == 200:
            before = len(tickers)
            for t in r.json().get('tickers', []):
                sym = t.get('ticker', '')
                if sym and sym.isalpha() and len(sym) <= 5:
                    tickers.add(sym)
            log_alert(f"📡 Polygon gainers: +{len(tickers)-before}")
        else:
            log_alert(f"⚠️ Polygon gainers: {r.status_code}")
    except Exception as e:
        log_alert(f"⚠️ Polygon gainers error: {e}")

    # Source 4: Yahoo gainers
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
            "?formatted=false&scrIds=day_gainers,small_cap_gainers&count=100",
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=10
        )
        if r.status_code == 200:
            before = len(tickers)
            for result in r.json().get('finance', {}).get('result', []):
                for q in result.get('quotes', []):
                    sym = q.get('symbol', '')
                    if sym and sym.isalpha() and len(sym) <= 5:
                        tickers.add(sym)
            log_alert(f"📡 Yahoo gainers: +{len(tickers)-before}")
        else:
            log_alert(f"⚠️ Yahoo gainers: {r.status_code}")
    except Exception as e:
        log_alert(f"⚠️ Yahoo gainers error: {e}")

    result = list(tickers)
    log_alert(f"📊 Universe total: {len(result)} tickers")
    return result



def get_polygon_gainers(filters):
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
            f"?apiKey={POLYGON_API_KEY}&include_otc=false",
            timeout=15
        )
        candidates = []
        if r.status_code == 200:
            for t in r.json().get('tickers', []):
                sym        = t.get('ticker', '')
                day        = t.get('day', {})
                prev       = t.get('prevDay', {})
                last       = t.get('lastTrade', {})
                price      = last.get('p') or day.get('c', 0)
                prev_close = prev.get('c', 0)
                volume     = day.get('v', 0)
                prev_vol   = prev.get('v', 1) or 1
                if not (price and prev_close and price > 0 and prev_close > 0): continue
                pct        = ((price - prev_close) / prev_close) * 100
                dollar_vol = price * volume
                rvol       = round(volume / prev_vol, 1) if prev_vol > 0 else 0

                if not sym: continue
                if not (filters['min_price'] <= price <= filters['max_price']): continue
                if pct < filters['min_gap_pct']:                               continue
                if dollar_vol < filters['min_dollar_vol']:                     continue
                if volume < filters.get('min_volume', 0):                      continue
                if rvol < filters.get('min_rvol', 0):                          continue

                candidates.append({
                    'ticker':     sym,
                    'price':      float(price),
                    'prev_close': float(prev_close),
                    'gap_pct':    round(float(pct), 1),
                    'volume':     float(volume),
                    'dollar_vol': float(dollar_vol),
                    'rvol':       rvol,
                    'source':     'polygon',
                })
            log_alert(f"📡 Polygon: {len(candidates)} candidates")
            return candidates
        log_alert(f"⚠️ Polygon error: {r.status_code}")
        return []
    except Exception as e:
        log_alert(f"⚠️ Polygon error: {e}")
        return []

def get_yahoo_gainers(filters):
    candidates = []
    headers    = {'User-Agent': 'Mozilla/5.0'}
    for scrId in ['day_gainers', 'small_cap_gainers']:
        try:
            r = requests.get(
                f"https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
                f"?formatted=false&scrIds={scrId}&count=50",
                headers=headers, timeout=10
            )
            if r.status_code != 200: continue
            quotes = r.json().get('finance', {}).get('result', [{}])[0].get('quotes', [])
            for q in quotes:
                sym        = q.get('symbol', '')
                price      = q.get('regularMarketPrice', 0)
                pct        = q.get('regularMarketChangePercent', 0)
                vol        = q.get('regularMarketVolume', 0)
                prev       = q.get('regularMarketPreviousClose', 0)
                avg_vol    = q.get('averageDailyVolume3Month', 1) or 1
                dollar_vol = price * vol
                rvol       = round(vol / avg_vol, 1) if avg_vol > 0 else 0

                if not sym: continue
                if not (filters['min_price'] <= price <= filters['max_price']): continue
                if pct < filters['min_gap_pct']:                               continue
                if dollar_vol < filters['min_dollar_vol']:                     continue
                if vol < filters.get('min_volume', 0):                         continue
                if rvol < filters.get('min_rvol', 0):                          continue

                candidates.append({
                    'ticker':     sym,
                    'price':      float(price),
                    'prev_close': float(prev),
                    'gap_pct':    round(float(pct), 1),
                    'volume':     float(vol),
                    'dollar_vol': float(dollar_vol),
                    'rvol':       rvol,
                    'source':     'yahoo',
                })
        except:
            pass
    log_alert(f"📡 Yahoo: {len(candidates)} candidates")
    return candidates

# ============================================================
# PROCESS TICKER — enrich with Alpaca snapshot
# ============================================================

def process_ticker(stock_data, mode='morning_gap', filters=None):
    try:
        ticker     = stock_data['ticker']
        filters    = filters or {}

        # Try Alpaca snapshot first for enriched data
        snap = alpaca_get_snapshot(ticker)

        if snap and snap.get('price', 0) > 0:
            price      = snap['price']
            prev_close = snap['prev_close']
            gap_pct    = snap['pct_change']
            dollar_vol = snap['dollar_vol']
            volume     = snap['volume']
            rvol       = snap['rvol']
            vwap       = snap['vwap']
            bid        = snap['bid']
            ask        = snap['ask']
        else:
            price      = stock_data['price']
            prev_close = stock_data['prev_close']
            gap_pct    = stock_data['gap_pct']
            dollar_vol = stock_data['dollar_vol']
            volume     = stock_data.get('volume', 0)
            rvol       = stock_data.get('rvol', 0)
            vwap       = 0
            bid        = 0
            ask        = 0

        # Re-check filters with fresh Alpaca data
        if price < filters.get('min_price', 0):      return None
        if price > filters.get('max_price', 9999):   return None
        if gap_pct < filters.get('min_gap_pct', 0):  return None
        if dollar_vol < filters.get('min_dollar_vol', 0): return None
        if volume < filters.get('min_volume', 0):    return None
        if rvol < filters.get('min_rvol', 0):         return None

        grade, notes             = grade_setup(gap_pct, dollar_vol, rvol)
        strength, news_count, headlines, warning = check_catalyst(ticker)

        if filters.get('require_news') and news_count == 0: return None
        if mode == 'biotech' and not is_biotech(headlines): return None

        final_grade = grade
        if warning:                                  final_grade = "D"
        elif strength == 'STRONG' and grade == 'A': final_grade = "A+"
        elif strength == 'NONE':
            if grade == 'A+':  final_grade = 'A'
            elif grade == 'A': final_grade = 'B'

        catalyst_label = (
            f"☠️ Danger — {news_count}" if warning else
            f"🔥 Strong Catalyst — {news_count}" if strength == 'STRONG' else
            f"✅ Moderate — {news_count}" if strength == 'MODERATE' else
            f"📰 {news_count} articles" if news_count > 0 else "📰 0 articles"
        )

        # Company info + exchange filter
        info         = get_company_info(ticker)
        exchange     = info.get('exchange', '')
        market_cap_m = info.get('market_cap_m', 0)

        if exchange and not any(ex in exchange.upper() for ex in ['NASDAQ','NYSE','AMEX','BATS','CBOE']):
            return None

        max_mktcap = filters.get('max_market_cap_m', 0)
        if max_mktcap > 0 and market_cap_m > 0 and market_cap_m > max_mktcap:
            return None

        # Float check
        float_m   = 0.0
        max_float = filters.get('max_float_m', 0)
        if max_float > 0:
            try:
                yf_info      = yf.Ticker(ticker).info
                float_shares = yf_info.get('floatShares', 0)
                if float_shares:
                    float_m = float_shares / 1_000_000
                    if float_m > max_float: return None
            except:
                pass

        # Historical closes for pattern
        try:
            hist   = yf.Ticker(ticker).history(period="10d", interval="1d")
            closes = hist['Close'].values.tolist() if not hist.empty else []
        except:
            closes = []

        halt_info                               = get_halt_info(ticker)
        pattern, pattern_desc, pattern_criteria  = detect_sykes_pattern(
            ticker, price, prev_close, gap_pct, closes, rvol, dollar_vol)

        above_vwap = price > vwap if vwap > 0 else None
        spread_pct = round(((ask - bid) / ((ask + bid) / 2)) * 100, 2) if bid > 0 and ask > 0 else 0

        entry_low  = round(price * 0.99, 2)
        entry_high = round(price * 1.02, 2)
        stop_loss  = round(entry_low * 0.95, 2)
        target1    = round(entry_high * 1.10, 2)
        target2    = round(entry_high * 1.20, 2)
        target3    = round(entry_high * 1.30, 2)

        return {
            'ticker':           ticker,
            'company_name':     info.get('company_name', ''),
            'exchange':         exchange,
            'industry':         info.get('industry', ''),
            'market_cap_m':     round(market_cap_m, 2),
            'float_m':          round(float_m, 1),
            'price':            round(price, 2),
            'prev_close':       round(prev_close, 2),
            'gap_pct':          round(gap_pct, 1),
            'dollar_vol':       round(dollar_vol, 0),
            'volume':           round(volume, 0),
            'rvol':             round(rvol, 1),
            'vwap':             round(vwap, 2),
            'above_vwap':       above_vwap,
            'bid':              bid,
            'ask':              ask,
            'spread_pct':       spread_pct,
            'grade':            final_grade,
            'notes':            notes,
            'catalyst':         catalyst_label,
            'news_count':       news_count,
            'headlines':        headlines[:2],
            'warning':          warning,
            'halt_info':        halt_info,
            'pattern':          pattern,
            'pattern_desc':     pattern_desc,
            'pattern_criteria': pattern_criteria,
            'entry_low':        entry_low,
            'entry_high':       entry_high,
            'stop_loss':        stop_loss,
            'target1':          target1,
            'target2':          target2,
            'target3':          target3,
            'source':           'alpaca' if snap else stock_data.get('source','live'),
            'time':             datetime.now().strftime('%H:%M:%S'),
        }
    except:
        return None

# ============================================================
# SCANNER LOOP
# ============================================================

async def do_scan(user_id):
    s       = user_state[user_id]
    filters = s['filters'].copy()
    mode    = s['mode']
    label   = SCAN_MODES.get(mode, {}).get('label', mode)

    status = {'phase':'fetching','message':f'📡 Fetching [{label}]...','progress':0,'total':0}
    await broadcast_to_user(user_id, {'type':'scan_status','status':status})
    await asyncio.sleep(0)

    # Tradier primary (real-time), Polygon second, Yahoo fallback
    candidates, scan_error = get_tradier_movers(filters, user_id)

    if scan_error and not candidates:
        # Show the error clearly on the frontend
        error_status = {
            'phase': 'error',
            'message': f'❌ Scanner error: {scan_error}',
            'progress': 0, 'total': 0
        }
        await broadcast_to_user(user_id, {'type':'scan_status','status':error_status})
        log_alert(f"❌ [{user_id}] Scan failed: {scan_error}")
        return [], error_status

    if not candidates:
        # Tradier worked but no stocks passed filters — try Polygon then Yahoo directly
        status['message'] = '⚠️ No stocks passed Tradier filters — trying Polygon...'
        await broadcast_to_user(user_id, {'type':'scan_status','status':status})
        await asyncio.sleep(0)
        candidates = get_polygon_gainers(filters)

    if not candidates:
        status['message'] = '⚠️ Polygon empty — trying Yahoo...'
        await broadcast_to_user(user_id, {'type':'scan_status','status':status})
        await asyncio.sleep(0)
        candidates = get_yahoo_gainers(filters)

    total = len(candidates)
    log_alert(f"📊 [{user_id}] {total} candidates")

    status = {'phase':'analyzing','message':f'Enriching {total} with Alpaca data...','progress':0,'total':total}
    await broadcast_to_user(user_id, {'type':'scan_status','status':status})
    await asyncio.sleep(0)

    results = []
    count   = 0

    for stock_data in candidates:
        if not s['running']: break
        count  += 1
        ticker  = stock_data.get('ticker', '')
        status  = {'phase':'analyzing','message':f'Checking {ticker} ({count}/{total})...','progress':count,'total':total}
        await broadcast_to_user(user_id, {'type':'scan_status','status':status})
        await asyncio.sleep(0.05)

        setup = process_ticker(stock_data, mode, filters)
        if setup:
            results.append(setup)
            await broadcast_to_user(user_id, {'type':'new_ticker','setup':setup})
            await asyncio.sleep(0)

            if setup['grade'] in ['A+','A'] and not setup['warning'] and ticker not in s['alerted']:
                s['alerted'].add(ticker)
                log_alert(f"🚀 [{user_id}] {setup['grade']}: {ticker} +{setup['gap_pct']}%")
                send_email(
                    subject=f"🚀 {setup['grade']} — {ticker} +{setup['gap_pct']}%",
                    body=(
                        f"Grade: {setup['grade']}\nPattern: {setup['pattern']}\n"
                        f"Company: {setup.get('company_name','')} — {setup.get('exchange','')}\n"
                        f"Gap: +{setup['gap_pct']}%  RVOL: {setup.get('rvol',0)}x\n"
                        f"Price: ${setup['price']}  VWAP: ${setup.get('vwap',0)}\n"
                        f"Float: {setup.get('float_m',0)}M\n\n"
                        f"Entry: ${setup['entry_low']}–${setup['entry_high']}\n"
                        f"Stop: ${setup['stop_loss']}\nT1: ${setup['target1']}\n"
                    )
                )

    results.sort(
        key=lambda x: (0 if x['warning'] else {'A+':4,'A':3,'B':2}.get(x['grade'],1)),
        reverse=True
    )
    done_status = {'phase':'done','message':f'✅ {len(results)} setup(s) found','progress':total,'total':total}
    return results, done_status

async def scanner_loop(user_id):
    s = user_state[user_id]
    log_alert(f"🔍 [{user_id}] Scanner started")
    await broadcast_to_user(user_id, {'type':'status','running':True,'mode':s['mode'],'filters':s['filters']})

    while s['running']:
        try:
            results, done_status = await do_scan(user_id)
            s['scan_results']    = results
            await broadcast_to_user(user_id, {
                'type':'scan_results','data':results,
                'time':datetime.now().strftime('%H:%M:%S'),
                'count':len(results),'alerts':alert_log[:20],
                'scan_status':done_status,'mode':s['mode'],'filters':s['filters'],
            })
        except Exception as e:
            log_alert(f"⚠️ [{user_id}] Scanner error: {e}")

        if s['running']:
            interval = int(os.getenv('SCAN_INTERVAL', 30))
            await asyncio.sleep(interval)

    log_alert(f"⏹️ [{user_id}] Scanner stopped")
    await broadcast_to_user(user_id, {'type':'status','running':False,'mode':s['mode']})

# ============================================================
# WEBSOCKET
# ============================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    user_id = websocket.query_params.get('user', 'shafkat')
    if user_id not in user_clients: user_id = 'shafkat'
    user_clients[user_id].append(websocket)
    log_alert(f"📱 [{user_id}] Connected")

    s = user_state[user_id]
    await websocket.send_json({
        'type':'scan_results','data':s['scan_results'],'alerts':alert_log[:20],
        'time':datetime.now().strftime('%H:%M:%S'),'count':len(s['scan_results']),
        'scan_status':{'phase':'idle'},'mode':s['mode'],'filters':s['filters'],
    })
    await websocket.send_json({'type':'status','running':s['running'],'mode':s['mode']})

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in user_clients.get(user_id, []):
            user_clients[user_id].remove(websocket)

@app.get("/")
async def serve_dashboard():
    try:
        with open("dashboard.html", "r") as f:
            return HTMLResponse(f.read())
    except:
        return HTMLResponse("<h1>Dashboard not found</h1>")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)