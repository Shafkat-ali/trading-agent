from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
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

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', 'W7T9tMZzRCsHUhJfPvL7SZOReXow4q8L')
FINNHUB_KEY     = os.getenv('FINNHUB_API_KEY', 'd812vi9r01qler4gpnmgd812vi9r01qler4gpnn0')
EMAIL_ADDRESS   = os.getenv('EMAIL_ADDRESS', '')
EMAIL_PASSWORD  = os.getenv('EMAIL_PASSWORD', '')
EMAIL_TO        = os.getenv('EMAIL_TO', '')

# Per-user state
user_state = {
    'shafkat': {'scan_results':[], 'alerted':set(), 'mode':'standard', 'running':False,
                'filters':{'min_price':0.10,'max_price':50.00,'min_gap_pct':20.0,'min_dollar_vol':50000}},
    'irfan':   {'scan_results':[], 'alerted':set(), 'mode':'standard', 'running':False,
                'filters':{'min_price':0.10,'max_price':50.00,'min_gap_pct':20.0,'min_dollar_vol':50000}},
}
user_clients = {'shafkat':[], 'irfan':[]}
alert_log    = []

# NYSE halt cache
halted_tickers = {}   # ticker -> {'reason': str, 'halt_time': str}
halt_cache_ts  = None

SCAN_MODES = {
    'standard':  {'label':'🔍 Standard',   'min_price':0.10,'max_price':50.00,'min_gap':20.0,'min_dvol':50_000,   'desc':'Gap 20%+, $0.10–$50, $50K vol'},
    'supernova': {'label':'🚀 Supernovas', 'min_price':0.10,'max_price':50.00,'min_gap':50.0,'min_dvol':100_000,  'desc':'Gap 50%+ only — massive movers'},
    'biotech':   {'label':'🧬 Biotech',    'min_price':0.50,'max_price':50.00,'min_gap':15.0,'min_dvol':100_000,  'desc':'Biotech/FDA plays, gap 15%+'},
    'low_float': {'label':'⚡ Low Float',  'min_price':0.10,'max_price':20.00,'min_gap':15.0,'min_dvol':50_000,   'desc':'Under $20, low float runners'},
    'premarket': {'label':'🌅 Pre-Market', 'min_price':0.10,'max_price':50.00,'min_gap':15.0,'min_dvol':25_000,   'desc':'Pre-market gaps 15%+'},
    'squeeze':   {'label':'💎 Squeeze',    'min_price':1.00,'max_price':50.00,'min_gap':10.0,'min_dvol':500_000,  'desc':'Short squeeze — high dollar vol'},
    'small_cap': {'label':'📊 Small Cap',  'min_price':1.00,'max_price':15.00,'min_gap':15.0,'min_dvol':100_000,  'desc':'Small cap $1–$15, gap 15%+'},
    'afternoon': {'label':'🌆 Afternoon',  'min_price':0.10,'max_price':50.00,'min_gap':5.0, 'min_dvol':50_000,   'desc':'Afternoon HOD breakouts'},
    'penny':     {'label':'🪙 Penny',      'min_price':0.01,'max_price':1.00, 'min_gap':20.0,'min_dvol':10_000,   'desc':'Pure penny stocks under $1'},
    'custom':    {'label':'⚙️ Custom',     'min_price':0.10,'max_price':50.00,'min_gap':20.0,'min_dvol':50_000,   'desc':'Your custom filter settings'},
}

STRONG_KEYWORDS = ['fda','approval','approved','breakthrough','contract','partnership','merger',
    'acquisition','earnings','beat','guidance','revenue','clinical','trial','phase','results',
    'patent','exclusive','launch','deal','awarded','wins','secures','signs','robot','ai',
    'technology','crypto','bitcoin']
DANGER_KEYWORDS = ['offering','dilut','shelf','warrant','investigation','lawsuit','sec',
    'subpoena','delay','failed','withdrawn','suspended','reverse split','compliance']
BIOTECH_KEYWORDS = ['fda','trial','phase','clinical','drug','therapy','biotech',
    'pharmaceutical','approval','nda','bla','cancer','oncology']
SEC_DANGER_KEYWORDS = ['s-1','s-3','424b','offering','dilut','warrant','reverse split',
    'sec filing','8-k','going concern','non-compliance']

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
            log_alert(f"📧 Email: {subject}")
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
        if c in user_clients.get(user_id, []): user_clients[user_id].remove(c)

# ============================================================
# NYSE HALT DETECTION
# ============================================================

def refresh_halt_cache():
    global halted_tickers, halt_cache_ts
    try:
        r = requests.get(
            'https://www.nyse.com/api/trade-halts/current/download',
            timeout=10,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200 and r.text:
            halted_tickers = {}
            reader = csv.DictReader(io.StringIO(r.text))
            for row in reader:
                sym = (row.get('Symbol') or row.get('symbol') or '').strip().upper()
                reason = row.get('Reason Halted') or row.get('reasonHalted') or row.get('Reason') or 'Unknown'
                halt_time = row.get('Halt Time') or row.get('haltTime') or row.get('Time') or ''
                if sym:
                    halted_tickers[sym] = {'reason': reason.strip(), 'halt_time': halt_time.strip()}
            halt_cache_ts = datetime.now()
            log_alert(f"🛑 NYSE halt cache: {len(halted_tickers)} halted stocks")
    except Exception as e:
        log_alert(f"⚠️ NYSE halt fetch failed: {e}")

def get_halt_info(ticker):
    global halt_cache_ts
    # Refresh every 2 minutes
    if halt_cache_ts is None or (datetime.now() - halt_cache_ts).seconds > 120:
        refresh_halt_cache()
    return halted_tickers.get(ticker.upper(), None)

# ============================================================
# DEBUG
# ============================================================

@app.get("/api/debug")
async def debug():
    return {
        'finnhub_key_set': bool(FINNHUB_KEY),
        'finnhub_key_preview': FINNHUB_KEY[:8]+'...' if FINNHUB_KEY else 'NOT SET',
        'polygon_key_set': bool(POLYGON_API_KEY),
        'halted_count': len(halted_tickers),
        'users': {uid: {'mode':s['mode'],'running':s['running'],'results':len(s['scan_results'])}
                  for uid,s in user_state.items()}
    }

@app.get("/api/scan_modes")
async def get_scan_modes():
    return {'modes': SCAN_MODES}

@app.get("/api/halts")
async def get_halts():
    refresh_halt_cache()
    return {'halted': halted_tickers, 'count': len(halted_tickers), 'as_of': datetime.now().strftime('%H:%M:%S')}

# ============================================================
# PER-USER ENDPOINTS
# ============================================================

@app.post("/api/scanner/mode/{user_id}/{mode}")
async def set_scan_mode(user_id: str, mode: str):
    if user_id not in user_state: return {'error': 'Unknown user'}
    if mode not in SCAN_MODES:   return {'error': f'Unknown mode: {mode}'}
    m = SCAN_MODES[mode]
    user_state[user_id]['mode'] = mode
    user_state[user_id]['filters'] = {
        'min_price': m['min_price'], 'max_price': m['max_price'],
        'min_gap_pct': m['min_gap'], 'min_dollar_vol': m['min_dvol'],
    }
    log_alert(f"🔄 [{user_id}] Mode: {m['label']}")
    return {'status':'ok','mode':mode,'filters':user_state[user_id]['filters']}

@app.post("/api/scanner/filters/{user_id}")
async def set_filters(user_id: str, filters: dict):
    if user_id not in user_state: return {'error': 'Unknown user'}
    user_state[user_id]['mode'] = 'custom'
    f = user_state[user_id]['filters']
    if 'min_price'      in filters: f['min_price']      = float(filters['min_price'])
    if 'max_price'      in filters: f['max_price']      = float(filters['max_price'])
    if 'min_gap_pct'    in filters: f['min_gap_pct']    = float(filters['min_gap_pct'])
    if 'min_dollar_vol' in filters: f['min_dollar_vol'] = float(filters['min_dollar_vol'])
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
    log_alert(f"🗑️ [{user_id}] Alerts cleared")
    return {'status':'cleared'}

# ============================================================
# STOCK DATA ENDPOINTS
# ============================================================

@app.get("/api/stock/quote/{ticker}")
async def get_stock_quote(ticker: str):
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=8
        )
        if r.status_code == 200:
            return r.json()
        return {'error': f'Status {r.status_code}'}
    except Exception as e:
        return {'error': str(e)}

@app.get("/api/stock/profile/{ticker}")
async def get_stock_profile(ticker: str):
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/stock/profile2",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=8
        )
        if r.status_code == 200:
            return r.json()
        return {'error': f'Status {r.status_code}'}
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

        # Supplement float & short ratio from yfinance if missing
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
        return {'news': r.json()} if r.status_code == 200 else {'news': [], 'error': f'Status {r.status_code}'}
    except Exception as e:
        return {'news': [], 'error': str(e)}

@app.get("/api/stock/sec/{ticker}")
async def get_sec_filings(ticker: str):
    """Get SEC filings from Finnhub — free endpoint"""
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/stock/filings",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=8
        )
        filings = r.json() if r.status_code == 200 else []
        # Flag danger filings
        danger = []
        for f in filings[:20]:
            form = (f.get('form') or '').upper()
            desc = (f.get('description') or '').lower()
            if any(d in form for d in ['S-1','S-3','424B','8-K']) or any(kw in desc for kw in SEC_DANGER_KEYWORDS):
                f['danger'] = True
            danger.append(f)
        return {'filings': danger}
    except Exception as e:
        return {'filings': [], 'error': str(e)}

@app.get("/api/news/market")
async def get_market_news():
    """General market news from Finnhub — for world news tab"""
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/news",
            params={'category': 'general', 'token': FINNHUB_KEY},
            timeout=8
        )
        news = r.json() if r.status_code == 200 else []
        return {'news': news[:30]}
    except Exception as e:
        return {'news': [], 'error': str(e)}

@app.get("/api/news/sec")
async def get_sec_news():
    """SEC/regulatory news — merger, offering, dilution alerts"""
    try:
        # Finnhub merger/regulatory news
        r = requests.get(
            "https://finnhub.io/api/v1/news",
            params={'category': 'merger', 'token': FINNHUB_KEY},
            timeout=8
        )
        news = r.json() if r.status_code == 200 else []

        # Tag danger items
        tagged = []
        for n in news[:30]:
            headline = (n.get('headline') or '').lower()
            n['danger'] = any(kw in headline for kw in SEC_DANGER_KEYWORDS)
            tagged.append(n)

        return {'news': tagged}
    except Exception as e:
        return {'news': [], 'error': str(e)}

@app.get("/api/stock/search/{ticker}")
async def search_ticker(ticker: str):
    try:
        ticker = ticker.upper().strip()

        # Quote
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
            stock      = yf.Ticker(ticker)
            info       = stock.fast_info
            price      = getattr(info, 'last_price', 0) or 0
            prev_close = getattr(info, 'previous_close', 0) or 0

        if not price or price == 0:
            return {'error': f'Could not find ticker {ticker}'}

        pct_change = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0

        # Profile for company name
        profile_r = requests.get(
            "https://finnhub.io/api/v1/stock/profile2",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=8
        )
        profile = profile_r.json() if profile_r.status_code == 200 else {}
        company_name = profile.get('name', '')
        exchange     = profile.get('exchange', '')
        industry     = profile.get('finnhubIndustry', '')

        # Historical
        stock    = yf.Ticker(ticker)
        hist     = stock.history(period="10d", interval="1d")
        hist_1m  = stock.history(period="1d",  interval="1m")
        volume   = float(hist_1m['Volume'].sum()) if not hist_1m.empty else 0
        avg_vol  = float(hist['Volume'].mean())   if not hist.empty else 0
        vol_ratio = volume / avg_vol if avg_vol > 0 else 0
        closes   = hist['Close'].values.tolist()  if not hist.empty else []
        dollar_vol = price * volume

        # Halt check
        halt_info = get_halt_info(ticker)

        pattern, pattern_desc, pattern_criteria = detect_sykes_pattern(
            ticker, price, prev_close, pct_change, closes, vol_ratio, dollar_vol)
        grade, notes = grade_setup(pct_change, dollar_vol)
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

        return {
            'ticker':           ticker,
            'company_name':     company_name,
            'exchange':         exchange,
            'industry':         industry,
            'price':            round(price, 2),
            'prev_close':       round(prev_close, 2),
            'gap_pct':          round(pct_change, 1),
            'high':             round(high, 2),
            'low':              round(low, 2),
            'open':             round(open_price, 2),
            'dollar_vol':       round(dollar_vol, 0),
            'vol_ratio':        round(vol_ratio, 1),
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
            'source':           'Search',
            'time':             datetime.now().strftime('%H:%M:%S')
        }
    except Exception as e:
        return {'error': str(e)}

# ============================================================
# SYKES PATTERN DETECTION — 14 PATTERNS
# ============================================================

def detect_sykes_pattern(ticker, price, prev_close, pct_change, closes, vol_ratio, dollar_vol):
    n = len(closes)

    if pct_change >= 50:
        return ("🚀 Supernova",
            "Stock exploded 50%+ in one session. Massive volatility and liquidity. "
            "Sykes: 'Supernovas can last longer and are greater than anybody thinks possible.'",
            [f"✅ Up {pct_change:.1f}% today (need 50%+)",
             f"✅ Volume {vol_ratio:.1f}x average" if vol_ratio>=3 else f"⚠️ Volume {vol_ratio:.1f}x (want 3x+)",
             "📌 Sykes Rule: Buy momentum OR wait for first red day to short",
             "📌 Entry: Buy early momentum or dip buy the first panic",
             "📌 Exit: Sell into strength — supernovas crash hard and fast",
             "⚠️ Risk: Supernovas can crash 90% in one day — use stop loss"])

    if n >= 5:
        recent_high = max(closes[-5:])
        recent_run  = ((recent_high - closes[-5]) / closes[-5] * 100) if closes[-5] > 0 else 0
        dip_from_high = ((price - recent_high) / recent_high * 100) if recent_high > 0 else 0
        if recent_run >= 50 and dip_from_high <= -20 and pct_change > 0:
            return ("🎯 Morning Panic Dip Buy",
                "Sykes #1 all-time favorite. Stock ran 50%+ recently then panicked hard. "
                "Stop losses cascade creating massive sell-off. Buyers see price as discount.",
                [f"✅ Stock ran {recent_run:.0f}% recently",
                 f"✅ Down {abs(dip_from_high):.0f}% from recent high",
                 f"✅ Bouncing {pct_change:.1f}% today",
                 "📌 Sykes Rule: Wait for selling to STOP — patience is key",
                 "📌 Entry: Buy when Level 2 shows wall of buyers forming",
                 "📌 Watch: Double bottom = stronger bounce",
                 "📌 Exit: Quick scalp — sell into the bounce",
                 "⚠️ Cut losses quickly if it breaks support"])

    if n >= 4:
        prev_days_red = all(closes[i] < closes[i-1] for i in range(-3, -1))
        if prev_days_red and pct_change > 5:
            return ("🟢 First Green Day",
                "First green candle after multiple red days. Jack Kellogg's all-time favorite ($22M+). "
                "Look for stocks closing near highs for overnight hold.",
                [f"✅ First green day up {pct_change:.1f}% after 2+ red days",
                 f"✅ Volume {vol_ratio:.1f}x" if vol_ratio>=2 else f"⚠️ Volume {vol_ratio:.1f}x (want 2x+)",
                 "📌 Sykes Rule: Buy dips on the run-up — never chase the spike",
                 "📌 Entry: Wait for morning dip, buy bounce toward HOD",
                 "📌 If closing near HOD → consider overnight hold for gap up",
                 "📌 Exit: Sell into gap up next morning",
                 "⚠️ First red day after this = potential short"])

    if n >= 6:
        green_days = sum(1 for i in range(-5, 0) if closes[i] > closes[i-1])
        total_run  = ((closes[-1] - closes[-6]) / closes[-6] * 100) if closes[-6] > 0 else 0
        if green_days >= 3 and pct_change > 0 and total_run >= 30:
            return ("📈 Multi-Day Breakout",
                "Running consecutively for multiple days — Jack Kellogg's favorite. "
                "Sykes: 'Stocks that spike 500% don't do it in one day.'",
                [f"✅ {green_days} of last 5 days green",
                 f"✅ Total {total_run:.0f}% run",
                 f"✅ Up {pct_change:.1f}% continuing",
                 "📌 Buy 10-20% dips off morning highs",
                 "📌 Watch: Stock closing near HOD = bullish continuation",
                 "📌 Exit: Sell into strength — don't hold through the crash"])

    if pct_change >= 15 and vol_ratio >= 2:
        return ("⚡ Gap and Go",
            "Gapped up on catalyst and continues higher. Shorts get squeezed adding fuel. "
            "Sykes only trades with confirmed news.",
            [f"✅ Gapped up {pct_change:.1f}%",
             f"✅ Volume {vol_ratio:.1f}x" if vol_ratio>=3 else f"⚠️ Volume {vol_ratio:.1f}x (want 3x+)",
             "📌 ONLY trade with strong catalyst — no catalyst = skip",
             "📌 Entry: Buy first pullback after open",
             "📌 Exit: Scale out — T1 +10%, T2 +20%, T3 +30%",
             "⚠️ Risk: Gap fills happen fast — stop below gap support"])

    if n >= 5 and pct_change >= 5:
        recent_max = max(closes[-5:])
        flagpole   = ((recent_max - closes[-5]) / closes[-5] * 100) if closes[-5] > 0 else 0
        flag_range = ((recent_max - min(closes[-3:])) / recent_max * 100) if recent_max > 0 else 0
        if flagpole >= 15 and flag_range <= 15 and pct_change < flagpole * 0.5:
            return ("🏳️ Bull Flag / Pennant",
                "Spike (flagpole) + tight consolidation (flag) + breakout. "
                "Volume drops during flag, spikes on breakout.",
                [f"✅ Flagpole: {flagpole:.0f}% spike",
                 f"✅ Consolidation: {flag_range:.1f}% range (tight = bullish)",
                 "📌 Wait for CONFIRMED breakout above flag top",
                 "📌 Volume should DROP during flag, SPIKE on breakout",
                 "📌 Exit: Flagpole length added to breakout point",
                 "⚠️ Failed breakouts look exactly like real ones"])

    if n >= 6 and pct_change >= 3:
        step_pattern = all(closes[i] > closes[i-1] * 0.93 for i in range(-4, -1))
        total_climb  = ((closes[-1] - closes[-6]) / closes[-6] * 100) if closes[-6] > 0 else 0
        if step_pattern and total_climb >= 20:
            return ("🪜 Stair Stepper",
                "Slower supernova — rises progressively with brief pullbacks. "
                "Each step acts as support and resistance.",
                [f"✅ Progressive uptrend {total_climb:.0f}%",
                 f"✅ Up {pct_change:.1f}% today",
                 "📌 Buy the pullbacks (steps), sell into spikes",
                 "📌 Use bottom of green candles as your stop",
                 "⚠️ Can reverse suddenly if catalyst fades"])

    if n >= 5:
        recent_high_5d = max(closes[-5:])
        if price >= recent_high_5d * 0.97 and vol_ratio >= 2 and pct_change > 5:
            return ("💥 Breakout",
                "Breaking above recent resistance with volume confirmation. "
                "Sykes: 'Buy breakouts only with volume.'",
                [f"✅ Price near 5-day high (${recent_high_5d:.2f})",
                 f"✅ Up {pct_change:.1f}%",
                 f"✅ Volume {vol_ratio:.1f}x" if vol_ratio>=2 else f"⚠️ Volume {vol_ratio:.1f}x (need 2x+)",
                 "📌 Only buy with volume — no volume = fake breakout",
                 "📌 Set stop below resistance level",
                 "⚠️ Failed breakouts trap longs"])

    if n >= 7:
        mid        = n // 2
        left_high  = max(closes[:mid]) if mid > 0 else 0
        cup_bottom = min(closes[mid-2:mid+2]) if mid > 2 else 0
        right_high = max(closes[mid:]) if mid < n else 0
        cup_depth  = ((left_high - cup_bottom) / left_high * 100) if left_high > 0 else 0
        recovery   = ((right_high - cup_bottom) / cup_bottom * 100) if cup_bottom > 0 else 0
        if cup_depth >= 15 and cup_depth <= 60 and recovery >= 50 and pct_change > 0:
            return ("☕ Cup and Handle",
                "U-shaped recovery + handle consolidation + breakout. "
                "Sykes tweeted 'beautiful premarket cup and handle' Nov 2025.",
                [f"✅ Cup depth: {cup_depth:.0f}%",
                 f"✅ Recovery: {recovery:.0f}%",
                 "📌 Wait for handle breakout with volume",
                 "📌 Exit: Cup depth added to breakout point"])

    now_hour = datetime.now().hour
    if 13 <= now_hour <= 16 and pct_change >= 5 and vol_ratio >= 1.5:
        return ("🌆 Afternoon Breakout",
            "Breaking above morning HOD in afternoon. Safer for beginners — more info available.",
            [f"✅ Up {pct_change:.1f}% in afternoon",
             f"✅ Volume {vol_ratio:.1f}x" if vol_ratio>=2 else f"⚠️ Volume {vol_ratio:.1f}x",
             "📌 Draw line at morning HOD — if it breaks, buy it",
             "📌 Safer than premarket — more info available",
             "⚠️ Afternoon fades happen — exit fast if it reverses"])

    if 15 <= now_hour <= 16 and n >= 3:
        prev_red = closes[-2] < closes[-3] if n >= 3 else False
        if pct_change >= 10 and prev_red:
            return ("🌙 Overnight Hold Setup",
                "Stock closing strong near HOD on first green day. "
                "Sykes: 'Buy before close. Goal is to sell into gap up next morning.'",
                [f"✅ Up {pct_change:.1f}% closing strong near HOD",
                 "✅ Previous days were red",
                 "📌 Only hold overnight if closing within 10% of HOD",
                 "📌 Entry: Buy in last 30 minutes of session",
                 "📌 Exit: Sell into gap up next morning",
                 "⚠️ Bad news overnight = gap DOWN — small position"])

    if n >= 3 and pct_change >= 3:
        dip1 = ((closes[-2] - closes[-3]) / closes[-3] * 100) if closes[-3] > 0 else 0
        if dip1 <= -10:
            return ("📉 Double Bottom Morning Panic",
                "Two panic lows forming support. "
                "Sykes: 'Second bottom holds better. Add on second bounce.'",
                [f"✅ First panic: {dip1:.0f}%",
                 f"✅ Bouncing {pct_change:.1f}%",
                 "📌 Buy the SECOND bottom, not the first",
                 "📌 W-shape on chart at similar price levels",
                 "⚠️ If second bottom breaks lower, exit immediately"])

    if n >= 3 and pct_change < -10:
        prev_was_big_up = closes[-2] > closes[-3] * 1.2 if n >= 3 else False
        if prev_was_big_up:
            return ("🔴 Supernova Fade (Short)",
                "First red day after big run. Sykes' prime short setup. "
                "Trapped longs panic selling adds to downside.",
                [f"✅ Down {abs(pct_change):.1f}% after big run",
                 "✅ Previous session was up 20%+",
                 "📌 Short the first red day — every bounce is a sell",
                 "📌 Cover into panics",
                 "⚠️ Short squeezes brutal — keep position small"])

    if n >= 5 and pct_change < -5:
        consec_red = sum(1 for i in range(-4, 0) if closes[i] < closes[i-1])
        if consec_red >= 3:
            return ("🦅 The Crow — AVOID",
                "Continuous selling pressure. Every recovery gets sold. "
                "Sykes: 'I don't recommend trading this pattern.'",
                [f"⚠️ Down {abs(pct_change):.1f}% — continuing downtrend",
                 f"⚠️ {consec_red} of last 4 days red",
                 "🚨 DO NOT trade The Crow on the long side",
                 "🚨 SKIP — wait for a better opportunity"])

    if pct_change >= 10:
        return ("📊 Momentum Play",
            "Decent move but no clean Sykes pattern. Trade with caution.",
            [f"⚠️ Up {pct_change:.1f}% — no clean pattern",
             "📌 Check: catalyst? float? sector hot?",
             "📌 Wait for cleaner setup"])

    return ("🔍 No Clear Pattern",
        "Does not match any defined Sykes setup. "
        "Sykes: 'If there's no pattern, there's no trade.'",
        [f"⚠️ Move too small for Sykes patterns",
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
                    news_items.append({'headline': n.get('title', ''), 'source': 'yf'})

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

        if not news_items: return 'NONE', 0, [], False
        if warning and danger_hits > strong_hits: return 'DANGER', news_count, headlines[:2], True
        elif strong_hits >= 2: return 'STRONG', news_count, headlines[:2], False
        elif strong_hits == 1: return 'MODERATE', news_count, headlines[:2], False
        else: return 'WEAK', news_count, headlines[:2], False
    except:
        return 'NONE', 0, [], False

# ============================================================
# DATA SOURCES
# ============================================================

def get_company_info(ticker):
    """Get company name, exchange, industry from Finnhub"""
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/stock/profile2",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=6
        )
        if r.status_code == 200:
            d = r.json()
            return {
                'company_name': d.get('name', ''),
                'exchange':     d.get('exchange', ''),
                'industry':     d.get('finnhubIndustry', ''),
            }
    except:
        pass
    return {'company_name': '', 'exchange': '', 'industry': ''}

def get_massive_gainers(filters):
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
                if not (price and prev_close and price > 0 and prev_close > 0): continue
                pct        = ((price - prev_close) / prev_close) * 100
                dollar_vol = price * volume
                if (sym and filters['min_price'] <= price <= filters['max_price']
                        and pct >= filters['min_gap_pct'] and dollar_vol >= filters['min_dollar_vol']):
                    candidates.append({
                        'ticker': sym, 'price': float(price), 'prev_close': float(prev_close),
                        'gap_pct': round(float(pct), 1), 'volume': float(volume),
                        'dollar_vol': float(dollar_vol), 'source': 'Live'
                    })
            log_alert(f"📡 Massive: {len(candidates)} stocks")
            return candidates
        log_alert(f"⚠️ Massive error: {r.status_code}")
        return []
    except Exception as e:
        log_alert(f"⚠️ Massive error: {e}")
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
                dollar_vol = price * vol
                if (sym and filters['min_price'] <= price <= filters['max_price']
                        and pct >= filters['min_gap_pct'] and dollar_vol >= filters['min_dollar_vol']):
                    candidates.append({
                        'ticker': sym, 'price': float(price), 'prev_close': float(prev),
                        'gap_pct': round(float(pct), 1), 'volume': float(vol),
                        'dollar_vol': float(dollar_vol), 'source': 'Live'
                    })
        except:
            pass
    log_alert(f"📡 Yahoo: {len(candidates)} stocks")
    return candidates

def grade_setup(gap_pct, dollar_vol):
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
        grade = "A+" if grade == "A" else grade
    elif dollar_vol >= 1_000_000:
        notes.append(f"Strong $vol ${dollar_vol/1e6:.1f}M ✅")
    else:
        notes.append(f"$Vol ${dollar_vol:,.0f}")
    return grade, notes

def is_biotech(headlines):
    combined = " ".join(headlines).lower()
    return any(kw in combined for kw in BIOTECH_KEYWORDS)

def process_ticker(stock_data, mode='standard'):
    try:
        ticker     = stock_data['ticker']
        price      = stock_data['price']
        prev_close = stock_data['prev_close']
        gap_pct    = stock_data['gap_pct']
        dollar_vol = stock_data['dollar_vol']
        source     = stock_data.get('source', 'Live')

        grade, notes = grade_setup(gap_pct, dollar_vol)
        strength, news_count, headlines, warning = check_catalyst(ticker)

        if mode == 'biotech' and not is_biotech(headlines):
            return None

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
            f"📰 {news_count} article(s)" if news_count > 0 else "📰 0 articles"
        )

        # Company info
        info = get_company_info(ticker)

        # Halt check
        halt_info = get_halt_info(ticker)

        try:
            stock     = yf.Ticker(ticker)
            hist      = stock.history(period="10d", interval="1d")
            hist_1m   = stock.history(period="1d",  interval="1m")
            volume    = float(hist_1m['Volume'].sum()) if not hist_1m.empty else dollar_vol / price if price > 0 else 0
            avg_vol   = float(hist['Volume'].mean()) if not hist.empty else 0
            vol_ratio = volume / avg_vol if avg_vol > 0 else 0
            closes    = hist['Close'].values.tolist() if not hist.empty else []
        except:
            vol_ratio = 0
            closes    = []

        pattern, pattern_desc, pattern_criteria = detect_sykes_pattern(
            ticker, price, prev_close, gap_pct, closes, vol_ratio, dollar_vol)

        entry_low  = round(price * 0.99, 2)
        entry_high = round(price * 1.02, 2)
        stop_loss  = round(entry_low * 0.95, 2)
        target1    = round(entry_high * 1.10, 2)
        target2    = round(entry_high * 1.20, 2)
        target3    = round(entry_high * 1.30, 2)

        return {
            'ticker':           ticker,
            'company_name':     info.get('company_name', ''),
            'exchange':         info.get('exchange', ''),
            'industry':         info.get('industry', ''),
            'price':            round(price, 2),
            'prev_close':       round(prev_close, 2),
            'gap_pct':          round(gap_pct, 1),
            'dollar_vol':       round(dollar_vol, 0),
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
            'source':           source,
            'time':             datetime.now().strftime('%H:%M:%S')
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

    status = {'phase':'fetching','message':f'📡 Fetching [{SCAN_MODES[mode]["label"]}]...','progress':0,'total':0}
    await broadcast_to_user(user_id, {'type':'scan_status','status':status})
    await asyncio.sleep(0)

    candidates = get_massive_gainers(filters)
    if not candidates:
        status['message'] = '⚠️ Massive empty, trying Yahoo...'
        await broadcast_to_user(user_id, {'type':'scan_status','status':status})
        await asyncio.sleep(0)
        candidates = get_yahoo_gainers(filters)

    total = len(candidates)
    log_alert(f"📊 [{user_id}] {total} candidates")

    status = {'phase':'analyzing','message':f'Analyzing {total} candidates...','progress':0,'total':total}
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

        setup = process_ticker(stock_data, mode)
        if setup:
            results.append(setup)
            await broadcast_to_user(user_id, {'type':'new_ticker','setup':setup})
            await asyncio.sleep(0)

            if setup['grade'] in ['A+','A'] and not setup['warning'] and ticker not in s['alerted']:
                s['alerted'].add(ticker)
                log_alert(f"🚀 [{user_id}] {setup['grade']}: {ticker} +{setup['gap_pct']}%")
                send_email(
                    subject=f"🚀 {setup['grade']} — {ticker} +{setup['gap_pct']}%",
                    body=(f"Grade: {setup['grade']}\nPattern: {setup['pattern']}\n"
                          f"Company: {setup.get('company_name','')} {setup.get('exchange','')}\n"
                          f"Gap: +{setup['gap_pct']}%\nPrice: ${setup['price']}\n\n"
                          f"Entry: ${setup['entry_low']}–${setup['entry_high']}\n"
                          f"Stop: ${setup['stop_loss']}\nT1: ${setup['target1']}\n")
                )

    results.sort(
        key=lambda x: (0 if x['warning'] else {'A+':4,'A':3,'B':2}.get(x['grade'],1)),
        reverse=True
    )
    done_status = {'phase':'done','message':f'✅ {len(results)} setup(s) found','progress':total,'total':total}
    return results, done_status

async def scanner_loop(user_id):
    s = user_state[user_id]
    log_alert(f"🔍 [{user_id}] Scanner started [{SCAN_MODES[s['mode']]['label']}]")
    await broadcast_to_user(user_id, {'type':'status','running':True,'mode':s['mode'],'filters':s['filters']})

    while s['running']:
        try:
            results, done_status = await do_scan(user_id)
            s['scan_results']    = results
            await broadcast_to_user(user_id, {
                'type':        'scan_results',
                'data':        results,
                'time':        datetime.now().strftime('%H:%M:%S'),
                'count':       len(results),
                'alerts':      alert_log[:20],
                'scan_status': done_status,
                'mode':        s['mode'],
                'filters':     s['filters']
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
    log_alert(f"📱 [{user_id}] Client connected")

    s = user_state[user_id]
    await websocket.send_json({
        'type':'scan_results','data':s['scan_results'],'alerts':alert_log[:20],
        'time':datetime.now().strftime('%H:%M:%S'),'count':len(s['scan_results']),
        'scan_status':{'phase':'idle'},'mode':s['mode'],'filters':s['filters']
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