from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import yfinance as yf
import requests
import smtplib
import threading
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

scanner_running     = False
alerted_tickers     = set()
scan_results        = []
alert_log           = []
connected_clients   = []
active_scan_mode    = 'standard'
current_scan_status = {'phase':'idle','message':'Scanner stopped','progress':0,'total':0}

active_filters = {
    'min_price':      0.10,
    'max_price':      50.00,
    'min_gap_pct':    20.0,
    'min_dollar_vol': 50000,
}

SCAN_MODES = {
    'standard':  {'label':'🔍 Standard',    'emoji':'🔍','min_price':0.10,'max_price':50.00,'min_gap':20.0,'min_dvol':50_000,   'desc':'Gap 20%+, $0.10–$50, $50K vol'},
    'supernova': {'label':'🚀 Supernovas',  'emoji':'🚀','min_price':0.10,'max_price':50.00,'min_gap':50.0,'min_dvol':100_000,  'desc':'Gap 50%+ only — massive movers'},
    'biotech':   {'label':'🧬 Biotech',     'emoji':'🧬','min_price':0.50,'max_price':50.00,'min_gap':15.0,'min_dvol':100_000,  'desc':'Biotech/FDA plays, gap 15%+'},
    'low_float': {'label':'⚡ Low Float',   'emoji':'⚡','min_price':0.10,'max_price':20.00,'min_gap':15.0,'min_dvol':50_000,   'desc':'Under $20, low float runners'},
    'premarket': {'label':'🌅 Pre-Market',  'emoji':'🌅','min_price':0.10,'max_price':50.00,'min_gap':15.0,'min_dvol':25_000,   'desc':'Pre-market gaps 15%+, looser vol'},
    'squeeze':   {'label':'💎 Squeeze',     'emoji':'💎','min_price':1.00,'max_price':50.00,'min_gap':10.0,'min_dvol':500_000,  'desc':'Short squeeze — high dollar vol'},
    'small_cap': {'label':'📊 Small Cap',   'emoji':'📊','min_price':1.00,'max_price':15.00,'min_gap':15.0,'min_dvol':100_000,  'desc':'Small cap $1–$15, gap 15%+'},
    'afternoon': {'label':'🌆 Afternoon',   'emoji':'🌆','min_price':0.10,'max_price':50.00,'min_gap':5.0, 'min_dvol':50_000,   'desc':'Afternoon HOD breakouts, gap 5%+'},
    'penny':     {'label':'🪙 Penny',       'emoji':'🪙','min_price':0.01,'max_price':1.00, 'min_gap':20.0,'min_dvol':10_000,   'desc':'Pure penny stocks under $1'},
    'custom':    {'label':'⚙️ Custom',      'emoji':'⚙️','min_price':0.10,'max_price':50.00,'min_gap':20.0,'min_dvol':50_000,   'desc':'Your custom filter settings'},
}

STRONG_KEYWORDS = ['fda','approval','approved','breakthrough','contract','partnership','merger',
    'acquisition','earnings','beat','guidance','revenue','clinical','trial','phase','results',
    'patent','exclusive','launch','deal','awarded','wins','secures','signs','robot','ai',
    'technology','crypto','bitcoin']
DANGER_KEYWORDS = ['offering','dilut','shelf','warrant','investigation','lawsuit','sec',
    'subpoena','delay','failed','withdrawn','suspended','reverse split','compliance']
BIOTECH_KEYWORDS = ['fda','trial','phase','clinical','drug','therapy','biotech','pharmaceutical',
    'approval','nda','bla','cancer','oncology','rare disease','orphan']

# ============================================================
# HELPERS
# ============================================================

def send_email(subject, body):
    def _send():
        try:
            msg = MIMEMultipart()
            msg['From'] = EMAIL_ADDRESS; msg['To'] = EMAIL_TO; msg['Subject'] = subject
            msg.attach(MIMEText(body,'plain'))
            s = smtplib.SMTP('smtp.gmail.com',587); s.starttls()
            s.login(EMAIL_ADDRESS,EMAIL_PASSWORD)
            s.sendmail(EMAIL_ADDRESS,EMAIL_TO,msg.as_string()); s.quit()
            log_alert(f"📧 Email: {subject}")
        except Exception as e:
            log_alert(f"⚠️ Email failed: {e}")
    threading.Thread(target=_send,daemon=True).start()

def log_alert(message):
    alert_log.insert(0,{'time':datetime.now().strftime('%H:%M:%S'),'message':message})
    if len(alert_log)>100: alert_log.pop()

async def broadcast(data):
    disconnected=[]
    for client in connected_clients:
        try: await client.send_json(data)
        except: disconnected.append(client)
    for c in disconnected:
        if c in connected_clients: connected_clients.remove(c)

# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/api/debug")
async def debug():
    return {'finnhub_key_set':bool(FINNHUB_KEY),
            'finnhub_key_preview':FINNHUB_KEY[:8]+'...' if FINNHUB_KEY else 'NOT SET',
            'polygon_key_set':bool(POLYGON_API_KEY),
            'active_mode':active_scan_mode,'active_filters':active_filters}

@app.get("/api/scan_modes")
async def get_scan_modes():
    return {'modes':SCAN_MODES,'active':active_scan_mode,'filters':active_filters}

@app.post("/api/scanner/mode/{mode}")
async def set_scan_mode(mode: str):
    global active_scan_mode, active_filters, scan_results, scanner_running
    if mode not in SCAN_MODES:
        return {'error': f'Unknown mode: {mode}'}
    m = SCAN_MODES[mode]
    active_scan_mode = mode
    active_filters = {
        'min_price':      m['min_price'],
        'max_price':      m['max_price'],
        'min_gap_pct':    m['min_gap'],
        'min_dollar_vol': m['min_dvol'],
    }
    log_alert(f"🔄 Mode: {m['label']} — {m['desc']}")
    return {'status':'ok','mode':mode,'filters':active_filters}

@app.post("/api/scanner/filters")
async def set_filters(filters: dict):
    global active_filters, active_scan_mode
    active_scan_mode = 'custom'
    if 'min_price'      in filters: active_filters['min_price']      = float(filters['min_price'])
    if 'max_price'      in filters: active_filters['max_price']      = float(filters['max_price'])
    if 'min_gap_pct'    in filters: active_filters['min_gap_pct']    = float(filters['min_gap_pct'])
    if 'min_dollar_vol' in filters: active_filters['min_dollar_vol'] = float(filters['min_dollar_vol'])
    log_alert(f"⚙️ Custom filters: ${active_filters['min_price']}–${active_filters['max_price']} | "
              f"Gap {active_filters['min_gap_pct']}%+ | Vol ${active_filters['min_dollar_vol']:,.0f}+")
    return {'status':'ok','filters':active_filters,'mode':'custom'}

@app.get("/api/stock/quote/{ticker}")
async def get_stock_quote(ticker: str):
    try:
        r = requests.get("https://finnhub.io/api/v1/quote",
            params={'symbol':ticker,'token':FINNHUB_KEY},timeout=8)
        return r.json() if r.status_code==200 else {'error':f'Status {r.status_code}'}
    except Exception as e: return {'error':str(e)}

@app.get("/api/stock/profile/{ticker}")
async def get_stock_profile(ticker: str):
    try:
        r = requests.get("https://finnhub.io/api/v1/stock/profile2",
            params={'symbol':ticker,'token':FINNHUB_KEY},timeout=8)
        return r.json() if r.status_code==200 else {'error':f'Status {r.status_code}'}
    except Exception as e: return {'error':str(e)}

@app.get("/api/stock/metrics/{ticker}")
async def get_stock_metrics(ticker: str):
    try:
        r = requests.get("https://finnhub.io/api/v1/stock/metric",
            params={'symbol':ticker,'metric':'all','token':FINNHUB_KEY},timeout=8)
        return r.json() if r.status_code==200 else {'error':f'Status {r.status_code}'}
    except Exception as e: return {'error':str(e)}

@app.get("/api/stock/news/{ticker}")
async def get_stock_news(ticker: str):
    try:
        today=datetime.now()
        from_date=(today-timedelta(days=5)).strftime('%Y-%m-%d')
        to_date=today.strftime('%Y-%m-%d')
        r = requests.get("https://finnhub.io/api/v1/company-news",
            params={'symbol':ticker,'from':from_date,'to':to_date,'token':FINNHUB_KEY},timeout=8)
        return {'news':r.json()} if r.status_code==200 else {'news':[],'error':f'Status {r.status_code}'}
    except Exception as e: return {'news':[],'error':str(e)}

@app.get("/api/stock/search/{ticker}")
async def search_ticker(ticker: str):
    try:
        ticker = ticker.upper().strip()
        quote_r = requests.get("https://finnhub.io/api/v1/quote",
            params={'symbol':ticker,'token':FINNHUB_KEY},timeout=8)
        quote      = quote_r.json() if quote_r.status_code==200 else {}
        price      = quote.get('c',0)
        prev_close = quote.get('pc',0)
        high       = quote.get('h',0)
        low        = quote.get('l',0)
        open_price = quote.get('o',0)

        if not price or price==0:
            stock=yf.Ticker(ticker); info=stock.fast_info
            price=getattr(info,'last_price',0) or 0
            prev_close=getattr(info,'previous_close',0) or 0

        if not price or price==0:
            return {'error':f'Could not find ticker {ticker}'}

        pct_change=((price-prev_close)/prev_close*100) if prev_close>0 else 0
        stock=yf.Ticker(ticker)
        hist=stock.history(period="10d",interval="1d")
        hist_1m=stock.history(period="1d",interval="1m")
        volume=float(hist_1m['Volume'].sum()) if not hist_1m.empty else 0
        avg_vol=float(hist['Volume'].mean()) if not hist.empty else 0
        vol_ratio=volume/avg_vol if avg_vol>0 else 0
        closes=hist['Close'].values.tolist() if not hist.empty else []
        dollar_vol=price*volume

        pattern,pattern_desc,pattern_criteria=detect_sykes_pattern(
            ticker,price,prev_close,pct_change,closes,vol_ratio,dollar_vol)
        grade,notes=grade_setup(pct_change,dollar_vol)
        strength,news_count,headlines,warning=check_catalyst(ticker)

        final_grade=grade
        if warning:                                  final_grade="D"
        elif strength=='STRONG' and grade=='A':      final_grade="A+"
        elif strength=='NONE':
            if grade=='A+': final_grade='A'
            elif grade=='A': final_grade='B'

        catalyst_label=(f"☠️ Danger — {news_count} article(s)" if warning else
                        f"🔥 Strong Catalyst — {news_count} article(s)" if strength=='STRONG' else
                        f"✅ Moderate — {news_count} article(s)" if strength=='MODERATE' else
                        f"📰 {news_count} article(s) in 5 days" if news_count>0 else
                        "📰 0 articles in past 5 days")

        entry_low=round(price*0.99,2); entry_high=round(price*1.02,2)
        stop_loss=round(entry_low*0.95,2)
        target1=round(entry_high*1.10,2); target2=round(entry_high*1.20,2); target3=round(entry_high*1.30,2)

        return {
            'ticker':ticker,'price':round(price,2),'prev_close':round(prev_close,2),
            'gap_pct':round(pct_change,1),'high':round(high,2),'low':round(low,2),
            'open':round(open_price,2),'dollar_vol':round(dollar_vol,0),
            'vol_ratio':round(vol_ratio,1),'grade':final_grade,'notes':notes,
            'catalyst':catalyst_label,'news_count':news_count,'headlines':headlines[:2],
            'warning':warning,'pattern':pattern,'pattern_desc':pattern_desc,
            'pattern_criteria':pattern_criteria,
            'entry_low':entry_low,'entry_high':entry_high,'stop_loss':stop_loss,
            'target1':target1,'target2':target2,'target3':target3,
            'source':'Search','time':datetime.now().strftime('%H:%M:%S')
        }
    except Exception as e: return {'error':str(e)}

# ============================================================
# SYKES PATTERN DETECTION — 14 PATTERNS
# ============================================================

def detect_sykes_pattern(ticker,price,prev_close,pct_change,closes,vol_ratio,dollar_vol):
    n=len(closes)

    if pct_change>=50:
        return("🚀 Supernova",
            "Stock exploded 50%+ in one session — classic Sykes supernova. Massive volatility and liquidity. "
            "Sykes: 'With the supernova the amount they go up can vary — sometimes 2x, 3x, even 10x or more.'",
            [f"✅ Up {pct_change:.1f}% today (need 50%+)",
             f"✅ Volume {vol_ratio:.1f}x average" if vol_ratio>=3 else f"⚠️ Volume {vol_ratio:.1f}x (want 3x+)",
             f"✅ Dollar volume ${dollar_vol/1e6:.2f}M" if dollar_vol>=1e6 else f"⚠️ Dollar vol ${dollar_vol:,.0f} (want $1M+)",
             "📌 Sykes Rule: Buy momentum OR wait for first red day to short",
             "📌 Entry: Buy early momentum or dip buy the first panic",
             "📌 Exit: Sell into strength — supernovas crash hard and fast",
             "📌 Watch: Morning panic next day = dip buy opportunity",
             "⚠️ Risk: Supernovas can crash 90% in one day — always use stop loss"])

    if n>=5:
        recent_high=max(closes[-5:])
        recent_run=((recent_high-closes[-5])/closes[-5]*100) if closes[-5]>0 else 0
        dip_from_high=((price-recent_high)/recent_high*100) if recent_high>0 else 0
        if recent_run>=50 and dip_from_high<=-20 and pct_change>0:
            return("🎯 Morning Panic Dip Buy",
                "Sykes #1 all-time favorite pattern. Stock ran 50%+ recently then panicked hard at the open. "
                "Stop losses cascade creating a massive sell-off. Buyers see the price as a discount and step in.",
                [f"✅ Stock ran {recent_run:.0f}% recently (need 50%+)",
                 f"✅ Down {abs(dip_from_high):.0f}% from recent high — panic territory",
                 f"✅ Bouncing {pct_change:.1f}% — buyers stepping in",
                 f"✅ Volume {vol_ratio:.1f}x average" if vol_ratio>=2 else f"⚠️ Volume {vol_ratio:.1f}x (want 2x+)",
                 "📌 Sykes Rule: Wait for selling to STOP before buying — patience is key",
                 "📌 Entry: Buy when Level 2 shows a wall of buyers forming",
                 "📌 Watch for: Double bottom = stronger bounce (second bottom holds better)",
                 "📌 Exit: Quick scalp — sell into the bounce, never hold and hope",
                 "⚠️ Risk: Cut losses quickly if it breaks support"])

    if n>=4:
        prev_days_red=all(closes[i]<closes[i-1] for i in range(-3,-1))
        if prev_days_red and pct_change>5:
            return("🟢 First Green Day",
                "After multiple consecutive red days, this is the first green candle with volume support. "
                "Jack Kellogg's (#1 student, $22M+ profits) all-time favorite pattern. "
                "Look for stocks closing near highs — candidate for overnight hold and gap up.",
                [f"✅ First green day up {pct_change:.1f}% after 2+ red days",
                 f"✅ Volume {vol_ratio:.1f}x average" if vol_ratio>=2 else f"⚠️ Volume {vol_ratio:.1f}x (want 2x+)",
                 "📌 Sykes Rule: Buy dips on the run-up — never chase the spike",
                 "📌 Entry: Wait for morning dip, buy bounce toward HOD",
                 "📌 Entry: Afternoon breakout above HOD is safer for beginners",
                 "📌 If closing near HOD → consider overnight hold for gap up next morning",
                 "📌 Exit: Sell into gap up next morning — don't get greedy",
                 "⚠️ First red day after this = potential short opportunity"])

    if n>=6:
        green_days=sum(1 for i in range(-5,0) if closes[i]>closes[i-1])
        total_run=((closes[-1]-closes[-6])/closes[-6]*100) if closes[-6]>0 else 0
        if green_days>=3 and pct_change>0 and total_run>=30:
            return("📈 Multi-Day Breakout",
                "Stock running consecutively for multiple days — Jack Kellogg's all-time favorite. "
                "Sykes: 'Stocks that spike 50-500% usually don't do it in one day. It takes several days.' "
                "Each dip along the multi-day run is a buy opportunity.",
                [f"✅ {green_days} of last 5 days green",
                 f"✅ Total {total_run:.0f}% run over multiple days",
                 f"✅ Up {pct_change:.1f}% today — momentum continuing",
                 f"✅ Volume {vol_ratio:.1f}x average" if vol_ratio>=1.5 else f"⚠️ Volume {vol_ratio:.1f}x (want 1.5x+)",
                 "📌 Sykes Rule: Buy dips along the multi-day run — take singles",
                 "📌 Entry: Buy 10-20% dips off morning highs for best risk/reward",
                 "📌 Watch: Stock closing near HOD each day = bullish continuation",
                 "📌 Exit: Sell into strength — don't hold through the crash",
                 "⚠️ Risk: Multi-day runners crash hard — know your exit before entry"])

    if pct_change>=15 and vol_ratio>=2:
        return("⚡ Gap and Go",
            "Stock gapped up significantly on a catalyst and continues higher with volume support. "
            "Shorts get squeezed adding fuel to the fire. Sykes only trades these with confirmed news.",
            [f"✅ Gapped up {pct_change:.1f}% from previous close",
             f"✅ Volume {vol_ratio:.1f}x average" if vol_ratio>=3 else f"⚠️ Volume {vol_ratio:.1f}x (want 3x+)",
             "📌 Sykes Rule: ONLY trade Gap and Go with a strong catalyst — no catalyst = skip",
             "📌 Entry: Buy first pullback after open — never buy the gap open price",
             "📌 Exit: Scale out in thirds — T1 +10%, T2 +20%, T3 +30%",
             "📌 Watch: If it holds the gap and doesn't fill, very bullish",
             "⚠️ Risk: Gap fills happen fast — stop loss below gap support"])

    if n>=5 and pct_change>=5:
        recent_max=max(closes[-5:])
        flagpole=((recent_max-closes[-5])/closes[-5]*100) if closes[-5]>0 else 0
        flag_range=((recent_max-min(closes[-3:]))/recent_max*100) if recent_max>0 else 0
        if flagpole>=15 and flag_range<=15 and pct_change<flagpole*0.5:
            return("🏳️ Bull Flag / Pennant",
                "Spike (flagpole) followed by tight consolidation (flag) then breakout. "
                "Sykes: 'When a stock spikes and bases, it proves it can hold the new level.' "
                "Volume drops during flag, spikes on breakout.",
                [f"✅ Flagpole: {flagpole:.0f}% spike",
                 f"✅ Consolidation range: {flag_range:.1f}% (tight = bullish)",
                 f"✅ Up {pct_change:.1f}% testing breakout",
                 "📌 Sykes Rule: Wait for CONFIRMED breakout above flag top — never buy the flag",
                 "📌 Entry: Buy breakout above flag's high with volume",
                 "📌 Watch: Volume should DROP during flag, SPIKE on breakout",
                 "📌 Exit: Measured move = flagpole length added to breakout point",
                 "⚠️ Risk: Failed breakouts look exactly like real ones — cut losses fast"])

    if n>=6 and pct_change>=3:
        step_pattern=all(closes[i]>closes[i-1]*0.93 for i in range(-4,-1))
        total_climb=((closes[-1]-closes[-6])/closes[-6]*100) if closes[-6]>0 else 0
        if step_pattern and total_climb>=20:
            return("🪜 Stair Stepper",
                "Slower supernova — rises progressively with brief pullbacks forming a staircase. "
                "Sykes: 'Each step acts as both support and resistance.' "
                "Can turn suddenly if catalyst fades.",
                [f"✅ Progressive uptrend {total_climb:.0f}% with stair-step structure",
                 f"✅ Up {pct_change:.1f}% continuing the staircase",
                 f"✅ Volume {vol_ratio:.1f}x average" if vol_ratio>=1.5 else f"⚠️ Volume {vol_ratio:.1f}x (want 1.5x+)",
                 "📌 Sykes Rule: Buy the pullbacks (steps), sell into the spikes",
                 "📌 Entry: Buy near the bottom of each step at support",
                 "📌 Use the bottom of large green candles as your stop",
                 "📌 Exit: Sell into each spike — take singles, let winners run",
                 "⚠️ Risk: Can reverse suddenly without warning if catalyst fades"])

    if n>=5:
        recent_high_5d=max(closes[-5:])
        if price>=recent_high_5d*0.97 and vol_ratio>=2 and pct_change>5:
            return("💥 Breakout",
                "Breaking above recent resistance with volume confirmation. "
                "Sykes: 'Buy breakouts only with volume — never buy a stock you THINK will breakout.' "
                "Low float stocks can explode 30-100% in minutes on breakouts.",
                [f"✅ Price near 5-day high (${recent_high_5d:.2f})",
                 f"✅ Up {pct_change:.1f}% today",
                 f"✅ Volume {vol_ratio:.1f}x confirming" if vol_ratio>=2 else f"⚠️ Volume {vol_ratio:.1f}x (need 2x+)",
                 "📌 Sykes Rule: Only buy with volume — no volume = fake breakout",
                 "📌 Entry: Buy the breakout above resistance, stop below it",
                 "📌 Entry: Afternoon breakout above HOD is safer for beginners",
                 "📌 Exit: Sell into the spike — take singles, don't get greedy",
                 "⚠️ Risk: Failed breakouts trap longs — cut losses if it reverses fast"])

    if n>=7:
        mid=n//2
        left_high=max(closes[:mid]) if mid>0 else 0
        cup_bottom=min(closes[mid-2:mid+2]) if mid>2 else 0
        right_high=max(closes[mid:]) if mid<n else 0
        cup_depth=((left_high-cup_bottom)/left_high*100) if left_high>0 else 0
        recovery=((right_high-cup_bottom)/cup_bottom*100) if cup_bottom>0 else 0
        if cup_depth>=15 and cup_depth<=60 and recovery>=50 and pct_change>0:
            return("☕ Cup and Handle",
                "U-shaped recovery followed by small handle consolidation then breakout. "
                "Sykes called a 'beautiful premarket cup and handle' in Nov 2025. "
                "Very reliable when volume confirms the breakout.",
                [f"✅ Cup depth: {cup_depth:.0f}% pullback then recovery",
                 f"✅ Recovery: {recovery:.0f}% off cup bottom",
                 f"✅ Up {pct_change:.1f}% — handle/breakout forming",
                 "📌 Sykes Rule: Wait for handle breakout confirmation",
                 "📌 Entry: Buy breakout above handle's high with volume surge",
                 "📌 Intraday cup and handle can form in one session",
                 "📌 Exit: Target = cup depth added to breakout point",
                 "⚠️ Risk: Messy cups are harder to trade — cleaner = better"])

    now_hour=datetime.now().hour
    if 13<=now_hour<=16 and pct_change>=5 and vol_ratio>=1.5:
        return("🌆 Afternoon Breakout",
            "Breaking above morning HOD in the afternoon. "
            "Sykes: 'Safer for beginners — not as fast as premarket. "
            "In the afternoon you have more information. If it breaks HOD with volume, it can explode quickly.'",
            [f"✅ Up {pct_change:.1f}% in afternoon session",
             f"✅ Volume {vol_ratio:.1f}x average" if vol_ratio>=2 else f"⚠️ Volume {vol_ratio:.1f}x (want 2x+)",
             "📌 Sykes Rule: Draw line at morning HOD — if it breaks convincingly, buy it",
             "📌 Entry: Buy HOD breakout in afternoon with volume surge",
             "📌 Why safer: More info available, less volatile than premarket",
             "📌 Exit: Sell before close unless closing near HOD for overnight hold",
             "⚠️ Risk: Afternoon fades happen — if it reverses quickly, exit fast"])

    if 15<=now_hour<=16 and n>=3:
        prev_red=closes[-2]<closes[-3] if n>=3 else False
        if pct_change>=10 and prev_red:
            return("🌙 Overnight Hold Setup",
                "Stock closing strong near HOD on first green day. "
                "Sykes: 'Buy before close to hold overnight. Goal is to sell into gap up next morning. "
                "OTC stocks pile up orders overnight creating a gap up at the open.'",
                [f"✅ Up {pct_change:.1f}% closing strong near HOD",
                 "✅ Previous days were red — first green day setup",
                 f"✅ Volume {vol_ratio:.1f}x" if vol_ratio>=2 else f"⚠️ Volume {vol_ratio:.1f}x (want 2x+ for overnight)",
                 "📌 Sykes Rule: Only hold overnight if closing within 10% of HOD",
                 "📌 Entry: Buy in last 30 minutes of trading session",
                 "📌 Exit: Sell into gap up next morning — don't get greedy",
                 "📌 Works best: OTC/penny stocks that don't trade afterhours",
                 "⚠️ Risk: Bad news overnight = gap DOWN — small position size"])

    if n>=3 and pct_change>=3:
        dip1=((closes[-2]-closes[-3])/closes[-3]*100) if closes[-3]>0 else 0
        if dip1<=-10 and pct_change>0:
            return("📉 Double Bottom Morning Panic",
                "Two panic lows forming support — classic Sykes double bottom. "
                "Sykes: 'Second bottom tends to hold better if the first doesn't. "
                "Add to the trade on the second bounce for better average entry.'",
                [f"✅ First panic leg: {dip1:.0f}% drop",
                 f"✅ Bouncing {pct_change:.1f}% — second leg forming",
                 "📌 Sykes Rule: The second bottom holds better than the first",
                 "📌 Entry: Buy the second bottom NOT the first",
                 "📌 Look for: W-shape on chart — two lows at similar level",
                 "📌 Level 2: Watch for wall of buyers at second bottom",
                 "📌 Exit: Sell into the bounce — target previous resistance",
                 "⚠️ Risk: If second bottom breaks lower, exit immediately"])

    if n>=3 and pct_change<-10:
        prev_was_big_up=closes[-2]>closes[-3]*1.2 if n>=3 else False
        if prev_was_big_up:
            return("🔴 Supernova Fade (Short)",
                "First red day after a big run — Sykes' prime short selling setup. "
                "Sykes: 'Look to short sell on first red day. Every recovery met with more selling.' "
                "Trapped longs panic selling adds to downside.",
                [f"✅ Down {abs(pct_change):.1f}% after big prior run",
                 "✅ Previous session was up 20%+ — overextended runner fading",
                 f"⚠️ Volume {vol_ratio:.1f}x average",
                 "📌 Sykes Rule: Short the first red day — every bounce is a sell",
                 "📌 Entry: Short into bounces — sell the rips, don't chase the drop",
                 "📌 Overhead resistance = your risk level",
                 "📌 Exit: Cover into panics — buy dips to cover short",
                 "⚠️ Risk: Short squeezes can be brutal — keep position small"])

    if n>=5 and pct_change<-5:
        consec_red=sum(1 for i in range(-4,0) if closes[i]<closes[i-1])
        if consec_red>=3:
            return("🦅 The Crow — AVOID",
                "Continuous selling pressure — every recovery met with more selling. "
                "Sykes: 'The overall downtrend is clear. Every recovery gets sold into. "
                "I don't recommend trading this pattern.'",
                [f"⚠️ Down {abs(pct_change):.1f}% today — continuing downtrend",
                 f"⚠️ {consec_red} of last 4 days red — strong downtrend",
                 "🚨 Sykes Rule: DO NOT trade The Crow on the long side",
                 "🚨 Every bounce attempt gets sold — don't catch falling knives",
                 "📌 Sykes: 'The trades you don't take are often more important'",
                 "🚨 SKIP THIS SETUP — wait for a better opportunity"])

    if pct_change>=10:
        return("📊 Momentum Play",
            "Decent move but doesn't fit a clean Sykes pattern. "
            "Sykes: 'Pattern/price is only 20% of my reason for a trade. "
            "Use the Sykes Sliding Scale to check all 7 indicators.' Trade with caution.",
            [f"⚠️ Up {pct_change:.1f}% — no clean Sykes pattern",
             f"⚠️ Volume {vol_ratio:.1f}x average",
             "📌 Check: catalyst? float? sector hot?",
             "📌 Wait for a cleaner setup — don't force trades",
             "📌 Pattern/price is only 20% of the decision"])

    return("🔍 No Clear Pattern",
        "Does not match any defined Sykes setup. "
        "Sykes: 'I only trade a few select patterns. If there's no pattern, there's no trade.'",
        [f"⚠️ Up {pct_change:.1f}% — move too small for Sykes patterns",
         "📌 Sykes Rule: If there's no pattern, there's no trade",
         "🚨 SKIP — not a Sykes setup"])

# ============================================================
# CATALYST CHECK
# ============================================================

def check_catalyst(ticker):
    try:
        today=datetime.now()
        from_date=(today-timedelta(days=5)).strftime('%Y-%m-%d')
        to_date=today.strftime('%Y-%m-%d')
        r=requests.get('https://finnhub.io/api/v1/company-news',
            params={'symbol':ticker,'from':from_date,'to':to_date,'token':FINNHUB_KEY},timeout=8)
        news_items=r.json()[:15] if r.status_code==200 else []

        if not news_items:
            yf_news=yf.Ticker(ticker).news or []
            cutoff=datetime.now()-timedelta(days=5)
            for n in yf_news[:10]:
                pub=n.get('providerPublishTime',0)
                if pub and datetime.fromtimestamp(pub)>=cutoff:
                    news_items.append({'headline':n.get('title',''),'source':'yf'})

        news_count=len(news_items); headlines=[]; strong_hits=0; danger_hits=0; warning=False
        for item in news_items[:5]:
            title=(item.get('headline') or item.get('title') or '').lower()
            headlines.append(item.get('headline') or item.get('title') or '')
            if any(kw in title for kw in STRONG_KEYWORDS): strong_hits+=1
            if any(kw in title for kw in DANGER_KEYWORDS): danger_hits+=1; warning=True

        if not news_items: return 'NONE',0,[],False
        if warning and danger_hits>strong_hits: return 'DANGER',news_count,headlines[:2],True
        elif strong_hits>=2: return 'STRONG',news_count,headlines[:2],False
        elif strong_hits==1: return 'MODERATE',news_count,headlines[:2],False
        else: return 'WEAK',news_count,headlines[:2],False
    except: return 'NONE',0,[],False

# ============================================================
# DATA SOURCES
# ============================================================

def get_massive_gainers(filters):
    try:
        url=(f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
             f"?apiKey={POLYGON_API_KEY}&include_otc=false")
        r=requests.get(url,timeout=15)
        candidates=[]
        if r.status_code==200:
            for t in r.json().get('tickers',[]):
                sym=t.get('ticker',''); day=t.get('day',{}); prev=t.get('prevDay',{}); last=t.get('lastTrade',{})
                price=last.get('p') or day.get('c',0); prev_close=prev.get('c',0); volume=day.get('v',0)
                if not (price and prev_close and price>0 and prev_close>0): continue
                pct=((price-prev_close)/prev_close)*100; dollar_vol=price*volume
                if (sym and filters['min_price']<=price<=filters['max_price']
                        and pct>=filters['min_gap_pct'] and dollar_vol>=filters['min_dollar_vol']):
                    candidates.append({'ticker':sym,'price':float(price),'prev_close':float(prev_close),
                        'gap_pct':round(float(pct),1),'volume':float(volume),'dollar_vol':float(dollar_vol),'source':'Live'})
            log_alert(f"📡 Massive: {len(candidates)} stocks")
            return candidates
        log_alert(f"⚠️ Massive error: {r.status_code}"); return []
    except Exception as e:
        log_alert(f"⚠️ Massive error: {e}"); return []

def get_yahoo_gainers(filters):
    candidates=[]; headers={'User-Agent':'Mozilla/5.0'}
    for scrId in ['day_gainers','small_cap_gainers']:
        try:
            url=(f"https://query1.finance.yahoo.com/v1/finance/screener/"
                 f"predefined/saved?formatted=false&scrIds={scrId}&count=50")
            r=requests.get(url,headers=headers,timeout=10)
            if r.status_code!=200: continue
            quotes=(r.json().get('finance',{}).get('result',[{}])[0].get('quotes',[]))
            for q in quotes:
                sym=q.get('symbol',''); price=q.get('regularMarketPrice',0)
                pct=q.get('regularMarketChangePercent',0); vol=q.get('regularMarketVolume',0)
                prev=q.get('regularMarketPreviousClose',0); dollar_vol=price*vol
                if (sym and filters['min_price']<=price<=filters['max_price']
                        and pct>=filters['min_gap_pct'] and dollar_vol>=filters['min_dollar_vol']):
                    candidates.append({'ticker':sym,'price':float(price),'prev_close':float(prev),
                        'gap_pct':round(float(pct),1),'volume':float(vol),'dollar_vol':float(dollar_vol),'source':'Live'})
        except: pass
    log_alert(f"📡 Yahoo: {len(candidates)} stocks")
    return candidates

# ============================================================
# GRADE + PROCESS
# ============================================================

def grade_setup(gap_pct,dollar_vol):
    grade,notes="B",[]
    if gap_pct>=50:   grade="A+"; notes.append(f"Supernova +{gap_pct:.1f}% 🔥🔥🔥")
    elif gap_pct>=30: grade="A+"; notes.append(f"Huge gap +{gap_pct:.1f}% 🔥🔥")
    elif gap_pct>=20: grade="A";  notes.append(f"Strong gap +{gap_pct:.1f}% 🔥")
    elif gap_pct>=15: grade="A";  notes.append(f"Good gap +{gap_pct:.1f}%")
    elif gap_pct>=10: grade="B";  notes.append(f"Moderate gap +{gap_pct:.1f}%")
    elif gap_pct>0:   grade="C";  notes.append(f"Weak gap +{gap_pct:.1f}%")
    else:             grade="D";  notes.append(f"Down {gap_pct:.1f}%")
    if dollar_vol>=5_000_000:   notes.append(f"Monster $vol ${dollar_vol/1e6:.1f}M 🔥"); grade="A+" if grade=="A" else grade
    elif dollar_vol>=1_000_000: notes.append(f"Strong $vol ${dollar_vol/1e6:.1f}M ✅")
    else:                       notes.append(f"$Vol ${dollar_vol:,.0f}")
    return grade,notes

def is_biotech(ticker,headlines):
    combined=" ".join(headlines).lower()
    return any(kw in combined for kw in BIOTECH_KEYWORDS)

def process_ticker(stock_data,mode='standard'):
    try:
        ticker=stock_data['ticker']; price=stock_data['price']
        prev_close=stock_data['prev_close']; gap_pct=stock_data['gap_pct']
        dollar_vol=stock_data['dollar_vol']; source=stock_data.get('source','Live')

        grade,notes=grade_setup(gap_pct,dollar_vol)
        strength,news_count,headlines,warning=check_catalyst(ticker)

        # Biotech mode: skip non-biotech stocks
        if mode=='biotech' and not is_biotech(ticker,headlines):
            return None

        final_grade=grade
        if warning:                                  final_grade="D"
        elif strength=='STRONG' and grade=='A':      final_grade="A+"
        elif strength=='NONE':
            if grade=='A+': final_grade='A'
            elif grade=='A': final_grade='B'

        catalyst_label=(f"☠️ Danger — {news_count} article(s)" if warning else
                        f"🔥 Strong Catalyst — {news_count} article(s)" if strength=='STRONG' else
                        f"✅ Moderate — {news_count} article(s)" if strength=='MODERATE' else
                        f"📰 {news_count} article(s) in 5 days" if news_count>0 else
                        "📰 0 articles in past 5 days")

        try:
            stock=yf.Ticker(ticker)
            hist=stock.history(period="10d",interval="1d")
            hist_1m=stock.history(period="1d",interval="1m")
            volume=float(hist_1m['Volume'].sum()) if not hist_1m.empty else dollar_vol/price if price>0 else 0
            avg_vol=float(hist['Volume'].mean()) if not hist.empty else 0
            vol_ratio=volume/avg_vol if avg_vol>0 else 0
            closes=hist['Close'].values.tolist() if not hist.empty else []
        except: vol_ratio=0; closes=[]

        pattern,pattern_desc,pattern_criteria=detect_sykes_pattern(
            ticker,price,prev_close,gap_pct,closes,vol_ratio,dollar_vol)

        entry_low=round(price*0.99,2); entry_high=round(price*1.02,2)
        stop_loss=round(entry_low*0.95,2)
        target1=round(entry_high*1.10,2); target2=round(entry_high*1.20,2); target3=round(entry_high*1.30,2)

        return {
            'ticker':ticker,'price':round(price,2),'prev_close':round(prev_close,2),
            'gap_pct':round(gap_pct,1),'dollar_vol':round(dollar_vol,0),
            'grade':final_grade,'notes':notes,'catalyst':catalyst_label,
            'news_count':news_count,'headlines':headlines[:2],'warning':warning,
            'pattern':pattern,'pattern_desc':pattern_desc,'pattern_criteria':pattern_criteria,
            'entry_low':entry_low,'entry_high':entry_high,'stop_loss':stop_loss,
            'target1':target1,'target2':target2,'target3':target3,
            'source':source,'time':datetime.now().strftime('%H:%M:%S')
        }
    except: return None

# ============================================================
# SCANNER LOOP
# ============================================================

async def do_scan():
    global current_scan_status
    filters=active_filters.copy(); mode=active_scan_mode

    current_scan_status={'phase':'fetching','message':f'📡 Fetching [{SCAN_MODES[mode]["label"]}] from Massive.com...','progress':0,'total':0}
    await broadcast({'type':'scan_status','status':current_scan_status})
    await asyncio.sleep(0)

    candidates=get_massive_gainers(filters)
    if not candidates:
        current_scan_status['message']='⚠️ Massive empty, trying Yahoo...'
        await broadcast({'type':'scan_status','status':current_scan_status})
        await asyncio.sleep(0)
        candidates=get_yahoo_gainers(filters)

    total=len(candidates)
    log_alert(f"📊 {total} candidates [{SCAN_MODES[mode]['label']}]")

    current_scan_status={'phase':'analyzing','message':f'Analyzing {total} candidates...','progress':0,'total':total}
    await broadcast({'type':'scan_status','status':current_scan_status})
    await asyncio.sleep(0)

    results=[]; count=0
    for stock_data in candidates:
        if not scanner_running: break
        count+=1; ticker=stock_data.get('ticker','')
        current_scan_status={'phase':'analyzing','message':f'Checking {ticker} ({count}/{total})...','progress':count,'total':total}
        await broadcast({'type':'scan_status','status':current_scan_status})
        await asyncio.sleep(0.05)

        setup=process_ticker(stock_data,mode)
        if setup:
            results.append(setup)
            await broadcast({'type':'new_ticker','setup':setup})
            await asyncio.sleep(0)
            if setup['grade'] in ['A+','A'] and not setup['warning'] and ticker not in alerted_tickers:
                alerted_tickers.add(ticker)
                log_alert(f"🚀 {setup['grade']}: {ticker} +{setup['gap_pct']}%")
                send_email(
                    subject=f"🚀 {setup['grade']} — {ticker} +{setup['gap_pct']}%",
                    body=(f"Grade: {setup['grade']}\nPattern: {setup['pattern']}\n"
                          f"Gap: +{setup['gap_pct']}%\nPrice: ${setup['price']}\n"
                          f"Mode: {SCAN_MODES[mode]['label']}\n\n"
                          f"Entry: ${setup['entry_low']}–${setup['entry_high']}\n"
                          f"Stop: ${setup['stop_loss']}\nT1: ${setup['target1']}\n"))

    results.sort(key=lambda x:(0 if x['warning'] else {'A+':4,'A':3,'B':2}.get(x['grade'],1)),reverse=True)
    current_scan_status={'phase':'done','message':f'✅ {len(results)} setup(s) found','progress':total,'total':total}
    return results

async def scanner_loop():
    global scan_results,scanner_running,current_scan_status
    log_alert(f"🔍 Scanner started [{SCAN_MODES[active_scan_mode]['label']}]")
    await broadcast({'type':'status','running':True,'mode':active_scan_mode,'filters':active_filters})

    while scanner_running:
        try:
            results=await do_scan(); scan_results=results
            await broadcast({'type':'scan_results','data':results,
                'time':datetime.now().strftime('%H:%M:%S'),'count':len(results),
                'alerts':alert_log[:20],'scan_status':current_scan_status,
                'mode':active_scan_mode,'filters':active_filters})
        except Exception as e:
            log_alert(f"⚠️ Scanner error: {e}")
        if scanner_running:
            interval=int(os.getenv('SCAN_INTERVAL',30))
            log_alert(f"⏱ Next scan in {interval}s")
            await asyncio.sleep(interval)

    log_alert("⏹️ Scanner stopped")
    current_scan_status={'phase':'idle','message':'Scanner stopped','progress':0,'total':0}
    await broadcast({'type':'status','running':False})

# ============================================================
# API ROUTES
# ============================================================

@app.get("/api/status")
async def get_status():
    return {'running':scanner_running,'results':len(scan_results),
            'time':datetime.now().strftime('%H:%M:%S'),'mode':active_scan_mode,'filters':active_filters}

@app.get("/api/results")
async def get_results():
    return {'data':scan_results,'alerts':alert_log[:20],
            'time':datetime.now().strftime('%H:%M:%S'),'mode':active_scan_mode}

@app.post("/api/scanner/start")
async def start_scanner():
    global scanner_running,scan_results
    if not scanner_running:
        scanner_running=True; scan_results=[]
        asyncio.create_task(scanner_loop())
        log_alert("✅ Scanner started")
        return {'status':'started','mode':active_scan_mode}
    return {'status':'already running','mode':active_scan_mode}

@app.post("/api/scanner/stop")
async def stop_scanner():
    global scanner_running
    scanner_running=False
    log_alert("⏹️ Scanner stopped")
    return {'status':'stopped'}

@app.post("/api/clear-alerts")
async def clear_alerts_route():
    alerted_tickers.clear(); log_alert("🗑️ Alerts cleared")
    return {'status':'cleared'}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    log_alert("📱 Client connected")
    await websocket.send_json({
        'type':'scan_results','data':scan_results,'alerts':alert_log[:20],
        'time':datetime.now().strftime('%H:%M:%S'),'count':len(scan_results),
        'scan_status':current_scan_status,'mode':active_scan_mode,'filters':active_filters
    })
    await websocket.send_json({'type':'status','running':scanner_running,'mode':active_scan_mode})
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in connected_clients: connected_clients.remove(websocket)

@app.get("/")
async def serve_dashboard():
    try:
        with open("dashboard.html","r") as f: return HTMLResponse(f.read())
    except: return HTMLResponse("<h1>Dashboard not found</h1>")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0",port=8000)