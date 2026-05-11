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

load_dotenv()

app = FastAPI(title="Payda x UyghurKid Trading Agent")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

MIN_PRICE       = float(os.getenv('MIN_PRICE', 0.10))
MAX_PRICE       = float(os.getenv('MAX_PRICE', 50.00))
MIN_GAP_PCT     = float(os.getenv('MIN_GAP_PCT', 20.0))
MIN_DOLLAR_VOL  = float(os.getenv('MIN_DOLLAR_VOL', 50000))
SCAN_INTERVAL   = int(os.getenv('SCAN_INTERVAL', 30))
EMAIL_ADDRESS   = os.getenv('EMAIL_ADDRESS', '')
EMAIL_PASSWORD  = os.getenv('EMAIL_PASSWORD', '')
EMAIL_TO        = os.getenv('EMAIL_TO', '')
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', '')
FINNHUB_KEY     = os.getenv('FINNHUB_API_KEY', '')

scanner_running   = False
alerted_tickers   = set()
scan_results      = []
alert_log         = []
connected_clients = []
current_scan_status = {
    'phase': 'idle',
    'message': 'Scanner stopped',
    'progress': 0,
    'total': 0
}

STRONG_KEYWORDS = [
    'fda', 'approval', 'approved', 'breakthrough', 'contract',
    'partnership', 'merger', 'acquisition', 'earnings', 'beat',
    'guidance', 'revenue', 'clinical', 'trial', 'phase', 'results',
    'patent', 'exclusive', 'launch', 'deal', 'awarded', 'wins',
    'secures', 'signs', 'robot', 'ai', 'technology', 'crypto', 'bitcoin'
]

DANGER_KEYWORDS = [
    'offering', 'dilut', 'shelf', 'warrant', 'investigation',
    'lawsuit', 'sec', 'subpoena', 'delay', 'failed', 'withdrawn',
    'suspended', 'reverse split', 'compliance'
]

# ============================================================
# EMAIL
# ============================================================

def send_email(subject, body):
    def _send():
        try:
            msg = MIMEMultipart()
            msg['From']    = EMAIL_ADDRESS
            msg['To']      = EMAIL_TO
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            s = smtplib.SMTP('smtp.gmail.com', 587)
            s.starttls()
            s.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            s.sendmail(EMAIL_ADDRESS, EMAIL_TO, msg.as_string())
            s.quit()
            log_alert(f"📧 Email sent: {subject}")
        except Exception as e:
            log_alert(f"⚠️ Email failed: {e}")
    threading.Thread(target=_send, daemon=True).start()

def log_alert(message):
    alert_log.insert(0, {
        'time':    datetime.now().strftime('%H:%M:%S'),
        'message': message
    })
    if len(alert_log) > 100:
        alert_log.pop()

async def broadcast(data):
    if not connected_clients:
        return
    disconnected = []
    for client in connected_clients:
        try:
            await client.send_json(data)
        except:
            disconnected.append(client)
    for c in disconnected:
        if c in connected_clients:
            connected_clients.remove(c)

# ============================================================
# FINNHUB API ENDPOINTS (server-side to avoid CORS)
# ============================================================

@app.get("/api/stock/quote/{ticker}")
async def get_stock_quote(ticker: str):
    """Get real-time quote from Finnhub"""
    try:
        r = requests.get(
            f"https://finnhub.io/api/v1/quote",
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
    """Get company profile from Finnhub"""
    try:
        r = requests.get(
            f"https://finnhub.io/api/v1/stock/profile2",
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
    """Get key metrics from Finnhub"""
    try:
        r = requests.get(
            f"https://finnhub.io/api/v1/stock/metric",
            params={'symbol': ticker, 'metric': 'all', 'token': FINNHUB_KEY},
            timeout=8
        )
        if r.status_code == 200:
            return r.json()
        return {'error': f'Status {r.status_code}'}
    except Exception as e:
        return {'error': str(e)}

@app.get("/api/stock/news/{ticker}")
async def get_stock_news(ticker: str):
    """Get company news from Finnhub"""
    try:
        today = datetime.now()
        from_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
        to_date   = today.strftime('%Y-%m-%d')
        r = requests.get(
            f"https://finnhub.io/api/v1/company-news",
            params={
                'symbol': ticker,
                'from':   from_date,
                'to':     to_date,
                'token':  FINNHUB_KEY
            },
            timeout=8
        )
        if r.status_code == 200:
            return {'news': r.json()}
        return {'news': [], 'error': f'Status {r.status_code}'}
    except Exception as e:
        return {'news': [], 'error': str(e)}

# ============================================================
# CATALYST CHECK (for scanner)
# ============================================================

def check_catalyst(ticker):
    try:
        # Try Finnhub news first
        today     = datetime.now()
        from_date = (today - timedelta(days=2)).strftime('%Y-%m-%d')
        to_date   = today.strftime('%Y-%m-%d')

        r = requests.get(
            'https://finnhub.io/api/v1/company-news',
            params={
                'symbol': ticker,
                'from':   from_date,
                'to':     to_date,
                'token':  FINNHUB_KEY
            },
            timeout=8
        )

        news_items = []
        if r.status_code == 200:
            news_items = r.json()[:10]

        # Fallback to yfinance if no results
        if not news_items:
            yf_news = yf.Ticker(ticker).news or []
            cutoff  = datetime.now() - timedelta(hours=48)
            for n in yf_news[:10]:
                pub = n.get('providerPublishTime', 0)
                if pub and datetime.fromtimestamp(pub) >= cutoff:
                    news_items.append({'headline': n.get('title',''), 'source': 'yf'})

        if not news_items:
            return 'NONE', '❌ No Recent News', [], False, 0

        headlines   = []
        strong_hits = 0
        danger_hits = 0
        warning     = False

        for item in news_items[:5]:
            title = (item.get('headline') or item.get('title') or '').lower()
            headlines.append(item.get('headline') or item.get('title') or '')
            if any(kw in title for kw in STRONG_KEYWORDS): strong_hits += 1
            if any(kw in title for kw in DANGER_KEYWORDS): danger_hits += 1; warning = True

        news_count = len(news_items)

        if warning and danger_hits > strong_hits:
            return 'DANGER', f'☠️ DANGER: Dilution/Legal Risk ({news_count} articles)', headlines[:2], True, news_count
        elif strong_hits >= 2:
            return 'STRONG', f'🔥 STRONG Catalyst ({news_count} articles today)', headlines[:2], False, news_count
        elif strong_hits == 1:
            return 'MODERATE', f'✅ Moderate Catalyst ({news_count} articles)', headlines[:2], False, news_count
        elif news_count > 0:
            return 'WEAK', f'📰 {news_count} article(s) — weak catalyst', headlines[:2], False, news_count
        else:
            return 'NONE', '❌ No Recent News', [], False, 0

    except Exception as e:
        return 'UNKNOWN', '❓ Could not fetch news', [], False, 0

# ============================================================
# DATA SOURCES
# ============================================================

def get_massive_gainers():
    try:
        url = (f"https://api.polygon.io/v2/snapshot/locale/us/"
               f"markets/stocks/gainers"
               f"?apiKey={POLYGON_API_KEY}&include_otc=false")
        r = requests.get(url, timeout=15)
        candidates = []
        if r.status_code == 200:
            for t in r.json().get('tickers', []):
                sym  = t.get('ticker', '')
                day  = t.get('day', {})
                prev = t.get('prevDay', {})
                last = t.get('lastTrade', {})
                price      = last.get('p') or day.get('c', 0)
                prev_close = prev.get('c', 0)
                volume     = day.get('v', 0)
                if not (price and prev_close and price > 0 and prev_close > 0):
                    continue
                pct        = ((price - prev_close) / prev_close) * 100
                dollar_vol = price * volume
                if (sym and MIN_PRICE <= price <= MAX_PRICE and
                        pct >= MIN_GAP_PCT and dollar_vol >= MIN_DOLLAR_VOL):
                    candidates.append({
                        'ticker': sym, 'price': float(price),
                        'prev_close': float(prev_close),
                        'gap_pct': round(float(pct), 1),
                        'volume': float(volume),
                        'dollar_vol': float(dollar_vol),
                        'source': 'Massive'
                    })
            log_alert(f"📡 Massive: {len(candidates)} stocks")
            return candidates
        else:
            log_alert(f"⚠️ Massive API error: {r.status_code}")
            return []
    except Exception as e:
        log_alert(f"⚠️ Massive error: {e}")
        return []

def get_yahoo_gainers():
    candidates = []
    headers    = {'User-Agent': 'Mozilla/5.0'}
    for scrId in ['day_gainers', 'small_cap_gainers']:
        try:
            url = (f"https://query1.finance.yahoo.com/v1/finance/screener/"
                   f"predefined/saved?formatted=false&scrIds={scrId}&count=50")
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200:
                continue
            quotes = (r.json().get('finance', {})
                               .get('result', [{}])[0]
                               .get('quotes', []))
            for q in quotes:
                sym        = q.get('symbol', '')
                price      = q.get('regularMarketPrice', 0)
                pct        = q.get('regularMarketChangePercent', 0)
                vol        = q.get('regularMarketVolume', 0)
                prev       = q.get('regularMarketPreviousClose', 0)
                dollar_vol = price * vol
                if (sym and MIN_PRICE <= price <= MAX_PRICE and
                        pct >= MIN_GAP_PCT and dollar_vol >= MIN_DOLLAR_VOL):
                    candidates.append({
                        'ticker': sym, 'price': float(price),
                        'prev_close': float(prev),
                        'gap_pct': round(float(pct), 1),
                        'volume': float(vol),
                        'dollar_vol': float(dollar_vol),
                        'source': 'Live'
                    })
        except:
            pass
    log_alert(f"📡 Yahoo: {len(candidates)} stocks")
    return candidates

# ============================================================
# GRADE + PROCESS
# ============================================================

def grade_setup(gap_pct, dollar_vol):
    grade, notes = "B", []
    if gap_pct >= 50:   grade = "A+"; notes.append(f"Massive gap +{gap_pct:.1f}% 🔥🔥🔥")
    elif gap_pct >= 30: grade = "A+"; notes.append(f"Huge gap +{gap_pct:.1f}% 🔥🔥")
    elif gap_pct >= 20: grade = "A";  notes.append(f"Strong gap +{gap_pct:.1f}% 🔥")
    elif gap_pct >= 15: grade = "A";  notes.append(f"Good gap +{gap_pct:.1f}%")
    elif gap_pct >= 10: grade = "B";  notes.append(f"Moderate gap +{gap_pct:.1f}%")
    if dollar_vol >= 5_000_000:
        notes.append(f"Monster $vol ${dollar_vol/1e6:.1f}M 🔥")
        if grade == "A": grade = "A+"
    elif dollar_vol >= 1_000_000:
        notes.append(f"Strong $vol ${dollar_vol/1e6:.1f}M ✅")
    else:
        notes.append(f"$Vol ${dollar_vol:,.0f}")
    return grade, notes

def process_ticker(stock_data):
    try:
        ticker     = stock_data['ticker']
        price      = stock_data['price']
        prev_close = stock_data['prev_close']
        gap_pct    = stock_data['gap_pct']
        dollar_vol = stock_data['dollar_vol']
        source     = stock_data.get('source', 'Live')

        grade, notes = grade_setup(gap_pct, dollar_vol)
        strength, label, headlines, warning, news_count = check_catalyst(ticker)

        final_grade = grade
        if warning:                                   final_grade = "D"
        elif strength == 'STRONG' and grade == 'A':  final_grade = "A+"
        elif strength == 'NONE':
            if grade == 'A+':  final_grade = 'A'
            elif grade == 'A': final_grade = 'B'

        entry_low  = round(price * 0.99, 2)
        entry_high = round(price * 1.02, 2)
        stop_loss  = round(entry_low * 0.95, 2)
        target1    = round(entry_high * 1.10, 2)
        target2    = round(entry_high * 1.20, 2)
        target3    = round(entry_high * 1.30, 2)

        return {
            'ticker':     ticker,
            'price':      round(price, 2),
            'prev_close': round(prev_close, 2),
            'gap_pct':    round(gap_pct, 1),
            'dollar_vol': round(dollar_vol, 0),
            'grade':      final_grade,
            'notes':      notes,
            'catalyst':   label,
            'news_count': news_count,
            'headlines':  headlines[:2],
            'warning':    warning,
            'entry_low':  entry_low,
            'entry_high': entry_high,
            'stop_loss':  stop_loss,
            'target1':    target1,
            'target2':    target2,
            'target3':    target3,
            'source':     source,
            'time':       datetime.now().strftime('%H:%M:%S')
        }
    except:
        return None

# ============================================================
# SCANNER LOOP
# ============================================================

async def do_scan():
    global current_scan_status

    current_scan_status = {
        'phase': 'fetching',
        'message': '📡 Fetching gainers from Massive.com...',
        'progress': 0, 'total': 0
    }
    await broadcast({'type': 'scan_status', 'status': current_scan_status})
    await asyncio.sleep(0)

    candidates = get_massive_gainers()
    if not candidates:
        current_scan_status['message'] = '⚠️ Massive empty, trying Yahoo...'
        await broadcast({'type': 'scan_status', 'status': current_scan_status})
        await asyncio.sleep(0)
        candidates = get_yahoo_gainers()

    total = len(candidates)
    log_alert(f"📊 {total} candidates")

    current_scan_status = {
        'phase': 'analyzing',
        'message': f'Analyzing {total} candidates...',
        'progress': 0, 'total': total
    }
    await broadcast({'type': 'scan_status', 'status': current_scan_status})
    await asyncio.sleep(0)

    results = []
    count   = 0

    for stock_data in candidates:
        if not scanner_running:
            break

        count  += 1
        ticker  = stock_data.get('ticker', '')

        current_scan_status = {
            'phase':    'analyzing',
            'message':  f'Checking {ticker} ({count}/{total})...',
            'progress': count,
            'total':    total
        }
        await broadcast({'type': 'scan_status', 'status': current_scan_status})
        await asyncio.sleep(0.05)

        setup = process_ticker(stock_data)
        if setup:
            results.append(setup)
            await broadcast({'type': 'new_ticker', 'setup': setup})
            await asyncio.sleep(0)

            if (setup['grade'] in ['A+', 'A'] and
                    not setup['warning'] and
                    ticker not in alerted_tickers):
                alerted_tickers.add(ticker)
                log_alert(f"🚀 {setup['grade']}: {ticker} +{setup['gap_pct']}%")
                send_email(
                    subject=f"🚀 {setup['grade']} — {ticker} +{setup['gap_pct']}%",
                    body=(
                        f"Grade: {setup['grade']}\n"
                        f"Gap: +{setup['gap_pct']}%\n"
                        f"Price: ${setup['price']}\n"
                        f"Catalyst: {setup['catalyst']}\n\n"
                        f"Entry: ${setup['entry_low']}–${setup['entry_high']}\n"
                        f"Stop: ${setup['stop_loss']}\n"
                        f"T1: ${setup['target1']}\n"
                    )
                )

    results.sort(
        key=lambda x: (
            0 if x['warning'] else
            {'A+': 4, 'A': 3, 'B': 2}.get(x['grade'], 1)
        ),
        reverse=True
    )

    current_scan_status = {
        'phase': 'done',
        'message': f'✅ {len(results)} setup(s) found',
        'progress': total, 'total': total
    }

    return results

async def scanner_loop():
    global scan_results, scanner_running, current_scan_status

    log_alert("🔍 Scanner started")
    await broadcast({'type': 'status', 'running': True})

    while scanner_running:
        try:
            results      = await do_scan()
            scan_results = results

            await broadcast({
                'type':        'scan_results',
                'data':        results,
                'time':        datetime.now().strftime('%H:%M:%S'),
                'count':       len(results),
                'alerts':      alert_log[:20],
                'scan_status': current_scan_status
            })

        except Exception as e:
            log_alert(f"⚠️ Scanner error: {e}")

        if scanner_running:
            log_alert(f"⏱ Next scan in {SCAN_INTERVAL}s")
            await asyncio.sleep(SCAN_INTERVAL)

    log_alert("⏹️ Scanner stopped")
    current_scan_status = {
        'phase': 'idle', 'message': 'Scanner stopped',
        'progress': 0, 'total': 0
    }
    await broadcast({'type': 'status', 'running': False})

# ============================================================
# API ROUTES
# ============================================================

@app.get("/api/status")
async def get_status():
    return {
        'running': scanner_running,
        'results': len(scan_results),
        'time':    datetime.now().strftime('%H:%M:%S')
    }

@app.get("/api/results")
async def get_results():
    return {
        'data':   scan_results,
        'alerts': alert_log[:20],
        'time':   datetime.now().strftime('%H:%M:%S')
    }

@app.post("/api/scanner/start")
async def start_scanner():
    global scanner_running, scan_results
    if not scanner_running:
        scanner_running = True
        scan_results    = []
        asyncio.create_task(scanner_loop())
        log_alert("✅ Scanner started")
        return {'status': 'started'}
    return {'status': 'already running'}

@app.post("/api/scanner/stop")
async def stop_scanner():
    global scanner_running
    scanner_running = False
    log_alert("⏹️ Scanner stopped")
    return {'status': 'stopped'}

@app.post("/api/clear-alerts")
async def clear_alerts_route():
    alerted_tickers.clear()
    log_alert("🗑️ Alerts cleared")
    return {'status': 'cleared'}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    log_alert("📱 Client connected")
    await websocket.send_json({
        'type':        'scan_results',
        'data':        scan_results,
        'alerts':      alert_log[:20],
        'time':        datetime.now().strftime('%H:%M:%S'),
        'count':       len(scan_results),
        'scan_status': current_scan_status
    })
    await websocket.send_json({
        'type': 'status', 'running': scanner_running
    })
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)

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