from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import yfinance as yf
import requests
import re
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
SCAN_INTERVAL   = int(os.getenv('SCAN_INTERVAL', 10))
STOP_LOSS_PCT   = float(os.getenv('STOP_LOSS_PCT', 5.0))
EMAIL_ADDRESS   = os.getenv('EMAIL_ADDRESS', '')
EMAIL_PASSWORD  = os.getenv('EMAIL_PASSWORD', '')
EMAIL_TO        = os.getenv('EMAIL_TO', '')
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', '')

scanner_running   = False
alerted_tickers   = set()
scan_results      = []
alert_log         = []
connected_clients = []
scan_status       = {
    'phase':    'idle',
    'message':  'Scanner stopped',
    'progress': 0,
    'total':    0
}

STRONG_KEYWORDS = [
    'fda', 'approval', 'approved', 'breakthrough',
    'contract', 'partnership', 'merger', 'acquisition',
    'earnings', 'beat', 'guidance', 'revenue',
    'clinical', 'trial', 'phase', 'results',
    'patent', 'exclusive', 'launch', 'deal',
    'awarded', 'wins', 'secures', 'signs',
    'robot', 'ai', 'technology', 'crypto', 'bitcoin'
]

DANGER_KEYWORDS = [
    'offering', 'dilut', 'shelf', 'warrant',
    'investigation', 'lawsuit', 'sec', 'subpoena',
    'delay', 'failed', 'withdrawn', 'suspended',
    'reverse split', 'compliance'
]

def send_email(subject, body):
    def _send():
        try:
            msg            = MIMEMultipart()
            msg['From']    = EMAIL_ADDRESS
            msg['To']      = EMAIL_TO
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, EMAIL_TO, msg.as_string())
            server.quit()
            log_alert(f"📧 Email sent: {subject}")
        except Exception as e:
            log_alert(f"⚠️ Email failed: {e}")
    threading.Thread(target=_send, daemon=True).start()

def log_alert(message):
    entry = {
        'time':    datetime.now().strftime('%H:%M:%S'),
        'message': message
    }
    alert_log.insert(0, entry)
    if len(alert_log) > 100:
        alert_log.pop()

async def broadcast(data):
    disconnected = []
    for client in connected_clients:
        try:
            await client.send_json(data)
        except:
            disconnected.append(client)
    for c in disconnected:
        if c in connected_clients:
            connected_clients.remove(c)

def check_catalyst(ticker):
    try:
        stock = yf.Ticker(ticker)
        news  = stock.news
        if not news:
            return 'NONE', '❌ No Catalyst', [], False

        cutoff = datetime.now() - timedelta(hours=48)
        recent = []
        for item in news[:10]:
            pub = item.get('providerPublishTime', 0)
            if pub and datetime.fromtimestamp(pub) >= cutoff:
                recent.append(item)

        if not recent:
            return 'NONE', '❌ No Recent News', [], False

        headlines   = []
        strong_hits = 0
        danger_hits = 0
        warning     = False

        for item in recent[:5]:
            title = item.get('title', '').lower()
            headlines.append(item.get('title', ''))
            if any(kw in title for kw in STRONG_KEYWORDS):
                strong_hits += 1
            if any(kw in title for kw in DANGER_KEYWORDS):
                danger_hits += 1
                warning = True

        if warning and danger_hits > strong_hits:
            return 'DANGER', '☠️ DANGER: Dilution/Legal Risk', headlines[:2], True
        elif strong_hits >= 2:
            return 'STRONG', '🔥 STRONG Catalyst', headlines[:2], False
        elif strong_hits == 1:
            return 'MODERATE', '✅ Moderate Catalyst', headlines[:2], False
        else:
            return 'WEAK', '⚠️ Weak Catalyst', headlines[:2], False
    except:
        return 'UNKNOWN', '❓ Could not fetch news', [], False

def get_polygon_price(ticker):
    """
    Get real-time price from Polygon.io
    Works pre-market, after-hours, and regular hours
    """
    try:
        # Try snapshot first (real-time)
        url = (f"https://api.polygon.io/v2/snapshot/locale/us/"
               f"markets/stocks/tickers/{ticker}"
               f"?apiKey={POLYGON_API_KEY}")
        r = requests.get(url, timeout=10)

        if r.status_code == 200:
            data   = r.json()
            ticker_data = data.get('ticker', {})

            # Get pre-market or regular price
            day    = ticker_data.get('day', {})
            prev   = ticker_data.get('prevDay', {})
            last   = ticker_data.get('lastTrade', {})
            min_data = ticker_data.get('min', {})

            # Current price — use last trade
            price  = (last.get('p') or
                      min_data.get('c') or
                      day.get('c', 0))

            # Previous close
            prev_close = prev.get('c', 0)

            # Volume
            volume = day.get('v', 0)

            if price and prev_close and price > 0 and prev_close > 0:
                pct_change = ((price - prev_close) / prev_close) * 100
                return float(price), float(pct_change), float(volume), float(prev_close)

        return None, None, None, None

    except Exception as e:
        return None, None, None, None

def get_polygon_gainers():
    """
    Get top gainers from Polygon.io snapshot
    Works pre-market and after-hours
    """
    try:
        url = (f"https://api.polygon.io/v2/snapshot/locale/us/"
               f"markets/stocks/gainers"
               f"?apiKey={POLYGON_API_KEY}&include_otc=false")
        r = requests.get(url, timeout=15)

        tickers = []
        if r.status_code == 200:
            data    = r.json()
            results = data.get('tickers', [])

            for t in results:
                day    = t.get('day', {})
                prev   = t.get('prevDay', {})
                last   = t.get('lastTrade', {})

                price      = (last.get('p') or day.get('c', 0))
                prev_close = prev.get('c', 0)
                volume     = day.get('v', 0)

                if price and prev_close and price > 0 and prev_close > 0:
                    pct = ((price - prev_close) / prev_close) * 100
                    dollar_vol = price * volume

                    if (MIN_PRICE <= price <= MAX_PRICE and
                            pct >= MIN_GAP_PCT and
                            dollar_vol >= MIN_DOLLAR_VOL):
                        tickers.append({
                            'ticker':     t.get('ticker', ''),
                            'price':      float(price),
                            'prev_close': float(prev_close),
                            'gap_pct':    round(float(pct), 1),
                            'volume':     float(volume),
                            'dollar_vol': float(dollar_vol)
                        })

        return tickers

    except Exception as e:
        log_alert(f"⚠️ Polygon gainers error: {e}")
        return []

def get_yahoo_gainers():
    """Fallback — Yahoo Finance gainers"""
    tickers = []
    headers = {'User-Agent': 'Mozilla/5.0'}

    for scrId in ['day_gainers', 'small_cap_gainers']:
        try:
            url = (
                f"https://query1.finance.yahoo.com/v1/finance/screener/"
                f"predefined/saved?formatted=false"
                f"&scrIds={scrId}&count=50"
            )
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200:
                continue
            quotes = (r.json()
                       .get('finance', {})
                       .get('result', [{}])[0]
                       .get('quotes', []))
            for q in quotes:
                sym       = q.get('symbol', '')
                price     = q.get('regularMarketPrice', 0)
                pct       = q.get('regularMarketChangePercent', 0)
                vol       = q.get('regularMarketVolume', 0)
                prev      = q.get('regularMarketPreviousClose', 0)
                dollar_vol = price * vol

                if (sym and MIN_PRICE <= price <= MAX_PRICE and
                        pct >= MIN_GAP_PCT and
                        dollar_vol >= MIN_DOLLAR_VOL):
                    tickers.append({
                        'ticker':     sym,
                        'price':      float(price),
                        'prev_close': float(prev),
                        'gap_pct':    round(float(pct), 1),
                        'volume':     float(vol),
                        'dollar_vol': float(dollar_vol)
                    })
        except:
            pass

    return tickers

def grade_setup(gap_pct, dollar_vol):
    grade = "B"
    notes = []

    if gap_pct >= 50:
        grade = "A+"
        notes.append(f"Massive gap +{gap_pct:.1f}% 🔥🔥🔥")
    elif gap_pct >= 30:
        grade = "A+"
        notes.append(f"Huge gap +{gap_pct:.1f}% 🔥🔥")
    elif gap_pct >= 20:
        grade = "A"
        notes.append(f"Strong gap +{gap_pct:.1f}% 🔥")
    elif gap_pct >= 15:
        grade = "A"
        notes.append(f"Good gap +{gap_pct:.1f}%")
    elif gap_pct >= 10:
        grade = "B"
        notes.append(f"Moderate gap +{gap_pct:.1f}%")

    if dollar_vol >= 5_000_000:
        notes.append(f"Monster $vol ${dollar_vol/1e6:.1f}M 🔥")
        if grade == "A":
            grade = "A+"
    elif dollar_vol >= 1_000_000:
        notes.append(f"Strong $vol ${dollar_vol/1e6:.1f}M ✅")
    else:
        notes.append(f"$Vol ${dollar_vol:,.0f}")

    return grade, notes

def process_ticker(stock_data):
    """Process a single ticker and return setup or None"""
    try:
        ticker     = stock_data['ticker']
        price      = stock_data['price']
        prev_close = stock_data['prev_close']
        gap_pct    = stock_data['gap_pct']
        volume     = stock_data['volume']
        dollar_vol = stock_data['dollar_vol']

        if not ticker:
            return None

        grade, notes = grade_setup(gap_pct, dollar_vol)
        strength, label, headlines, warning = check_catalyst(ticker)

        final_grade = grade
        if warning:
            final_grade = "D"
        elif strength == 'STRONG' and grade == 'A':
            final_grade = 'A+'
        elif strength == 'NONE':
            if grade == 'A+': final_grade = 'A'
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
            'headlines':  headlines[:2],
            'warning':    warning,
            'entry_low':  entry_low,
            'entry_high': entry_high,
            'stop_loss':  stop_loss,
            'target1':    target1,
            'target2':    target2,
            'target3':    target3,
            'time':       datetime.now().strftime('%H:%M:%S')
        }
    except:
        return None

async def scanner_loop():
    global scan_results, scanner_running, scan_status

    log_alert("🔍 Scanner started")
    await broadcast({'type': 'status', 'running': True})

    while scanner_running:
        try:
            # Phase 1 — Fetching
            scan_status = {
                'phase':    'fetching',
                'message':  'Fetching gainers from Polygon.io...',
                'progress': 0,
                'total':    0
            }
            await broadcast({'type': 'scan_status', 'status': scan_status})
            log_alert(f"🔍 Scanning — {datetime.now().strftime('%H:%M:%S')}")

            # Get gainers from Polygon first, fallback to Yahoo
            candidates = get_polygon_gainers()

            if not candidates:
                log_alert("⚠️ Polygon returned no results, trying Yahoo...")
                scan_status['message'] = 'Trying Yahoo Finance backup...'
                await broadcast({'type': 'scan_status', 'status': scan_status})
                candidates = get_yahoo_gainers()

            total = len(candidates)
            log_alert(f"📊 Found {total} candidates to analyze")

            # Phase 2 — Analyzing
            scan_status = {
                'phase':    'analyzing',
                'message':  f'Analyzing {total} candidates...',
                'progress': 0,
                'total':    total
            }
            await broadcast({'type': 'scan_status', 'status': scan_status})

            results = []
            count   = 0

            for stock_data in candidates:
                if not scanner_running:
                    break

                count  += 1
                ticker  = stock_data.get('ticker', '')

                scan_status = {
                    'phase':    'analyzing',
                    'message':  f'Analyzing {ticker} ({count}/{total})',
                    'progress': count,
                    'total':    total
                }
                await broadcast({
                    'type':   'scan_status',
                    'status': scan_status
                })

                setup = process_ticker(stock_data)

                if setup:
                    results.append(setup)

                    # Broadcast each result immediately as found!
                    await broadcast({
                        'type':  'new_ticker',
                        'setup': setup
                    })

                    # Email alert for A/A+
                    if (setup['grade'] in ['A+', 'A'] and
                            not setup['warning'] and
                            ticker not in alerted_tickers):
                        alerted_tickers.add(ticker)
                        log_alert(
                            f"🚀 {setup['grade']} SETUP: "
                            f"{ticker} +{setup['gap_pct']}%"
                        )
                        send_email(
                            subject=(
                                f"🚀 {setup['grade']} SETUP — "
                                f"{ticker} +{setup['gap_pct']}%"
                            ),
                            body=(
                                f"Grade: {setup['grade']}\n"
                                f"Gap: +{setup['gap_pct']}%\n"
                                f"Price: ${setup['price']}\n"
                                f"Catalyst: {setup['catalyst']}\n\n"
                                f"Entry: ${setup['entry_low']}–"
                                f"${setup['entry_high']}\n"
                                f"Stop: ${setup['stop_loss']}\n"
                                f"Target 1: ${setup['target1']}\n"
                            )
                        )

            # Sort final results
            results.sort(
                key=lambda x: (
                    0 if x['warning'] else
                    {'A+': 4, 'A': 3, 'B': 2}.get(x['grade'], 1)
                ),
                reverse=True
            )

            scan_results = results
            scan_status  = {
                'phase':    'done',
                'message':  f'✅ Found {len(results)} setup(s)',
                'progress': total,
                'total':    total
            }

            await broadcast({
                'type':        'scan_results',
                'data':        results,
                'time':        datetime.now().strftime('%H:%M:%S'),
                'count':       len(results),
                'alerts':      alert_log[:20],
                'scan_status': scan_status
            })

        except Exception as e:
            log_alert(f"⚠️ Scanner error: {e}")

        await asyncio.sleep(SCAN_INTERVAL)

    log_alert("⏹️ Scanner stopped")
    await broadcast({'type': 'status', 'running': False})

@app.get("/api/status")
async def get_status():
    return {
        'running': scanner_running,
        'results': len(scan_results),
        'alerts':  len(alert_log),
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
        log_alert("✅ Scanner started by user")
        return {'status': 'started'}
    return {'status': 'already running'}

@app.post("/api/scanner/stop")
async def stop_scanner():
    global scanner_running
    scanner_running = False
    log_alert("⏹️ Scanner stopped by user")
    return {'status': 'stopped'}

@app.post("/api/clear-alerts")
async def clear_alerts():
    alerted_tickers.clear()
    log_alert("🗑️ Alerted tickers cleared")
    return {'status': 'cleared'}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    log_alert("📱 New client connected")

    await websocket.send_json({
        'type':   'scan_results',
        'data':   scan_results,
        'alerts': alert_log[:20],
        'time':   datetime.now().strftime('%H:%M:%S'),
        'count':  len(scan_results)
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