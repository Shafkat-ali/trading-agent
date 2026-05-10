from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
import yfinance as yf
import requests
import re
import smtplib
import sys
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# ============================================================
#   SYKES METHOD TRADING AGENT — WEB SERVER
#   Cloud compatible (no winsound)
# ============================================================

load_dotenv()

app = FastAPI(title="Sykes Trading Agent")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# SETTINGS FROM .ENV
# ============================================================

MIN_PRICE      = float(os.getenv('MIN_PRICE', 0.10))
MAX_PRICE      = float(os.getenv('MAX_PRICE', 50.00))
MIN_GAP_PCT    = float(os.getenv('MIN_GAP_PCT', 20.0))
MIN_DOLLAR_VOL = float(os.getenv('MIN_DOLLAR_VOL', 50000))
SCAN_INTERVAL  = int(os.getenv('SCAN_INTERVAL', 10))
STOP_LOSS_PCT  = float(os.getenv('STOP_LOSS_PCT', 5.0))
WARNING_PCT    = float(os.getenv('WARNING_PCT', 3.0))
PROFIT_PCT     = float(os.getenv('PROFIT_TARGET_PCT', 10.0))
EMAIL_ADDRESS  = os.getenv('EMAIL_ADDRESS', '')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '')
EMAIL_TO       = os.getenv('EMAIL_TO', '')

# ============================================================
# STATE
# ============================================================

scanner_running   = False
alerted_tickers   = set()
scan_results      = []
position_data     = []
alert_log         = []
connected_clients = []

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

# ============================================================
# EMAIL ALERT
# ============================================================

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

# ============================================================
# ALERT LOG
# ============================================================

def log_alert(message):
    entry = {
        'time':    datetime.now().strftime('%H:%M:%S'),
        'message': message
    }
    alert_log.insert(0, entry)
    if len(alert_log) > 100:
        alert_log.pop()

# ============================================================
# BROADCAST TO ALL WEBSOCKET CLIENTS
# ============================================================

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

# ============================================================
# NEWS CATALYST
# ============================================================

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

# ============================================================
# GET CANDIDATE TICKERS
# ============================================================

def get_candidate_tickers():
    tickers = set()
    headers = {'User-Agent': 'Mozilla/5.0'}

    for scrId in ['day_gainers', 'small_cap_gainers', 'most_actives']:
        try:
            url = (
                f"https://query1.finance.yahoo.com/v1/finance/screener/"
                f"predefined/saved?formatted=false"
                f"&scrIds={scrId}&count=100"
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
                dollar_vol = price * vol
                if (sym and
                        MIN_PRICE <= price <= MAX_PRICE and
                        pct >= MIN_GAP_PCT and
                        dollar_vol >= MIN_DOLLAR_VOL):
                    tickers.add(sym)
        except:
            pass

    try:
        url = ("https://finviz.com/screener.ashx?v=111&f="
               "geo_usa,price_u50,price_o0.1,change_o20"
               "&ft=4&o=-change")
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            found = re.findall(r'quote\.ashx\?t=([A-Z]+)&', r.text)
            for t in found[:50]:
                tickers.add(t)
    except:
        pass

    return tickers

# ============================================================
# GET REAL PRICE
# ============================================================

def get_real_price(ticker):
    try:
        stock      = yf.Ticker(ticker)
        hist       = stock.history(
            period="1d", interval="1m", prepost=True
        )
        if hist.empty:
            return None, None, None, None

        price      = float(hist.iloc[-1]['Close'])
        volume     = float(hist['Volume'].sum())
        hist_daily = stock.history(period="5d", interval="1d")

        if hist_daily.empty or len(hist_daily) < 2:
            return None, None, None, None

        prev_close = float(hist_daily['Close'].iloc[-2])
        if prev_close == 0:
            return None, None, None, None

        pct_change = ((price - prev_close) / prev_close) * 100
        return price, pct_change, volume, prev_close
    except:
        return None, None, None, None

# ============================================================
# GRADE SETUP
# ============================================================

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

# ============================================================
# MAIN SCANNER LOOP
# ============================================================

async def scanner_loop():
    global scan_results, scanner_running

    log_alert("🔍 Scanner started")
    await broadcast({'type': 'status', 'running': True})

    while scanner_running:
        try:
            log_alert(
                f"🔍 Scanning — "
                f"{datetime.now().strftime('%H:%M:%S')}"
            )

            tickers = get_candidate_tickers()
            results = []

            for ticker in tickers:
                if not scanner_running:
                    break

                try:
                    price, gap_pct, volume, prev_close = \
                        get_real_price(ticker)

                    if not all([price, gap_pct, volume, prev_close]):
                        continue

                    dollar_vol = price * volume

                    if not (MIN_PRICE <= price <= MAX_PRICE and
                            gap_pct >= MIN_GAP_PCT and
                            dollar_vol >= MIN_DOLLAR_VOL):
                        continue

                    grade, notes = grade_setup(gap_pct, dollar_vol)
                    strength, label, headlines, warning = \
                        check_catalyst(ticker)

                    # Adjust grade
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

                    setup = {
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

                    results.append(setup)

                    # Email for new A/A+ setups
                    if (final_grade in ['A+', 'A'] and
                            not warning and
                            ticker not in alerted_tickers):
                        alerted_tickers.add(ticker)
                        log_alert(
                            f"🚀 {final_grade} SETUP: {ticker} "
                            f"+{gap_pct:.1f}%"
                        )
                        send_email(
                            subject=(
                                f"🚀 {final_grade} SETUP — "
                                f"{ticker} +{gap_pct:.1f}%"
                            ),
                            body=(
                                f"Grade: {final_grade}\n"
                                f"Gap: +{gap_pct:.1f}%\n"
                                f"Price: ${price:.2f}\n"
                                f"Catalyst: {label}\n\n"
                                f"Entry: ${entry_low}–${entry_high}\n"
                                f"Stop: ${stop_loss}\n"
                                f"Target 1: ${target1}\n"
                            )
                        )
                except:
                    continue

            # Sort — A+ first, dangerous last
            results.sort(
                key=lambda x: (
                    0 if x['warning'] else
                    {'A+': 4, 'A': 3, 'B': 2}.get(x['grade'], 1)
                ),
                reverse=True
            )

            scan_results = results

            await broadcast({
                'type':   'scan_results',
                'data':   results,
                'time':   datetime.now().strftime('%H:%M:%S'),
                'count':  len(results),
                'alerts': alert_log[:20]
            })

        except Exception as e:
            log_alert(f"⚠️ Scanner error: {e}")

        await asyncio.sleep(SCAN_INTERVAL)

    log_alert("⏹️ Scanner stopped")
    await broadcast({'type': 'status', 'running': False})

# ============================================================
# API ROUTES
# ============================================================

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
    global scanner_running
    if not scanner_running:
        scanner_running = True
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

# ============================================================
# WEBSOCKET
# ============================================================

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

# ============================================================
# SERVE DASHBOARD
# ============================================================

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