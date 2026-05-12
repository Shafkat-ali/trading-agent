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
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', 'W7T9tMZzRCsHUhJfPvL7SZOReXow4q8L')
FINNHUB_KEY     = os.getenv('FINNHUB_API_KEY', 'd812vi9r01qler4gpnmgd812vi9r01qler4gpnn0')

scanner_running     = False
alerted_tickers     = set()
scan_results        = []
alert_log           = []
connected_clients   = []
current_scan_status = {
    'phase': 'idle', 'message': 'Scanner stopped',
    'progress': 0, 'total': 0
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
# HELPERS
# ============================================================

def send_email(subject, body):
    def _send():
        try:
            msg            = MIMEMultipart()
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
# DEBUG ENDPOINT
# ============================================================

@app.get("/api/debug")
async def debug():
    return {
        'finnhub_key_set':     bool(FINNHUB_KEY),
        'finnhub_key_length':  len(FINNHUB_KEY),
        'finnhub_key_preview': FINNHUB_KEY[:8] + '...' if FINNHUB_KEY else 'NOT SET',
        'polygon_key_set':     bool(POLYGON_API_KEY),
    }

# ============================================================
# FINNHUB ENDPOINTS
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
        if r.status_code == 200:
            return r.json()
        return {'error': f'Status {r.status_code}'}
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
# SEARCH TICKER ENDPOINT
# ============================================================

@app.get("/api/stock/search/{ticker}")
async def search_ticker(ticker: str):
    """
    Search for any ticker and return full analysis
    including Sykes pattern detection
    """
    try:
        ticker = ticker.upper().strip()

        # Get quote from Finnhub
        quote_r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={'symbol': ticker, 'token': FINNHUB_KEY},
            timeout=8
        )
        quote = quote_r.json() if quote_r.status_code == 200 else {}

        price      = quote.get('c', 0)
        prev_close = quote.get('pc', 0)
        high       = quote.get('h', 0)
        low        = quote.get('l', 0)
        open_price = quote.get('o', 0)

        if not price or price == 0:
            # Fallback to yfinance
            stock      = yf.Ticker(ticker)
            info       = stock.fast_info
            price      = info.get('last_price', 0)
            prev_close = info.get('previous_close', 0)

        if not price or price == 0:
            return {'error': f'Could not find ticker {ticker}'}

        pct_change = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0

        # Get historical data for pattern detection
        stock     = yf.Ticker(ticker)
        hist      = stock.history(period="10d", interval="1d")
        hist_vol  = stock.history(period="1d",  interval="1m")

        volume      = float(hist_vol['Volume'].sum()) if not hist_vol.empty else 0
        dollar_vol  = price * volume
        avg_vol     = float(hist['Volume'].mean()) if not hist.empty else 0
        vol_ratio   = volume / avg_vol if avg_vol > 0 else 0
        closes      = hist['Close'].values.tolist() if not hist.empty else []

        # Detect Sykes pattern
        pattern, pattern_desc, pattern_criteria = detect_sykes_pattern(
            ticker, price, prev_close, pct_change,
            closes, vol_ratio, dollar_vol
        )

        # Grade
        grade, notes = grade_setup(pct_change, dollar_vol)

        # Catalyst
        strength, news_count, headlines, warning = check_catalyst(ticker)

        final_grade = grade
        if warning:
            final_grade = "D"
        elif strength == 'STRONG' and grade == 'A':
            final_grade = "A+"
        elif strength == 'NONE':
            if grade == 'A+':  final_grade = 'A'
            elif grade == 'A': final_grade = 'B'

        if warning:
            catalyst_label = f"☠️ Danger — {news_count} article(s) in 5 days"
        elif strength == 'STRONG':
            catalyst_label = f"🔥 Strong Catalyst — {news_count} article(s) in 5 days"
        elif strength == 'MODERATE':
            catalyst_label = f"✅ Moderate Catalyst — {news_count} article(s) in 5 days"
        elif news_count > 0:
            catalyst_label = f"📰 {news_count} article(s) in past 5 days"
        else:
            catalyst_label = "📰 0 articles in past 5 days"

        entry_low  = round(price * 0.99, 2)
        entry_high = round(price * 1.02, 2)
        stop_loss  = round(entry_low * 0.95, 2)
        target1    = round(entry_high * 1.10, 2)
        target2    = round(entry_high * 1.20, 2)
        target3    = round(entry_high * 1.30, 2)

        return {
            'ticker':          ticker,
            'price':           round(price, 2),
            'prev_close':      round(prev_close, 2),
            'gap_pct':         round(pct_change, 1),
            'high':            round(high, 2),
            'low':             round(low, 2),
            'open':            round(open_price, 2),
            'dollar_vol':      round(dollar_vol, 0),
            'vol_ratio':       round(vol_ratio, 1),
            'grade':           final_grade,
            'notes':           notes,
            'catalyst':        catalyst_label,
            'news_count':      news_count,
            'headlines':       headlines[:2],
            'warning':         warning,
            'pattern':         pattern,
            'pattern_desc':    pattern_desc,
            'pattern_criteria': pattern_criteria,
            'entry_low':       entry_low,
            'entry_high':      entry_high,
            'stop_loss':       stop_loss,
            'target1':         target1,
            'target2':         target2,
            'target3':         target3,
            'source':          'Search',
            'time':            datetime.now().strftime('%H:%M:%S')
        }

    except Exception as e:
        return {'error': str(e)}

# ============================================================
# SYKES PATTERN DETECTION
# Based on Tim Sykes published strategy criteria
# ============================================================

def detect_sykes_pattern(ticker, price, prev_close, pct_change,
                          closes, vol_ratio, dollar_vol):
    """
    Detect which Sykes pattern this stock matches.
    Returns: (pattern_name, description, criteria_list)
    """

    pattern      = "No Clear Pattern"
    description  = "Does not match a defined Sykes setup"
    criteria     = []

    # ── SUPERNOVA ──
    # Criteria: Up 50%+ in one session, parabolic move, massive volume
    if pct_change >= 50:
        pattern     = "🚀 Supernova"
        description = ("Stock exploded 50%+ in one session — classic Sykes supernova. "
                       "These stocks experience massive volatility and liquidity. "
                       "Can be traded long on the way up or short on the first red day.")
        criteria = [
            f"✅ Up {pct_change:.1f}% today (need 50%+)",
            f"✅ Volume ratio {vol_ratio:.1f}x average" if vol_ratio >= 3 else f"⚠️ Volume ratio {vol_ratio:.1f}x (want 3x+)",
            "📌 Sykes Rule: Buy the momentum OR wait for first red day to short",
            "📌 Exit: Sell into strength, never hold a supernova too long",
            "⚠️ Risk: Supernovas can last longer than expected — use stop loss"
        ]
        return pattern, description, criteria

    # ── FIRST GREEN DAY ──
    # Criteria: Stock was red for 2+ days, now first green day with volume + catalyst
    if len(closes) >= 4:
        prev_days_red = all(closes[i] < closes[i-1] for i in range(-3, -1))
        if prev_days_red and pct_change > 5:
            pattern     = "🟢 First Green Day"
            description = ("After multiple red days, this is the first green candle with "
                           "volume. Sykes' favorite OTC pattern. Look for stocks closing "
                           "near highs — potential overnight hold for gap up next morning.")
            criteria = [
                f"✅ {pct_change:.1f}% green today after 2+ red days",
                f"✅ Volume ratio {vol_ratio:.1f}x" if vol_ratio >= 2 else f"⚠️ Volume {vol_ratio:.1f}x (want 2x+)",
                "📌 Sykes Rule: Buy dips on the run-up, not the spike",
                "📌 Entry: Wait for morning dip then buy bounce",
                "📌 If closes near HOD → consider overnight hold for gap up",
                "📌 Exit: Sell into next morning gap up"
            ]
            return pattern, description, criteria

    # ── GAP AND GO ──
    # Criteria: Gapped up 15%+ premarket on catalyst, continues higher
    if pct_change >= 15 and price > prev_close * 1.10:
        pattern     = "⚡ Gap and Go"
        description = ("Stock gapped up significantly on a catalyst and is continuing "
                       "higher. Sykes trades these when there is confirmed news and "
                       "volume is supporting the move.")
        criteria = [
            f"✅ Gapped up {pct_change:.1f}% from previous close",
            f"✅ Volume ratio {vol_ratio:.1f}x" if vol_ratio >= 3 else f"⚠️ Volume {vol_ratio:.1f}x (want 3x+)",
            "📌 Sykes Rule: Only trade with a strong catalyst — no catalyst = skip",
            "📌 Entry: Buy the first pullback after open, not the gap open price",
            "📌 Exit: Scale out in thirds — T1, T2, T3",
            "⚠️ Risk: Gap fills happen — always have stop loss ready"
        ]
        return pattern, description, criteria

    # ── BREAKOUT ──
    # Criteria: Breaking multi-day resistance with volume
    if len(closes) >= 5:
        recent_high = max(closes[-5:])
        if price >= recent_high * 0.98 and vol_ratio >= 2 and pct_change > 5:
            pattern     = "💥 Breakout"
            description = ("Stock is breaking above recent resistance with volume support. "
                           "Sykes likes multi-day breakouts with low float stocks where "
                           "volume can make a big difference.")
            criteria = [
                f"✅ Price near {len(closes)}-day high (${recent_high:.2f})",
                f"✅ Up {pct_change:.1f}% today",
                f"✅ Volume {vol_ratio:.1f}x average" if vol_ratio >= 2 else f"⚠️ Volume {vol_ratio:.1f}x (want 2x+)",
                "📌 Sykes Rule: Buy breakouts only with volume confirmation",
                "📌 Entry: Buy the breakout above resistance, set stop below it",
                "📌 Exit: Sell into the spike — take singles, don't get greedy"
            ]
            return pattern, description, criteria

    # ── DIP BUY / MORNING PANIC ──
    # Criteria: Former runner pulling back 20-50%, bouncing on support
    if len(closes) >= 5:
        recent_high = max(closes[-5:])
        dip_pct     = ((price - recent_high) / recent_high * 100) if recent_high > 0 else 0
        if -50 <= dip_pct <= -15 and pct_change >= 3:
            pattern     = "📈 Dip Buy / Morning Panic"
            description = ("Former runner pulled back hard and is showing a bounce. "
                           "Sykes loves buying morning panics on stocks that held support. "
                           "Psychology of the market creates oversold bounces.")
            criteria = [
                f"✅ Down {abs(dip_pct):.0f}% from recent high — oversold",
                f"✅ Bouncing {pct_change:.1f}% today",
                "📌 Sykes Rule: Look for maximum pain levels, dip buy at support",
                "📌 Entry: Buy when selling slows, not before — patience is key",
                "📌 Exit: Quick scalp — sell into the bounce, don't overstay",
                "⚠️ Risk: Dead cat bounces happen — tight stop loss required"
            ]
            return pattern, description, criteria

    # ── SUPERNOVA FADE ──
    # Criteria: Was supernova, now first red day — short opportunity
    if len(closes) >= 3 and pct_change < -10:
        prev_was_up = closes[-2] > closes[-3] * 1.3 if len(closes) >= 3 else False
        if prev_was_up:
            pattern     = "🔴 Supernova Fade (Short)"
            description = ("This was a supernova that is now fading. The first red day "
                           "after a big run is a short selling opportunity per Sykes. "
                           "Shorts squeeze out longs who are trapped bag holding.")
            criteria = [
                f"✅ Down {abs(pct_change):.1f}% today after big run",
                "✅ Previous session was up 30%+",
                "📌 Sykes Rule: Short the first red day of a former supernova",
                "📌 Entry: Short into bounces — sell the rips",
                "📌 Exit: Cover into panics — buy the dips to cover",
                "⚠️ Risk: Short squeezes can be brutal — keep position small"
            ]
            return pattern, description, criteria

    # ── WEAK PATTERN ──
    if pct_change >= 10:
        pattern     = "📊 Momentum Play"
        description = ("Stock is showing momentum but doesn't fit a classic Sykes pattern "
                       "perfectly. Trade with caution and look for additional confirmation.")
        criteria = [
            f"⚠️ Up {pct_change:.1f}% — decent move but no clear pattern",
            f"⚠️ Volume {vol_ratio:.1f}x average",
            "📌 Wait for a cleaner setup before entering",
            "📌 Look for catalyst to confirm the move",
            "📌 If pattern develops, reassess for entry"
        ]
        return pattern, description, criteria

    return pattern, description, criteria

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
            news_items = r.json()[:15]

        if not news_items:
            yf_news = yf.Ticker(ticker).news or []
            cutoff  = datetime.now() - timedelta(days=5)
            for n in yf_news[:10]:
                pub = n.get('providerPublishTime', 0)
                if pub and datetime.fromtimestamp(pub) >= cutoff:
                    news_items.append({
                        'headline': n.get('title', ''),
                        'source':   'yf'
                    })

        news_count  = len(news_items)
        headlines   = []
        strong_hits = 0
        danger_hits = 0
        warning     = False

        for item in news_items[:5]:
            title = (item.get('headline') or item.get('title') or '').lower()
            headlines.append(item.get('headline') or item.get('title') or '')
            if any(kw in title for kw in STRONG_KEYWORDS):
                strong_hits += 1
            if any(kw in title for kw in DANGER_KEYWORDS):
                danger_hits += 1
                warning = True

        if not news_items:
            return 'NONE', 0, [], False

        if warning and danger_hits > strong_hits:
            return 'DANGER', news_count, headlines[:2], True
        elif strong_hits >= 2:
            return 'STRONG', news_count, headlines[:2], False
        elif strong_hits == 1:
            return 'MODERATE', news_count, headlines[:2], False
        else:
            return 'WEAK', news_count, headlines[:2], False

    except:
        return 'NONE', 0, [], False

# ============================================================
# DATA SOURCES
# ============================================================

def get_massive_gainers():
    try:
        url = (
            f"https://api.polygon.io/v2/snapshot/locale/us/"
            f"markets/stocks/gainers"
            f"?apiKey={POLYGON_API_KEY}&include_otc=false"
        )
        r = requests.get(url, timeout=15)
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
                if not (price and prev_close and price > 0 and prev_close > 0):
                    continue
                pct        = ((price - prev_close) / prev_close) * 100
                dollar_vol = price * volume
                if (sym and MIN_PRICE <= price <= MAX_PRICE and
                        pct >= MIN_GAP_PCT and
                        dollar_vol >= MIN_DOLLAR_VOL):
                    candidates.append({
                        'ticker':     sym,
                        'price':      float(price),
                        'prev_close': float(prev_close),
                        'gap_pct':    round(float(pct), 1),
                        'volume':     float(volume),
                        'dollar_vol': float(dollar_vol),
                        'source':     'Live'
                    })
            log_alert(f"📡 Massive: {len(candidates)} stocks")
            return candidates
        else:
            log_alert(f"⚠️ Massive error: {r.status_code}")
            return []
    except Exception as e:
        log_alert(f"⚠️ Massive error: {e}")
        return []

def get_yahoo_gainers():
    candidates = []
    headers    = {'User-Agent': 'Mozilla/5.0'}
    for scrId in ['day_gainers', 'small_cap_gainers']:
        try:
            url = (
                f"https://query1.finance.yahoo.com/v1/finance/screener/"
                f"predefined/saved?formatted=false&scrIds={scrId}&count=50"
            )
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
                        pct >= MIN_GAP_PCT and
                        dollar_vol >= MIN_DOLLAR_VOL):
                    candidates.append({
                        'ticker':     sym,
                        'price':      float(price),
                        'prev_close': float(prev),
                        'gap_pct':    round(float(pct), 1),
                        'volume':     float(vol),
                        'dollar_vol': float(dollar_vol),
                        'source':     'Live'
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
    if gap_pct >= 50:
        grade = "A+"; notes.append(f"Supernova +{gap_pct:.1f}% 🔥🔥🔥")
    elif gap_pct >= 30:
        grade = "A+"; notes.append(f"Huge gap +{gap_pct:.1f}% 🔥🔥")
    elif gap_pct >= 20:
        grade = "A";  notes.append(f"Strong gap +{gap_pct:.1f}% 🔥")
    elif gap_pct >= 15:
        grade = "A";  notes.append(f"Good gap +{gap_pct:.1f}%")
    elif gap_pct >= 10:
        grade = "B";  notes.append(f"Moderate gap +{gap_pct:.1f}%")
    elif gap_pct > 0:
        grade = "C";  notes.append(f"Weak gap +{gap_pct:.1f}%")
    else:
        grade = "D";  notes.append(f"Down {gap_pct:.1f}%")

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
        strength, news_count, headlines, warning = check_catalyst(ticker)

        final_grade = grade
        if warning:
            final_grade = "D"
        elif strength == 'STRONG' and grade == 'A':
            final_grade = "A+"
        elif strength == 'NONE':
            if grade == 'A+':  final_grade = 'A'
            elif grade == 'A': final_grade = 'B'

        if warning:
            catalyst_label = f"☠️ Danger — {news_count} article(s) in 5 days"
        elif strength == 'STRONG':
            catalyst_label = f"🔥 Strong Catalyst — {news_count} article(s) in 5 days"
        elif strength == 'MODERATE':
            catalyst_label = f"✅ Moderate Catalyst — {news_count} article(s) in 5 days"
        elif news_count > 0:
            catalyst_label = f"📰 {news_count} article(s) in past 5 days"
        else:
            catalyst_label = "📰 0 articles in past 5 days"

        # Get historical data for pattern detection
        try:
            stock     = yf.Ticker(ticker)
            hist      = stock.history(period="10d", interval="1d")
            hist_1m   = stock.history(period="1d",  interval="1m")
            volume    = float(hist_1m['Volume'].sum()) if not hist_1m.empty else dollar_vol / price if price > 0 else 0
            avg_vol   = float(hist['Volume'].mean()) if not hist.empty else 0
            vol_ratio = volume / avg_vol if avg_vol > 0 else 0
            closes    = hist['Close'].values.tolist() if not hist.empty else []
        except:
            volume    = 0
            vol_ratio = 0
            closes    = []

        pattern, pattern_desc, pattern_criteria = detect_sykes_pattern(
            ticker, price, prev_close, gap_pct,
            closes, vol_ratio, dollar_vol
        )

        entry_low  = round(price * 0.99, 2)
        entry_high = round(price * 1.02, 2)
        stop_loss  = round(entry_low * 0.95, 2)
        target1    = round(entry_high * 1.10, 2)
        target2    = round(entry_high * 1.20, 2)
        target3    = round(entry_high * 1.30, 2)

        return {
            'ticker':           ticker,
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
                        f"Pattern: {setup['pattern']}\n"
                        f"Gap: +{setup['gap_pct']}%\n"
                        f"Price: ${setup['price']}\n"
                        f"News: {setup['news_count']} articles\n\n"
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