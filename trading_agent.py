from ib_insync import *
from alerts import *
from config import *
import yfinance as yf
import time
import winsound
import requests
from datetime import datetime

# ============================================================
#   SYKES METHOD TRADING AGENT v3
#   - Small Cap Scanner (Long Setups)
#   - Position Monitor (5% Stop Loss)
#   - News Catalyst Checker
#   - Email Alerts
#   - Pre/After Market Support (4am - 8pm EST)
# ============================================================

# Connect to IBKR
ib = IB()
ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT)

# Already alerted tickers (avoid duplicate emails)
alerted_tickers = set()

# ============================================================
# NEWS CATALYST CHECKER
# ============================================================

STRONG_KEYWORDS = [
    'fda', 'approval', 'approved', 'breakthrough',
    'contract', 'partnership', 'merger', 'acquisition',
    'earnings', 'beat', 'guidance', 'revenue',
    'clinical', 'trial', 'phase', 'results',
    'patent', 'exclusive', 'launch', 'deal',
    'awarded', 'wins', 'secures', 'signs'
]

DANGER_KEYWORDS = [
    'offering', 'dilut', 'shelf', 'warrant',
    'investigation', 'lawsuit', 'sec', 'subpoena',
    'delay', 'failed', 'withdrawn', 'suspended'
]

def check_catalyst(ticker):
    """Check news catalyst for ticker"""
    try:
        stock = yf.Ticker(ticker)
        news = stock.news

        if not news:
            return {
                'strength': 'NONE',
                'label': '❌ No Catalyst',
                'headlines': [],
                'warning': False
            }

        from datetime import timedelta
        cutoff = datetime.now() - timedelta(hours=48)
        recent_news = []

        for item in news[:10]:
            pub_time = item.get('providerPublishTime', 0)
            if pub_time:
                from datetime import datetime as dt
                pub_dt = dt.fromtimestamp(pub_time)
                if pub_dt >= cutoff:
                    recent_news.append(item)

        if not recent_news:
            return {
                'strength': 'NONE',
                'label': '❌ No Recent News (48hr)',
                'headlines': [],
                'warning': False
            }

        headlines = []
        strong_hits = 0
        danger_hits = 0
        warning = False

        for item in recent_news[:5]:
            title = item.get('title', '').lower()
            headlines.append(item.get('title', ''))

            for kw in STRONG_KEYWORDS:
                if kw in title:
                    strong_hits += 1
                    break

            for kw in DANGER_KEYWORDS:
                if kw in title:
                    danger_hits += 1
                    warning = True
                    break

        if warning and danger_hits > strong_hits:
            return {
                'strength': 'DANGER',
                'label': '☠️  DANGER: Dilution/Legal Risk',
                'headlines': headlines[:2],
                'warning': True
            }
        elif strong_hits >= 2:
            return {
                'strength': 'STRONG',
                'label': '🔥 STRONG Catalyst',
                'headlines': headlines[:2],
                'warning': False
            }
        elif strong_hits == 1:
            return {
                'strength': 'MODERATE',
                'label': '✅ Moderate Catalyst',
                'headlines': headlines[:2],
                'warning': False
            }
        else:
            return {
                'strength': 'WEAK',
                'label': '⚠️  Weak Catalyst',
                'headlines': headlines[:2],
                'warning': False
            }

    except Exception as e:
        return {
            'strength': 'UNKNOWN',
            'label': '❓ Could not fetch news',
            'headlines': [],
            'warning': False
        }

# ============================================================
# TOP GAINERS FETCHER
# ============================================================

def get_top_gainers():
    """Get top % gainers from Yahoo Finance"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        url = ("https://query1.finance.yahoo.com/v1/finance/screener/"
               "predefined/saved?formatted=false&scrIds=day_gainers&count=50")
        response = requests.get(url, headers=headers, timeout=10)

        gainers = []
        if response.status_code == 200:
            data = response.json()
            quotes = (data.get('finance', {})
                         .get('result', [{}])[0]
                         .get('quotes', []))

            for q in quotes:
                price = q.get('regularMarketPrice', 0)
                change_pct = q.get('regularMarketChangePercent', 0)
                volume = q.get('regularMarketVolume', 0)

                if (MIN_PRICE <= price <= MAX_PRICE and
                        change_pct >= MIN_PCT_CHANGE and
                        volume >= MIN_VOLUME):
                    gainers.append(q.get('symbol', ''))

        return gainers[:20]

    except Exception as e:
        print(f"  ⚠️  Screener error: {e}")
        return []

# ============================================================
# PATTERN ANALYZER
# ============================================================

def analyze_setup(ticker):
    """Analyze ticker using Sykes method patterns"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.fast_info
        hist = stock.history(period="10d", interval="1d")

        if hist.empty or len(hist) < 3:
            return None

        current_price = info.get('last_price', 0)
        prev_close = hist['Close'].iloc[-2] if len(hist) >= 2 else 0

        if current_price == 0 or prev_close == 0:
            return None

        pct_change = ((current_price - prev_close) / prev_close) * 100
        avg_volume = hist['Volume'].mean()
        volume_today = info.get('three_month_average_volume', avg_volume)
        volume_ratio = volume_today / avg_volume if avg_volume > 0 else 0
        week_high = info.get('fifty_two_week_high', 0)
        week_low = info.get('fifty_two_week_low', 0)
        closes = hist['Close'].values

        pattern = "No Clear Setup"
        grade = "C"
        notes = []

        # --- PATTERN 1: FIRST GREEN DAY ---
        if len(closes) >= 4:
            prev_days_red = all(
                closes[i] < closes[i-1] for i in range(-3, -1)
            )
            if prev_days_red and pct_change > 0:
                pattern = "🟢 FIRST GREEN DAY"
                grade = "A"
                notes.append("Was red 2+ days, now reversing")
                if volume_ratio >= 3:
                    grade = "A+"
                    notes.append("Huge volume confirmation ✅")

        # --- PATTERN 2: GAP AND GO ---
        if pct_change >= 15 and current_price > prev_close * 1.10:
            pattern = "🚀 GAP AND GO"
            grade = "A"
            notes.append(f"Gapped up {pct_change:.1f}% from prev close")
            if volume_ratio >= 5:
                grade = "A+"
                notes.append("Monster volume ✅")

        # --- PATTERN 3: BREAKOUT ---
        if week_high > 0:
            pct_from_high = ((current_price - week_high) / week_high) * 100
            if pct_from_high >= -3 and volume_ratio >= 2:
                pattern = "💥 BREAKOUT"
                grade = "A"
                notes.append("Near 52-week high within 3%")
                if volume_ratio >= 4:
                    grade = "A+"
                    notes.append("Volume confirming breakout ✅")

        # --- PATTERN 4: DIP AND RIP ---
        if len(closes) >= 5:
            recent_high = max(closes[-5:])
            dip_pct = ((current_price - recent_high) / recent_high) * 100
            if -20 <= dip_pct <= -8 and pct_change >= 5:
                pattern = "📈 DIP AND RIP"
                grade = "B"
                notes.append(
                    f"Dipped {dip_pct:.1f}% from recent high, bouncing"
                )
                if volume_ratio >= 3:
                    grade = "A"
                    notes.append("Volume on the bounce ✅")

        # Entry / Stop / Targets
        entry_low  = current_price * 0.99
        entry_high = current_price * 1.01
        stop_loss  = entry_low * 0.95
        target1    = entry_high * 1.10
        target2    = entry_high * 1.20
        target3    = entry_high * 1.30
        rr_ratio   = ((target1 - entry_high) /
                      (entry_high - stop_loss)
                      if (entry_high - stop_loss) > 0 else 0)

        if volume_ratio < 2:
            if grade == "A+": grade = "A"
            elif grade == "A": grade = "B"
            notes.append("⚠️ Volume weak")

        return {
            'ticker':     ticker,
            'price':      current_price,
            'pct_change': pct_change,
            'volume_ratio': volume_ratio,
            'pattern':    pattern,
            'grade':      grade,
            'notes':      notes,
            'entry_low':  entry_low,
            'entry_high': entry_high,
            'stop_loss':  stop_loss,
            'target1':    target1,
            'target2':    target2,
            'target3':    target3,
            'rr_ratio':   rr_ratio,
            'week_high':  week_high,
            'week_low':   week_low
        }

    except Exception as e:
        return None

# ============================================================
# PRINT SETUP
# ============================================================

def print_setup(setup, catalyst):
    """Print formatted setup with catalyst info"""
    if not setup:
        return

    grade = setup['grade']

    if catalyst['warning']:
        grade = "D"
    elif catalyst['strength'] == 'STRONG' and grade == 'A':
        grade = 'A+'

    if grade == "A+":
        star = "⭐⭐⭐"
    elif grade == "A":
        star = "⭐⭐"
    elif grade == "B":
        star = "⭐"
    else:
        star = "⛔"

    print(f"\n{'='*55}")
    print(f"  {star} {setup['ticker']} — Grade: {grade}  {star}")
    print(f"{'='*55}")
    print(f"  Pattern  : {setup['pattern']}")
    print(f"  Price    : ${setup['price']:.2f} "
          f"({setup['pct_change']:+.1f}%)")
    print(f"  Volume   : {setup['volume_ratio']:.1f}x average")
    print(f"  ---")
    print(f"  📰 Catalyst: {catalyst['label']}")
    for h in catalyst['headlines'][:2]:
        print(f"     → {h[:65]}...")
    print(f"  ---")
    print(f"  Entry    : ${setup['entry_low']:.2f} – "
          f"${setup['entry_high']:.2f}")
    print(f"  Stop Loss: ${setup['stop_loss']:.2f} (-5%)")
    print(f"  Target 1 : ${setup['target1']:.2f} (+10%)")
    print(f"  Target 2 : ${setup['target2']:.2f} (+20%)")
    print(f"  Target 3 : ${setup['target3']:.2f} (+30%)")
    print(f"  R/R Ratio: 1:{setup['rr_ratio']:.1f}")
    print(f"  ---")
    for note in setup['notes']:
        print(f"  📌 {note}")

    if catalyst['warning']:
        print(f"\n  ⛔ SKIP THIS TRADE — Dangerous catalyst!")

    print(f"{'='*55}")

    return grade

# ============================================================
# POSITION MONITOR
# ============================================================

def check_positions():
    """Monitor open positions for stop loss"""
    positions = ib.positions()

    if not positions:
        return

    print(f"\n{'─'*55}")
    print(f"  📊 OPEN POSITIONS — "
          f"{datetime.now().strftime('%H:%M:%S')}")
    print(f"{'─'*55}")

    for pos in positions:
        ticker   = pos.contract.symbol
        shares   = pos.position
        avg_cost = pos.avgCost

        try:
            stock = yf.Ticker(ticker)
            current_price = stock.fast_info['last_price']
        except:
            current_price = None

        if current_price and avg_cost > 0:
            pnl_pct   = ((current_price - avg_cost) / avg_cost) * 100
            pnl_dollar = (current_price - avg_cost) * shares
            status    = "✅" if pnl_pct >= 0 else "🔴"

            print(f"  {status} {ticker} | {shares} shares | "
                  f"Avg: ${avg_cost:.2f} | "
                  f"Now: ${current_price:.2f} | "
                  f"P&L: {pnl_pct:+.1f}% (${pnl_dollar:+.2f})")

            # Alerts
            if pnl_pct <= -STOP_LOSS_PCT:
                alert_stop_loss(
                    ticker, pnl_pct, pnl_dollar, current_price
                )
            elif pnl_pct <= -WARNING_PCT:
                alert_warning(ticker, pnl_pct, current_price)
            elif pnl_pct >= PROFIT_TARGET_PCT:
                alert_profit(
                    ticker, pnl_pct, pnl_dollar, current_price
                )

# ============================================================
# SCANNER
# ============================================================

def run_scanner():
    """Run the small cap scanner with news check"""
    print(f"\n{'─'*55}")
    print(f"  🔍 SCANNING — "
          f"{datetime.now().strftime('%H:%M:%S')}")
    print(f"{'─'*55}")

    tickers = get_top_gainers()

    if not tickers:
        print("  No gainers found. Market may not be open.")
        return

    print(f"  Found {len(tickers)} candidates. Analyzing...")

    setups = []
    for ticker in tickers:
        setup = analyze_setup(ticker)
        if setup and setup['grade'] in ['A+', 'A', 'B']:
            catalyst = check_catalyst(ticker)
            setups.append((setup, catalyst))

    # Sort — dangerous last, A+ first
    def sort_key(item):
        setup, catalyst = item
        if catalyst['warning']: return 10
        order = {'A+': 0, 'A': 1, 'B': 2, 'C': 3}
        return order.get(setup['grade'], 4)

    setups.sort(key=sort_key)

    if setups:
        print(f"\n  ✅ {len(setups)} setup(s) found:\n")
        for setup, catalyst in setups:
            grade = print_setup(setup, catalyst)

            # Email for A/A+ only, no duplicates
            ticker = setup['ticker']
            if (grade in ['A+', 'A'] and
                    not catalyst['warning'] and
                    ticker not in alerted_tickers):
                alerted_tickers.add(ticker)
                alert_aplus_setup(
                    ticker=ticker,
                    pattern=setup['pattern'],
                    grade=grade,
                    price=setup['price'],
                    pct_change=setup['pct_change'],
                    entry_low=setup['entry_low'],
                    entry_high=setup['entry_high'],
                    stop_loss=setup['stop_loss'],
                    target1=setup['target1'],
                    target2=setup['target2'],
                    target3=setup['target3'],
                    catalyst_label=catalyst['label'],
                    notes=setup['notes']
                )
    else:
        print("  No valid setups this scan.")

# ============================================================
# MAIN LOOP
# ============================================================

print("\n" + "="*55)
print("  🔥 SYKES METHOD TRADING AGENT v3 🔥")
print("="*55)
print(f"  Time     : {datetime.now().strftime('%I:%M %p')}")
print(f"  Hours    : 4:00 AM – 8:00 PM EST")
print(f"  Strategy : Long Setups (Sykes Method)")
print(f"  Stop Loss: -{STOP_LOSS_PCT}% hard rule")
print(f"  Scanning : Every {SCAN_INTERVAL} seconds")
print(f"  Positions: Every {POSITION_INTERVAL} seconds")
print(f"  Email    : Active ✅")
print("="*55)
print("\n  Press Ctrl+C to stop\n")

last_scan            = 0
last_position_check  = 0

try:
    while True:
        now          = time.time()
        current_hour = datetime.now().hour

        if 4 <= current_hour < 20:

            # Position check
            if now - last_position_check >= POSITION_INTERVAL:
                check_positions()
                last_position_check = now

            # Scanner
            if now - last_scan >= SCAN_INTERVAL:
                run_scanner()
                last_scan = now

        else:
            print(f"  💤 [{datetime.now().strftime('%H:%M:%S')}] "
                  f"Outside trading hours. Waiting...")

        time.sleep(5)

except KeyboardInterrupt:
    print("\n\n  Agent stopped. Good trading! 📈")
    ib.disconnect()