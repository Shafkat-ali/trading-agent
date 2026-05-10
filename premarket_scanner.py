from alerts import *
from config import *
import yfinance as yf
import time
import winsound
import requests
import pytz
import re
import threading
from datetime import datetime, timedelta
from tkinter import messagebox
import tkinter as tk

# ============================================================
#   SYKES METHOD - PRE-MARKET GAP SCANNER v5
#   Runs: 4:00 AM – 9:30 AM EST
#   STT Criteria:
#   Price $0.10-$50, % Change >= 20%,
#   $Volume >= $50,000
#   FIXED: Uses real pre-market prices
# ============================================================

alerted_tickers = set()
EST = pytz.timezone('US/Eastern')

MIN_PRICE      = 0.10
MAX_PRICE      = 50.00
MIN_GAP_PCT    = 20.0
MIN_DOLLAR_VOL = 50_000

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
# POPUP ALERT
# ============================================================

def show_popup(ticker, grade, gap_pct, price, catalyst_label,
               entry_low, entry_high, stop_loss, target1):
    def _popup():
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        message = (
            f"{'⭐'*3 if grade == 'A+' else '⭐⭐'}\n\n"
            f"TICKER:    {ticker}\n"
            f"GRADE:     {grade}\n"
            f"GAP UP:    +{gap_pct:.1f}%\n"
            f"PRICE:     ${price:.2f}\n"
            f"CATALYST:  {catalyst_label}\n\n"
            f"──────────────────\n"
            f"ENTRY:     ${entry_low:.2f} – ${entry_high:.2f}\n"
            f"STOP LOSS: ${stop_loss:.2f} (-5%)\n"
            f"TARGET 1:  ${target1:.2f} (+10%)\n"
            f"──────────────────\n\n"
            f"⚠️ You execute — stay disciplined!"
        )
        messagebox.showinfo(
            f"🚀 PRE-MARKET ALERT — {ticker} Grade: {grade}",
            message
        )
        root.destroy()
    thread = threading.Thread(target=_popup)
    thread.daemon = True
    thread.start()

# ============================================================
# BEEP
# ============================================================

def beep_alert(grade):
    if grade == "A+":
        for _ in range(3):
            winsound.Beep(1200, 500)
            time.sleep(0.1)
    elif grade == "A":
        for _ in range(2):
            winsound.Beep(1000, 400)
            time.sleep(0.1)
    else:
        winsound.Beep(800, 300)

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
            return 'DANGER', '☠️  DANGER: Dilution/Legal Risk', headlines[:2], True
        elif strong_hits >= 2:
            return 'STRONG', '🔥 STRONG Catalyst', headlines[:2], False
        elif strong_hits == 1:
            return 'MODERATE', '✅ Moderate Catalyst', headlines[:2], False
        else:
            return 'WEAK', '⚠️  Weak Catalyst', headlines[:2], False
    except:
        return 'UNKNOWN', '❓ Could not fetch news', [], False

# ============================================================
# GET REAL PRE-MARKET PRICE FOR A TICKER
# ============================================================

def get_premarket_price(ticker):
    """
    Get real pre-market price using yfinance 1m data.
    Returns (price, pct_change, volume) or None
    """
    try:
        stock = yf.Ticker(ticker)

        # Get today's 1-minute pre-market data
        hist = stock.history(
            period="1d",
            interval="1m",
            prepost=True  # Include pre/after market
        )

        if hist.empty:
            return None, None, None

        # Get the most recent price (pre-market)
        latest       = hist.iloc[-1]
        current_price = float(latest['Close'])
        volume        = float(hist['Volume'].sum())

        # Get previous close
        hist_daily = stock.history(period="5d", interval="1d")
        if hist_daily.empty or len(hist_daily) < 2:
            return None, None, None

        prev_close = float(hist_daily['Close'].iloc[-2])

        if prev_close == 0:
            return None, None, None

        pct_change = ((current_price - prev_close) / prev_close) * 100

        return current_price, pct_change, volume, prev_close

    except Exception as e:
        return None, None, None, None

# ============================================================
# FETCH MOVERS FROM SCREENERS
# ============================================================

def get_candidate_tickers():
    """
    Get candidate tickers from Yahoo Finance screeners + Finviz.
    Returns set of ticker symbols.
    """
    tickers = set()
    headers = {'User-Agent': 'Mozilla/5.0'}

    # Yahoo Finance screeners
    screener_ids = [
        'day_gainers',
        'small_cap_gainers',
        'most_actives'
    ]

    for scrId in screener_ids:
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
                sym   = q.get('symbol', '')
                price = q.get('regularMarketPrice', 0)
                if sym and MIN_PRICE <= price <= MAX_PRICE:
                    tickers.add(sym)

        except Exception as e:
            print(f"  ⚠️  {scrId} error: {e}")

    # Finviz screener
    try:
        url = ("https://finviz.com/screener.ashx?v=111&f="
               "geo_usa,price_u50,price_o0.1,"
               "change_o20&ft=4&o=-change")
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            found = re.findall(
                r'quote\.ashx\?t=([A-Z]+)&', r.text
            )
            for t in found[:50]:
                tickers.add(t)
    except Exception as e:
        print(f"  ⚠️  Finviz error: {e}")

    return tickers

# ============================================================
# GRADE THE SETUP
# ============================================================

def grade_setup(gap_pct, dollar_vol):
    grade = "B"
    notes = []

    # Grade by gap size
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

    # Dollar volume bonus
    if dollar_vol >= 5_000_000:
        notes.append(f"Monster $volume ${dollar_vol/1_000_000:.1f}M 🔥")
        if grade == "A":
            grade = "A+"
    elif dollar_vol >= 1_000_000:
        notes.append(f"Strong $volume ${dollar_vol/1_000_000:.1f}M ✅")
    elif dollar_vol >= 50_000:
        notes.append(f"$Volume ${dollar_vol:,.0f}")

    return grade, notes

# ============================================================
# PRINT SETUP
# ============================================================

def print_setup(ticker, name, price, prev_close, gap_pct,
                dollar_vol, grade, notes, catalyst_strength,
                catalyst_label, headlines, warning):

    # Adjust grade based on catalyst
    if warning:
        grade = "D"
    elif catalyst_strength == 'STRONG' and grade == 'A':
        grade = 'A+'
    elif catalyst_strength == 'NONE':
        if grade == 'A+': grade = 'A'
        elif grade == 'A': grade = 'B'

    star = {"A+": "⭐⭐⭐", "A": "⭐⭐",
            "B": "⭐"}.get(grade, "⛔")

    entry_low  = price * 0.99
    entry_high = price * 1.02
    stop_loss  = entry_low * 0.95
    target1    = entry_high * 1.10
    target2    = entry_high * 1.20
    target3    = entry_high * 1.30
    rr         = ((target1 - entry_high) /
                  (entry_high - stop_loss)
                  if (entry_high - stop_loss) > 0 else 0)

    print(f"\n{'='*55}")
    print(f"  {star} {ticker} — {name[:30]}")
    print(f"  Grade: {grade}")
    print(f"{'='*55}")
    print(f"  Gap Up    : +{gap_pct:.1f}%")
    print(f"  Price NOW : ${price:.2f}  "
          f"(prev close: ${prev_close:.2f})")
    print(f"  $Volume   : ${dollar_vol:,.0f}")
    print(f"  ---")
    print(f"  📰 Catalyst: {catalyst_label}")
    for h in headlines[:2]:
        print(f"     → {h[:65]}...")
    print(f"  ---")
    print(f"  Entry     : ${entry_low:.2f} – ${entry_high:.2f}")
    print(f"  Stop Loss : ${stop_loss:.2f} (-5%)")
    print(f"  Target 1  : ${target1:.2f} (+10%)")
    print(f"  Target 2  : ${target2:.2f} (+20%)")
    print(f"  Target 3  : ${target3:.2f} (+30%)")
    print(f"  R/R Ratio : 1:{rr:.1f}")
    print(f"  ---")
    for note in notes:
        print(f"  📌 {note}")

    if warning:
        print(f"\n  ⛔ SKIP — Dangerous catalyst!")
    elif grade in ['A+', 'A']:
        print(f"\n  ✅ STRONG SETUP — Watch at open!")

    print(f"{'='*55}")

    return grade, entry_low, entry_high, stop_loss, target1

# ============================================================
# MAIN SCAN
# ============================================================

def run_premarket_scan():
    print(f"\n{'─'*55}")
    print(f"  🔍 PRE-MARKET SCAN — "
          f"{datetime.now().strftime('%H:%M:%S EST')}")
    print(f"{'─'*55}")

    # Step 1 — get candidate tickers
    candidate_tickers = get_candidate_tickers()
    print(f"  Found {len(candidate_tickers)} candidates. "
          f"Fetching real pre-market prices...")

    # Step 2 — get REAL pre-market price for each
    qualified = []

    for ticker in candidate_tickers:
        try:
            result = get_premarket_price(ticker)
            if len(result) < 4:
                continue

            price, gap_pct, volume, prev_close = result

            if not price or not gap_pct or not prev_close:
                continue

            dollar_vol = price * volume if volume else 0

            # Apply STT filters with REAL pre-market data
            if (MIN_PRICE <= price <= MAX_PRICE and
                    gap_pct >= MIN_GAP_PCT and
                    dollar_vol >= MIN_DOLLAR_VOL):

                qualified.append({
                    'ticker':     ticker,
                    'name':       ticker,
                    'price':      price,
                    'prev_close': prev_close,
                    'gap_pct':    gap_pct,
                    'volume':     volume,
                    'dollar_vol': dollar_vol
                })

        except Exception as e:
            continue

    if not qualified:
        print("  No qualifying pre-market gaps found this scan.")
        return

    # Sort by gap % descending
    qualified.sort(key=lambda x: x['gap_pct'], reverse=True)

    print(f"\n  🚀 {len(qualified)} qualifying gap(s) found!\n")

    # Step 3 — analyze and print each
    for stock in qualified:
        ticker    = stock['ticker']
        price     = stock['price']
        prev_close = stock['prev_close']
        gap_pct   = stock['gap_pct']
        dollar_vol = stock['dollar_vol']

        grade, notes = grade_setup(gap_pct, dollar_vol)
        strength, label, headlines, warning = check_catalyst(ticker)

        final_grade, entry_low, entry_high, stop_loss, target1 = \
            print_setup(
                ticker=ticker,
                name=stock['name'],
                price=price,
                prev_close=prev_close,
                gap_pct=gap_pct,
                dollar_vol=dollar_vol,
                grade=grade,
                notes=notes,
                catalyst_strength=strength,
                catalyst_label=label,
                headlines=headlines,
                warning=warning
            )

        # Alert for new A/A+ setups only
        if final_grade in ['A+', 'A'] and not warning:
            if ticker not in alerted_tickers:
                alerted_tickers.add(ticker)
                beep_alert(final_grade)
                show_popup(
                    ticker=ticker,
                    grade=final_grade,
                    gap_pct=gap_pct,
                    price=price,
                    catalyst_label=label,
                    entry_low=entry_low,
                    entry_high=entry_high,
                    stop_loss=stop_loss,
                    target1=target1
                )
                alert_premarket_gap(
                    ticker=ticker,
                    grade=final_grade,
                    gap_pct=gap_pct,
                    price=price,
                    catalyst_label=label,
                    entry_low=entry_low,
                    entry_high=entry_high,
                    stop_loss=stop_loss,
                    target1=target1,
                    notes=notes
                )

# ============================================================
# MAIN LOOP
# ============================================================

print("\n" + "="*55)
print("  🌅 SYKES METHOD — PRE-MARKET GAP SCANNER v5")
print("="*55)
print(f"  Started  : {datetime.now().strftime('%I:%M %p')}")
print(f"  Window   : 4:00 AM – 9:30 AM EST")
print(f"  Min Gap  : +{MIN_GAP_PCT}%")
print(f"  Price    : ${MIN_PRICE} – ${MAX_PRICE}")
print(f"  Min $Vol : ${MIN_DOLLAR_VOL:,}")
print(f"  Prices   : Real pre-market data ✅")
print(f"  Alerts   : Terminal + Pop-up + Email ✅")
print(f"  Sources  : Yahoo Finance + Finviz ✅")
print(f"  Interval : Every {SCAN_INTERVAL} seconds")
print("="*55)
print("\n  Press Ctrl+C to stop\n")

try:
    while True:
        now    = datetime.now(EST)
        hour   = now.hour
        minute = now.minute

        is_premarket = (
            (hour >= 4 and hour < 9) or
            (hour == 9 and minute < 30)
        )

        if is_premarket:
            run_premarket_scan()
            print(f"\n  ⏱  Next scan in {SCAN_INTERVAL} seconds...")
            time.sleep(SCAN_INTERVAL)

        elif hour >= 9 and (hour > 9 or minute >= 30) and hour < 20:
            print(f"\n  📈 [{now.strftime('%H:%M')}] Market is open!")
            print(f"  Switch to: python trading_agent.py")
            print(f"  Pre-market scanner shutting down...")
            break

        else:
            print(f"  💤 [{now.strftime('%H:%M')}] "
                  f"Pre-market starts at 4:00 AM EST. Waiting...")
            time.sleep(60)

except KeyboardInterrupt:
    print("\n\n  Pre-market scanner stopped. Good hunting! 🚀")