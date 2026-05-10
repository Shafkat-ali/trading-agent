from ib_insync import *
import time
import winsound
import yfinance as yf
from datetime import datetime

# Connect to IBKR
ib = IB()
ib.connect('127.0.0.1', 7496, clientId=1)

print("=" * 50)
print("  SYKES METHOD - POSITION MONITOR ACTIVE")
print("=" * 50)
print(f"  Started: {datetime.now().strftime('%H:%M:%S')}")
print(f"  Stop Loss Rule: -5%")
print("=" * 50)

def alert(message, urgent=False):
    print(f"\n🚨 [{datetime.now().strftime('%H:%M:%S')}] {message}")
    if urgent:
        for _ in range(3):
            winsound.Beep(1000, 500)
            time.sleep(0.2)
    else:
        winsound.Beep(800, 300)

def check_positions():
    positions = ib.positions()

    if not positions:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] No open positions. Monitoring...")
        return

    for pos in positions:
        ticker = pos.contract.symbol
        shares = pos.position
        avg_cost = pos.avgCost

        # Get live price via Yahoo Finance (free)
        try:
            stock = yf.Ticker(ticker)
            current_price = stock.fast_info['last_price']
        except:
            current_price = None

        if current_price and current_price > 0 and avg_cost > 0:
            pnl_pct = ((current_price - avg_cost) / avg_cost) * 100
            pnl_dollar = (current_price - avg_cost) * shares

            status = "✅" if pnl_pct >= 0 else "🔴"
            print(f"\n{status} {ticker} | Shares: {shares} | "
                  f"Avg: ${avg_cost:.2f} | "
                  f"Now: ${current_price:.2f} | "
                  f"P&L: {pnl_pct:+.1f}% (${pnl_dollar:+.2f})")

            # 5% STOP LOSS ALERT
            if pnl_pct <= -5:
                alert(f"STOP LOSS HIT! {ticker} is down {pnl_pct:.1f}%! "
                      f"CONSIDER EXITING NOW! Loss: ${pnl_dollar:.2f}", urgent=True)

            # Warning at -3%
            elif pnl_pct <= -3:
                alert(f"WARNING: {ticker} down {pnl_pct:.1f}% - "
                      f"Approaching stop loss zone!", urgent=False)

            # Profit alert
            elif pnl_pct >= 10:
                alert(f"PROFIT TARGET: {ticker} up {pnl_pct:.1f}%! "
                      f"Consider taking partial profits!")

        else:
            print(f"⚠️  Could not get price for {ticker}")

# Main loop
print("\nMonitoring positions... (Press Ctrl+C to stop)\n")
try:
    while True:
        check_positions()
        print(f"\n--- Next check in 30 seconds ---")
        time.sleep(30)

except KeyboardInterrupt:
    print("\n\nMonitor stopped.")
    ib.disconnect()