import smtplib
import winsound
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from config import *

# ============================================================
#   SHARED ALERTS MODULE
#   Used by both trading_agent.py and premarket_scanner.py
# ============================================================

def send_email(subject, body):
    """Send email alert in background thread"""
    def _send():
        try:
            msg = MIMEMultipart()
            msg['From'] = EMAIL_ADDRESS
            msg['To'] = EMAIL_TO
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, EMAIL_TO, msg.as_string())
            server.quit()
            print(f"  📧 Email sent: {subject}")
            
        except Exception as e:
            print(f"  ⚠️  Email failed: {e}")
    
    # Send in background so it doesn't block scanner
    thread = threading.Thread(target=_send)
    thread.daemon = True
    thread.start()

def alert_stop_loss(ticker, pnl_pct, pnl_dollar, current_price):
    """Stop loss alert — email + beep"""
    print(f"\n  🚨🚨🚨 STOP LOSS HIT — {ticker} DOWN {pnl_pct:.1f}%!")
    print(f"  🚨🚨🚨 EXIT NOW! Loss: ${pnl_dollar:.2f}")
    
    # Loud beep x3
    for _ in range(3):
        winsound.Beep(1000, 600)
        
    send_email(
        subject=f"🚨 STOP LOSS HIT — {ticker} DOWN {pnl_pct:.1f}%!",
        body=f"""
🚨 STOP LOSS ALERT — EXIT NOW!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Ticker     : {ticker}
Loss       : {pnl_pct:.1f}%
P&L        : ${pnl_dollar:.2f}
Price Now  : ${current_price:.2f}
Time       : {datetime.now().strftime('%I:%M:%S %p')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️  Sykes Rule: Cut losses FAST.
EXIT the position NOW!
        """
    )

def alert_warning(ticker, pnl_pct, current_price):
    """Warning alert — approaching stop loss"""
    print(f"  ⚠️  WARNING: {ticker} down {pnl_pct:.1f}% — approaching stop!")
    winsound.Beep(800, 400)
    
    send_email(
        subject=f"⚠️ WARNING — {ticker} down {pnl_pct:.1f}% approaching stop loss",
        body=f"""
⚠️ STOP LOSS WARNING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Ticker     : {ticker}
Loss       : {pnl_pct:.1f}%
Price Now  : ${current_price:.2f}
Time       : {datetime.now().strftime('%I:%M:%S %p')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Stop loss triggers at -5%.
Stay alert and be ready to exit!
        """
    )

def alert_profit(ticker, pnl_pct, pnl_dollar, current_price):
    """Profit target alert"""
    print(f"  🎯 PROFIT TARGET: {ticker} up {pnl_pct:.1f}%!")
    winsound.Beep(1200, 400)
    
    send_email(
        subject=f"🎯 PROFIT TARGET HIT — {ticker} UP {pnl_pct:.1f}%!",
        body=f"""
🎯 PROFIT TARGET ALERT!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Ticker     : {ticker}
Gain       : +{pnl_pct:.1f}%
P&L        : +${pnl_dollar:.2f}
Price Now  : ${current_price:.2f}
Time       : {datetime.now().strftime('%I:%M:%S %p')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Consider taking partial profits!
Lock in gains. Let rest run.
        """
    )

def alert_aplus_setup(ticker, pattern, grade, price, pct_change,
                       entry_low, entry_high, stop_loss,
                       target1, target2, target3, catalyst_label, notes):
    """A+ setup found alert"""
    print(f"  🚀 {grade} SETUP FOUND: {ticker}!")
    
    # Beep pattern for A+
    for _ in range(2):
        winsound.Beep(1200, 400)
        
    notes_text = "\n".join([f"• {n}" for n in notes])
    
    send_email(
        subject=f"🚀 {grade} SETUP — {ticker} {pattern} ({pct_change:+.1f}%)",
        body=f"""
🚀 {grade} SETUP ALERT — {ticker}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pattern    : {pattern}
Grade      : {grade}
Price      : ${price:.2f} ({pct_change:+.1f}%)
Catalyst   : {catalyst_label}
Time       : {datetime.now().strftime('%I:%M:%S %p')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TRADE PLAN:
Entry      : ${entry_low:.2f} – ${entry_high:.2f}
Stop Loss  : ${stop_loss:.2f} (-5%)
Target 1   : ${target1:.2f} (+10%)
Target 2   : ${target2:.2f} (+20%)
Target 3   : ${target3:.2f} (+30%)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NOTES:
{notes_text}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️  YOU execute — stay disciplined!
        """
    )

def alert_premarket_gap(ticker, grade, gap_pct, price,
                         catalyst_label, entry_low, entry_high,
                         stop_loss, target1, notes):
    """Pre-market gap alert"""
    print(f"  🌅 PRE-MARKET GAP: {ticker} +{gap_pct:.1f}%!")
    
    for _ in range(2):
        winsound.Beep(1100, 400)
        
    notes_text = "\n".join([f"• {n}" for n in notes])
    
    send_email(
        subject=f"🌅 PRE-MARKET GAP — {ticker} +{gap_pct:.1f}% Grade: {grade}",
        body=f"""
🌅 PRE-MARKET GAP ALERT — {ticker}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Grade      : {grade}
Gap Up     : +{gap_pct:.1f}%
Price      : ${price:.2f}
Catalyst   : {catalyst_label}
Time       : {datetime.now().strftime('%I:%M:%S %p')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TRADE PLAN:
Entry      : ${entry_low:.2f} – ${entry_high:.2f}
Stop Loss  : ${stop_loss:.2f} (-5%)
Target 1   : ${target1:.2f} (+10%)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NOTES:
{notes_text}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Watch for continuation at open!
        """
    )