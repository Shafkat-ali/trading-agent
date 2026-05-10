import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import *

print("Sending test email...")

try:
    # Create message
    msg = MIMEMultipart()
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = EMAIL_TO
    msg['Subject'] = "✅ Trading Agent — Email Alerts Working!"
    
    body = """
🔥 SYKES METHOD TRADING AGENT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Email alerts are now active!

You will be notified for:

🚨 STOP LOSS — Position down 5%
🚀 A+ SETUP  — Strong pattern found
🌅 PRE-MARKET — Big gap up detected
📰 CATALYST  — Strong news found
🎯 PROFIT    — Target hit (+10%)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Stay disciplined. Cut losses fast.
Let winners run. — Tim Sykes
    """
    
    msg.attach(MIMEText(body, 'plain'))
    
    # Send via Gmail
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    server.sendmail(EMAIL_ADDRESS, EMAIL_TO, msg.as_string())
    server.quit()
    
    print("✅ Email sent successfully!")
    print(f"Check your inbox at {EMAIL_TO}")
    
except Exception as e:
    print(f"❌ Email failed: {e}")