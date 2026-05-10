from twilio.rest import Client
from config import *

print("Sending test SMS...")

try:
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    
    message = client.messages.create(
        body=(
            "✅ TRADING AGENT ONLINE\n"
            "SMS alerts are working!\n"
            "You'll be notified of:\n"
            "🚨 Stop loss hits\n"
            "🚀 A+ setups\n"
            "🌅 Pre-market gaps\n"
            "📰 Strong catalysts"
        ),
        from_=TWILIO_FROM_NUMBER,
        to=TWILIO_TO_NUMBER
    )
    
    print(f"✅ SMS sent! Message SID: {message.sid}")
    print(f"Check your phone!")
    
except Exception as e:
    print(f"❌ SMS failed: {e}")