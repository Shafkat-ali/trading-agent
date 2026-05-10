from ib_insync import *
import time

print("Connecting to IBKR TWS...")

ib = IB()

try:
    ib.connect('127.0.0.1', 7496, clientId=1)  # 7497 = paper trading
    print("✅ Connected successfully!")
    print(f"Account: {ib.wrapper.accounts}")
    
    # Get account summary
    account = ib.accountSummary()
    for item in account:
        if item.tag in ['NetLiquidation', 'TotalCashValue']:
            print(f"{item.tag}: ${float(item.value):,.2f}")
            
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print("Make sure TWS is open and API is enabled")
    
finally:
    ib.disconnect()
    print("Disconnected.")