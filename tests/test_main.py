from ib_insync import *
from datetime import datetime

ib = IB()

ib.connect('172.27.80.1', 4001, clientId=1)
print("✅ Connected:", ib.isConnected())

print("\n===== ACCOUNT SUMMARY =====")
account = ib.accountSummary()

def get_value(tag):
    for item in account:
        if item.tag == tag:
            return item.value
    return None

print("NetLiquidation:", get_value("NetLiquidation"))
print("TotalCashValue:", get_value("TotalCashValue"))
print("BuyingPower:", get_value("BuyingPower"))
print("AvailableFunds:", get_value("AvailableFunds"))

print("\n===== POSITIONS =====")
positions = ib.positions()

if not positions:
    print("No positions")
else:
    for p in positions:
        print({
            "symbol": p.contract.symbol,
            "position": p.position,
            "avgCost": p.avgCost
        })

print("\n===== OPEN ORDERS =====")
open_orders = ib.openOrders()

if not open_orders:
    print("No open orders")
else:
    for o in open_orders:
        print(o)

print("\n===== EXECUTIONS =====")
executions = ib.executions()

if not executions:
    print("No executions")
else:
    for e in executions:
        print({
            "symbol": e.contract.symbol,
            "side": e.execution.side,
            "qty": e.execution.shares,
            "price": e.execution.price,
            "time": e.execution.time
        })

print("\n===== MARKET DATA (SPY) =====")

ib.reqMarketDataType(3)  # 3 = delayed, 1 = real-time

contract = Stock('SPY', 'SMART', 'USD')
ticker = ib.reqMktData(contract)

ib.sleep(2)

print({
    "last": ticker.last,
    "bid": ticker.bid,
    "ask": ticker.ask
})

print("\n===== REAL-TIME BAR =====")

bars = ib.reqRealTimeBars(contract, 60, 'TRADES', False)

def on_bar(bar):
    print(f"[{datetime.now()}] close={bar.close}")

bars.updateEvent += on_bar

print("Listening for bars... (Ctrl+C to stop)")
ib.run()

