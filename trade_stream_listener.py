#!/usr/bin/env python3
"""
Multi-Broker Trade Stream Listener Daemon (Alpaca + IBKR)
=========================================================
Runs standalone via PM2/systemd.
Maintains concurrent asynchronous listeners for:
  - Alpaca (Paper/Live TradingStream via websockets)
  - Interactive Brokers (Client Portal Gateway WebSocket on localhost:5000)

Translates wire updates into UnifiedTradeEvent objects and reconciles
the PostgreSQL oms_managed_trades ledger in real time.
"""

import os
import ssl
import json
import asyncio
import psycopg2
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
import websockets
from alpaca.trading.stream import TradingStream
from vault import load_secrets

load_secrets()

DATABASE_URL = os.environ.get("DATABASE_URL")
MARKET_TZ = ZoneInfo("America/New_York")
IB_WS_URL = os.environ.get("IB_WS_URL", "wss://localhost:5000/v1/api/ws")


# =========================================================
# 1. NORMALIZED DATA MODEL
# =========================================================
@dataclass
class UnifiedTradeEvent:
    broker: str            # 'alpaca' or 'ibkr'
    account_id: str
    ticker: str
    event_type: str        # 'FILL', 'PARTIAL_FILL', 'CANCELED', 'REJECTED'
    side: str              # 'BUY' or 'SELL'
    qty: float
    fill_price: float
    client_order_id: str
    broker_order_id: str


def get_db():
    return psycopg2.connect(DATABASE_URL)


async def process_unified_event(event: UnifiedTradeEvent):
    """Unified handler that updates oms_managed_trades regardless of broker origin."""
    print(f"[{event.broker.upper()}:{event.account_id}] ⚡ {event.event_type} | {event.side} {event.qty} {event.ticker} @ ${event.fill_price:.2f}")

    if event.event_type in ['FILL', 'PARTIAL_FILL']:
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("""
                UPDATE oms_managed_trades 
                SET entry_price = %s,
                    qty = %s,
                    is_active = TRUE,
                    entry_time = NOW(),
                    updated_at = NOW()
                WHERE (lot_id = %s OR ticker = %s) 
                  AND broker = %s 
                  AND is_active = TRUE;
            """, (event.fill_price, event.qty, event.client_order_id, event.ticker, event.broker))
            conn.commit()
            cur.close()
            conn.close()
            print(f"[{event.broker.upper()}] [✓] Reconciled fill in PostgreSQL for {event.ticker}.")
        except Exception as e:
            print(f"[{event.broker.upper()}] [!] DB fill sync error: {e}")

    elif event.event_type in ['CANCELED', 'REJECTED']:
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("""
                UPDATE oms_managed_trades 
                SET is_active = FALSE, updated_at = NOW()
                WHERE (lot_id = %s OR ticker = %s) 
                  AND broker = %s;
            """, (event.client_order_id, event.ticker, event.broker))
            conn.commit()
            cur.close()
            conn.close()
            print(f"[{event.broker.upper()}] [✓] Closed lot in PostgreSQL for {event.ticker}.")
        except Exception as e:
            print(f"[{event.broker.upper()}] [!] DB cancel sync error: {e}")


# =========================================================
# 2. ALPACA STREAM ADAPTER
# =========================================================
async def run_alpaca_stream(account_id: str, key: str, secret: str, paper: bool):
    while True:
        try:
            print(f"[*] [ALPACA:{account_id}] Connecting Trade WebSocket (Paper={paper})...")
            stream = TradingStream(key, secret, paper=paper)

            async def handle_alpaca(data):
                order = data.order
                event_name = data.event.upper() # 'FILL', 'PARTIAL_FILL', 'CANCELED', etc.
                
                normalized = UnifiedTradeEvent(
                    broker="alpaca",
                    account_id=account_id,
                    ticker=order['symbol'].upper(),
                    event_type=event_name,
                    side=str(order['side']).upper(),
                    qty=float(order.get('filled_qty') or order.get('qty') or 0),
                    fill_price=float(order.get('filled_avg_price') or 0.0),
                    client_order_id=order.get('client_order_id', ''),
                    broker_order_id=str(order.get('id', ''))
                )
                await process_unified_event(normalized)

            stream.subscribe_trade_updates(handle_alpaca)
            await stream._run_forever()

        except Exception as e:
            print(f"[!] [ALPACA:{account_id}] Disconnected: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


# =========================================================
# 3. INTERACTIVE BROKERS (IBKR) STREAM ADAPTER
# =========================================================
async def run_ibkr_stream(account_id: str):
    """
    Maintains a persistent WSS connection to the local IBKR Client Portal Gateway.
    Subscribes to live orders ('sor+{}') and executions ('str+{}').
    Sends periodic heartbeat frames ('ech+hb') every 10 seconds.
    """
    # Disable SSL verification for IBKR local self-signed cert
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    while True:
        try:
            print(f"[*] [IBKR:{account_id}] Connecting to Gateway WebSocket at {IB_WS_URL}...")
            async with websockets.connect(IB_WS_URL, ssl=ssl_context) as ws:
                print(f"[✓] [IBKR:{account_id}] WebSocket connected.")

                # Subscribe to Live Orders and Real-time Trades
                await ws.send('sor+{}')
                await ws.send('str+{"realtimeUpdatesOnly": true}')

                async def heartbeat():
                    while True:
                        await asyncio.sleep(10)
                        try:
                            await ws.send("ech+hb")
                        except Exception:
                            break

                hb_task = asyncio.create_task(heartbeat())

                try:
                    async for raw_msg in ws:
                        # Skip heartbeat echoes
                        if raw_msg.startswith("ech+"):
                            continue

                        # IBKR sends messages with topic prefixes, e.g., 'str+{...}' or 'sor+{...}'
                        topic = raw_msg[:3]
                        payload_str = raw_msg[4:] if len(raw_msg) > 4 and raw_msg[3] == '+' else raw_msg

                        try:
                            data = json.loads(payload_str)
                        except json.JSONDecodeError:
                            continue

                        # Handle Trade Fills ('str' = Streaming Trades)
                        if topic == 'str':
                            trades = data if isinstance(data, list) else [data]
                            for tr in trades:
                                side_raw = tr.get('side', '').upper()
                                side = 'BUY' if side_raw in ['BOT', 'BUY', 'B'] else 'SELL'
                                
                                normalized = UnifiedTradeEvent(
                                    broker="ibkr",
                                    account_id=account_id,
                                    ticker=tr.get('symbol', tr.get('contractDescription', '')).upper(),
                                    event_type="FILL",
                                    side=side,
                                    qty=float(tr.get('size', 0)),
                                    fill_price=float(tr.get('price', 0.0)),
                                    client_order_id=tr.get('orderRef', ''),
                                    broker_order_id=str(tr.get('executionId', ''))
                                )
                                await process_unified_event(normalized)

                        # Handle Order Status / Cancellations ('sor' = Streaming Orders)
                        elif topic == 'sor':
                            orders = data if isinstance(data, list) else [data]
                            for o in orders:
                                status = o.get('status', '').lower()
                                if status in ['cancelled', 'inactive']:
                                    normalized = UnifiedTradeEvent(
                                        broker="ibkr",
                                        account_id=account_id,
                                        ticker=o.get('ticker', '').upper(),
                                        event_type="CANCELED",
                                        side=o.get('side', '').upper(),
                                        qty=float(o.get('remainingQuantity', 0)),
                                        fill_price=0.0,
                                        client_order_id=o.get('cOID', ''),
                                        broker_order_id=str(o.get('orderId', ''))
                                    )
                                    await process_unified_event(normalized)

                finally:
                    hb_task.cancel()

        except Exception as e:
            print(f"[!] [IBKR:{account_id}] Gateway socket disconnected: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


# =========================================================
# 4. MULTI-BROKER DAEMON ORCHESTRATOR
# =========================================================
async def main():
    tasks = []

    # 1. Register Alpaca Accounts
    alpaca_key = os.environ.get("ALPACA_API_KEY")
    alpaca_secret = os.environ.get("ALPACA_SECRET_KEY")
    if alpaca_key and alpaca_secret:
        tasks.append(run_alpaca_stream("paper_primary", alpaca_key, alpaca_secret, paper=True))

    # 2. Register IBKR Gateway Account
    ibkr_account_id = os.environ.get("IBKR_ACCOUNT_ID", "DU1234567")
    tasks.append(run_ibkr_stream(ibkr_account_id))

    print(f"🚀 Starting Unified Trade Stream Listener across {len(tasks)} broker pipelines...")
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
