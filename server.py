#!/usr/bin/env python3
"""
Portfolio Sync Server & Execution Gateway (OMS)
================================================
Location: /root/development/portfolio_sync_server/server.py
Managed by: portfolio-sync.service

Features:
  - Multi-broker read sync: Questrade, IBKR, Alpaca
  - Multi-account execution:
      * alpaca_macro (3-day swings)
      * alpaca_midday (90-min mean reversions)
      * alpaca_power_hour / alpaca_paper (50-min closing momentum)
      * ibkr_primary (Live execution gateway)
  - Strict Long-Only Protection:
      * Rejects SELL orders when sellable inventory <= 0 (422 ZERO_SELLABLE_INVENTORY)
      * Clamps sell quantities to available long inventory to block net shorting
      * Per-ticker mutual exclusion locks
  - Stateful PostgreSQL Ledger (oms_managed_trades):
      * Multi-lot virtual accounting via unique lot_id
      * FIFO lot liquidation
  - Autonomous 24/7 Trade Lifecycle Daemon:
      * Dynamic EOD liquidations: 15:55 EDT (Power Hour) vs 15:45 EDT (Midday)
      * Time horizon expiration
      * 50% smart partial take-profit with breakeven stop ratchets (+0.1%)
      * Hard stop loss and trailing stops
      * -3.0% daily portfolio equity drawdown circuit breaker
  - Embedded Multi-Broker WebSocket Listeners:
      * Concurrent Alpaca TradingStream listeners
      * IBKR Client Portal Gateway WSS listener (localhost:5000)
"""

import os
import time
import json
import ssl
import asyncio
import requests
import psycopg2
import psycopg2.extras
import urllib3
import threading
import concurrent.futures
from functools import wraps
from datetime import datetime
from dataclasses import dataclass
from collections import defaultdict
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request
from flask_cors import CORS
import websockets

from vault import load_secrets

# Alpaca trading SDK
from alpaca.trading.client import TradingClient
from alpaca.trading.stream import TradingStream
from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus

# Suppress self-signed cert warnings for IBKR Client Portal Gateway
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
CORS(app, max_age=86400)

load_secrets()

PORT = int(os.environ.get("PORT", 3000))
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://username:password@localhost:5432/your_database")
MARKET_TZ = ZoneInfo("America/New_York")
IB_WS_URL = os.environ.get("IB_WS_URL", "wss://localhost:5000/v1/api/ws")

# =========================================================
# GATEWAY SECURITY, LOCKS & MULTI-ACCOUNT REGISTRY
# =========================================================
GATEWAY_API_KEY = os.environ.get("GATEWAY_API_KEY", "your_super_secret_tailnet_key_2026")
GLOBAL_TRADING_HALTED = False

ticker_locks = defaultdict(threading.Lock)
db_lock = threading.Lock()

ALPACA_ACCOUNTS = {}

# 1. Macro Swings Account (Default Paper)
k_macro = os.environ.get("ALPACA_MACRO_KEY") or os.environ.get("ALPACA_API_KEY")
s_macro = os.environ.get("ALPACA_MACRO_SECRET") or os.environ.get("ALPACA_SECRET_KEY")
if k_macro and s_macro:
    ALPACA_ACCOUNTS["alpaca_macro"] = TradingClient(k_macro, s_macro, paper=True)

# 2. Midday Reversion Account (Secondary Paper)
k_midday = os.environ.get("ALPACA_MIDDAY_KEY")
s_midday = os.environ.get("ALPACA_MIDDAY_SECRET")
if k_midday and s_midday:
    ALPACA_ACCOUNTS["alpaca_midday"] = TradingClient(k_midday, s_midday, paper=True)
elif "alpaca_macro" in ALPACA_ACCOUNTS:
    ALPACA_ACCOUNTS["alpaca_midday"] = ALPACA_ACCOUNTS["alpaca_macro"]

# 3. Power Hour / Paper Staging Aliases
k_power = os.environ.get("ALPACA_POWER_HOUR_KEY") or os.environ.get("ALPACA_PAPER_KEY")
s_power = os.environ.get("ALPACA_POWER_HOUR_SECRET") or os.environ.get("ALPACA_PAPER_SECRET")
if k_power and s_power:
    client_ph = TradingClient(k_power, s_power, paper=True)
    ALPACA_ACCOUNTS["alpaca_power_hour"] = client_ph
    ALPACA_ACCOUNTS["alpaca_paper"] = client_ph
elif "alpaca_macro" in ALPACA_ACCOUNTS:
    ALPACA_ACCOUNTS["alpaca_power_hour"] = ALPACA_ACCOUNTS["alpaca_macro"]
    ALPACA_ACCOUNTS["alpaca_paper"] = ALPACA_ACCOUNTS["alpaca_macro"]

# 4. Live Production Account
k_live = os.environ.get("ALPACA_LIVE_KEY")
s_live = os.environ.get("ALPACA_LIVE_SECRET")
if k_live and s_live:
    ALPACA_ACCOUNTS["alpaca_live"] = TradingClient(k_live, s_live, paper=False)


def get_alpaca_client(account_id: str = "alpaca_macro") -> TradingClient | None:
    if account_id in ALPACA_ACCOUNTS:
        return ALPACA_ACCOUNTS[account_id]
    return ALPACA_ACCOUNTS.get("alpaca_macro")


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def send_oms_telegram_alert(msg: str) -> None:
    bot_token = os.environ.get("TELEGRAM_TOKEN") or os.environ.get("TG_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID") or os.environ.get("TG_CHAT_ID")
    if not bot_token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=4)
    except Exception as e:
        print(f"[OMS] Telegram alert error: {e}")


def require_apikey(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        provided_key = request.headers.get('X-Gateway-Token')
        if not provided_key or provided_key != GATEWAY_API_KEY:
            print(f"[OMS] 🚨 UNAUTHORIZED ATTEMPT BLOCKED FROM IP: {request.remote_addr} | Target: {request.path}")
            return jsonify({"status": "error", "message": "Unauthorized. Invalid Gateway Token."}), 401
        return f(*args, **kwargs)
    return decorated_function


# =========================================================
# DATABASE INITIALIZATION & MIGRATION
# =========================================================
def init_oms_db():
    try:
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS oms_managed_trades (
                    id SERIAL PRIMARY KEY,
                    lot_id VARCHAR(64) UNIQUE,
                    account_id VARCHAR(32) DEFAULT 'alpaca_macro',
                    ticker VARCHAR(16) NOT NULL,
                    broker VARCHAR(16) NOT NULL,
                    agent_id VARCHAR(64) DEFAULT 'macro_fleet',
                    strategy_id VARCHAR(64) DEFAULT 'macro_ohlcv',
                    side VARCHAR(8) NOT NULL,
                    qty NUMERIC(12, 4) NOT NULL,
                    entry_price NUMERIC(12, 4) NOT NULL,
                    mfe_target NUMERIC(8, 4) DEFAULT 0.0250,
                    mae_stop NUMERIC(8, 4) DEFAULT -0.0180,
                    tp_price NUMERIC(12, 4),
                    sl_price NUMERIC(12, 4),
                    horizon_sec INT DEFAULT 259200,
                    force_eod_flat BOOLEAN DEFAULT FALSE,
                    eod_cutoff VARCHAR(16) DEFAULT '15:45',
                    trail_to_breakeven BOOLEAN DEFAULT TRUE,
                    trailing_status VARCHAR(32) DEFAULT 'INITIAL',
                    entry_time TIMESTAMPTZ DEFAULT NOW(),
                    is_active BOOLEAN DEFAULT TRUE,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                ALTER TABLE oms_managed_trades ADD COLUMN IF NOT EXISTS eod_cutoff VARCHAR(16) DEFAULT '15:45';
                CREATE INDEX IF NOT EXISTS idx_oms_managed_active ON oms_managed_trades (account_id, ticker, is_active);
            """)
            conn.commit()
            cur.close()
            conn.close()
        print("[OMS] Database: oms_managed_trades verified with dynamic eod_cutoff support.")
    except Exception as e:
        print(f"[OMS] Database init error: {e}")


# =========================================================
# INVENTORY CONTROLS & STRICT SHORT PREVENTION
# =========================================================
def get_sellable_inventory(broker: str, ticker: str, account_id: str = "alpaca_macro") -> float:
    broker = broker.lower()
    ticker = ticker.upper()
    current_long_qty = 0.0

    if broker == 'alpaca':
        client = get_alpaca_client(account_id)
        if client:
            try:
                pos = client.get_open_position(ticker)
                current_long_qty = max(0.0, float(pos.qty))
            except Exception:
                current_long_qty = 0.0

    elif broker == 'ibkr':
        for p in get_ibkr_positions():
            if p.get('symbol', '').upper() == ticker:
                current_long_qty = max(0.0, float(p.get('qty', 0)))
                break

    if current_long_qty <= 0.0:
        return 0.0

    pending_sell_qty = 0.0
    if broker == 'alpaca':
        client = get_alpaca_client(account_id)
        if client:
            try:
                req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[ticker])
                open_orders = client.get_orders(req)
                for o in open_orders:
                    side = str(o.side.value if hasattr(o.side, 'value') else o.side).upper()
                    if side == 'SELL':
                        pending_sell_qty += float(o.qty or 0)
            except Exception as e:
                print(f"[OMS] Error fetching open orders for {ticker} ({account_id}): {e}")

    elif broker == 'ibkr':
        try:
            IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
            res = requests.get(f"{IB_GATEWAY_URL}/iserver/account/orders", verify=False, timeout=2)
            if res.status_code == 200:
                for o in res.json().get('orders', []):
                    if o.get('ticker', '').upper() == ticker and o.get('side', '').upper() == 'SELL':
                        pending_sell_qty += float(o.get('remainingQuantity', 0))
        except Exception:
            pass

    return max(0.0, current_long_qty - pending_sell_qty)


# =========================================================
# COLD-BOOT RECONCILIATION ENGINE
# =========================================================
def reconcile_cold_boot_state():
    print("[OMS] Running multi-account cold-boot position reconciliation...")

    with db_lock:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("SELECT ticker, parameters FROM active_deployments WHERE is_active = TRUE;")
        deployed_models = {}
        for r in cur.fetchall():
            params = r['parameters']
            deployed_models[r['ticker'].upper()] = json.loads(params) if isinstance(params, str) else params

        for acct_id, client in ALPACA_ACCOUNTS.items():
            try:
                broker_positions = client.get_all_positions()
                broker_map = {p.symbol.upper(): float(p.qty) for p in broker_positions}

                cur.execute("""
                    SELECT ticker, SUM(qty) as total_db_qty, array_agg(id ORDER BY entry_time ASC) as lot_ids,
                           array_agg(qty ORDER BY entry_time ASC) as lot_qtys
                    FROM oms_managed_trades
                    WHERE account_id = %s AND is_active = TRUE
                    GROUP BY ticker;
                """, (acct_id,))
                db_summary = {r['ticker'].upper(): r for r in cur.fetchall()}

                for sym, db_info in db_summary.items():
                    broker_qty = broker_map.get(sym, 0.0)
                    total_db_qty = float(db_info['total_db_qty'])

                    if broker_qty == 0.0:
                        print(f"[OMS:{acct_id}] ⚠️ {sym} is flat at broker. Closing orphaned DB lots.")
                        cur.execute("UPDATE oms_managed_trades SET is_active = FALSE, updated_at = NOW() WHERE ticker = %s AND account_id = %s AND is_active = TRUE;", (sym, acct_id))
                        conn.commit()
                    elif broker_qty < total_db_qty:
                        deficit = total_db_qty - broker_qty
                        print(f"[OMS:{acct_id}] ⚠️ {sym} quantity drift: Deficit of {deficit} shares. Reconciling FIFO lots.")
                        for l_id, l_qty in zip(db_info['lot_ids'], db_info['lot_qtys']):
                            l_qty = float(l_qty)
                            if deficit >= l_qty:
                                cur.execute("UPDATE oms_managed_trades SET is_active = FALSE, updated_at = NOW() WHERE id = %s;", (l_id,))
                                deficit -= l_qty
                            else:
                                cur.execute("""
                                    UPDATE oms_managed_trades 
                                    SET qty = %s, mae_stop = 0.0010, trailing_status = 'BREAKEVEN_LOCKED', updated_at = NOW() 
                                    WHERE id = %s;
                                """, (l_qty - deficit, l_id))
                                deficit = 0.0
                                break
                        conn.commit()

                # Adopt or quarantine unmanaged broker holdings
                for p in broker_positions:
                    sym = p.symbol.upper()
                    qty = float(p.qty)
                    avg_px = float(p.avg_entry_price)

                    if sym not in db_summary:
                        if sym in deployed_models:
                            conf = deployed_models[sym]
                            mfe = float(conf.get('mfe_target', 0.025))
                            mae = float(conf.get('mae_stop', -0.018))
                            horizon = int(conf.get('horizon_sec', 259200))
                            model_type = conf.get('strategy_id') or conf.get('model_type', 'macro_ohlcv')
                            force_eod = bool(model_type in ['intraday_midday_reversion', 'intraday_power_hour'])
                            eod_cutoff = conf.get('eod_cutoff', '15:55' if model_type == 'intraday_power_hour' else '15:45')
                            lot_id = f"adopt_{sym}_{int(time.time()*1000)}"

                            cur.execute("""
                                INSERT INTO oms_managed_trades 
                                (lot_id, account_id, ticker, broker, agent_id, strategy_id, side, qty, entry_price, 
                                 mfe_target, mae_stop, tp_price, sl_price, horizon_sec, force_eod_flat, eod_cutoff,
                                 trail_to_breakeven, trailing_status, entry_time, is_active, updated_at)
                                VALUES (%s, %s, %s, 'alpaca', 'cold_boot_recovery', %s, 'BUY', %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, TRUE, 'INITIAL', NOW(), TRUE, NOW());
                            """, (lot_id, acct_id, sym, model_type, qty, avg_px, mfe, mae,
                                  round(avg_px * (1 + mfe), 2), round(avg_px * (1 + mae), 2),
                                  horizon, force_eod, eod_cutoff))
                            conn.commit()
                            print(f"[OMS:{acct_id}] [✓] Auto-adopted {qty} {sym} @ ${avg_px:.2f} under {model_type} (Cutoff: {eod_cutoff}).")
                        else:
                            print(f"[OMS:{acct_id}] ⚠️ Quarantined unmanaged position: {qty} {sym}.")

            except Exception as e:
                print(f"[OMS] Error reconciling account {acct_id}: {e}")

        cur.close()
        conn.close()

    print("[OMS] [✓] Cold-boot reconciliation complete.")


# =========================================================
# 1. QUESTRADE INTEGRATION
# =========================================================
qt_access_token = ''
qt_api_server = ''

def refresh_questrade_token():
    global qt_access_token, qt_api_server
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT value FROM system_secrets WHERE key = 'qt_refresh_token'")
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return False
            
        refresh_token = result[0]
        url = f"https://login.questrade.com/oauth2/token?grant_type=refresh_token&refresh_token={refresh_token}"
        response = requests.get(url, timeout=10)
        response.raise_for_status() 
        data = response.json()

        qt_access_token = data['access_token']
        qt_api_server = data['api_server']

        new_refresh_token = data['refresh_token']
        cur.execute("""
            INSERT INTO system_secrets (key, value) 
            VALUES ('qt_refresh_token', %s)
            ON CONFLICT (key) DO UPDATE 
            SET value = EXCLUDED.value
        """, (new_refresh_token,))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Questrade Token Refresh Error: {e}")
        return False

def get_questrade_positions(is_retry=False):
    global qt_access_token, qt_api_server
    if not qt_access_token and not refresh_questrade_token():
        return []

    try:
        headers = {'Authorization': f'Bearer {qt_access_token}'}
        acct_res = requests.get(f"{qt_api_server}v1/accounts", headers=headers, timeout=5)
        acct_res.raise_for_status()
        accounts = acct_res.json().get('accounts', [])
        if not accounts:
            return []
            
        account_id = accounts[0]['number']
        pos_res = requests.get(f"{qt_api_server}v1/accounts/{account_id}/positions", headers=headers, timeout=5)
        pos_res.raise_for_status()

        positions = []
        for p in pos_res.json().get('positions', []):
            positions.append({
                'symbol': p['symbol'],
                'qty': p['openQuantity'],
                'avgPrice': p['averageEntryPrice']
            })
        return positions
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401 and not is_retry:
            qt_access_token = ''
            return get_questrade_positions(is_retry=True)
        return []
    except Exception:
        return []

def get_questrade_balances(is_retry=False):
    global qt_access_token, qt_api_server
    if not qt_access_token and not refresh_questrade_token():
        return {'cash': 0, 'buyingPower': 0, 'equity': 0}

    try:
        headers = {'Authorization': f'Bearer {qt_access_token}'}
        acct_res = requests.get(f"{qt_api_server}v1/accounts", headers=headers, timeout=5)
        acct_res.raise_for_status()
        accounts = acct_res.json().get('accounts', [])
        if not accounts:
            return {'cash': 0, 'buyingPower': 0, 'equity': 0}
            
        account_id = accounts[0]['number']
        bal_res = requests.get(f"{qt_api_server}v1/accounts/{account_id}/balances", headers=headers, timeout=5)
        bal_res.raise_for_status()

        combined = bal_res.json().get('combinedBalances', [])
        target_bal = next((b for b in combined if b.get('currency') == 'USD'), combined[0] if combined else {})
        return {
            'cash': target_bal.get('cash', 0),
            'buyingPower': target_bal.get('buyingPower', 0),
            'equity': target_bal.get('totalEquity', 0)
        }
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401 and not is_retry:
            qt_access_token = '' 
            return get_questrade_balances(is_retry=True)
        return {'cash': 0, 'buyingPower': 0, 'equity': 0}
    except Exception:
        return {'cash': 0, 'buyingPower': 0, 'equity': 0}


# =========================================================
# 2. INTERACTIVE BROKERS (IBKR) INTEGRATION
# =========================================================
def get_ibkr_positions():
    try:
        IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
        acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=2)
        acct_res.raise_for_status()
        accounts = acct_res.json()
        if not accounts:
            return []
            
        all_positions = []
        for account in accounts:
            account_id = account.get('id') or account.get('accountId')
            try:
                pos_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/{account_id}/positions/0", verify=False, timeout=2)
                pos_res.raise_for_status()
                for p in pos_res.json():
                    all_positions.append({
                        'symbol': p.get('contractDesc', ''), 
                        'qty': float(p.get('position', 0)),
                        'avgPrice': float(p.get('avgCost', 0))
                    })
            except Exception:
                continue
        return all_positions
    except Exception:
        return []

def get_ibkr_balances():
    try:
        IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
        acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=2)
        acct_res.raise_for_status()
        accounts = acct_res.json()
        if not accounts:
            return {'cash': 0, 'buyingPower': 0, 'equity': 0}
            
        total_cash = 0.0
        total_bp = 0.0
        total_equity = 0.0
        for account in accounts:
            account_id = account.get('id') or account.get('accountId')
            try:
                bal_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/{account_id}/summary", verify=False, timeout=2)
                bal_res.raise_for_status()
                summary = bal_res.json()
                total_cash += float(summary.get('totalcashvalue', {}).get('amount', 0))
                total_bp += float(summary.get('buyingpower', {}).get('amount', 0))
                total_equity += float(summary.get('netliquidation', {}).get('amount', 0))
            except Exception:
                continue
        return {'cash': total_cash, 'buyingPower': total_bp, 'equity': total_equity}
    except Exception:
        return {'cash': 0, 'buyingPower': 0, 'equity': 0}


# =========================================================
# 3. ALPACA INTEGRATION (MULTI-ACCOUNT)
# =========================================================
def get_alpaca_positions_for_account(account_id: str = "alpaca_macro"):
    client = get_alpaca_client(account_id)
    if not client:
        return []
    try:
        positions = client.get_all_positions()
        return [{
            'symbol': p.symbol.upper(),
            'qty': float(p.qty),
            # Support both camelCase and snake_case for UI consumers
            'entry_price': float(p.avg_entry_price),
            'avgPrice': float(p.avg_entry_price),
            'current_price': float(p.current_price),
            'currentPrice': float(p.current_price),
            'market_value': float(p.market_value),
            'marketValue': float(p.market_value),
            'cost_basis': float(p.cost_basis),
            'unrealized_pl': float(p.unrealized_pl),
            'unrealized_pnl': float(p.unrealized_pl),
            'unrealized_plpc': float(p.unrealized_plpc),
            'pnl_pct': float(p.unrealized_plpc) * 100.0,
            'change_today': float(p.change_today or 0.0)
        } for p in positions]
    except Exception as e:
        print(f"⚠️ Alpaca Error fetching positions for {account_id}: {e}")
        return []

def get_alpaca_balances_for_account(account_id: str = "alpaca_macro"):
    client = get_alpaca_client(account_id)
    if not client:
        return {'cash': 0, 'buyingPower': 0, 'equity': 0, 'portfolio_value': 0, 'last_equity': 0}
    try:
        acct = client.get_account()
        equity_val = float(acct.equity)
        return {
            'cash': float(acct.cash),
            'buyingPower': float(acct.buying_power),
            'buying_power': float(acct.buying_power),
            'equity': equity_val,
            'portfolio_value': float(getattr(acct, 'portfolio_value', equity_val)),
            'capital': equity_val,
            'account_capital': equity_val,
            'last_equity': float(acct.last_equity)
        }
    except Exception as e:
        print(f"⚠️ Alpaca Error fetching balances for {account_id}: {e}")
        return {'cash': 0, 'buyingPower': 0, 'equity': 0, 'portfolio_value': 0, 'last_equity': 0}


# =========================================================
# 4. READ-ONLY SYNC ROUTES
# =========================================================
@app.route('/positions.json', methods=['GET'])
def positions():
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_positions = executor.submit(get_questrade_positions).result()
            ibkr_positions = executor.submit(get_ibkr_positions).result()

        merged_map = {}
        for p in qt_positions + ibkr_positions:
            sym = p['symbol']
            if sym not in merged_map:
                merged_map[sym] = {'qty': p['qty'], 'avgPrice': p['avgPrice']}
            else:
                existing = merged_map[sym]
                new_qty = existing['qty'] + p['qty']
                new_avg = 0 if new_qty == 0 else ((existing['qty'] * existing['avgPrice']) + (p['qty'] * p['avgPrice'])) / new_qty
                merged_map[sym] = {'qty': new_qty, 'avgPrice': new_avg}

        final_array = [{'symbol': sym, 'qty': data['qty'], 'avgPrice': data['avgPrice']} 
                       for sym, data in merged_map.items() if data['qty'] != 0]

        return jsonify({"questrade": qt_positions, "ibkr": ibkr_positions, "total": final_array})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/balances.json', methods=['GET'])
def balances():
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_bal = executor.submit(get_questrade_balances).result()
            ib_bal = executor.submit(get_ibkr_balances).result()

        return jsonify({
            "questrade": qt_bal, "ibkr": ib_bal,
            "total": {
                "cash": qt_bal['cash'] + ib_bal['cash'],
                "buyingPower": qt_bal['buyingPower'] + ib_bal['buyingPower']
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/equity.json', methods=['GET'])
def equity():
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_data = executor.submit(get_questrade_balances).result()
            ib_data = executor.submit(get_ibkr_balances).result()

        return jsonify({
            "questrade": {"equity": qt_data['equity']},
            "ibkr": {"equity": ib_data['equity']},
            "total": {"equity": qt_data['equity'] + ib_data['equity']}
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/networth.csv', methods=['GET'])
def networth_csv():
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_bal = executor.submit(get_questrade_balances).result()
            ib_bal = executor.submit(get_ibkr_balances).result()
        return app.response_class(response=str(qt_bal.get('equity', 0) + ib_bal.get('equity', 0)), status=200, mimetype='text/plain')
    except Exception:
        return app.response_class(response="0", status=500, mimetype='text/plain')


# =========================================================
# 5. ENRICHED OMS POSITION BOOK & EXECUTION GATES
# =========================================================
def enrich_positions_with_managed_metadata(raw_positions: list, broker: str, account_id: str) -> list:
    if not raw_positions:
        return []
    try:
        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("""
                SELECT lot_id, account_id, ticker, strategy_id, agent_id, side, qty, entry_price, 
                       mfe_target, mae_stop, tp_price, sl_price, horizon_sec, 
                       force_eod_flat, eod_cutoff, trail_to_breakeven, trailing_status,
                       EXTRACT(EPOCH FROM entry_time) as entry_epoch
                FROM oms_managed_trades
                WHERE broker = %s AND account_id = %s AND is_active = TRUE
                ORDER BY entry_time ASC;
            """, (broker, account_id))
            rows = cur.fetchall()
            cur.close()
            conn.close()

        managed_lots_by_ticker = defaultdict(list)
        for r in rows:
            managed_lots_by_ticker[r['ticker'].upper()].append(dict(r))

        now_ts = time.time()
        enriched = []

        for p in raw_positions:
            sym = p['symbol'].upper()
            pos_dict = dict(p)

            if sym in managed_lots_by_ticker:
                lots = managed_lots_by_ticker[sym]
                primary_lot = lots[0]
                elapsed = max(0, int(now_ts - float(primary_lot['entry_epoch'])))
                rem = max(0, int(primary_lot['horizon_sec'] - elapsed))

                pos_dict.update({
                    "is_managed": True,
                    "account_id": account_id,
                    "active_lots_count": len(lots),
                    "primary_lot_id": primary_lot['lot_id'],
                    "strategy_id": primary_lot['strategy_id'],
                    "agent_id": primary_lot['agent_id'],
                    "active_mfe": float(primary_lot['mfe_target']),
                    "active_mae": float(primary_lot['mae_stop']),
                    "tp_price": float(primary_lot['tp_price']) if primary_lot['tp_price'] else None,
                    "sl_price": float(primary_lot['sl_price']) if primary_lot['sl_price'] else None,
                    "horizon_sec": int(primary_lot['horizon_sec']),
                    "elapsed_sec": elapsed,
                    "time_remaining_sec": rem,
                    "force_eod_flat": bool(primary_lot['force_eod_flat']),
                    "eod_cutoff": primary_lot.get('eod_cutoff', '15:45'),
                    "trailing_status": primary_lot['trailing_status'],
                    "entry_time": datetime.fromtimestamp(primary_lot['entry_epoch'], tz=MARKET_TZ).isoformat()
                })
            else:
                pos_dict.update({
                    "is_managed": False,
                    "account_id": account_id,
                    "active_lots_count": 0,
                    "strategy_id": "untracked",
                    "active_mfe": 0.025,
                    "active_mae": -0.018,
                    "horizon_sec": 259200,
                    "time_remaining_sec": 259200,
                    "force_eod_flat": False,
                    "eod_cutoff": "none",
                    "trailing_status": "INITIAL"
                })
            enriched.append(pos_dict)
        return enriched
    except Exception as e:
        print(f"[OMS] Error enriching position book: {e}")
        return raw_positions


@app.route('/api/oms/positions', methods=['GET'])
@require_apikey
def oms_positions():
    broker = request.args.get('broker', '').lower()
    account_id = request.args.get('account', 'alpaca_macro')

    if broker == 'alpaca':
        raw_pos = get_alpaca_positions_for_account(account_id)
    elif broker == 'ibkr':
        raw_pos = get_ibkr_positions()
        account_id = "ibkr_primary"
    elif broker == 'questrade':
        raw_pos = get_questrade_positions()
        account_id = "questrade_primary"
    else:
        raw_pos = []

    enriched = enrich_positions_with_managed_metadata(raw_pos, broker, account_id)
    return jsonify({"status": "success", "broker": broker, "account_id": account_id, "positions": enriched}), 200


@app.route('/api/oms/balances', methods=['GET'])
@require_apikey
def oms_balances():
    broker = request.args.get('broker', '').lower()
    account_id = request.args.get('account', 'alpaca_macro')

    if broker == 'alpaca':
        bal = get_alpaca_balances_for_account(account_id)
    elif broker == 'ibkr':
        bal = get_ibkr_balances()
    elif broker == 'questrade':
        bal = get_questrade_balances()
    else:
        bal = {'cash': 0, 'buyingPower': 0, 'equity': 0, 'last_equity': 0}

    return jsonify({"status": "success", "broker": broker, "account_id": account_id, "balances": bal}), 200


@app.route('/api/open_orders', methods=['GET'])
@require_apikey
def get_open_orders():
    ticker = request.args.get('ticker')
    account_id = request.args.get('account', 'alpaca_macro')
    open_orders = []

    client = get_alpaca_client(account_id)
    if client:
        try:
            req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
            if ticker:
                req.symbols = [ticker]
            alpaca_orders = client.get_orders(req)
            for o in alpaca_orders:
                open_orders.append({
                    "broker": "alpaca",
                    "account_id": account_id,
                    "order_id": str(o.id),
                    "ticker": o.symbol,
                    "side": str(o.side.value if hasattr(o.side, 'value') else o.side).upper(),
                    "qty": float(o.qty or 0),
                    "limit_price": float(o.limit_price or 0)
                })
        except Exception as e:
            print(f"[OMS] ⚠️ Failed to fetch Alpaca open orders: {e}")

    try:
        IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
        res = requests.get(f"{IB_GATEWAY_URL}/iserver/account/orders", verify=False, timeout=2)
        if res.status_code == 200:
            for o in res.json().get('orders', []):
                if ticker and o.get('ticker') != ticker:
                    continue
                open_orders.append({
                    "broker": "ibkr",
                    "account_id": "ibkr_primary",
                    "order_id": str(o.get('orderId')),
                    "ticker": o.get('ticker'),
                    "side": o.get('side'), 
                    "qty": float(o.get('remainingQuantity', 0)),
                    "limit_price": float(o.get('price', 0))
                })
    except Exception:
        pass

    return jsonify({"status": "success", "orders": open_orders}), 200


@app.route('/api/kill_switch', methods=['POST'])
@require_apikey
def kill_switch():
    global GLOBAL_TRADING_HALTED
    data = request.json or {}
    halt = data.get('halt', True)
    GLOBAL_TRADING_HALTED = halt
    msg = "🛑 GLOBAL KILL SWITCH ENGAGED." if halt else "🟢 GLOBAL TRADING RESUMED."
    print(f"\n[OMS] {msg}\n")
    send_oms_telegram_alert(f"<b>[GATEWAY OMS]</b> {msg}")
    return jsonify({"status": "success", "global_halt": GLOBAL_TRADING_HALTED}), 200


@app.route('/api/order', methods=['POST'])
@require_apikey
def place_order():
    if GLOBAL_TRADING_HALTED:
        return jsonify({"status": "rejected", "message": "Global Kill Switch is Active"}), 403

    data = request.json or {}
    broker = data.get('broker', 'shadow').lower()
    ticker = data.get('ticker', '').upper()
    side = data.get('side', '').upper()
    qty = float(data.get('qty', 0))
    limit_price = float(data.get('limit_price', 0))
    agent_id = data.get('agent_id', 'macro_fleet')
    strategy_id = data.get('strategy_id', 'macro_ohlcv')

    account_id = data.get('account_id')
    if not account_id:
        if strategy_id == 'intraday_power_hour':
            account_id = 'alpaca_paper'
        elif strategy_id == 'intraday_midday_reversion' and 'alpaca_midday' in ALPACA_ACCOUNTS:
            account_id = 'alpaca_midday'
        else:
            account_id = 'alpaca_macro'

    mfe_target = float(data.get('mfe_target', 0.0075 if strategy_id == 'intraday_power_hour' else 0.025))
    mae_stop = float(data.get('mae_stop', -0.0045 if strategy_id == 'intraday_power_hour' else -0.018))
    horizon_sec = int(data.get('horizon_sec', 3000 if strategy_id == 'intraday_power_hour' else 259200))
    force_eod_flat = bool(data.get('force_eod_flat', strategy_id in ['intraday_power_hour', 'intraday_midday_reversion']))
    eod_cutoff = str(data.get('eod_cutoff', '15:55' if strategy_id == 'intraday_power_hour' else '15:45'))
    trail_to_breakeven = bool(data.get('trail_to_breakeven', True))

    tp_price = round(limit_price * (1.0 + mfe_target), 2)
    sl_price = round(limit_price * (1.0 + mae_stop), 2)

    if not all([ticker, side, qty, limit_price]):
        return jsonify({"status": "error", "message": "Missing required parameters"}), 400

    lock_key = f"{account_id}:{ticker}"
    with ticker_locks[lock_key]:

        # Strict Long-Only Gate: Deduct pending orders and check inventory
        if side == 'SELL' and broker in ['alpaca', 'ibkr']:
            sellable_qty = get_sellable_inventory(broker, ticker, account_id)

            if sellable_qty <= 0.0:
                print(f"[OMS:{account_id}] 🛑 SHORT REJECTED: {agent_id} attempted to SELL {qty} {ticker}, but sellable inventory is 0.0.")
                return jsonify({
                    "status": "rejected",
                    "reason": "ZERO_SELLABLE_INVENTORY",
                    "message": f"Cannot execute SELL for {ticker}. No uncommitted long shares exist on {account_id}."
                }), 422

            if qty > sellable_qty:
                print(f"[OMS:{account_id}] ⚠️ QUANTITY CLAMPED: Requested SELL {qty} {ticker}, but only {sellable_qty} available. Clamping to {sellable_qty}.")
                qty = sellable_qty

        if broker == 'ibkr' and side == 'BUY':
            balances = get_ibkr_balances()
            current_bp = balances.get('buyingPower', 0)
            estimated_cost = qty * limit_price
            if estimated_cost > (current_bp - 2000):
                return jsonify({"status": "rejected", "reason": "GLOBAL_MARGIN_LIMIT"}), 403

        order_id = f"sim_{int(time.time())}"
        lot_id = f"lot_{ticker}_{int(time.time()*1000)}"
        filled_immediately = False
        actual_entry_price = limit_price

        if broker == 'alpaca':
            client = get_alpaca_client(account_id)
            if not client:
                return jsonify({"status": "error", "message": f"Alpaca account '{account_id}' not configured"}), 500
            try:
                order_side = OrderSide.BUY if side == 'BUY' else OrderSide.SELL
                limit_order = LimitOrderRequest(
                    symbol=ticker, 
                    qty=qty, 
                    side=order_side, 
                    time_in_force=TimeInForce.DAY,
                    limit_price=round(limit_price, 2),
                    extended_hours=True
                )
                order = client.submit_order(order_data=limit_order)
                order_id = str(order.id)
                print(f"[OMS:{account_id}] 🦙 ALPACA ORDER PLACED: {side} {qty} {ticker} @ ${limit_price:.2f} (ID: {order_id})")

                # Fast-poll for immediate execution
                for _ in range(5):
                    time.sleep(0.5)
                    live_o = client.get_order_by_id(order_id)
                    status_val = live_o.status.value if hasattr(live_o.status, 'value') else str(live_o.status)
                    if status_val == 'filled':
                        filled_immediately = True
                        actual_entry_price = float(live_o.filled_avg_price or limit_price)
                        qty = float(live_o.filled_qty or qty)
                        break
                    elif status_val in ['canceled', 'expired', 'rejected']:
                        return jsonify({"status": "rejected", "reason": f"Order {status_val}"}), 400

            except Exception as e:
                print(f"[OMS:{account_id}] ❌ ALPACA ROUTING FAILED: {e}")
                return jsonify({"status": "error", "message": str(e)}), 500

        elif broker == 'ibkr':
            try:
                IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
                acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=2)
                account_num = acct_res.json()[0].get('accountId')
                ibkr_payload = {
                    "orders": [{
                        "cOID": lot_id, 
                        "ticker": ticker,
                        "orderType": "LMT",
                        "price": round(limit_price, 2),
                        "side": side,
                        "quantity": qty,
                        "tif": "DAY",
                        "outsideRTH": True
                    }]
                }
                order_res = requests.post(f"{IB_GATEWAY_URL}/iserver/account/{account_num}/orders", json=ibkr_payload, verify=False, timeout=2)
                order_res.raise_for_status()
                order_id = str(order_res.json())
                filled_immediately = True
            except Exception as e:
                print(f"[OMS] ❌ IBKR ROUTING FAILED: {e}")
                return jsonify({"status": "error", "message": str(e)}), 500

        elif broker == 'shadow':
            filled_immediately = True

        # Database Ledger Sync
        try:
            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                if side == 'BUY':
                    cur.execute("""
                        INSERT INTO oms_managed_trades 
                        (lot_id, account_id, ticker, broker, agent_id, strategy_id, side, qty, entry_price, 
                         mfe_target, mae_stop, tp_price, sl_price, horizon_sec, force_eod_flat, eod_cutoff,
                         trail_to_breakeven, trailing_status, entry_time, is_active, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'INITIAL', NOW(), %s, NOW());
                    """, (lot_id, account_id, ticker, broker, agent_id, strategy_id, side, qty, actual_entry_price,
                          mfe_target, mae_stop, tp_price, sl_price, horizon_sec, force_eod_flat, eod_cutoff, trail_to_breakeven,
                          filled_immediately))
                else:
                    rem_to_deduct = qty
                    cur.execute("""
                        SELECT id, qty FROM oms_managed_trades 
                        WHERE ticker = %s AND account_id = %s AND is_active = TRUE 
                        ORDER BY entry_time ASC;
                    """, (ticker, account_id))
                    active_lots = cur.fetchall()

                    for l_id, l_qty in active_lots:
                        l_qty = float(l_qty)
                        if rem_to_deduct >= l_qty:
                            cur.execute("UPDATE oms_managed_trades SET is_active = FALSE, updated_at = NOW() WHERE id = %s;", (l_id,))
                            rem_to_deduct -= l_qty
                        else:
                            cur.execute("UPDATE oms_managed_trades SET qty = %s, updated_at = NOW() WHERE id = %s;", (l_qty - rem_to_deduct, l_id))
                            rem_to_deduct = 0
                            break

                conn.commit()
                cur.close()
                conn.close()
        except Exception as e:
            print(f"[OMS] Database ledger sync error: {e}")

        status_str = "filled" if filled_immediately else "submitted"
        return jsonify({
            "status": "success", 
            "execution": status_str,
            "broker": broker, 
            "account_id": account_id,
            "order_id": order_id, 
            "lot_id": lot_id
        }), 200


@app.route('/api/cancel', methods=['POST'])
@require_apikey
def cancel_order():
    data = request.json or {}
    broker = data.get('broker', '').lower()
    account_id = data.get('account', 'alpaca_macro')
    order_id = data.get('order_id')
    if not broker or not order_id:
        return jsonify({"status": "error", "message": "Missing broker or order_id"}), 400

    if broker == 'alpaca':
        client = get_alpaca_client(account_id)
        if client:
            try:
                client.cancel_order_by_id(order_id)
                return jsonify({"status": "success", "message": "Order canceled."}), 200
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500

    elif broker == 'ibkr':
        try:
            IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
            acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=2)
            account_num = acct_res.json()[0].get('accountId')
            res = requests.delete(f"{IB_GATEWAY_URL}/iserver/account/{account_num}/order/{order_id}", verify=False, timeout=2)
            res.raise_for_status()
            return jsonify({"status": "success", "message": "Order canceled."}), 200
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    return jsonify({"status": "error", "message": "Invalid broker."}), 400


@app.route('/api/flatten', methods=['POST'])
@require_apikey
def flatten_ticker():
    data = request.json or {}
    broker = data.get('broker', '').lower()
    account_id = data.get('account', 'alpaca_macro')
    ticker = data.get('ticker', '').upper()
    qty = float(data.get('qty', 0))

    if not ticker:
        return jsonify({"status": "error", "message": "Ticker is required."}), 400

    lock_key = f"{account_id}:{ticker}"
    with ticker_locks[lock_key]:
        sellable = get_sellable_inventory(broker, ticker, account_id)
        if sellable <= 0.0:
            return jsonify({"status": "rejected", "message": f"{ticker} has no uncommitted long shares to flatten on {account_id}."}), 422

        actual_qty = min(qty, sellable) if qty > 0 else sellable
        print(f"[OMS:{account_id}] ⚠️ FLATTEN: Executing market exit for {actual_qty} shares of {ticker} on {broker.upper()}")

        try:
            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("UPDATE oms_managed_trades SET is_active = FALSE, updated_at = NOW() WHERE ticker = %s AND account_id = %s;", (ticker, account_id))
                conn.commit()
                cur.close()
                conn.close()
        except Exception as e:
            print(f"[OMS] Database error during flatten: {e}")

        if broker == 'alpaca':
            client = get_alpaca_client(account_id)
            if client:
                try:
                    client.submit_order(MarketOrderRequest(
                        symbol=ticker, 
                        qty=actual_qty, 
                        side=OrderSide.SELL, 
                        time_in_force=TimeInForce.DAY
                    ))
                    return jsonify({"status": "success", "message": f"{actual_qty} shares of {ticker} flattened on Alpaca ({account_id})."}), 200
                except Exception as e:
                    return jsonify({"status": "error", "message": str(e)}), 500

        elif broker == 'ibkr':
            try:
                IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
                acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=2)
                account_num = acct_res.json()[0].get('accountId')
                ibkr_payload = {
                    "orders": [{
                        "cOID": f"FLATTEN_{ticker}_{int(time.time())}", 
                        "ticker": ticker,
                        "orderType": "MKT", 
                        "side": "SELL", 
                        "quantity": actual_qty, 
                        "tif": "DAY", 
                        "outsideRTH": True
                    }]
                }
                res = requests.post(f"{IB_GATEWAY_URL}/iserver/account/{account_num}/orders", json=ibkr_payload, verify=False, timeout=2)
                res.raise_for_status()
                return jsonify({"status": "success", "message": f"{actual_qty} shares of {ticker} flattened on IBKR."}), 200
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500

    return jsonify({"status": "error", "message": "Invalid broker."}), 400


# =========================================================
# 6. AUTONOMOUS 24/7 TRADE LIFECYCLE MONITOR DAEMON
# =========================================================
def trade_lifecycle_daemon():
    """Independent background worker for time expiration, stops, and dynamic EOD exits."""
    global GLOBAL_TRADING_HALTED
    print("[*] Trade Lifecycle Monitor Daemon online (Dynamic EOD Cutoffs Active).")

    last_circuit_breaker_check = 0.0

    while True:
        try:
            time.sleep(4)
            now_ts = time.time()
            now_edt = datetime.now(MARKET_TZ)
            current_time_str = now_edt.strftime("%H:%M")

            # Daily Drawdown Circuit Breaker Check (-3.0%)
            if now_ts - last_circuit_breaker_check >= 60.0 and not GLOBAL_TRADING_HALTED:
                last_circuit_breaker_check = now_ts
                for acct_id, client in ALPACA_ACCOUNTS.items():
                    try:
                        acct_info = client.get_account()
                        eq = float(acct_info.equity)
                        last_eq = float(acct_info.last_equity)
                        if last_eq > 0:
                            daily_dd = (eq - last_eq) / last_eq
                            if daily_dd <= -0.03:
                                GLOBAL_TRADING_HALTED = True
                                alert_msg = (
                                    f"🚨 <b>[AUTOMATIC CIRCUIT BREAKER TRIPPED]</b>\n"
                                    f"Account <code>{acct_id}</code> breached -3.0% daily drawdown: <b>{daily_dd*100:.2f}%</b>\n"
                                    f"All fleet order entry is now <b>HALTED</b>."
                                )
                                print(f"[OMS] {alert_msg}")
                                send_oms_telegram_alert(alert_msg)
                                break
                    except Exception:
                        pass

            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cur.execute("""
                    SELECT id, lot_id, account_id, ticker, broker, strategy_id, qty, entry_price, mfe_target, mae_stop, 
                           horizon_sec, force_eod_flat, eod_cutoff, trail_to_breakeven, trailing_status,
                           EXTRACT(EPOCH FROM entry_time) as entry_epoch
                    FROM oms_managed_trades
                    WHERE is_active = TRUE
                    ORDER BY entry_time ASC;
                """)
                active_trades = cur.fetchall()
                cur.close()
                conn.close()

            if not active_trades:
                continue

            account_positions_cache = {}
            for acct_id, client in ALPACA_ACCOUNTS.items():
                try:
                    account_positions_cache[acct_id] = {p.symbol.upper(): p for p in client.get_all_positions()}
                except Exception:
                    account_positions_cache[acct_id] = {}

            for t in active_trades:
                trade_id = t['id']
                sym = t['ticker'].upper()
                broker = t['broker'].lower()
                acct_id = t['account_id']
                strat_id = t['strategy_id']
                lot_qty = float(t['qty'])
                entry_px = float(t['entry_price'])
                mfe = float(t['mfe_target'])
                mae = float(t['mae_stop'])
                horizon = int(t['horizon_sec'])
                elapsed = now_ts - float(t['entry_epoch'])
                trailing_status = t['trailing_status']

                # Dynamic per-trade EOD Cutoff calculation
                cutoff_str = t.get('eod_cutoff') or ('15:55' if strat_id == 'intraday_power_hour' else '15:45')

                if broker == 'alpaca':
                    client = get_alpaca_client(acct_id)
                    cached_pos = account_positions_cache.get(acct_id, {})

                    if sym not in cached_pos:
                        with db_lock:
                            conn = get_db_connection()
                            cur = conn.cursor()
                            cur.execute("UPDATE oms_managed_trades SET is_active = FALSE, updated_at = NOW() WHERE id = %s;", (trade_id,))
                            conn.commit()
                            cur.close()
                            conn.close()
                        continue

                    pos = cached_pos[sym]
                    cur_px = float(pos.current_price)
                    pnl_pct = (cur_px - entry_px) / (entry_px + 1e-8)
                    lock_key = f"{acct_id}:{sym}"

                    # 1. Dynamic Forced EOD Flat Barrier (15:55 for Power Hour, 15:45 for Midday)
                    if t['force_eod_flat'] and current_time_str >= cutoff_str:
                        with ticker_locks[lock_key]:
                            sellable = get_sellable_inventory('alpaca', sym, acct_id)
                            exit_qty = min(lot_qty, sellable)
                            if exit_qty > 0 and client:
                                print(f"[OMS:{acct_id}] ⏱️ EOD CUTOFF ({cutoff_str} EDT): Exiting {exit_qty} {sym} @ ${cur_px:.2f} (PnL: {pnl_pct*100:+.2f}%)")
                                send_oms_telegram_alert(f"<b>[OMS EOD CUTOFF]</b> Flattened {exit_qty} {sym} ({acct_id} | {strat_id}) @ ${cur_px:.2f} (PnL: {pnl_pct*100:+.2f}%)")
                                client.submit_order(MarketOrderRequest(symbol=sym, qty=exit_qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY))

                            with db_lock:
                                conn = get_db_connection()
                                cur = conn.cursor()
                                cur.execute("UPDATE oms_managed_trades SET is_active = FALSE, updated_at = NOW() WHERE id = %s;", (trade_id,))
                                conn.commit()
                                cur.close()
                                conn.close()
                        continue

                    # 2. Holding Horizon Expiration
                    if elapsed >= horizon:
                        with ticker_locks[lock_key]:
                            sellable = get_sellable_inventory('alpaca', sym, acct_id)
                            exit_qty = min(lot_qty, sellable)
                            if exit_qty > 0 and client:
                                h_str = f"{int(elapsed//3600)}h" if elapsed >= 3600 else f"{int(elapsed//60)}m"
                                print(f"[OMS:{acct_id}] ⌛ HORIZON EXPIRED: Exiting {exit_qty} {sym} after {h_str} @ ${cur_px:.2f} (PnL: {pnl_pct*100:+.2f}%)")
                                send_oms_telegram_alert(f"<b>[OMS HORIZON EXPIRED]</b> Flattened {exit_qty} {sym} ({acct_id}) after {h_str} @ ${cur_px:.2f} (PnL: {pnl_pct*100:+.2f}%)")
                                client.submit_order(MarketOrderRequest(symbol=sym, qty=exit_qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY))

                            with db_lock:
                                conn = get_db_connection()
                                cur = conn.cursor()
                                cur.execute("UPDATE oms_managed_trades SET is_active = FALSE, updated_at = NOW() WHERE id = %s;", (trade_id,))
                                conn.commit()
                                cur.close()
                                conn.close()
                        continue

                    # 3. Stop-Loss Barrier
                    if pnl_pct <= mae:
                        reason = "BREAKEVEN STOP" if mae >= 0 else "STOP LOSS"
                        with ticker_locks[lock_key]:
                            sellable = get_sellable_inventory('alpaca', sym, acct_id)
                            exit_qty = min(lot_qty, sellable)
                            if exit_qty > 0 and client:
                                print(f"[OMS:{acct_id}] 🛑 {reason}: Exiting {exit_qty} {sym} @ ${cur_px:.2f} ({pnl_pct*100:.2f}%)")
                                send_oms_telegram_alert(f"<b>[OMS {reason}]</b> Sold {exit_qty} {sym} ({acct_id}) @ ${cur_px:.2f} ({pnl_pct*100:.2f}%)")
                                client.submit_order(MarketOrderRequest(symbol=sym, qty=exit_qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY))

                            with db_lock:
                                conn = get_db_connection()
                                cur = conn.cursor()
                                cur.execute("UPDATE oms_managed_trades SET is_active = FALSE, updated_at = NOW() WHERE id = %s;", (trade_id,))
                                conn.commit()
                                cur.close()
                                conn.close()
                        continue

                    # 4. Take-Profit & Smart Partial Ratchet
                    if pnl_pct >= mfe:
                        with ticker_locks[lock_key]:
                            if t['trail_to_breakeven'] and trailing_status == 'INITIAL' and lot_qty > 1:
                                half_qty = int(lot_qty // 2)
                                remaining_qty = lot_qty - half_qty
                                sellable = get_sellable_inventory('alpaca', sym, acct_id)
                                actual_partial = min(half_qty, sellable)

                                if actual_partial > 0 and client:
                                    print(f"[OMS:{acct_id}] 🎯 SMART PARTIAL (+{pnl_pct*100:.2f}%): Selling {actual_partial} {sym}, trailing {remaining_qty} to Breakeven (+0.1%).")
                                    send_oms_telegram_alert(
                                        f"<b>[OMS SMART PARTIAL]</b> Sold {actual_partial} {sym} ({acct_id}) @ ${cur_px:.2f} (+{pnl_pct*100:.2f}%).\n"
                                        f"Trailing remaining {remaining_qty} shares with stop at Breakeven (+0.1%)."
                                    )
                                    client.submit_order(MarketOrderRequest(symbol=sym, qty=actual_partial, side=OrderSide.SELL, time_in_force=TimeInForce.DAY))

                                with db_lock:
                                    conn = get_db_connection()
                                    cur = conn.cursor()
                                    cur.execute("""
                                        UPDATE oms_managed_trades 
                                        SET qty = %s, mae_stop = 0.0010, mfe_target = %s, 
                                            trailing_status = 'BREAKEVEN_LOCKED', updated_at = NOW() 
                                        WHERE id = %s;
                                    """, (remaining_qty, mfe * 1.5, trade_id))
                                    conn.commit()
                                    cur.close()
                                    conn.close()
                            else:
                                sellable = get_sellable_inventory('alpaca', sym, acct_id)
                                exit_qty = min(lot_qty, sellable)
                                if exit_qty > 0 and client:
                                    print(f"[OMS:{acct_id}] 🎯 FULL TAKE PROFIT (+{pnl_pct*100:.2f}%): Exiting {exit_qty} {sym} @ ${cur_px:.2f}")
                                    send_oms_telegram_alert(f"<b>[OMS TAKE PROFIT]</b> Closed {exit_qty} {sym} ({acct_id}) @ ${cur_px:.2f} (+{pnl_pct*100:.2f}%)")
                                    client.submit_order(MarketOrderRequest(symbol=sym, qty=exit_qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY))

                                with db_lock:
                                    conn = get_db_connection()
                                    cur = conn.cursor()
                                    cur.execute("UPDATE oms_managed_trades SET is_active = FALSE, updated_at = NOW() WHERE id = %s;", (trade_id,))
                                    conn.commit()
                                    cur.close()
                                    conn.close()

        except Exception as e:
            print(f"[OMS Lifecycle Monitor] Error: {e}")


# =========================================================
# 7. EMBEDDED MULTI-BROKER TRADE STREAM LISTENERS
# =========================================================
@dataclass
class UnifiedTradeEvent:
    broker: str
    account_id: str
    ticker: str
    event_type: str
    side: str
    qty: float
    fill_price: float
    client_order_id: str
    broker_order_id: str


async def process_unified_event(event: UnifiedTradeEvent):
    """Reconciles live WebSocket execution reports straight to PostgreSQL."""
    print(f"[{event.broker.upper()}:{event.account_id}] ⚡ WS {event.event_type} | {event.side} {event.qty} {event.ticker} @ ${event.fill_price:.2f}")

    if event.event_type in ['FILL', 'PARTIAL_FILL']:
        try:
            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE oms_managed_trades 
                    SET entry_price = %s,
                        qty = %s,
                        is_active = TRUE,
                        entry_time = NOW(),
                        updated_at = NOW()
                    WHERE (lot_id = %s OR ticker = %s) 
                      AND account_id = %s 
                      AND is_active = TRUE;
                """, (event.fill_price, event.qty, event.client_order_id, event.ticker, event.account_id))
                conn.commit()
                cur.close()
                conn.close()
            print(f"[{event.broker.upper()}:{event.account_id}] [✓] Reconciled fill in PostgreSQL for {event.ticker}.")
        except Exception as e:
            print(f"[WS Error] DB sync failed: {e}")

    elif event.event_type in ['CANCELED', 'REJECTED']:
        try:
            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE oms_managed_trades 
                    SET is_active = FALSE, updated_at = NOW()
                    WHERE (lot_id = %s OR ticker = %s) 
                      AND account_id = %s;
                """, (event.client_order_id, event.ticker, event.account_id))
                conn.commit()
                cur.close()
                conn.close()
        except Exception as e:
            print(f"[WS Error] DB cancel sync failed: {e}")


async def run_alpaca_stream(account_id: str, key: str, secret: str, paper: bool):
    while True:
        try:
            print(f"[*] [ALPACA:{account_id}] Connecting Trade WebSocket (Paper={paper})...")
            stream = TradingStream(key, secret, paper=paper)

            async def handle_alpaca(data):
                order = data.order
                ev = data.event.upper()
                normalized = UnifiedTradeEvent(
                    broker="alpaca",
                    account_id=account_id,
                    ticker=order['symbol'].upper(),
                    event_type=ev,
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
            print(f"[!] [ALPACA:{account_id}] Socket disconnected: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


async def run_ibkr_stream(account_id: str):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    while True:
        try:
            print(f"[*] [IBKR:{account_id}] Connecting WebSocket to {IB_WS_URL}...")
            async with websockets.connect(IB_WS_URL, ssl=ssl_context) as ws:
                print(f"[✓] [IBKR:{account_id}] Gateway WebSocket connected.")
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
                        if raw_msg.startswith("ech+"):
                            continue
                        topic = raw_msg[:3]
                        payload_str = raw_msg[4:] if len(raw_msg) > 4 and raw_msg[3] == '+' else raw_msg
                        try:
                            data = json.loads(payload_str)
                        except Exception:
                            continue

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
                finally:
                    hb_task.cancel()
        except Exception as e:
            print(f"[!] [IBKR:{account_id}] Socket disconnected: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


def start_embedded_trade_streams():
    """Background worker thread that runs the asyncio stream loops."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    tasks = []

    # 1. Macro Swings Account
    if k_macro and s_macro:
        tasks.append(run_alpaca_stream("alpaca_macro", k_macro, s_macro, paper=True))

    # 2. Midday Reversion Account (if configured with distinct credentials)
    if k_midday and s_midday and k_midday != k_macro:
        tasks.append(run_alpaca_stream("alpaca_midday", k_midday, s_midday, paper=True))

    # 3. Dedicated Power Hour Account (if configured with distinct credentials)
    if k_power and s_power and k_power not in [k_macro, k_midday]:
        tasks.append(run_alpaca_stream("alpaca_power_hour", k_power, s_power, paper=True))

    # 4. IBKR Gateway Account
    ib_acct = os.environ.get("IBKR_ACCOUNT_ID", "DU1234567")
    tasks.append(run_ibkr_stream(ib_acct))

    print(f"[*] Embedded Trade Stream Listener active ({len(tasks)} broker pipelines).")
    loop.run_until_complete(asyncio.gather(*tasks))


# =========================================================
# SERVER ENTRYPOINT
# =========================================================
if __name__ == '__main__':
    init_oms_db()
    reconcile_cold_boot_state()

    # 1. 24/7 Stop / Target / Horizon Monitor Daemon
    threading.Thread(target=trade_lifecycle_daemon, daemon=True).start()

    # 2. Embedded Multi-Broker WebSocket Streams Daemon
    threading.Thread(target=start_embedded_trade_streams, daemon=True).start()

    active_alpaca_str = ", ".join(ALPACA_ACCOUNTS.keys()) if ALPACA_ACCOUNTS else "None"
    print(f"🚀 Execution Gateway & Lifecycle OMS listening on http://0.0.0.0:{PORT}")
    print(f"💼 Configured Alpaca Accounts: [{active_alpaca_str}]")
    print(f"🔗 Enriched Positions: http://localhost:{PORT}/api/oms/positions?broker=alpaca&account=alpaca_macro")
    print(f"🔒 OMS Routing: PORT {PORT} | Protected via Tailnet X-Gateway-Token")
    app.run(host='0.0.0.0', port=PORT)
