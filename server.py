import os
import time
import requests
import psycopg2
from functools import wraps
from flask import Flask, jsonify, request
from flask_cors import CORS
import urllib3
import concurrent.futures
from vault import load_secrets

# Alpaca routing dependencies
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus

# Suppress insecure request warnings for IBKR local gateway's self-signed cert
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
# CRITICAL: Enables your web UI to fetch data from this API
CORS(app, max_age=86400)

load_secrets()

PORT = int(os.environ.get("PORT", 3000))
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://username:password@localhost:5432/your_database")

# ==========================================
# GATEWAY SECURITY & GLOBAL STATE
# ==========================================
GATEWAY_API_KEY = os.environ.get("GATEWAY_API_KEY", "your_super_secret_tailnet_key_2026")
GLOBAL_TRADING_HALTED = False

# Initialize Alpaca Client securely on the SERVER
ALPACA_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET_KEY")
alpaca_client = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True) if ALPACA_KEY else None

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def require_apikey(f):
    """
    Flask decorator to enforce Gateway API Key authorization over the Tailnet.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        provided_key = request.headers.get('X-Gateway-Token')
        if not provided_key or provided_key != GATEWAY_API_KEY:
            print(f"[OMS] 🚨 UNAUTHORIZED ATTEMPT BLOCKED FROM IP: {request.remote_addr} | Target: {request.path}")
            return jsonify({"status": "error", "message": "Unauthorized. Invalid Gateway Token."}), 401
        return f(*args, **kwargs)
    return decorated_function


# =========================================================
# 1. QUESTRADE INTEGRATION (Full OAuth Lifecycle)
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
            print("❌ Questrade Error: No 'qt_refresh_token' found in system_secrets table.")
            cur.close()
            conn.close()
            return False
            
        refresh_token = result[0]
        print("🔄 Exchanging Questrade refresh token...")
        url = f"https://login.questrade.com/oauth2/token?grant_type=refresh_token&refresh_token={refresh_token}"
        
        response = requests.get(url)
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
        
        print("✅ Questrade token refreshed successfully!")
        return True

    except psycopg2.Error as e:
        print(f"❌ Database Error: {e}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to refresh Questrade token: {e}")
        return False

def get_questrade_positions(is_retry=False):
    global qt_access_token, qt_api_server
    if not qt_access_token:
        if not refresh_questrade_token():
            return []

    try:
        headers = {'Authorization': f'Bearer {qt_access_token}'}
        acct_res = requests.get(f"{qt_api_server}v1/accounts", headers=headers)
        acct_res.raise_for_status()
        accounts = acct_res.json().get('accounts', [])
        
        if not accounts: return []
        account_id = accounts[0]['number']

        pos_res = requests.get(f"{qt_api_server}v1/accounts/{account_id}/positions", headers=headers)
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
            print("⚠️ Questrade token expired during request. Refreshing and retrying...")
            qt_access_token = '' 
            return get_questrade_positions(is_retry=True)
        print(f"❌ Questrade Error fetching positions: {e}")
        return []
    except Exception as e:
        print(f"❌ Questrade Error: {e}")
        return []

def get_questrade_balances(is_retry=False):
    global qt_access_token, qt_api_server
    if not qt_access_token:
        if not refresh_questrade_token():
            return {'cash': 0, 'buyingPower': 0, 'equity': 0}

    try:
        headers = {'Authorization': f'Bearer {qt_access_token}'}
        acct_res = requests.get(f"{qt_api_server}v1/accounts", headers=headers)
        acct_res.raise_for_status()
        accounts = acct_res.json().get('accounts', [])
        
        if not accounts: return {'cash': 0, 'buyingPower': 0, 'equity': 0}
        account_id = accounts[0]['number']

        bal_res = requests.get(f"{qt_api_server}v1/accounts/{account_id}/balances", headers=headers)
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
        print(f"❌ Questrade Error fetching balances: {e}")
        return {'cash': 0, 'buyingPower': 0, 'equity': 0}
    except Exception as e:
        print(f"❌ Questrade Error: {e}")
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
        
        if not accounts: return []
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
            except Exception as e:
                print(f"⚠️ IBKR Error fetching positions for account {account_id}: {e}")
                continue 
                
        return all_positions

    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return []
    except Exception as e:
        print(f"❌ IBKR Critical Error: {e}")
        return []

def get_ibkr_balances():
    try:
        IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
        acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=2)
        acct_res.raise_for_status()
        accounts = acct_res.json()
        
        if not accounts: return {'cash': 0, 'buyingPower': 0, 'equity': 0}
            
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
            except Exception as e:
                print(f"⚠️ IBKR Error fetching balances for account {account_id}: {e}")
                continue

        return {
            'cash': total_cash,
            'buyingPower': total_bp,
            'equity': total_equity
        }

    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return {'cash': 0, 'buyingPower': 0, 'equity': 0}
    except Exception as e:
        print(f"❌ IBKR Balances Critical Error: {e}")
        return {'cash': 0, 'buyingPower': 0, 'equity': 0}


# =========================================================
# 3. READ-ONLY SYNC ROUTES (Dashboards & Spreadsheets)
# =========================================================
@app.route('/positions.json', methods=['GET'])
def positions():
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_future = executor.submit(get_questrade_positions)
            ibkr_future = executor.submit(get_ibkr_positions)
            
            qt_positions = qt_future.result()
            ibkr_positions = ibkr_future.result()

        all_positions = qt_positions + ibkr_positions
        merged_map = {}
        for p in all_positions:
            sym = p['symbol']
            if sym not in merged_map:
                merged_map[sym] = {'qty': p['qty'], 'avgPrice': p['avgPrice']}
            else:
                existing = merged_map[sym]
                new_qty = existing['qty'] + p['qty']
                if new_qty == 0: new_avg = 0
                else: new_avg = ((existing['qty'] * existing['avgPrice']) + (p['qty'] * p['avgPrice'])) / new_qty
                merged_map[sym] = {'qty': new_qty, 'avgPrice': new_avg}

        final_array = [{'symbol': sym, 'qty': data['qty'], 'avgPrice': data['avgPrice']} for sym, data in merged_map.items() if data['qty'] != 0]

        return jsonify({
            "questrade": qt_positions,
            "ibkr": ibkr_positions,
            "total": final_array
        })

    except Exception as e:
        print(f"Error generating positions: {e}")
        return jsonify({"error": "Failed to generate positions"}), 500

@app.route('/balances.json', methods=['GET'])
def balances():
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_future = executor.submit(get_questrade_balances)
            ibkr_future = executor.submit(get_ibkr_balances)
            
            qt_bal = qt_future.result()
            ib_bal = ibkr_future.result()

        return jsonify({
            "questrade": qt_bal,
            "ibkr": ib_bal,
            "total": {
                "cash": qt_bal['cash'] + ib_bal['cash'],
                "buyingPower": qt_bal['buyingPower'] + ib_bal['buyingPower']
            }
        })
    except Exception as e:
        print(f"Error generating balances: {e}")
        return jsonify({"error": "Failed to generate balances"}), 500

@app.route('/equity.json', methods=['GET'])
def equity():
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_future = executor.submit(get_questrade_balances)
            ibkr_future = executor.submit(get_ibkr_balances)
            
            qt_data = qt_future.result()
            ib_data = ibkr_future.result()

        return jsonify({
            "questrade": {"equity": qt_data['equity']},
            "ibkr": {"equity": ib_data['equity']},
            "total": {"equity": qt_data['equity'] + ib_data['equity']}
        })
    except Exception as e:
        print(f"Error generating equity: {e}")
        return jsonify({"error": "Failed to generate equity"}), 500

@app.route('/networth.csv', methods=['GET'])
def networth_csv():
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_future = executor.submit(get_questrade_balances)
            ibkr_future = executor.submit(get_ibkr_balances)
            qt_bal = qt_future.result()
            ib_bal = ibkr_future.result()

        total_eq = qt_bal.get('equity', 0) + ib_bal.get('equity', 0)
        return app.response_class(response=str(total_eq), status=200, mimetype='text/plain')
    except Exception as e:
        return app.response_class(response="0", status=500, mimetype='text/plain')


# =========================================================
# 4. EXECUTION GATEWAY (OMS - Secured Routes)
# =========================================================

@app.route('/api/kill_switch', methods=['POST'])
@require_apikey
def kill_switch():
    """
    Toggles the Global Trading Halt. If True, ALL incoming /api/order requests are instantly rejected.
    """
    global GLOBAL_TRADING_HALTED
    data = request.json
    halt = data.get('halt', True)
    
    GLOBAL_TRADING_HALTED = halt
    
    if halt:
        print("\n" + "="*50)
        print("[OMS] 🛑 GLOBAL KILL SWITCH ENGAGED. ALL NEW ORDERS BLOCKED.")
        print("="*50 + "\n")
    else:
        print("\n[OMS] 🟢 GLOBAL TRADING RESUMED.")

    return jsonify({"status": "success", "global_halt": GLOBAL_TRADING_HALTED}), 200


@app.route('/api/order', methods=['POST'])
@require_apikey
def place_order():
    """
    Receives trade commands from distributed AI agents.
    Enforces Global Risk Limits, then routes to Shadow, Alpaca, or IBKR.
    """
    if GLOBAL_TRADING_HALTED:
        print("[OMS] 🛑 ORDER REJECTED: Global Kill Switch is Active!")
        return jsonify({"status": "rejected", "message": "Global Kill Switch is Active"}), 403

    data = request.json
    broker = data.get('broker', 'shadow').lower()
    ticker = data.get('ticker')
    side = data.get('side', '').upper()
    qty = float(data.get('qty', 0))
    limit_price = float(data.get('limit_price', 0))
    agent_id = data.get('agent_id', 'unknown_agent')

    if not all([ticker, side, qty, limit_price]):
        return jsonify({"status": "error", "message": "Missing required parameters"}), 400

    print(f"\n[OMS] 🚦 RECEIVED ORDER FROM {agent_id.upper()}: {side} {qty} {ticker} @ ${limit_price:.2f} -> DEST: {broker.upper()}")

    # --- GLOBAL RISK MANAGEMENT (IBKR only for now) ---
    if broker == 'ibkr' and side == 'BUY':
        balances = get_ibkr_balances()
        current_bp = balances.get('buyingPower', 0)
        estimated_cost = qty * limit_price
        
        # Leave a $2,000 global safety buffer
        if estimated_cost > (current_bp - 2000):
            print(f"[OMS] ❌ REJECTED: Insufficient Global Buying Power. Cost: ${estimated_cost:.2f} | BP: ${current_bp:.2f}")
            return jsonify({"status": "rejected", "reason": "GLOBAL_MARGIN_LIMIT"}), 403

    # ==========================================
    # ROUTE 1: SHADOW MODE
    # ==========================================
    if broker == 'shadow':
        print(f"[OMS] 👻 SHADOW EXECUTION LOGGED: {side} {ticker}. No broker API invoked.")
        return jsonify({"status": "success", "broker": "shadow", "message": "Shadow order acknowledged."}), 200

    # ==========================================
    # ROUTE 2: ALPACA 
    # ==========================================
    elif broker == 'alpaca':
        if not alpaca_client:
            print("[OMS] ❌ REJECTED: Alpaca API keys not configured on Gateway.")
            return jsonify({"status": "error", "message": "Alpaca not configured"}), 500
            
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
            order = alpaca_client.submit_order(order_data=limit_order)
            print(f"[OMS] 🦙 ALPACA ORDER SUBMITTED: ID {order.id}")
            
            return jsonify({"status": "success", "broker": "alpaca", "order_id": str(order.id)}), 200
            
        except Exception as e:
            print(f"[OMS] ❌ ALPACA ROUTING FAILED: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    # ==========================================
    # ROUTE 3: INTERACTIVE BROKERS
    # ==========================================
    elif broker == 'ibkr':
        try:
            IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
            acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=2)
            account_id = acct_res.json()[0].get('accountId')

            ibkr_payload = {
                "orders": [{
                    "cOID": f"{agent_id}_{int(time.time())}", 
                    "ticker": ticker,
                    "orderType": "LMT",
                    "price": round(limit_price, 2),
                    "side": side,
                    "quantity": qty,
                    "tif": "DAY",
                    "outsideRTH": True
                }]
            }

            order_res = requests.post(f"{IB_GATEWAY_URL}/iserver/account/{account_id}/orders", json=ibkr_payload, verify=False, timeout=2)
            order_res.raise_for_status()
            
            print(f"[OMS] ✅ IBKR ORDER SUBMITTED: {order_res.json()}")
            return jsonify({"status": "success", "broker": "ibkr", "response": order_res.json()}), 200

        except Exception as e:
            print(f"[OMS] ❌ IBKR ROUTING FAILED: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500
            
    else:
        return jsonify({"status": "error", "message": f"Unknown broker target: {broker}"}), 400


@app.route('/api/open_orders', methods=['GET'])
@require_apikey
def get_open_orders():
    """
    Fetches all currently open (unfilled) orders across brokers.
    Accepts an optional ?ticker=XYZ parameter to filter.
    """
    ticker = request.args.get('ticker')
    open_orders = []

    # 1. ALPACA OPEN ORDERS
    if alpaca_client:
        try:
            req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
            if ticker:
                req.symbols = [ticker]
                
            alpaca_orders = alpaca_client.get_orders(req)
            for o in alpaca_orders:
                open_orders.append({
                    "broker": "alpaca",
                    "order_id": str(o.id),
                    "ticker": o.symbol,
                    "side": str(o.side.value if hasattr(o.side, 'value') else o.side).upper(),
                    "qty": float(o.qty or 0),
                    "limit_price": float(o.limit_price or 0)
                })
        except Exception as e:
            print(f"[OMS] ⚠️ Failed to fetch Alpaca open orders: {e}")

    # 2. IBKR OPEN ORDERS
    try:
        IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
        res = requests.get(f"{IB_GATEWAY_URL}/iserver/account/orders", verify=False, timeout=2)
        if res.status_code == 200:
            for o in res.json().get('orders', []):
                if ticker and o.get('ticker') != ticker:
                    continue
                open_orders.append({
                    "broker": "ibkr",
                    "order_id": str(o.get('orderId')),
                    "ticker": o.get('ticker'),
                    "side": o.get('side'), 
                    "qty": float(o.get('remainingQuantity', 0)),
                    "limit_price": float(o.get('price', 0))
                })
    except Exception as e:
        print(f"[OMS] ⚠️ Failed to fetch IBKR open orders: {e}")

    return jsonify({"status": "success", "orders": open_orders}), 200


@app.route('/api/cancel', methods=['POST'])
@require_apikey
def cancel_order():
    """
    Cancels a specific open order by ID.
    Requires JSON: {"broker": "alpaca", "order_id": "12345"}
    """
    data = request.json
    broker = data.get('broker', '').lower()
    order_id = data.get('order_id')

    if not broker or not order_id:
        return jsonify({"status": "error", "message": "Missing broker or order_id"}), 400

    print(f"[OMS] 🗑️ CANCEL REQUEST: {broker.upper()} Order {order_id}")

    if broker == 'alpaca' and alpaca_client:
        try:
            alpaca_client.cancel_order_by_id(order_id)
            return jsonify({"status": "success", "message": "Order canceled."}), 200
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    elif broker == 'ibkr':
        try:
            IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
            acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=2)
            account_id = acct_res.json()[0].get('accountId')
            
            res = requests.delete(f"{IB_GATEWAY_URL}/iserver/account/{account_id}/order/{order_id}", verify=False, timeout=2)
            res.raise_for_status()
            return jsonify({"status": "success", "message": "Order canceled."}), 200
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    return jsonify({"status": "error", "message": "Invalid broker target."}), 400


@app.route('/api/flatten', methods=['POST'])
@require_apikey
def flatten_ticker():
    """
    Emergency override: Closes an open position at the market price.
    Requires JSON: {"broker": "alpaca", "ticker": "AMD", "qty": 100}
    """
    data = request.json
    broker = data.get('broker', '').lower()
    ticker = data.get('ticker')
    qty = float(data.get('qty', 0))

    if not ticker:
        return jsonify({"status": "error", "message": "Ticker is required."}), 400

    print(f"[OMS] ⚠️ EMERGENCY FLATTEN: {qty} shares of {ticker} on {broker.upper()}")

    if broker == 'alpaca' and alpaca_client:
        try:
            alpaca_client.close_position(ticker)
            return jsonify({"status": "success", "message": f"{ticker} flattened on Alpaca."}), 200
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    elif broker == 'ibkr':
        try:
            IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
            acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=2)
            account_id = acct_res.json()[0].get('accountId')

            ibkr_payload = {
                "orders": [{
                    "cOID": f"FLATTEN_{ticker}_{int(time.time())}", 
                    "ticker": ticker,
                    "orderType": "MKT", 
                    "side": "SELL",
                    "quantity": qty,
                    "tif": "DAY",
                    "outsideRTH": True
                }]
            }

            res = requests.post(f"{IB_GATEWAY_URL}/iserver/account/{account_id}/orders", json=ibkr_payload, verify=False, timeout=2)
            res.raise_for_status()
            return jsonify({"status": "success", "message": f"{ticker} flattened on IBKR."}), 200
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    return jsonify({"status": "error", "message": "Invalid broker."}), 400


if __name__ == '__main__':
    print(f"🚀 Execution Gateway & Sync Server running at http://localhost:{PORT}")
    print(f"🔗 View Unified Balances: http://localhost:{PORT}/balances.json")
    print(f"🔗 View Unified Positions: http://localhost:{PORT}/positions.json")
    print(f"🔒 OMS Routing Active: PORT {PORT} | Gateway API Key Required for Execution")
    app.run(host='0.0.0.0', port=PORT)
