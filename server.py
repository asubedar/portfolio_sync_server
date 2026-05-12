import os
import requests
import psycopg2
from flask import Flask, jsonify
from flask_cors import CORS
import urllib3
import concurrent.futures # NEW: Added for concurrent requests

# Suppress insecure request warnings for IBKR local gateway's self-signed cert
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
# CRITICAL: Enables your web UI to fetch data from this API
CORS(app, max_age=86400)

PORT = int(os.environ.get("PORT", 3000))
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://username:password@localhost:5432/your_database")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

# ---------------------------------------------------------
# 1. QUESTRADE INTEGRATION (Full OAuth Lifecycle)
# ---------------------------------------------------------
qt_access_token = ''
qt_api_server = ''

def refresh_questrade_token():
    global qt_access_token, qt_api_server
    
    try:
        # Fetch the token from Postgres
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

        # Store the temporary access token and unique server URL in memory
        qt_access_token = data['access_token']
        qt_api_server = data['api_server']

        # CRITICAL: Save the NEW refresh token back to Postgres
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
    
    # If we don't have an access token in memory, get one
    if not qt_access_token:
        if not refresh_questrade_token():
            return []

    try:
        headers = {'Authorization': f'Bearer {qt_access_token}'}
        
        # 1. Get Accounts
        acct_res = requests.get(f"{qt_api_server}v1/accounts", headers=headers)
        acct_res.raise_for_status()
        accounts = acct_res.json().get('accounts', [])
        
        if not accounts: 
            return []
            
        account_id = accounts[0]['number']

        # 2. Get Positions
        pos_res = requests.get(f"{qt_api_server}v1/accounts/{account_id}/positions", headers=headers)
        pos_res.raise_for_status()

        # 3. Map to Dashboard format
        positions = []
        for p in pos_res.json().get('positions', []):
            positions.append({
                'symbol': p['symbol'],
                'qty': p['openQuantity'],
                'avgPrice': p['averageEntryPrice']
            })
            
        return positions

    except requests.exceptions.HTTPError as e:
        # If the token expired during the request, catch the 401, refresh, and retry exactly once
        if e.response.status_code == 401 and not is_retry:
            print("⚠️ Questrade token expired during request. Refreshing and retrying...")
            qt_access_token = '' # Force a refresh
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
            return {'cash': 0, 'buyingPower': 0}

    try:
        headers = {'Authorization': f'Bearer {qt_access_token}'}
        
        # 1. Get Accounts
        acct_res = requests.get(f"{qt_api_server}v1/accounts", headers=headers)
        acct_res.raise_for_status()
        accounts = acct_res.json().get('accounts', [])
        
        if not accounts: 
            return {'cash': 0, 'buyingPower': 0}
            
        account_id = accounts[0]['number']

        # 2. Get Balances
        bal_res = requests.get(f"{qt_api_server}v1/accounts/{account_id}/balances", headers=headers)
        bal_res.raise_for_status()

        # Questrade returns arrays of balances. Let's grab the combined USD balance.
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
        return {'cash': 0, 'buyingPower': 0}
    except Exception as e:
        print(f"❌ Questrade Error: {e}")
        return {'cash': 0, 'buyingPower': 0}

# ---------------------------------------------------------
# 2. INTERACTIVE BROKERS (IBKR) INTEGRATION
# ---------------------------------------------------------
def get_ibkr_positions():
    try:
        IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
        
        # Get portfolio accounts
        acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=2)
        acct_res.raise_for_status()
        accounts = acct_res.json()
        
        if not accounts: 
            return []
            
        all_positions = []

        # Loop through EVERY account returned by IBKR
        for account in accounts:
            account_id = account.get('id') or account.get('accountId')
            
            try:
                # Note: IBKR API usually expects a page number at the end for positions (e.g., /positions/0)
                pos_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/{account_id}/positions/0", verify=False, timeout=2)
                pos_res.raise_for_status()

                # Map to our Dashboard format and append to master list
                for p in pos_res.json():
                    all_positions.append({
                        'symbol': p.get('contractDesc', ''), 
                        'qty': float(p.get('position', 0)),
                        'avgPrice': float(p.get('avgCost', 0))
                    })
            except Exception as e:
                print(f"⚠️ IBKR Error fetching positions for account {account_id}: {e}")
                continue # If one account fails, skip it and keep fetching the others
                
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
        
        if not accounts: 
            return {'cash': 0, 'buyingPower': 0}
            
        total_cash = 0.0
        total_bp = 0.0
        total_equity = 0.0

        # Loop through EVERY account to sum the balances
        for account in accounts:
            account_id = account.get('id') or account.get('accountId')
            
            try:
                # Get balance summary for this specific account
                bal_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/{account_id}/summary", verify=False, timeout=2)
                bal_res.raise_for_status()
                summary = bal_res.json()

                # Safely extract and add to running totals
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
        return {'cash': 0, 'buyingPower': 0}
    except Exception as e:
        print(f"❌ IBKR Balances Critical Error: {e}")
        return {'cash': 0, 'buyingPower': 0}

# ---------------------------------------------------------
# 3. MERGE & SERVE
# ---------------------------------------------------------
@app.route('/positions.json', methods=['GET'])
def positions():
    try:
        # NEW: Fetch from both brokers concurrently instead of waiting for one to finish
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_future = executor.submit(get_questrade_positions)
            ibkr_future = executor.submit(get_ibkr_positions)
            
            qt_positions = qt_future.result()
            ibkr_positions = ibkr_future.result()

        all_positions = qt_positions + ibkr_positions

        # Weighted average for shared symbols across brokers
        merged_map = {}
        for p in all_positions:
            sym = p['symbol']
            if sym not in merged_map:
                merged_map[sym] = {'qty': p['qty'], 'avgPrice': p['avgPrice']}
            else:
                existing = merged_map[sym]
                new_qty = existing['qty'] + p['qty']
                if new_qty == 0:
                    new_avg = 0
                else:
                    new_avg = ((existing['qty'] * existing['avgPrice']) + (p['qty'] * p['avgPrice'])) / new_qty
                
                merged_map[sym] = {'qty': new_qty, 'avgPrice': new_avg}

        # Convert back to array format
        final_array = []
        for sym, data in merged_map.items():
            if data['qty'] != 0: # Hide closed positions
                final_array.append({
                    'symbol': sym,
                    'qty': data['qty'],
                    'avgPrice': data['avgPrice']
                })

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
        # NEW: Fetch balances concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_future = executor.submit(get_questrade_balances)
            ibkr_future = executor.submit(get_ibkr_balances)
            
            qt_bal = qt_future.result()
            ib_bal = ibkr_future.result()

        # Calculate totals
        total_cash = qt_bal['cash'] + ib_bal['cash']
        total_bp = qt_bal['buyingPower'] + ib_bal['buyingPower']

        return jsonify({
            "questrade": qt_bal,
            "ibkr": ib_bal,
            "total": {
                "cash": total_cash,
                "buyingPower": total_bp
            }
        })

    except Exception as e:
        print(f"Error generating balances: {e}")
        return jsonify({"error": "Failed to generate balances"}), 500

@app.route('/equity.json', methods=['GET'])
def equity():
    try:
        # Fetch data concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_future = executor.submit(get_questrade_balances)
            ibkr_future = executor.submit(get_ibkr_balances)
            
            qt_data = qt_future.result()
            ib_data = ibkr_future.result()

        # Calculate total combined equity
        total_equity = qt_data['equity'] + ib_data['equity']

        return jsonify({
            "questrade": {
                "equity": qt_data['equity']
            },
            "ibkr": {
                "equity": ib_data['equity']
            },
            "total": {
                "equity": total_equity
            }
        })

    except Exception as e:
        print(f"Error generating equity: {e}")
        return jsonify({"error": "Failed to generate equity"}), 500

if __name__ == '__main__':
    print(f"🚀 Portfolio Sync Server running at http://localhost:{PORT}")
    print(f"🔗 Set your dashboard Sync URL to: http://localhost:{PORT}/positions.json")
    print(f"🔗 Check live balances at: http://localhost:{PORT}/balances.json")
    app.run(host='0.0.0.0', port=PORT)
