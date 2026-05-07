import os
import requests
import psycopg2
from flask import Flask, jsonify
from flask_cors import CORS
import urllib3

# Suppress insecure request warnings for IBKR local gateway's self-signed cert
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
# CRITICAL: Enables your web UI to fetch data from this API
CORS(app)

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
        cur.execute("SELECT value FROM system_secrets WHERE key = 'QT_PAPP'")
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

# ---------------------------------------------------------
# 2. INTERACTIVE BROKERS (IBKR) INTEGRATION
# ---------------------------------------------------------
def get_ibkr_positions():
    try:
        IB_GATEWAY_URL = 'https://localhost:5000/v1/api'
        
        # Get portfolio accounts (verify=False ignores the local self-signed cert warning)
        acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False)
        acct_res.raise_for_status()
        accounts = acct_res.json()
        
        if not accounts: 
            return []
            
        account_id = accounts[0]['id']

        # Get positions for the account
        pos_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/{account_id}/positions", verify=False)
        pos_res.raise_for_status()

        # Map to our Dashboard format
        positions = []
        for p in pos_res.json():
            positions.append({
                'symbol': p['contractDesc'], 
                'qty': p['position'],
                'avgPrice': p['avgCost']
            })
            
        return positions

    except requests.exceptions.ConnectionError:
        # Suppress IBKR errors if the gateway isn't running so it doesn't spam the console
        return []
    except Exception as e:
        print(f"IBKR Error: {e}")
        return []

# ---------------------------------------------------------
# 3. MERGE & SERVE
# ---------------------------------------------------------
@app.route('/positions.json', methods=['GET'])
def positions():
    try:
        # Fetch from both brokers
        qt_positions = get_questrade_positions()
        ibkr_positions = get_ibkr_positions()

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

        return jsonify(final_array)

    except Exception as e:
        print(f"Error generating positions: {e}")
        return jsonify({"error": "Failed to generate positions"}), 500


if __name__ == '__main__':
    print(f"🚀 Portfolio Sync Server running at http://localhost:{PORT}")
    print(f"🔗 Set your dashboard Sync URL to: http://localhost:{PORT}/positions.json")
    app.run(host='0.0.0.0', port=PORT)
