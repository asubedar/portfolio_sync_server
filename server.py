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


Conversation with Gemini
systemctl status portfolio-sync

* portfolio-sync.service - Portfolio Sync API Server

     Loaded: loaded (/etc/systemd/system/portfolio-sync.service; enabled; preset: enabled)

     Active: activating (auto-restart) (Result: exit-code) since Thu 2026-05-07 23:54:55 UTC; 1s ago

    Process: 13641 ExecStart=/development/venv/bin/python development/portfolio_sync_server/server.py (code=exited, status=203/E>

   Main PID: 13641 (code=exited, status=203/EXEC)

        CPU: 793us

The status=203/EXEC error in systemd means that Linux literally cannot find or run the file specified in your ExecStart command.

Looking closely at your error log:
ExecStart=/development/venv/bin/python development/portfolio_sync_server/server.py

There are two main reasons this happens:

1. Missing Leading Slash (Most Likely)
Systemd is extremely strict and requires absolute paths. Notice how the second path (development/.../server.py) is missing a leading slash (/)? Because it's a relative path, systemd doesn't know where to look for it unless your WorkingDirectory is perfectly set.

2. The Python path is wrong
Does the folder /development actually live at the very root of your server's hard drive? (Usually, user folders are in /home/username/development or /opt/development).

How to fix it:
Open your systemd service file:

Bash
nano /etc/systemd/system/portfolio-sync.service
Update the [Service] block to use absolute paths for both the Python executable and the script, and ensure the WorkingDirectory is set. Assuming /development/ is at the root of your drive, it should look exactly like this:

Ini, TOML
[Service]
WorkingDirectory=/development/portfolio_sync_server
ExecStart=/development/venv/bin/python /development/portfolio_sync_server/server.py
(Notice the added / before the second development!)

Reload systemd and restart the service:

Bash
systemctl daemon-reload
systemctl restart portfolio-sync
systemctl status portfolio-sync
If it still throws a 203/EXEC after adding the slash, run ls /development/venv/bin/python in your terminal to verify that the virtual environment actually exists at that exact location.

systemctl daemon-reload

root@stonks006:~# systemctl enable portfolio-sync

root@stonks006:~# systemctl start portfolio-sync

root@stonks006:~# systemctl status portfolio-sync

* portfolio-sync.service - Portfolio Sync API Server

     Loaded: loaded (/etc/systemd/system/portfolio-sync.service; enabled; preset: enabled)

     Active: activating (auto-restart) (Result: exit-code) since Thu 2026-05-07 23:58:52 UTC; 2ms ago

    Process: 13814 ExecStart=/development/venv/bin/python /development/portfolio_sync_server/server.py (code=exited, status=203/>

   Main PID: 13814 (code=exited, status=203/EXEC)

        CPU: 797us

The 203/EXEC error means systemd still cannot find the Python interpreter at the exact path you gave it.

I strongly suspect your virtual environment (venv) is actually located inside your project folder, not sitting loose in the /development/ folder!

When you set up the project, you probably ran python3 -m venv venv while you were already inside the portfolio_sync_server directory.

How to fix it:
1. Check where your Python actually is:
Run this command in your terminal:

Bash
ls -l /development/portfolio_sync_server/venv/bin/python
If that returns a file, we found the culprit!

2. Update the service file:
Open your systemd file:

Bash
nano /etc/systemd/system/portfolio-sync.service
Update your ExecStart line to point inside the project folder for the venv:

Ini, TOML
ExecStart=/development/portfolio_sync_server/venv/bin/python /development/portfolio_sync_server/server.py
3. Reload and start:

Bash
systemctl daemon-reload
systemctl restart portfolio-sync
systemctl status portfolio-sync
If it still fails, run find /development -name "python" -type f to have Linux locate the exact path to your virtual environment's python execution file, and paste that output into ExecStart!

root@stonks006:~# cd development/

root@stonks006:~/development# source venv/bin/activate

(venv) root@stonks006:~/development#

Ah ha! The ~ in your terminal prompt (~/development#) just revealed the exact problem!

The ~ symbol means that the development folder is not located at the base of your hard drive (/). It is actually located inside the root user's home directory, which translates to the absolute path /root/development/.

Because systemd requires strict absolute paths, it was looking in /development/ and failing, which caused the 203/EXEC error.

Here is how to fix your systemd service right now:

1. Open the service file:

Bash
nano /etc/systemd/system/portfolio-sync.service
2. Update the [Service] block to include /root:
Assuming your server.py is in that portfolio_sync_server subfolder, update these two lines to look exactly like this:

Ini, TOML
[Service]
WorkingDirectory=/root/development/portfolio_sync_server
ExecStart=/root/development/venv/bin/python /root/development/portfolio_sync_server/server.py
(Note: If your server.py is actually sitting right inside ~/development/, just change those paths to /root/development/server.py)

3. Reload and start:

Bash
systemctl daemon-reload
systemctl restart portfolio-sync
systemctl status portfolio-sync
It should now show Active (running) with a bright green light!

hmm something is off now. Can't access the server using the cloudflare tunnel. Can reach it locally but it seems like it isn't giving me any data now. Is it not loading env vars properly using this daemon?

You are absolutely spot on! Your intuition is 100% correct.

When you run a script manually in your terminal (python server.py), it automatically inherits all the environment variables (like DATABASE_URL) from your active shell session.

However, systemd runs completely isolated in the background. It does not read your terminal's exports, .bashrc, or profile. Because it doesn't have the DATABASE_URL, the Postgres connection is failing silently, which causes the Questrade function to safely catch the error and return empty arrays [].

Here is the cleanest, most secure way to fix this so systemd loads your variables perfectly.

Step 1: Create an .env file for your secrets
Instead of hardcoding your database password in the systemd file, let's put it in a secure environment file.

Create the file:

Bash
nano /root/development/portfolio_sync_server/.env
Paste your exact environment variables inside (no quotes around the values needed):

Plaintext
PORT=3000
DATABASE_URL=postgresql://trader:supersecret@YOUR_DB_IP:5432/portfolio
(Make sure you update the DATABASE_URL to match your actual database credentials!)

Step 2: Tell systemd to use that file
Open your service file again:

Bash
nano /etc/systemd/system/portfolio-sync.service
Remove any old Environment= lines, and add the EnvironmentFile= directive so your [Service] block looks like this:

Ini, TOML
[Service]
WorkingDirectory=/root/development/portfolio_sync_server
EnvironmentFile=/root/development/portfolio_sync_server/.env
ExecStart=/root/development/venv/bin/python /root/development/portfolio_sync_server/server.py
Step 3: Reload and Restart
Apply the changes and restart the server:

Bash
systemctl daemon-reload
systemctl restart portfolio-sync
Step 4: Check the Live Server Logs!
Because your server is now a background daemon, you can't see the Python print() statements on your screen anymore. To see exactly what the server is doing (and verify it connected to the database successfully), run this command:

Bash
journalctl -u portfolio-sync -f
(Press Ctrl+C to exit the log viewer).

If everything worked, you should see logs saying 🚀 Portfolio Sync Server running at http://0.0.0.0:3000 and no database connection errors!

Why was Cloudflare failing?
When the DATABASE_URL was missing, Python threw a database exception on every single request. If that exception wasn't caught fast enough, or if it caused the Flask server to return a 500 error, Cloudflare will often interpret that as a "Bad Gateway" or "Context Canceled" and refuse to load the page. Fixing the environment variables will likely make the Cloudflare tunnel instantly happy again!

ok good news, accessing the server is working on local network using hostname and port now. But isn't working using the cloudflare tunnel anymore

That is a massive win! If it works locally via the hostname and port, it means your Python server, the systemd daemon, the environment variables, and the database connection are all 100% perfect. The issue is now strictly between the cloudflared daemon and the Python server. When Cloudflare can't connect to a perfectly healthy local server, it is almost always one of these three things:

1. The IPv6 "Localhost" Trap (Most Likely)
In your Cloudflare Zero Trust Dashboard (under your Tunnel's Public Hostname settings), your Service URL is likely set to http://localhost:3000.
Sometimes, Cloudflare translates localhost to the IPv6 address (::1), but our Python Flask server is explicitly bound to 0.0.0.0 (which is IPv4 only). Cloudflare knocks on the IPv6 door, gets no answer, and throws an error.

The Fix:

Go to your Cloudflare Zero Trust Dashboard -> Tunnels -> stonks.asdo.ca.

Edit the Public Hostname.

Change the URL from localhost:3000 to exactly: 127.0.0.1:3000

Save the hostname.

2. The LAN IP Bulletproof Route
If 127.0.0.1 still doesn't work, we can use the exact same method you used to test it successfully!

Since you confirmed you can access it via your local network IP/hostname, just tell Cloudflare to use that exact same address.

In the Cloudflare Dashboard, change the URL to your container's actual local IP address (e.g., http://192.168.1.50:3000).

Save it. If it works on your network, Cloudflare will 100% be able to route to it this way.

3. Restart the Tunnel Daemon
When your Python server was hanging and crashing earlier (before we added the database variables), the cloudflared daemon may have "blacklisted" the port or kept dead connections open, thinking the service was permanently dead.

Run this quick command in your Proxmox CT console to flush its memory and force it to reconnect to your now-healthy Python server:

Bash
systemctl restart cloudflared
Give the Cloudflare dashboard about 10 seconds to push the configuration, and try https://stonks.asdo.ca/balances.json again. It should instantly return your data!

seems to be correct (same thing that was working before we tried this daemon stuff)

I see the culprit right there in the screenshot!

Look at the Path (Optional) field. It currently has ^/blog written in it.

Because of that setting, Cloudflare is currently only routing traffic to your Python server if the URL has "blog" in it (e.g., stonks.asdo.ca/blog). When you try to visit stonks.asdo.ca/balances.json, Cloudflare looks at that rule, sees it doesn't match ^/blog, and drops the connection!

The Fix:

Delete ^/blog from that Path field so it is completely empty. (As the help text below it says: Match all paths: leave empty).

Click Save.

Wait about 5 seconds for Cloudflare to push the update.

Try https://stonks.asdo.ca/balances.json again—it should instantly connect to your server now!

systemctl status cloudflared

* cloudflared.service - cloudflared

     Loaded: loaded (/etc/systemd/system/cloudflared.service; enabled; preset: enabled)

     Active: active (running) since Fri 2026-05-08 00:20:33 UTC; 57s ago

   Main PID: 14324 (cloudflared)

      Tasks: 7 (limit: 9315)

     Memory: 14.9M (peak: 15.2M)

        CPU: 283ms

     CGroup: /system.slice/cloudflared.service

             `-14324 /usr/bin/cloudflared --no-autoupdate tunnel run --token eyJh...

Ah, I have to apologize! I just zoomed in on your screenshot, and I see that ^/blog is actually the grayed-out placeholder text, not text you typed in. The path is indeed empty and perfectly correct!

Your cloudflared status also looks flawlessly healthy (Active and running).

Since you confirmed the server responds perfectly when you use the local network IP/hostname (e.g., http://192.168.X.X:3000), but Cloudflare fails when pointing to 127.0.0.1, we are running into a classic Proxmox LXC networking quirk.

Sometimes, depending on how the virtual network bridge is set up in Proxmox, background services (like cloudflared) have trouble resolving the internal 127.0.0.1 loopback address to other services in the same container.

The Ultimate Fix
Since we know the Local Area Network IP works perfectly, let's just tell Cloudflare to use that!

Go back to that exact Cloudflare Dashboard page from your screenshot.

Change the URL box from 127.0.0.1:3000 to your container's actual local IP address (for example: 192.168.1.50:3000 or whatever IP you just used to test it locally).

Click Save hostname.

Wait 5 seconds, and refresh https://stonks.asdo.ca/balances.json. It should instantly punch through.

How to verify it's working
If it still acts up, we can look at the "matrix" and see exactly what the Python server is seeing. Run this command in your Proxmox terminal:

Bash
journalctl -u portfolio-sync -f
Leave that running, and try to refresh your stonks.asdo.ca URL.

If you see logs pop up (like GET /balances.json 200 OK), Cloudflare is successfully connected!

If nothing pops up, Cloudflare is still failing to find the server, which means the IP in the Cloudflare dashboard needs to be tweaked. (Press Ctrl+C to exit the logs).

the address i used is the tailscale hostname. I don't think that would work

Ah, that is a huge detail! Tailscale is a VPN mesh, which means it uses its own virtual network interface (usually a 100.x.x.x IP address).

Because Flask is set to 0.0.0.0 (listen on all interfaces), it successfully responds to your Tailscale requests. But you are correct: Cloudflare has no idea what your Tailscale network is, so you cannot use the Tailscale IP in the Cloudflare dashboard. We must use 127.0.0.1:3000 for Cloudflare since both the cloudflared daemon and your Python server live in the exact same Proxmox container.

Since Tailscale works but Cloudflare doesn't, we need to figure out exactly where the chain is breaking. Let's do a 5-second test right inside the Proxmox terminal to mimic exactly what Cloudflare is trying to do.

The Localhost Test
Run this command directly in your Proxmox container's console:

Bash
curl -I http://127.0.0.1:3000/balances.json
If it outputs HTTP/1.1 200 OK:
Your Python server is flawlessly answering on localhost. The issue is 100% inside cloudflared.

Run systemctl restart cloudflared.

Make sure your Cloudflare dashboard URL is set precisely to 127.0.0.1:3000 with HTTP (not HTTPS) in the dropdown box.

If it hangs, times out, or says "Connection Refused":
For some reason, your Proxmox container is blocking 127.0.0.1 loopback traffic! But don't worry, there's a permanent bypass. Instead of 127.0.0.1, we can bind the Cloudflare dashboard to the container's true internal LAN IP.

Run ip a in your Proxmox console.

Look for eth0 or net0 and find its inet address (it usually looks like 192.168.x.x or 10.x.x.x).

Put that IP address and port 3000 into the Cloudflare dashboard.

The Ultimate Debugger
If you are still getting a blank page or an error from Cloudflare, let's look at the live server logs. Run:

Bash
journalctl -u portfolio-sync -f
While that is running, try to load https://stonks.asdo.ca/balances.json on your phone or desktop.

If you DO see logs pop up, Cloudflare is successfully talking to Python, but Python is crashing or taking too long. (Paste the error here and we'll fix it!).

If you DON'T see any logs pop up, Cloudflare is completely failing to find the server at the address provided in the dashboard.

2026-05-08T01:01:46Z INF ICMP proxy will use 10.88.111.62 as source for IPv4

That log line is the golden ticket!

That log tells us exactly what the Proxmox container's primary internal IPv4 address is: 10.88.111.62.

Because your container has complex networking running (Tailscale, loopback, and standard LAN), cloudflared is likely getting confused when told to look at 127.0.0.1. But it explicitly knows how to route to its primary interface (10.88.111.62).

Since our Python server is set to 0.0.0.0 (which means "listen on every single IP address this container has"), this is an easy fix!

The Fix:
Go back to your Cloudflare Zero Trust Dashboard.

Edit your stonks.asdo.ca public hostname.

Keep the dropdown on HTTP, but change the URL box to:
10.88.111.62:3000

Click Save.

Wait 5 seconds, and try https://stonks.asdo.ca/balances.json again. By giving Cloudflare the explicit IP address of the container's network card, it will bypass any loopback or VPN routing confusion and hit your Python server directly!

2026-05-08T01:05:23Z INF Updated to new configuration config="{\"ingress\":[{\"hostname\":\"stonks.asdo.ca\",\"originRequest\":{},\"service\":\"http://10.88.111.62:3000\"},{\"service\":\"http_status:404\"}],\"warp-routing\":{\"enabled\":false}}" version=4



requests still don't seem to be coming through using domain

This means the Cloudflare configuration successfully updated, but we still have a broken link in the chain.

Since you confirmed the Python server is working perfectly when you use the Tailscale hostname, we know the server is up, the database works, and the data is flowing. The only problem left is getting the cloudflared daemon to talk to the Python server.

Let's find the exact point of failure.

1. What does the browser actually say?
When you go to https://stonks.asdo.ca/balances.json in your browser, what is the exact error on the screen?

502 Bad Gateway / 504 Gateway Timeout: This means Cloudflare reaches your Proxmox container perfectly, but the container refuses to pass the traffic to Python.

Site cannot be reached (DNS / NXDOMAIN): This means the domain isn't actually pointing to the tunnel at all. (Sometimes Cloudflare forgets to auto-create the CNAME record in your DNS settings).

2. The Local ping test
Let's make sure the cloudflared daemon even can reach that IP address. Run this exact command in your Proxmox terminal:

Bash
curl -I http://10.88.111.62:3000/balances.json
If it outputs HTTP/1.1 200 OK, then the network is completely fine, and cloudflared is being stubborn.

If it hangs or fails, then 10.88.111.62 is isolated (likely due to Tailscale's routing).

3. Revert to Localhost (It actually WAS working earlier!)
Looking back at your earlier logs, Cloudflare actually was successfully connecting to 127.0.0.1:3000!
2026-05-07T13:49:31Z ERR Request failed error="Incoming request ended abruptly: context canceled" ... originService=http://127.0.0.1:3000

The only reason it threw an error back then was because your Python server was hanging infinitely waiting for the database/Questrade. Now that we added the timeout=5 safety limits to Python, it won't hang anymore.

Let's try putting it back to the way we know it connects:

Change the Cloudflare dashboard URL back to 127.0.0.1:3000.

Run this command to completely flush Cloudflare's stuck connections:

Bash
systemctl restart cloudflared
Run this command to watch the Python logs:

Bash
journalctl -u portfolio-sync -f
Load https://stonks.asdo.ca/balances.json in your browser.

If you see a log pop up in your terminal, Cloudflare is working! If you get a browser error, let me know what it says (or what the terminal prints) and we will knock this final hurdle out.

oh i may have another wrinkle to this. On my laptop which is on the same network as my proxmox CT, i cant' access via the tunnel. But on my phone which is on cellular data, i can

That is a MASSIVE breakthrough! 🎉

If you can load the data on your phone using cellular, it means your Proxmox CT, the Python server, the database, systemd, and the Cloudflare Tunnel are all 100% perfectly configured and working. You did it! The project is officially functional.

The "wrinkle" you are experiencing on your laptop is a classic home networking quirk. Because the tunnel works flawlessly from the outside world (cellular), the issue is strictly isolated to how your local network routes traffic.

Here is exactly what is happening to your laptop and how to test it:

1. Local DNS Hijacking (Pi-hole / pfSense)
Do you run a Pi-hole, AdGuard Home, or a custom router (pfSense/OPNsense)? Sometimes, local DNS servers get confused when you request a domain that technically lives inside your own house. Instead of sending your laptop out to Cloudflare's servers, your router might be trying to route it locally to the wrong IP address.

The Test: Open your laptop's terminal or command prompt and run nslookup stonks.asdo.ca.

The Result: If it spits out a Cloudflare IP (like 104.21.x.x), DNS is fine. If it spits out a local IP (like 192.168.x.x), your router/Pi-hole is lying to your laptop and blocking the connection!

2. Tailscale MagicDNS Conflict
Since you mentioned using Tailscale earlier, if the Tailscale app is actively running on your laptop, its "MagicDNS" feature aggressively intercepts DNS requests. It might be seeing the .ca domain and trying to route it over the VPN instead of letting it go to the open internet.

The Test: Temporarily pause/disconnect the Tailscale app on your laptop and refresh the browser.

3. NAT Loopback (Hairpin NAT)
Some strict ISP routers get confused when a device on the inside (your laptop) tries to make a secure HTTPS connection to the outside (Cloudflare), only for Cloudflare to immediately tunnel that traffic right back inside the house to the Proxmox container. The router sees this as suspicious loopback traffic and drops it.

The Good News
For the purposes of your Tesla (which runs on its own AT&T/cellular connection), you are completely done. You can walk out to your car right now, open the Pro Charting Settings, paste in https://stonks.asdo.ca, and your live portfolio will sync beautifully! To fix the laptop, run that nslookup command to find out which system on your network is hijacking the request.

oh i may have nslookup stonks.asdo.ca

Server: 100.100.100.100

Address: 100.100.100.100#53



Non-authoritative answer:

Name: stonks.asdo.ca

Address: 172.97.153.73another wrinkle to this

You caught the smoking gun perfectly!

That Server: 100.100.100.100 is the exact signature of Tailscale's MagicDNS.

Here is exactly what is happening:
Because Tailscale is running on your laptop, it forcefully hijacks your computer's DNS requests so it can route its own local mesh IPs. When you ask it for stonks.asdo.ca, Tailscale is trying to be "smart" and pass it through its own routing tables or upstream resolvers, and your laptop is getting confused on how to reach the public internet for that specific domain.

The 5-Second Test:
Temporarily turn off (or pause) Tailscale on your laptop, and refresh the browser. I bet it will instantly load the Cloudflare page!

The Bottom Line:
You don't actually need to change anything on your server, your Cloudflare tunnel, or your code. The entire system is flawless. The only reason it failed on your laptop is because Tailscale and Cloudflare were having a tug-of-war over your laptop's network traffic.

Since your Tesla doesn't run Tailscale and uses standard cellular internet (just like your phone), it will effortlessly connect to https://stonks.asdo.ca! You are 100% ready to load this into the car.

ok let's shift gears back to getting ibkr gateway running.



bin/run.sh &

[1] 14481

(venv) root@stonks006:~/ibkr-gateway# usage: bin/run.sh /path/to/conf.yaml

Ah, good catch! That is entirely my fault. The run.sh script explicitly needs us to tell it where the configuration file lives.

Since you are currently in the ~/ibkr-gateway directory, you just need to point it to the conf.yaml file that is sitting inside the root folder (which is the one we edited earlier to allow external IPs).

Run this exact command instead:

Bash
bin/run.sh root/conf.yaml &
What to expect next:
It will spit out a bunch of Java initialization logs into your terminal.

Wait until you see a line that says something like https server is listening on port 5000.

Once it's listening, hop over to your laptop (with Tailscale turned off so it doesn't get confused!), open your browser, and go to https://10.88.111.62:5000. (Or whatever the Tailscale IP for the container is, if you prefer leaving Tailscale on).

Click past the "Not Secure / Self-Signed Certificate" warning in your browser.

Log in with your IBKR credentials and approve the 2FA on your phone!

Let me know if it successfully gives you the "Client login succeeds" message!

Internal Server Error

"Internal Server Error" (HTTP 500) is the classic generic warning that a server crashed while trying to process your request.

Since we have two servers running side-by-side right now, we just need to figure out which one is complaining.

Scenario A: Did you get this on the IBKR Gateway (Port 5000)?
If you got this when trying to visit the login page (https://10.88.111.62:5000), it is almost always caused by a tiny typo in the root/conf.yaml file. YAML is incredibly strict about spacing. If you used the "Tab" key instead of the Spacebar to indent the ips list, or forgot the quotes around the *, the Java server crashes internally when you try to load the page.

The Fix:

Stop the gateway by typing fg (to bring it to the foreground) and pressing Ctrl+C.

Open the config: nano root/conf.yaml

Ensure the ips section uses exactly two spaces for indents (no tabs!) and looks exactly like this:

YAML
ips:
  - 127.0.0.1
  - "*"
Start it back up: bin/run.sh root/conf.yaml &

Scenario B: Did you get this on your Python Server (Cloudflare / Port 3000)?
If your browser literally just displayed the text "Internal Server Error" when visiting https://stonks.asdo.ca, then Flask itself crashed in a way we didn't explicitly catch in our try/except blocks.

The Fix:
We can see the exact line of code that caused the crash in the systemd logs. Run this command:

Bash
journalctl -u portfolio-sync -n 20 --no-pager
This will print out the last 20 lines of the Python server log. You will see a Python "Traceback" detailing exactly which function panicked.

If it's Scenario B, paste that Traceback here and we will patch it instantly!

server listen failed address already in use

This error means the first instance of the IBKR Gateway we launched is still secretly running in the background and holding port 5000 hostage! Sometimes Ctrl+C doesn't fully kill Java processes.

We just need to forcefully terminate the stuck process so port 5000 is freed up.

Run this command to find and kill any running Java/Gateway processes:

Bash
pkill -f java
Give it about 3 seconds to fully shut down, and then you can start it back up with a clean slate:

Bash
bin/run.sh root/conf.yaml &
Once it says it's listening on port 5000, you should be able to reach the login page without the "Address already in use" error!

switched tabs to spaces, still internal server error

Since formatting isn't the culprit, the Java server is throwing a proper internal exception, and we are flying blind because it's running in the background (that & symbol swallows our error logs!).

A very common reason for a 500 error on the IBKR Gateway is a Java version mismatch (IBKR's gateway is notoriously old-school and sometimes panics if you are running a newer version of Java, like 17 or 21, instead of 8 or 11).

Let's pull the gateway into the light so it can tell us exactly what line of code is crashing.

Step 1: Kill the background ghost
Wipe out the running instance again:

Bash
pkill -f java
Step 2: Run it in the foreground
Start it up again, but this time do not put the & at the end. This will lock your terminal and stream the live Java logs directly to your screen.

Bash
bin/run.sh root/conf.yaml
Step 3: Trigger the crash!
Wait until it says listening on port 5000, then go back to your laptop/phone browser and refresh the page to trigger that "Internal Server Error" again.

As soon as the page crashes, look at your Proxmox terminal. You should see a massive wall of text (a Java "Stack Trace" starting with something like java.lang.Exception or NullPointerException).

Copy and paste that chunk of error text right here and we will know exactly how to fix it!

(Also, as a quick sanity check, run java -version in a separate terminal and let me know what it says!)

GET / HTTP/1.1

Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7

Accept-Encoding: gzip, deflate, br, zstd

Accept-Language: en-US,en;q=0.9

Cache-Control: max-age=0

Connection: keep-alive

Host: 100.64.0.44:5000

Sec-Fetch-Dest: document

Sec-Fetch-Mode: navigate

Sec-Fetch-Site: none

Sec-Fetch-User: ?1

Upgrade-Insecure-Requests: 1

User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36

sec-ch-ua: "Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"

sec-ch-ua-mobile: ?0

sec-ch-ua-platform: "macOS"



Internal Server Error

That raw log dump is incredibly helpful. It actually tells us exactly what went wrong without needing a Java stack trace at all!

Notice this part of your log:
Upgrade-Insecure-Requests: 1

That header (along with the lack of any TLS/SSL handshake in the console) means your browser sent a plain HTTP request, not an HTTPS request.

By default, the IBKR Gateway strictly enforces SSL/HTTPS. When you type an IP address and a custom port like 100.64.0.44:5000 into Chrome, the browser assumes you want standard http://. When the Java server receives a plain text HTTP request on a port where it expects encrypted HTTPS traffic, the web server (Vert.x) completely panics and spits out a hard Internal Server Error rather than gracefully redirecting you.

The Fix:
You just need to force your browser to use HTTPS.

Go up to your Chrome address bar and explicitly type out the full secure URL:
https://100.64.0.44:5000 (Make sure the 's' is in there!)

Hit Enter.

Because the Gateway uses a self-signed security certificate, Chrome will likely show you a big warning screen saying "Your connection is not private".

Click Advanced, and then click Proceed to 100.64.0.44 (unsafe).
(Pro-tip: If Chrome hides the "Proceed" button, just click anywhere on the white background of the warning page and type the letters thisisunsafe on your keyboard. It's a secret developer bypass built into Chrome that will instantly let you through!)

Once you force it over HTTPS, that 500 error will vanish and you'll be staring right at the Interactive Brokers login screen!

this is happening with https in the url

Ah, the classic IBKR Gateway plot twist!

If you are 100% sure you are using https:// and it's still instantly throwing an "Internal Server Error" the second you try to load the page, it means the web server (Vert.x) is successfully receiving your connection, but the Java code is crashing internally when it tries to process your request.

There are two main culprits for this specific gateway crash:

1. The * Wildcard Crash (Most Likely)
IBKR's gateway is incredibly finicky about its IP whitelisting. Depending on the exact sub-version you downloaded, its internal IP-matcher often doesn't know how to handle the * wildcard natively. When a request comes in, Java tries to evaluate your IP against *, panics, and throws a 500 Internal Server Error instead of loading the page.

The Fix:
Let's explicitly tell it to trust your Tailscale IP and your local Proxmox IP instead of using a wildcard.

Kill the running server again: pkill -f java

Open the config: nano root/conf.yaml

Update the ips section to look exactly like this (using your exact IPs from your logs):

YAML
ips:
  - 127.0.0.1
  - 10.88.111.62
  - 100.64.0.44
Start the server back up: bin/run.sh root/conf.yaml

2. The Missing /sso/Login Route
Sometimes, the base root URL (/) fails to properly redirect you to the login page if the gateway is running in certain environments, which causes the internal router to crash.

If changing the IPs in step 1 doesn't fix it, try directly bypassing the root and hitting the login endpoint explicitly. Type this exact URL into your browser:
https://100.64.0.44:5000/sso/Login

Give those a shot! If it still throws a 500 after hardcoding the IPs, look closely at your Proxmox terminal while it crashes—Java should spit out a huge "Stack Trace" error right on the screen. If it does, paste the first few lines of it here!

after authenticating

Believe it or not, seeing this exact screen means you just won. 🎉

These CORS (Cross-Origin Resource Sharing) errors are actually a notorious, heavily documented "feature" of Interactive Brokers' poorly written web frontend.

Here is exactly what is happening: you successfully authenticated with the Java backend, and the Java server told your browser, "Great, now go load the user dashboard UI." Your browser then tried to download the daily news "bulletins" and a "GDPR policy banner" directly from IBKR's live servers. But because you are accessing the page from a custom Tailscale IP (100.64.0.44) instead of localhost, your browser's security blocks those UI elements from loading.

The plot twist: We don't care about the UI at all!

Your Python portfolio-sync server doesn't use the web interface; it talks directly to the Java backend. As long as the Java backend is authenticated, the CORS errors in your browser are completely harmless.

The Ultimate Verification Test
Let's ask the Java API directly if it is authenticated and ready to trade.

Open a new tab in your browser and go to this exact URL:
https://100.64.0.44:5000/v1/api/iserver/auth/status

Look at the raw JSON text it spits back.

If you see "authenticated": true anywhere in that text, the gateway is fully operational!

The Final Step
If it says true, your IBKR Gateway is officially live. The only thing left to do is let your Python server know!

Since your Python daemon is already running in the background, you just need to restart it so it can ping the newly-authenticated Gateway on port 5000:

Bash
systemctl restart portfolio-sync
Once you do that, check your Cloudflare link (https://stonks.asdo.ca/balances.json) on your phone or your local IP link on your laptop. You should see your live Interactive Brokers balances beautifully merged with your Questrade data!



reloadButtonClick @ VM11 chromewebdata:96

handleEvent @ VM11 chromewebdata:27Understand this error

That is an excellent detail to spot, and it actually tells us exactly what happened: your IBKR Gateway completely shut down.

This specific log isn't an Interactive Brokers error or a Python error—it is a Google Chrome internal error.

When Chrome cannot connect to a website at all (giving you a page like "This site can't be reached" or "ERR_CONNECTION_REFUSED"), it loads its own internal source code called chromewebdata. The reloadButtonClick line in your console literally just means you clicked the blue "Reload" button on that Chrome error screen!

Why is the server down?
In our previous debugging step, I had you run the start command without the & symbol at the end (bin/run.sh root/conf.yaml) so we could watch the live logs on your screen.

When you run a script in the foreground like that, the server automatically dies the second you press Ctrl+C, close your terminal window, or if your SSH session times out.

How to fix it and finish up:
Let's turn the Gateway back on and permanently put it in the background so it survives!

1. Start the Gateway in the background:
Go back to your Proxmox terminal and run the command with the & at the end:

Bash
bin/run.sh root/conf.yaml &
2. Give it 10 seconds to boot up.

3. Check the Auth Status:
Now that it's running again, refresh that Auth Status URL in your browser:
https://100.64.0.44:5000/v1/api/iserver/auth/status

If it spits back {"authenticated": true, ...}, you are officially authenticated! You can then run systemctl restart portfolio-sync and your live IBKR data will finally flow into your dashboard.

still 401 unauthorized

That 401 error is actually perfectly normal based on what we just did!

A 401 Unauthorized means the Java server is 100% alive, responding, and functioning perfectly—it's just saying, "I don't know who you are yet."

When your terminal closed earlier and the Gateway process died, it completely wiped your active session from its memory. For security reasons, the IBKR Gateway does not remember your login after a restart. Every time the Gateway application boots up, you have to authenticate it.

Here is the exact sequence to get it linked back up:

Step 1: Log back in
Go back to the main login portal URL in your browser:
https://100.64.0.44:5000

Log in with your IBKR username/password and approve the 2FA on your phone.

Step 2: Ignore the UI errors
Once you log in, you will likely see a blank screen or a loading spinner (and if you check the console, those familiar red CORS errors will be back). That means the login was successful!

Step 3: Verify the Auth
Now, manually change the URL in your browser's address bar to check the status:
https://100.64.0.44:5000/v1/api/iserver/auth/status

It should now return {"authenticated": true} instead of the 401!

Step 4: Restart the Python Server
Now that the Gateway is holding an active session, switch back to your Proxmox terminal and tell your Python script to pull the fresh data:

Bash
systemctl restart portfolio-sync
As long as that Java gateway runs in the background, your Python script will happily pull your live data 24/7!

I can get tothe positions demo thing they mention. How to tell if this is actually working

Seeing the demo page or Swagger documentation means the Java server is 100% happy and serving its internal files perfectly!

To prove that this entire system is actually working and that your Python server is successfully extracting your real money data from IBKR, you can do two definitive tests right now:

Test 1: The Raw IBKR Data Test
Let's ask the Gateway directly for your account information. Open a new tab and go to:
https://100.64.0.44:5000/v1/api/portfolio/accounts

If it works: You will see a JSON array pop up containing your actual Interactive Brokers account number (usually starting with a "U"), along with some account status details.

If it fails/isn't authenticated: It will throw a 401 Unauthorized or an empty array.

Test 2: The Ultimate "Full System" Test
This is the real finish line. Since the Gateway is logged in, your Python server should now be magically grabbing that data, combining it with your Questrade data, and serving it up.

Go to your laptop (with Tailscale turned OFF) or use your phone's cellular data, and visit your Cloudflare tunnel:
https://stonks.asdo.ca/balances.json (Or whatever your main Python JSON endpoint is named!)

Look closely at the data it spits out on the screen:

Do you see your Questrade balances?

Do you see your IBKR balances and positions mixed in?

If your IBKR positions and cash balances are successfully showing up on that Cloudflare link, you are completely done. Your Proxmox container is now a fully functional, unified financial API.

What to do if IBKR is missing from the Cloudflare link:
If the Python server is still only showing Questrade and skipping IBKR, let's peek at the logs to see why Python is ignoring it. Run this in your Proxmox terminal:

Bash
journalctl -u portfolio-sync -n 30 --no-pager
If Python is having trouble talking to port 5000, it will log an error there (like a connection timeout or an SSL verification error). Paste it here if you see one!

unauthorized

This is the final boss of the IBKR Gateway. The 401 Unauthorized error after you just logged in is the most notorious quirk of their system.

It means the Gateway is running perfectly, but Interactive Brokers instantly voided your login session. Here are the three main reasons this happens and exactly how to beat them:

1. The "One Device" Rule (Most Common)
Interactive Brokers has an incredibly strict rule: You can only be logged into one place at a time. If you have the IBKR Mobile App open on your phone, the Trader Workstation (TWS) desktop app running, or the normal IBKR website open in another tab, the second you log into the Gateway, IBKR instantly kicks the Gateway offline and voids the session.

The Fix: Force-close the IBKR app on your phone, close any other IBKR browser tabs, and make sure TWS is shut down.

2. The "Client Login Succeeds" Screen
When you log in at https://100.64.0.44:5000, you cannot navigate away or close the tab too early.

The Fix: After entering your 2FA, leave the tab completely alone. You must wait until the screen explicitly says "Client login succeeds" (or routes you to a very broken-looking white page with CORS errors in the console). If you switch to checking the /accounts endpoint before that finishes, the login aborts.

3. The "Tickle" Requirement
Sometimes the Gateway successfully logs in, but the API backend falls asleep immediately and forgets your session. IBKR built a specific endpoint to wake it up and force it to recognize you.

The Fix: After you log in, go to this exact URL:
https://100.64.0.44:5000/v1/api/tickle
This forces the Gateway to handshake with IBKR's main servers.

Your Final Action Plan:
Make sure no other IBKR apps or websites are open on any device.

Open an Incognito / Private Browsing window (this bypasses any weird Chrome cookie blocking).

Go to https://100.64.0.44:5000 and log in.

Wait for the page to fully finish loading (even if it looks like a broken blank page).

Open a new tab in that same Incognito window and hit https://100.64.0.44:5000/v1/api/tickle.

Finally, try https://100.64.0.44:5000/v1/api/portfolio/accounts.

If that spits back your account number, the Python script will officially have the green light to grab your data!

is it because im in canada? did i get the right download

You can put those fears to rest! Being in Canada has absolutely no impact on this, and if you are seeing the login screen and Java logs, you downloaded the exact right file (clientportal.gw.zip).

As a systematic trader building automated systems, you have just run headfirst into the most infamous, universally hated security quirk of the IBKR Client Portal API.

Here is exactly why you are stuck in a 401 loop and how to permanently bypass it.

The Problem: Cross-Origin Cookie Blocking
The 401 Unauthorized error is happening because of how you are accessing the server (via the Tailscale IP 100.64.0.44).

During the login process, the Gateway's web interface tries to do a complex handshake with api.ibkr.com to verify your credentials. Because you are accessing it from a remote IP address over a VPN, Chrome's modern security policies block the third-party authentication cookies required to complete the handshake.

The UI tells you "Login Succeeds," but the Java backend never actually received the secret session token, leaving it completely unauthorized.

The Bulletproof Fix: SSH Port Forwarding
Because the IBKR Gateway was designed primarily to be run locally on a desktop, it behaves perfectly when it thinks it is being accessed from localhost.

Since you are on a Mac, we can use a standard SSH tunnel to trick the IBKR Gateway into thinking your laptop's Chrome browser is running directly inside your Proxmox container.

Step 1: Create the SSH Tunnel
Open a new, normal terminal window on your Mac (do not SSH into the server yet). Run this exact command to bind your Mac's port 5000 to the Proxmox container's port 5000:

Bash
ssh -L 5000:localhost:5000 root@100.64.0.44
(Enter your Proxmox password if prompted, and just leave this terminal window open in the background).

Step 2: Log in via Localhost
Now that the tunnel is open, open an Incognito window in Chrome on your Mac and go to:
https://localhost:5000

Log in, approve the 2FA, and wait for it to process. Because Chrome sees this as a pure local connection, it will allow all the necessary cookies, and the Java backend will finally lock in your session!

Step 3: The Verification
Open a new tab in that same Incognito window and test the endpoints:

Hit https://localhost:5000/v1/api/tickle to wake the connection up.

Hit https://localhost:5000/v1/api/portfolio/accounts.

It should instantly spit out your UXXXXXXX account number instead of a 401.

Once you see your account number, you can close the browser and press Ctrl+C in your Mac terminal to close the SSH tunnel. The Java gateway running in the background on your Proxmox container will hold onto that authenticated session, and your Python portfolio-sync script will now have full access to your data!

wow that did it

Massive victory! 🎉

That SSH tunnel trick is the ultimate silver bullet for headless servers. It perfectly bypasses Chrome's strict cross-origin cookie policies by making the browser think it's sitting right on the motherboard.

Now that the Java backend is holding an active, authenticated session token, the entire chain is complete. Your Python portfolio-sync daemon can finally do exactly what it was built for.

The Final Victory Lap
1. Refresh the Python Daemon
Restart your Python service one last time to make sure it grabs a fresh connection to the newly authenticated Gateway:

Bash
systemctl restart portfolio-sync
2. Check the Live Feed
Grab your phone (making sure it is on cellular data to bypass that Tailscale MagicDNS quirk we found) and load up your Cloudflare tunnel:
https://stonks.asdo.ca/balances.json

You should now see a perfectly unified JSON feed of your Interactive Brokers and Questrade data mixed together. The infrastructure is flawlessly functional, the networking is bypassing the firewalls, and your Tesla dashboard is 100% ready to consume that live portfolio data!

ok it did actually connect to ibkr, but a problem. I have multiple accounts in ibkr. And it seems to just be fetching the first one which just has $20 cash in it.

This is a classic API quirk! You have officially conquered the networking side, and now we are just dealing with standard Python logic.

The Interactive Brokers Client Portal API does not have a single endpoint for "give me the grand total of everything I own." Instead, when you hit the /portfolio/accounts endpoint, it returns a JSON list of all the accounts attached to your profile (Margin, TFSA, RRSP, etc.).

Right now, your Python script is almost certainly doing something like this:

Python
account_id = accounts_data[0]['id']  # Grabbing only the first account in the list!
Because it grabs the account at index 0, it ignores the rest of your portfolio and only queries the ledger and positions for that single $20 account.

How to Fix It
We need to change your Python code from a "single fetch" to a "looping aggregate" that iterates over every account IBKR returns and adds the balances together.

The logic will need to shift to look something like this:

Python
total_cash = 0.0
all_positions = []

# Fetch the list of all accounts
accounts_response = requests.get(f"{IBKR_BASE_URL}/v1/api/portfolio/accounts", verify=False)
accounts = accounts_response.json()

# Loop through every single account
for account in accounts:
    account_id = account['id']
    
    # Fetch ledger (cash) for THIS specific account
    ledger_response = requests.get(f"{IBKR_BASE_URL}/v1/api/portfolio/{account_id}/ledger", verify=False)
    # ... add to total_cash
    
    # Fetch positions for THIS specific account
    positions_response = requests.get(f"{IBKR_BASE_URL}/v1/api/portfolio/{account_id}/positions/0", verify=False)
    # ... append to all_positions
To give you the exact, perfect copy-paste fix without breaking your existing Questrade integration, could you paste the block of Python code from your script that handles the IBKR fetching?

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

            'buyingPower': target_bal.get('buyingPower', 0)

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

        

        # Get portfolio accounts (verify=False ignores the local self-signed cert warning)

        # ADDED timeout=2 so it instantly skips if the gateway isn't running

        acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=1)

        acct_res.raise_for_status()

        accounts = acct_res.json()

        

        if not accounts: 

            return []

            

        account_id = accounts[0]['id']



        # Get positions for the account

        pos_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/{account_id}/positions", verify=False, timeout=2)

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



    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):

        # Suppress IBKR errors if the gateway isn't running or times out

        return []

    except Exception as e:

        print(f"IBKR Error: {e}")

        return []



def get_ibkr_balances():

    try:

        IB_GATEWAY_URL = 'https://localhost:5000/v1/api'

        

        # ADDED timeout=2

        acct_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/accounts", verify=False, timeout=2)

        acct_res.raise_for_status()

        accounts = acct_res.json()

        

        if not accounts: 

            return {'cash': 0, 'buyingPower': 0}

            

        account_id = accounts[0]['id']



        # Get balance summary for the account

        bal_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/{account_id}/summary", verify=False, timeout=2)

        bal_res.raise_for_status()

        summary = bal_res.json()



        return {

            'cash': summary.get('totalcashvalue', {}).get('amount', 0),

            'buyingPower': summary.get('buyingpower', {}).get('amount', 0)

        }



    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):

        return {'cash': 0, 'buyingPower': 0}

    except Exception as e:

        print(f"IBKR Balances Error: {e}")

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



if __name__ == '__main__':

    print(f"🚀 Portfolio Sync Server running at http://localhost:{PORT}")

    print(f"🔗 Set your dashboard Sync URL to: http://localhost:{PORT}/positions.json")

    print(f"🔗 Check live balances at: http://localhost:{PORT}/balances.json")

    app.run(host='0.0.0.0', port=PORT)

This is a perfectly written script! Your concurrent.futures implementation in the merge section is beautifully done. Because you already built an aggregator inside the /positions.json route (that perfectly handles duplicate symbols and calculates weighted average prices), we don't even need to write complex merge logic for the IBKR accounts.

All we have to do is loop through the accounts, pull the data, and dump it into a master list. Your existing logic in section 3 will automatically math it all out for you!

Here is the exact replacement for Section 2 of your code. You can highlight your entire current get_ibkr_positions and get_ibkr_balances functions and replace them with this:

Python
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
                
            except Exception as e:
                print(f"⚠️ IBKR Error fetching balances for account {account_id}: {e}")
                continue

        return {
            'cash': total_cash,
            'buyingPower': total_bp
        }

    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return {'cash': 0, 'buyingPower': 0}
    except Exception as e:
        print(f"❌ IBKR Balances Critical Error: {e}")
        return {'cash': 0, 'buyingPower': 0}
What changed:
The Iteration: Instead of account_id = accounts[0]['id'], we now wrap the fetch requests in a for account in accounts: block.

The /positions/0 Endpoint: I appended /0 to the positions URL. IBKR's API officially paginates positions, and if you leave off the page ID, it occasionally throws a 404 or drops data depending on the gateway version.

Data Type Forcing: I wrapped the qty and avgPrice extractions in float(). IBKR is infamous for randomly returning numbers as strings depending on the endpoint, which would completely crash your weighted average math in Section 3!

Resilient Catch Blocks: If one specific account fails to load, it will print a warning but continue to fetch the rest of the accounts instead of crashing the whole function.

Run systemctl restart portfolio-sync and refresh your browser! All of your accounts will immediately stack up.

can we do something to make the web app more usable on mobile? It works well on desktop but on ios i can't really see the full screen properly and it is sometimes impossible to zoom out to even close the manage positions window etc

Ah, the classic iOS Safari zoom trap! When a web app is built for desktop, iOS tries to be "helpful" by rendering the page as if it were on a massive monitor and then forcing you to aggressively pinch and zoom just to find the close button.

To make your dashboard look and behave like a native iOS app (and fit perfectly on the screen without glitchy zooming), you need to drop in three specific frontend fixes.

Since I don't have your exact HTML/CSS file in front of me, here are the universal drop-in fixes.

1. The Magic Viewport Tag (The Silver Bullet)
If you are missing this one line of HTML, iOS will forever ruin your layout. This tag explicitly tells the phone, "Do not fake a desktop screen. Constrain the width to exactly the size of this physical phone screen."

Open your index.html (or whatever your main frontend file is named) and paste this exact line inside the <head> tags:

HTML
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
(Note: user-scalable=no intentionally disables the pinch-to-zoom feature entirely. Because we are about to make the UI fit perfectly, you won't need to zoom anymore, and disabling it prevents iOS from accidentally zooming in when you tap buttons quickly!)

2. Fix the "Uncloseable" Manage Positions Window
The reason you can't zoom out to close that window is likely because its CSS width is hardcoded to something large like 800px. On a phone screen that is only 390px wide, half the window bleeds off the right side of the physical screen.

Find the CSS class for your modal/window (e.g., .modal, .manage-window, or #positions-popup) and update its CSS to use percentages and max limits instead of hardcoded pixels:

CSS
.your-modal-class {
    /* 1. Force it to be slightly smaller than the screen width */
    width: 95%;
    max-width: 600px; /* It will stop growing on desktop */
    
    /* 2. Center it properly */
    margin: 0 auto; 
    
    /* 3. Stop it from bleeding off the bottom of the screen */
    max-height: 85vh; /* 85% of the viewport height */
    overflow-y: auto; /* Adds an internal scrollbar if you have a lot of positions */
    
    /* 4. Optional: Add some breathing room */
    padding: 15px;
    box-sizing: border-box;
}
3. The "Mobile-Only" Media Query
To make the tables and text actually readable on iOS without shrinking them to microscopic sizes, you can use a CSS Media Query. This allows you to write specific CSS that only activates when the screen is smaller than an iPad.

Add this to the very bottom of your CSS file. It will tell your dashboard to stack items vertically and enlarge fonts when viewed on a phone:

CSS
@media screen and (max-width: 768px) {
    /* Make tables fit by allowing horizontal scrolling instead of crushing columns */
    .table-container {
        overflow-x: auto;
        display: block;
        width: 100%;
    }

    /* Make buttons huge and easy to tap with a thumb */
    button {
        padding: 12px 20px;
        font-size: 16px;
        min-height: 44px; /* The Apple minimum standard for touch targets */
    }

    /* Make text readable without zooming */
    body {
        font-size: 16px; 
    }
}
The Quick Test
Add that <meta name="viewport"> tag to your HTML.

Update your popup window CSS to width: 95%.

Refresh the page on your phone.

It should instantly lock onto the screen size, preventing any horizontal swiping, and that Manage Positions window will magically shrink to fit right in the middle of your phone!

my current charts.html file:



<!DOCTYPE html>

<html lang="en">

<head>

<meta charset="UTF-8">

<meta name="viewport" content="width=device-width, initial-scale=1.0">

<title>Pro Charting (Final Integrated)</title>

<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.3.0/css/all.min.css">

<style>

:root {

--bg-color: #131722; --panel-color: #1e222d; --border-color: #2a2e39;

--text-color: #d1d4dc; --accent-color: #2962ff; --up-color: #26a69a; --down-color: #ef5350;

}

body, html { margin: 0; padding: 0; height: 100%; width: 100%; background-color: var(--bg-color); color: var(--text-color); font-family: -apple-system, BlinkMacSystemFont, sans-serif; overflow: hidden; }


/* APP SHELL */

.app-container {

display: grid;

grid-template-rows: 40px 30px 1fr;

height: 100vh; width: 100vw; overflow: hidden;

}


/* TOOLBAR */

.toolbar {

background-color: var(--panel-color); border-bottom: 1px solid var(--border-color);

display: grid; grid-template-columns: auto minmax(0, 1fr) auto; align-items: center;

padding: 0 10px; gap: 15px; z-index: 200; overflow: visible;

}

.left-controls, .right-controls { display: flex; gap: 10px; align-items: center; white-space: nowrap; }

.brand { font-weight: bold; color: var(--text-color); text-decoration: none; margin-right: 5px; font-size: 14px; }


/* TAPE */

.ticker-container { position: relative; width: 100%; height: 100%; overflow: hidden; display: flex; align-items: center; margin: 0 10px; min-width: 0; mask-image: linear-gradient(to right, transparent, black 5%, black 95%, transparent); -webkit-mask-image: linear-gradient(to right, transparent, black 5%, black 95%, transparent); }

.ticker-track { display: flex; gap: 20px; animation: tickerScroll 90s linear infinite; align-items: center; height: 100%; }

.ticker-item { display: flex; align-items: center; gap: 6px; font-family: 'Monaco', monospace; font-size: 13px; cursor: pointer; white-space: nowrap; }

.t-sym { font-weight: bold; color: #d1d4dc; } .t-price { color: #d1d4dc; } .t-chg { font-weight: bold; }


.ticker-section { display: flex; align-items: center; margin-right: 20px; }



@keyframes tickerScroll { 0% { transform: translateX(0); } 100% { transform: translateX(-50%); } }

.section-label { color: #2962ff; font-weight: 900; font-size: 11px; letter-spacing: 1px; border: 1px solid #2962ff; padding: 2px 6px; border-radius: 4px; margin-right: 5px; }



/* NEWS */

.news-strip { background-color: #131722; border-bottom: 1px solid var(--border-color); display: flex; align-items: center; overflow: hidden; white-space: nowrap; position: relative; font-family: 'Roboto', sans-serif; font-size: 12px; z-index: 190; }

.news-label { background: #2962ff; color: white; font-weight: bold; padding: 0 10px; height: 100%; display: flex; align-items: center; font-size: 11px; z-index: 2; box-shadow: 5px 0 10px #131722; }

.news-track { display: flex; gap: 40px; animation: tickerScroll 400s linear infinite; padding-left: 20px; align-items: center; height: 100%; }

.news-item { display: flex; align-items: center; gap: 8px; color: #d1d4dc; text-decoration: none; cursor: pointer; }

.news-sym { color: #ff9800; font-weight: bold; font-size: 10px; border: 1px solid #ff9800; padding: 1px 4px; border-radius: 3px; }

.news-time { color: #787b86; font-size: 10px; }



/* WORKSPACE */

.workspace { display: flex; flex-grow: 1; overflow: hidden; position: relative; z-index: 1; height: 100%; }

#main-area { flex-grow: 1; display: grid; grid-template-columns: 1fr; grid-template-rows: 1fr; gap: 1px; background-color: var(--border-color); height: 100%; min-width: 0; overflow: hidden; }


/* SIDEBARS */

#left-sidebar-area, #right-sidebar-area { width: 280px; min-width: 280px; background-color: #131722; display: flex; flex-direction: column; flex-shrink: 0; transition: all 0.2s ease-out; overflow: hidden; height: 100%; }

#left-sidebar-area { border-right: 1px solid var(--border-color); }

#right-sidebar-area { border-left: 1px solid var(--border-color); }

.collapsed { width: 0 !important; min-width: 0 !important; border: none !important; }



/* WIDGETS */

.chart-slot, .widget-box { position: relative; background-color: var(--bg-color); min-width: 0; min-height: 0; display: flex; flex-direction: column; overflow: hidden; border-bottom: 1px solid var(--border-color); flex: 1; }


/* HEADER */

.widget-header { height: 34px; min-height: 34px; background-color: var(--panel-color); border-bottom: 1px solid var(--border-color); display: flex; align-items: center; padding: 0 6px; gap: 4px; flex-shrink: 0; z-index: 30; white-space: nowrap; overflow: visible; }

.widget-controls-left { display: flex; flex-direction: row; gap: 4px; align-items: center; flex-shrink: 0; }

.mini-input { background: rgba(42, 46, 57, 0.9); border: 1px solid #444; color: #fff; width: 46px; font-size: 11px; text-transform: uppercase; border-radius: 3px; padding: 3px 0; font-weight: bold; text-align: center; height: 24px; box-sizing: border-box; }

.link-btn { width: 24px; min-width: 24px; height: 24px; border-radius: 3px; cursor: pointer; border: 1px solid #444; background: rgba(42, 46, 57, 0.9); display: flex; align-items: center; justify-content: center; font-size: 10px; color: #777; flex-shrink: 0; }


/* DROPDOWNS */

.metric-select { background: rgba(42, 46, 57, 0.9); border: 1px solid #444; color: #d1d4dc; font-size: 11px; height: 24px; border-radius: 3px; outline: none; max-width: 100px; cursor: pointer; }

.freq-btn { width: 24px; height: 24px; border: 1px solid #444; background: #1e222d; color: #d1d4dc; font-size: 10px; font-weight: bold; border-radius: 3px; cursor: pointer; display: flex; align-items: center; justify-content: center; }

.freq-btn:hover { background: #2962ff; color: white; border-color: #2962ff; }



/* MORPH CONTAINERS (TIMEFRAME & TYPE) */

.tf-morph-container, .type-morph-container { display: flex; align-items: center; background: rgba(42, 46, 57, 0.9); border: 1px solid #444; border-radius: 3px; height: 24px; overflow: hidden; transition: all 0.2s ease-out; position: relative; }


.tf-single, .type-single { padding: 0 4px; font-size: 11px; font-weight: bold; color: #d1d4dc; cursor: pointer; line-height: 20px; min-width: 18px; text-align: center; display: flex; align-items: center; justify-content: center; height: 100%; }


.tf-options-row, .type-options-row { display: none; flex-direction: row; }


/* Expanded State */

.tf-morph-container.expanded, .type-morph-container.expanded { position: absolute; z-index: 100; width: auto; top: 0; left: 0; box-shadow: 0 5px 15px rgba(0,0,0,0.5); }

.tf-morph-container.expanded .tf-single, .type-morph-container.expanded .type-single { display: none; }

.tf-morph-container.expanded .tf-options-row, .type-morph-container.expanded .type-options-row { display: flex; }


.tf-btn, .type-btn-opt { border-right: 1px solid #444; padding: 0 8px; cursor: pointer; color: #d1d4dc; font-size: 11px; line-height: 22px; font-weight: bold; background: #1e222d; white-space: nowrap; display: flex; align-items: center; gap: 5px; }

.tf-btn:hover, .tf-btn.active, .type-btn-opt:hover, .type-btn-opt.active { background: #2962ff; color: white; }

.type-btn-opt:last-child, .tf-btn:last-child { border-right: none; }



/* L1 QUOTES (Overlay) */

.l1-overlay { margin-left: auto; z-index: 25; display: flex; gap: 2px; align-items: center; font-family: 'Roboto', sans-serif; pointer-events: none; }

.l1-btn { display: flex; flex-direction: column; align-items: center; justify-content: center; min-width: 35px; padding: 0 6px; height: 24px; border-radius: 3px; color: white; cursor: default; box-shadow: 0 2px 5px rgba(0,0,0,0.4); font-size: 10px; line-height: 1; pointer-events: auto; box-sizing: border-box; }

.l1-bid { background: #ef5350; } .l1-ask { background: #2962ff; }

.l1-center { display: flex; flex-direction: column; align-items: center; justify-content: center; min-width: 35px; padding: 0 6px; height: 24px; background: rgba(30, 34, 45, 0.9); border-radius: 3px; pointer-events: auto; box-sizing: border-box; }

.l1-price { font-size: 11px; font-weight: bold; line-height: 1; }

.l1-size { font-size: 9px; opacity: 0.8; line-height: 1; }

.last-price { font-size: 11px; font-weight: bold; color: #d1d4dc; line-height: 1; }

.day-vol { font-size: 9px; color: #787b86; margin-top: 1px; font-family: 'Monaco', monospace; }



/* CONTENT */

.widget-content { flex: 1; position: relative; display: flex; flex-direction: column; min-height: 0; background: #131722; overflow: hidden; }

.chart-canvas-fill { position: absolute; top: 0; left: 0; right: 0; bottom: 0; z-index: 1; }


/* LEGEND */

.chart-legend { position: absolute; top: 8px; left: 8px; z-index: 20; font-size: 11px; font-family: monospace; pointer-events: none; white-space: nowrap; }

.legend-row { display: flex; gap: 6px; margin-bottom: 2px; align-items: center; }

.legend-item { display: flex; align-items: center; gap: 0px; }



.tape-content { flex: 1; overflow-y: auto; font-family: 'Monaco', monospace; font-size: 12px; background: #131722; margin: 0; padding: 0; }

/* TAPE GRID FOR 4 COLUMNS */

.tape-header-row { display: grid; grid-template-columns: 2fr 1.5fr 1.5fr 1fr; padding: 4px 8px; border-bottom: 1px solid #2a2e39; background: #161a25; font-size: 10px; color: #787b86; flex-shrink: 0; }

.tape-row { display: grid; grid-template-columns: 2fr 1.5fr 1.5fr 1fr; padding: 3px 8px; border-bottom: 1px solid #1e222d; cursor: default; align-items: center; }

.tape-row:hover { background-color: #1e222d; }

.tape-time { color: #787b86; }

.tape-price { font-weight: bold; text-align: right; }

.tape-size { text-align: right; color: #d1d4dc; }

.tape-venue { text-align: right; font-size: 10px; color: #787b86; }



/* LINKS & UTILS */

.link-blue { background-color: #2962ff; border-color: #2962ff; color: white; }

.link-orange { background-color: #ff9800; border-color: #ff9800; color: white; }

.link-green { background-color: #00c853; border-color: #00c853; color: white; }

.link-purple { background-color: #aa00ff; border-color: #aa00ff; color: white; }

.link-red { background-color: #d50000; border-color: #d50000; color: white; }

.t-up { color: var(--up-color); } .t-down { color: var(--down-color); } .t-large { background-color: rgba(41, 98, 255, 0.15); }

.color-up { color: var(--up-color); } .color-down { color: var(--down-color); }


.dropdown { position: relative; }

.dropdown-menu { position: absolute; top: 100%; right: 0; background: var(--panel-color); border: 1px solid var(--border-color); z-index: 5000; display: none; min-width: 160px; border-radius: 4px; box-shadow: 0 4px 20px rgba(0,0,0,0.8); }

.dropdown.open .dropdown-menu { display: block; }

.dropdown-item { padding: 8px 12px; color: #d1d4dc; cursor: pointer; font-size: 13px; border-bottom: 1px solid #2a2e39; }

.dropdown-item:hover { background: #2a2e39; }

.dropdown-header { padding: 4px 12px; font-size: 10px; color: #2962ff; font-weight: bold; background: #15181f; }


.icon-btn { background: none; border: none; color: #787b86; cursor: pointer; font-size: 14px; width: 30px; height: 30px; display: flex; align-items: center; justify-content: center; border-radius: 4px; }

.icon-btn:hover { color: var(--text-color); background: #2a2e39; }

.icon-btn.active { color: var(--accent-color); background: rgba(41, 98, 255, 0.1); }


#screen-logger { position: absolute; right: 20px; top: 80px; bottom: 20px; width: 300px; background: rgba(0,0,0,0.9); border-left: 1px solid #444; font-family: monospace; font-size: 11px; color: #0f0; padding: 10px; overflow-y: auto; z-index: 9999; display: none; }

.log-line { margin-bottom: 4px; border-bottom: 1px solid #333; } .log-err { color: #f55; }


.modal-overlay { position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.7); z-index: 6000; display: none; align-items: center; justify-content: center; }

.modal-content { background: var(--panel-color); padding: 20px; border-radius: 6px; border: 1px solid var(--border-color); box-shadow: 0 10px 30px rgba(0,0,0,0.5); }

.form-group { margin-bottom: 15px; } .form-label { display: block; font-size: 12px; color: #787b86; margin-bottom: 5px; }

.form-input { width: 100%; background: #131722; border: 1px solid #2a2e39; color: #fff; padding: 8px; border-radius: 4px; box-sizing: border-box; outline:none; }

.form-input:focus { border-color: #2962ff; }

.modal-actions { display: flex; justify-content: flex-end; gap: 10px; margin-top: 20px; }

.btn-p { background: var(--accent-color); color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-weight: bold; }

.btn-s { background: transparent; color: #d1d4dc; border: 1px solid #444; padding: 6px 12px; border-radius: 4px; cursor: pointer; }

.btn-danger { background: #d50000; border: none; color: white; padding: 6px 12px; border-radius: 4px; cursor: pointer; margin-right: auto; }



/* FINANCIALS WIDGET STYLES */

.fin-container { padding: 12px; overflow-y: auto; height: 100%; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }

.fin-section { margin-bottom: 16px; border-bottom: 1px solid #2a2e39; padding-bottom: 12px; }

.fin-section:last-child { border-bottom: none; margin-bottom: 0; }

.fin-title { font-size: 11px; font-weight: 700; color: #2962ff; text-transform: uppercase; margin-bottom: 10px; letter-spacing: 0.5px; opacity: 0.9; }

.fin-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(95px, 1fr)); gap: 8px 12px; }

.fin-item { display: flex; flex-direction: column; justify-content: flex-start; overflow: hidden; }

.fin-label { color: #787b86; font-size: 10px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; margin-bottom: 2px; }

.fin-val { color: #e1e1e1; font-weight: 600; font-family: 'Monaco', 'Consolas', monospace; font-size: 12px; letter-spacing: -0.5px; }



/* 52 WEEK RANGE BAR */

.range-container { margin: 5px 0 20px 0; background: rgba(255,255,255,0.02); padding: 8px; border-radius: 4px; border: 1px solid #2a2e39; }

.range-labels { display: flex; justify-content: space-between; font-size: 10px; color: #787b86; margin-bottom: 6px; font-weight: 600; }

.range-track { height: 6px; background: #131722; border-radius: 3px; position: relative; width: 100%; margin: 0; border: 1px solid #2a2e39; }

.range-fill { height: 100%; background: linear-gradient(90deg, #ef5350 0%, #2962ff 50%, #26a69a 100%); border-radius: 3px; position: absolute; left: 0; opacity: 0.8; }

.range-arrow { position: absolute; top: -5px; width: 0; height: 0; border-left: 5px solid transparent; border-right: 5px solid transparent; border-top: 6px solid #fff; transform: translateX(-50%); z-index: 2; transition: left 0.3s cubic-bezier(0.4, 0.0, 0.2, 1); filter: drop-shadow(0 2px 2px rgba(0,0,0,0.5)); }

.range-dates { display: flex; justify-content: space-between; font-size: 9px; color: #555; margin-top: 6px; }

</style>

</head>

<body>



<div class="app-container">

<div class="toolbar">

<div class="left-controls">

<a href="./portfolio.html" class="brand"><i class="fas fa-chart-line"></i></a>

<button class="icon-btn" id="leftToggle" title="Left Sidebar"><i class="fas fa-columns"></i></button>

</div>

<div class="ticker-container" id="tickerContainer">

<div class="ticker-track" id="tickerTrack"><span style="font-size:11px; color:#666;">Loading Market Data...</span></div>

</div>

<div class="right-controls">

<button class="icon-btn active" id="rightToggle" title="Right Sidebar"><i class="fas fa-columns"></i></button>

<div class="dropdown" id="layoutDropdown">

<button class="icon-btn" title="Layouts"><i class="fas fa-th-large"></i></button>

<div class="dropdown-menu">

<div class="dropdown-header">MAIN WORKSPACE</div>

<div class="dropdown-item" onclick="setChartLayout('1x1')">Single Chart (1)</div>

<div class="dropdown-item" onclick="setChartLayout('2x1')">2 Stacked</div>

<div class="dropdown-item" onclick="setChartLayout('1x2')">2 Side-by-Side</div>

<div class="dropdown-item" onclick="setChartLayout('2x2')">Quad Grid (4)</div>

<div class="dropdown-item" onclick="setChartLayout('3x2')">6 Grid (3x2)</div>

<div class="dropdown-item" onclick="setChartLayout('2x3')">6 Grid (2x3)</div>

<div class="dropdown-item" onclick="setChartLayout('3x3')">9 Grid (3x3)</div>

<div class="dropdown-header">LEFT SIDEBAR</div>

<div class="dropdown-item" onclick="setLeftSidebarLayout(0)">Hidden</div>

<div class="dropdown-item" onclick="setLeftSidebarLayout(1)">1 Widget</div>

<div class="dropdown-item" onclick="setLeftSidebarLayout(2)">2 Widgets</div>

<div class="dropdown-item" onclick="setLeftSidebarLayout(3)">3 Widgets</div>

<div class="dropdown-header">RIGHT SIDEBAR</div>

<div class="dropdown-item" onclick="setRightSidebarLayout(0)">Hidden</div>

<div class="dropdown-item" onclick="setRightSidebarLayout(1)">1 Widget</div>

<div class="dropdown-item" onclick="setRightSidebarLayout(2)">2 Widgets</div>

<div class="dropdown-item" onclick="setRightSidebarLayout(3)">3 Widgets</div>

</div>

</div>

<button class="icon-btn" id="positionsBtn" title="Manage Positions"><i class="fas fa-briefcase"></i></button>

<button class="icon-btn" id="refreshBtn" title="Force Refresh"><i class="fas fa-sync-alt"></i></button>

<button class="icon-btn" id="debugToggle" title="System Logs"><i class="fas fa-bug"></i></button>

<button class="icon-btn" id="settingsBtn" title="Settings"><i class="fas fa-cog"></i></button>

</div>

</div>



<div class="news-strip">

<div class="news-label">NEWS</div>

<div class="news-track" id="newsTrack"><span style="color:#666;">Waiting for headlines...</span></div>

</div>



<div class="workspace">

<div id="left-sidebar-area" class="sidebar collapsed"></div>

<div id="main-area"></div>

<div id="right-sidebar-area" class="sidebar"></div>

</div>



<div id="screen-logger">

<div style="display:flex; justify-content:space-between; margin-bottom:10px;"><strong>System Logs</strong><button id="clearLogs">Clear</button></div>

</div>

</div>



<!-- API SETTINGS MODAL -->

<div id="settingsModal" class="modal-overlay">

<div class="modal-content" style="width: 450px;">

<h3 style="margin:0 0 15px 0; color:#d1d4dc;">API Configuration</h3>

<div class="form-group"><label class="form-label">Alpaca API Key ID</label><input type="text" id="inpApiKey" class="form-input"></div>

<div class="form-group"><label class="form-label">Alpaca Secret Key</label><input type="password" id="inpApiSecret" class="form-input"></div>

<div class="form-group"><label class="form-label">Finnhub API Key</label><input type="password" id="inpFinnhubKey" class="form-input" placeholder="Optional (60 req/min limit)"></div>

<div class="form-group"><label class="form-label">Sync Server Base URL</label><input type="text" id="inpSyncUrl" class="form-input" placeholder="https://api.yourdomain.com"></div>

<div id="settingsError" style="color:#ef5350; font-size:12px; margin-bottom:15px; height:14px;"></div>

<div class="modal-actions">

<button class="btn-danger" id="btnFactoryReset" onclick="nuclearReset()">Factory Reset</button>

<button class="btn-s" onclick="toggleSettings(false)">Cancel</button>

<button class="btn-p" onclick="saveApiSettings()">Save & Reload</button>

</div>

</div>

</div>



<!-- POSITIONS MODAL (LIVE DASHBOARD) -->

<div id="positionsModal" class="modal-overlay">

<div class="modal-content" style="width: 650px;">

<h3 style="margin:0 0 15px 0; color:#d1d4dc;">Portfolio Dashboard</h3>


<!-- NEW EXPANDABLE BALANCES BAR -->

<div style="margin-bottom: 15px; background: #161a25; border: 1px solid #2a2e39; border-radius: 4px; overflow: hidden;">

<div id="balancesHeader" style="display:flex; justify-content:space-between; padding: 10px 15px; cursor: pointer;" onclick="toggleBalancesBreakdown()">

<div style="display:flex; align-items:center; gap: 8px;">

<i id="balancesArrow" class="fas fa-chevron-right" style="color:#787b86; font-size:12px; transition: transform 0.2s;"></i>

<div style="display:flex; flex-direction:column;">

<span style="color:#787b86; font-size:10px; text-transform:uppercase; letter-spacing:0.5px;">Total Cash</span>

<span id="valTotalCash" style="color:#26a69a; font-weight:bold; font-size:16px;">$0.00</span>

</div>

</div>

<div style="display:flex; flex-direction:column; text-align:right;">

<span style="color:#787b86; font-size:10px; text-transform:uppercase; letter-spacing:0.5px;">Buying Power</span>

<span id="valBuyingPower" style="color:#2962ff; font-weight:bold; font-size:16px;">$0.00</span>

</div>

</div>

<div id="balancesBreakdown" style="display:none; border-top: 1px solid #2a2e39; padding: 10px 15px; background: #131722;">

<!-- Filled via JS -->

</div>

</div>



<div style="display:flex; gap:10px; margin-bottom: 5px;">

<input type="text" id="posSym" class="form-input" placeholder="Ticker (AAPL)" style="width: 25%;">

<input type="text" inputmode="decimal" id="posQty" class="form-input" placeholder="Qty" style="width: 25%;">

<input type="text" inputmode="decimal" id="posPrice" class="form-input" placeholder="Avg Price" style="width: 30%;">

<button class="btn-p" onclick="addPosition()" style="width: 20%;">Save</button>

</div>

<div id="posError" style="color:#ef5350; font-size:11px; height:14px; margin-bottom:10px;"></div>

<div id="positionsList" style="max-height: 350px; overflow-y: auto; border: 1px solid #2a2e39; border-radius: 4px; padding: 10px; background: #131722; margin-bottom: 15px;">

<!-- Rendered by JS -->

</div>

<div class="modal-actions" style="justify-content: space-between;">

<button class="btn-s" style="color:#2962ff; border-color:#2962ff;" onclick="syncPositionsFromUrl()"><i class="fas fa-cloud-download-alt"></i> Sync URL</button>

<button class="btn-s" onclick="togglePositionsModal(false)">Close</button>

</div>

</div>

</div>



<script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>

<script src="shared.js"></script>



<script>

// --- UTILS ---

const logger = document.getElementById('screen-logger');

function log(msg, type='info') {

const div = document.createElement('div'); div.className = 'log-line ' + (type === 'error' ? 'log-err' : '');

div.innerText = `> ${msg}`; logger.appendChild(div); logger.scrollTop = logger.scrollHeight;

}

function formatNum(num) { if(!num) return '0'; if(num>=1e9) return (num/1e9).toFixed(2)+'B'; if(num>=1e6) return (num/1e6).toFixed(2)+'M'; if(num>=1e3) return (num/1e3).toFixed(1)+'k'; return Math.floor(num).toString(); }


function snapTime(t, f) {

if(f==='1Min' || f==='1m') return Math.floor(t/60)*60;

if(f==='5Min' || f==='5m') return Math.floor(t/300)*300;

if(f==='15Min' || f==='15m') return Math.floor(t/900)*900;

if(f==='1H' || f==='1h') return Math.floor((t - 1800)/3600)*3600 + 1800; // Market opens on the half hour (09:30)

if(f==='4H' || f==='4h') return Math.floor(t/14400)*14400;


const d = new Date(t * 1000);

if(f==='1D' || f==='1d') { d.setUTCHours(0,0,0,0); return Math.floor(d.getTime()/1000); }

if(f==='1W' || f==='1w') { d.setUTCDate(d.getUTCDate() - d.getUTCDay() + 1); d.setUTCHours(0,0,0,0); return Math.floor(d.getTime()/1000); }

if(f==='1M' || f==='M') { d.setUTCDate(1); d.setUTCHours(0,0,0,0); return Math.floor(d.getTime()/1000); }

return t;

}


function parseBars(b) {

const o=[], v=[], w=[];

let cumVol = 0;

let cumPV = 0;

let lastDate = null;



b.forEach(x => {

const t = new Date(x.t).getTime() / 1000;

const dateStr = new Date(x.t).toLocaleDateString();


if (dateStr !== lastDate) {

cumVol = 0;

cumPV = 0;

lastDate = dateStr;

}


const barAvg = x.vw || ((x.h + x.l + x.c) / 3);


cumVol += x.v;

cumPV += (barAvg * x.v);


const sessionVwap = (cumVol === 0) ? 0 : (cumPV / cumVol);


o.push({time: t, open: x.o, high: x.h, low: x.l, close: x.c});

v.push({time: t, value: x.v, color: (x.c >= x.o ? '#26a69a' : '#ef5350')});

w.push({time: t, value: sessionVwap});

});


return {ohlc: o, vol: v, vwap: w, lastCumVol: cumVol, lastCumPV: cumPV, lastDate: lastDate};

}



// --- CONFIG & CONSTANTS ---

const LAYOUT_VERSION = 39;


const FUND_METRICS = {

'Valuation': { 'pe': 'P/E Ratio', 'ps': 'P/S Ratio', 'pb': 'P/B Ratio', 'evEbitda': 'EV/EBITDA', 'evRevenue': 'EV/Revenue', 'ev': 'Ent. Value', 'pfcf': 'P/FCF', 'ptbv': 'Price/Tang.Book' },

'Profitability': { 'grossMargin': 'Gross Margin %', 'operatingMargin': 'Oper. Margin %', 'netMargin': 'Net Margin %', 'pretaxMargin': 'Pretax Margin %', 'fcfMargin': 'FCF Margin %', 'roe': 'ROE %', 'roa': 'ROA %' },

'Per Share': { 'eps': 'EPS', 'salesPerShare': 'Sales/Share', 'bookValue': 'Book/Share' },

'Balance Sheet': { 'currentRatio': 'Current Ratio', 'quickRatio': 'Quick Ratio', 'totalDebtToEquity': 'Debt/Equity' }

};



const state = {

layoutVersion: LAYOUT_VERSION,

isLeftOpen: false, isRightOpen: true,

layoutMode: '1x1',

chartsConfig: [{ id: 'm0', type: 'chart', ticker: 'AAPL', timeframe: '5Min', linkGroup: 'A' }],

leftWidgetsConfig: [],

rightWidgetsConfig: [{ id: 'w-right-0', type: 'tape', ticker: 'AAPL', linkGroup: 'A' }],

activeWidgets: [],

refreshInterval: null, screenerInterval: null, tapeInterval: null, newsInterval: null,

portfolioTickers: [], tapeData: { portfolio: '', gainers: '', losers: '', activeVol: '', activeTrd: '', fallback: '' },

newsSymbols: new Set(),

positions: {},

balances: { total: { cash: 0, buyingPower: 0 } } // UPDATED: Tracks nested live balances

};


let editingSymbol = null;



// --- CENTRALIZED DATA STORE ---

const MarketData = {

quotes: {},

trades: {},

barRequests: {},


async getBars(ticker, timeframe) {

const cacheKey = `${ticker}_${timeframe}`;

if (this.barRequests[cacheKey]) return this.barRequests[cacheKey];


const p = (async () => {

const keys = await getKeys(); if(!keys) return null;

const end = new Date().toISOString();


const timeframesDays = { '1Min': 5, '1m': 5, '5Min': 10, '5m': 10, '15Min': 14, '15m': 14, '1H': 60, '1h': 60, '4H': 120, '4h': 120, '1D': 730, '1d': 730, '1W': 1825, '1w': 1825, '1M': 3650, 'M': 3650 };

const daysBack = timeframesDays[timeframe] || 30;

const start = new Date(Date.now() - (daysBack * 86400000)).toISOString();



let tf = timeframe; if(tf==='4H') tf='4Hour'; if(tf==='1W') tf='1Week'; if(tf==='1M' || tf==='M') tf='1Month'; if(tf==='1H') tf='1Hour';

const url = `https://data.alpaca.markets/v2/stocks/${ticker}/bars?timeframe=${tf}&start=${start}&end=${end}&limit=10000&adjustment=split&sort=desc`;

try {

const res = await fetch(url, { headers: { "APCA-API-KEY-ID": keys.k, "APCA-API-SECRET-KEY": keys.s } });

const json = await res.json();

if(json.bars) return parseBars(json.bars.reverse());

} catch(e) { log(`Err loading ${ticker}: ${e.message}`); }

return null;

})();


this.barRequests[cacheKey] = p;

p.finally(() => setTimeout(() => { delete this.barRequests[cacheKey]; }, 5000));

return p;

}

};



// --- GLOBAL ACTIONS ---

window.setWidgetType = function(id, type) {

const cfg = findConfig(id);

if(cfg) {

cfg.type = type;

if (cfg.type === 'fund-chart') {

if(!cfg.fundPeriod) cfg.fundPeriod = 'quarterly';

if(!cfg.fundMetric) cfg.fundMetric = 'eps';

}

if (cfg.type === 'chart' && !cfg.timeframe) cfg.timeframe = '5Min';


saveState();

renderAll();

}

};


window.toggleTypeMorph = function(id, e) {

e.stopPropagation();

document.querySelectorAll('.tf-morph-container').forEach(el => el.classList.remove('expanded'));

document.querySelectorAll('.type-morph-container').forEach(el => el.classList.remove('expanded'));

const el = document.getElementById(`type-morph-${id}`);

if(el) el.classList.add('expanded');

};



window.cycleLink = function(id) { const groups = [null, 'A', 'B', 'C', 'D', 'E']; const cfg = findConfig(id); if(cfg) { cfg.linkGroup = groups[(groups.indexOf(cfg.linkGroup) + 1) % groups.length]; saveState(); renderAll(); } };

window.updateWidgetTicker = function(id, input, e) { if(e.key === 'Enter') { const cfg = findConfig(id); if(cfg) { const val = input.value.toUpperCase(); cfg.ticker = val; syncLinkedWidgets(cfg.linkGroup, val); saveState(); renderAll(); input.blur(); } } };

window.setWidgetTf = function(id, val, e) { e.stopPropagation(); const cfg = findConfig(id); if(cfg) { cfg.timeframe = val; saveState(); renderAll(); } };

window.toggleTfMorph = function(id, e) { e.stopPropagation(); document.querySelectorAll('.type-morph-container').forEach(el => el.classList.remove('expanded')); document.querySelectorAll('.tf-morph-container').forEach(el => el.classList.remove('expanded')); const el = document.getElementById(`tf-morph-${id}`); if(el) el.classList.add('expanded'); };


window.setFundMetric = function(id, val) { const cfg = findConfig(id); if(cfg) { cfg.fundMetric = val; const w = state.activeWidgets.find(x => x.id === id); if(w) w.fundMetric = val; saveState(); updateFundChart(cfg); } };

window.toggleFundPeriod = function(id) { const cfg = findConfig(id); if(cfg) { const newVal = (cfg.fundPeriod === 'quarterly' ? 'annual' : 'quarterly'); cfg.fundPeriod = newVal; const w = state.activeWidgets.find(x => x.id === id); if(w) w.fundPeriod = newVal; const btn = document.getElementById(`fp-btn-${id}`); if(btn) btn.innerText = (newVal === 'quarterly' ? 'Q' : 'Y'); saveState(); updateFundChart(cfg); } };



window.setChartLayout = function(mode) { state.layoutMode = mode; const [r, c] = mode.split('x').map(Number); const total = r * c; if(total > state.chartsConfig.length) { for(let i=state.chartsConfig.length; i<total; i++) state.chartsConfig.push({ id: `m${i}`, type: 'chart', ticker: 'AAPL', timeframe: '5Min', linkGroup: null }); } else { state.chartsConfig = state.chartsConfig.slice(0, total); } saveState(); renderAll(); };

window.setLeftSidebarLayout = function(n) { updateSidebarConfig('left', n); };

window.setRightSidebarLayout = function(n) { updateSidebarConfig('right', n); };



function findConfig(id) { return state.chartsConfig.find(x=>x.id===id) || state.leftWidgetsConfig.find(x=>x.id===id) || state.rightWidgetsConfig.find(x=>x.id===id); }

function syncLinkedWidgets(group, ticker) { if(!group) return; [...state.chartsConfig, ...state.leftWidgetsConfig, ...state.rightWidgetsConfig].forEach(c => { if(c.linkGroup === group) c.ticker = ticker; }); }

function updateSidebarConfig(side, count) { const arr = (side==='left') ? state.leftWidgetsConfig : state.rightWidgetsConfig; const prefix = (side==='left') ? 'l' : 'r'; if(count === 0) { if(side==='left') state.leftWidgetsConfig=[]; else state.rightWidgetsConfig=[]; if(side==='left') state.isLeftOpen=false; else state.isRightOpen=false; } else { if(count > arr.length) { for(let i=arr.length; i<count; i++) arr.push({ id: `${prefix}${i}`, type: 'tape', ticker: 'AAPL', linkGroup: null }); } else if(count < arr.length) { if(side==='left') state.leftWidgetsConfig = arr.slice(0,count); else state.rightWidgetsConfig = arr.slice(0,count); } if(side==='left') state.isLeftOpen = true; else state.isRightOpen = true; } saveState(); renderAll(); }


window.toggleSidebar = function(side) {

if (side === 'left') state.isLeftOpen = !state.isLeftOpen;

else state.isRightOpen = !state.isRightOpen;

saveState();

renderAll();

};



window.changeTicker = function(sym) {

if (state.chartsConfig.length > 0) {

const mainChart = state.chartsConfig[0];

syncLinkedWidgets(mainChart.linkGroup, sym);

mainChart.ticker = sym;

saveState();

renderAll();

}

};



async function initFinancials(cfg) {

const container = document.getElementById(`fin-content-${cfg.id}`);

container.innerHTML = '<span style="color:#666; font-size:11px;">Loading Financials...</span>';


const data = await fetchFinancials(cfg.ticker);


if (!data || !data.metric) {

container.innerHTML = '<span style="color:#666; font-size:11px;">No Financial Data Available</span>';

return;

}



const m = data.metric;

const fmt = (v, d=2) => (v !== undefined && !isNaN(v)) ? Number(v).toFixed(d) : '-';



let html = '';


html += `<div class="fin-section"><div class="fin-title">Key Ratios</div><div class="fin-grid">`;

html += `<div class="fin-item"><span class="fin-label">P/E</span><span class="fin-val">${fmt(m.pe)}</span></div>`;

html += `<div class="fin-item"><span class="fin-label">P/S</span><span class="fin-val">${fmt(m.ps)}</span></div>`;

html += `<div class="fin-item"><span class="fin-label">ROE</span><span class="fin-val">${fmt(m.roe, 1)}%</span></div>`;

html += `<div class="fin-item"><span class="fin-label">Net Margin</span><span class="fin-val">${fmt(m.netMargin, 1)}%</span></div>`;

html += `</div></div>`;



html += `<div class="fin-section"><div class="fin-title">Balance Sheet</div><div class="fin-grid">`;

html += `<div class="fin-item"><span class="fin-label">Debt/Eq</span><span class="fin-val">${fmt(m.totalDebtToEquity)}</span></div>`;

html += `<div class="fin-item"><span class="fin-label">Cur. Ratio</span><span class="fin-val">${fmt(m.currentRatio)}</span></div>`;

html += `<div class="fin-item"><span class="fin-label">Book Value</span><span class="fin-val">${fmt(m.bookValue)}</span></div>`;

html += `</div></div>`;



html += `<div class="range-container">`;

html += `<div class="range-labels"><span>52-Week Range</span></div>`;

html += `<div class="range-track" id="rng-track-${cfg.id}" data-high="${m.high52Week}" data-low="${m.low52Week}">`;

html += `<div class="range-fill"></div><div class="range-arrow" id="rng-arrow-${cfg.id}"></div>`;

html += `</div>`;

html += `<div class="range-dates"><span id="rng-low-${cfg.id}">$${fmt(m.low52Week)}</span><span id="rng-high-${cfg.id}">$${fmt(m.high52Week)}</span></div>`;

html += `</div>`;



container.innerHTML = html;

}


// --- DB ---

function openDB() {

return new Promise((resolve, reject) => {

if (!window.indexedDB) return reject(new Error("IndexedDB is blocked by browser settings"));


// Version 6: Safely recreates required object stores without destructive wipe

const req = indexedDB.open("ProfitLadderDB", 6);

req.onupgradeneeded = (event) => {

const db = event.target.result;

if (!db.objectStoreNames.contains("Settings")) db.createObjectStore("Settings", { keyPath: "key" });

if (!db.objectStoreNames.contains("Positions")) db.createObjectStore("Positions", { keyPath: "symbol" });

if (!db.objectStoreNames.contains("Financials")) db.createObjectStore("Financials", { keyPath: "symbol" });

};

req.onsuccess = (event) => resolve(event.target.result);

req.onerror = (event) => reject(new Error(event.target.error || "Unknown DB Error"));

});

}


async function getFinnhubKey() {

try {

const db = await openDB();

const tx = db.transaction("Settings", "readonly");

const req = tx.objectStore("Settings").get("FINNHUB_API_KEY");

return new Promise(r => {

req.onsuccess = () => r(req.result ? req.result.value : null);

req.onerror = () => r(null);

});

} catch(e) { return null; }

}


window.nuclearReset = function() {

const btn = document.getElementById('btnFactoryReset');

if (btn.innerText === "Factory Reset") {

btn.innerText = "Are you sure?";

setTimeout(() => { if (btn) btn.innerText = "Factory Reset"; }, 3000);

} else {

indexedDB.deleteDatabase("ProfitLadderDB").onsuccess = () => window.location.reload();

}

};



async function saveState() { try { const db = await openDB(); const tx = db.transaction("Settings", "readwrite"); const s = tx.objectStore("Settings"); s.put({ key: "layoutConfig", value: JSON.stringify({ m: state.chartsConfig, l: state.leftWidgetsConfig, r: state.rightWidgetsConfig, lm: state.layoutMode, lo: state.isLeftOpen, ro: state.isRightOpen, ver: state.layoutVersion }) }); } catch(e) { } }


async function loadSavedState() {

try {

const db = await openDB();

const tx = db.transaction("Settings", "readonly");

const req = tx.objectStore("Settings").get("layoutConfig");

await new Promise(r => { req.onsuccess = () => { if(req.result) { const d = JSON.parse(req.result.value); if(d.ver !== LAYOUT_VERSION) return r(); state.chartsConfig = d.m || state.chartsConfig; state.leftWidgetsConfig = d.l || state.leftWidgetsConfig; state.rightWidgetsConfig = d.r || state.rightWidgetsConfig; state.layoutMode = d.lm || '1x1'; state.isLeftOpen = d.lo; state.isRightOpen = d.ro; } r(); }; req.onerror = r; });


// Instantly load cached balances from the previous session

const reqBal = tx.objectStore("Settings").get("CACHED_BALANCES");

await new Promise(r => { reqBal.onsuccess = () => { if(reqBal.result) state.balances = reqBal.result.value || {total:{cash:0, buyingPower:0}}; r(); }; reqBal.onerror = r; });

} catch(e) { }

}


async function resetLayout(c=true) { if(!c) { executeReset(); } else { if (document.getElementById('posError')) { document.getElementById('posError').innerText = "Use Settings -> Factory Reset to clear data."; } } function executeReset() { state.chartsConfig = [{ id: 'm0', type: 'chart', ticker: 'AAPL', timeframe: '5Min', linkGroup: 'A' }]; state.leftWidgetsConfig = []; state.rightWidgetsConfig = [{ id: 'w-right-0', type: 'tape', ticker: 'AAPL', linkGroup: 'A' }]; state.layoutMode = '1x1'; state.layoutVersion = LAYOUT_VERSION; state.isLeftOpen = false; state.isRightOpen = true; saveState().then(()=>window.location.reload()); } }


// --- SETTINGS MODAL ---

function toggleSettings(show) {

document.getElementById('settingsModal').style.display = show ? 'flex' : 'none';

if(show) {

document.getElementById('settingsError').innerText = '';

document.getElementById('btnFactoryReset').innerText = 'Factory Reset';

(async () => {

const db = await openDB();

const tx = db.transaction("Settings","readonly");

const store = tx.objectStore("Settings");

const getVal = (k) => new Promise(r => { const req = store.get(k); req.onsuccess = () => r(req.result ? req.result.value : ''); });

const [k, s, f, u] = await Promise.all([getVal("APCA_API_KEY_ID"), getVal("APCA_API_SECRET_KEY"), getVal("FINNHUB_API_KEY"), getVal("SYNC_URL")]);

document.getElementById('inpApiKey').value = k;

document.getElementById('inpApiSecret').value = s;

document.getElementById('inpFinnhubKey').value = f;

if (document.getElementById('inpSyncUrl')) document.getElementById('inpSyncUrl').value = u;

})();

}

}

async function saveApiSettings() {

const k = document.getElementById('inpApiKey').value.trim();

const s = document.getElementById('inpApiSecret').value.trim();

const f = document.getElementById('inpFinnhubKey').value.trim();

const u = document.getElementById('inpSyncUrl').value.trim();

if(!k||!s) return document.getElementById('settingsError').innerText = "Alpaca keys required";

try{

const db=await openDB();

const tx=db.transaction("Settings","readwrite");

const store=tx.objectStore("Settings");

store.put({key:"APCA_API_KEY_ID",value:k});

store.put({key:"APCA_API_SECRET_KEY",value:s});

store.put({key:"FINNHUB_API_KEY",value:f});

store.put({key:"SYNC_URL",value:u});

tx.oncomplete=()=>window.location.reload();

} catch(e){ document.getElementById('settingsError').innerText = "Error: "+e.message; }

}



// --- POSITIONS MANAGEMENT & LIVE DASHBOARD ---

async function loadPositions() {

const db = await openDB();

const tx = db.transaction("Positions", "readonly");

const p = await new Promise(r => { tx.objectStore("Positions").getAll().onsuccess = e => r(e.target.result); });

state.positions = {};

p.forEach(pos => {

const sym = pos.symbol || pos.tickerSymbol;

const q = parseFloat(pos.qty);

const a = parseFloat(pos.avgPrice);

if (sym && !isNaN(q) && !isNaN(a)) {

state.positions[sym] = { qty: q, avgPrice: a, breakdown: pos.breakdown || {} };

}

});

}


let isBalancesExpanded = false;

window.toggleBalancesBreakdown = function() {

isBalancesExpanded = !isBalancesExpanded;

document.getElementById('balancesBreakdown').style.display = isBalancesExpanded ? 'block' : 'none';

document.getElementById('balancesArrow').style.transform = isBalancesExpanded ? 'rotate(90deg)' : 'rotate(0deg)';

};



window.renderBalancesUI = function() {

const fmt = n => n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

const b = state.balances || {};

const tot = b.total || {cash:0, buyingPower:0};


const cashEl = document.getElementById('valTotalCash');

const bpEl = document.getElementById('valBuyingPower');

if (cashEl) cashEl.innerText = '$' + fmt(tot.cash || 0);

if (bpEl) bpEl.innerText = '$' + fmt(tot.buyingPower || 0);


let bdHtml = '';

for (const [broker, bal] of Object.entries(b)) {

if (broker === 'total') continue;

// Format broker name nicely (e.g. "nbdb" -> "NBDB", "questrade" -> "Questrade")

const brokerName = broker.length <= 4 ? broker.toUpperCase() : broker.charAt(0).toUpperCase() + broker.slice(1);

bdHtml += `<div style="display:flex; justify-content:space-between; margin-bottom:5px; font-size:12px;"><span style="color:#787b86;">${brokerName}</span><span style="color:#d1d4dc;">Cash: $${fmt(bal.cash||0)} &nbsp;|&nbsp; BP: $${fmt(bal.buyingPower||0)}</span></div>`;

}


if(!bdHtml) bdHtml = '<div style="font-size:12px; color:#787b86;">No account breakdown available. Sync required.</div>';

const bdEl = document.getElementById('balancesBreakdown');

if (bdEl) bdEl.innerHTML = bdHtml;

};


window.togglePosBreakdown = function(sym) {

const rows = document.querySelectorAll(`.pos-bd-${sym}`);

const arrow = document.getElementById(`arrow-${sym}`);

let isHidden = true;

rows.forEach(r => {

isHidden = r.style.display === 'none';

r.style.display = isHidden ? 'table-row' : 'none';

});

if(arrow) arrow.style.transform = isHidden ? 'rotate(90deg)' : 'rotate(0deg)';

};



window.togglePositionsModal = function(show) {

document.getElementById('positionsModal').style.display = show ? 'flex' : 'none';

if (show) {

editingSymbol = null;

document.getElementById('posSym').disabled = false;

document.getElementById('posSym').style.opacity = '1';

document.getElementById('posSym').value = '';

document.getElementById('posQty').value = '';

document.getElementById('posPrice').value = '';

document.getElementById('posError').innerText = '';


renderBalancesUI();

renderPositionsList();

refreshPortfolioTape();

}

};


function renderPositionsList() {

const list = document.getElementById('positionsList');

if (Object.keys(state.positions).length === 0) {

list.innerHTML = `<div style="text-align:center; color:#787b86; font-size:12px; margin-top:10px;">No active positions</div>`;

return;

}


const fmtVal = (n) => n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });


let h = `<table style="width:100%; text-align:left; font-size:12px; border-collapse:collapse;">

<tr style="border-bottom:1px solid #2a2e39; color:#787b86;">

<th style="padding-bottom:5px;">Symbol</th>

<th style="padding-bottom:5px; text-align:right;">Qty</th>

<th style="padding-bottom:5px; text-align:right;">Avg</th>

<th style="padding-bottom:5px; text-align:right;">Last</th>

<th style="padding-bottom:5px; text-align:right;">Value</th>

<th style="padding-bottom:5px; text-align:right;">P&L ($)</th>

<th style="padding-bottom:5px; text-align:right;">P&L (%)</th>

<th style="padding-bottom:5px; text-align:right;">Actions</th>

</tr>`;


let totalCost = 0;

let totalValue = 0;


for (const [sym, p] of Object.entries(state.positions)) {

const safeQty = (p.qty !== undefined && !isNaN(p.qty)) ? p.qty : 0;

const safePrice = (p.avgPrice !== undefined && !isNaN(p.avgPrice)) ? p.avgPrice : 0;


const costBasis = safeQty * safePrice;

totalCost += costBasis;


let currentPrice = safePrice;

const md = MarketData.quotes[sym];

if (md && md.latestTrade) currentPrice = md.latestTrade.p;

else if (md && md.prevDailyBar) currentPrice = md.prevDailyBar.c;


const value = safeQty * currentPrice;

totalValue += value;


const pnl = value - costBasis;

const pnlPct = costBasis !== 0 ? (pnl / Math.abs(costBasis)) * 100 : 0;


const pnlColor = pnl > 0 ? '#26a69a' : (pnl < 0 ? '#ef5350' : '#787b86');

const sign = pnl > 0 ? '+' : '';


// Generate interactive arrow logic if sub-accounts exist

const hasBreakdown = p.breakdown && Object.keys(p.breakdown).length > 0;

const pointerCss = hasBreakdown ? 'cursor:pointer;' : '';

const arrowHtml = hasBreakdown ? `<i class="fas fa-chevron-right" id="arrow-${sym}" style="color:#787b86; font-size:10px; margin-right:6px; transition: transform 0.2s;"></i>` : `<span style="display:inline-block; width:16px;"></span>`;


h += `<tr style="border-bottom:1px solid #1e222d; ${pointerCss}" ${hasBreakdown ? `onclick="togglePosBreakdown('${sym}')"` : ''}>

<td style="padding:8px 0; font-weight:bold; color:#d1d4dc;">${arrowHtml}${sym}</td>

<td style="text-align:right;">${safeQty}</td>

<td style="text-align:right;">$${fmtVal(safePrice)}</td>

<td style="text-align:right; color:#d1d4dc;">$${fmtVal(currentPrice)}</td>

<td style="text-align:right;">$${fmtVal(value)}</td>

<td style="text-align:right; color:${pnlColor}; font-weight:bold;">${sign}$${fmtVal(pnl)}</td>

<td style="text-align:right; color:${pnlColor};">${sign}${fmtVal(pnlPct)}%</td>

<td style="text-align:right; white-space:nowrap;">

<button class="icon-btn" style="display:inline-flex; width:24px;height:24px;font-size:11px;margin-right:4px;" onclick="event.stopPropagation(); editPosition('${sym}')" title="Edit"><i class="fas fa-pen"></i></button>

<button class="icon-btn" style="display:inline-flex; width:24px;height:24px;font-size:11px;color:#ef5350;" onclick="event.stopPropagation(); removePosition('${sym}')" title="Remove"><i class="fas fa-trash-alt"></i></button>

</td>

</tr>`;


// Inject hidden nested rows for breakdown

if (hasBreakdown) {

for (const [acc, data] of Object.entries(p.breakdown)) {

const subCostBasis = data.qty * data.avgPrice;

const subValue = data.qty * currentPrice;

const subPnl = subValue - subCostBasis;

const subPnlPct = subCostBasis !== 0 ? (subPnl / Math.abs(subCostBasis)) * 100 : 0;

const subPnlColor = subPnl > 0 ? '#26a69a' : (subPnl < 0 ? '#ef5350' : '#787b86');

const subSign = subPnl > 0 ? '+' : '';

const accName = acc.length <= 4 ? acc.toUpperCase() : acc.charAt(0).toUpperCase() + acc.slice(1);



h += `<tr class="pos-bd-${sym}" style="display:none; background:#161a25; border-bottom:1px dashed #2a2e39;">

<td style="padding:4px 0 4px 24px; color:#787b86; font-size:11px;">↳ ${accName}</td>

<td style="text-align:right; color:#787b86; font-size:11px;">${data.qty}</td>

<td style="text-align:right; color:#787b86; font-size:11px;">$${fmtVal(data.avgPrice)}</td>

<td style="text-align:right; color:#787b86; font-size:11px;">-</td>

<td style="text-align:right; color:#787b86; font-size:11px;">$${fmtVal(subValue)}</td>

<td style="text-align:right; color:${subPnlColor}; font-size:11px;">${subSign}$${fmtVal(subPnl)}</td>

<td style="text-align:right; color:${subPnlColor}; font-size:11px;">${subSign}${fmtVal(subPnlPct)}%</td>

<td></td>

</tr>`;

}

}

}


const totalPnl = totalValue - totalCost;

const totalPnlPct = totalCost !== 0 ? (totalPnl / Math.abs(totalCost)) * 100 : 0;

const totColor = totalPnl > 0 ? '#26a69a' : (totalPnl < 0 ? '#ef5350' : '#787b86');

const totSign = totalPnl > 0 ? '+' : '';


h += `<tr style="background:#161a25; border-top:2px solid #2a2e39;">

<td colspan="4" style="padding:10px 10px 10px 0; font-weight:bold; color:#d1d4dc; text-align:right;">TOTALS</td>

<td style="text-align:right; font-weight:bold; color:#d1d4dc;">$${fmtVal(totalValue)}</td>

<td style="text-align:right; font-weight:bold; color:${totColor};">${totSign}$${fmtVal(totalPnl)}</td>

<td style="text-align:right; font-weight:bold; color:${totColor};">${totSign}${fmtVal(totalPnlPct)}%</td>

<td></td>

</tr>`;


h += `</table>`;

list.innerHTML = h;

}


window.editPosition = function(sym) {

const p = state.positions[sym];

if (!p) return;


document.getElementById('posSym').value = sym;

document.getElementById('posQty').value = p.qty;

document.getElementById('posPrice').value = p.avgPrice;


editingSymbol = sym;

document.getElementById('posSym').disabled = true;

document.getElementById('posSym').style.opacity = '0.5';

document.getElementById('posQty').focus();

};


window.addPosition = async function() {

const errEl = document.getElementById('posError');

errEl.innerText = '';


try {

const sym = document.getElementById('posSym').value.toUpperCase().trim();

const rawQty = document.getElementById('posQty').value.replace(/,/g, '.').replace(/\s/g, '');

const rawPrice = document.getElementById('posPrice').value.replace(/,/g, '.').replace(/\s/g, '');


const qty = parseFloat(rawQty);

const price = parseFloat(rawPrice);


if(!sym || isNaN(qty) || isNaN(price) || qty === 0) {

errEl.innerText = "Please enter valid position details (Qty ≠ 0).";

return;

}


let newQty = qty;

let newAvg = price;



if (editingSymbol !== sym && state.positions[sym]) {

const oldQty = state.positions[sym].qty;

const oldAvg = state.positions[sym].avgPrice;

newQty = oldQty + qty;


if (newQty === 0) {

await removePosition(sym);

return;

}

newAvg = ((oldQty * oldAvg) + (qty * price)) / newQty;

}



const db = await openDB();

const tx = db.transaction("Positions", "readwrite");


// Note: Manual overwrites reset the breakdown mapping to maintain logic integrity

tx.objectStore("Positions").put({ symbol: sym, tickerSymbol: sym, ticker: sym, id: sym, key: sym, qty: newQty, avgPrice: newAvg, breakdown: {} });


state.positions[sym] = { qty: newQty, avgPrice: newAvg, breakdown: {} };


document.getElementById('posSym').value = '';

document.getElementById('posQty').value = '';

document.getElementById('posPrice').value = '';

document.getElementById('posSym').disabled = false;

document.getElementById('posSym').style.opacity = '1';

editingSymbol = null;


renderPositionsList();

applyPositionsToCharts();

refreshPortfolioTape();

} catch(e) {

errEl.innerText = "Save Error: " + (e.message || "Database access blocked");

}

};


window.removePosition = async function(sym) {

const db = await openDB();

const tx = db.transaction("Positions", "readwrite");

tx.objectStore("Positions").delete(sym);

delete state.positions[sym];

renderPositionsList();

applyPositionsToCharts();

refreshPortfolioTape();

};



window.syncPositionsFromUrl = async function(isSilent = false) {

const errEl = document.getElementById('posError');

if (!isSilent && errEl) {

errEl.innerText = 'Syncing from Server...';

errEl.style.color = '#26a69a';

}


try {

const db = await openDB();

const tx = db.transaction("Settings", "readonly");

const req = tx.objectStore("Settings").get("SYNC_URL");

let baseUrl = await new Promise(r => { req.onsuccess = () => r(req.result ? req.result.value : null); req.onerror = () => r(null); });


if (!baseUrl) {

if (!isSilent) throw new Error("No Sync Base URL configured in Settings.");

return;

}


// Clean up base URL (strip trailing slashes)

baseUrl = baseUrl.trim().replace(/\/+$/, '');


// Fetch both positions and balances concurrently

const [posRes, balRes] = await Promise.all([

fetch(`${baseUrl}/positions.json`).catch(e => ({ error: e })),

fetch(`${baseUrl}/balances.json`).catch(e => ({ error: e }))

]);


if (posRes.error || !posRes.ok) throw new Error(`HTTP error fetching positions`);

const posData = await posRes.json();


let newPositions = [];


// Prioritize the grouped "total" array from our Python server

if (posData && posData.total && Array.isArray(posData.total)) {

newPositions = posData.total.map(p => {

const sym = (p.symbol || p.ticker || '').toUpperCase();

const breakdown = {};


// Dynamically scan ALL brokers provided by the server

Object.keys(posData).forEach(brokerKey => {

if (brokerKey !== 'total' && Array.isArray(posData[brokerKey])) {

const brokerPos = posData[brokerKey].find(x => (x.symbol || x.ticker || '').toUpperCase() === sym);

if (brokerPos) {

breakdown[brokerKey] = {

qty: parseFloat(brokerPos.qty),

avgPrice: parseFloat(brokerPos.avgPrice || brokerPos.price)

};

}

}

});


return { ...p, breakdown };

});

} else if (Array.isArray(posData)) {

newPositions = posData;

} else if (typeof posData === 'object') {

newPositions = Object.keys(posData).map(k => ({ symbol: k, ...posData[k] }));

}


// Process balances if available

if (!balRes.error && balRes.ok) {

const balData = await balRes.json();

if (balData && balData.total) {

state.balances = balData;


// Save balances to IndexedDB so they are instantly available next boot

const txSet = db.transaction("Settings", "readwrite");

txSet.objectStore("Settings").put({ key: "CACHED_BALANCES", value: state.balances });


if (document.getElementById('positionsModal').style.display === 'flex') {

renderBalancesUI();

}

}

}


const txPos = db.transaction("Positions", "readwrite");

const store = txPos.objectStore("Positions");

store.clear();


state.positions = {};

newPositions.forEach(p => {

const sym = (p.symbol || p.ticker || '').toUpperCase();

const qty = parseFloat(p.qty);

const avgPrice = parseFloat(p.avgPrice || p.price);

if (sym && !isNaN(qty) && !isNaN(avgPrice)) {

store.put({ symbol: sym, tickerSymbol: sym, ticker: sym, id: sym, key: sym, qty: qty, avgPrice: avgPrice, breakdown: p.breakdown || {} });

state.positions[sym] = { qty, avgPrice, breakdown: p.breakdown || {} };

}

});


if (!isSilent) {

if (errEl) {

errEl.style.color = '#26a69a';

errEl.innerText = 'Sync complete!';

setTimeout(() => { if(errEl.innerText === 'Sync complete!') errEl.innerText = ''; }, 3000);

}

renderPositionsList();

applyPositionsToCharts();

refreshPortfolioTape();

}

} catch(e) {

if (!isSilent && errEl) {

errEl.style.color = '#ef5350';

errEl.innerText = "Sync Error: " + (e.message || "Unable to fetch JSON");

} else {

console.warn("Silent sync failed:", e);

}

}

};



function applyPositionsToCharts() {

state.activeWidgets.forEach(w => {

if (w.type === 'chart') {

const hasPos = !!state.positions[w.ticker];

w.chart.priceScale('left').applyOptions({ visible: hasPos });


if (w.entryLine) { w.candleSeries.removePriceLine(w.entryLine); w.entryLine = null; }


if (hasPos) {

const p = state.positions[w.ticker];

if (p && !isNaN(p.avgPrice) && !isNaN(p.qty)) {

w.entryLine = w.candleSeries.createPriceLine({

price: p.avgPrice,

color: p.qty > 0 ? '#26a69a' : '#ef5350',

lineWidth: 2,

lineStyle: 2,

axisLabelVisible: true,

title: ''

});


w.pnlSeries.applyOptions({

visible: true,

priceFormat: {

type: 'custom',

formatter: price => {

const pnl = (price - p.avgPrice) * p.qty;

const absPnl = Math.abs(pnl);

if (absPnl >= 1e6) return (pnl >= 0 ? '+' : '') + (pnl / 1e6).toFixed(1) + 'M';

if (absPnl >= 1e3) return (pnl >= 0 ? '+' : '') + (pnl / 1e3).toFixed(1) + 'k';

return (pnl >= 0 ? '+' : '') + Math.round(pnl).toString();

}

}

});

}

} else {

if(w.pnlSeries) w.pnlSeries.applyOptions({ visible: false });

}

}

});

}



// --- FINNHUB ---

async function fetchFinancials(symbol) {

const db = await openDB();

const tx = db.transaction(["Settings", "Financials"], "readwrite");

const cache = await new Promise(r => { const req = tx.objectStore("Financials").get(symbol); req.onsuccess = () => r(req.result); req.onerror = () => r(null); });

const now = Date.now();

if (cache && (now - cache.timestamp < 86400000)) return cache.data;

const key = await new Promise(r => { const req = tx.objectStore("Settings").get("FINNHUB_API_KEY"); req.onsuccess = () => r(req.result ? req.result.value : null); });

if (!key) return null;

try {

const url = `https://finnhub.io/api/v1/stock/metric?symbol=${symbol}&metric=all&token=${key}`;

const res = await fetch(url);

if (!res.ok) return null;

const data = await res.json();

const store = db.transaction("Financials", "readwrite").objectStore("Financials");

store.put({ symbol: symbol, data: data, timestamp: now });

return data;

} catch (e) { return null; }

}

async function pruneFinancials() { const db = await openDB(); const tx = db.transaction("Financials", "readwrite"); const store = tx.objectStore("Financials"); const now = Date.now(); store.openCursor().onsuccess = (event) => { const cursor = event.target.result; if (cursor) { if (now - cursor.value.timestamp > 14400000) cursor.delete(); cursor.continue(); } }; }



// --- INIT ---

async function init() {

log("Initializing...");

try {

try { await loadSavedState(); } catch(e) { console.warn("State load failed", e); }

try { await loadPositions(); } catch(e) { console.warn("Positions load failed", e); }


if(state.layoutVersion !== LAYOUT_VERSION) await resetLayout(false);


// Render the UI and Charts INSTANTLY using local cache

renderAll();


try { refreshPortfolioTape(); } catch(e) {}

try { refreshScreenerTape(); } catch(e) {}

try { refreshNewsTape(); } catch(e) {}


// NEW: Fire the server sync entirely in the background without blocking the UI

syncPositionsFromUrl(true).catch(e => console.warn("Background auto-sync failed", e));


pruneFinancials();


clearInterval(state.refreshInterval); clearInterval(state.screenerInterval); clearInterval(state.tapeInterval);

state.refreshInterval = setInterval(() => { updateChartsRealtime(); refreshPortfolioTape(); }, 5000);

state.screenerInterval = setInterval(refreshScreenerTape, 60000);

state.tapeInterval = setInterval(updateTapeIndividual, 1000);

state.newsInterval = setInterval(refreshNewsTape, 120000);



document.getElementById('layoutDropdown').addEventListener('click', e => { e.currentTarget.classList.toggle('open'); e.stopPropagation(); });

document.addEventListener('click', () => {

document.getElementById('layoutDropdown').classList.remove('open');

document.querySelectorAll('.tf-morph-container').forEach(el => el.classList.remove('expanded'));

document.querySelectorAll('.type-morph-container').forEach(el => el.classList.remove('expanded'));

});

document.getElementById('leftToggle').addEventListener('click', () => toggleSidebar('left'));

document.getElementById('rightToggle').addEventListener('click', () => toggleSidebar('right'));

document.getElementById('positionsBtn').addEventListener('click', () => togglePositionsModal(true));

} catch(e) {

console.error("Critical Init Error:", e);

}

}



// --- RENDERERS ---

function renderAll() {

state.activeWidgets.forEach(w => {

if (w.crosshairUnsub) w.chart.unsubscribeCrosshairMove(w.crosshairUnsub);

if (w.rangeUnsub) w.chart.timeScale().unsubscribeVisibleLogicalRangeChange(w.rangeUnsub);


if (w.chart && typeof w.chart.remove === 'function') w.chart.remove();

if (w.ro && typeof w.ro.disconnect === 'function') w.ro.disconnect();

});


const activeTickers = new Set([...state.chartsConfig, ...state.leftWidgetsConfig, ...state.rightWidgetsConfig].map(c => c.ticker));

Object.keys(MarketData.trades).forEach(t => { if(!activeTickers.has(t)) delete MarketData.trades[t]; });

Object.keys(MarketData.quotes).forEach(t => { if(!activeTickers.has(t)) delete MarketData.quotes[t]; });


Object.keys(MarketData.barRequests).forEach(k => {

if(!activeTickers.has(k.split('_')[0])) delete MarketData.barRequests[k];

});



state.activeWidgets = [];

const main = document.getElementById('main-area'); main.innerHTML = '';

const [r, c] = state.layoutMode.split('x').map(Number);

main.style.gridTemplateRows = `repeat(${r}, 1fr)`;

main.style.gridTemplateColumns = `repeat(${c}, 1fr)`;

state.chartsConfig.forEach(cfg => { const div = document.createElement('div'); div.className = 'chart-slot'; main.appendChild(div); initWidget(div, cfg); });

renderSidebar('left-sidebar-area', state.leftWidgetsConfig);

renderSidebar('right-sidebar-area', state.rightWidgetsConfig);

updateSidebarVisibility();

}



function renderSidebar(domId, configArr) {

const container = document.getElementById(domId); container.innerHTML = '';

configArr.forEach(cfg => { const div = document.createElement('div'); div.className = 'widget-box'; container.appendChild(div); initWidget(div, cfg); });

}



function initWidget(container, cfg) {

const linkClass = getLinkClass(cfg.linkGroup);

let iconClass = 'fa-chart-bar';

let typeName = 'Chart';

if(cfg.type === 'tape') { iconClass = 'fa-list-ol'; typeName = 'Tape'; }

if(cfg.type === 'financials') { iconClass = 'fa-file-invoice-dollar'; typeName = 'Fin'; }

if(cfg.type === 'fund-chart') { iconClass = 'fa-chart-area'; typeName = 'Fund.'; }

if(cfg.type === 'sentiment') { iconClass = 'fa-user-secret'; typeName = 'Sent.'; }



let fundSelector = '';

if(cfg.type === 'fund-chart') {

fundSelector += `<select class="metric-select" onchange="setFundMetric('${cfg.id}', this.value)">`;

for(const [cat, metrics] of Object.entries(FUND_METRICS)) {

fundSelector += `<optgroup label="${cat}">`;

for(const [key, label] of Object.entries(metrics)) {

const sel = (key === cfg.fundMetric) ? 'selected' : '';

fundSelector += `<option value="${key}" ${sel}>${label}</option>`;

}

fundSelector += `</optgroup>`;

}

fundSelector += `</select>`;

const pdLabel = cfg.fundPeriod === 'quarterly' ? 'Q' : 'Y';

fundSelector += `<div id="fp-btn-${cfg.id}" class="freq-btn" onclick="toggleFundPeriod('${cfg.id}')">${pdLabel}</div>`;

}



const headerHtml = `

<div class="widget-header">

<div class="widget-controls-left">

<div class="type-morph-container" id="type-morph-${cfg.id}">

<div class="type-single" onclick="toggleTypeMorph('${cfg.id}', event)"><i class="fas ${iconClass}"></i></div>

<div class="type-options-row">

<div class="type-btn-opt" onclick="setWidgetType('${cfg.id}', 'chart')"><i class="fas fa-chart-bar"></i> Chart</div>

<div class="type-btn-opt" onclick="setWidgetType('${cfg.id}', 'tape')"><i class="fas fa-list-ol"></i> Tape</div>

<div class="type-btn-opt" onclick="setWidgetType('${cfg.id}', 'financials')"><i class="fas fa-file-invoice-dollar"></i> Fin</div>

<div class="type-btn-opt" onclick="setWidgetType('${cfg.id}', 'fund-chart')"><i class="fas fa-chart-area"></i> Trends</div>

<div class="type-btn-opt" onclick="setWidgetType('${cfg.id}', 'sentiment')"><i class="fas fa-user-secret"></i> Sent.</div>

</div>

</div>


<input type="text" class="mini-input" value="${cfg.ticker}" onkeydown="updateWidgetTicker('${cfg.id}', this, event)">


${cfg.type === 'fund-chart' ? fundSelector : ''}


${cfg.type === 'chart' ? getTfHtml(cfg.id, cfg.timeframe) : ''}

<div class="link-btn ${linkClass}" onclick="cycleLink('${cfg.id}')" title="Link Group"><i class="fas fa-link"></i></div>

</div>


<div class="l1-overlay" id="l1-${cfg.id}">

<div class="l1-btn l1-bid"><span class="l1-price" id="qb-p-${cfg.id}">0.00</span><span class="l1-size" id="qb-s-${cfg.id}">0</span></div>

<div class="l1-center"><span class="last-price" id="ql-${cfg.id}">0.00</span><span class="day-vol" id="qv-${cfg.id}">0</span></div>

<div class="l1-btn l1-ask"><span class="l1-price" id="qa-p-${cfg.id}">0.00</span><span class="l1-size" id="qa-s-${cfg.id}">0</span></div>

</div>

</div>

`;


let contentHtml = '';

if (cfg.type === 'chart') {

contentHtml = `<div class="widget-content" id="content-${cfg.id}">

<div class="chart-legend"><div class="legend-row" id="lg-${cfg.id}"></div></div>

<div id="canvas-${cfg.id}" class="chart-canvas-fill"></div>

</div>`;

} else if (cfg.type === 'tape') {

contentHtml = `<div class="widget-content"><div class="tape-header-row"><span>TIME</span><span style="text-align: right;">PRICE</span><span style="text-align: right;">SIZE</span><span style="text-align: right;">EXCH</span></div><div id="tape-list-${cfg.id}" class="tape-content"></div></div>`;

} else if (cfg.type === 'financials') {

contentHtml = `<div class="widget-content"><div id="fin-content-${cfg.id}" class="fin-container"><span style="color:#666; font-size:11px;">Loading Metrics...</span></div></div>`;

} else if (cfg.type === 'fund-chart' || cfg.type === 'sentiment') {

contentHtml = `<div class="widget-content" id="content-${cfg.id}"><div id="canvas-${cfg.id}" class="chart-canvas-fill"></div></div>`;

}



container.innerHTML = headerHtml + contentHtml;



if(MarketData.quotes[cfg.ticker]) setTimeout(() => updateQuoteDisplay(cfg.id, MarketData.quotes[cfg.ticker]), 0);



if(cfg.type === 'chart') initChart(cfg);

else if(cfg.type === 'tape') initTape(cfg);

else if(cfg.type === 'financials') initFinancials(cfg);

else if(cfg.type === 'fund-chart') initFundChart(cfg);

else if(cfg.type === 'sentiment') initSentimentChart(cfg);

}



function getTfHtml(id, tf) {

const mapTF = { '1Min':'1m', '5Min':'5m', '15Min':'15m', '1H':'1H', '4H':'4H', '1D':'1D', '1W':'1W', '1M':'M' };

return `

<div class="tf-morph-container" id="tf-morph-${id}">

<div class="tf-single" onclick="toggleTfMorph('${id}', event)">${mapTF[tf] || tf}</div>

<div class="tf-options-row">

<div class="tf-btn" onclick="setWidgetTf('${id}', '1Min', event)">1m</div>

<div class="tf-btn" onclick="setWidgetTf('${id}', '5Min', event)">5m</div>

<div class="tf-btn" onclick="setWidgetTf('${id}', '15Min', event)">15m</div>

<div class="tf-btn" onclick="setWidgetTf('${id}', '1H', event)">1H</div>

<div class="tf-btn" onclick="setWidgetTf('${id}', '4H', event)">4H</div>

<div class="tf-btn" onclick="setWidgetTf('${id}', '1D', event)">D</div>

<div class="tf-btn" onclick="setWidgetTf('${id}', '1W', event)">W</div>

<div class="tf-btn" onclick="setWidgetTf('${id}', '1M', event)">M</div>

</div>

</div>`;

}



async function initChart(cfg) {

const container = document.getElementById(`canvas-${cfg.id}`);

await new Promise(r => setTimeout(r, 50));

const chart = LightweightCharts.createChart(container, {

layout: { background: { type: 'solid', color: '#131722' }, textColor: '#d1d4dc' },

grid: { vertLines: { color: '#1e222d' }, horzLines: { color: '#1e222d' } },


leftPriceScale: {

visible: !!state.positions[cfg.ticker],

borderColor: '#2a2e39',

scaleMargins: { top: 0.1, bottom: 0.25 }

},


timeScale: {

timeVisible: true,

secondsVisible: false,

borderColor: '#2a2e39',

minBarSpacing: 0.005,

tickMarkFormatter: (time, tickMarkType, locale) => {

const d = new Date(time * 1000);

if (tickMarkType < 3) return `${d.getMonth() + 1}/${d.getDate()}`;

return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });

}

},


crosshair: { mode: 0 },

rightPriceScale: { scaleMargins: { top: 0.1, bottom: 0.25 } },

localization: {

timeFormatter: (t) => {

const d = new Date(t * 1000);

if (['1D', '1W', '1M'].includes(cfg.timeframe)) return d.toLocaleDateString();

return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });

}

}

});


const volSeries = chart.addHistogramSeries({

color: '#26a69a',

priceFormat: { type: 'volume' },

priceScaleId: ''

});

volSeries.priceScale().applyOptions({ scaleMargins: { top: 0.75, bottom: 0 } });


const candleSeries = chart.addCandlestickSeries({ upColor: '#26a69a', downColor: '#ef5350', borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350' });



const pnlSeries = chart.addCandlestickSeries({

priceScaleId: 'left',

visible: !!state.positions[cfg.ticker],

upColor: 'transparent', downColor: 'transparent', wickUpColor: 'transparent', wickDownColor: 'transparent', borderVisible: false,

lastValueVisible: true, priceLineVisible: true, priceLineColor: '#2962ff',

priceFormat: {

type: 'custom',

formatter: price => {

const p = state.positions[cfg.ticker];

if (!p) return '';

const pnl = (price - p.avgPrice) * p.qty;

const absPnl = Math.abs(pnl);

if (absPnl >= 1e6) return (pnl >= 0 ? '+' : '') + (pnl / 1e6).toFixed(1) + 'M';

if (absPnl >= 1e3) return (pnl >= 0 ? '+' : '') + (pnl / 1e3).toFixed(1) + 'k';

return (pnl >= 0 ? '+' : '') + Math.round(pnl).toString();

}

}

});



const vwapSeries = chart.addLineSeries({ color: '#b042f5', lineWidth: 1, crosshairMarkerVisible: false, priceLineVisible: false });



const wrapper = document.getElementById(`content-${cfg.id}`);


const ro = new ResizeObserver(e => {

if (e.length && e[0].contentRect) {

if (typeof obj !== 'undefined') obj.width = e[0].contentRect.width;

chart.applyOptions({ width: e[0].contentRect.width, height: e[0].contentRect.height });

}

});

ro.observe(wrapper);



const obj = {

...cfg, chart, candleSeries, volSeries, vwapSeries, pnlSeries, entryLine: null,

lastPrice: 0, earliestTime: null, isLoading: false, width: wrapper.clientWidth,

lastCumVol: 0, lastCumPV: 0, lastSessionDate: null, ro

};


if (state.positions[cfg.ticker]) {

const p = state.positions[cfg.ticker];

obj.entryLine = candleSeries.createPriceLine({

price: p.avgPrice,

color: p.qty > 0 ? '#26a69a' : '#ef5350',

lineWidth: 2, lineStyle: 2, axisLabelVisible: true, title: ''

});

}


setTimeout(() => { if(wrapper.clientWidth > 0) chart.applyOptions({ width: wrapper.clientWidth, height: wrapper.clientHeight }); }, 200);



state.activeWidgets.push(obj);


const crosshairUnsub = p => {

const c = p.seriesData.get(candleSeries);

const v = p.seriesData.get(volSeries);

const vw = p.seriesData.get(vwapSeries);

const el = document.getElementById(`lg-${cfg.id}`);


if (c && el) {

const d = new Date(p.time * 1000);

const isDailyPlus = ['1D', '1W', '1M'].includes(cfg.timeframe);

let dateStr = isDailyPlus ? d.toLocaleDateString() : `${d.getMonth() + 1}/${d.getDate()} ${d.getHours()}:${d.getMinutes().toString().padStart(2, '0')}`;

const color = c.close >= c.open ? '#26a69a' : '#ef5350';

const volVal = v ? v.value : 0;

const dolVol = volVal * c.close;

const w = obj.width || 400;


let items = [];

if (w >= 520) items.push(`<span style="color:#d1d4dc; font-weight:bold;">${dateStr}</span>`);

if (w >= 320) {

items.push(`O:<span style="color:${color}">${c.open.toFixed(2)}</span>`);

items.push(`H:<span style="color:${color}">${c.high.toFixed(2)}</span>`);

items.push(`L:<span style="color:${color}">${c.low.toFixed(2)}</span>`);

}

items.push(`C:<span style="color:${color}">${c.close.toFixed(2)}</span>`);

if (w >= 250) items.push(`V:<span style="color:#d1d4dc">${formatNum(volVal)}</span>`);

if (w >= 320) items.push(`$V:<span style="color:#2962ff">${formatNum(dolVol)}</span>`);

if (w >= 450 && vw && vw.value) items.push(`VWAP:<span style="color:#b042f5">${vw.value.toFixed(2)}</span>`);


const pos = state.positions[cfg.ticker];

if (pos && w >= 320) {

const pnl = (c.close - pos.avgPrice) * pos.qty;

const sign = pnl >= 0 ? '+' : '';

const pnlColor = pnl >= 0 ? '#26a69a' : '#ef5350';

items.push(`P&L:<span style="color:${pnlColor}">${sign}${formatNum(pnl)}</span>`);

}


el.innerHTML = items.map(item => `<div class="legend-item">${item}</div>`).join('');

}

};

chart.subscribeCrosshairMove(crosshairUnsub);

obj.crosshairUnsub = crosshairUnsub;


let loadTimeout = null;

const rangeUnsub = range => {

if(range && range.from < 10 && !obj.isLoading) {

if (loadTimeout) clearTimeout(loadTimeout);

loadTimeout = setTimeout(() => { loadOlderData(obj); }, 300);

}

};

chart.timeScale().subscribeVisibleLogicalRangeChange(rangeUnsub);

obj.rangeUnsub = rangeUnsub;


await loadChartData(obj);

updateChartsRealtime();

}



function initTape(cfg) {

const obj = { ...cfg, lastTradeId: 0, lastPrice: 0 };

state.activeWidgets.push(obj);

updateTapeIndividual();

}



async function updateTapeIndividual() {

if(!state.isLeftOpen && !state.isRightOpen && !state.chartsConfig.some(x=>x.type==='tape')) return;

const k=await getKeys(); if(!k) return;

const start = new Date(Date.now() - 86400000 * 3).toISOString();

const tapeWidgets = state.activeWidgets.filter(w => w.type === 'tape');

const exMap = { 'A':'AMEX', 'B':'BSE', 'C':'NSX', 'D':'ADF', 'I':'ISE', 'J':'EDGA', 'K':'EDGX', 'M':'CHX', 'N':'NYSE', 'P':'ARCA', 'Q':'NASD', 'S':'CTS', 'T':'TRF', 'V':'IEX', 'X':'PHLX', 'Y':'BYX', 'Z':'BZX' };



const widgetsByTicker = {};

tapeWidgets.forEach(w => {

if (!widgetsByTicker[w.ticker]) widgetsByTicker[w.ticker] = [];

widgetsByTicker[w.ticker].push(w);

});



await Promise.all(Object.entries(widgetsByTicker).map(async ([ticker, widgets]) => {

if (!widgets.some(w => document.getElementById(`tape-list-${w.id}`)?.offsetParent !== null)) return;

const u = `https://data.alpaca.markets/v2/stocks/${ticker}/trades?limit=50&sort=desc&feed=sip&start=${start}`;

try {

const res = await fetch(u, { headers: { "APCA-API-KEY-ID": k.k, "APCA-API-SECRET-KEY": k.s } });

if(!res.ok) return;

const json = await res.json();


if(json.trades) {

if (!MarketData.trades[ticker]) MarketData.trades[ticker] = [];

let masterList = MarketData.trades[ticker];


const newTrades = json.trades.filter(t => !masterList.some(existing => existing.i === t.i));

if(newTrades.length > 0) {

masterList = [...masterList, ...newTrades].sort((a, b) => new Date(b.t) - new Date(a.t));

if(masterList.length > 50) masterList = masterList.slice(0, 50);

MarketData.trades[ticker] = masterList;

}


widgets.forEach(w => {

const list = document.getElementById(`tape-list-${w.id}`);

if(!list || list.offsetParent === null) return;


const rows = masterList.map((t, i) => {

const prevPrice = (i < masterList.length - 1) ? masterList[i+1].p : t.p;

const c = t.p >= prevPrice ? 't-up' : 't-down';

const bg = t.s >= 100 ? 't-large' : '';

const tm = new Date(t.t).toLocaleTimeString([],{hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'});

const exch = exMap[t.x] || t.x || '-';


return `<div class="tape-row ${bg}">

<span class="tape-time">${tm}</span>

<span class="tape-price ${c}">${t.p.toFixed(2)}</span>

<span class="tape-size">${t.s}</span>

<span class="tape-venue">${exch}</span>

</div>`;

});

list.innerHTML = rows.join('');

});

}

} catch(e) { }

}));

}


async function initFundChart(cfg) {

const container = document.getElementById(`canvas-${cfg.id}`);

await new Promise(r => setTimeout(r, 50));

const chart = LightweightCharts.createChart(container, {

layout: { background: { type: 'solid', color: '#131722' }, textColor: '#d1d4dc' },

grid: { vertLines: { visible: false }, horzLines: { color: '#1e222d', style: 2 } },

timeScale: { borderColor: '#2a2e39', fixLeftEdge: true, fixRightEdge: true },

rightPriceScale: { borderColor: '#2a2e39' }

});

const series = chart.addHistogramSeries({ color: '#2962ff' });

const wrapper = document.getElementById(`content-${cfg.id}`);


const ro = new ResizeObserver(e => { if (e.length && e[0].contentRect) chart.applyOptions({ width: e[0].contentRect.width, height: e[0].contentRect.height }); });

ro.observe(wrapper);


setTimeout(() => { if(wrapper.clientWidth > 0) chart.applyOptions({ width: wrapper.clientWidth, height: wrapper.clientHeight }); }, 200);

const obj = { ...cfg, chart, series, type: 'fund-chart', ro };

state.activeWidgets.push(obj);

updateFundChart(obj);

}



async function updateFundChart(cfg) {

const w = state.activeWidgets.find(x => x.id === cfg.id);

if(!w) return;

const data = await fetchFinancials(w.ticker);

if(!data || !data.series) return;

const rawSeries = data.series[w.fundPeriod];

if(!rawSeries) return;

let metricData = rawSeries[w.fundMetric];

if (!metricData) metricData = rawSeries[w.fundMetric + 'TTM'];

if (!metricData) return;

const chartData = metricData.filter(d => d.v !== null).map(d => ({ time: d.period, value: d.v, color: d.v >= 0 ? '#26a69a' : '#ef5350' })).sort((a,b) => new Date(a.time) - new Date(b.time));

w.series.setData(chartData);

w.chart.timeScale().fitContent();

}



async function initSentimentChart(cfg) {

const container = document.getElementById(`canvas-${cfg.id}`);

await new Promise(r => setTimeout(r, 50));

const chart = LightweightCharts.createChart(container, {

layout: { background: { type: 'solid', color: '#131722' }, textColor: '#d1d4dc' },

grid: { vertLines: { color: '#1e222d' }, horzLines: { color: '#1e222d' } },

timeScale: { borderColor: '#2a2e39' },

rightPriceScale: { scaleMargins: { top: 0.05, bottom: 0.35 } }

});

const sentSeries = chart.addHistogramSeries({

color: '#26a69a',

priceFormat: { type: 'volume' },

priceScaleId: ''

});

sentSeries.priceScale().applyOptions({ scaleMargins: { top: 0.70, bottom: 0 } });

const candleSeries = chart.addCandlestickSeries({ upColor: '#26a69a', downColor: '#ef5350', borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350' });

const wrapper = document.getElementById(`content-${cfg.id}`);


const ro = new ResizeObserver(e => { if (e.length && e[0].contentRect) chart.applyOptions({ width: e[0].contentRect.width, height: e[0].contentRect.height }); });

ro.observe(wrapper);


setTimeout(() => { if(wrapper.clientWidth > 0) chart.applyOptions({ width: wrapper.clientWidth, height: wrapper.clientHeight }); }, 200);

const obj = { ...cfg, chart, candleSeries, sentSeries, type: 'sentiment', ro };

state.activeWidgets.push(obj);

updateSentimentChart(obj);

}



async function updateSentimentChart(w) {

const k = await getKeys();

const fhToken = await getFinnhubKey();

if(!k || !fhToken) { log("Missing API Keys for Sentiment Chart", "error"); return; }


const start = new Date(Date.now() - (3650 * 86400000)).toISOString();

const end = new Date().toISOString();


const cUrl = `https://data.alpaca.markets/v2/stocks/${w.ticker}/bars?timeframe=1Month&start=${start}&end=${end}&limit=500&adjustment=split&sort=desc`;

const today = new Date().toISOString().split('T')[0];

const sUrl = `https://finnhub.io/api/v1/stock/insider-sentiment?symbol=${w.ticker}&from=2020-01-01&to=${today}&token=${fhToken}`;

try {

const [cRes, sRes] = await Promise.all([ fetch(cUrl, { headers: { "APCA-API-KEY-ID": k.k, "APCA-API-SECRET-KEY": k.s } }), fetch(sUrl) ]);

if(cRes.ok && sRes.ok) {

const cJson = await cRes.json();

const sJson = await sRes.json();

if(cJson.bars) {

const candles = parseBars(cJson.bars.reverse());

w.candleSeries.setData(candles.ohlc);

}

if(sJson.data) {

const sentData = sJson.data.map(d => {

const time = new Date(d.year, d.month - 1, 1).getTime() / 1000;

return { time: time, value: d.mspr, color: d.mspr > 0 ? '#26a69a' : '#ef5350' };

}).sort((a,b) => a.time - b.time);

w.sentSeries.setData(sentData);

}

w.chart.timeScale().fitContent();

}

} catch(e) { log(e.message); }

}



async function loadChartData(c) {

c.isLoading = true;

const data = await MarketData.getBars(c.ticker, c.timeframe);


if(data) {

c.candleSeries.setData(data.ohlc);

c.pnlSeries.setData(data.ohlc);

c.volSeries.setData(data.vol);

if(c.vwapSeries) c.vwapSeries.setData(data.vwap);


c.lastCumVol = data.lastCumVol;

c.lastCumPV = data.lastCumPV;

c.lastSessionDate = data.lastDate;



const len=data.ohlc.length;

if(len>250) c.chart.timeScale().setVisibleLogicalRange({from:len-250,to:len});

else c.chart.timeScale().fitContent();

c.lastPrice = data.ohlc[len-1].close;

c.earliestTime = data.ohlc[0].time;

}

c.isLoading = false;

}



async function loadOlderData(c) {

if(!c.earliestTime || c.isLoading) return;

c.isLoading = true;

const keys = await getKeys();

const visibleBars = Math.ceil(c.chart.timeScale().getVisibleLogicalRange().to - c.chart.timeScale().getVisibleLogicalRange().from);

const limit = Math.min(Math.max(visibleBars*2, 500), 10000);

const endStr = new Date((c.earliestTime - 1) * 1000).toISOString();

let tf = c.timeframe; if(tf==='4H') tf='4Hour'; if(tf==='1W') tf='1Week'; if(tf==='1M' || tf==='M') tf='1Month'; if(tf==='1H') tf='1Hour';

const url = `https://data.alpaca.markets/v2/stocks/${c.ticker}/bars?timeframe=${tf}&start=2015-01-01T00:00:00Z&end=${endStr}&limit=${limit}&sort=desc&adjustment=split`;

try {

const res = await fetch(url, { headers: { "APCA-API-KEY-ID": keys.k, "APCA-API-SECRET-KEY": keys.s } });

const json = await res.json();

if(json.bars) {

const oldBars = json.bars.reverse();

const data = parseBars(oldBars);

c.candleSeries.setData(data.ohlc.concat(c.candleSeries.data()));

c.pnlSeries.setData(data.ohlc.concat(c.pnlSeries.data()));

c.volSeries.setData(data.vol.concat(c.volSeries.data()));

if(c.vwapSeries) c.vwapSeries.setData(data.vwap.concat(c.vwapSeries.data()));

c.earliestTime = data.ohlc[0].time;

}

} catch(e) {} finally { c.isLoading = false; }

}



// --- REALTIME ---

async function updateChartsRealtime() {

const keys = await getKeys(); if(!keys) return;

const widgets = state.activeWidgets;

if(widgets.length === 0) return;

const symbols = [...new Set(widgets.map(w => w.ticker))];

const u = `https://data.alpaca.markets/v2/stocks/snapshots?symbols=${symbols.join(',')}`;

try {

const res = await fetch(u, { headers: { "APCA-API-KEY-ID": keys.k, "APCA-API-SECRET-KEY": keys.s } });

const json = await res.json();

symbols.forEach(sym => {

const d = json[sym] || json[sym.toUpperCase()];

if(d) MarketData.quotes[sym] = d;

});

widgets.forEach(w => {

const data = MarketData.quotes[w.ticker] || MarketData.quotes[w.ticker.toUpperCase()];

if(!data) return;

updateQuoteDisplay(w.id, data);

if(w.type === 'chart') {

const t = data.latestTrade;

if (t) {

const tradeTime = new Date(t.t).getTime() / 1000;

const currentData = w.candleSeries.data();

const lastBar = currentData.length > 0 ? currentData[currentData.length - 1] : null;


const durations = { '1Min': 60, '5Min': 300, '15Min': 900, '1H': 3600, '4H': 14400, '1D': 86400, '1W': 604800, '1M': 2592000 };

const dur = durations[w.timeframe] || 60;


let tfTime;

if (lastBar && tradeTime >= lastBar.time && tradeTime < lastBar.time + dur) {

tfTime = lastBar.time;

} else {

tfTime = snapTime(tradeTime, w.timeframe);

if (lastBar && tfTime <= lastBar.time) {

tfTime = lastBar.time + dur;

}

}


let newO = t.p, newH = t.p, newL = t.p, newC = t.p;

if (lastBar && lastBar.time === tfTime) {

newO = lastBar.open;

newH = Math.max(lastBar.high, t.p);

newL = Math.min(lastBar.low, t.p);

} else if (lastBar) {

newO = lastBar.close;

newH = Math.max(newO, t.p);

newL = Math.min(newO, t.p);

}


updateChartBar(w, tfTime, newO, newH, newL, newC, undefined, undefined);

}

}

if(w.type === 'financials') {

const track = document.getElementById(`rng-track-${w.id}`);

const arrow = document.getElementById(`rng-arrow-${w.id}`);

if(track && arrow && data.latestTrade) {

const h = parseFloat(track.dataset.high);

const l = parseFloat(track.dataset.low);

const p = data.latestTrade.p;

if(h > l) {

let pct = ((p - l) / (h - l)) * 100;

pct = Math.max(0, Math.min(100, pct));

arrow.style.left = `${pct}%`;

}

}

}

});

} catch(e) { console.error(e); }

}


function updateChartBar(w, time, open, high, low, close, vol, intervalVwap) {

if (!w.candleSeries || !w.volSeries) return;


const bar = { time: time, open: open, high: high, low: low, close: close, vw: intervalVwap };

w.candleSeries.update(bar);

w.pnlSeries.update(bar);


if (vol !== undefined) {

w.volSeries.update({ time: time, value: vol, color: close >= open ? '#26a69a' : '#ef5350' });


if (w.vwapSeries && intervalVwap) {

const dateStr = new Date(time * 1000).toLocaleDateString();


if (w.lastSessionDate) {

let tempCumVol = w.lastCumVol;

let tempCumPV = w.lastCumPV;


if (dateStr !== w.lastSessionDate) {

tempCumVol = 0;

tempCumPV = 0;

}


const currentSessionVwap = (tempCumPV + (vol * intervalVwap)) / (tempCumVol + vol);

w.vwapSeries.update({ time: time, value: currentSessionVwap });

}

}

}

w.lastPrice = close;

}


function updateQuoteDisplay(id, data) {

const q = data.latestQuote;

const t = data.latestTrade;

const d = data.dailyBar;

const l1 = document.getElementById(`l1-${id}`);

if(l1 && q) {

document.getElementById(`qb-p-${id}`).innerText = q.bp.toFixed(2);

document.getElementById(`qb-s-${id}`).innerText = q.bs;

document.getElementById(`qa-p-${id}`).innerText = q.ap.toFixed(2);

document.getElementById(`qa-s-${id}`).innerText = q.as;

if(t) { document.getElementById(`ql-${id}`).innerText = t.p.toFixed(2); }

if(d) document.getElementById(`qv-${id}`).innerText = formatNum(d.v);

}

}

function updateSidebarVisibility() { const l=document.getElementById('left-sidebar-area'), r=document.getElementById('right-sidebar-area'), bl=document.getElementById('leftToggle'), br=document.getElementById('rightToggle'); if(state.isLeftOpen){l.classList.remove('collapsed');bl.classList.add('active');}else{l.classList.add('collapsed');bl.classList.remove('active');} if(state.isRightOpen){r.classList.remove('collapsed');br.classList.add('active');}else{r.classList.add('collapsed');br.classList.remove('active');} setTimeout(() => { state.activeWidgets.forEach(w => { if(w.type==='chart') w.chart.applyOptions({ width: document.getElementById(`content-${w.id}`).clientWidth }); }); }, 300); }


async function getKeys() {

try {

const db = await openDB();

const tx = db.transaction("Settings", "readonly");

const getVal = (k) => new Promise(r => { const req = tx.objectStore("Settings").get(k); req.onsuccess = () => r(req.result ? req.result.value : ''); });

const [k, s] = await Promise.all([getVal("APCA_API_KEY_ID"), getVal("APCA_API_SECRET_KEY")]);

if (k && s) return { k, s };

} catch(e) {}


if(typeof getApiKeys!=='function') return null;

const k=await getApiKeys();

return (k.alpacaKey||k.k) ? {k:k.alpacaKey||k.k, s:k.alpacaSecret||k.s} : null;

}


function getLinkClass(g){if(g==='A')return'link-blue';if(g==='B')return'link-orange';if(g==='C')return'link-green';if(g==='D')return'link-purple';if(g==='E')return'link-red';return'';}


async function refreshPortfolioTape() {

const k = await getKeys(); if(!k) return;

const t = Object.keys(state.positions);

t.forEach(s => state.newsSymbols.add(s));


if (t.length) {

try {

const r = await fetch(`https://data.alpaca.markets/v2/stocks/snapshots?symbols=${t.join(',')}`, { headers: { "APCA-API-KEY-ID": k.k, "APCA-API-SECRET-KEY": k.s } });

if (r.ok) {

const j = await r.json();


Object.keys(j).forEach(sym => {

MarketData.quotes[sym] = j[sym] || MarketData.quotes[sym];

});


const i = Object.entries(j).map(([s,d]) => parseSnapshot(d,s)).filter(Boolean);

state.tapeData.portfolio = generateSectionHTML("PORTFOLIO", i);

renderTape();


if (document.getElementById('positionsModal').style.display === 'flex') {

renderPositionsList();

}

}

} catch(e) {}

} else {

state.tapeData.portfolio = '';

renderTape();

}

}


async function refreshScreenerTape(){const k=await getKeys();if(!k)return;fetchScreener(k,'movers?top=10','gainers',"TOP GAINERS").then(h=>{state.tapeData.gainers=h;renderTape();});fetchScreener(k,'movers?top=10','losers',"TOP LOSERS").then(h=>{state.tapeData.losers=h;renderTape();});fetchActives(k,'volume','activeVol',"ACTIVE (VOL)");fetchActives(k,'trades','activeTrd',"ACTIVE (TRD)");if(!state.tapeData.activeVol)fetchFallbackIndices(k);}

async function fetchScreener(k,e,n,t){try{const r=await fetch(`https://data.alpaca.markets/v1beta1/screener/stocks/${e}`,{headers:{"APCA-API-KEY-ID":k.k,"APCA-API-SECRET-KEY":k.s}});if(!r.ok)return'';const j=await r.json();const i=(j[n]||[]).map(x=>({symbol:x.symbol,price:x.price,pct:x.percent_change}));i.forEach(x=>state.newsSymbols.add(x.symbol));return generateSectionHTML(t,i);}catch(e){return'';}}

async function fetchActives(k,b,sk,t){try{const r=await fetch(`https://data.alpaca.markets/v1beta1/screener/stocks/most-actives?by=${b}&top=10`,{headers:{"APCA-API-KEY-ID":k.k,"APCA-API-SECRET-KEY":k.s}});if(!r.ok)return;const j=await r.json();const l=j.most_actives||[];const s=l.map(i=>i.symbol);s.forEach(sym=>state.newsSymbols.add(sym));if(!s.length)return;const sr=await fetch(`https://data.alpaca.markets/v2/stocks/snapshots?symbols=${s.join(',')}`,{headers:{"APCA-API-KEY-ID":k.k,"APCA-API-SECRET-KEY":k.s}});if(sr.ok){const sj=await sr.json();const i=Object.entries(sj).map(([sy,d])=>parseSnapshot(d,sy));state.tapeData[sk]=generateSectionHTML(t,i);renderTape();}}catch(e){}}

async function fetchFallbackIndices(k){try{const r=await fetch(`https://data.alpaca.markets/v2/stocks/snapshots?symbols=SPY,QQQ,IWM,DIA`,{headers:{"APCA-API-KEY-ID":k.k,"APCA-API-SECRET-KEY":k.s}});if(r.ok){const j=await r.json();const i=Object.entries(j).map(([s,d])=>parseSnapshot(d,s));state.tapeData.fallback=generateSectionHTML("INDICES",i);renderTape();}}catch(e){} }


async function refreshNewsTape() {

const k = await getKeys(); if(!k) return;


const portfolioSyms = Object.keys(state.positions);

const otherSyms = Array.from(state.newsSymbols).filter(sym => !state.positions[sym]);


let s = [...portfolioSyms, ...otherSyms];

if (!s.length) s = ['SPY', 'QQQ'];


const r = s.slice(0, 25).join(',');

const u = `https://data.alpaca.markets/v1beta1/news?symbols=${r}&limit=50`;


try {

const rs = await fetch(u, { headers: { "APCA-API-KEY-ID": k.k, "APCA-API-SECRET-KEY": k.s } });

if (!rs.ok) return;

const j = await rs.json();

if (j.news) {

let h = '';

j.news.forEach(n => {

const sym = (n.symbols && n.symbols.length) ? n.symbols[0] : 'MKT';

const t = new Date(n.updated_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

h += `<a class="news-item" href="${n.url}" target="_blank"><span class="news-sym">${sym}</span><span>${n.headline}</span><span class="news-time">${t}</span></a>`;

});

document.getElementById('newsTrack').innerHTML = h + h;

}

} catch(e) {}

}


function renderTape() { const t=document.getElementById('tickerTrack'); let h=[state.tapeData.portfolio,state.tapeData.gainers,state.tapeData.losers,state.tapeData.activeVol,state.tapeData.activeTrd,state.tapeData.fallback].join(''); if(h.trim().length===0) h='<span style="color:#666;">Loading...</span>'; t.innerHTML=h+h; }

function parseSnapshot(d,s) { if(!d||!d.latestTrade)return null; const p=d.latestTrade.p; const pr=d.prevDailyBar?d.prevDailyBar.c:p; return{symbol:s,price:p,pct:((p-pr)/pr)*100}; }

function generateSectionHTML(t,i) { if(!i||i.length===0)return ''; let h=`<div class="ticker-section"><span class="section-label">${t}</span>`; i.forEach(x=>{ if(!x)return; const c=x.pct>=0?'color-up':'color-down'; const ic=x.pct>=0?'▲':'▼'; h+=`<div class="ticker-item" onclick="changeTicker('${x.symbol}')"><span class="t-sym">${x.symbol}</span><span class="t-price">${x.price.toFixed(2)}</span><span class="t-chg ${c}">${ic} ${Math.abs(x.pct).toFixed(2)}%</span></div>`; }); return h+'</div>'; }



document.addEventListener('DOMContentLoaded', init);

document.getElementById('debugToggle').addEventListener('click', () => logger.style.display = logger.style.display === 'block' ? 'none' : 'block');

document.getElementById('refreshBtn').addEventListener('click', () => state.activeWidgets.forEach(w=>{if(w.type==='chart')loadChartData(w)}));

document.getElementById('settingsBtn').addEventListener('click', () => toggleSettings(true));

document.getElementById('clearLogs').addEventListener('click', () => logger.innerHTML = '');

</script>

</body>

</html>

This is exactly what is happening: your desktop CSS is hardcoding the popup windows to width: 650px;, which is physically wider than an iPhone screen. iOS Safari panics, tries to fake a massive desktop monitor, and then traps you in a zoomed-in state where you can't reach the close button!

I have updated your entire charts.html file to be perfectly mobile-responsive. You can completely replace your current file with the code block below.

Here are the specific fixes I injected to make it work natively on iOS:

The Viewport Lock: Updated the <meta> tag to include maximum-scale=1.0, user-scalable=no. This explicitly forbids iOS from fake-zooming and locks the UI perfectly to the screen size.

Relative Modal Sizing: Changed your hardcoded 650px inline styles to max-width: 650px; width: 95%; max-height: 90vh;. Now the popup will never be wider than the physical phone screen, and it won't bleed off the bottom.

Responsive Form Inputs: I grouped your ticker/qty/price inputs into a new .pos-inputs class. On desktop, they stay on one line. On mobile, they cleanly wrap into a 2x2 grid so they are easy to tap.

Touch Targets & Scrolling: Added a @media screen and (max-width: 768px) block to slightly enlarge your toolbar buttons (Apple recommends minimum 44px touch targets) and allow horizontal swiping natively inside the positions table so the columns don't get crushed.

Copy and paste this directly over your current charts.html:

HTML
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Pro Charting (Final Integrated)</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.3.0/css/all.min.css">
    <style>
        :root {
            --bg-color: #131722; --panel-color: #1e222d; --border-color: #2a2e39;
            --text-color: #d1d4dc; --accent-color: #2962ff; --up-color: #26a69a; --down-color: #ef5350;
        }
        body, html { margin: 0; padding: 0; height: 100%; width: 100%; background-color: var(--bg-color); color: var(--text-color); font-family: -apple-system, BlinkMacSystemFont, sans-serif; overflow: hidden; }
        
        /* APP SHELL */
        .app-container { 
            display: grid; 
            grid-template-rows: 40px 30px 1fr; 
            height: 100vh; width: 100vw; overflow: hidden;
        }
        
        /* TOOLBAR */
        .toolbar { 
            background-color: var(--panel-color); border-bottom: 1px solid var(--border-color); 
            display: grid; grid-template-columns: auto minmax(0, 1fr) auto; align-items: center; 
            padding: 0 10px; gap: 15px; z-index: 200; overflow: visible; 
        }
        .left-controls, .right-controls { display: flex; gap: 10px; align-items: center; white-space: nowrap; }
        .brand { font-weight: bold; color: var(--text-color); text-decoration: none; margin-right: 5px; font-size: 14px; }
        
        /* TAPE */
        .ticker-container { position: relative; width: 100%; height: 100%; overflow: hidden; display: flex; align-items: center; margin: 0 10px; min-width: 0; mask-image: linear-gradient(to right, transparent, black 5%, black 95%, transparent); -webkit-mask-image: linear-gradient(to right, transparent, black 5%, black 95%, transparent); }
        .ticker-track { display: flex; gap: 20px; animation: tickerScroll 90s linear infinite; align-items: center; height: 100%; }
        .ticker-item { display: flex; align-items: center; gap: 6px; font-family: 'Monaco', monospace; font-size: 13px; cursor: pointer; white-space: nowrap; }
        .t-sym { font-weight: bold; color: #d1d4dc; } .t-price { color: #d1d4dc; } .t-chg { font-weight: bold; }
        
        .ticker-section { display: flex; align-items: center; margin-right: 20px; } 

        @keyframes tickerScroll { 0% { transform: translateX(0); } 100% { transform: translateX(-50%); } }
        .section-label { color: #2962ff; font-weight: 900; font-size: 11px; letter-spacing: 1px; border: 1px solid #2962ff; padding: 2px 6px; border-radius: 4px; margin-right: 5px; }

        /* NEWS */
        .news-strip { background-color: #131722; border-bottom: 1px solid var(--border-color); display: flex; align-items: center; overflow: hidden; white-space: nowrap; position: relative; font-family: 'Roboto', sans-serif; font-size: 12px; z-index: 190; }
        .news-label { background: #2962ff; color: white; font-weight: bold; padding: 0 10px; height: 100%; display: flex; align-items: center; font-size: 11px; z-index: 2; box-shadow: 5px 0 10px #131722; }
        .news-track { display: flex; gap: 40px; animation: tickerScroll 400s linear infinite; padding-left: 20px; align-items: center; height: 100%; }
        .news-item { display: flex; align-items: center; gap: 8px; color: #d1d4dc; text-decoration: none; cursor: pointer; }
        .news-sym { color: #ff9800; font-weight: bold; font-size: 10px; border: 1px solid #ff9800; padding: 1px 4px; border-radius: 3px; }
        .news-time { color: #787b86; font-size: 10px; }

        /* WORKSPACE */
        .workspace { display: flex; flex-grow: 1; overflow: hidden; position: relative; z-index: 1; height: 100%; }
        #main-area { flex-grow: 1; display: grid; grid-template-columns: 1fr; grid-template-rows: 1fr; gap: 1px; background-color: var(--border-color); height: 100%; min-width: 0; overflow: hidden; }
        
        /* SIDEBARS */
        #left-sidebar-area, #right-sidebar-area { width: 280px; min-width: 280px; background-color: #131722; display: flex; flex-direction: column; flex-shrink: 0; transition: all 0.2s ease-out; overflow: hidden; height: 100%; }
        #left-sidebar-area { border-right: 1px solid var(--border-color); }
        #right-sidebar-area { border-left: 1px solid var(--border-color); }
        .collapsed { width: 0 !important; min-width: 0 !important; border: none !important; }

        /* WIDGETS */
        .chart-slot, .widget-box { position: relative; background-color: var(--bg-color); min-width: 0; min-height: 0; display: flex; flex-direction: column; overflow: hidden; border-bottom: 1px solid var(--border-color); flex: 1; }
        
        /* HEADER */
        .widget-header { height: 34px; min-height: 34px; background-color: var(--panel-color); border-bottom: 1px solid var(--border-color); display: flex; align-items: center; padding: 0 6px; gap: 4px; flex-shrink: 0; z-index: 30; white-space: nowrap; overflow: visible; }
        .widget-controls-left { display: flex; flex-direction: row; gap: 4px; align-items: center; flex-shrink: 0; }
        .mini-input { background: rgba(42, 46, 57, 0.9); border: 1px solid #444; color: #fff; width: 46px; font-size: 11px; text-transform: uppercase; border-radius: 3px; padding: 3px 0; font-weight: bold; text-align: center; height: 24px; box-sizing: border-box; }
        .link-btn { width: 24px; min-width: 24px; height: 24px; border-radius: 3px; cursor: pointer; border: 1px solid #444; background: rgba(42, 46, 57, 0.9); display: flex; align-items: center; justify-content: center; font-size: 10px; color: #777; flex-shrink: 0; }
        
        /* DROPDOWNS */
        .metric-select { background: rgba(42, 46, 57, 0.9); border: 1px solid #444; color: #d1d4dc; font-size: 11px; height: 24px; border-radius: 3px; outline: none; max-width: 100px; cursor: pointer; }
        .freq-btn { width: 24px; height: 24px; border: 1px solid #444; background: #1e222d; color: #d1d4dc; font-size: 10px; font-weight: bold; border-radius: 3px; cursor: pointer; display: flex; align-items: center; justify-content: center; }
        .freq-btn:hover { background: #2962ff; color: white; border-color: #2962ff; }

        /* MORPH CONTAINERS (TIMEFRAME & TYPE) */
        .tf-morph-container, .type-morph-container { display: flex; align-items: center; background: rgba(42, 46, 57, 0.9); border: 1px solid #444; border-radius: 3px; height: 24px; overflow: hidden; transition: all 0.2s ease-out; position: relative; }
        
        .tf-single, .type-single { padding: 0 4px; font-size: 11px; font-weight: bold; color: #d1d4dc; cursor: pointer; line-height: 20px; min-width: 18px; text-align: center; display: flex; align-items: center; justify-content: center; height: 100%; }
        
        .tf-options-row, .type-options-row { display: none; flex-direction: row; }
        
        /* Expanded State */
        .tf-morph-container.expanded, .type-morph-container.expanded { position: absolute; z-index: 100; width: auto; top: 0; left: 0; box-shadow: 0 5px 15px rgba(0,0,0,0.5); }
        .tf-morph-container.expanded .tf-single, .type-morph-container.expanded .type-single { display: none; }
        .tf-morph-container.expanded .tf-options-row, .type-morph-container.expanded .type-options-row { display: flex; }
        
        .tf-btn, .type-btn-opt { border-right: 1px solid #444; padding: 0 8px; cursor: pointer; color: #d1d4dc; font-size: 11px; line-height: 22px; font-weight: bold; background: #1e222d; white-space: nowrap; display: flex; align-items: center; gap: 5px; }
        .tf-btn:hover, .tf-btn.active, .type-btn-opt:hover, .type-btn-opt.active { background: #2962ff; color: white; }
        .type-btn-opt:last-child, .tf-btn:last-child { border-right: none; }

        /* L1 QUOTES (Overlay) */
        .l1-overlay { margin-left: auto; z-index: 25; display: flex; gap: 2px; align-items: center; font-family: 'Roboto', sans-serif; pointer-events: none; }
        .l1-btn { display: flex; flex-direction: column; align-items: center; justify-content: center; min-width: 35px; padding: 0 6px; height: 24px; border-radius: 3px; color: white; cursor: default; box-shadow: 0 2px 5px rgba(0,0,0,0.4); font-size: 10px; line-height: 1; pointer-events: auto; box-sizing: border-box; }
        .l1-bid { background: #ef5350; } .l1-ask { background: #2962ff; } 
        .l1-center { display: flex; flex-direction: column; align-items: center; justify-content: center; min-width: 35px; padding: 0 6px; height: 24px; background: rgba(30, 34, 45, 0.9); border-radius: 3px; pointer-events: auto; box-sizing: border-box; }
        .l1-price { font-size: 11px; font-weight: bold; line-height: 1; }
        .l1-size { font-size: 9px; opacity: 0.8; line-height: 1; }
        .last-price { font-size: 11px; font-weight: bold; color: #d1d4dc; line-height: 1; }
        .day-vol { font-size: 9px; color: #787b86; margin-top: 1px; font-family: 'Monaco', monospace; }

        /* CONTENT */
        .widget-content { flex: 1; position: relative; display: flex; flex-direction: column; min-height: 0; background: #131722; overflow: hidden; }
        .chart-canvas-fill { position: absolute; top: 0; left: 0; right: 0; bottom: 0; z-index: 1; }
        
        /* LEGEND */
        .chart-legend { position: absolute; top: 8px; left: 8px; z-index: 20; font-size: 11px; font-family: monospace; pointer-events: none; white-space: nowrap; }
        .legend-row { display: flex; gap: 6px; margin-bottom: 2px; align-items: center; }
        .legend-item { display: flex; align-items: center; gap: 0px; } 

        .tape-content { flex: 1; overflow-y: auto; font-family: 'Monaco', monospace; font-size: 12px; background: #131722; margin: 0; padding: 0; }
        /* TAPE GRID FOR 4 COLUMNS */
        .tape-header-row { display: grid; grid-template-columns: 2fr 1.5fr 1.5fr 1fr; padding: 4px 8px; border-bottom: 1px solid #2a2e39; background: #161a25; font-size: 10px; color: #787b86; flex-shrink: 0; }
        .tape-row { display: grid; grid-template-columns: 2fr 1.5fr 1.5fr 1fr; padding: 3px 8px; border-bottom: 1px solid #1e222d; cursor: default; align-items: center; }
        .tape-row:hover { background-color: #1e222d; }
        .tape-time { color: #787b86; } 
        .tape-price { font-weight: bold; text-align: right; } 
        .tape-size { text-align: right; color: #d1d4dc; }
        .tape-venue { text-align: right; font-size: 10px; color: #787b86; }

        /* LINKS & UTILS */
        .link-blue { background-color: #2962ff; border-color: #2962ff; color: white; }
        .link-orange { background-color: #ff9800; border-color: #ff9800; color: white; }
        .link-green { background-color: #00c853; border-color: #00c853; color: white; }
        .link-purple { background-color: #aa00ff; border-color: #aa00ff; color: white; }
        .link-red { background-color: #d50000; border-color: #d50000; color: white; }
        .t-up { color: var(--up-color); } .t-down { color: var(--down-color); } .t-large { background-color: rgba(41, 98, 255, 0.15); }
        .color-up { color: var(--up-color); } .color-down { color: var(--down-color); }
        
        .dropdown { position: relative; }
        .dropdown-menu { position: absolute; top: 100%; right: 0; background: var(--panel-color); border: 1px solid var(--border-color); z-index: 5000; display: none; min-width: 160px; border-radius: 4px; box-shadow: 0 4px 20px rgba(0,0,0,0.8); }
        .dropdown.open .dropdown-menu { display: block; }
        .dropdown-item { padding: 8px 12px; color: #d1d4dc; cursor: pointer; font-size: 13px; border-bottom: 1px solid #2a2e39; }
        .dropdown-item:hover { background: #2a2e39; }
        .dropdown-header { padding: 4px 12px; font-size: 10px; color: #2962ff; font-weight: bold; background: #15181f; }
        
        .icon-btn { background: none; border: none; color: #787b86; cursor: pointer; font-size: 14px; width: 30px; height: 30px; display: flex; align-items: center; justify-content: center; border-radius: 4px; }
        .icon-btn:hover { color: var(--text-color); background: #2a2e39; }
        .icon-btn.active { color: var(--accent-color); background: rgba(41, 98, 255, 0.1); }
        
        #screen-logger { position: absolute; right: 20px; top: 80px; bottom: 20px; width: 300px; background: rgba(0,0,0,0.9); border-left: 1px solid #444; font-family: monospace; font-size: 11px; color: #0f0; padding: 10px; overflow-y: auto; z-index: 9999; display: none; }
        .log-line { margin-bottom: 4px; border-bottom: 1px solid #333; } .log-err { color: #f55; }
        
        /* NEW: Responsive Modal CSS */
        .modal-overlay { position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.7); z-index: 6000; display: none; align-items: center; justify-content: center; }
        .modal-content { 
            background: var(--panel-color); padding: 20px; border-radius: 6px; border: 1px solid var(--border-color); 
            box-shadow: 0 10px 30px rgba(0,0,0,0.5); width: 95%; max-height: 90vh; display: flex; flex-direction: column; box-sizing: border-box; 
        }
        
        .form-group { margin-bottom: 15px; } .form-label { display: block; font-size: 12px; color: #787b86; margin-bottom: 5px; }
        .form-input { width: 100%; background: #131722; border: 1px solid #2a2e39; color: #fff; padding: 8px; border-radius: 4px; box-sizing: border-box; outline:none; }
        .form-input:focus { border-color: #2962ff; }
        .modal-actions { display: flex; justify-content: flex-end; gap: 10px; margin-top: 10px; flex-shrink: 0; }
        .btn-p { background: var(--accent-color); color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-weight: bold; }
        .btn-s { background: transparent; color: #d1d4dc; border: 1px solid #444; padding: 6px 12px; border-radius: 4px; cursor: pointer; }
        .btn-danger { background: #d50000; border: none; color: white; padding: 6px 12px; border-radius: 4px; cursor: pointer; margin-right: auto; }

        /* FINANCIALS WIDGET STYLES */
        .fin-container { padding: 12px; overflow-y: auto; height: 100%; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
        .fin-section { margin-bottom: 16px; border-bottom: 1px solid #2a2e39; padding-bottom: 12px; }
        .fin-section:last-child { border-bottom: none; margin-bottom: 0; }
        .fin-title { font-size: 11px; font-weight: 700; color: #2962ff; text-transform: uppercase; margin-bottom: 10px; letter-spacing: 0.5px; opacity: 0.9; }
        .fin-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(95px, 1fr)); gap: 8px 12px; }
        .fin-item { display: flex; flex-direction: column; justify-content: flex-start; overflow: hidden; }
        .fin-label { color: #787b86; font-size: 10px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; margin-bottom: 2px; }
        .fin-val { color: #e1e1e1; font-weight: 600; font-family: 'Monaco', 'Consolas', monospace; font-size: 12px; letter-spacing: -0.5px; }

        /* 52 WEEK RANGE BAR */
        .range-container { margin: 5px 0 20px 0; background: rgba(255,255,255,0.02); padding: 8px; border-radius: 4px; border: 1px solid #2a2e39; }
        .range-labels { display: flex; justify-content: space-between; font-size: 10px; color: #787b86; margin-bottom: 6px; font-weight: 600; }
        .range-track { height: 6px; background: #131722; border-radius: 3px; position: relative; width: 100%; margin: 0; border: 1px solid #2a2e39; }
        .range-fill { height: 100%; background: linear-gradient(90deg, #ef5350 0%, #2962ff 50%, #26a69a 100%); border-radius: 3px; position: absolute; left: 0; opacity: 0.8; }
        .range-arrow { position: absolute; top: -5px; width: 0; height: 0; border-left: 5px solid transparent; border-right: 5px solid transparent; border-top: 6px solid #fff; transform: translateX(-50%); z-index: 2; transition: left 0.3s cubic-bezier(0.4, 0.0, 0.2, 1); filter: drop-shadow(0 2px 2px rgba(0,0,0,0.5)); }
        .range-dates { display: flex; justify-content: space-between; font-size: 9px; color: #555; margin-top: 6px; }

        /* NEW: MOBILE SPECIFIC FIXES */
        @media screen and (max-width: 768px) {
            .app-container { grid-template-rows: 45px 30px 1fr; } /* Taller touch toolbar */
            .toolbar { padding: 0 5px; gap: 5px; }
            .icon-btn { width: 34px; height: 34px; font-size: 16px; } /* Apple minimum touch targets */
            .btn-p, .btn-s, .btn-danger { padding: 12px 14px; font-size: 14px; }
            .modal-content { padding: 15px; }
            
            /* Wrap inputs properly so they don't squish */
            .pos-inputs { flex-wrap: wrap; }
            .pos-inputs > input { width: calc(50% - 5px) !important; margin-bottom: 5px; }
            .pos-inputs > button { width: 100% !important; margin-top: 5px; }
            
            /* Allow horizontal scrolling on crushed tables */
            #positionsList { overflow-x: auto; }
            #positionsList table { min-width: 500px; }
        }
    </style>
</head>
<body>

<div class="app-container">
    <div class="toolbar">
        <div class="left-controls">
            <a href="./portfolio.html" class="brand"><i class="fas fa-chart-line"></i></a>
            <button class="icon-btn" id="leftToggle" title="Left Sidebar"><i class="fas fa-columns"></i></button>
        </div>
        <div class="ticker-container" id="tickerContainer">
            <div class="ticker-track" id="tickerTrack"><span style="font-size:11px; color:#666;">Loading Market Data...</span></div>
        </div>
        <div class="right-controls">
            <button class="icon-btn active" id="rightToggle" title="Right Sidebar"><i class="fas fa-columns"></i></button>
            <div class="dropdown" id="layoutDropdown">
                <button class="icon-btn" title="Layouts"><i class="fas fa-th-large"></i></button>
                <div class="dropdown-menu">
                    <div class="dropdown-header">MAIN WORKSPACE</div>
                    <div class="dropdown-item" onclick="setChartLayout('1x1')">Single Chart (1)</div>
                    <div class="dropdown-item" onclick="setChartLayout('2x1')">2 Stacked</div>
                    <div class="dropdown-item" onclick="setChartLayout('1x2')">2 Side-by-Side</div>
                    <div class="dropdown-item" onclick="setChartLayout('2x2')">Quad Grid (4)</div>
                    <div class="dropdown-item" onclick="setChartLayout('3x2')">6 Grid (3x2)</div>
                    <div class="dropdown-item" onclick="setChartLayout('2x3')">6 Grid (2x3)</div>
                    <div class="dropdown-item" onclick="setChartLayout('3x3')">9 Grid (3x3)</div>
                    <div class="dropdown-header">LEFT SIDEBAR</div>
                    <div class="dropdown-item" onclick="setLeftSidebarLayout(0)">Hidden</div>
                    <div class="dropdown-item" onclick="setLeftSidebarLayout(1)">1 Widget</div>
                    <div class="dropdown-item" onclick="setLeftSidebarLayout(2)">2 Widgets</div>
                    <div class="dropdown-item" onclick="setLeftSidebarLayout(3)">3 Widgets</div>
                    <div class="dropdown-header">RIGHT SIDEBAR</div>
                    <div class="dropdown-item" onclick="setRightSidebarLayout(0)">Hidden</div>
                    <div class="dropdown-item" onclick="setRightSidebarLayout(1)">1 Widget</div>
                    <div class="dropdown-item" onclick="setRightSidebarLayout(2)">2 Widgets</div>
                    <div class="dropdown-item" onclick="setRightSidebarLayout(3)">3 Widgets</div>
                </div>
            </div>
            <button class="icon-btn" id="positionsBtn" title="Manage Positions"><i class="fas fa-briefcase"></i></button>
            <button class="icon-btn" id="refreshBtn" title="Force Refresh"><i class="fas fa-sync-alt"></i></button>
            <button class="icon-btn" id="debugToggle" title="System Logs"><i class="fas fa-bug"></i></button>
            <button class="icon-btn" id="settingsBtn" title="Settings"><i class="fas fa-cog"></i></button>
        </div>
    </div>

    <div class="news-strip">
        <div class="news-label">NEWS</div>
        <div class="news-track" id="newsTrack"><span style="color:#666;">Waiting for headlines...</span></div>
    </div>

    <div class="workspace">
        <div id="left-sidebar-area" class="sidebar collapsed"></div>
        <div id="main-area"></div>
        <div id="right-sidebar-area" class="sidebar"></div>
    </div>

    <div id="screen-logger">
        <div style="display:flex; justify-content:space-between; margin-bottom:10px;"><strong>System Logs</strong><button id="clearLogs">Clear</button></div>
    </div>
</div>

<div id="settingsModal" class="modal-overlay">
    <div class="modal-content" style="max-width: 450px;">
        <h3 style="margin:0 0 15px 0; color:#d1d4dc;">API Configuration</h3>
        <div style="overflow-y:auto; flex-grow:1;">
            <div class="form-group"><label class="form-label">Alpaca API Key ID</label><input type="text" id="inpApiKey" class="form-input"></div>
            <div class="form-group"><label class="form-label">Alpaca Secret Key</label><input type="password" id="inpApiSecret" class="form-input"></div>
            <div class="form-group"><label class="form-label">Finnhub API Key</label><input type="password" id="inpFinnhubKey" class="form-input" placeholder="Optional (60 req/min limit)"></div>
            <div class="form-group"><label class="form-label">Sync Server Base URL</label><input type="text" id="inpSyncUrl" class="form-input" placeholder="https://api.yourdomain.com"></div>
            <div id="settingsError" style="color:#ef5350; font-size:12px; margin-bottom:15px; height:14px;"></div>
        </div>
        <div class="modal-actions">
            <button class="btn-danger" id="btnFactoryReset" onclick="nuclearReset()">Factory Reset</button>
            <button class="btn-s" onclick="toggleSettings(false)">Cancel</button>
            <button class="btn-p" onclick="saveApiSettings()">Save</button>
        </div>
    </div>
</div>

<div id="positionsModal" class="modal-overlay">
    <div class="modal-content" style="max-width: 650px;">
        <h3 style="margin:0 0 15px 0; color:#d1d4dc; flex-shrink: 0;">Portfolio Dashboard</h3>
        
        <div style="overflow-y:auto; flex-grow:1; padding-right:5px;">
            <div style="margin-bottom: 15px; background: #161a25; border: 1px solid #2a2e39; border-radius: 4px; overflow: hidden;">
                <div id="balancesHeader" style="display:flex; justify-content:space-between; padding: 10px 15px; cursor: pointer;" onclick="toggleBalancesBreakdown()">
                    <div style="display:flex; align-items:center; gap: 8px;">
                        <i id="balancesArrow" class="fas fa-chevron-right" style="color:#787b86; font-size:12px; transition: transform 0.2s;"></i>
                        <div style="display:flex; flex-direction:column;">
                            <span style="color:#787b86; font-size:10px; text-transform:uppercase; letter-spacing:0.5px;">Total Cash</span> 
                            <span id="valTotalCash" style="color:#26a69a; font-weight:bold; font-size:16px;">$0.00</span>
                        </div>
                    </div>
                    <div style="display:flex; flex-direction:column; text-align:right;">
                        <span style="color:#787b86; font-size:10px; text-transform:uppercase; letter-spacing:0.5px;">Buying Power</span> 
                        <span id="valBuyingPower" style="color:#2962ff; font-weight:bold; font-size:16px;">$0.00</span>
                    </div>
                </div>
                <div id="balancesBreakdown" style="display:none; border-top: 1px solid #2a2e39; padding: 10px 15px; background: #131722;">
                    </div>
            </div>

            <div class="pos-inputs" style="display:flex; gap:10px; margin-bottom: 5px;">
                <input type="text" id="posSym" class="form-input" placeholder="Ticker (AAPL)" style="width: 25%;">
                <input type="text" inputmode="decimal" id="posQty" class="form-input" placeholder="Qty" style="width: 25%;">
                <input type="text" inputmode="decimal" id="posPrice" class="form-input" placeholder="Avg Price" style="width: 30%;">
                <button class="btn-p" onclick="addPosition()" style="width: 20%;">Save</button>
            </div>
            <div id="posError" style="color:#ef5350; font-size:11px; height:14px; margin-bottom:10px;"></div>
            
            <div id="positionsList" style="border: 1px solid #2a2e39; border-radius: 4px; padding: 10px; background: #131722; margin-bottom: 5px;">
                </div>
        </div>
        
        <div class="modal-actions" style="justify-content: space-between;">
            <button class="btn-s" style="color:#2962ff; border-color:#2962ff;" onclick="syncPositionsFromUrl()"><i class="fas fa-cloud-download-alt"></i> Sync</button>
            <button class="btn-s" onclick="togglePositionsModal(false)">Close</button>
        </div>
    </div>
</div>

<script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
<script src="shared.js"></script>

<script>
    // --- UTILS ---
    const logger = document.getElementById('screen-logger');
    function log(msg, type='info') {
        const div = document.createElement('div'); div.className = 'log-line ' + (type === 'error' ? 'log-err' : '');
        div.innerText = `> ${msg}`; logger.appendChild(div); logger.scrollTop = logger.scrollHeight;
    }
    function formatNum(num) { if(!num) return '0'; if(num>=1e9) return (num/1e9).toFixed(2)+'B'; if(num>=1e6) return (num/1e6).toFixed(2)+'M'; if(num>=1e3) return (num/1e3).toFixed(1)+'k'; return Math.floor(num).toString(); }
    
    function snapTime(t, f) {
        if(f==='1Min' || f==='1m') return Math.floor(t/60)*60;
        if(f==='5Min' || f==='5m') return Math.floor(t/300)*300;
        if(f==='15Min' || f==='15m') return Math.floor(t/900)*900;
        if(f==='1H' || f==='1h') return Math.floor((t - 1800)/3600)*3600 + 1800; // Market opens on the half hour (09:30)
        if(f==='4H' || f==='4h') return Math.floor(t/14400)*14400; 
        
        const d = new Date(t * 1000);
        if(f==='1D' || f==='1d') { d.setUTCHours(0,0,0,0); return Math.floor(d.getTime()/1000); }
        if(f==='1W' || f==='1w') { d.setUTCDate(d.getUTCDate() - d.getUTCDay() + 1); d.setUTCHours(0,0,0,0); return Math.floor(d.getTime()/1000); }
        if(f==='1M' || f==='M') { d.setUTCDate(1); d.setUTCHours(0,0,0,0); return Math.floor(d.getTime()/1000); }
        return t;
    }
    
    function parseBars(b) {
        const o=[], v=[], w=[];
        let cumVol = 0;
        let cumPV = 0;
        let lastDate = null;

        b.forEach(x => {
            const t = new Date(x.t).getTime() / 1000;
            const dateStr = new Date(x.t).toLocaleDateString();
            
            if (dateStr !== lastDate) {
                cumVol = 0;
                cumPV = 0;
                lastDate = dateStr;
            }
            
            const barAvg = x.vw || ((x.h + x.l + x.c) / 3);
            
            cumVol += x.v;
            cumPV += (barAvg * x.v);
            
            const sessionVwap = (cumVol === 0) ? 0 : (cumPV / cumVol);
            
            o.push({time: t, open: x.o, high: x.h, low: x.l, close: x.c}); 
            v.push({time: t, value: x.v, color: (x.c >= x.o ? '#26a69a' : '#ef5350')});
            w.push({time: t, value: sessionVwap});
        });
        
        return {ohlc: o, vol: v, vwap: w, lastCumVol: cumVol, lastCumPV: cumPV, lastDate: lastDate};
    }

    // --- CONFIG & CONSTANTS ---
    const LAYOUT_VERSION = 39; 
    
    const FUND_METRICS = {
        'Valuation': { 'pe': 'P/E Ratio', 'ps': 'P/S Ratio', 'pb': 'P/B Ratio', 'evEbitda': 'EV/EBITDA', 'evRevenue': 'EV/Revenue', 'ev': 'Ent. Value', 'pfcf': 'P/FCF', 'ptbv': 'Price/Tang.Book' },
        'Profitability': { 'grossMargin': 'Gross Margin %', 'operatingMargin': 'Oper. Margin %', 'netMargin': 'Net Margin %', 'pretaxMargin': 'Pretax Margin %', 'fcfMargin': 'FCF Margin %', 'roe': 'ROE %', 'roa': 'ROA %' },
        'Per Share': { 'eps': 'EPS', 'salesPerShare': 'Sales/Share', 'bookValue': 'Book/Share' },
        'Balance Sheet': { 'currentRatio': 'Current Ratio', 'quickRatio': 'Quick Ratio', 'totalDebtToEquity': 'Debt/Equity' }
    };

    const state = {
        layoutVersion: LAYOUT_VERSION,
        isLeftOpen: false, isRightOpen: true,
        layoutMode: '1x1',
        chartsConfig: [{ id: 'm0', type: 'chart', ticker: 'AAPL', timeframe: '5Min', linkGroup: 'A' }],
        leftWidgetsConfig: [], 
        rightWidgetsConfig: [{ id: 'w-right-0', type: 'tape', ticker: 'AAPL', linkGroup: 'A' }],
        activeWidgets: [], 
        refreshInterval: null, screenerInterval: null, tapeInterval: null, newsInterval: null,
        portfolioTickers: [], tapeData: { portfolio: '', gainers: '', losers: '', activeVol: '', activeTrd: '', fallback: '' },
        newsSymbols: new Set(),
        positions: {},
        balances: { total: { cash: 0, buyingPower: 0 } }
    };
    
    let editingSymbol = null; 

    // --- CENTRALIZED DATA STORE ---
    const MarketData = {
        quotes: {},
        trades: {},
        barRequests: {},
        
        async getBars(ticker, timeframe) {
            const cacheKey = `${ticker}_${timeframe}`;
            if (this.barRequests[cacheKey]) return this.barRequests[cacheKey]; 
            
            const p = (async () => {
                const keys = await getKeys(); if(!keys) return null;
                const end = new Date().toISOString(); 
                
                const timeframesDays = { '1Min': 5, '1m': 5, '5Min': 10, '5m': 10, '15Min': 14, '15m': 14, '1H': 60, '1h': 60, '4H': 120, '4h': 120, '1D': 730, '1d': 730, '1W': 1825, '1w': 1825, '1M': 3650, 'M': 3650 };
                const daysBack = timeframesDays[timeframe] || 30;
                const start = new Date(Date.now() - (daysBack * 86400000)).toISOString();

                let tf = timeframe; if(tf==='4H') tf='4Hour'; if(tf==='1W') tf='1Week'; if(tf==='1M' || tf==='M') tf='1Month'; if(tf==='1H') tf='1Hour';
                const url = `https://data.alpaca.markets/v2/stocks/${ticker}/bars?timeframe=${tf}&start=${start}&end=${end}&limit=10000&adjustment=split&sort=desc`; 
                try {
                    const res = await fetch(url, { headers: { "APCA-API-KEY-ID": keys.k, "APCA-API-SECRET-KEY": keys.s } });
                    const json = await res.json();
                    if(json.bars) return parseBars(json.bars.reverse());
                } catch(e) { log(`Err loading ${ticker}: ${e.message}`); }
                return null;
            })();
            
            this.barRequests[cacheKey] = p;
            p.finally(() => setTimeout(() => { delete this.barRequests[cacheKey]; }, 5000));
            return p;
        }
    };

    // --- GLOBAL ACTIONS ---
    window.setWidgetType = function(id, type) { 
        const cfg = findConfig(id); 
        if(cfg) { 
            cfg.type = type;
            if (cfg.type === 'fund-chart') {
                if(!cfg.fundPeriod) cfg.fundPeriod = 'quarterly';
                if(!cfg.fundMetric) cfg.fundMetric = 'eps';
            }
            if (cfg.type === 'chart' && !cfg.timeframe) cfg.timeframe = '5Min';
            
            saveState(); 
            renderAll(); 
        } 
    };
    
    window.toggleTypeMorph = function(id, e) { 
        e.stopPropagation(); 
        document.querySelectorAll('.tf-morph-container').forEach(el => el.classList.remove('expanded'));
        document.querySelectorAll('.type-morph-container').forEach(el => el.classList.remove('expanded')); 
        const el = document.getElementById(`type-morph-${id}`); 
        if(el) el.classList.add('expanded'); 
    };

    window.cycleLink = function(id) { const groups = [null, 'A', 'B', 'C', 'D', 'E']; const cfg = findConfig(id); if(cfg) { cfg.linkGroup = groups[(groups.indexOf(cfg.linkGroup) + 1) % groups.length]; saveState(); renderAll(); } };
    window.updateWidgetTicker = function(id, input, e) { if(e.key === 'Enter') { const cfg = findConfig(id); if(cfg) { const val = input.value.toUpperCase(); cfg.ticker = val; syncLinkedWidgets(cfg.linkGroup, val); saveState(); renderAll(); input.blur(); } } };
    window.setWidgetTf = function(id, val, e) { e.stopPropagation(); const cfg = findConfig(id); if(cfg) { cfg.timeframe = val; saveState(); renderAll(); } };
    window.toggleTfMorph = function(id, e) { e.stopPropagation(); document.querySelectorAll('.type-morph-container').forEach(el => el.classList.remove('expanded')); document.querySelectorAll('.tf-morph-container').forEach(el => el.classList.remove('expanded')); const el = document.getElementById(`tf-morph-${id}`); if(el) el.classList.add('expanded'); };
    
    window.setFundMetric = function(id, val) { const cfg = findConfig(id); if(cfg) { cfg.fundMetric = val; const w = state.activeWidgets.find(x => x.id === id); if(w) w.fundMetric = val; saveState(); updateFundChart(cfg); } };
    window.toggleFundPeriod = function(id) { const cfg = findConfig(id); if(cfg) { const newVal = (cfg.fundPeriod === 'quarterly' ? 'annual' : 'quarterly'); cfg.fundPeriod = newVal; const w = state.activeWidgets.find(x => x.id === id); if(w) w.fundPeriod = newVal; const btn = document.getElementById(`fp-btn-${id}`); if(btn) btn.innerText = (newVal === 'quarterly' ? 'Q' : 'Y'); saveState(); updateFundChart(cfg); } };

    window.setChartLayout = function(mode) { state.layoutMode = mode; const [r, c] = mode.split('x').map(Number); const total = r * c; if(total > state.chartsConfig.length) { for(let i=state.chartsConfig.length; i<total; i++) state.chartsConfig.push({ id: `m${i}`, type: 'chart', ticker: 'AAPL', timeframe: '5Min', linkGroup: null }); } else { state.chartsConfig = state.chartsConfig.slice(0, total); } saveState(); renderAll(); };
    window.setLeftSidebarLayout = function(n) { updateSidebarConfig('left', n); };
    window.setRightSidebarLayout = function(n) { updateSidebarConfig('right', n); };

    function findConfig(id) { return state.chartsConfig.find(x=>x.id===id) || state.leftWidgetsConfig.find(x=>x.id===id) || state.rightWidgetsConfig.find(x=>x.id===id); }
    function syncLinkedWidgets(group, ticker) { if(!group) return; [...state.chartsConfig, ...state.leftWidgetsConfig, ...state.rightWidgetsConfig].forEach(c => { if(c.linkGroup === group) c.ticker = ticker; }); }
    function updateSidebarConfig(side, count) { const arr = (side==='left') ? state.leftWidgetsConfig : state.rightWidgetsConfig; const prefix = (side==='left') ? 'l' : 'r'; if(count === 0) { if(side==='left') state.leftWidgetsConfig=[]; else state.rightWidgetsConfig=[]; if(side==='left') state.isLeftOpen=false; else state.isRightOpen=false; } else { if(count > arr.length) { for(let i=arr.length; i<count; i++) arr.push({ id: `${prefix}${i}`, type: 'tape', ticker: 'AAPL', linkGroup: null }); } else if(count < arr.length) { if(side==='left') state.leftWidgetsConfig = arr.slice(0,count); else state.rightWidgetsConfig = arr.slice(0,count); } if(side==='left') state.isLeftOpen = true; else state.isRightOpen = true; } saveState(); renderAll(); }
    
    window.toggleSidebar = function(side) {
        if (side === 'left') state.isLeftOpen = !state.isLeftOpen;
        else state.isRightOpen = !state.isRightOpen;
        saveState();
        renderAll();
    };

    window.changeTicker = function(sym) {
        if (state.chartsConfig.length > 0) {
            const mainChart = state.chartsConfig[0];
            syncLinkedWidgets(mainChart.linkGroup, sym);
            mainChart.ticker = sym;
            saveState();
            renderAll();
        }
    };

    async function initFinancials(cfg) {
        const container = document.getElementById(`fin-content-${cfg.id}`);
        container.innerHTML = '<span style="color:#666; font-size:11px;">Loading Financials...</span>';
        
        const data = await fetchFinancials(cfg.ticker);
        
        if (!data || !data.metric) {
            container.innerHTML = '<span style="color:#666; font-size:11px;">No Financial Data Available</span>';
            return;
        }

        const m = data.metric;
        const fmt = (v, d=2) => (v !== undefined && !isNaN(v)) ? Number(v).toFixed(d) : '-';

        let html = '';
        
        html += `<div class="fin-section"><div class="fin-title">Key Ratios</div><div class="fin-grid">`;
        html += `<div class="fin-item"><span class="fin-label">P/E</span><span class="fin-val">${fmt(m.pe)}</span></div>`;
        html += `<div class="fin-item"><span class="fin-label">P/S</span><span class="fin-val">${fmt(m.ps)}</span></div>`;
        html += `<div class="fin-item"><span class="fin-label">ROE</span><span class="fin-val">${fmt(m.roe, 1)}%</span></div>`;
        html += `<div class="fin-item"><span class="fin-label">Net Margin</span><span class="fin-val">${fmt(m.netMargin, 1)}%</span></div>`;
        html += `</div></div>`;

        html += `<div class="fin-section"><div class="fin-title">Balance Sheet</div><div class="fin-grid">`;
        html += `<div class="fin-item"><span class="fin-label">Debt/Eq</span><span class="fin-val">${fmt(m.totalDebtToEquity)}</span></div>`;
        html += `<div class="fin-item"><span class="fin-label">Cur. Ratio</span><span class="fin-val">${fmt(m.currentRatio)}</span></div>`;
        html += `<div class="fin-item"><span class="fin-label">Book Value</span><span class="fin-val">${fmt(m.bookValue)}</span></div>`;
        html += `</div></div>`;

        html += `<div class="range-container">`;
        html += `<div class="range-labels"><span>52-Week Range</span></div>`;
        html += `<div class="range-track" id="rng-track-${cfg.id}" data-high="${m.high52Week}" data-low="${m.low52Week}">`;
        html += `<div class="range-fill"></div><div class="range-arrow" id="rng-arrow-${cfg.id}"></div>`;
        html += `</div>`;
        html += `<div class="range-dates"><span id="rng-low-${cfg.id}">$${fmt(m.low52Week)}</span><span id="rng-high-${cfg.id}">$${fmt(m.high52Week)}</span></div>`;
        html += `</div>`;

        container.innerHTML = html;
    }
    
    // --- DB ---
    function openDB() {
        return new Promise((resolve, reject) => {
            if (!window.indexedDB) return reject(new Error("IndexedDB is blocked by browser settings"));
            
            // Version 6: Safely recreates required object stores without destructive wipe
            const req = indexedDB.open("ProfitLadderDB", 6); 
            req.onupgradeneeded = (event) => {
                const db = event.target.result;
                if (!db.objectStoreNames.contains("Settings")) db.createObjectStore("Settings", { keyPath: "key" });
                if (!db.objectStoreNames.contains("Positions")) db.createObjectStore("Positions", { keyPath: "symbol" });
                if (!db.objectStoreNames.contains("Financials")) db.createObjectStore("Financials", { keyPath: "symbol" });
            };
            req.onsuccess = (event) => resolve(event.target.result);
            req.onerror = (event) => reject(new Error(event.target.error || "Unknown DB Error"));
        });
    }
    
    async function getFinnhubKey() {
        try {
            const db = await openDB();
            const tx = db.transaction("Settings", "readonly");
            const req = tx.objectStore("Settings").get("FINNHUB_API_KEY");
            return new Promise(r => {
                req.onsuccess = () => r(req.result ? req.result.value : null);
                req.onerror = () => r(null);
            });
        } catch(e) { return null; }
    }
    
    window.nuclearReset = function() { 
        const btn = document.getElementById('btnFactoryReset');
        if (btn.innerText === "Factory Reset") {
            btn.innerText = "Are you sure?";
            setTimeout(() => { if (btn) btn.innerText = "Factory Reset"; }, 3000);
        } else {
            indexedDB.deleteDatabase("ProfitLadderDB").onsuccess = () => window.location.reload(); 
        }
    };

    async function saveState() { try { const db = await openDB(); const tx = db.transaction("Settings", "readwrite"); const s = tx.objectStore("Settings"); s.put({ key: "layoutConfig", value: JSON.stringify({ m: state.chartsConfig, l: state.leftWidgetsConfig, r: state.rightWidgetsConfig, lm: state.layoutMode, lo: state.isLeftOpen, ro: state.isRightOpen, ver: state.layoutVersion }) }); } catch(e) { } }
    
    async function loadSavedState() { 
        try { 
            const db = await openDB(); 
            const tx = db.transaction("Settings", "readonly"); 
            const req = tx.objectStore("Settings").get("layoutConfig"); 
            await new Promise(r => { req.onsuccess = () => { if(req.result) { const d = JSON.parse(req.result.value); if(d.ver !== LAYOUT_VERSION) return r(); state.chartsConfig = d.m || state.chartsConfig; state.leftWidgetsConfig = d.l || state.leftWidgetsConfig; state.rightWidgetsConfig = d.r || state.rightWidgetsConfig; state.layoutMode = d.lm || '1x1'; state.isLeftOpen = d.lo; state.isRightOpen = d.ro; } r(); }; req.onerror = r; }); 
            
            // Instantly load cached balances from the previous session
            const reqBal = tx.objectStore("Settings").get("CACHED_BALANCES");
            await new Promise(r => { reqBal.onsuccess = () => { if(reqBal.result) state.balances = reqBal.result.value || {total:{cash:0, buyingPower:0}}; r(); }; reqBal.onerror = r; });
        } catch(e) { } 
    }
    
    async function resetLayout(c=true) { if(!c) { executeReset(); } else { if (document.getElementById('posError')) { document.getElementById('posError').innerText = "Use Settings -> Factory Reset to clear data."; } } function executeReset() { state.chartsConfig = [{ id: 'm0', type: 'chart', ticker: 'AAPL', timeframe: '5Min', linkGroup: 'A' }]; state.leftWidgetsConfig = []; state.rightWidgetsConfig = [{ id: 'w-right-0', type: 'tape', ticker: 'AAPL', linkGroup: 'A' }]; state.layoutMode = '1x1'; state.layoutVersion = LAYOUT_VERSION; state.isLeftOpen = false; state.isRightOpen = true; saveState().then(()=>window.location.reload()); } }
    
    // --- SETTINGS MODAL ---
    function toggleSettings(show) { 
        document.getElementById('settingsModal').style.display = show ? 'flex' : 'none'; 
        if(show) { 
            document.getElementById('settingsError').innerText = '';
            document.getElementById('btnFactoryReset').innerText = 'Factory Reset';
            (async () => { 
                const db = await openDB(); 
                const tx = db.transaction("Settings","readonly"); 
                const store = tx.objectStore("Settings"); 
                const getVal = (k) => new Promise(r => { const req = store.get(k); req.onsuccess = () => r(req.result ? req.result.value : ''); }); 
                const [k, s, f, u] = await Promise.all([getVal("APCA_API_KEY_ID"), getVal("APCA_API_SECRET_KEY"), getVal("FINNHUB_API_KEY"), getVal("SYNC_URL")]); 
                document.getElementById('inpApiKey').value = k; 
                document.getElementById('inpApiSecret').value = s; 
                document.getElementById('inpFinnhubKey').value = f; 
                if (document.getElementById('inpSyncUrl')) document.getElementById('inpSyncUrl').value = u; 
            })(); 
        } 
    }
    async function saveApiSettings() { 
        const k = document.getElementById('inpApiKey').value.trim(); 
        const s = document.getElementById('inpApiSecret').value.trim(); 
        const f = document.getElementById('inpFinnhubKey').value.trim(); 
        const u = document.getElementById('inpSyncUrl').value.trim(); 
        if(!k||!s) return document.getElementById('settingsError').innerText = "Alpaca keys required"; 
        try{ 
            const db=await openDB(); 
            const tx=db.transaction("Settings","readwrite"); 
            const store=tx.objectStore("Settings"); 
            store.put({key:"APCA_API_KEY_ID",value:k}); 
            store.put({key:"APCA_API_SECRET_KEY",value:s}); 
            store.put({key:"FINNHUB_API_KEY",value:f}); 
            store.put({key:"SYNC_URL",value:u}); 
            tx.oncomplete=()=>window.location.reload(); 
        } catch(e){ document.getElementById('settingsError').innerText = "Error: "+e.message; } 
    }

    // --- POSITIONS MANAGEMENT & LIVE DASHBOARD ---
    async function loadPositions() {
        const db = await openDB();
        const tx = db.transaction("Positions", "readonly");
        const p = await new Promise(r => { tx.objectStore("Positions").getAll().onsuccess = e => r(e.target.result); });
        state.positions = {};
        p.forEach(pos => {
            const sym = pos.symbol || pos.tickerSymbol; 
            const q = parseFloat(pos.qty);
            const a = parseFloat(pos.avgPrice);
            if (sym && !isNaN(q) && !isNaN(a)) {
                state.positions[sym] = { qty: q, avgPrice: a, breakdown: pos.breakdown || {} };
            }
        });
    }
    
    let isBalancesExpanded = false;
    window.toggleBalancesBreakdown = function() {
        isBalancesExpanded = !isBalancesExpanded;
        document.getElementById('balancesBreakdown').style.display = isBalancesExpanded ? 'block' : 'none';
        document.getElementById('balancesArrow').style.transform = isBalancesExpanded ? 'rotate(90deg)' : 'rotate(0deg)';
    };

    window.renderBalancesUI = function() {
        const fmt = n => n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        const b = state.balances || {};
        const tot = b.total || {cash:0, buyingPower:0};
        
        const cashEl = document.getElementById('valTotalCash');
        const bpEl = document.getElementById('valBuyingPower');
        if (cashEl) cashEl.innerText = '$' + fmt(tot.cash || 0);
        if (bpEl) bpEl.innerText = '$' + fmt(tot.buyingPower || 0);
        
        let bdHtml = '';
        for (const [broker, bal] of Object.entries(b)) {
            if (broker === 'total') continue;
            // Format broker name nicely (e.g. "nbdb" -> "NBDB", "questrade" -> "Questrade")
            const brokerName = broker.length <= 4 ? broker.toUpperCase() : broker.charAt(0).toUpperCase() + broker.slice(1);
            bdHtml += `<div style="display:flex; justify-content:space-between; margin-bottom:5px; font-size:12px;"><span style="color:#787b86;">${brokerName}</span><span style="color:#d1d4dc;">Cash: $${fmt(bal.cash||0)} &nbsp;|&nbsp; BP: $${fmt(bal.buyingPower||0)}</span></div>`;
        }
        
        if(!bdHtml) bdHtml = '<div style="font-size:12px; color:#787b86;">No account breakdown available. Sync required.</div>';
        const bdEl = document.getElementById('balancesBreakdown');
        if (bdEl) bdEl.innerHTML = bdHtml;
    };
    
    window.togglePosBreakdown = function(sym) {
        const rows = document.querySelectorAll(`.pos-bd-${sym}`);
        const arrow = document.getElementById(`arrow-${sym}`);
        let isHidden = true;
        rows.forEach(r => {
            isHidden = r.style.display === 'none';
            r.style.display = isHidden ? 'table-row' : 'none';
        });
        if(arrow) arrow.style.transform = isHidden ? 'rotate(90deg)' : 'rotate(0deg)';
    };

    window.togglePositionsModal = function(show) {
        document.getElementById('positionsModal').style.display = show ? 'flex' : 'none';
        if (show) {
            editingSymbol = null;
            document.getElementById('posSym').disabled = false;
            document.getElementById('posSym').style.opacity = '1';
            document.getElementById('posSym').value = '';
            document.getElementById('posQty').value = '';
            document.getElementById('posPrice').value = '';
            document.getElementById('posError').innerText = ''; 
            
            renderBalancesUI();
            renderPositionsList();
            refreshPortfolioTape(); 
        }
    };
    
    function renderPositionsList() {
        const list = document.getElementById('positionsList');
        if (Object.keys(state.positions).length === 0) {
            list.innerHTML = `<div style="text-align:center; color:#787b86; font-size:12px; margin-top:10px;">No active positions</div>`;
            return;
        }
        
        const fmtVal = (n) => n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        
        let h = `<table style="width:100%; text-align:left; font-size:12px; border-collapse:collapse;">
                    <tr style="border-bottom:1px solid #2a2e39; color:#787b86;">
                        <th style="padding-bottom:5px;">Symbol</th>
                        <th style="padding-bottom:5px; text-align:right;">Qty</th>
                        <th style="padding-bottom:5px; text-align:right;">Avg</th>
                        <th style="padding-bottom:5px; text-align:right;">Last</th>
                        <th style="padding-bottom:5px; text-align:right;">Value</th>
                        <th style="padding-bottom:5px; text-align:right;">P&L ($)</th>
                        <th style="padding-bottom:5px; text-align:right;">P&L (%)</th>
                        <th style="padding-bottom:5px; text-align:right;">Actions</th>
                    </tr>`;
                    
        let totalCost = 0;
        let totalValue = 0;
        
        for (const [sym, p] of Object.entries(state.positions)) {
            const safeQty = (p.qty !== undefined && !isNaN(p.qty)) ? p.qty : 0;
            const safePrice = (p.avgPrice !== undefined && !isNaN(p.avgPrice)) ? p.avgPrice : 0;
            
            const costBasis = safeQty * safePrice;
            totalCost += costBasis;
            
            let currentPrice = safePrice;
            const md = MarketData.quotes[sym];
            if (md && md.latestTrade) currentPrice = md.latestTrade.p;
            else if (md && md.prevDailyBar) currentPrice = md.prevDailyBar.c;
            
            const value = safeQty * currentPrice;
            totalValue += value;
            
            const pnl = value - costBasis;
            const pnlPct = costBasis !== 0 ? (pnl / Math.abs(costBasis)) * 100 : 0;
            
            const pnlColor = pnl > 0 ? '#26a69a' : (pnl < 0 ? '#ef5350' : '#787b86');
            const sign = pnl > 0 ? '+' : ''; 
            
            // Generate interactive arrow logic if sub-accounts exist
            const hasBreakdown = p.breakdown && Object.keys(p.breakdown).length > 0;
            const pointerCss = hasBreakdown ? 'cursor:pointer;' : '';
            const arrowHtml = hasBreakdown ? `<i class="fas fa-chevron-right" id="arrow-${sym}" style="color:#787b86; font-size:10px; margin-right:6px; transition: transform 0.2s;"></i>` : `<span style="display:inline-block; width:16px;"></span>`;
            
            h += `<tr style="border-bottom:1px solid #1e222d; ${pointerCss}" ${hasBreakdown ? `onclick="togglePosBreakdown('${sym}')"` : ''}>
                    <td style="padding:8px 0; font-weight:bold; color:#d1d4dc;">${arrowHtml}${sym}</td>
                    <td style="text-align:right;">${safeQty}</td>
                    <td style="text-align:right;">$${fmtVal(safePrice)}</td>
                    <td style="text-align:right; color:#d1d4dc;">$${fmtVal(currentPrice)}</td>
                    <td style="text-align:right;">$${fmtVal(value)}</td>
                    <td style="text-align:right; color:${pnlColor}; font-weight:bold;">${sign}$${fmtVal(pnl)}</td>
                    <td style="text-align:right; color:${pnlColor};">${sign}${fmtVal(pnlPct)}%</td>
                    <td style="text-align:right; white-space:nowrap;">
                        <button class="icon-btn" style="display:inline-flex; width:24px;height:24px;font-size:11px;margin-right:4px;" onclick="event.stopPropagation(); editPosition('${sym}')" title="Edit"><i class="fas fa-pen"></i></button>
                        <button class="icon-btn" style="display:inline-flex; width:24px;height:24px;font-size:11px;color:#ef5350;" onclick="event.stopPropagation(); removePosition('${sym}')" title="Remove"><i class="fas fa-trash-alt"></i></button>
                    </td>
                  </tr>`;
                  
            // Inject hidden nested rows for breakdown
            if (hasBreakdown) {
                for (const [acc, data] of Object.entries(p.breakdown)) {
                    const subCostBasis = data.qty * data.avgPrice;
                    const subValue = data.qty * currentPrice;
                    const subPnl = subValue - subCostBasis;
                    const subPnlPct = subCostBasis !== 0 ? (subPnl / Math.abs(subCostBasis)) * 100 : 0;
                    const subPnlColor = subPnl > 0 ? '#26a69a' : (subPnl < 0 ? '#ef5350' : '#787b86');
                    const subSign = subPnl > 0 ? '+' : '';
                    const accName = acc.length <= 4 ? acc.toUpperCase() : acc.charAt(0).toUpperCase() + acc.slice(1);

                    h += `<tr class="pos-bd-${sym}" style="display:none; background:#161a25; border-bottom:1px dashed #2a2e39;">
                            <td style="padding:4px 0 4px 24px; color:#787b86; font-size:11px;">↳ ${accName}</td>
                            <td style="text-align:right; color:#787b86; font-size:11px;">${data.qty}</td>
                            <td style="text-align:right; color:#787b86; font-size:11px;">$${fmtVal(data.avgPrice)}</td>
                            <td style="text-align:right; color:#787b86; font-size:11px;">-</td>
                            <td style="text-align:right; color:#787b86; font-size:11px;">$${fmtVal(subValue)}</td>
                            <td style="text-align:right; color:${subPnlColor}; font-size:11px;">${subSign}$${fmtVal(subPnl)}</td>
                            <td style="text-align:right; color:${subPnlColor}; font-size:11px;">${subSign}${fmtVal(subPnlPct)}%</td>
                            <td></td>
                          </tr>`;
                }
            }
        }
        
        const totalPnl = totalValue - totalCost;
        const totalPnlPct = totalCost !== 0 ? (totalPnl / Math.abs(totalCost)) * 100 : 0;
        const totColor = totalPnl > 0 ? '#26a69a' : (totalPnl < 0 ? '#ef5350' : '#787b86');
        const totSign = totalPnl > 0 ? '+' : '';
        
        h += `<tr style="background:#161a25; border-top:2px solid #2a2e39;">
                <td colspan="4" style="padding:10px 10px 10px 0; font-weight:bold; color:#d1d4dc; text-align:right;">TOTALS</td>
                <td style="text-align:right; font-weight:bold; color:#d1d4dc;">$${fmtVal(totalValue)}</td>
                <td style="text-align:right; font-weight:bold; color:${totColor};">${totSign}$${fmtVal(totalPnl)}</td>
                <td style="text-align:right; font-weight:bold; color:${totColor};">${totSign}${fmtVal(totalPnlPct)}%</td>
                <td></td>
              </tr>`;
              
        h += `</table>`;
        list.innerHTML = h;
    }
    
    window.editPosition = function(sym) {
        const p = state.positions[sym];
        if (!p) return;
        
        document.getElementById('posSym').value = sym;
        document.getElementById('posQty').value = p.qty;
        document.getElementById('posPrice').value = p.avgPrice;
        
        editingSymbol = sym;
        document.getElementById('posSym').disabled = true;
        document.getElementById('posSym').style.opacity = '0.5';
        document.getElementById('posQty').focus();
    };
    
    window.addPosition = async function() {
        const errEl = document.getElementById('posError');
        errEl.innerText = '';
        
        try {
            const sym = document.getElementById('posSym').value.toUpperCase().trim();
            const rawQty = document.getElementById('posQty').value.replace(/,/g, '.').replace(/\s/g, '');
            const rawPrice = document.getElementById('posPrice').value.replace(/,/g, '.').replace(/\s/g, '');
            
            const qty = parseFloat(rawQty);
            const price = parseFloat(rawPrice);
            
            if(!sym || isNaN(qty) || isNaN(price) || qty === 0) {
                errEl.innerText = "Please enter valid position details (Qty ≠ 0).";
                return;
            }
            
            let newQty = qty;
            let newAvg = price;

            if (editingSymbol !== sym && state.positions[sym]) {
                const oldQty = state.positions[sym].qty;
                const oldAvg = state.positions[sym].avgPrice;
                newQty = oldQty + qty;
                
                if (newQty === 0) {
                    await removePosition(sym); 
                    return;
                }
                newAvg = ((oldQty * oldAvg) + (qty * price)) / newQty;
            }

            const db = await openDB();
            const tx = db.transaction("Positions", "readwrite");
            
            // Note: Manual overwrites reset the breakdown mapping to maintain logic integrity
            tx.objectStore("Positions").put({ symbol: sym, tickerSymbol: sym, ticker: sym, id: sym, key: sym, qty: newQty, avgPrice: newAvg, breakdown: {} });
            
            state.positions[sym] = { qty: newQty, avgPrice: newAvg, breakdown: {} };
            
            document.getElementById('posSym').value = '';
            document.getElementById('posQty').value = '';
            document.getElementById('posPrice').value = '';
            document.getElementById('posSym').disabled = false;
            document.getElementById('posSym').style.opacity = '1';
            editingSymbol = null;
            
            renderPositionsList();
            applyPositionsToCharts(); 
            refreshPortfolioTape(); 
        } catch(e) {
            errEl.innerText = "Save Error: " + (e.message || "Database access blocked");
        }
    };
    
    window.removePosition = async function(sym) {
        const db = await openDB();
        const tx = db.transaction("Positions", "readwrite");
        tx.objectStore("Positions").delete(sym);
        delete state.positions[sym];
        renderPositionsList();
        applyPositionsToCharts(); 
        refreshPortfolioTape(); 
    };

    window.syncPositionsFromUrl = async function(isSilent = false) {
        const errEl = document.getElementById('posError');
        if (!isSilent && errEl) {
            errEl.innerText = 'Syncing from Server...';
            errEl.style.color = '#26a69a';
        }
        
        try {
            const db = await openDB();
            const tx = db.transaction("Settings", "readonly");
            const req = tx.objectStore("Settings").get("SYNC_URL");
            let baseUrl = await new Promise(r => { req.onsuccess = () => r(req.result ? req.result.value : null); req.onerror = () => r(null); });
            
            if (!baseUrl) {
                if (!isSilent) throw new Error("No Sync Base URL configured in Settings.");
                return;
            }
            
            // Clean up base URL (strip trailing slashes)
            baseUrl = baseUrl.trim().replace(/\/+$/, '');
            
            // Fetch both positions and balances concurrently
            const [posRes, balRes] = await Promise.all([
                fetch(`${baseUrl}/positions.json`).catch(e => ({ error: e })),
                fetch(`${baseUrl}/balances.json`).catch(e => ({ error: e }))
            ]);
            
            if (posRes.error || !posRes.ok) throw new Error(`HTTP error fetching positions`);
            const posData = await posRes.json();
            
            let newPositions = [];
            
            // Prioritize the grouped "total" array from our Python server
            if (posData && posData.total && Array.isArray(posData.total)) {
                newPositions = posData.total.map(p => {
                    const sym = (p.symbol || p.ticker || '').toUpperCase();
                    const breakdown = {};
                    
                    // Dynamically scan ALL brokers provided by the server
                    Object.keys(posData).forEach(brokerKey => {
                        if (brokerKey !== 'total' && Array.isArray(posData[brokerKey])) {
                            const brokerPos = posData[brokerKey].find(x => (x.symbol || x.ticker || '').toUpperCase() === sym);
                            if (brokerPos) {
                                breakdown[brokerKey] = { 
                                    qty: parseFloat(brokerPos.qty), 
                                    avgPrice: parseFloat(brokerPos.avgPrice || brokerPos.price) 
                                };
                            }
                        }
                    });
                    
                    return { ...p, breakdown };
                });
            } else if (Array.isArray(posData)) { 
                newPositions = posData; 
            } else if (typeof posData === 'object') { 
                newPositions = Object.keys(posData).map(k => ({ symbol: k, ...posData[k] })); 
            }
            
            // Process balances if available
            if (!balRes.error && balRes.ok) {
                const balData = await balRes.json();
                if (balData && balData.total) {
                    state.balances = balData; 
                    
                    // Save balances to IndexedDB so they are instantly available next boot
                    const txSet = db.transaction("Settings", "readwrite");
                    txSet.objectStore("Settings").put({ key: "CACHED_BALANCES", value: state.balances });
                    
                    if (document.getElementById('positionsModal').style.display === 'flex') {
                        renderBalancesUI();
                    }
                }
            }
            
            const txPos = db.transaction("Positions", "readwrite");
            const store = txPos.objectStore("Positions");
            store.clear(); 
            
            state.positions = {};
            newPositions.forEach(p => {
                const sym = (p.symbol || p.ticker || '').toUpperCase();
                const qty = parseFloat(p.qty);
                const avgPrice = parseFloat(p.avgPrice || p.price);
                if (sym && !isNaN(qty) && !isNaN(avgPrice)) {
                    store.put({ symbol: sym, tickerSymbol: sym, ticker: sym, id: sym, key: sym, qty: qty, avgPrice: avgPrice, breakdown: p.breakdown || {} });
                    state.positions[sym] = { qty, avgPrice, breakdown: p.breakdown || {} };
                }
            });
            
            if (!isSilent) {
                if (errEl) {
                    errEl.style.color = '#26a69a';
                    errEl.innerText = 'Sync complete!';
                    setTimeout(() => { if(errEl.innerText === 'Sync complete!') errEl.innerText = ''; }, 3000);
                }
                renderPositionsList();
                applyPositionsToCharts();
                refreshPortfolioTape();
            }
        } catch(e) {
            if (!isSilent && errEl) {
                errEl.style.color = '#ef5350';
                errEl.innerText = "Sync Error: " + (e.message || "Unable to fetch JSON");
            } else {
                console.warn("Silent sync failed:", e);
            }
        }
    };

    function applyPositionsToCharts() {
        state.activeWidgets.forEach(w => {
            if (w.type === 'chart') {
                const hasPos = !!state.positions[w.ticker];
                w.chart.priceScale('left').applyOptions({ visible: hasPos });
                
                if (w.entryLine) { w.candleSeries.removePriceLine(w.entryLine); w.entryLine = null; }
                
                if (hasPos) {
                    const p = state.positions[w.ticker];
                    if (p && !isNaN(p.avgPrice) && !isNaN(p.qty)) {
                        w.entryLine = w.candleSeries.createPriceLine({
                            price: p.avgPrice,
                            color: p.qty > 0 ? '#26a69a' : '#ef5350',
                            lineWidth: 2,
                            lineStyle: 2,
                            axisLabelVisible: true,
                            title: ''
                        });
                        
                        w.pnlSeries.applyOptions({
                            visible: true,
                            priceFormat: {
                                type: 'custom',
                                formatter: price => {
                                    const pnl = (price - p.avgPrice) * p.qty;
                                    const absPnl = Math.abs(pnl);
                                    if (absPnl >= 1e6) return (pnl >= 0 ? '+' : '') + (pnl / 1e6).toFixed(1) + 'M';
                                    if (absPnl >= 1e3) return (pnl >= 0 ? '+' : '') + (pnl / 1e3).toFixed(1) + 'k';
                                    return (pnl >= 0 ? '+' : '') + Math.round(pnl).toString();
                                }
                            }
                        });
                    }
                } else {
                    if(w.pnlSeries) w.pnlSeries.applyOptions({ visible: false });
                }
            }
        });
    }

    // --- FINNHUB ---
    async function fetchFinancials(symbol) {
        const db = await openDB();
        const tx = db.transaction(["Settings", "Financials"], "readwrite");
        const cache = await new Promise(r => { const req = tx.objectStore("Financials").get(symbol); req.onsuccess = () => r(req.result); req.onerror = () => r(null); });
        const now = Date.now();
        if (cache && (now - cache.timestamp < 86400000)) return cache.data;
        const key = await new Promise(r => { const req = tx.objectStore("Settings").get("FINNHUB_API_KEY"); req.onsuccess = () => r(req.result ? req.result.value : null); });
        if (!key) return null;
        try {
            const url = `https://finnhub.io/api/v1/stock/metric?symbol=${symbol}&metric=all&token=${key}`;
            const res = await fetch(url);
            if (!res.ok) return null;
            const data = await res.json();
            const store = db.transaction("Financials", "readwrite").objectStore("Financials");
            store.put({ symbol: symbol, data: data, timestamp: now });
            return data;
        } catch (e) { return null; }
    }
    async function pruneFinancials() { const db = await openDB(); const tx = db.transaction("Financials", "readwrite"); const store = tx.objectStore("Financials"); const now = Date.now(); store.openCursor().onsuccess = (event) => { const cursor = event.target.result; if (cursor) { if (now - cursor.value.timestamp > 14400000) cursor.delete(); cursor.continue(); } }; }

    // --- INIT ---
    async function init() {
        log("Initializing...");
        try {
            try { await loadSavedState(); } catch(e) { console.warn("State load failed", e); }
            try { await loadPositions(); } catch(e) { console.warn("Positions load failed", e); }
            
            if(state.layoutVersion !== LAYOUT_VERSION) await resetLayout(false);
            
            // Render the UI and Charts INSTANTLY using local cache
            renderAll();
            
            try { refreshPortfolioTape(); } catch(e) {}
            try { refreshScreenerTape(); } catch(e) {}
            try { refreshNewsTape(); } catch(e) {}
            
            // NEW: Fire the server sync entirely in the background without blocking the UI
            syncPositionsFromUrl(true).catch(e => console.warn("Background auto-sync failed", e));
            
            pruneFinancials();
            
            clearInterval(state.refreshInterval); clearInterval(state.screenerInterval); clearInterval(state.tapeInterval);
            state.refreshInterval = setInterval(() => { updateChartsRealtime(); refreshPortfolioTape(); }, 5000);
            state.screenerInterval = setInterval(refreshScreenerTape, 60000);
            state.tapeInterval = setInterval(updateTapeIndividual, 1000);
            state.newsInterval = setInterval(refreshNewsTape, 120000);

            document.getElementById('layoutDropdown').addEventListener('click', e => { e.currentTarget.classList.toggle('open'); e.stopPropagation(); });
            document.addEventListener('click', () => { 
                document.getElementById('layoutDropdown').classList.remove('open'); 
                document.querySelectorAll('.tf-morph-container').forEach(el => el.classList.remove('expanded')); 
                document.querySelectorAll('.type-morph-container').forEach(el => el.classList.remove('expanded'));
            });
            document.getElementById('leftToggle').addEventListener('click', () => toggleSidebar('left'));
            document.getElementById('rightToggle').addEventListener('click', () => toggleSidebar('right'));
            document.getElementById('positionsBtn').addEventListener('click', () => togglePositionsModal(true));
        } catch(e) {
            console.error("Critical Init Error:", e);
        }
    }

    // --- RENDERERS ---
    function renderAll() {
        state.activeWidgets.forEach(w => {
            if (w.crosshairUnsub) w.chart.unsubscribeCrosshairMove(w.crosshairUnsub);
            if (w.rangeUnsub) w.chart.timeScale().unsubscribeVisibleLogicalRangeChange(w.rangeUnsub);
            
            if (w.chart && typeof w.chart.remove === 'function') w.chart.remove();
            if (w.ro && typeof w.ro.disconnect === 'function') w.ro.disconnect();
        });
        
        const activeTickers = new Set([...state.chartsConfig, ...state.leftWidgetsConfig, ...state.rightWidgetsConfig].map(c => c.ticker));
        Object.keys(MarketData.trades).forEach(t => { if(!activeTickers.has(t)) delete MarketData.trades[t]; });
        Object.keys(MarketData.quotes).forEach(t => { if(!activeTickers.has(t)) delete MarketData.quotes[t]; });
        
        Object.keys(MarketData.barRequests).forEach(k => {
            if(!activeTickers.has(k.split('_')[0])) delete MarketData.barRequests[k];
        });

        state.activeWidgets = [];
        const main = document.getElementById('main-area'); main.innerHTML = '';
        const [r, c] = state.layoutMode.split('x').map(Number);
        main.style.gridTemplateRows = `repeat(${r}, 1fr)`;
        main.style.gridTemplateColumns = `repeat(${c}, 1fr)`;
        state.chartsConfig.forEach(cfg => { const div = document.createElement('div'); div.className = 'chart-slot'; main.appendChild(div); initWidget(div, cfg); });
        renderSidebar('left-sidebar-area', state.leftWidgetsConfig);
        renderSidebar('right-sidebar-area', state.rightWidgetsConfig);
        updateSidebarVisibility();
    }

    function renderSidebar(domId, configArr) {
        const container = document.getElementById(domId); container.innerHTML = '';
        configArr.forEach(cfg => { const div = document.createElement('div'); div.className = 'widget-box'; container.appendChild(div); initWidget(div, cfg); });
    }

    function initWidget(container, cfg) {
        const linkClass = getLinkClass(cfg.linkGroup);
        let iconClass = 'fa-chart-bar';
        let typeName = 'Chart';
        if(cfg.type === 'tape') { iconClass = 'fa-list-ol'; typeName = 'Tape'; }
        if(cfg.type === 'financials') { iconClass = 'fa-file-invoice-dollar'; typeName = 'Fin'; }
        if(cfg.type === 'fund-chart') { iconClass = 'fa-chart-area'; typeName = 'Fund.'; }
        if(cfg.type === 'sentiment') { iconClass = 'fa-user-secret'; typeName = 'Sent.'; }

        let fundSelector = '';
        if(cfg.type === 'fund-chart') {
            fundSelector += `<select class="metric-select" onchange="setFundMetric('${cfg.id}', this.value)">`;
            for(const [cat, metrics] of Object.entries(FUND_METRICS)) {
                fundSelector += `<optgroup label="${cat}">`;
                for(const [key, label] of Object.entries(metrics)) {
                    const sel = (key === cfg.fundMetric) ? 'selected' : '';
                    fundSelector += `<option value="${key}" ${sel}>${label}</option>`;
                }
                fundSelector += `</optgroup>`;
            }
            fundSelector += `</select>`;
            const pdLabel = cfg.fundPeriod === 'quarterly' ? 'Q' : 'Y';
            fundSelector += `<div id="fp-btn-${cfg.id}" class="freq-btn" onclick="toggleFundPeriod('${cfg.id}')">${pdLabel}</div>`;
        }

        const headerHtml = `
            <div class="widget-header">
                <div class="widget-controls-left">
                    <div class="type-morph-container" id="type-morph-${cfg.id}">
                        <div class="type-single" onclick="toggleTypeMorph('${cfg.id}', event)"><i class="fas ${iconClass}"></i></div>
                        <div class="type-options-row">
                            <div class="type-btn-opt" onclick="setWidgetType('${cfg.id}', 'chart')"><i class="fas fa-chart-bar"></i> Chart</div>
                            <div class="type-btn-opt" onclick="setWidgetType('${cfg.id}', 'tape')"><i class="fas fa-list-ol"></i> Tape</div>
                            <div class="type-btn-opt" onclick="setWidgetType('${cfg.id}', 'financials')"><i class="fas fa-file-invoice-dollar"></i> Fin</div>
                            <div class="type-btn-opt" onclick="setWidgetType('${cfg.id}', 'fund-chart')"><i class="fas fa-chart-area"></i> Trends</div>
                            <div class="type-btn-opt" onclick="setWidgetType('${cfg.id}', 'sentiment')"><i class="fas fa-user-secret"></i> Sent.</div>
                        </div>
                    </div>
                    
                    <input type="text" class="mini-input" value="${cfg.ticker}" onkeydown="updateWidgetTicker('${cfg.id}', this, event)">
                    
                    ${cfg.type === 'fund-chart' ? fundSelector : ''}
                    
                    ${cfg.type === 'chart' ? getTfHtml(cfg.id, cfg.timeframe) : ''}
                    <div class="link-btn ${linkClass}" onclick="cycleLink('${cfg.id}')" title="Link Group"><i class="fas fa-link"></i></div>
                </div>
                
                <div class="l1-overlay" id="l1-${cfg.id}">
                    <div class="l1-btn l1-bid"><span class="l1-price" id="qb-p-${cfg.id}">0.00</span><span class="l1-size" id="qb-s-${cfg.id}">0</span></div>
                    <div class="l1-center"><span class="last-price" id="ql-${cfg.id}">0.00</span><span class="day-vol" id="qv-${cfg.id}">0</span></div>
                    <div class="l1-btn l1-ask"><span class="l1-price" id="qa-p-${cfg.id}">0.00</span><span class="l1-size" id="qa-s-${cfg.id}">0</span></div>
                </div>
            </div>
        `;
        
        let contentHtml = '';
        if (cfg.type === 'chart') {
            contentHtml = `<div class="widget-content" id="content-${cfg.id}">
                <div class="chart-legend"><div class="legend-row" id="lg-${cfg.id}"></div></div>
                <div id="canvas-${cfg.id}" class="chart-canvas-fill"></div>
            </div>`;
        } else if (cfg.type === 'tape') {
            contentHtml = `<div class="widget-content"><div class="tape-header-row"><span>TIME</span><span style="text-align: right;">PRICE</span><span style="text-align: right;">SIZE</span><span style="text-align: right;">EXCH</span></div><div id="tape-list-${cfg.id}" class="tape-content"></div></div>`;
        } else if (cfg.type === 'financials') {
            contentHtml = `<div class="widget-content"><div id="fin-content-${cfg.id}" class="fin-container"><span style="color:#666; font-size:11px;">Loading Metrics...</span></div></div>`;
        } else if (cfg.type === 'fund-chart' || cfg.type === 'sentiment') {
            contentHtml = `<div class="widget-content" id="content-${cfg.id}"><div id="canvas-${cfg.id}" class="chart-canvas-fill"></div></div>`;
        }

        container.innerHTML = headerHtml + contentHtml;

        if(MarketData.quotes[cfg.ticker]) setTimeout(() => updateQuoteDisplay(cfg.id, MarketData.quotes[cfg.ticker]), 0);

        if(cfg.type === 'chart') initChart(cfg);
        else if(cfg.type === 'tape') initTape(cfg);
        else if(cfg.type === 'financials') initFinancials(cfg);
        else if(cfg.type === 'fund-chart') initFundChart(cfg);
        else if(cfg.type === 'sentiment') initSentimentChart(cfg);
    }

    function getTfHtml(id, tf) {
        const mapTF = { '1Min':'1m', '5Min':'5m', '15Min':'15m', '1H':'1H', '4H':'4H', '1D':'1D', '1W':'1W', '1M':'M' };
        return `
            <div class="tf-morph-container" id="tf-morph-${id}">
                <div class="tf-single" onclick="toggleTfMorph('${id}', event)">${mapTF[tf] || tf}</div>
                <div class="tf-options-row">
                    <div class="tf-btn" onclick="setWidgetTf('${id}', '1Min', event)">1m</div>
                    <div class="tf-btn" onclick="setWidgetTf('${id}', '5Min', event)">5m</div>
                    <div class="tf-btn" onclick="setWidgetTf('${id}', '15Min', event)">15m</div>
                    <div class="tf-btn" onclick="setWidgetTf('${id}', '1H', event)">1H</div>
                    <div class="tf-btn" onclick="setWidgetTf('${id}', '4H', event)">4H</div>
                    <div class="tf-btn" onclick="setWidgetTf('${id}', '1D', event)">D</div>
                    <div class="tf-btn" onclick="setWidgetTf('${id}', '1W', event)">W</div>
                    <div class="tf-btn" onclick="setWidgetTf('${id}', '1M', event)">M</div>
                </div>
            </div>`;
    }

    async function initChart(cfg) {
        const container = document.getElementById(`canvas-${cfg.id}`);
        await new Promise(r => setTimeout(r, 50)); 
        const chart = LightweightCharts.createChart(container, {
            layout: { background: { type: 'solid', color: '#131722' }, textColor: '#d1d4dc' },
            grid: { vertLines: { color: '#1e222d' }, horzLines: { color: '#1e222d' } },
            
            leftPriceScale: { 
                visible: !!state.positions[cfg.ticker], 
                borderColor: '#2a2e39',
                scaleMargins: { top: 0.1, bottom: 0.25 }
            },
            
            timeScale: { 
                timeVisible: true, 
                secondsVisible: false, 
                borderColor: '#2a2e39', 
                minBarSpacing: 0.005,
                tickMarkFormatter: (time, tickMarkType, locale) => {
                    const d = new Date(time * 1000);
                    if (tickMarkType < 3) return `${d.getMonth() + 1}/${d.getDate()}`;
                    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
                }
            },
            
            crosshair: { mode: 0 },
            rightPriceScale: { scaleMargins: { top: 0.1, bottom: 0.25 } },
            localization: {
                timeFormatter: (t) => {
                    const d = new Date(t * 1000);
                    if (['1D', '1W', '1M'].includes(cfg.timeframe)) return d.toLocaleDateString();
                    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
                }
            }
        });
        
        const volSeries = chart.addHistogramSeries({ 
            color: '#26a69a', 
            priceFormat: { type: 'volume' }, 
            priceScaleId: '' 
        });
        volSeries.priceScale().applyOptions({ scaleMargins: { top: 0.75, bottom: 0 } });
        
        const candleSeries = chart.addCandlestickSeries({ upColor: '#26a69a', downColor: '#ef5350', borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350' });

        const pnlSeries = chart.addCandlestickSeries({
            priceScaleId: 'left',
            visible: !!state.positions[cfg.ticker],
            upColor: 'transparent', downColor: 'transparent', wickUpColor: 'transparent', wickDownColor: 'transparent', borderVisible: false,
            lastValueVisible: true, priceLineVisible: true, priceLineColor: '#2962ff',
            priceFormat: {
                type: 'custom',
                formatter: price => {
                    const p = state.positions[cfg.ticker];
                    if (!p) return '';
                    const pnl = (price - p.avgPrice) * p.qty;
                    const absPnl = Math.abs(pnl);
                    if (absPnl >= 1e6) return (pnl >= 0 ? '+' : '') + (pnl / 1e6).toFixed(1) + 'M';
                    if (absPnl >= 1e3) return (pnl >= 0 ? '+' : '') + (pnl / 1e3).toFixed(1) + 'k';
                    return (pnl >= 0 ? '+' : '') + Math.round(pnl).toString();
                }
            }
        });

        const vwapSeries = chart.addLineSeries({ color: '#b042f5', lineWidth: 1, crosshairMarkerVisible: false, priceLineVisible: false });

        const wrapper = document.getElementById(`content-${cfg.id}`);
        
        const ro = new ResizeObserver(e => { 
            if (e.length && e[0].contentRect) {
                if (typeof obj !== 'undefined') obj.width = e[0].contentRect.width; 
                chart.applyOptions({ width: e[0].contentRect.width, height: e[0].contentRect.height }); 
            }
        });
        ro.observe(wrapper);

        const obj = { 
            ...cfg, chart, candleSeries, volSeries, vwapSeries, pnlSeries, entryLine: null,
            lastPrice: 0, earliestTime: null, isLoading: false, width: wrapper.clientWidth,
            lastCumVol: 0, lastCumPV: 0, lastSessionDate: null, ro
        };
        
        if (state.positions[cfg.ticker]) {
            const p = state.positions[cfg.ticker];
            obj.entryLine = candleSeries.createPriceLine({
                price: p.avgPrice,
                color: p.qty > 0 ? '#26a69a' : '#ef5350',
                lineWidth: 2, lineStyle: 2, axisLabelVisible: true, title: ''
            });
        }
        
        setTimeout(() => { if(wrapper.clientWidth > 0) chart.applyOptions({ width: wrapper.clientWidth, height: wrapper.clientHeight }); }, 200);

        state.activeWidgets.push(obj);
        
        const crosshairUnsub = p => {
            const c = p.seriesData.get(candleSeries);
            const v = p.seriesData.get(volSeries);
            const vw = p.seriesData.get(vwapSeries);
            const el = document.getElementById(`lg-${cfg.id}`);
            
            if (c && el) {
                const d = new Date(p.time * 1000);
                const isDailyPlus = ['1D', '1W', '1M'].includes(cfg.timeframe);
                let dateStr = isDailyPlus ? d.toLocaleDateString() : `${d.getMonth() + 1}/${d.getDate()} ${d.getHours()}:${d.getMinutes().toString().padStart(2, '0')}`;
                const color = c.close >= c.open ? '#26a69a' : '#ef5350';
                const volVal = v ? v.value : 0;
                const dolVol = volVal * c.close; 
                const w = obj.width || 400; 
                
                let items = [];
                if (w >= 520) items.push(`<span style="color:#d1d4dc; font-weight:bold;">${dateStr}</span>`);
                if (w >= 320) {
                    items.push(`O:<span style="color:${color}">${c.open.toFixed(2)}</span>`);
                    items.push(`H:<span style="color:${color}">${c.high.toFixed(2)}</span>`);
                    items.push(`L:<span style="color:${color}">${c.low.toFixed(2)}</span>`);
                }
                items.push(`C:<span style="color:${color}">${c.close.toFixed(2)}</span>`);
                if (w >= 250) items.push(`V:<span style="color:#d1d4dc">${formatNum(volVal)}</span>`);
                if (w >= 320) items.push(`$V:<span style="color:#2962ff">${formatNum(dolVol)}</span>`);
                if (w >= 450 && vw && vw.value) items.push(`VWAP:<span style="color:#b042f5">${vw.value.toFixed(2)}</span>`);
                
                const pos = state.positions[cfg.ticker];
                if (pos && w >= 320) {
                    const pnl = (c.close - pos.avgPrice) * pos.qty;
                    const sign = pnl >= 0 ? '+' : '';
                    const pnlColor = pnl >= 0 ? '#26a69a' : '#ef5350';
                    items.push(`P&L:<span style="color:${pnlColor}">${sign}${formatNum(pnl)}</span>`);
                }
                
                el.innerHTML = items.map(item => `<div class="legend-item">${item}</div>`).join('');
            }
        };
        chart.subscribeCrosshairMove(crosshairUnsub);
        obj.crosshairUnsub = crosshairUnsub; 
        
        let loadTimeout = null;
        const rangeUnsub = range => { 
            if(range && range.from < 10 && !obj.isLoading) {
                if (loadTimeout) clearTimeout(loadTimeout);
                loadTimeout = setTimeout(() => { loadOlderData(obj); }, 300);
            }
        };
        chart.timeScale().subscribeVisibleLogicalRangeChange(rangeUnsub);
        obj.rangeUnsub = rangeUnsub; 
        
        await loadChartData(obj);
        updateChartsRealtime();
    }

    function initTape(cfg) { 
        const obj = { ...cfg, lastTradeId: 0, lastPrice: 0 };
        state.activeWidgets.push(obj); 
        updateTapeIndividual();
    }

    async function updateTapeIndividual() {
        if(!state.isLeftOpen && !state.isRightOpen && !state.chartsConfig.some(x=>x.type==='tape')) return;
        const k=await getKeys(); if(!k) return;
        const start = new Date(Date.now() - 86400000 * 3).toISOString(); 
        const tapeWidgets = state.activeWidgets.filter(w => w.type === 'tape');
        const exMap = { 'A':'AMEX', 'B':'BSE', 'C':'NSX', 'D':'ADF', 'I':'ISE', 'J':'EDGA', 'K':'EDGX', 'M':'CHX', 'N':'NYSE', 'P':'ARCA', 'Q':'NASD', 'S':'CTS', 'T':'TRF', 'V':'IEX', 'X':'PHLX', 'Y':'BYX', 'Z':'BZX' };

        const widgetsByTicker = {};
        tapeWidgets.forEach(w => {
            if (!widgetsByTicker[w.ticker]) widgetsByTicker[w.ticker] = [];
            widgetsByTicker[w.ticker].push(w);
        });

        await Promise.all(Object.entries(widgetsByTicker).map(async ([ticker, widgets]) => {
            if (!widgets.some(w => document.getElementById(`tape-list-${w.id}`)?.offsetParent !== null)) return;
            const u = `https://data.alpaca.markets/v2/stocks/${ticker}/trades?limit=50&sort=desc&feed=sip&start=${start}`;
            try {
                const res = await fetch(u, { headers: { "APCA-API-KEY-ID": k.k, "APCA-API-SECRET-KEY": k.s } });
                if(!res.ok) return;
                const json = await res.json();
                
                if(json.trades) {
                    if (!MarketData.trades[ticker]) MarketData.trades[ticker] = [];
                    let masterList = MarketData.trades[ticker];
                    
                    const newTrades = json.trades.filter(t => !masterList.some(existing => existing.i === t.i));
                    if(newTrades.length > 0) {
                        masterList = [...masterList, ...newTrades].sort((a, b) => new Date(b.t) - new Date(a.t));
                        if(masterList.length > 50) masterList = masterList.slice(0, 50);
                        MarketData.trades[ticker] = masterList; 
                    }
                    
                    widgets.forEach(w => {
                        const list = document.getElementById(`tape-list-${w.id}`);
                        if(!list || list.offsetParent === null) return;
                        
                        const rows = masterList.map((t, i) => {
                            const prevPrice = (i < masterList.length - 1) ? masterList[i+1].p : t.p;
                            const c = t.p >= prevPrice ? 't-up' : 't-down';
                            const bg = t.s >= 100 ? 't-large' : '';
                            const tm = new Date(t.t).toLocaleTimeString([],{hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'});
                            const exch = exMap[t.x] || t.x || '-';
                            
                            return `<div class="tape-row ${bg}">
                                    <span class="tape-time">${tm}</span>
                                    <span class="tape-price ${c}">${t.p.toFixed(2)}</span>
                                    <span class="tape-size">${t.s}</span>
                                    <span class="tape-venue">${exch}</span>
                                  </div>`;
                        });
                        list.innerHTML = rows.join('');
                    });
                }
            } catch(e) { }
        }));
    }
    
    async function initFundChart(cfg) {
        const container = document.getElementById(`canvas-${cfg.id}`);
        await new Promise(r => setTimeout(r, 50));
        const chart = LightweightCharts.createChart(container, {
            layout: { background: { type: 'solid', color: '#131722' }, textColor: '#d1d4dc' },
            grid: { vertLines: { visible: false }, horzLines: { color: '#1e222d', style: 2 } },
            timeScale: { borderColor: '#2a2e39', fixLeftEdge: true, fixRightEdge: true },
            rightPriceScale: { borderColor: '#2a2e39' }
        });
        const series = chart.addHistogramSeries({ color: '#2962ff' });
        const wrapper = document.getElementById(`content-${cfg.id}`);
        
        const ro = new ResizeObserver(e => { if (e.length && e[0].contentRect) chart.applyOptions({ width: e[0].contentRect.width, height: e[0].contentRect.height }); });
        ro.observe(wrapper);
        
        setTimeout(() => { if(wrapper.clientWidth > 0) chart.applyOptions({ width: wrapper.clientWidth, height: wrapper.clientHeight }); }, 200);
        const obj = { ...cfg, chart, series, type: 'fund-chart', ro };
        state.activeWidgets.push(obj);
        updateFundChart(obj);
    }

    async function updateFundChart(cfg) {
        const w = state.activeWidgets.find(x => x.id === cfg.id);
        if(!w) return;
        const data = await fetchFinancials(w.ticker);
        if(!data || !data.series) return;
        const rawSeries = data.series[w.fundPeriod];
        if(!rawSeries) return;
        let metricData = rawSeries[w.fundMetric];
        if (!metricData) metricData = rawSeries[w.fundMetric + 'TTM'];
        if (!metricData) return;
        const chartData = metricData.filter(d => d.v !== null).map(d => ({ time: d.period, value: d.v, color: d.v >= 0 ? '#26a69a' : '#ef5350' })).sort((a,b) => new Date(a.time) - new Date(b.time));
        w.series.setData(chartData);
        w.chart.timeScale().fitContent();
    }

    async function initSentimentChart(cfg) {
        const container = document.getElementById(`canvas-${cfg.id}`);
        await new Promise(r => setTimeout(r, 50));
        const chart = LightweightCharts.createChart(container, {
            layout: { background: { type: 'solid', color: '#131722' }, textColor: '#d1d4dc' },
            grid: { vertLines: { color: '#1e222d' }, horzLines: { color: '#1e222d' } },
            timeScale: { borderColor: '#2a2e39' },
            rightPriceScale: { scaleMargins: { top: 0.05, bottom: 0.35 } } 
        });
        const sentSeries = chart.addHistogramSeries({ 
            color: '#26a69a', 
            priceFormat: { type: 'volume' }, 
            priceScaleId: ''
        });
        sentSeries.priceScale().applyOptions({ scaleMargins: { top: 0.70, bottom: 0 } });
        const candleSeries = chart.addCandlestickSeries({ upColor: '#26a69a', downColor: '#ef5350', borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350' });
        const wrapper = document.getElementById(`content-${cfg.id}`);
        
        const ro = new ResizeObserver(e => { if (e.length && e[0].contentRect) chart.applyOptions({ width: e[0].contentRect.width, height: e[0].contentRect.height }); });
        ro.observe(wrapper);
        
        setTimeout(() => { if(wrapper.clientWidth > 0) chart.applyOptions({ width: wrapper.clientWidth, height: wrapper.clientHeight }); }, 200);
        const obj = { ...cfg, chart, candleSeries, sentSeries, type: 'sentiment', ro };
        state.activeWidgets.push(obj);
        updateSentimentChart(obj);
    }

    async function updateSentimentChart(w) {
        const k = await getKeys(); 
        const fhToken = await getFinnhubKey();
        if(!k || !fhToken) { log("Missing API Keys for Sentiment Chart", "error"); return; }
        
        const start = new Date(Date.now() - (3650 * 86400000)).toISOString();
        const end = new Date().toISOString();
        
        const cUrl = `https://data.alpaca.markets/v2/stocks/${w.ticker}/bars?timeframe=1Month&start=${start}&end=${end}&limit=500&adjustment=split&sort=desc`;
        const today = new Date().toISOString().split('T')[0];
        const sUrl = `https://finnhub.io/api/v1/stock/insider-sentiment?symbol=${w.ticker}&from=2020-01-01&to=${today}&token=${fhToken}`;
        try {
            const [cRes, sRes] = await Promise.all([ fetch(cUrl, { headers: { "APCA-API-KEY-ID": k.k, "APCA-API-SECRET-KEY": k.s } }), fetch(sUrl) ]);
            if(cRes.ok && sRes.ok) {
                const cJson = await cRes.json();
                const sJson = await sRes.json();
                if(cJson.bars) {
                    const candles = parseBars(cJson.bars.reverse());
                    w.candleSeries.setData(candles.ohlc);
                }
                if(sJson.data) {
                    const sentData = sJson.data.map(d => {
                        const time = new Date(d.year, d.month - 1, 1).getTime() / 1000; 
                        return { time: time, value: d.mspr, color: d.mspr > 0 ? '#26a69a' : '#ef5350' };
                    }).sort((a,b) => a.time - b.time);
                    w.sentSeries.setData(sentData);
                }
                w.chart.timeScale().fitContent();
            }
        } catch(e) { log(e.message); }
    }

    async function loadChartData(c) { 
        c.isLoading = true;
        const data = await MarketData.getBars(c.ticker, c.timeframe);
        
        if(data) { 
            c.candleSeries.setData(data.ohlc); 
            c.pnlSeries.setData(data.ohlc); 
            c.volSeries.setData(data.vol); 
            if(c.vwapSeries) c.vwapSeries.setData(data.vwap);
            
            c.lastCumVol = data.lastCumVol;
            c.lastCumPV = data.lastCumPV;
            c.lastSessionDate = data.lastDate;

            const len=data.ohlc.length; 
            if(len>250) c.chart.timeScale().setVisibleLogicalRange({from:len-250,to:len}); 
            else c.chart.timeScale().fitContent(); 
            c.lastPrice = data.ohlc[len-1].close; 
            c.earliestTime = data.ohlc[0].time; 
        }
        c.isLoading = false;
    }

    async function loadOlderData(c) { 
        if(!c.earliestTime || c.isLoading) return; 
        c.isLoading = true; 
        const keys = await getKeys(); 
        const visibleBars = Math.ceil(c.chart.timeScale().getVisibleLogicalRange().to - c.chart.timeScale().getVisibleLogicalRange().from); 
        const limit = Math.min(Math.max(visibleBars*2, 500), 10000); 
        const endStr = new Date((c.earliestTime - 1) * 1000).toISOString(); 
        let tf = c.timeframe; if(tf==='4H') tf='4Hour'; if(tf==='1W') tf='1Week'; if(tf==='1M' || tf==='M') tf='1Month'; if(tf==='1H') tf='1Hour';
        const url = `https://data.alpaca.markets/v2/stocks/${c.ticker}/bars?timeframe=${tf}&start=2015-01-01T00:00:00Z&end=${endStr}&limit=${limit}&sort=desc&adjustment=split`; 
        try { 
            const res = await fetch(url, { headers: { "APCA-API-KEY-ID": keys.k, "APCA-API-SECRET-KEY": keys.s } }); 
            const json = await res.json(); 
            if(json.bars) { 
                const oldBars = json.bars.reverse();
                const data = parseBars(oldBars); 
                c.candleSeries.setData(data.ohlc.concat(c.candleSeries.data())); 
                c.pnlSeries.setData(data.ohlc.concat(c.pnlSeries.data())); 
                c.volSeries.setData(data.vol.concat(c.volSeries.data())); 
                if(c.vwapSeries) c.vwapSeries.setData(data.vwap.concat(c.vwapSeries.data()));
                c.earliestTime = data.ohlc[0].time; 
            } 
        } catch(e) {} finally { c.isLoading = false; } 
    }

    // --- REALTIME ---
    async function updateChartsRealtime() {
        const keys = await getKeys(); if(!keys) return;
        const widgets = state.activeWidgets;
        if(widgets.length === 0) return;
        const symbols = [...new Set(widgets.map(w => w.ticker))];
        const u = `https://data.alpaca.markets/v2/stocks/snapshots?symbols=${symbols.join(',')}`;
        try {
            const res = await fetch(u, { headers: { "APCA-API-KEY-ID": keys.k, "APCA-API-SECRET-KEY": keys.s } });
            const json = await res.json();
            symbols.forEach(sym => {
                const d = json[sym] || json[sym.toUpperCase()];
                if(d) MarketData.quotes[sym] = d; 
            });
            widgets.forEach(w => {
                const data = MarketData.quotes[w.ticker] || MarketData.quotes[w.ticker.toUpperCase()];
                if(!data) return;
                updateQuoteDisplay(w.id, data);
                if(w.type === 'chart') {
                    const t = data.latestTrade;
                    if (t) {
                        const tradeTime = new Date(t.t).getTime() / 1000;
                        const currentData = w.candleSeries.data();
                        const lastBar = currentData.length > 0 ? currentData[currentData.length - 1] : null;
                        
                        const durations = { '1Min': 60, '5Min': 300, '15Min': 900, '1H': 3600, '4H': 14400, '1D': 86400, '1W': 604800, '1M': 2592000 };
                        const dur = durations[w.timeframe] || 60;
                        
                        let tfTime;
                        if (lastBar && tradeTime >= lastBar.time && tradeTime < lastBar.time + dur) {
                            tfTime = lastBar.time;
                        } else {
                            tfTime = snapTime(tradeTime, w.timeframe);
                            if (lastBar && tfTime <= lastBar.time) {
                                tfTime = lastBar.time + dur;
                            }
                        }
                        
                        let newO = t.p, newH = t.p, newL = t.p, newC = t.p;
                        if (lastBar && lastBar.time === tfTime) {
                            newO = lastBar.open;
                            newH = Math.max(lastBar.high, t.p);
                            newL = Math.min(lastBar.low, t.p);
                        } else if (lastBar) {
                            newO = lastBar.close; 
                            newH = Math.max(newO, t.p);
                            newL = Math.min(newO, t.p);
                        }
                        
                        updateChartBar(w, tfTime, newO, newH, newL, newC, undefined, undefined);
                    }
                }
                if(w.type === 'financials') {
                   const track = document.getElementById(`rng-track-${w.id}`);
                   const arrow = document.getElementById(`rng-arrow-${w.id}`);
                   if(track && arrow && data.latestTrade) {
                       const h = parseFloat(track.dataset.high);
                       const l = parseFloat(track.dataset.low);
                       const p = data.latestTrade.p;
                       if(h > l) {
                           let pct = ((p - l) / (h - l)) * 100;
                           pct = Math.max(0, Math.min(100, pct));
                           arrow.style.left = `${pct}%`;
                       }
                   }
                }
            });
        } catch(e) { console.error(e); }
    }
    
    function updateChartBar(w, time, open, high, low, close, vol, intervalVwap) {
        if (!w.candleSeries || !w.volSeries) return;
        
        const bar = { time: time, open: open, high: high, low: low, close: close, vw: intervalVwap }; 
        w.candleSeries.update(bar);
        w.pnlSeries.update(bar); 
        
        if (vol !== undefined) {
            w.volSeries.update({ time: time, value: vol, color: close >= open ? '#26a69a' : '#ef5350' });
            
            if (w.vwapSeries && intervalVwap) {
                const dateStr = new Date(time * 1000).toLocaleDateString();
                
                if (w.lastSessionDate) {
                    let tempCumVol = w.lastCumVol;
                    let tempCumPV = w.lastCumPV;
                    
                    if (dateStr !== w.lastSessionDate) {
                        tempCumVol = 0;
                        tempCumPV = 0;
                    }
                    
                    const currentSessionVwap = (tempCumPV + (vol * intervalVwap)) / (tempCumVol + vol);
                    w.vwapSeries.update({ time: time, value: currentSessionVwap });
                }
            }
        }
        w.lastPrice = close;
    }
    
    function updateQuoteDisplay(id, data) {
        const q = data.latestQuote;
        const t = data.latestTrade;
        const d = data.dailyBar;
        const l1 = document.getElementById(`l1-${id}`);
        if(l1 && q) {
            document.getElementById(`qb-p-${id}`).innerText = q.bp.toFixed(2);
            document.getElementById(`qb-s-${id}`).innerText = q.bs;
            document.getElementById(`qa-p-${id}`).innerText = q.ap.toFixed(2);
            document.getElementById(`qa-s-${id}`).innerText = q.as;
            if(t) { document.getElementById(`ql-${id}`).innerText = t.p.toFixed(2); }
            if(d) document.getElementById(`qv-${id}`).innerText = formatNum(d.v);
        }
    }
    function updateSidebarVisibility() { const l=document.getElementById('left-sidebar-area'), r=document.getElementById('right-sidebar-area'), bl=document.getElementById('leftToggle'), br=document.getElementById('rightToggle'); if(state.isLeftOpen){l.classList.remove('collapsed');bl.classList.add('active');}else{l.classList.add('collapsed');bl.classList.remove('active');} if(state.isRightOpen){r.classList.remove('collapsed');br.classList.add('active');}else{r.classList.add('collapsed');br.classList.remove('active');} setTimeout(() => { state.activeWidgets.forEach(w => { if(w.type==='chart') w.chart.applyOptions({ width: document.getElementById(`content-${w.id}`).clientWidth }); }); }, 300); }
    
    async function getKeys() { 
        try {
            const db = await openDB();
            const tx = db.transaction("Settings", "readonly");
            const getVal = (k) => new Promise(r => { const req = tx.objectStore("Settings").get(k); req.onsuccess = () => r(req.result ? req.result.value : ''); });
            const [k, s] = await Promise.all([getVal("APCA_API_KEY_ID"), getVal("APCA_API_SECRET_KEY")]);
            if (k && s) return { k, s };
        } catch(e) {}
        
        if(typeof getApiKeys!=='function') return null; 
        const k=await getApiKeys(); 
        return (k.alpacaKey||k.k) ? {k:k.alpacaKey||k.k, s:k.alpacaSecret||k.s} : null; 
    }
    
    function getLinkClass(g){if(g==='A')return'link-blue';if(g==='B')return'link-orange';if(g==='C')return'link-green';if(g==='D')return'link-purple';if(g==='E')return'link-red';return'';}
    
    async function refreshPortfolioTape() {
        const k = await getKeys(); if(!k) return;
        const t = Object.keys(state.positions); 
        t.forEach(s => state.newsSymbols.add(s));
        
        if (t.length) {
            try {
                const r = await fetch(`https://data.alpaca.markets/v2/stocks/snapshots?symbols=${t.join(',')}`, { headers: { "APCA-API-KEY-ID": k.k, "APCA-API-SECRET-KEY": k.s } });
                if (r.ok) {
                    const j = await r.json();
                    
                    Object.keys(j).forEach(sym => {
                        MarketData.quotes[sym] = j[sym] || MarketData.quotes[sym];
                    });
                    
                    const i = Object.entries(j).map(([s,d]) => parseSnapshot(d,s)).filter(Boolean);
                    state.tapeData.portfolio = generateSectionHTML("PORTFOLIO", i);
                    renderTape();
                    
                    if (document.getElementById('positionsModal').style.display === 'flex') {
                        renderPositionsList();
                    }
                }
            } catch(e) {}
        } else {
            state.tapeData.portfolio = '';
            renderTape();
        }
    }
    
    async function refreshScreenerTape(){const k=await getKeys();if(!k)return;fetchScreener(k,'movers?top=10','gainers',"TOP GAINERS").then(h=>{state.tapeData.gainers=h;renderTape();});fetchScreener(k,'movers?top=10','losers',"TOP LOSERS").then(h=>{state.tapeData.losers=h;renderTape();});fetchActives(k,'volume','activeVol',"ACTIVE (VOL)");fetchActives(k,'trades','activeTrd',"ACTIVE (TRD)");if(!state.tapeData.activeVol)fetchFallbackIndices(k);}
    async function fetchScreener(k,e,n,t){try{const r=await fetch(`https://data.alpaca.markets/v1beta1/screener/stocks/${e}`,{headers:{"APCA-API-KEY-ID":k.k,"APCA-API-SECRET-KEY":k.s}});if(!r.ok)return'';const j=await r.json();const i=(j[n]||[]).map(x=>({symbol:x.symbol,price:x.price,pct:x.percent_change}));i.forEach(x=>state.newsSymbols.add(x.symbol));return generateSectionHTML(t,i);}catch(e){return'';}}
    async function fetchActives(k,b,sk,t){try{const r=await fetch(`https://data.alpaca.markets/v1beta1/screener/stocks/most-actives?by=${b}&top=10`,{headers:{"APCA-API-KEY-ID":k.k,"APCA-API-SECRET-KEY":k.s}});if(!r.ok)return;const j=await r.json();const l=j.most_actives||[];const s=l.map(i=>i.symbol);s.forEach(sym=>state.newsSymbols.add(sym));if(!s.length)return;const sr=await fetch(`https://data.alpaca.markets/v2/stocks/snapshots?symbols=${s.join(',')}`,{headers:{"APCA-API-KEY-ID":k.k,"APCA-API-SECRET-KEY":k.s}});if(sr.ok){const sj=await sr.json();const i=Object.entries(sj).map(([sy,d])=>parseSnapshot(d,sy));state.tapeData[sk]=generateSectionHTML(t,i);renderTape();}}catch(e){}}
    async function fetchFallbackIndices(k){try{const r=await fetch(`https://data.alpaca.markets/v2/stocks/snapshots?symbols=SPY,QQQ,IWM,DIA`,{headers:{"APCA-API-KEY-ID":k.k,"APCA-API-SECRET-KEY":k.s}});if(r.ok){const j=await r.json();const i=Object.entries(j).map(([s,d])=>parseSnapshot(d,s));state.tapeData.fallback=generateSectionHTML("INDICES",i);renderTape();}}catch(e){} }
    
    async function refreshNewsTape() {
        const k = await getKeys(); if(!k) return;
        
        const portfolioSyms = Object.keys(state.positions);
        const otherSyms = Array.from(state.newsSymbols).filter(sym => !state.positions[sym]);
        
        let s = [...portfolioSyms, ...otherSyms];
        if (!s.length) s = ['SPY', 'QQQ'];
        
        const r = s.slice(0, 25).join(',');
        const u = `https://data.alpaca.markets/v1beta1/news?symbols=${r}&limit=50`;
        
        try {
            const rs = await fetch(u, { headers: { "APCA-API-KEY-ID": k.k, "APCA-API-SECRET-KEY": k.s } });
            if (!rs.ok) return;
            const j = await rs.json();
            if (j.news) {
                let h = '';
                j.news.forEach(n => {
                    const sym = (n.symbols && n.symbols.length) ? n.symbols[0] : 'MKT';
                    const t = new Date(n.updated_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                    h += `<a class="news-item" href="${n.url}" target="_blank"><span class="news-sym">${sym}</span><span>${n.headline}</span><span class="news-time">${t}</span></a>`;
                });
                document.getElementById('newsTrack').innerHTML = h + h;
            }
        } catch(e) {}
    }
    
    function renderTape() { const t=document.getElementById('tickerTrack'); let h=[state.tapeData.portfolio,state.tapeData.gainers,state.tapeData.losers,state.tapeData.activeVol,state.tapeData.activeTrd,state.tapeData.fallback].join(''); if(h.trim().length===0) h='<span style="color:#666;">Loading...</span>'; t.innerHTML=h+h; }
    function parseSnapshot(d,s) { if(!d||!d.latestTrade)return null; const p=d.latestTrade.p; const pr=d.prevDailyBar?d.prevDailyBar.c:p; return{symbol:s,price:p,pct:((p-pr)/pr)*100}; }
    function generateSectionHTML(t,i) { if(!i||i.length===0)return ''; let h=`<div class="ticker-section"><span class="section-label">${t}</span>`; i.forEach(x=>{ if(!x)return; const c=x.pct>=0?'color-up':'color-down'; const ic=x.pct>=0?'▲':'▼'; h+=`<div class="ticker-item" onclick="changeTicker('${x.symbol}')"><span class="t-sym">${x.symbol}</span><span class="t-price">${x.price.toFixed(2)}</span><span class="t-chg ${c}">${ic} ${Math.abs(x.pct).toFixed(2)}%</span></div>`; }); return h+'</div>'; }

    document.addEventListener('DOMContentLoaded', init);
    document.getElementById('debugToggle').addEventListener('click', () => logger.style.display = logger.style.display === 'block' ? 'none' : 'block');
    document.getElementById('refreshBtn').addEventListener('click', () => state.activeWidgets.forEach(w=>{if(w.type==='chart')loadChartData(w)}));
    document.getElementById('settingsBtn').addEventListener('click', () => toggleSettings(true));
    document.getElementById('clearLogs').addEventListener('click', () => logger.innerHTML = '');
</script>
</body>
</html>
since we have access to ibkr-gateway now, could we actually open and close positions from this?

You absolutely can. This is exactly what algorithmic traders and quant funds use this exact gateway to do!

Since your Python server is fully authenticated with the Java gateway, you now have the power to execute live market, limit, and stop orders directly from your web dashboard.

However, before we build the "Buy/Sell" buttons into your UI, you need to be aware of how the IBKR API handles trading, because it is slightly more complex than just sending "Buy 10 AAPL".

If you want to add this, here are the 3 things we will need to build into your Python script:

1. The "Contract ID" (ConID) Hurdle
Unlike Questrade or Alpaca, Interactive Brokers does not let you trade using ticker symbols. You cannot send an order for AAPL.
First, your Python script has to ping the gateway and say, "What is the internal database ID for AAPL?" The gateway returns 265598. Then you can submit the buy order using that specific ID number.

2. The "Reply Message" System
IBKR is famously cautious. If you place a trade, the API will sometimes intercept it and hold it in a pending state, returning a warning like: "You are submitting this order outside of regular trading hours. Do you want to continue?" or "This order exceeds 5% of your account value."
Your Python script will need a handler to automatically intercept those warnings and reply with a {"confirmed": true} signal to force the trade through.

3. The Security Warning (CRITICAL)
Right now, your dashboard is accessible via your Cloudflare Tunnel (stonks.asdo.ca). If we add live trading buttons to this dashboard, anyone on the internet who finds your URL can click the button and execute trades with your real money.
Before we add trading, you must go into your Cloudflare dashboard and turn on Cloudflare Access (Zero Trust) for that tunnel. It takes 2 minutes and will put a login screen in front of your URL that requires an email PIN code before the dashboard even loads.

How to Implement It
If you have locked down your Cloudflare tunnel and are ready to turn this into a live execution platform, we will need to do this in two steps:

The Python Backend: I will write a new @app.route('/trade', methods=['POST']) for your Python script. It will handle looking up the ConID, formatting the JSON payload, submitting it to the IBKR Gateway, and automatically answering any safety warnings IBKR throws back.

The HTML Frontend: We will add a sleek "Trade Ticket" popup to your UI with BUY and SELL buttons, letting you type in a quantity, select Market or Limit, and hit execute.

Let me know if you want to pull the trigger on this, and I'll generate the Python code to add the trade routing!

I'm not able to access via localhost:5000. Did this:

ssh -L 5000:localhost:5000 root@100.64.0.44



Accessing the server using tailscale hostname works.

This is almost certainly the infamous macOS Port 5000 trap.

Since you are using a Mac (which we saw in your Chrome logs earlier!), Apple introduced a feature in macOS Monterey called "AirPlay Receiver" that permanently runs in the background and secretly hogs port 5000.

When you ran the SSH tunnel command, your Mac likely couldn't actually grab port 5000, so the tunnel failed to open (even if the SSH login succeeded). Additionally, sometimes localhost in SSH commands resolves to the IPv6 address (::1) which the Java server ignores.

Let's sidestep both of those issues completely by changing the local port to 5001 and explicitly forcing IPv4 (127.0.0.1).

The Fix
Step 1: Close the broken tunnel
Go to the terminal window where you ran the SSH command, type exit, and press Enter to close the connection.

Step 2: Run the modified tunnel command
Run this command instead. It binds your Mac's port 5001 to the Proxmox container's port 5000:

Bash
ssh -L 5001:127.0.0.1:5000 root@100.64.0.44
Step 3: Log in

Open a new Chrome Incognito window.

Explicitly type in: https://localhost:5001 (Make sure it's HTTPS and port 5001!)

Bypass the Chrome security warning (click Advanced -> Proceed, or type thisisunsafe anywhere on the page).

You should now see the IBKR login screen perfectly!

Once you log in and approve the 2FA, don't forget to run the "tickle" to lock it in:

Open a new tab and hit: https://localhost:5001/v1/api/tickle

Check the status: https://localhost:5001/v1/api/portfolio/accounts

Once that spits out your account IDs, you can close the terminal and we can start building those live execution buttons!

do i need to get the server to restart

bin/run.sh root/conf.yaml &



on startup?

Yes, absolutely! Right now, because you just ran it directly in the terminal, if your Proxmox server restarts (or if it updates and reboots), the IBKR Gateway will stay dead until you manually log back in via SSH and run that command again.

Since you already conquered systemd for your Python script and Cloudflare, we should wrap the IBKR Gateway in a systemd service too. That way, it boots up automatically in the background the second the container powers on.

Here is exactly how to make it permanent.

Step 1: Create the Service File
Run this command to create a new systemd file for the gateway:

Bash
nano /etc/systemd/system/ibkr-gateway.service
Step 2: Paste the Configuration
Paste the following text into the editor. (Note: This assumes your gateway folder is located at /root/ibkr-gateway. If it's under a different user, change the WorkingDirectory and User).

Ini, TOML
[Unit]
Description=Interactive Brokers Client Portal Gateway
After=network.target

[Service]
Type=simple
WorkingDirectory=/root/ibkr-gateway
# Run the script and point it to the config file
ExecStart=/bin/bash bin/run.sh root/conf.yaml
Restart=always
RestartSec=10
User=root

[Install]
WantedBy=multi-user.target
Press Ctrl+O, Enter to save, and Ctrl+X to exit.

Step 3: Kill the manual process
Before we start the automated service, we need to kill the manual one you ran earlier with the & symbol so they don't fight over port 5000:

Bash
pkill -f java
Step 4: Enable and Start
Now tell the system to load your new file, turn it on, and set it to auto-start on boot:

Bash
systemctl daemon-reload
systemctl enable ibkr-gateway
systemctl start ibkr-gateway
You can verify it's happily running in the background by checking the status:

Bash
systemctl status ibkr-gateway
⚠️ The One Critical Catch
While the server application will now automatically start when Proxmox boots, your login session will not. Interactive Brokers enforces strict security. If your Proxmox server restarts, the gateway app will boot up perfectly on port 5000, but it will be sitting in an "Unauthorized" state.

After a reboot, you will still need to:

Open your Mac terminal and run the SSH tunnel: ssh -L 5001:127.0.0.1:5000 root@100.64.0.44

Go to https://localhost:5001 in your browser.

Log in with your username/password and approve the 2FA.

Once you do that, it will stay authenticated 24/7 as long as the server doesn't restart!

our portfolio server provides us with a balances and positions endpoints. is there another similar endpoint we can create that provides us the total equity (per broker, and overall)?

Yes, absolutely! And the best part is that it is incredibly easy to do.

We don't even have to write complex logic to manually multiply your position sizes by live market prices. Both Questrade and Interactive Brokers natively calculate your Total Equity (Cash + Live Market Value of all positions) on their backend servers in real-time.

All we have to do is pluck that specific data point out of the balance dictionaries we are already fetching and expose it via a new @app.route('/equity.json').

Here is how to add it to your Python portfolio-sync server in two simple steps:

Step 1: Update the Fetcher Functions
We need to tell our existing balance fetchers to grab the "Equity" metric from the brokers' API responses.

Find your get_questrade_balances function, and update the very bottom return statement to include totalEquity:

Python
        return {
            'cash': target_bal.get('cash', 0),
            'buyingPower': target_bal.get('buyingPower', 0),
            'equity': target_bal.get('totalEquity', 0)  # NEW: Questrade's total equity field
        }
Next, find your get_ibkr_balances function. First, add total_equity = 0.0 right below where you define total_cash and total_bp. Then, update the parsing loop and the return statement like this:

Python
        total_cash = 0.0
        total_bp = 0.0
        total_equity = 0.0 # NEW

        # Loop through EVERY account to sum the balances
        for account in accounts:
            account_id = account.get('id') or account.get('accountId')
            
            try:
                bal_res = requests.get(f"{IB_GATEWAY_URL}/portfolio/{account_id}/summary", verify=False, timeout=2)
                bal_res.raise_for_status()
                summary = bal_res.json()

                total_cash += float(summary.get('totalcashvalue', {}).get('amount', 0))
                total_bp += float(summary.get('buyingpower', {}).get('amount', 0))
                # NEW: IBKR calls Total Equity "Net Liquidation Value"
                total_equity += float(summary.get('netliquidation', {}).get('amount', 0)) 
                
            except Exception as e:
                print(f"⚠️ IBKR Error fetching balances for account {account_id}: {e}")
                continue

        return {
            'cash': total_cash,
            'buyingPower': total_bp,
            'equity': total_equity # NEW
        }
Step 2: Add the New Endpoint
Now we just wire up the new /equity.json URL route at the bottom of your file (right below your @app.route('/balances.json') block).

Because we added equity to the balance fetchers, we can just reuse them concurrently without writing duplicate API calls!

Python
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
(Bonus: Because we added the equity key to the fetcher functions, your existing /balances.json endpoint will now actually output the equity alongside cash and buying power, so you can pull it from whichever endpoint you prefer in the future!)

Step 3: Restart and Verify
Save the Python script, and run your trusty restart command:

Bash
systemctl restart portfolio-sync
If you visit https://stonks.asdo.ca/equity.json, it will output a beautifully clean JSON payload with your exact net worth across both brokerages!

can we get an endpoint that is easy to use with google sheets built in functions for importing data

Google Sheets notoriously does not have a native IMPORTJSON function without writing custom Apps Script. However, it loves CSV (Comma-Separated Values) files.

If we create an endpoint that spits out raw, unstyled comma-separated text, you can import your live portfolio into any Google Sheet with a single, built-in formula: =IMPORTDATA("https://stonks.asdo.ca/positions.csv").

Here is the exact code to add two new spreadsheet-friendly endpoints (/positions.csv and /balances.csv) to your Python server.

Paste this block right above your if __name__ == '__main__': line:

Python
# ---------------------------------------------------------
# 4. GOOGLE SHEETS EXPORTS (CSV)
# ---------------------------------------------------------
@app.route('/positions.csv', methods=['GET'])
def positions_csv():
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_future = executor.submit(get_questrade_positions)
            ibkr_future = executor.submit(get_ibkr_positions)
            
            qt_positions = qt_future.result()
            ibkr_positions = ibkr_future.result()

        # Build standard CSV headers
        csv_output = "Broker,Symbol,Quantity,AveragePrice\n"
        
        # Append rows
        for p in qt_positions:
            csv_output += f"Questrade,{p['symbol']},{p['qty']},{p['avgPrice']}\n"
        for p in ibkr_positions:
            csv_output += f"IBKR,{p['symbol']},{p['qty']},{p['avgPrice']}\n"

        return app.response_class(
            response=csv_output,
            status=200,
            mimetype='text/csv'
        )

    except Exception as e:
        return app.response_class(response=f"Error,{e}\n", status=500, mimetype='text/csv')

@app.route('/balances.csv', methods=['GET'])
def balances_csv():
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_future = executor.submit(get_questrade_balances)
            ibkr_future = executor.submit(get_ibkr_balances)
            
            qt_bal = qt_future.result()
            ib_bal = ibkr_future.result()

        # Build CSV string
        csv_output = "Account,Cash,BuyingPower,TotalEquity\n"
        csv_output += f"Questrade,{qt_bal['cash']},{qt_bal['buyingPower']},{qt_bal.get('equity', 0)}\n"
        csv_output += f"IBKR,{ib_bal['cash']},{ib_bal['buyingPower']},{ib_bal.get('equity', 0)}\n"
        
        # Add a combined totals row for easy spreadsheet math
        total_cash = qt_bal['cash'] + ib_bal['cash']
        total_bp = qt_bal['buyingPower'] + ib_bal['buyingPower']
        total_eq = qt_bal.get('equity', 0) + ib_bal.get('equity', 0)
        csv_output += f"GRAND TOTAL,{total_cash},{total_bp},{total_eq}\n"

        return app.response_class(
            response=csv_output,
            status=200,
            mimetype='text/csv'
        )

    except Exception as e:
        return app.response_class(response=f"Error,{e}\n", status=500, mimetype='text/csv')

@app.route('/networth.csv', methods=['GET'])
def networth_csv():
    try:
        # Fetch data concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            qt_future = executor.submit(get_questrade_balances)
            ibkr_future = executor.submit(get_ibkr_balances)
            
            qt_bal = qt_future.result()
            ib_bal = ibkr_future.result()

        # Calculate only the grand total equity
        total_eq = qt_bal.get('equity', 0) + ib_bal.get('equity', 0)

        # Return JUST the raw number as plain text
        return app.response_class(
            response=str(total_eq),
            status=200,
            mimetype='text/plain'
        )

    except Exception as e:
        # Return 0 if there's an error so it doesn't break your spreadsheet math
        return app.response_class(response="0", status=500, mimetype='text/plain')

if __name__ == '__main__':
    print(f"🚀 Portfolio Sync Server running at http://localhost:{PORT}")
    print(f"🔗 Set your dashboard Sync URL to: http://localhost:{PORT}/positions.json")
    print(f"🔗 Check live balances at: http://localhost:{PORT}/balances.json")
    app.run(host='0.0.0.0', port=PORT)
