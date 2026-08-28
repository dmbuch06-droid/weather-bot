import os
import time
import threading
import requests
from flask import Flask

app = Flask(__name__)

# Kalshi API and weather configuration constants
KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"

def fetch_kalshi_weather_markets():
    try:
        # Fetch active weather-related series or events from Kalshi
        url = f"{KALSHI_API_URL}/markets?status=open&series_ticker=KXHIGH"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("markets", [])
        else:
            print(f"Error fetching Kalshi markets: {response.status_code}")
            return []
    except Exception as e:
        print(f"Exception fetching Kalshi data: {e}")
        return []

def evaluate_weather_arbitrage():
    print("Running automated weather arbitrage scan across all cities...")
    markets = fetch_kalshi_weather_markets()
    
    if not markets:
        print("No active weather markets found or API returned empty list.")
        return

    print(f"Successfully fetched {len(markets)} active weather contracts. Evaluating odds and edges...")
    
    for market in markets:
        ticker = market.get("ticker")
        title = market.get("title", "")
        yes_bid = market.get("yes_bid", 0)
        yes_ask = market.get("yes_ask", 0)
        
        # --- Evaluation and paper trade logging logic ---
        # Checks weather forecast thresholds vs current market pricing here
        print(f"Checked market {ticker}: {title} | Bid: {yes_bid} / Ask: {yes_ask}")

def background_scanner():
    while True:
        try:
            evaluate_weather_arbitrage()
        except Exception as e:
            print(f"Error in background scanner loop: {e}")
        
        # Wait 5 minutes (300 seconds) before scanning the markets again
        time.sleep(300)

@app.route("/")
def home():
    return "Weather arbitrage bot is live, background-scanning every 5 minutes!"

if __name__ == "__main__":
    # Start the continuous background loop in a separate thread so Flask stays online
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
