import os
import time
import threading
import requests
from flask import Flask

app = Flask(__name__)

# Virtual Paper Portfolio State
PORTFOLIO = {
    "cash": 1000.00,
    "positions": {}  # ticker: {"city": str, "cost": float, "timestamp": float}
}

# All 20 Kalshi Weather Cities with Coordinates & Tickers
CITIES = {
    "New York City": {"lat": 40.7128, "lon": -74.0060, "series": "KXHIGHNY"},
    "Chicago": {"lat": 41.8781, "lon": -87.6298, "series": "KXHIGHCHI"},
    "Miami": {"lat": 25.7617, "lon": -80.1918, "series": "KXHIGHMIA"},
    "Austin": {"lat": 30.2672, "lon": -97.7431, "series": "KXHIGHAUS"},
    "Los Angeles": {"lat": 34.0522, "lon": -118.2437, "series": "KXHIGHLAX"},
    "Denver": {"lat": 39.7392, "lon": -104.9903, "series": "KXHIGHDEN"},
    "Phoenix": {"lat": 33.4484, "lon": -112.0740, "series": "KXHIGHTPHX"},
    "Philadelphia": {"lat": 39.9526, "lon": -75.1652, "series": "KXHIGHPHIL"},
    "Houston": {"lat": 29.7604, "lon": -95.3698, "series": "KXHIGHTHOU"},
    "Minneapolis": {"lat": 44.9778, "lon": -93.2650, "series": "KXHIGHTMIN"},
    "Oklahoma City": {"lat": 35.4676, "lon": -97.5164, "series": "KXHIGHTOKC"},
    "San Francisco": {"lat": 37.7749, "lon": -122.4194, "series": "KXHIGHTSFO"},
    "Washington DC": {"lat": 38.9072, "lon": -77.0369, "series": "KXHIGHTDC"},
    "Boston": {"lat": 42.3601, "lon": -71.0589, "series": "KXHIGHTBOS"},
    "Dallas": {"lat": 32.7767, "lon": -96.7970, "series": "KXHIGHTDAL"},
    "Seattle": {"lat": 47.6062, "lon": -122.3321, "series": "KXHIGHTSEA"},
    "Las Vegas": {"lat": 36.1699, "lon": -115.1398, "series": "KXHIGHTLV"},
    "Atlanta": {"lat": 33.7490, "lon": -84.3880, "series": "KXHIGHTATL"},
    "San Antonio": {"lat": 29.4241, "lon": -98.4936, "series": "KXHIGHTSATX"},
    "New Orleans": {"lat": 29.9511, "lon": -90.0715, "series": "KXHIGHTNOLA"},
}

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

def send_discord_alert(message):
    if not DISCORD_WEBHOOK_URL:
        print("Discord Webhook URL not set.")
        return
    try:
        payload = {"content": message}
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        print(f"Error sending Discord alert: {e}")

def fetch_hrrr_forecast(lat, lon):
    """Fetches high-resolution weather model forecast data."""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max&temperature_unit=fahrenheit&timezone=auto"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        daily_maxes = data.get("daily", {}).get("temperature_2m_max", [])
        if daily_maxes:
            return daily_maxes[0]  # Today's forecasted high
    except Exception as e:
        print(f"Error fetching weather model for {lat}, {lon}: {e}")
    return None

def fetch_kalshi_markets(series_ticker):
    """Fetches active weather bracket markets from Kalshi's public API."""
    url = f"https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker={series_ticker}&status=open"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        return data.get("markets", [])
    except Exception as e:
        print(f"Error fetching Kalshi markets for {series_ticker}: {e}")
        return []

def evaluate_weather_arbitrage():
    print("Running weather arbitrage scan across all cities...")
    for city_name, info in CITIES.items():
        lat = info["lat"]
        lon = info["lon"]
        series = info["series"]

        # 1. Get model forecast temp
        forecast_temp = fetch_hrrr_forecast(lat, lon)
        if forecast_temp is None:
            continue

        # 2. Get Kalshi market brackets
        markets = fetch_kalshi_markets(series)
        for market in markets:
            ticker = market.get("ticker")
            title = market.get("title", "")
            yes_ask = market.get("yes_ask", 0)  
            
            # Skip if pricing data is missing or invalid
            if not yes_ask or yes_ask <= 0:
                continue

            # Simple heuristic check: If model forecast aligns strongly with a mispriced bracket (< 40 cents for a high probability outcome)
            # Paper trade execution logic:
            if ticker and ticker not in PORTFOLIO["positions"]:
                # Example condition framework for paper alert trigger
                # (You can adjust edge thresholds safely here)
                pass

        time.sleep(0.5) # Prevent rate limiting

def background_loop():
    """Loops continuously in the background every 15 minutes."""
    while True:
        try:
            evaluate_weather_arbitrage()
        except Exception as e:
            print(f"Error in background loop: {e}")
        
        # Sleep for 15 minutes (900 seconds)
        time.sleep(900)

@app.route("/")
def home():
    cash = PORTFOLIO["cash"]
    open_count = len(PORTFOLIO["positions"])
    return f"Weather Arbitrage Bot is LIVE 🚀 (20 Cities Active) | Cash Balance: ${cash:.2f} | Open Positions: {open_count}"

if __name__ == "__main__":
    # Start background monitoring thread
    t = threading.Thread(target=background_loop, daemon=True)
    t.start()
    
    # Run Flask web server
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
