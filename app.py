import os
import time
import threading
import re
import requests
from flask import Flask

app = Flask(__name__)

# Virtual Paper Portfolio State
PORTFOLIO = {
    "cash": 1000.00,
    "positions": {}  # ticker: {"city": str, "cost": float, "timestamp": float, "ev": float}
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
MIN_EV_THRESHOLD = 0.10  # Minimum 10% Expected Value required to trigger

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
    """Fetches high-resolution weather model forecast data (Open-Meteo)."""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max&temperature_unit=fahrenheit&timezone=auto"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        daily_maxes = data.get("daily", {}).get("temperature_2m_max", [])
        if daily_maxes:
            return float(daily_maxes[0])  # Today's forecasted high in Fahrenheit
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

def parse_bracket_range(title):
    """Parses Kalshi market titles to extract temperature brackets (e.g., '70 to 71', 'or above', 'or below')."""
    title_lower = title.lower()
    
    # Check for 'or higher' / 'or above'
    high_match = re.search(r'(\d+)(?:\s*degrees)?\s*(?:or higher|or above|and above)', title_lower)
    if high_match:
        return float(high_match.group(1)), 999.0

    # Check for 'or lower' / 'or below'
    low_match = re.search(r'(\d+)(?:\s*degrees)?\s*(?:or lower|or below|and below)', title_lower)
    if low_match:
        return -999.0, float(low_match.group(1))

    # Check for range (e.g., "70 to 71")
    range_match = re.search(r'(\d+)\s*(?:to|-)\s*(\d+)', title_lower)
    if range_match:
        return float(range_match.group(1)), float(range_match.group(2))
        
    return None, None

def calculate_model_probability(forecast_temp, low_bound, high_bound):
    """
    Assigns a baseline probability using a normal distribution curve 
    around the forecast temperature.
    """
    import math
    sigma = 1.5  # Standard deviation uncertainty window in Fahrenheit for high temps
    
    def normal_cdf(x, mu, sd):
        return 0.5 * (1 + math.erf((x - mu) / (sd * math.sqrt(2))))

    # If it's a range bracket
    if low_bound != -999.0 and high_bound != 999.0:
        # Probability that temp falls between (low_bound - 0.5) and (high_bound + 0.5)
        prob = normal_cdf(high_bound + 0.5, forecast_temp, sigma) - normal_cdf(low_bound - 0.5, forecast_temp, sigma)
    elif low_bound == -999.0:
        prob = normal_cdf(high_bound + 0.5, forecast_temp, sigma)
    else:
        prob = 1.0 - normal_cdf(low_bound - 0.5, forecast_temp, sigma)
        
    return max(0.01, min(0.99, prob))

def evaluate_weather_arbitrage():
    print("Running weather arbitrage scan across all cities...")
    for city_name, info in CITIES.items():
        lat = info["lat"]
        lon = info["lon"]
        series = info["series"]

        forecast_temp = fetch_hrrr_forecast(lat, lon)
        if forecast_temp is None:
            continue

        markets = fetch_kalshi_markets(series)
        for market in markets:
            ticker = market.get("title") or market.get("ticker")
            market_ticker = market.get("ticker")
            title = market.get("title", "")
            yes_ask_cents = market.get("yes_ask", 0)  # Price in cents (e.g., 35 means $0.35)

            if not yes_ask_cents or yes_ask_cents <= 0:
                continue

            current_price = yes_ask_cents / 100.0  # Convert to dollar scale ($0.35)
            
            low_bound, high_bound = parse_bracket_range(title)
            if low_bound is None:
                continue

            # Calculate model probability for this specific bracket
            model_prob = calculate_model_probability(forecast_temp, low_bound, high_bound)
            
            # Mathematical calculations
            max_profitable_price = model_prob  # Fair value price ceiling
            expected_value_pct = (model_prob - current_price) / current_price

            # Check if this qualifies as a high-EV opportunity and hasn't been traded yet
            if expected_value_pct >= MIN_EV_THRESHOLD and market_ticker not in PORTFOLIO["positions"]:
                # Execute Paper Trade
                trade_cost = current_price * 100  # Assume 100 contract sizing simulation or $1 unit block
                if PORTFOLIO["cash"] >= trade_cost:
                    PORTFOLIO["cash"] -= trade_cost
                    PORTFOLIO["positions"][market_ticker] = {
                        "city": city_name,
                        "title": title,
                        "cost": current_price,
                        "ev": expected_value_pct,
                        "timestamp": time.time()
                    }

                    # Format Discord Alert Message as requested
                    alert_msg = (
                        f"🚨 **WEATHER +EV PAPER TRADE DETECTED** 🚨\n"
                        f"🌍 **City:** {city_name} (Forecast High: {forecast_temp}°F)\n"
                        f"📊 **Market:** {title}\n"
                        f"🏷️ **Ticker:** `{market_ticker}`\n"
                        f"💰 **Current Ask Price:** `${current_price:.2f}`\n"
                        f"📈 **Max Profitable Price (Fair Value):** `${max_profitable_price:.2f}`\n"
                        f"⚡ **Expected Value (EV):** `+{expected_value_pct * 100:.1f}%`\n"
                        f"💵 **Virtual Cash Remaining:** `${PORTFOLIO['cash']:.2f}`"
                    )
                    send_discord_alert(alert_msg)

        time.sleep(0.5)

def background_loop():
    while True:
        try:
            evaluate_weather_arbitrage()
        except Exception as e:
            print(f"Error in background loop: {e}")
        time.sleep(900) # Loop every 15 minutes

@app.route("/")
def home():
    cash = PORTFOLIO["cash"]
    open_count = len(PORTFOLIO["positions"])
    return f"Weather Arbitrage Bot is LIVE 🚀 (20 Cities + EV Engine Active) | Cash Balance: ${cash:.2f} | Open Positions: {open_count}"

if __name__ == "__main__":
    t = threading.Thread(target=background_loop, daemon=True)
    t.start()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
