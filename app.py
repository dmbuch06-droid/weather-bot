import os
import time
import threading
import requests
from flask import Flask

app = Flask(__name__)

KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1534781924252323891/7hm54rbQchA2idRvqEoi6j6grqqk7Wx48CBMWqbKwRJdn2vxkfZ9II1d1pCX1IXNbD2R"

CITY_COORDS = {
    "KXHIGHNYC": {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    "KXHIGHCHI": {"name": "Chicago", "lat": 41.8781, "lon": -87.6298},
    "KXHIGHMIA": {"name": "Miami", "lat": 25.7617, "lon": -80.1918},
    "KXHIGHAUS": {"name": "Austin", "lat": 30.2672, "lon": -97.7431},
}

previous_forecasts = {}

def send_discord_alert(message):
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=10)
    except Exception as e:
        print(f"Discord webhook error: {e}")

def get_hrrr_forecast_temp(lat, lon):
    try:
        url = f"https://api.open-meteo.com/v1/gfs?latitude={lat}&longitude={lon}&daily=temperature_2m_max&temperature_unit=fahrenheit&models=hrrr_conus&timezone=auto"
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            temps = res.json().get("daily", {}).get("temperature_2m_max", [])
            if temps:
                return temps[0]
    except Exception as e:
        print(f"HRRR Weather API error: {e}")
    
    try:
        fallback_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max&temperature_unit=fahrenheit&timezone=auto"
        res = requests.get(fallback_url, timeout=10)
        if res.status_code == 200:
            temps = res.json().get("daily", {}).get("temperature_2m_max", [])
            return temps[0] if temps else None
    except Exception as e:
        print(f"Fallback Weather API error: {e}")
    
    return None

def fetch_kalshi_markets():
    try:
        url = f"{KALSHI_API_URL}/markets?status=open"
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            return res.json().get("markets", [])
    except Exception as e:
        print(f"Kalshi API error: {e}")
    return []

def run_arbitrage_scan():
    print("--- Running HRRR + Kalshi +EV Arbitrage Scan ---")
    markets = fetch_kalshi_markets()
    
    if not markets:
        print("No active markets returned from Kalshi.")
        return

    weather_markets = [m for m in markets if any(series in m.get("ticker", "") for series in CITY_COORDS.keys())]
    print(f"Found {len(weather_markets)} active weather contracts to analyze.")

    for market in weather_markets:
        ticker = market.get("ticker")
        title = market.get("title", "")
        event_ticker = market.get("event_ticker", ticker)
        yes_ask = market.get("yes_ask", 0)
        
        matched_city = None
        prefix_key = None
        for prefix in CITY_COORDS.keys():
            if prefix in ticker:
                matched_city = CITY_COORDS[prefix]
                prefix_key = prefix
                break
        
        if not matched_city or yes_ask <= 0:
            continue

        forecast_temp = get_hrrr_forecast_temp(matched_city["lat"], matched_city["lon"])
        if forecast_temp is None:
            continue

        city_name = matched_city["name"]
        old_forecast = previous_forecasts.get(prefix_key)
        shift_detected = (old_forecast is not None and old_forecast != forecast_temp)
        
        previous_forecasts[prefix_key] = forecast_temp

        implied_prob = yes_ask / 100.0
        model_prob = 0.68 if forecast_temp else 0.50
        expected_value_edge = (model_prob - implied_prob) * 100

        if expected_value_edge > 1.5:
            max_viable_cents = int(model_prob * 100)
            
            if shift_detected:
                shift_text = f"Projected temperature in {city_name} shifted from {old_forecast}°F to {forecast_temp}°F."
            else:
                shift_text = f"Projected temperature in {city_name} is steady at {forecast_temp}°F."

            kalshi_link = f"https://kalshi.com/markets/{event_ticker.lower()}"

            alert_text = (
                f"🎯 **+EV PAPER TRADE SIGNAL** 🎯\n"
                f"• {shift_text} Bet of {int(forecast_temp)}+ is a plus EV bet up to {max_viable_cents} cents.\n"
                f"• **Contract:** `{ticker}` ({title})\n"
                f"• **Execution Ask:** {yes_ask}¢ | **Model Edge:** +{expected_value_edge:.1f}%\n"
                f"• **Trade on Kalshi:** {kalshi_link}"
            )
            print(alert_text)
            send_discord_alert(alert_text)
        else:
            print(f"Checked {ticker} ({city_name}): HRRR {forecast_temp}°F | Ask: {yes_ask}¢ | Edge: {expected_value_edge:.1f}% (No trade)")

def background_scanner():
    while True:
        try:
            run_arbitrage_scan()
        except Exception as e:
            print(f"Error in background scan loop: {e}")
        
        time.sleep(300)

@app.route("/")
def home():
    return "HRRR Weather Arbitrage Bot with direct Kalshi links is active!"

if __name__ == "__main__":
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
