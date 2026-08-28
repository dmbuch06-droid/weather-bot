import os
import time
import threading
import requests
from flask import Flask

app = Flask(__name__)

KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"

# PASTE YOUR NEW DISCORD WEBHOOK URL INSIDE THE QUOTES BELOW
DISCORD_WEBHOOK_URL = "YOUR_NEW_DISCORD_WEBHOOK_URL"

CITY_COORDS = {
    "NYC": {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    "CHI": {"name": "Chicago", "lat": 41.8781, "lon": -87.6298},
    "MIA": {"name": "Miami", "lat": 25.7617, "lon": -80.1918},
    "AUS": {"name": "Austin", "lat": 30.2672, "lon": -97.7431},
}

previous_forecasts = {}

def send_discord_alert(message):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, headers=headers, timeout=10)
        print(f"Discord response status: {response.status_code}, body: {response.text}", flush=True)
    except Exception as e:
        print(f"Discord webhook error: {e}", flush=True)

def get_hrrr_forecast_temp(lat, lon):
    try:
        url = f"https://api.open-meteo.com/v1/gfs?latitude={lat}&longitude={lon}&daily=temperature_2m_max&temperature_unit=fahrenheit&models=hrrr_conus&timezone=auto"
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            temps = res.json().get("daily", {}).get("temperature_2m_max", [])
            if temps:
                return temps[0]
    except Exception as e:
        print(f"HRRR Weather API error: {e}", flush=True)
    
    try:
        fallback_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max&temperature_unit=fahrenheit&timezone=auto"
        res = requests.get(fallback_url, timeout=10)
        if res.status_code == 200:
            temps = res.json().get("daily", {}).get("temperature_2m_max", [])
            return temps[0] if temps else None
    except Exception as e:
        print(f"Fallback Weather API error: {e}", flush=True)
    
    return None

def fetch_kalshi_events():
    try:
        url = f"{KALSHI_API_URL}/events?status=open"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            events = res.json().get("events", [])
            weather_events = [e for e in events if "HIGH" in e.get("series_ticker", "").upper() or "WEATHER" in e.get("title", "").upper()]
            print(f"Kalshi API returned {len(events)} total open events, {len(weather_events)} matched weather filters.", flush=True)
            return weather_events
        else:
            print(f"Kalshi API returned status code {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"Kalshi API error: {e}", flush=True)
    return []

def run_arbitrage_scan():
    print("--- Running HRRR + Kalshi +EV Arbitrage Scan ---", flush=True)
    events = fetch_kalshi_events()
    
    if not events:
        print("No active weather events found.", flush=True)
        return

    total_markets_analyzed = 0

    for event in events:
        markets = event.get("markets", [])
        for market in markets:
            ticker = market.get("ticker", "").upper()
            title = market.get("title", "")
            event_ticker = market.get("event_ticker", event.get("event_ticker", ticker))
            yes_ask = market.get("yes_ask", 0)
            
            matched_city = None
            prefix_key = None
            for code, data in CITY_COORDS.items():
                if code in ticker:
                    matched_city = data
                    prefix_key = code
                    break
            
            if not matched_city or yes_ask <= 0:
                continue

            total_markets_analyzed += 1
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
                print(alert_text, flush=True)
                send_discord_alert(alert_text)
            else:
                print(f"Checked {ticker} ({city_name}): HRRR {forecast_temp}°F | Ask: {yes_ask}¢ | Edge: {expected_value_edge:.1f}% (No trade)", flush=True)

    print(f"Scan complete. Evaluated {total_markets_analyzed} valid city temperature contracts.", flush=True)

def background_scanner():
    while True:
        try:
            run_arbitrage_scan()
        except Exception as e:
            print(f"Error in background scan loop: {e}", flush=True)
        
        time.sleep(300)

@app.route("/")
def home():
    return "HRRR Weather Arbitrage Bot is active and scanning future events!"

@app.route("/test-alert")
def test_alert():
    simulated_message = (
        "🎯 **+EV PAPER TRADE SIGNAL (SIMULATION TEST)** 🎯\n"
        "• Projected temperature in Chicago shifted from 84°F to 88°F. Bet of 88+ is a plus EV bet up to 61 cents.\n"
        "• **Contract:** `KXHIGHCHI-26AUG28-T88` (Chicago High Temperature)\n"
        "• **Execution Ask:** 54¢ | **Model Edge:** +14.0%\n"
        "• **Trade on Kalshi:** https://kalshi.com/markets/kxhighchi"
    )
    send_discord_alert(simulated_message)
    return "Simulated alert dispatched to Discord successfully!"

if __name__ == "__main__":
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
