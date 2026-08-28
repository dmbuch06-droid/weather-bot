import os
import time
import threading
from datetime import datetime

import requests
from flask import Flask, jsonify

# ============================================================

# CONFIGURATION

# ============================================================

app = Flask(**name**)

KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Discord webhook is stored in Render as an environment variable.

# DO NOT paste your actual webhook URL into this file.

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# Scan every 5 minutes

SCAN_INTERVAL_SECONDS = 300

# ============================================================

# CITIES

# ============================================================

CITY_COORDS = {
"NYC": {
"name": "New York",
"lat": 40.7128,
"lon": -74.0060,
},
"CHI": {
"name": "Chicago",
"lat": 41.8781,
"lon": -87.6298,
},
"MIA": {
"name": "Miami",
"lat": 25.7617,
"lon": -80.1918,
},
"AUS": {
"name": "Austin",
"lat": 30.2672,
"lon": -97.7431,
},
}

# ============================================================

# TEMPORARY MEMORY

#

# This resets if Render restarts.

# Later we will replace this with a database.

# ============================================================

previous_forecasts = {}
forecast_cache = {}

# ============================================================

# DISCORD

# ============================================================

def send_discord_alert(message):

```
if not DISCORD_WEBHOOK_URL:
    print(
        "ERROR: DISCORD_WEBHOOK_URL is not configured.",
        flush=True,
    )
    return False

try:

    response = requests.post(
        DISCORD_WEBHOOK_URL,
        json={"content": message},
        timeout=10,
    )

    print(
        f"Discord response: {response.status_code}",
        flush=True,
    )

    if response.status_code in (200, 204):
        return True

    print(
        f"Discord error body: {response.text}",
        flush=True,
    )

    return False

except Exception as e:

    print(
        f"Discord webhook error: {e}",
        flush=True,
    )

    return False
```

# ============================================================

# WEATHER

# ============================================================

def get_weather_forecast(lat, lon):

```
try:

    url = (
        "https://api.open-meteo.com/v1/gfs"
        f"?latitude={lat}"
        f"&longitude={lon}"
        "&daily=temperature_2m_max"
        "&temperature_unit=fahrenheit"
        "&models=hrrr_conus"
        "&timezone=auto"
    )

    response = requests.get(
        url,
        timeout=15,
    )

    print(
        f"Weather API status: {response.status_code}",
        flush=True,
    )

    if response.status_code == 200:

        data = response.json()

        daily = data.get(
            "daily",
            {},
        )

        dates = daily.get(
            "time",
            [],
        )

        temps = daily.get(
            "temperature_2m_max",
            [],
        )

        if dates and temps:

            return {
                "source": "HRRR",
                "dates": dates,
                "temps": temps,
            }

except Exception as e:

    print(
        f"HRRR forecast error: {e}",
        flush=True,
    )

# --------------------------------------------------------
# FALLBACK
# --------------------------------------------------------

try:

    fallback_url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}"
        f"&longitude={lon}"
        "&daily=temperature_2m_max"
        "&temperature_unit=fahrenheit"
        "&timezone=auto"
    )

    response = requests.get(
        fallback_url,
        timeout=15,
    )

    print(
        f"Fallback weather API status: "
        f"{response.status_code}",
        flush=True,
    )

    if response.status_code == 200:

        data = response.json()

        daily = data.get(
            "daily",
            {},
        )

        dates = daily.get(
            "time",
            [],
        )

        temps = daily.get(
            "temperature_2m_max",
            [],
        )

        if dates and temps:

            return {
                "source": "Open-Meteo",
                "dates": dates,
                "temps": temps,
            }

except Exception as e:

    print(
        f"Fallback weather error: {e}",
        flush=True,
    )

return None
```

def get_forecast_for_city(city_code):

```
global forecast_cache

if city_code in forecast_cache:
    return forecast_cache[city_code]

city = CITY_COORDS.get(city_code)

if not city:
    return None

weather_data = get_weather_forecast(
    city["lat"],
    city["lon"],
)

if not weather_data:
    return None

dates = weather_data.get(
    "dates",
    [],
)

temps = weather_data.get(
    "temps",
    [],
)

if not dates or not temps:
    return None

result = {
    "city_code": city_code,
    "city_name": city["name"],
    "date": dates[0],
    "forecast_temp": temps[0],
    "source": weather_data["source"],
    "retrieved_at": datetime.utcnow().isoformat(),
}

forecast_cache[city_code] = result

return result
```

# ============================================================

# KALSHI

# ============================================================

def fetch_kalshi_events():

```
try:

    url = (
        f"{KALSHI_API_URL}"
        "/events?status=open"
    )

    response = requests.get(
        url,
        timeout=15,
    )

    print(
        f"Kalshi API status: "
        f"{response.status_code}",
        flush=True,
    )

    if response.status_code != 200:

        print(
            f"Kalshi error: {response.text}",
            flush=True,
        )

        return []

    data = response.json()

    events = data.get(
        "events",
        [],
    )

    print(
        f"Kalshi returned "
        f"{len(events)} open events.",
        flush=True,
    )

    return events

except Exception as e:

    print(
        f"Kalshi API error: {e}",
        flush=True,
    )

    return []
```

# ============================================================

# CITY IDENTIFICATION

# ============================================================

def identify_city(text):

```
text = text.upper()

for code in CITY_COORDS:

    if code in text:
        return code

return None
```

# ============================================================

# FORECAST CHANGE DETECTION

# ============================================================

def detect_forecast_change(
city_code,
target_date,
forecast_temp,
):

```
key = (
    f"{city_code}:"
    f"{target_date}:"
    f"high_temperature"
)

old_forecast = previous_forecasts.get(key)

previous_forecasts[key] = forecast_temp

if old_forecast is None:

    return {
        "changed": False,
        "old": None,
        "new": forecast_temp,
        "delta": 0,
    }

delta = forecast_temp - old_forecast

return {
    "changed": old_forecast != forecast_temp,
    "old": old_forecast,
    "new": forecast_temp,
    "delta": delta,
}
```

# ============================================================

# TEMPORARY RESEARCH MODEL

#

# THIS IS NOT A REAL PROBABILITY MODEL YET.

# ============================================================

def temporary_model_probability():

```
return 0.68
```

# ============================================================

# MAIN SCANNER

# ============================================================

def run_scan():

```
global forecast_cache

print(
    "\n========================================",
    flush=True,
)

print(
    "STARTING WEATHER MARKET SCAN",
    flush=True,
)

print(
    f"Time: {datetime.utcnow().isoformat()} UTC",
    flush=True,
)

# Fresh weather cache for each scan
forecast_cache = {}

events = fetch_kalshi_events()

if not events:

    print(
        "No events returned from Kalshi.",
        flush=True,
    )

    return

markets_checked = 0

weather_related_events = 0


# --------------------------------------------------------
# LOOP THROUGH EVENTS
# --------------------------------------------------------

for event in events:

    event_ticker = event.get(
        "event_ticker",
        "",
    )

    event_title = event.get(
        "title",
        "",
    )

    series_ticker = event.get(
        "series_ticker",
        "",
    )

    combined_text = (
        f"{event_ticker} "
        f"{event_title} "
        f"{series_ticker}"
    )

    city_code = identify_city(
        combined_text,
    )


    # Only continue if one of our cities is detected.
    #
    # IMPORTANT:
    # This is intentionally broad while we diagnose
    # Kalshi's current market structure.
    if not city_code:
        continue


    weather_related_events += 1

    print(
        f"\nPotential city event found:",
        flush=True,
    )

    print(
        f"Event ticker: {event_ticker}",
        flush=True,
    )

    print(
        f"Title: {event_title}",
        flush=True,
    )

    print(
        f"City: {city_code}",
        flush=True,
    )


    # ----------------------------------------------------
    # GET WEATHER FORECAST
    # ----------------------------------------------------

    forecast = get_forecast_for_city(
        city_code,
    )

    if not forecast:

        print(
            f"No weather forecast for "
            f"{city_code}",
            flush=True,
        )

        continue


    city_name = forecast[
        "city_name"
    ]

    forecast_temp = forecast[
        "forecast_temp"
    ]

    forecast_date = forecast[
        "date"
    ]

    forecast_source = forecast[
        "source"
    ]


    # ----------------------------------------------------
    # FORECAST CHANGE
    # ----------------------------------------------------

    change = detect_forecast_change(
        city_code,
        forecast_date,
        forecast_temp,
    )


    # ----------------------------------------------------
    # GET MARKETS
    #
    # Some Kalshi API responses may not include
    # markets directly inside the event.
    #
    # We log this for now so we can inspect the structure.
    # ----------------------------------------------------

    markets = event.get(
        "markets",
        [],
    )

    print(
        f"Markets attached to event: "
        f"{len(markets)}",
        flush=True,
    )


    if not markets:

        continue


    # ----------------------------------------------------
    # LOOP THROUGH MARKETS
    # ----------------------------------------------------

    for market in markets:

        markets_checked += 1

        ticker = market.get(
            "ticker",
            "",
        )

        title = market.get(
            "title",
            "",
        )

        yes_ask = market.get(
            "yes_ask",
            0,
        )


        print(
            f"Checking market: {ticker}",
            flush=True,
        )


        if not yes_ask:

            print(
                "No YES ask available.",
                flush=True,
            )

            continue


        implied_probability = (
            float(yes_ask)
            / 100
        )


        # TEMPORARY PLACEHOLDER MODEL
        model_probability = (
            temporary_model_probability()
        )


        edge = (
            model_probability
            - implied_probability
        )


        print(
            f"Forecast: "
            f"{forecast_temp:.1f}F | "
            f"Ask: {yes_ask}c | "
            f"Temporary edge: "
            f"{edge * 100:.1f}%",
            flush=True,
        )


        # ------------------------------------------------
        # SIGNAL
        # ------------------------------------------------

        if edge > 0.015:

            if change["old"] is not None:

                forecast_text = (
                    f"{change['old']:.1f}F "
                    f"-> "
                    f"{forecast_temp:.1f}F "
                    f"({change['delta']:+.1f}F)"
                )

            else:

                forecast_text = (
                    f"Initial forecast: "
                    f"{forecast_temp:.1f}F"
                )


            alert = (
                "WEATHER BOT PAPER SIGNAL\n\n"

                f"City: {city_name}\n"
                f"Date: {forecast_date}\n\n"

                f"Forecast Source: "
                f"{forecast_source}\n"

                f"Forecast: "
                f"{forecast_text}\n\n"

                f"Contract: "
                f"{ticker}\n"

                f"{title}\n\n"

                f"YES Ask: "
                f"{yes_ask} cents\n"

                f"Temporary Model Probability: "
                f"{model_probability * 100:.1f}%\n"

                f"Temporary Edge: "
                f"{edge * 100:+.1f}%\n\n"

                "NOTE: Probability model is currently "
                "a placeholder."
            )


            print(
                "\nSIGNAL GENERATED:",
                flush=True,
            )

            print(
                alert,
                flush=True,
            )


            send_discord_alert(
                alert,
            )


print(
    "\nSCAN COMPLETE",
    flush=True,
)

print(
    f"Total events: {len(events)}",
    flush=True,
)

print(
    f"Potential city events: "
    f"{weather_related_events}",
    flush=True,
)

print(
    f"Markets checked: "
    f"{markets_checked}",
    flush=True,
)

print(
    "========================================\n",
    flush=True,
)
```

# ============================================================

# BACKGROUND SCANNER

# ============================================================

def background_scanner():

```
print(
    "Background scanner started.",
    flush=True,
)

while True:

    try:

        run_scan()

    except Exception as e:

        print(
            f"BACKGROUND SCAN ERROR: {e}",
            flush=True,
        )


    print(
        f"Waiting {SCAN_INTERVAL_SECONDS} seconds...",
        flush=True,
    )

    time.sleep(
        SCAN_INTERVAL_SECONDS
    )
```

# ============================================================

# WEB ROUTES

# ============================================================

@app.route("/")
def home():

```
return (
    "Weather Bot is running."
)
```

@app.route("/health")
def health():

```
return jsonify(
    {
        "status": "ok",
        "discord_configured": bool(
            DISCORD_WEBHOOK_URL
        ),
        "scan_interval_seconds": (
            SCAN_INTERVAL_SECONDS
        ),
        "cities": list(
            CITY_COORDS.keys()
        ),
    }
)
```

@app.route("/test-alert")
def test_alert():

```
message = (
    "WEATHER BOT TEST ALERT\n\n"
    "If you received this message, "
    "your Discord webhook is working."
)

success = send_discord_alert(
    message,
)

if success:

    return (
        "Test alert sent successfully."
    )

return (
    "Test alert failed. "
    "Check Render environment variables."
), 500
```

@app.route("/run-scan")
def manual_scan():

```
try:

    run_scan()

    return (
        "Scan completed. "
        "Check Render logs."
    )

except Exception as e:

    return (
        f"Scan failed: {e}"
    ), 500
```

# ============================================================

# START APP

# ============================================================

if **name** == "**main**":

```
scanner_thread = threading.Thread(
    target=background_scanner,
    daemon=True,
)

scanner_thread.start()


port = int(
    os.environ.get(
        "PORT",
        10000,
    )
)


print(
    f"Starting server on port {port}",
    flush=True,
)


app.run(
    host="0.0.0.0",
    port=port,
)
