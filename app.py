```python
import os
import time
import threading
from datetime import datetime

import requests
from flask import Flask, jsonify


# ============================================================
# APP CONFIGURATION
# ============================================================

app = Flask(__name__)

# Kalshi API
KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Discord webhook
#
# DO NOT put the actual webhook URL in this file.
#
# On Render, create an environment variable:
#
# Key:
# DISCORD_WEBHOOK_URL
#
# Value:
# Your actual Discord webhook URL
#
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")


# How often to run the scanner, in seconds
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
# MEMORY STORAGE
#
# NOTE:
# These are in-memory dictionaries.
#
# If Render restarts, they reset.
#
# Later we should replace these with PostgreSQL or another
# persistent database.
# ============================================================

previous_forecasts = {}

forecast_cache = {}


# ============================================================
# DISCORD
# ============================================================

def send_discord_alert(message):
    """
    Send a message to Discord using the webhook stored
    in the environment variable DISCORD_WEBHOOK_URL.
    """

    if not DISCORD_WEBHOOK_URL:
        print(
            "ERROR: DISCORD_WEBHOOK_URL environment variable "
            "is not configured.",
            flush=True,
        )
        return False

    try:
        headers = {
            "User-Agent": (
                "WeatherMarketResearchBot/1.0"
            )
        }

        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": message},
            headers=headers,
            timeout=10,
        )

        print(
            f"Discord response status: {response.status_code}",
            flush=True,
        )

        if response.status_code in (200, 204):
            return True

        print(
            f"Discord response body: {response.text}",
            flush=True,
        )

        return False

    except Exception as e:
        print(
            f"Discord webhook error: {e}",
            flush=True,
        )

        return False


# ============================================================
# WEATHER FORECASTS
# ============================================================

def get_hrrr_forecast(lat, lon):
    """
    Get daily maximum temperature forecasts.

    Returns a dictionary:

    {
        "dates": [...],
        "temps": [...]
    }

    The first request attempts to request the HRRR CONUS model
    through Open-Meteo.

    If that fails, the function falls back to the standard
    Open-Meteo forecast endpoint.
    """

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

        res = requests.get(
            url,
            timeout=15,
        )

        if res.status_code == 200:

            data = res.json()

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

            print(
                "HRRR response returned no forecast data.",
                flush=True,
            )

        else:

            print(
                f"HRRR API status: {res.status_code}",
                flush=True,
            )

    except Exception as e:

        print(
            f"HRRR Weather API error: {e}",
            flush=True,
        )

    # --------------------------------------------------------
    # FALLBACK FORECAST
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

        res = requests.get(
            fallback_url,
            timeout=15,
        )

        if res.status_code == 200:

            data = res.json()

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
                    "source": "Open-Meteo Forecast",
                    "dates": dates,
                    "temps": temps,
                }

        else:

            print(
                f"Fallback weather API status: {res.status_code}",
                flush=True,
            )

    except Exception as e:

        print(
            f"Fallback Weather API error: {e}",
            flush=True,
        )

    return None


def get_forecast_for_date(
    city_code,
    target_date=None,
):
    """
    Get the forecast for a city.

    If target_date is None, return today's/first available
    daily forecast.

    Later, once we parse the Kalshi ticker date correctly,
    target_date should be passed here.

    target_date format:
    YYYY-MM-DD
    """

    city = CITY_COORDS.get(
        city_code,
    )

    if not city:
        return None

    # Cache key includes city AND date
    cache_key = (
        city_code,
        target_date or "first_available",
    )

    if cache_key in forecast_cache:

        return forecast_cache[cache_key]

    forecast_data = get_hrrr_forecast(
        city["lat"],
        city["lon"],
    )

    if not forecast_data:

        return None

    dates = forecast_data.get(
        "dates",
        [],
    )

    temps = forecast_data.get(
        "temps",
        [],
    )

    if not dates or not temps:

        return None

    # --------------------------------------------------------
    # TARGET DATE MATCHING
    # --------------------------------------------------------

    if target_date:

        for date, temp in zip(
            dates,
            temps,
        ):

            if date == target_date:

                result = {
                    "city_code": city_code,
                    "city_name": city["name"],
                    "date": date,
                    "forecast_temp": temp,
                    "source": forecast_data["source"],
                    "retrieved_at": (
                        datetime.utcnow()
                        .isoformat()
                    ),
                }

                forecast_cache[cache_key] = result

                return result

        print(
            f"No forecast found for "
            f"{city_code} "
            f"on {target_date}",
            flush=True,
        )

        return None

    # --------------------------------------------------------
    # FIRST AVAILABLE FORECAST
    # --------------------------------------------------------

    result = {
        "city_code": city_code,
        "city_name": city["name"],
        "date": dates[0],
        "forecast_temp": temps[0],
        "source": forecast_data["source"],
        "retrieved_at": (
            datetime.utcnow()
            .isoformat()
        ),
    }

    forecast_cache[cache_key] = result

    return result


# ============================================================
# KALSHI
# ============================================================

def fetch_kalshi_events():
    """
    Fetch open events from Kalshi.

    NOTE:
    The exact Kalshi API response structure should be verified
    with live API logging.

    This is intentionally kept close to your existing
    implementation for now.
    """

    try:

        url = (
            f"{KALSHI_API_URL}"
            "/events?status=open"
        )

        headers = {
            "User-Agent": (
                "WeatherMarketResearchBot/1.0"
            )
        }

        res = requests.get(
            url,
            headers=headers,
            timeout=15,
        )

        if res.status_code == 200:

            data = res.json()

            events = data.get(
                "events",
                [],
            )

            weather_events = []

            for event in events:

                series_ticker = event.get(
                    "series_ticker",
                    "",
                ).upper()

                title = event.get(
                    "title",
                    "",
                ).upper()

                if (
                    "HIGH" in series_ticker
                    or "WEATHER" in title
                    or "TEMPERATURE" in title
                ):

                    weather_events.append(
                        event
                    )

            print(
                f"Kalshi API returned "
                f"{len(events)} total open events, "
                f"{len(weather_events)} "
                f"matched weather filters.",
                flush=True,
            )

            return weather_events

        print(
            f"Kalshi API returned "
            f"status {res.status_code}: "
            f"{res.text}",
            flush=True,
        )

    except Exception as e:

        print(
            f"Kalshi API error: {e}",
            flush=True,
        )

    return []


# ============================================================
# CITY MATCHING
# ============================================================

def identify_city_from_ticker(ticker):
    """
    Try to identify one of our configured cities
    from a Kalshi ticker.
    """

    ticker_upper = ticker.upper()

    for code in CITY_COORDS:

        if code in ticker_upper:

            return code

    return None


# ============================================================
# FORECAST CHANGE TRACKING
# ============================================================

def check_forecast_change(
    city_code,
    target_date,
    forecast_temp,
):
    """
    Track forecast changes.

    Returns:

    {
        "old_forecast": ...,
        "new_forecast": ...,
        "changed": True/False,
        "delta": ...
    }

    The key includes city AND date so forecasts for different
    days do not overwrite each other.
    """

    forecast_key = (
        f"{city_code}:"
        f"{target_date}:"
        "high_temperature"
    )

    old_forecast = previous_forecasts.get(
        forecast_key,
    )

    previous_forecasts[
        forecast_key
    ] = forecast_temp

    if old_forecast is None:

        return {
            "old_forecast": None,
            "new_forecast": forecast_temp,
            "changed": False,
            "delta": 0,
        }

    delta = (
        forecast_temp
        - old_forecast
    )

    return {
        "old_forecast": old_forecast,
        "new_forecast": forecast_temp,
        "changed": (
            old_forecast
            != forecast_temp
        ),
        "delta": delta,
    }


# ============================================================
# TEMPORARY RESEARCH MODEL
#
# IMPORTANT:
#
# This is NOT a real calibrated probability model.
#
# We are keeping it temporarily so the rest of the pipeline
# can be tested.
#
# The next major upgrade should parse the contract threshold
# and calculate:
#
# P(actual temperature >= threshold)
#
# using a calibrated forecast-error distribution.
# ============================================================

def temporary_model_probability(
    forecast_temp,
):
    """
    Temporary placeholder probability.

    DO NOT interpret this as a validated trading model.
    """

    if forecast_temp is None:

        return 0.50

    return 0.68


# ============================================================
# SCANNER
# ============================================================

def run_arbitrage_scan():

    global forecast_cache

    print(
        "\n"
        "================================================",
        flush=True,
    )

    print(
        "--- Running Weather + Kalshi Research Scan ---",
        flush=True,
    )

    print(
        f"Scan started: "
        f"{datetime.utcnow().isoformat()} UTC",
        flush=True,
    )

    # Clear the per-scan forecast cache.
    #
    # This prevents repeated API calls during one scan while
    # ensuring the next scan requests fresh information.
    forecast_cache = {}

    events = fetch_kalshi_events()

    if not events:

        print(
            "No active weather events found.",
            flush=True,
        )

        return

    total_markets_analyzed = 0

    total_signals = 0


    # --------------------------------------------------------
    # EVENT LOOP
    # --------------------------------------------------------

    for event in events:

        markets = event.get(
            "markets",
            [],
        )

        event_ticker = event.get(
            "event_ticker",
            "",
        )


        # ----------------------------------------------------
        # MARKET LOOP
        # ----------------------------------------------------

        for market in markets:

            ticker = market.get(
                "ticker",
                "",
            ).upper()

            title = market.get(
                "title",
                "",
            )

            market_event_ticker = market.get(
                "event_ticker",
                event_ticker,
            )

            yes_ask = market.get(
                "yes_ask",
                0,
            )


            # ------------------------------------------------
            # CITY
            # ------------------------------------------------

            city_code = identify_city_from_ticker(
                ticker,
            )

            if not city_code:

                continue

            if not yes_ask:

                continue


            # ------------------------------------------------
            # FORECAST
            #
            # IMPORTANT:
            #
            # target_date is currently None because the Kalshi
            # contract date parser has not yet been built.
            #
            # This is the next important feature.
            # ------------------------------------------------

            forecast = get_forecast_for_date(
                city_code,
                target_date=None,
            )

            if not forecast:

                print(
                    f"No forecast available for "
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


            # ------------------------------------------------
            # FORECAST CHANGE
            # ------------------------------------------------

            change_data = check_forecast_change(
                city_code=city_code,
                target_date=forecast_date,
                forecast_temp=forecast_temp,
            )


            total_markets_analyzed += 1


            # ------------------------------------------------
            # MARKET PROBABILITY
            # ------------------------------------------------

            implied_prob = (
                float(yes_ask)
                / 100.0
            )


            # ------------------------------------------------
            # TEMPORARY MODEL
            # ------------------------------------------------

            model_prob = (
                temporary_model_probability(
                    forecast_temp
                )
            )


            expected_value_edge = (
                model_prob
                - implied_prob
            )


            # ------------------------------------------------
            # SIGNAL THRESHOLD
            # ------------------------------------------------

            MIN_EDGE = 0.015

            if (
                expected_value_edge
                > MIN_EDGE
            ):

                total_signals += 1

                max_viable_cents = int(
                    model_prob
                    * 100
                )


                # --------------------------------------------
                # FORECAST TEXT
                # --------------------------------------------

                if (
                    change_data["old_forecast"]
                    is not None
                ):

                    change_text = (
                        f"{change_data['old_forecast']:.1f}°F "
                        f"→ "
                        f"{forecast_temp:.1f}°F "
                        f"({change_data['delta']:+.1f}°F)"
                    )

                else:

                    change_text = (
                        f"Initial forecast: "
                        f"{forecast_temp:.1f}°F"
                    )


                # --------------------------------------------
                # KALSHI LINK
                # --------------------------------------------

                if market_event_ticker:

                    kalshi_link = (
                        "https://kalshi.com/markets/"
                        f"{market_event_ticker.lower()}"
                    )

                else:

                    kalshi_link = (
                        "https://kalshi.com"
                    )


                # --------------------------------------------
                # ALERT
                # --------------------------------------------

                alert_text = (

                    "🧪 **WEATHER PAPER TRADE SIGNAL**\n\n"

                    f"📍 **{city_name}**\n"
                    f"📅 Forecast Date: "
                    f"**{forecast_date}**\n\n"

                    f"🌡️ **Forecast ({forecast_source})**\n"
                    f"{change_text}\n\n"

                    f"🎯 **Contract**\n"
                    f"`{ticker}`\n"
                    f"{title}\n\n"

                    f"💰 **Market**\n"
                    f"YES Ask: "
                    f"**{yes_ask}¢**\n\n"

                    f"📊 **Temporary Research Model**\n"
                    f"Model Probability: "
                    f"**{model_prob * 100:.1f}%**\n"
                    f"Estimated Edge: "
                    f"**{expected_value_edge * 100:+.1f}%**\n\n"

                    f"⚠️ *Probability model is currently "
                    f"a placeholder and not yet calibrated.*\n\n"

                    f"🔗 {kalshi_link}"
                )


                print(
                    alert_text,
                    flush=True,
                )


                send_discord_alert(
                    alert_text,
                )


            else:

                print(
                    f"Checked {ticker} | "
                    f"{city_name} | "
                    f"{forecast_temp:.1f}°F | "
                    f"Ask: {yes_ask}¢ | "
                    f"Temp Edge: "
                    f"{expected_value_edge * 100:+.1f}% "
                    f"(No signal)",
                    flush=True,
                )


    # --------------------------------------------------------
    # SCAN COMPLETE
    # --------------------------------------------------------

    print(
        f"\nScan complete.",
        flush=True,
    )

    print(
        f"Markets analyzed: "
        f"{total_markets_analyzed}",
        flush=True,
    )

    print(
        f"Signals generated: "
        f"{total_signals}",
        flush=True,
    )

    print(
        "================================================\n",
        flush=True,
    )


# ============================================================
# BACKGROUND SCANNER
# ============================================================

def background_scanner():

    print(
        "Background scanner started.",
        flush=True,
    )

    while True:

        try:

            run_arbitrage_scan()

        except Exception as e:

            print(
                f"ERROR in background scan loop: "
                f"{e}",
                flush=True,
            )

        print(
            f"Waiting "
            f"{SCAN_INTERVAL_SECONDS} seconds "
            f"until next scan...",
            flush=True,
        )

        time.sleep(
            SCAN_INTERVAL_SECONDS
        )


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def home():

    return (
        "Weather Market Research Bot is active "
        "and scanning."
    )


@app.route("/health")
def health():

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


@app.route("/test-alert")
def test_alert():

    simulated_message = (

        "🧪 **WEATHER BOT TEST ALERT**\n\n"

        "This is a test message from your "
        "Render weather bot.\n\n"

        "If you received this message, the "
        "Discord environment variable is working."
    )


    success = send_discord_alert(
        simulated_message,
    )

    if success:

        return (
            "Test alert dispatched successfully."
        )

    return (
        "Test alert failed. "
        "Check Render logs and the "
        "DISCORD_WEBHOOK_URL environment variable."
    ), 500


@app.route("/run-scan")
def manual_scan():
    """
    Manually trigger a scan.

    Useful for testing.

    Do not expose this publicly long-term without
    authentication.
    """

    try:

        run_arbitrage_scan()

        return (
            "Manual scan completed. "
            "Check Render logs."
        )

    except Exception as e:

        return (
            f"Manual scan error: {e}"
        ), 500


# ============================================================
# START APPLICATION
# ============================================================

if __name__ == "__main__":

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
        f"Starting Flask server on port {port}",
        flush=True,
    )


    app.run(
        host="0.0.0.0",
        port=port,
    )
```
