import os
import re
import time
import threading
from datetime import datetime, timezone
from urllib.parse import quote

import requests
from flask import Flask, jsonify


# ============================================================
# WEATHER MARKET BOT
#
# RESEARCH / PAPER TRADING VERSION
#
# This bot:
#   1. Queries known Kalshi weather series directly.
#   2. Fetches active Kalshi markets.
#   3. Pulls weather forecasts.
#   4. Detects forecast changes.
#   5. Sends Discord research alerts.
#
# It DOES NOT automatically place trades.
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

KALSHI_API_URL = "https://external-api.kalshi.com/trade-api/v2"

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

SCAN_INTERVAL_SECONDS = 300

REQUEST_TIMEOUT = 20

# Minimum forecast change before generating a forecast-change alert.
TEMPERATURE_ALERT_CHANGE_F = 1.0

# Minimum precipitation change before alerting.
PRECIP_ALERT_CHANGE_IN = 0.05

# Prevent the same market alert from firing repeatedly
ALERT_COOLDOWN_SECONDS = 1800


# ============================================================
# KALSHI WEATHER SERIES
#
# The first confirmed example from Kalshi documentation is:
# KXHIGHNY
#
# The others are configured as likely series names but the bot
# will simply skip any that return no markets.
#
# If Kalshi uses a different ticker, we can update it later.
# ============================================================

WEATHER_SERIES = {
    "KXHIGHNY": {
        "city": "New York",
        "city_code": "NYC",
        "type": "high_temperature",
        "lat": 40.7128,
        "lon": -74.0060,
    },

    "KXHIGHCHI": {
        "city": "Chicago",
        "city_code": "CHI",
        "type": "high_temperature",
        "lat": 41.8781,
        "lon": -87.6298,
    },

    "KXHIGHMIA": {
        "city": "Miami",
        "city_code": "MIA",
        "type": "high_temperature",
        "lat": 25.7617,
        "lon": -80.1918,
    },

    "KXHIGHAUS": {
        "city": "Austin",
        "city_code": "AUS",
        "type": "high_temperature",
        "lat": 30.2672,
        "lon": -97.7431,
    },
}


# ============================================================
# MEMORY
#
# This is temporary in-memory storage.
# It resets when Render restarts.
#
# Later we should replace this with Redis/Postgres/Supabase.
# ============================================================

forecast_history = {}

alert_history = {}

last_scan_results = {
    "timestamp": None,
    "series_checked": 0,
    "markets_checked": 0,
    "forecast_changes": 0,
    "errors": [],
}


# ============================================================
# HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def utc_timestamp():
    return utc_now().isoformat()


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def dollars_to_cents(value):
    number = safe_float(value)

    if number is None:
        return None

    return round(number * 100, 2)


# ============================================================
# DISCORD
# ============================================================

def send_discord_alert(message):

    if not DISCORD_WEBHOOK_URL:
        print(
            "DISCORD_WEBHOOK_URL is not configured.",
            flush=True,
        )
        return False

    try:

        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={
                "content": message
            },
            timeout=REQUEST_TIMEOUT,
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

    except Exception as error:

        print(
            f"Discord error: {error}",
            flush=True,
        )

        return False


# ============================================================
# KALSHI API
# ============================================================

def fetch_kalshi_markets_for_series(series_ticker):

    all_markets = []

    cursor = None

    while True:

        params = {
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 1000,
        }

        if cursor:
            params["cursor"] = cursor

        try:

            response = requests.get(
                f"{KALSHI_API_URL}/markets",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            print(
                f"Kalshi {series_ticker} "
                f"status: {response.status_code}",
                flush=True,
            )

            if response.status_code != 200:

                print(
                    f"Kalshi response: "
                    f"{response.text[:500]}",
                    flush=True,
                )

                return all_markets

            data = response.json()

            markets = data.get(
                "markets",
                [],
            )

            all_markets.extend(
                markets
            )

            cursor = data.get(
                "cursor"
            )

            if not cursor:
                break

            # Safety limit
            if len(all_markets) >= 5000:
                break

        except Exception as error:

            print(
                f"Kalshi market request error "
                f"for {series_ticker}: {error}",
                flush=True,
            )

            return all_markets

    return all_markets


# ============================================================
# WEATHER DATA
#
# PRIMARY:
# Open-Meteo GFS/HRRR endpoint.
#
# HRRR is a short-horizon model, so longer forecasts may not
# be available. The code falls back to the standard forecast
# endpoint when necessary.
# ============================================================

def fetch_weather_forecast(lat, lon):

    result = None

    try:

        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": (
                "temperature_2m_max,"
                "precipitation_sum"
            ),
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "timezone": "auto",
            "forecast_days": 7,
        }

        response = requests.get(
            "https://api.open-meteo.com/v1/gfs",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        print(
            f"HRRR/GFS API status: "
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

            highs = daily.get(
                "temperature_2m_max",
                [],
            )

            precipitation = daily.get(
                "precipitation_sum",
                [],
            )

            if dates and highs:

                result = {
                    "source": "Open-Meteo GFS/HRRR",
                    "dates": dates,
                    "highs": highs,
                    "precipitation": precipitation,
                }

    except Exception as error:

        print(
            f"Primary weather error: {error}",
            flush=True,
        )


    # --------------------------------------------------------
    # FALLBACK
    # --------------------------------------------------------

    if result is None:

        try:

            params = {
                "latitude": lat,
                "longitude": lon,
                "daily": (
                    "temperature_2m_max,"
                    "precipitation_sum"
                ),
                "temperature_unit": "fahrenheit",
                "precipitation_unit": "inch",
                "timezone": "auto",
                "forecast_days": 7,
            }

            response = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            print(
                f"Fallback weather status: "
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

                highs = daily.get(
                    "temperature_2m_max",
                    [],
                )

                precipitation = daily.get(
                    "precipitation_sum",
                    [],
                )

                if dates and highs:

                    result = {
                        "source": "Open-Meteo Forecast",
                        "dates": dates,
                        "highs": highs,
                        "precipitation": precipitation,
                    }

        except Exception as error:

            print(
                f"Fallback weather error: {error}",
                flush=True,
            )

    return result


# ============================================================
# FORECAST LOOKUP
# ============================================================

def build_forecast_by_date(weather_data):

    if not weather_data:
        return {}

    dates = weather_data.get(
        "dates",
        [],
    )

    highs = weather_data.get(
        "highs",
        [],
    )

    precipitation = weather_data.get(
        "precipitation",
        [],
    )

    forecast_by_date = {}

    for index, date_value in enumerate(dates):

        high = None
        precip = None

        if index < len(highs):
            high = highs[index]

        if index < len(precipitation):
            precip = precipitation[index]

        forecast_by_date[date_value] = {
            "high_temperature": high,
            "precipitation": precip,
            "source": weather_data.get(
                "source",
                "Unknown",
            ),
        }

    return forecast_by_date


# ============================================================
# DATE PARSING
#
# Kalshi markets can expose occurrence_datetime or strike_date.
# We try several fields and normalize to YYYY-MM-DD.
# ============================================================

def normalize_date(value):

    if not value:
        return None

    value = str(value)

    # ISO datetime
    if len(value) >= 10:

        possible_date = value[:10]

        if re.match(
            r"^\d{4}-\d{2}-\d{2}$",
            possible_date,
        ):
            return possible_date

    return None


def get_market_date(market):

    date_fields = [
        "occurrence_datetime",
        "strike_date",
        "latest_expiration_time",
        "close_time",
    ]

    for field in date_fields:

        normalized = normalize_date(
            market.get(field)
        )

        if normalized:
            return normalized

    return None


# ============================================================
# STRIKE PARSING
#
# Prefer structured fields supplied by Kalshi:
#
# floor_strike
# cap_strike
# strike_type
#
# Then fall back to functional_strike.
# ============================================================

def get_market_strike_info(market):

    strike_type = market.get(
        "strike_type"
    )

    floor_strike = safe_float(
        market.get(
            "floor_strike"
        )
    )

    cap_strike = safe_float(
        market.get(
            "cap_strike"
        )
    )

    functional_strike = market.get(
        "functional_strike"
    )

    custom_strike = market.get(
        "custom_strike"
    )

    return {
        "strike_type": strike_type,
        "floor": floor_strike,
        "cap": cap_strike,
        "functional_strike": functional_strike,
        "custom_strike": custom_strike,
    }


# ============================================================
# MARKET DESCRIPTION
# ============================================================

def describe_market(market):

    ticker = market.get(
        "ticker",
        ""
    )

    title = market.get(
        "title",
        ""
    )

    yes_sub_title = market.get(
        "yes_sub_title",
        ""
    )

    strike = get_market_strike_info(
        market
    )

    description_parts = []

    if title:
        description_parts.append(
            title
        )

    if yes_sub_title:
        description_parts.append(
            yes_sub_title
        )

    if strike["strike_type"]:
        description_parts.append(
            f"strike_type={strike['strike_type']}"
        )

    if strike["floor"] is not None:
        description_parts.append(
            f"floor={strike['floor']}"
        )

    if strike["cap"] is not None:
        description_parts.append(
            f"cap={strike['cap']}"
        )

    return " | ".join(
        description_parts
    )


# ============================================================
# FORECAST CHANGE TRACKING
# ============================================================

def detect_forecast_change(
    series_ticker,
    forecast_date,
    forecast_type,
    value,
):

    if value is None:
        return {
            "changed": False,
            "old": None,
            "new": None,
            "delta": None,
            "first_observation": False,
        }

    key = (
        f"{series_ticker}|"
        f"{forecast_date}|"
        f"{forecast_type}"
    )

    old_value = forecast_history.get(
        key
    )

    forecast_history[key] = value

    if old_value is None:

        return {
            "changed": False,
            "old": None,
            "new": value,
            "delta": 0,
            "first_observation": True,
        }

    delta = value - old_value

    return {
        "changed": old_value != value,
        "old": old_value,
        "new": value,
        "delta": delta,
        "first_observation": False,
    }


# ============================================================
# ALERT COOLDOWN
# ============================================================

def can_send_alert(alert_key):

    now = time.time()

    previous_time = alert_history.get(
        alert_key
    )

    if previous_time is None:

        alert_history[alert_key] = now

        return True

    elapsed = now - previous_time

    if elapsed >= ALERT_COOLDOWN_SECONDS:

        alert_history[alert_key] = now

        return True

    return False


# ============================================================
# MARKET PRICE HELPERS
# ============================================================

def get_yes_ask_cents(market):

    yes_ask_dollars = market.get(
        "yes_ask_dollars"
    )

    if yes_ask_dollars is not None:

        return dollars_to_cents(
            yes_ask_dollars
        )

    # Compatibility fallback
    yes_ask = market.get(
        "yes_ask"
    )

    if yes_ask is not None:

        value = safe_float(
            yes_ask
        )

        if value is not None:

            if value <= 1:
                return value * 100

            return value

    return None


# ============================================================
# SIMPLE MARKET RELEVANCE
#
# This is NOT a probability model.
#
# It checks whether the forecast is near the market's strike.
# That is useful for prioritizing alerts because a forecast
# change near a strike can matter more than one far away.
# ============================================================

def calculate_forecast_strike_distance(
    forecast_value,
    strike_info,
):

    if forecast_value is None:
        return None

    floor_value = strike_info.get(
        "floor"
    )

    cap_value = strike_info.get(
        "cap"
    )

    distances = []

    if floor_value is not None:
        distances.append(
            abs(
                forecast_value
                - floor_value
            )
        )

    if cap_value is not None:
        distances.append(
            abs(
                forecast_value
                - cap_value
            )
        )

    if not distances:
        return None

    return min(
        distances
    )


# ============================================================
# KALSHI MARKET URL
# ============================================================

def get_kalshi_market_url(market):

    event_ticker = market.get(
        "event_ticker",
        ""
    )

    if event_ticker:

        return (
            "https://kalshi.com/markets/"
            f"{quote(event_ticker.lower())}"
        )

    return "https://kalshi.com"


# ============================================================
# DISCORD MESSAGE
# ============================================================

def build_alert_message(
    series_config,
    market,
    forecast_date,
    forecast_type,
    forecast_value,
    change,
):

    city = series_config[
        "city"
    ]

    ticker = market.get(
        "ticker",
        "Unknown",
    )

    title = market.get(
        "title",
        ""
    )

    yes_ask_cents = get_yes_ask_cents(
        market
    )

    strike_info = get_market_strike_info(
        market
    )

    market_description = describe_market(
        market
    )

    market_url = get_kalshi_market_url(
        market
    )


    if forecast_type == "high_temperature":

        units = "°F"

        value_text = (
            f"{forecast_value:.1f}"
            f"{units}"
        )

        if change["old"] is not None:

            change_text = (
                f"{change['old']:.1f}"
                f"{units} → "
                f"{forecast_value:.1f}"
                f"{units} "
                f"({change['delta']:+.1f}"
                f"{units})"
            )

        else:

            change_text = (
                f"Current forecast: "
                f"{value_text}"
            )

    else:

        units = " inches"

        value_text = (
            f"{forecast_value:.2f}"
            f"{units}"
        )

        if change["old"] is not None:

            change_text = (
                f"{change['old']:.2f}"
                f"{units} → "
                f"{forecast_value:.2f}"
                f"{units} "
                f"({change['delta']:+.2f}"
                f"{units})"
            )

        else:

            change_text = (
                f"Current forecast: "
                f"{value_text}"
            )


    if yes_ask_cents is None:

        price_text = (
            "No YES ask available"
        )

    else:

        price_text = (
            f"{yes_ask_cents:.1f}¢"
        )


    strike_text = []

    if strike_info["strike_type"]:

        strike_text.append(
            f"Type: "
            f"{strike_info['strike_type']}"
        )

    if strike_info["floor"] is not None:

        strike_text.append(
            f"Floor: "
            f"{strike_info['floor']}"
        )

    if strike_info["cap"] is not None:

        strike_text.append(
            f"Cap: "
            f"{strike_info['cap']}"
        )

    if not strike_text:

        strike_text.append(
            "Strike details unavailable"
        )


    return (
        "🌦️ **WEATHER FORECAST CHANGE — PAPER SIGNAL**\n\n"

        f"📍 **City:** {city}\n"
        f"📅 **Forecast date:** {forecast_date}\n"
        f"📊 **Forecast type:** "
        f"{forecast_type.replace('_', ' ').title()}\n\n"

        f"🔄 **Forecast change:**\n"
        f"{change_text}\n\n"

        f"🎯 **Kalshi contract:** "
        f"`{ticker}`\n"

        f"{title}\n\n"

        f"💰 **YES Ask:** "
        f"{price_text}\n\n"

        f"📐 **Market details:**\n"
        f"{market_description}\n"
        f"{' | '.join(strike_text)}\n\n"

        f"🔗 **Kalshi:** "
        f"{market_url}\n\n"

        "⚠️ **Research signal only.** "
        "This alert detects forecast movement and "
        "market proximity; it is not yet a calibrated "
        "+EV probability model."
    )


# ============================================================
# MAIN SCAN
# ============================================================

def run_scan():

    global last_scan_results

    print(
        "\n"
        "==================================================",
        flush=True,
    )

    print(
        "STARTING WEATHER MARKET SCAN",
        flush=True,
    )

    print(
        f"UTC: {utc_timestamp()}",
        flush=True,
    )


    results = {
        "timestamp": utc_timestamp(),
        "series_checked": 0,
        "markets_checked": 0,
        "forecast_changes": 0,
        "errors": [],
    }


    for series_ticker, config in WEATHER_SERIES.items():

        results[
            "series_checked"
        ] += 1


        print(
            "\n--------------------------------------------------",
            flush=True,
        )

        print(
            f"SERIES: {series_ticker}",
            flush=True,
        )

        print(
            f"CITY: {config['city']}",
            flush=True,
        )


        # ----------------------------------------------------
        # FETCH WEATHER
        # ----------------------------------------------------

        weather_data = fetch_weather_forecast(
            config["lat"],
            config["lon"],
        )

        if not weather_data:

            error_message = (
                f"No weather data for "
                f"{config['city']}"
            )

            print(
                error_message,
                flush=True,
            )

            results[
                "errors"
            ].append(
                error_message
            )

            continue


        forecast_by_date = build_forecast_by_date(
            weather_data
        )


        print(
            f"Weather source: "
            f"{weather_data.get('source')}",
            flush=True,
        )


        # ----------------------------------------------------
        # FETCH KALSHI MARKETS
        # ----------------------------------------------------

        markets = fetch_kalshi_markets_for_series(
            series_ticker
        )


        print(
            f"Markets found: "
            f"{len(markets)}",
            flush=True,
        )


        if not markets:

            print(
                "No open markets in this series.",
                flush=True,
            )

            continue


        # ----------------------------------------------------
        # PROCESS MARKETS
        # ----------------------------------------------------

        for market in markets:

            results[
                "markets_checked"
            ] += 1


            market_date = get_market_date(
                market
            )


            if not market_date:

                print(
                    f"Skipping market without "
                    f"recognizable date: "
                    f"{market.get('ticker')}",
                    flush=True,
                )

                continue


            forecast = forecast_by_date.get(
                market_date
            )


            if not forecast:

                print(
                    f"No matching forecast date "
                    f"for market "
                    f"{market.get('ticker')}: "
                    f"{market_date}",
                    flush=True,
                )

                continue


            # ------------------------------------------------
            # HIGH TEMPERATURE
            # ------------------------------------------------

            if config["type"] == "high_temperature":

                forecast_value = forecast.get(
                    "high_temperature"
                )

                change = detect_forecast_change(
                    series_ticker,
                    market_date,
                    "high_temperature",
                    forecast_value,
                )


                print(
                    f"Market: "
                    f"{market.get('ticker')}",
                    flush=True,
                )

                print(
                    f"Date: "
                    f"{market_date}",
                    flush=True,
                )

                print(
                    f"Forecast high: "
                    f"{forecast_value}",
                    flush=True,
                )

                print(
                    f"Change: "
                    f"{change['delta']}",
                    flush=True,
                )


                if (
                    not change["first_observation"]
                    and change["changed"]
                    and abs(change["delta"])
                    >= TEMPERATURE_ALERT_CHANGE_F
                ):

                    results[
                        "forecast_changes"
                    ] += 1


                    alert_key = (
                        f"{series_ticker}|"
                        f"{market.get('ticker')}|"
                        f"{forecast_value}"
                    )


                    if can_send_alert(
                        alert_key
                    ):

                        message = build_alert_message(
                            config,
                            market,
                            market_date,
                            "high_temperature",
                            forecast_value,
                            change,
                        )

                        print(
                            message,
                            flush=True,
                        )

                        send_discord_alert(
                            message
                        )


    last_scan_results = results


    print(
        "\n==================================================",
        flush=True,
    )

    print(
        "SCAN COMPLETE",
        flush=True,
    )

    print(
        f"Series checked: "
        f"{results['series_checked']}",
        flush=True,
    )

    print(
        f"Markets checked: "
        f"{results['markets_checked']}",
        flush=True,
    )

    print(
        f"Forecast changes: "
        f"{results['forecast_changes']}",
        flush=True,
    )

    print(
        "==================================================\n",
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

            run_scan()

        except Exception as error:

            print(
                f"BACKGROUND SCAN ERROR: "
                f"{error}",
                flush=True,
            )

            last_scan_results[
                "errors"
            ].append(
                str(error)
            )


        print(
            f"Waiting "
            f"{SCAN_INTERVAL_SECONDS} seconds...",
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

    return jsonify(
        {
            "status": "running",
            "message": (
                "Weather Market Research Bot "
                "is running."
            ),
        }
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
            "weather_series": list(
                WEATHER_SERIES.keys()
            ),
            "utc_time": utc_timestamp(),
        }
    )


@app.route("/status")
def status():

    return jsonify(
        last_scan_results
    )


@app.route("/test-alert")
def test_alert():

    message = (
        "🧪 **WEATHER BOT TEST ALERT**\n\n"
        "Your Render weather bot successfully "
        "connected to Discord."
    )

    success = send_discord_alert(
        message
    )


    if success:

        return jsonify(
            {
                "success": True,
                "message": (
                    "Test alert sent."
                ),
            }
        )


    return jsonify(
        {
            "success": False,
            "message": (
                "Test alert failed. "
                "Check DISCORD_WEBHOOK_URL "
                "in Render."
            ),
        }
    ), 500


@app.route("/run-scan")
def manual_scan():

    try:

        run_scan()

        return jsonify(
            {
                "success": True,
                "results": (
                    last_scan_results
                ),
            }
        )

    except Exception as error:

        return jsonify(
            {
                "success": False,
                "error": str(error),
            }
        ), 500


# ============================================================
# APPLICATION START
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
        f"Starting server on port {port}",
        flush=True,
    )


    app.run(
        host="0.0.0.0",
        port=port,
    )
