import os
import time
import json
import threading
import statistics
import re

from datetime import datetime, timezone, date

import requests
from flask import Flask, jsonify


# ============================================================
# CONFIGURATION
# ============================================================

app = Flask(__name__)

# Kalshi public API
KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"

# IMPORTANT:
# Put this in Render Environment Variables.
# DO NOT put your real webhook directly in this file.
DISCORD_WEBHOOK_URL = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    ""
).strip()

# Scan interval in seconds.
SCAN_INTERVAL_SECONDS = int(
    os.environ.get("SCAN_INTERVAL_SECONDS", "300")
)

# Minimum temperature movement required before we call it
# a meaningful forecast change.
MIN_TEMP_CHANGE_F = float(
    os.environ.get("MIN_TEMP_CHANGE_F", "1.0")
)

# Minimum precipitation movement required.
MIN_PRECIP_CHANGE_IN = float(
    os.environ.get("MIN_PRECIP_CHANGE_IN", "0.05")
)

# Minimum raw ensemble probability edge over the market ask.
MIN_EDGE_POINTS = float(
    os.environ.get("MIN_EDGE_POINTS", "8.0")
)

# Maximum number of days ahead to analyze.
MAX_FORECAST_DAYS_AHEAD = int(
    os.environ.get("MAX_FORECAST_DAYS_AHEAD", "3")
)

# State persistence file.
STATE_FILE = "weather_bot_state.json"

# This bot does NOT place real trades.
PAPER_TRADING_MODE = True


# ============================================================
# CITY / KALSHI SERIES CONFIGURATION
# ============================================================

CITIES = {
    "KXHIGHNY": {
        "city": "New York",
        "lat": 40.7128,
        "lon": -74.0060,
        "timezone": "America/New_York",
    },
    "KXHIGHCHI": {
        "city": "Chicago",
        "lat": 41.8781,
        "lon": -87.6298,
        "timezone": "America/Chicago",
    },
    "KXHIGHMIA": {
        "city": "Miami",
        "lat": 25.7617,
        "lon": -80.1918,
        "timezone": "America/New_York",
    },
    "KXHIGHAUS": {
        "city": "Austin",
        "lat": 30.2672,
        "lon": -97.7431,
        "timezone": "America/Chicago",
    },
}


# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.Lock()

bot_state = {
    "forecasts": {},
    "contract_probabilities": {},
    "last_scan": None,
    "scan_count": 0,
    "last_summary": {},
}


# ============================================================
# TIME / UTILITY FUNCTIONS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def safe_float(value, default=None):
    if value is None:
        return default

    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def load_state():
    global bot_state

    if not os.path.exists(STATE_FILE):
        print(
            "No previous state file found. Starting fresh.",
            flush=True
        )
        return

    try:
        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as file:
            loaded = json.load(file)

        if isinstance(loaded, dict):
            with state_lock:
                bot_state.update(loaded)

        print(
            "Previous state loaded successfully.",
            flush=True
        )

    except Exception as error:
        print(
            f"Could not load previous state: {error}",
            flush=True
        )


def save_state():
    try:
        with state_lock:
            snapshot = json.loads(
                json.dumps(bot_state)
            )

        with open(
            STATE_FILE,
            "w",
            encoding="utf-8"
        ) as file:
            json.dump(
                snapshot,
                file,
                indent=2
            )

    except Exception as error:
        print(
            f"Could not save state: {error}",
            flush=True
        )


# ============================================================
# DISCORD
# ============================================================

def send_discord_alert(message):
    if not DISCORD_WEBHOOK_URL:
        print(
            "Discord webhook not configured. "
            "Set DISCORD_WEBHOOK_URL in Render "
            "Environment Variables.",
            flush=True
        )
        return False

    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": message},
            timeout=15,
        )

        print(
            f"Discord response status: "
            f"{response.status_code}",
            flush=True
        )

        if response.status_code in (200, 204):
            return True

        print(
            f"Discord error body: {response.text}",
            flush=True
        )

        return False

    except Exception as error:
        print(
            f"Discord webhook error: {error}",
            flush=True
        )

        return False


# ============================================================
# WEATHER: POINT FORECAST
# ============================================================

def get_point_forecast(city_data):
    """
    Gets daily deterministic point forecasts.

    Used for:
    - forecast change detection
    - precipitation monitoring
    - diagnostic information
    """

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": city_data["lat"],
        "longitude": city_data["lon"],
        "daily": (
            "temperature_2m_max,"
            "precipitation_sum"
        ),
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "timezone": city_data["timezone"],
        "forecast_days": 7,
    }

    try:
        response = requests.get(
            url,
            params=params,
            timeout=20,
        )

        print(
            f"Point forecast status: "
            f"{response.status_code}",
            flush=True
        )

        if response.status_code != 200:
            print(
                f"Point forecast error: "
                f"{response.text}",
                flush=True
            )
            return {}

        data = response.json()

        daily = data.get("daily", {})

        dates = daily.get("time", [])
        highs = daily.get(
            "temperature_2m_max",
            []
        )
        precipitation = daily.get(
            "precipitation_sum",
            []
        )

        forecasts = {}

        for index, forecast_date in enumerate(dates):

            high = None
            precip = None

            if index < len(highs):
                high = safe_float(
                    highs[index]
                )

            if index < len(precipitation):
                precip = safe_float(
                    precipitation[index]
                )

            forecasts[forecast_date] = {
                "high": high,
                "precip": precip,
            }

        return forecasts

    except Exception as error:
        print(
            f"Point forecast error: {error}",
            flush=True
        )

        return {}


# ============================================================
# WEATHER: ENSEMBLE FORECAST
# ============================================================

def get_ensemble_forecast(city_data):
    """
    Gets ensemble temperature forecasts.

    IMPORTANT:

    The probabilities calculated from this data are raw
    ensemble frequencies.

    They are NOT calibrated probabilities and should not be
    interpreted as a guaranteed fair value estimate.
    """

    url = (
        "https://ensemble-api.open-meteo.com/"
        "v1/ensemble"
    )

    params = {
        "latitude": city_data["lat"],
        "longitude": city_data["lon"],
        "hourly": "temperature_2m",
        "temperature_unit": "fahrenheit",
        "timezone": city_data["timezone"],
        "forecast_days": 7,
    }

    try:
        response = requests.get(
            url,
            params=params,
            timeout=30,
        )

        print(
            f"Ensemble API status: "
            f"{response.status_code}",
            flush=True
        )

        if response.status_code != 200:
            print(
                f"Ensemble API error: "
                f"{response.text}",
                flush=True
            )
            return {}

        data = response.json()

        hourly = data.get(
            "hourly",
            {}
        )

        times = hourly.get(
            "time",
            []
        )

        temperature_keys = []

        for key in hourly.keys():
            if key.startswith(
                "temperature_2m_member"
            ):
                temperature_keys.append(key)

        print(
            f"Ensemble member temperature keys found: "
            f"{len(temperature_keys)}",
            flush=True
        )

        if not temperature_keys:
            print(
                "No ensemble member temperature "
                "keys found.",
                flush=True
            )
            return {}

        # Structure:
        #
        # daily_members = {
        #     "2026-08-28": {
        #         "temperature_2m_member01": [...],
        #         ...
        #     }
        # }

        daily_members = {}

        for key in temperature_keys:

            values = hourly.get(
                key,
                []
            )

            for index, timestamp in enumerate(times):

                if index >= len(values):
                    continue

                value = safe_float(
                    values[index]
                )

                if value is None:
                    continue

                forecast_date = timestamp[:10]

                if forecast_date not in daily_members:
                    daily_members[
                        forecast_date
                    ] = {}

                if key not in daily_members[
                    forecast_date
                ]:
                    daily_members[
                        forecast_date
                    ][key] = []

                daily_members[
                    forecast_date
                ][key].append(value)

        results = {}

        for forecast_date, members in (
            daily_members.items()
        ):

            member_highs = []

            for member_name, temperatures in (
                members.items()
            ):

                if not temperatures:
                    continue

                member_high = max(
                    temperatures
                )

                member_highs.append(
                    member_high
                )

            if member_highs:

                if len(member_highs) > 1:
                    standard_deviation = (
                        statistics.stdev(
                            member_highs
                        )
                    )
                else:
                    standard_deviation = 0.0

                results[forecast_date] = {
                    "member_highs": member_highs,
                    "member_count": len(
                        member_highs
                    ),
                    "mean": statistics.mean(
                        member_highs
                    ),
                    "minimum": min(
                        member_highs
                    ),
                    "maximum": max(
                        member_highs
                    ),
                    "stdev": standard_deviation,
                }

        print(
            f"Ensemble dates available: "
            f"{len(results)}",
            flush=True
        )

        return results

    except Exception as error:
        print(
            f"Ensemble forecast error: "
            f"{error}",
            flush=True
        )

        return {}


# ============================================================
# KALSHI DATA
# ============================================================

def fetch_kalshi_series(series_ticker):
    """
    Fetches open markets for one Kalshi series.
    """

    url = (
        f"{KALSHI_API_URL}/markets"
    )

    params = {
        "series_ticker": series_ticker,
        "status": "open",
        "limit": 1000,
    }

    headers = {
        "User-Agent":
            "WeatherForecastResearchBot/1.0"
    }

    try:
        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=25,
        )

        print(
            f"Kalshi {series_ticker} status: "
            f"{response.status_code}",
            flush=True
        )

        if response.status_code != 200:
            print(
                f"Kalshi error: "
                f"{response.text}",
                flush=True
            )
            return []

        data = response.json()

        markets = data.get(
            "markets",
            []
        )

        print(
            f"Markets found: {len(markets)}",
            flush=True
        )

        return markets

    except Exception as error:
        print(
            f"Kalshi request error: "
            f"{error}",
            flush=True
        )

        return []


# ============================================================
# MARKET DATE PARSING
# ============================================================

def parse_market_date_from_ticker(ticker):
    """
    Example:

    KXHIGHCHI-26AUG28-T87

    Date portion:

    26AUG28

    Returns:

    2026-08-28
    """

    try:
        parts = ticker.split("-")

        if len(parts) < 2:
            return None

        date_part = parts[1]

        if len(date_part) != 7:
            return None

        year_text = date_part[0:2]
        month_text = date_part[2:5]
        day_text = date_part[5:7]

        year = (
            2000 + int(year_text)
        )

        months = {
            "JAN": 1,
            "FEB": 2,
            "MAR": 3,
            "APR": 4,
            "MAY": 5,
            "JUN": 6,
            "JUL": 7,
            "AUG": 8,
            "SEP": 9,
            "OCT": 10,
            "NOV": 11,
            "DEC": 12,
        }

        month = months.get(
            month_text.upper()
        )

        if month is None:
            return None

        parsed = date(
            year,
            month,
            int(day_text)
        )

        return parsed.isoformat()

    except Exception:
        return None


def date_is_in_range(date_string):
    """
    Ignore stale markets and markets too far ahead.
    """

    try:
        market_date = datetime.strptime(
            date_string,
            "%Y-%m-%d"
        ).date()

        today = datetime.now().date()

        days_ahead = (
            market_date - today
        ).days

        return (
            days_ahead >= 0
            and days_ahead
            <= MAX_FORECAST_DAYS_AHEAD
        )

    except Exception:
        return False


# ============================================================
# MARKET PRICE PARSING
# ============================================================

def get_yes_ask_cents(market):
    """
    Returns YES ask in cents.

    Kalshi API representations can vary.

    Examples:

    yes_ask = 42
    -> 42 cents

    yes_ask_dollars = 0.42
    -> 42 cents
    """

    yes_ask = safe_float(
        market
