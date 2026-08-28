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
        market.get("yes_ask")
    )

    if yes_ask is not None:

        if yes_ask <= 1:
            return yes_ask * 100

        return yes_ask

    yes_ask_dollars = safe_float(
        market.get("yes_ask_dollars")
    )

    if yes_ask_dollars is not None:
        return yes_ask_dollars * 100

    return None


# ============================================================
# CONTRACT INTERPRETATION
# ============================================================

def interpret_contract(market):
    """
    Converts Kalshi strike information into a standardized
    contract definition.

    Supported strike types:

    - less
    - greater
    - between
    """

    strike_type = (
        market.get("strike_type")
        or ""
    ).lower()

    floor = safe_float(
        market.get("floor_strike")
    )

    cap = safe_float(
        market.get("cap_strike")
    )

    title = (
        market.get("title")
        or ""
    )

    yes_subtitle = (
        market.get("yes_sub_title")
        or market.get("yes_subtitle")
        or ""
    )

    # --------------------------------------------------------
    # LESS
    # --------------------------------------------------------

    if strike_type == "less":

        label = title

        if cap is not None:

            # Example:
            #
            # Market title:
            # "Will the maximum temperature be <88°?"
            #
            # Kalshi may display YES subtitle:
            # "87° or below"
            #
            # The actual exact settlement convention should be
            # validated against Kalshi's rules.
            label = (
                f"{cap:g}°F or lower"
            )

        return {
            "valid": True,
            "type": "less",
            "floor": None,
            "cap": cap,
            "label": label,
            "title": title,
            "yes_subtitle": yes_subtitle,
        }

    # --------------------------------------------------------
    # GREATER
    # --------------------------------------------------------

    if strike_type == "greater":

        label = title

        if floor is not None:
            label = (
                f"{floor:g}°F or higher"
            )

        return {
            "valid": True,
            "type": "greater",
            "floor": floor,
            "cap": None,
            "label": label,
            "title": title,
            "yes_subtitle": yes_subtitle,
        }

    # --------------------------------------------------------
    # BETWEEN
    # --------------------------------------------------------

    if strike_type == "between":

        label = title

        if (
            floor is not None
            and cap is not None
        ):
            label = (
                f"{floor:g}°F to "
                f"{cap:g}°F"
            )

        return {
            "valid": True,
            "type": "between",
            "floor": floor,
            "cap": cap,
            "label": label,
            "title": title,
            "yes_subtitle": yes_subtitle,
        }

    # --------------------------------------------------------
    # FALLBACK TITLE PARSING
    # --------------------------------------------------------

    lower_title = title.lower()

    # Example:
    # "Will the maximum temperature be 94-95°..."

    between_match = re.search(
        r"(\d+(?:\.\d+)?)\s*-\s*"
        r"(\d+(?:\.\d+)?)",
        title
    )

    if between_match:

        parsed_floor = safe_float(
            between_match.group(1)
        )

        parsed_cap = safe_float(
            between_match.group(2)
        )

        return {
            "valid": True,
            "type": "between",
            "floor": parsed_floor,
            "cap": parsed_cap,
            "label": (
                f"{parsed_floor:g}°F to "
                f"{parsed_cap:g}°F"
            ),
            "title": title,
            "yes_subtitle": yes_subtitle,
        }

    if "<" in title:

        numbers = re.findall(
            r"\d+(?:\.\d+)?",
            title
        )

        if numbers:

            parsed_cap = safe_float(
                numbers[0]
            )

            return {
                "valid": True,
                "type": "less",
                "floor": None,
                "cap": parsed_cap,
                "label": (
                    f"Below {parsed_cap:g}°F"
                ),
                "title": title,
                "yes_subtitle": yes_subtitle,
            }

    if ">" in title:

        numbers = re.findall(
            r"\d+(?:\.\d+)?",
            title
        )

        if numbers:

            parsed_floor = safe_float(
                numbers[0]
            )

            return {
                "valid": True,
                "type": "greater",
                "floor": parsed_floor,
                "cap": None,
                "label": (
                    f"Above {parsed_floor:g}°F"
                ),
                "title": title,
                "yes_subtitle": yes_subtitle,
            }

    return {
        "valid": False,
        "type": None,
        "floor": floor,
        "cap": cap,
        "label": title,
        "title": title,
        "yes_subtitle": yes_subtitle,
    }


# ============================================================
# ENSEMBLE PROBABILITY
# ============================================================

def calculate_raw_ensemble_probability(
    member_highs,
    contract
):
    """
    Calculates the percentage of ensemble members that satisfy
    the interpreted contract.

    IMPORTANT:

    This is a RAW ENSEMBLE FREQUENCY.

    It is NOT a calibrated probability model.
    """

    if not member_highs:
        return None

    contract_type = contract.get("type")

    floor = contract.get("floor")
    cap = contract.get("cap")

    wins = 0

    for temperature in member_highs:

        temperature = safe_float(
            temperature
        )

        if temperature is None:
            continue

        # ----------------------------------------------------
        # LESS
        # ----------------------------------------------------

        if contract_type == "less":

            if cap is None:
                continue

            # Approximation based on integer temperature
            # market convention.
            #
            # If cap is 88, market title may mean <88.
            #
            # Therefore member high <88 qualifies.
            if temperature < cap:
                wins += 1

        # ----------------------------------------------------
        # GREATER
        # ----------------------------------------------------

        elif contract_type == "greater":

            if floor is None:
                continue

            # If title is >104, member high >104 qualifies.
            if temperature > floor:
                wins += 1

        # ----------------------------------------------------
        # BETWEEN
        # ----------------------------------------------------

        elif contract_type == "between":

            if (
                floor is None
                or cap is None
            ):
                continue

            # Inclusive interval approximation.
            if (
                temperature >= floor
                and temperature <= cap
            ):
                wins += 1

    probability = (
        wins / len(member_highs)
    ) * 100

    return probability


# ============================================================
# FORECAST STATE
# ============================================================

def get_forecast_key(
    series_ticker,
    forecast_date
):
    return (
        f"{series_ticker}|"
        f"{forecast_date}"
    )


def update_forecast_history(
    series_ticker,
    forecast_date,
    point_high,
    point_precip
):
    """
    Saves forecast observations and returns information about
    whether the forecast changed.
    """

    key = get_forecast_key(
        series_ticker,
        forecast_date
    )

    with state_lock:

        previous = bot_state[
            "forecasts"
        ].get(key)

        current = {
            "high": point_high,
            "precip": point_precip,
            "updated_at": utc_now(),
        }

        bot_state[
            "forecasts"
        ][key] = current

    if previous is None:

        return {
            "first_observation": True,
            "previous_high": None,
            "previous_precip": None,
            "temp_change": 0.0,
            "precip_change": 0.0,
            "meaningful_change": False,
        }

    previous_high = safe_float(
        previous.get("high")
    )

    previous_precip = safe_float(
        previous.get("precip")
    )

    temp_change = 0.0
    precip_change = 0.0

    if (
        previous_high is not None
        and point_high is not None
    ):
        temp_change = (
            point_high - previous_high
        )

    if (
        previous_precip is not None
        and point_precip is not None
    ):
        precip_change = (
            point_precip
            - previous_precip
        )

    meaningful_change = (
        abs(temp_change)
        >= MIN_TEMP_CHANGE_F
        or abs(precip_change)
        >= MIN_PRECIP_CHANGE_IN
    )

    return {
        "first_observation": False,
        "previous_high": previous_high,
        "previous_precip": previous_precip,
        "temp_change": temp_change,
        "precip_change": precip_change,
        "meaningful_change": meaningful_change,
    }


# ============================================================
# CONTRACT PROBABILITY HISTORY
# ============================================================

def update_contract_probability(
    ticker,
    probability
):
    """
    Tracks changes in raw ensemble probability.

    Returns previous probability and change.
    """

    if probability is None:
        return None, 0.0

    with state_lock:

        previous = bot_state[
            "contract_probabilities"
        ].get(ticker)

        bot_state[
            "contract_probabilities"
        ][ticker] = {
            "probability": probability,
            "updated_at": utc_now(),
        }

    if previous is None:
        return None, 0.0

    previous_probability = safe_float(
        previous.get("probability")
    )

    if previous_probability is None:
        return None, 0.0

    probability_change = (
        probability
        - previous_probability
    )

    return (
        previous_probability,
        probability_change
    )


# ============================================================
# KALSHI LINK
# ============================================================

def get_kalshi_link(series_ticker):
    return (
        "https://kalshi.com/markets/"
        f"{series_ticker.lower()}"
    )


# ============================================================
# MARKET ANALYSIS
# ============================================================

def analyze_city_series(
    series_ticker,
    city_data
):
    city_name = city_data["city"]

    print(
        "",
        flush=True
    )

    print(
        "--------------------------------------------------",
        flush=True
    )

    print(
        f"SERIES: {series_ticker}",
        flush=True
    )

    print(
        f"CITY: {city_name}",
        flush=True
    )

    point_forecasts = get_point_forecast(
        city_data
    )

    ensemble_forecasts = get_ensemble_forecast(
        city_data
    )

    markets = fetch_kalshi_series(
        series_ticker
    )

    results_by_date = {}

    for market in markets:

        ticker = (
            market.get("ticker")
            or ""
        )

        market_date = (
            parse_market_date_from_ticker(
                ticker
            )
        )

        if market_date is None:
            print(
                f"Could not parse market date: "
                f"{ticker}",
                flush=True
            )
            continue

        if not date_is_in_range(
            market_date
        ):
            print(
                f"Ignoring stale/out-of-range date: "
                f"{market_date} | {ticker}",
                flush=True
            )
            continue

        if market_date not in results_by_date:
            results_by_date[
                market_date
            ] = []

        results_by_date[
            market_date
        ].append(market)

    city_summary = {
        "city": city_name,
        "series": series_ticker,
        "markets_checked": 0,
        "forecast_changes": 0,
        "signals": 0,
        "alerts": 0,
    }

    for market_date, date_markets in (
        results_by_date.items()
    ):

        point_data = point_forecasts.get(
            market_date
        )

        ensemble_data = ensemble_forecasts.get(
            market_date
        )

        if point_data is None:

            print(
                f"No point forecast available for "
                f"{market_date}",
                flush=True
            )

            continue

        if ensemble_data is None:

            print(
                f"No ensemble forecast available for "
                f"{market_date}",
                flush=True
            )

            continue

        point_high = safe_float(
            point_data.get("high")
        )

        point_precip = safe_float(
            point_data.get("precip")
        )

        member_highs = ensemble_data.get(
            "member_highs",
            []
        )

        print(
            "",
            flush=True
        )

        print(
            "..................................................",
            flush=True
        )

        print(
            f"DATE: {market_date}",
            flush=True
        )

        print(
            f"POINT FORECAST HIGH: "
            f"{point_high:.2f}°F"
            if point_high is not None
            else "POINT FORECAST HIGH: unavailable",
            flush=True
        )

        print(
            f"POINT FORECAST PRECIPITATION: "
            f"{point_precip:.2f} inches"
            if point_precip is not None
            else "POINT FORECAST PRECIPITATION: unavailable",
            flush=True
        )

        print(
            f"ENSEMBLE MEMBERS: "
            f"{len(member_highs)}",
            flush=True
        )

        if member_highs:

            print(
                f"ENSEMBLE MINIMUM: "
                f"{min(member_highs):.2f}°F",
                flush=True
            )

            print(
                f"ENSEMBLE MAXIMUM: "
                f"{max(member_highs):.2f}°F",
                flush=True
            )

            print(
                f"ENSEMBLE MEAN: "
                f"{statistics.mean(member_highs):.2f}°F",
                flush=True
            )

        forecast_status = update_forecast_history(
            series_ticker,
            market_date,
            point_high,
            point_precip
        )

        if forecast_status[
            "first_observation"
        ]:

            print(
                "FORECAST STATUS: FIRST OBSERVATION",
                flush=True
            )

            print(
                "Baseline stored.",
                flush=True
            )

        else:

            print(
                f"PREVIOUS FORECAST HIGH: "
                f"{forecast_status['previous_high']}",
                flush=True
            )

            print(
                f"FORECAST CHANGE: "
                f"{forecast_status['temp_change']:+.2f}°F",
                flush=True
            )

            print(
                f"PRECIPITATION CHANGE: "
                f"{forecast_status['precip_change']:+.2f} in",
                flush=True
            )

            if forecast_status[
                "meaningful_change"
            ]:

                city_summary[
                    "forecast_changes"
                ] += 1

                print(
                    "MEANINGFUL FORECAST CHANGE DETECTED",
                    flush=True
                )

        opportunities = []

        for market in date_markets:

            ticker = (
                market.get("ticker")
                or ""
            )

            contract = interpret_contract(
                market
            )

            if not contract.get("valid"):

                print(
                    f"Skipping unrecognized contract: "
                    f"{ticker}",
                    flush=True
                )

                continue

            ask_cents = get_yes_ask_cents(
                market
            )

            if ask_cents is None:

                print(
                    f"No YES ask available: "
                    f"{ticker}",
                    flush=True
                )

                continue

            raw_probability = (
                calculate_raw_ensemble_probability(
                    member_highs,
                    contract
                )
            )

            if raw_probability is None:
                continue

            previous_probability, probability_change = (
                update_contract_probability(
                    ticker,
                    raw_probability
                )
            )

            edge_points = (
                raw_probability
                - ask_cents
            )

            opportunity = {
                "ticker": ticker,
                "contract": contract,
                "ask_cents": ask_cents,
                "raw_probability": raw_probability,
                "edge_points": edge_points,
                "probability_change": probability_change,
                "previous_probability":
                    previous_probability,
            }

            opportunities.append(
                opportunity
            )

            city_summary[
                "markets_checked"
            ] += 1

        opportunities.sort(
            key=lambda item:
                item["edge_points"],
            reverse=True
        )

        print(
            "",
            flush=True
        )

        print(
            f"TOP OPPORTUNITIES: "
            f"{city_name} | {market_date}",
            flush=True
        )

        top_opportunities = opportunities[:5]

        if not top_opportunities:

            print(
                "No analyzable markets.",
                flush=True
            )

        for index, opportunity in enumerate(
            top_opportunities,
            start=1
        ):

            print(
                f"{index}. "
                f"{opportunity['contract']['label']} "
                f"| Raw ensemble: "
                f"{opportunity['raw_probability']:.1f}% "
                f"| Ask: "
                f"{opportunity['ask_cents']:.1f}¢ "
                f"| Edge: "
                f"{opportunity['edge_points']:+.1f} points "
                f"| {opportunity['ticker']}",
                flush=True
            )

        # ----------------------------------------------------
        # ALERT LOGIC
        # ----------------------------------------------------

        if forecast_status[
            "first_observation"
        ]:

            print(
                "No Discord alert: first observation.",
                flush=True
            )

            continue

        if not forecast_status[
            "meaningful_change"
        ]:

            print(
                "No Discord alert: forecast movement "
                "below configured threshold.",
                flush=True
            )

            continue

        qualifying = []

        for opportunity in opportunities:

            if (
                opportunity["edge_points"]
                >= MIN_EDGE_POINTS
            ):

                qualifying.append(
                    opportunity
                )

        if not qualifying:

            print(
                "No Discord alert: no qualifying "
                "raw ensemble discrepancy.",
                flush=True
            )

            continue

        city_summary["signals"] += len(
            qualifying
        )

        best = qualifying[0]

        contract_label = best[
            "contract"
        ]["label"]

        kalshi_link = get_kalshi_link(
            series_ticker
        )

        old_high = forecast_status[
            "previous_high"
        ]

        new_high = point_high

        message_lines = [
            "🌦️ **WEATHER FORECAST CHANGE DETECTED**",
            "",
            f"**City:** {city_name}",
            f"**Date:** {market_date}",
            "",
            f"Forecast high changed: "
            f"{old_high:.1f}°F → "
            f"{new_high:.1f}°F",
            "",
            f"**Potential paper opportunity:** "
            f"{contract_label}",
            f"Raw ensemble frequency: "
            f"{best['raw_probability']:.1f}%",
            f"Kalshi YES ask: "
            f"{best['ask_cents']:.1f}¢",
            f"Raw discrepancy: "
            f"{best['edge_points']:+.1f} points",
            f"Contract: `{best['ticker']}`",
            "",
            "⚠️ Raw ensemble frequency is not a "
            "calibrated probability.",
            f"Kalshi: {kalshi_link}",
        ]

        message = "\n".join(
            message_lines
        )

        print(
            "",
            flush=True
        )

        print(
            "PAPER SIGNAL:",
            flush=True
        )

        print(
            message,
            flush=True
        )

        if send_discord_alert(
            message
        ):

            city_summary[
                "alerts"
            ] += 1

    return city_summary


# ============================================================
# MAIN SCAN
# ============================================================

def run_weather_scan():

    print(
        "",
        flush=True
    )

    print(
        "==================================================",
        flush=True
    )

    print(
        "STARTING WEATHER MARKET SCAN",
        flush=True
    )

    print(
        f"UTC: {utc_now()}",
        flush=True
    )

    print(
        "==================================================",
        flush=True
    )

    summaries = []

    total_series = 0
    total_markets = 0
    total_changes = 0
    total_signals = 0
    total_alerts = 0

    for series_ticker, city_data in (
        CITIES.items()
    ):

        try:

            summary = analyze_city_series(
                series_ticker,
                city_data
            )

            summaries.append(
                summary
            )

            total_series += 1

            total_markets += summary[
                "markets_checked"
            ]

            total_changes += summary[
                "forecast_changes"
            ]

            total_signals += summary[
                "signals"
            ]

            total_alerts += summary[
                "alerts"
            ]

        except Exception as error:

            print(
                f"ERROR ANALYZING "
                f"{series_ticker}: {error}",
                flush=True
            )

    summary = {
        "timestamp": utc_now(),
        "series_checked": total_series,
        "markets_checked": total_markets,
        "forecast_changes": total_changes,
        "paper_signals": total_signals,
        "discord_alerts": total_alerts,
        "cities": summaries,
    }

    with state_lock:

        bot_state[
            "last_scan"
        ] = utc_now()

        bot_state[
            "scan_count"
        ] = (
            bot_state.get(
                "scan_count",
                0
            )
            + 1
        )

        bot_state[
            "last_summary"
        ] = summary

    save_state()

    print(
        "",
        flush=True
    )

    print(
        "==================================================",
        flush=True
    )

    print(
        "SCAN COMPLETE",
        flush=True
    )

    print(
        f"Series checked: "
        f"{total_series}",
        flush=True
    )

    print(
        f"Markets checked: "
        f"{total_markets}",
        flush=True
    )

    print(
        f"Forecast changes: "
        f"{total_changes}",
        flush=True
    )

    print(
        f"Positive preliminary signals: "
        f"{total_signals}",
        flush=True
    )

    print(
        f"Discord alerts sent: "
        f"{total_alerts}",
        flush=True
    )

    print(
        "==================================================",
        flush=True
    )

    return summary


# ============================================================
# BACKGROUND SCANNER
# ============================================================

def background_scanner():

    print(
        "Background scanner started.",
        flush=True
    )

    while True:

        try:

            run_weather_scan()

        except Exception as error:

            print(
                f"Background scan error: "
                f"{error}",
                flush=True
            )

        print(
            f"Waiting "
            f"{SCAN_INTERVAL_SECONDS} seconds...",
            flush=True
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
        "Weather Forecast Market Monitor is active. "
        "Paper trading mode enabled."
    )


@app.route("/status")
def status():

    with state_lock:

        return jsonify({
            "paper_trading_mode":
                PAPER_TRADING_MODE,
            "scan_interval_seconds":
                SCAN_INTERVAL_SECONDS,
            "last_scan":
                bot_state.get(
                    "last_scan"
                ),
            "scan_count":
                bot_state.get(
                    "scan_count"
                ),
            "last_summary":
                bot_state.get(
                    "last_summary"
                ),
            "cities": [
                city["city"]
                for city in CITIES.values()
            ],
            "discord_configured":
                bool(
                    DISCORD_WEBHOOK_URL
                ),
        })


@app.route("/test-alert")
def test_alert():

    message = (
        "🧪 **WEATHER BOT TEST ALERT**\n\n"
        "If you received this message, the "
        "Discord webhook is configured correctly.\n\n"
        "This is only a test. No trade was placed."
    )

    success = send_discord_alert(
        message
    )

    if success:
        return (
            "Test alert sent successfully."
        )

    return (
        "Test alert failed. Check Render logs and "
        "DISCORD_WEBHOOK_URL."
    ), 500


@app.route("/scan-now")
def scan_now():

    try:

        summary = run_weather_scan()

        return jsonify(summary)

    except Exception as error:

        return jsonify({
            "error": str(error)
        }), 500


# ============================================================
# STARTUP
# ============================================================

if __name__ == "__main__":

    load_state()

    scanner_thread = threading.Thread(
        target=background_scanner,
        daemon=True
    )

    scanner_thread.start()

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    print(
        f"Starting server on port {port}",
        flush=True
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )
