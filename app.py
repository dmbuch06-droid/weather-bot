import os
import re
import time
import math
import threading
from datetime import datetime, timezone, date

import requests
from flask import Flask, jsonify


# ============================================================
# WEATHER + KALSHI RESEARCH BOT
#
# PAPER TRADING / RESEARCH ONLY
#
# This bot:
#
# 1. Pulls Kalshi weather markets.
# 2. Parses the weather date from market tickers.
# 3. Pulls point forecasts.
# 4. Pulls ensemble forecasts.
# 5. Calculates preliminary probabilities from ensemble members.
# 6. Compares those probabilities to Kalshi prices.
# 7. Tracks forecast changes.
# 8. Sends Discord alerts when:
#
#       Forecast changed significantly
#       AND
#       A positive preliminary edge exists
#
# No automatic trading is performed.
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

KALSHI_API_URL = (
    "https://external-api.kalshi.com/trade-api/v2"
)

DISCORD_WEBHOOK_URL = os.environ.get(
    "DISCORD_WEBHOOK_URL"
)

REQUEST_TIMEOUT = 20

# How often to scan.
SCAN_INTERVAL_SECONDS = 300

# Minimum forecast movement before an alert can trigger.
TEMPERATURE_ALERT_CHANGE_F = 1.0

# Minimum estimated edge required for a signal.
MIN_EDGE_PERCENTAGE_POINTS = 5.0

# Number of top signals shown in Discord.
MAX_ALERTS_PER_CITY_DATE = 3

# Prevent repeating the same alert too frequently.
ALERT_COOLDOWN_SECONDS = 1800

# How many days ahead to consider.
MAX_FORECAST_DAYS_AHEAD = 7


# ============================================================
# WEATHER SERIES
#
# Add more cities here later.
# ============================================================

WEATHER_SERIES = {

    "KXHIGHNY": {
        "city": "New York",
        "city_code": "NYC",
        "lat": 40.7128,
        "lon": -74.0060,
    },

    "KXHIGHCHI": {
        "city": "Chicago",
        "city_code": "CHI",
        "lat": 41.8781,
        "lon": -87.6298,
    },

    "KXHIGHMIA": {
        "city": "Miami",
        "city_code": "MIA",
        "lat": 25.7617,
        "lon": -80.1918,
    },

    "KXHIGHAUS": {
        "city": "Austin",
        "city_code": "AUS",
        "lat": 30.2672,
        "lon": -97.7431,
    },

}


# ============================================================
# MEMORY
#
# NOTE:
# Render restarts clear this memory.
#
# Later we can replace this with persistent storage.
# ============================================================

forecast_history = {}

alert_history = {}

last_scan_results = {
    "timestamp": None,
    "series_checked": 0,
    "markets_checked": 0,
    "forecast_changes": 0,
    "signals_found": 0,
    "alerts_sent": 0,
    "errors": [],
}


# ============================================================
# TIME HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def utc_timestamp():
    return utc_now().isoformat()


def today_utc():
    return utc_now().date()


# ============================================================
# GENERAL HELPERS
# ============================================================

def safe_float(value, default=None):

    try:

        if value is None:
            return default

        return float(value)

    except Exception:

        return default


def average(values):

    valid_values = [
        value
        for value in values
        if value is not None
    ]

    if not valid_values:
        return None

    return sum(valid_values) / len(valid_values)


def standard_deviation(values):

    valid_values = [
        value
        for value in values
        if value is not None
    ]

    if len(valid_values) < 2:
        return None

    mean_value = average(valid_values)

    variance = sum(
        (value - mean_value) ** 2
        for value in valid_values
    ) / len(valid_values)

    return math.sqrt(variance)


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
            f"Discord error: {response.text[:500]}",
            flush=True,
        )

        return False

    except Exception as error:

        print(
            f"Discord exception: {error}",
            flush=True,
        )

        return False


# ============================================================
# KALSHI API
# ============================================================

def fetch_kalshi_markets(series_ticker):

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
                f"Kalshi {series_ticker} status: "
                f"{response.status_code}",
                flush=True,
            )

            if response.status_code != 200:

                print(
                    response.text[:500],
                    flush=True,
                )

                break

            data = response.json()

            markets = data.get(
                "markets",
                [],
            )

            all_markets.extend(markets)

            cursor = data.get("cursor")

            if not cursor:
                break

            if len(all_markets) >= 5000:
                break

        except Exception as error:

            print(
                f"Kalshi error for {series_ticker}: "
                f"{error}",
                flush=True,
            )

            break

    return all_markets


# ============================================================
# DATE PARSING
#
# Example:
#
# KXHIGHCHI-26AUG28-T87
#
# 26AUG28 = August 28, 2026
# ============================================================

def parse_date_from_ticker(ticker):

    if not ticker:
        return None

    pattern = (
        r"-(\d{2})"
        r"(JAN|FEB|MAR|APR|MAY|JUN|"
        r"JUL|AUG|SEP|OCT|NOV|DEC)"
        r"(\d{2})"
        r"(?:-|$)"
    )

    match = re.search(
        pattern,
        ticker.upper(),
    )

    if not match:
        return None

    year_short = match.group(1)

    month_text = match.group(2)

    day_text = match.group(3)

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

    month_number = months.get(month_text)

    if not month_number:
        return None

    year_number = 2000 + int(year_short)

    try:

        parsed_date = datetime(
            year_number,
            month_number,
            int(day_text),
        )

        return parsed_date.strftime("%Y-%m-%d")

    except ValueError:

        return None


def get_market_date(market):

    ticker = market.get(
        "ticker",
        "",
    )

    ticker_date = parse_date_from_ticker(
        ticker
    )

    if ticker_date:
        return ticker_date

    return None


# ============================================================
# DATE FILTER
#
# Ignore markets for weather dates that have already passed.
# ============================================================

def is_relevant_forecast_date(
    date_string
):

    try:

        market_date = datetime.strptime(
            date_string,
            "%Y-%m-%d",
        ).date()

        current_date = today_utc()

        days_ahead = (
            market_date
            - current_date
        ).days

        if days_ahead < 0:
            return False

        if days_ahead > MAX_FORECAST_DAYS_AHEAD:
            return False

        return True

    except Exception:

        return False


# ============================================================
# KALSHI PRICE
# ============================================================

def get_yes_ask_cents(market):

    yes_ask_dollars = market.get(
        "yes_ask_dollars"
    )

    if yes_ask_dollars is not None:

        value = safe_float(
            yes_ask_dollars
        )

        if value is not None:

            return value * 100


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
# MARKET STRIKE INFORMATION
# ============================================================

def get_strike_info(market):

    floor_value = safe_float(
        market.get(
            "floor_strike"
        )
    )

    cap_value = safe_float(
        market.get(
            "cap_strike"
        )
    )

    return {

        "ticker": market.get(
            "ticker",
            ""
        ),

        "strike_type": market.get(
            "strike_type",
            ""
        ),

        "floor": floor_value,

        "cap": cap_value,

    }


# ============================================================
# HUMAN READABLE CONTRACT DESCRIPTION
# ============================================================

def describe_contract(strike_info):

    floor_value = strike_info.get(
        "floor"
    )

    cap_value = strike_info.get(
        "cap"
    )


    if (
        floor_value is not None
        and cap_value is not None
    ):

        return (
            f"{floor_value:g}°F to "
            f"{cap_value:g}°F"
        )


    if floor_value is not None:

        return (
            f"{floor_value:g}°F or higher"
        )


    if cap_value is not None:

        return (
            f"{cap_value:g}°F or lower"
        )


    return "Unknown strike"


# ============================================================
# POINT FORECAST
#
# Returns daily:
#
# temperature_2m_max
# precipitation_sum
# ============================================================

def fetch_point_forecast(lat, lon):

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

        "forecast_days": MAX_FORECAST_DAYS_AHEAD,

    }

    try:

        response = requests.get(
            "https://api.open-meteo.com/v1/gfs",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        print(
            f"Point forecast status: "
            f"{response.status_code}",
            flush=True,
        )

        if response.status_code != 200:

            print(
                response.text[:500],
                flush=True,
            )

            return None


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


        forecasts = {}


        for index, date_value in enumerate(dates):

            high_value = None

            precipitation_value = None


            if index < len(highs):

                high_value = highs[index]


            if index < len(precipitation):

                precipitation_value = (
                    precipitation[index]
                )


            forecasts[date_value] = {

                "high_temperature": high_value,

                "precipitation": precipitation_value,

            }


        return forecasts


    except Exception as error:

        print(
            f"Point forecast error: "
            f"{error}",
            flush=True,
        )

        return None


# ============================================================
# ENSEMBLE FORECAST
# ============================================================

def fetch_ensemble_data(lat, lon):

    params = {

        "latitude": lat,

        "longitude": lon,

        "hourly": "temperature_2m",

        "temperature_unit": "fahrenheit",

        "timezone": "auto",

        "forecast_days": MAX_FORECAST_DAYS_AHEAD,

    }


    try:

        response = requests.get(
            "https://ensemble-api.open-meteo.com/v1/ensemble",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )


        print(
            f"Ensemble API status: "
            f"{response.status_code}",
            flush=True,
        )


        if response.status_code != 200:

            print(
                response.text[:500],
                flush=True,
            )

            return None


        return response.json()


    except Exception as error:

        print(
            f"Ensemble error: {error}",
            flush=True,
        )

        return None


# ============================================================
# BUILD DAILY ENSEMBLE HIGHS
#
# Finds all temperature ensemble members.
#
# Calculates the maximum hourly temperature
# for each member on each day.
# ============================================================

def build_daily_ensemble_highs(
    ensemble_data
):

    if not ensemble_data:
        return {}


    hourly = ensemble_data.get(
        "hourly",
        {},
    )


    times = hourly.get(
        "time",
        [],
    )


    if not times:
        return {}


    member_keys = []


    for key in hourly.keys():

        if (
            key.startswith(
                "temperature_2m"
            )
            and key != "time"
        ):

            member_keys.append(key)


    daily_members = {}


    for member_key in member_keys:

        temperatures = hourly.get(
            member_key,
            [],
        )


        for index, time_value in enumerate(times):

            if index >= len(temperatures):
                continue


            temperature = temperatures[index]


            if temperature is None:
                continue


            date_value = str(
                time_value
            )[:10]


            if (
                date_value
                not in daily_members
            ):

                daily_members[
                    date_value
                ] = {}


            existing_value = (
                daily_members[
                    date_value
                ].get(member_key)
            )


            if (
                existing_value is None
                or temperature > existing_value
            ):

                daily_members[
                    date_value
                ][member_key] = temperature


    result = {}


    for date_value, members in (
        daily_members.items()
    ):

        values = list(
            members.values()
        )


        if values:

            result[
                date_value
            ] = values


    return result


# ============================================================
# FORECAST CHANGE TRACKING
# ============================================================

def detect_forecast_change(
    series_ticker,
    forecast_date,
    forecast_value,
):

    key = (
        f"{series_ticker}|"
        f"{forecast_date}|"
        f"high_temperature"
    )


    old_value = forecast_history.get(
        key
    )


    forecast_history[key] = (
        forecast_value
    )


    if old_value is None:

        return {

            "first_observation": True,

            "changed": False,

            "old": None,

            "new": forecast_value,

            "delta": 0.0,

        }


    delta = (
        forecast_value
        - old_value
    )


    return {

        "first_observation": False,

        "changed": (
            forecast_value
            != old_value
        ),

        "old": old_value,

        "new": forecast_value,

        "delta": delta,

    }


# ============================================================
# PRELIMINARY PROBABILITY MODEL
#
# Uses raw ensemble-member frequency.
#
# Example:
#
# 20 of 31 members >= 85
#
# Probability estimate:
#
# 20 / 31 = 64.5%
#
# IMPORTANT:
#
# This is NOT YET CALIBRATED.
# ============================================================

def estimate_probability_from_ensemble(
    member_highs,
    strike_info,
):

    valid_members = [

        value

        for value in member_highs

        if value is not None

    ]


    if not valid_members:
        return None


    floor_value = strike_info.get(
        "floor"
    )

    cap_value = strike_info.get(
        "cap"
    )


    favorable = 0


    # Range contract
    if (
        floor_value is not None
        and cap_value is not None
    ):

        for value in valid_members:

            if (
                value >= floor_value
                and value < cap_value
            ):

                favorable += 1


    # "X or higher"
    elif floor_value is not None:

        for value in valid_members:

            if value >= floor_value:

                favorable += 1


    # "X or lower"
    elif cap_value is not None:

        for value in valid_members:

            if value <= cap_value:

                favorable += 1


    else:

        return None


    return (
        favorable
        / len(valid_members)
    )


# ============================================================
# MARKET ANALYSIS
# ============================================================

def analyze_market(
    market,
    member_highs,
):

    yes_ask_cents = (
        get_yes_ask_cents(
            market
        )
    )


    if yes_ask_cents is None:
        return None


    # Ignore invalid prices.
    if (
        yes_ask_cents <= 0
        or yes_ask_cents >= 100
    ):

        return None


    strike_info = get_strike_info(
        market
    )


    probability = (
        estimate_probability_from_ensemble(
            member_highs,
            strike_info,
        )
    )


    if probability is None:
        return None


    market_probability = (
        yes_ask_cents
        / 100.0
    )


    edge = (
        probability
        - market_probability
    )


    return {

        "market": market,

        "probability": probability,

        "market_probability": (
            market_probability
        ),

        "edge": edge,

        "edge_points": (
            edge * 100
        ),

        "yes_ask_cents": (
            yes_ask_cents
        ),

        "strike_info": (
            strike_info
        ),

    }


# ============================================================
# ALERT COOLDOWN
# ============================================================

def can_send_alert(alert_key):

    now = time.time()


    previous_time = (
        alert_history.get(
            alert_key
        )
    )


    if previous_time is None:

        alert_history[
            alert_key
        ] = now

        return True


    elapsed = (
        now
        - previous_time
    )


    if (
        elapsed
        >= ALERT_COOLDOWN_SECONDS
    ):

        alert_history[
            alert_key
        ] = now

        return True


    return False


# ============================================================
# KALSHI LINK
# ============================================================

def get_kalshi_url(market):

    event_ticker = market.get(
        "event_ticker",
        "",
    )


    if event_ticker:

        return (
            "https://kalshi.com/markets/"
            f"{event_ticker.lower()}"
        )


    return "https://kalshi.com"


# ============================================================
# DISCORD MESSAGE
# ============================================================

def build_discord_message(
    config,
    forecast_date,
    forecast_value,
    change,
    ensemble_members,
    signals,
):

    city = config["city"]


    ensemble_mean = average(
        ensemble_members
    )


    ensemble_std = standard_deviation(
        ensemble_members
    )


    lines = [

        "🌦️ **FORECAST CHANGE DETECTED**",

        "",

        f"📍 **{city}**",

        f"📅 Weather date: "
        f"{forecast_date}",

        "",

    ]


    if change["old"] is not None:

        lines.extend([

            "🔄 **Forecast high changed:**",

            (
                f"{change['old']:.1f}°F"
                f" → "
                f"{forecast_value:.1f}°F"
            ),

            (
                f"Change: "
                f"{change['delta']:+.1f}°F"
            ),

        ])

    else:

        lines.extend([

            "🔄 **Current forecast high:**",

            f"{forecast_value:.1f}°F",

        ])


    lines.extend([

        "",

        "📊 **Ensemble distribution:**",

        f"Members: "
        f"{len(ensemble_members)}",

    ])


    if ensemble_mean is not None:

        lines.append(

            f"Mean high: "
            f"{ensemble_mean:.1f}°F"

        )


    if ensemble_std is not None:

        lines.append(

            f"Spread: "
            f"±{ensemble_std:.1f}°F"

        )


    lines.extend([

        "",

        "🎯 **Top preliminary paper signals:**",

    ])


    for index, signal in enumerate(
        signals,
        start=1,
    ):

        market = signal["market"]

        strike_info = signal[
            "strike_info"
        ]


        contract_description = (
            describe_contract(
                strike_info
            )
        )


        lines.extend([

            "",

            (
                f"**{index}. "
                f"{contract_description}**"
            ),

            (
                f"Contract: "
                f"`{market.get('ticker', 'Unknown')}`"
            ),

            (
                f"Estimated probability: "
                f"{signal['probability'] * 100:.1f}%"
            ),

            (
                f"Kalshi YES ask: "
                f"{signal['yes_ask_cents']:.1f}¢"
            ),

            (
                f"Preliminary edge: "
                f"{signal['edge_points']:+.1f} points"
            ),

            get_kalshi_url(market),

        ])


    lines.extend([

        "",

        "⚠️ **PAPER / RESEARCH ONLY**",

        (
            "Probability currently uses "
            "raw ensemble-member frequency "
            "and is not historically calibrated."
        ),

    ])


    message = "\n".join(lines)


    return message[:1900]


# ============================================================
# PRINT TOP OPPORTUNITIES
#
# This runs EVERY scan.
#
# Discord does NOT fire every scan.
# ============================================================

def print_top_opportunities(
    city,
    forecast_date,
    analyses,
):

    if not analyses:

        print(
            "No analyzable markets.",
            flush=True,
        )

        return


    analyses.sort(
        key=lambda item:
        item["edge"],
        reverse=True,
    )


    print(
        "",
        flush=True,
    )

    print(
        f"TOP OPPORTUNITIES: "
        f"{city} | {forecast_date}",
        flush=True,
    )


    top_results = analyses[:5]


    for index, analysis in enumerate(
        top_results,
        start=1,
    ):

        market = analysis["market"]

        description = describe_contract(
            analysis["strike_info"]
        )


        print(

            f"{index}. "
            f"{description} | "
            f"Model: "
            f"{analysis['probability'] * 100:.1f}% | "
            f"Ask: "
            f"{analysis['yes_ask_cents']:.1f}¢ | "
            f"Edge: "
            f"{analysis['edge_points']:+.1f} points | "
            f"{market.get('ticker')}",

            flush=True,

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

        "signals_found": 0,

        "alerts_sent": 0,

        "errors": [],

    }


    # ========================================================
    # EACH CITY
    # ========================================================

    for series_ticker, config in (
        WEATHER_SERIES.items()
    ):


        results[
            "series_checked"
        ] += 1


        print(
            "\n"
            "--------------------------------------------------",
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


        # ====================================================
        # POINT FORECAST
        # ====================================================

        point_forecasts = (
            fetch_point_forecast(
                config["lat"],
                config["lon"],
            )
        )


        if not point_forecasts:

            error = (
                f"No point forecast "
                f"for {config['city']}"
            )

            print(
                error,
                flush=True,
            )

            results[
                "errors"
            ].append(error)

            continue


        # ====================================================
        # ENSEMBLE
        # ====================================================

        ensemble_data = (
            fetch_ensemble_data(
                config["lat"],
                config["lon"],
            )
        )


        daily_ensemble_highs = (
            build_daily_ensemble_highs(
                ensemble_data
            )
        )


        print(
            f"Ensemble dates available: "
            f"{len(daily_ensemble_highs)}",
            flush=True,
        )


        # ====================================================
        # KALSHI MARKETS
        # ====================================================

        markets = (
            fetch_kalshi_markets(
                series_ticker
            )
        )


        print(
            f"Markets found: "
            f"{len(markets)}",
            flush=True,
        )


        # ====================================================
        # GROUP MARKETS BY WEATHER DATE
        # ====================================================

        markets_by_date = {}


        for market in markets:

            results[
                "markets_checked"
            ] += 1


            market_date = get_market_date(
                market
            )


            if not market_date:

                print(
                    f"Could not parse date: "
                    f"{market.get('ticker')}",
                    flush=True,
                )

                continue


            if not is_relevant_forecast_date(
                market_date
            ):

                print(
                    f"Ignoring stale/out-of-range date: "
                    f"{market_date} | "
                    f"{market.get('ticker')}",
                    flush=True,
                )

                continue


            if (
                market_date
                not in markets_by_date
            ):

                markets_by_date[
                    market_date
                ] = []


            markets_by_date[
                market_date
            ].append(market)


        # ====================================================
        # PROCESS EACH DATE ONCE
        # ====================================================

        for market_date, date_markets in (
            markets_by_date.items()
        ):


            print(
                "\n"
                "..................................................",
                flush=True,
            )

            print(
                f"DATE: {market_date}",
                flush=True,
            )


            point_forecast = (
                point_forecasts.get(
                    market_date
                )
            )


            if not point_forecast:

                print(
                    "No point forecast for this date.",
                    flush=True,
                )

                continue


            forecast_high = (
                point_forecast.get(
                    "high_temperature"
                )
            )


            precipitation = (
                point_forecast.get(
                    "precipitation"
                )
            )


            if forecast_high is None:

                print(
                    "Forecast high missing.",
                    flush=True,
                )

                continue


            member_highs = (
                daily_ensemble_highs.get(
                    market_date,
                    []
                )
            )


            print(
                f"Point forecast high: "
                f"{forecast_high:.1f}°F",
                flush=True,
            )


            if precipitation is not None:

                print(
                    f"Forecast precipitation: "
                    f"{precipitation:.2f} in",
                    flush=True,
                )


            print(
                f"Ensemble members: "
                f"{len(member_highs)}",
                flush=True,
            )


            # =================================================
            # FORECAST CHANGE
            # =================================================

            change = (
                detect_forecast_change(
                    series_ticker,
                    market_date,
                    forecast_high,
                )
            )


            if change["first_observation"]:

                print(
                    "Forecast status: "
                    "FIRST OBSERVATION",
                    flush=True,
                )

                print(
                    "Baseline stored.",
                    flush=True,
                )

            else:

                print(
                    f"Previous forecast: "
                    f"{change['old']:.1f}°F",
                    flush=True,
                )

                print(
                    f"Forecast change: "
                    f"{change['delta']:+.1f}°F",
                    flush=True,
                )


            # =================================================
            # ANALYZE ALL MARKETS
            #
            # This happens EVERY scan.
            # =================================================

            analyses = []


            for market in date_markets:

                analysis = (
                    analyze_market(
                        market,
                        member_highs,
                    )
                )


                if analysis:

                    analyses.append(
                        analysis
                    )


            # =================================================
            # LOG BEST OPPORTUNITIES EVERY SCAN
            # =================================================

            print_top_opportunities(

                config["city"],

                market_date,

                analyses,

            )


            # =================================================
            # FILTER POSITIVE SIGNALS
            # =================================================

            signals = [

                analysis

                for analysis in analyses

                if (
                    analysis["edge_points"]
                    >= MIN_EDGE_PERCENTAGE_POINTS
                )

            ]


            if signals:

                results[
                    "signals_found"
                ] += len(signals)


            # =================================================
            # DISCORD ALERT CONDITIONS
            #
            # 1. Not first observation
            # 2. Forecast actually changed
            # 3. Change >= threshold
            # 4. Positive edge exists
            # =================================================

            if change["first_observation"]:

                print(
                    "No Discord alert: "
                    "first observation.",
                    flush=True,
                )

                continue


            if not change["changed"]:

                print(
                    "No Discord alert: "
                    "forecast unchanged.",
                    flush=True,
                )

                continue


            if (
                abs(change["delta"])
                < TEMPERATURE_ALERT_CHANGE_F
            ):

                print(
                    "No Discord alert: "
                    "forecast change below threshold.",
                    flush=True,
                )

                continue


            results[
                "forecast_changes"
            ] += 1


            if not signals:

                print(
                    "No Discord alert: "
                    "no positive preliminary signal.",
                    flush=True,
                )

                continue


            # Sort best edge first.
            signals.sort(
                key=lambda item:
                item["edge"],
                reverse=True,
            )


            top_signals = signals[
                :MAX_ALERTS_PER_CITY_DATE
            ]


            # =================================================
            # ALERT KEY
            #
            # Includes the new forecast so a new movement
            # can create a new alert.
            # =================================================

            alert_key = (

                f"{series_ticker}|"

                f"{market_date}|"

                f"{forecast_high:.1f}"

            )


            if not can_send_alert(
                alert_key
            ):

                print(
                    "Discord alert cooldown active.",
                    flush=True,
                )

                continue


            # =================================================
            # BUILD MESSAGE
            # =================================================

            message = build_discord_message(

                config,

                market_date,

                forecast_high,

                change,

                member_highs,

                top_signals,

            )


            print(
                "",
                flush=True,
            )

            print(
                "DISCORD ALERT:",
                flush=True,
            )

            print(
                message,
                flush=True,
            )


            success = (
                send_discord_alert(
                    message
                )
            )


            if success:

                results[
                    "alerts_sent"
                ] += 1


    # ========================================================
    # SAVE RESULTS
    # ========================================================

    last_scan_results = results


    print(
        "\n"
        "==================================================",
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
        f"Positive preliminary signals: "
        f"{results['signals_found']}",
        flush=True,
    )

    print(
        f"Discord alerts sent: "
        f"{results['alerts_sent']}",
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
                f"BACKGROUND ERROR: {error}",
                flush=True,
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
# WEB ROUTES
# ============================================================

@app.route("/")
def home():

    return jsonify({

        "status": "running",

        "name": (
            "Weather Kalshi "
            "Research Bot"
        ),

        "paper_trading": True,

    })


@app.route("/health")
def health():

    return jsonify({

        "status": "ok",

        "discord_configured": bool(
            DISCORD_WEBHOOK_URL
        ),

        "scan_interval_seconds": (
            SCAN_INTERVAL_SECONDS
        ),

        "series": list(
            WEATHER_SERIES.keys()
        ),

        "utc_time": utc_timestamp(),

    })


@app.route("/status")
def status():

    return jsonify(
        last_scan_results
    )


@app.route("/test-alert")
def test_alert():

    message = (
        "🧪 **WEATHER BOT TEST**\n\n"
        "Discord connection is working."
    )


    success = send_discord_alert(
        message
    )


    return jsonify({

        "success": success

    })


@app.route("/run-scan")
def manual_scan():

    try:

        run_scan()

        return jsonify({

            "success": True,

            "results": (
                last_scan_results
            ),

        })

    except Exception as error:

        return jsonify({

            "success": False,

            "error": str(error),

        }), 500


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
