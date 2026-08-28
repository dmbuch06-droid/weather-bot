import os
import re
import time
import math
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify


# ============================================================
# WEATHER KALSHI RESEARCH BOT
#
# PAPER TRADING / RESEARCH ONLY
#
# Main functions:
#
# 1. Find live Kalshi weather markets.
# 2. Parse the actual weather date from the market ticker.
# 3. Pull weather forecasts.
# 4. Pull ensemble forecast members.
# 5. Detect forecast changes.
# 6. Estimate preliminary probabilities.
# 7. Compare probabilities with Kalshi prices.
# 8. Send Discord research alerts.
#
# NO AUTOMATIC TRADING.
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

SCAN_INTERVAL_SECONDS = 300

TEMPERATURE_ALERT_CHANGE_F = 1.0

MIN_EDGE_PERCENTAGE_POINTS = 5.0

MAX_ALERTS_PER_CITY_PER_SCAN = 3


# ============================================================
# KALSHI WEATHER SERIES
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
# IN-MEMORY STATE
#
# IMPORTANT:
#
# Render restarts will reset this.
#
# This is acceptable for development.
# Later we should move this to persistent storage.
# ============================================================

forecast_history = {}

alert_history = {}

last_scan_results = {
    "timestamp": None,
    "series_checked": 0,
    "markets_checked": 0,
    "forecast_changes": 0,
    "signals_found": 0,
    "errors": [],
}


# ============================================================
# TIME HELPERS
# ============================================================

def utc_now():

    return datetime.now(
        timezone.utc
    )


def utc_timestamp():

    return utc_now().isoformat()


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

    return (
        sum(valid_values)
        / len(valid_values)
    )


def standard_deviation(values):

    valid_values = [
        value
        for value in values
        if value is not None
    ]

    if len(valid_values) < 2:
        return None

    mean_value = average(
        valid_values
    )

    variance = sum(
        (
            value
            - mean_value
        ) ** 2
        for value in valid_values
    ) / len(valid_values)

    return math.sqrt(
        variance
    )


# ============================================================
# DISCORD
# ============================================================

def send_discord_alert(message):

    if not DISCORD_WEBHOOK_URL:

        print(
            "DISCORD_WEBHOOK_URL "
            "is not configured.",
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
            f"Discord response: "
            f"{response.status_code}",
            flush=True,
        )

        if response.status_code in (
            200,
            204,
        ):

            return True

        print(
            f"Discord error: "
            f"{response.text[:500]}",
            flush=True,
        )

        return False

    except Exception as error:

        print(
            f"Discord exception: "
            f"{error}",
            flush=True,
        )

        return False


# ============================================================
# KALSHI API
# ============================================================

def fetch_kalshi_markets(
    series_ticker
):

    all_markets = []

    cursor = None

    while True:

        params = {
            "series_ticker": (
                series_ticker
            ),
            "status": "open",
            "limit": 1000,
        }

        if cursor:

            params["cursor"] = (
                cursor
            )

        try:

            response = requests.get(
                f"{KALSHI_API_URL}/markets",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            print(
                f"Kalshi "
                f"{series_ticker} "
                f"status: "
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

            all_markets.extend(
                markets
            )

            cursor = data.get(
                "cursor"
            )

            if not cursor:
                break

            if len(all_markets) >= 5000:
                break

        except Exception as error:

            print(
                f"Kalshi error for "
                f"{series_ticker}: "
                f"{error}",
                flush=True,
            )

            break

    return all_markets


# ============================================================
# KALSHI DATE PARSING
#
# Example:
#
# KXHIGHCHI-26AUG28-T87
#
# Contains:
#
# 26AUG28
#
# Which becomes:
#
# 2026-08-28
# ============================================================

def parse_date_from_ticker(
    ticker
):

    if not ticker:

        return None

    pattern = (
        r"-(\d{2})(JAN|FEB|MAR|APR|MAY|"
        r"JUN|JUL|AUG|SEP|OCT|NOV|DEC)"
        r"(\d{2})(?:-|$)"
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

    month_number = months.get(
        month_text
    )

    if not month_number:

        return None

    year_number = (
        2000
        + int(year_short)
    )

    try:

        date_object = datetime(
            year_number,
            month_number,
            int(day_text),
        )

        return date_object.strftime(
            "%Y-%m-%d"
        )

    except ValueError:

        return None


def get_market_date(
    market
):

    ticker = market.get(
        "ticker",
        "",
    )

    ticker_date = (
        parse_date_from_ticker(
            ticker
        )
    )

    if ticker_date:

        return ticker_date

    for field in [
        "occurrence_datetime",
        "strike_date",
    ]:

        value = market.get(
            field
        )

        if value:

            value = str(value)

            if re.match(
                r"^\d{4}-\d{2}-\d{2}",
                value,
            ):

                return value[:10]

    return None


# ============================================================
# KALSHI PRICE
# ============================================================

def get_yes_ask_cents(
    market
):

    yes_ask_dollars = (
        market.get(
            "yes_ask_dollars"
        )
    )

    if yes_ask_dollars is not None:

        value = safe_float(
            yes_ask_dollars
        )

        if value is not None:

            return (
                value * 100
            )

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

def get_strike_info(
    market
):

    return {
        "strike_type": (
            market.get(
                "strike_type"
            )
        ),

        "floor": safe_float(
            market.get(
                "floor_strike"
            )
        ),

        "cap": safe_float(
            market.get(
                "cap_strike"
            )
        ),

        "ticker": market.get(
            "ticker",
            ""
        ),
    }


# ============================================================
# WEATHER FORECAST
#
# PRIMARY:
#
# Open-Meteo GFS endpoint.
#
# For short range US forecasts,
# this can include HRRR coverage.
#
# We use this for the current
# point forecast.
# ============================================================

def fetch_point_forecast(
    lat,
    lon,
):

    params = {
        "latitude": lat,
        "longitude": lon,

        "daily": (
            "temperature_2m_max,"
            "precipitation_sum"
        ),

        "temperature_unit": (
            "fahrenheit"
        ),

        "precipitation_unit": (
            "inch"
        ),

        "timezone": "auto",

        "forecast_days": 7,
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

        for index, date_value in enumerate(
            dates
        ):

            high = None

            precip = None

            if index < len(highs):

                high = highs[index]

            if index < len(
                precipitation
            ):

                precip = (
                    precipitation[index]
                )

            forecasts[date_value] = {
                "high_temperature": high,
                "precipitation": precip,
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
#
# We request hourly ensemble
# temperatures.
#
# Then calculate the maximum
# temperature for each member
# on the target local date.
#
# This produces a collection
# of possible daily highs.
# ============================================================

def fetch_ensemble_data(
    lat,
    lon,
):

    params = {
        "latitude": lat,
        "longitude": lon,

        "hourly": (
            "temperature_2m"
        ),

        "temperature_unit": (
            "fahrenheit"
        ),

        "timezone": "auto",

        "forecast_days": 7,
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
                response.text[:300],
                flush=True,
            )

            return None

        return response.json()

    except Exception as error:

        print(
            f"Ensemble error: "
            f"{error}",
            flush=True,
        )

        return None


# ============================================================
# BUILD DAILY ENSEMBLE HIGHS
#
# The ensemble API returns
# hourly member arrays.
#
# We detect fields such as:
#
# temperature_2m_member01
# temperature_2m_member02
#
# For each member we calculate
# the maximum temperature
# for each date.
# ============================================================

def build_daily_ensemble_highs(
    ensemble_data
):

    if not ensemble_data:

        return {}

    hourly = ensemble_data.get(
        "hourly",
        {}
    )

    times = hourly.get(
        "time",
        []
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

            member_keys.append(
                key
            )

    daily_members = {}

    for member_key in member_keys:

        temperatures = hourly.get(
            member_key,
            []
        )

        for index, time_value in enumerate(
            times
        ):

            if index >= len(
                temperatures
            ):

                continue

            temperature = (
                temperatures[index]
            )

            if temperature is None:

                continue

            date_value = (
                str(time_value)[:10]
            )

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
                ].get(
                    member_key
                )
            )

            if (
                existing_value is None
                or temperature
                > existing_value
            ):

                daily_members[
                    date_value
                ][
                    member_key
                ] = temperature


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
            "changed": False,
            "first_observation": True,
            "old": None,
            "new": forecast_value,
            "delta": 0,
        }

    delta = (
        forecast_value
        - old_value
    )

    return {
        "changed": (
            old_value
            != forecast_value
        ),

        "first_observation": False,

        "old": old_value,

        "new": forecast_value,

        "delta": delta,
    }


# ============================================================
# ESTIMATE PROBABILITY
#
# IMPORTANT:
#
# This is preliminary and NOT
# calibrated yet.
#
# If we have ensemble members,
# calculate empirical probability:
#
# number of members satisfying
# the condition
#
# divided by
#
# total valid members
#
# ============================================================

def estimate_probability_from_ensemble(
    member_highs,
    strike_info,
):

    if not member_highs:

        return None

    floor_value = (
        strike_info.get(
            "floor"
        )
    )

    cap_value = (
        strike_info.get(
            "cap"
        )
    )

    strike_type = (
        strike_info.get(
            "strike_type"
        )
        or ""
    ).lower()


    valid_members = [
        value
        for value in member_highs
        if value is not None
    ]

    if not valid_members:

        return None


    favorable = 0


    # --------------------------------------------------------
    # RANGE CONTRACT
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # FLOOR ONLY
    # --------------------------------------------------------

    elif floor_value is not None:

        for value in valid_members:

            if value >= floor_value:

                favorable += 1


    # --------------------------------------------------------
    # CAP ONLY
    # --------------------------------------------------------

    elif cap_value is not None:

        for value in valid_members:

            if value <= cap_value:

                favorable += 1


    else:

        return None


    probability = (
        favorable
        / len(valid_members)
    )


    return probability


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

def can_send_alert(
    alert_key
):

    cooldown_seconds = (
        1800
    )

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


    if (
        now
        - previous_time
        >= cooldown_seconds
    ):

        alert_history[
            alert_key
        ] = now

        return True


    return False


# ============================================================
# KALSHI URL
# ============================================================

def get_kalshi_url(
    market
):

    event_ticker = (
        market.get(
            "event_ticker",
            ""
        )
    )

    if event_ticker:

        return (
            "https://kalshi.com/markets/"
            f"{event_ticker.lower()}"
        )

    return (
        "https://kalshi.com"
    )


# ============================================================
# BUILD DISCORD ALERT
# ============================================================

def build_discord_message(
    config,
    forecast_date,
    forecast_value,
    change,
    ensemble_members,
    signals,
):

    city = config[
        "city"
    ]


    ensemble_mean = average(
        ensemble_members
    )


    ensemble_std = (
        standard_deviation(
            ensemble_members
        )
    )


    if change["old"] is not None:

        change_text = (
            f"{change['old']:.1f}°F → "
            f"{forecast_value:.1f}°F "
            f"({change['delta']:+.1f}°F)"
        )

    else:

        change_text = (
            f"Current: "
            f"{forecast_value:.1f}°F"
        )


    lines = [

        "🌦️ **FORECAST CHANGE DETECTED**",

        "",

        f"📍 **{city}**",

        f"📅 Forecast date: "
        f"{forecast_date}",

        "",

        "🔄 **Forecast:**",

        change_text,

        "",

        "📊 **Ensemble:**",

        f"Members: "
        f"{len(ensemble_members)}",

    ]


    if ensemble_mean is not None:

        lines.append(
            f"Mean daily high: "
            f"{ensemble_mean:.1f}°F"
        )


    if ensemble_std is not None:

        lines.append(
            f"Spread: "
            f"±{ensemble_std:.1f}°F"
        )


    lines.extend(
        [

            "",

            "🎯 **Top paper signals:**",

        ]
    )


    for index, signal in enumerate(
        signals,
        start=1,
    ):

        market = signal[
            "market"
        ]

        ticker = market.get(
            "ticker",
            "Unknown"
        )

        probability = (
            signal[
                "probability"
            ] * 100
        )

        ask = signal[
            "yes_ask_cents"
        ]

        edge = (
            signal["edge"]
            * 100
        )


        lines.extend(
            [

                "",

                f"**{index}. {ticker}**",

                f"Model probability: "
                f"{probability:.1f}%",

                f"Kalshi YES ask: "
                f"{ask:.1f}¢",

                f"Preliminary edge: "
                f"{edge:+.1f} points",

                get_kalshi_url(
                    market
                ),

            ]
        )


    lines.extend(
        [

            "",

            "⚠️ **PAPER / RESEARCH ONLY**",

            "Probability is currently based "
            "on raw ensemble-member frequency "
            "and is not historically calibrated.",

        ]
    )


    message = "\n".join(
        lines
    )


    # Discord content limit
    return message[:1900]


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
        f"UTC: "
        f"{utc_timestamp()}",
        flush=True,
    )


    results = {
        "timestamp": (
            utc_timestamp()
        ),

        "series_checked": 0,

        "markets_checked": 0,

        "forecast_changes": 0,

        "signals_found": 0,

        "errors": [],
    }


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
            f"SERIES: "
            f"{series_ticker}",
            flush=True,
        )

        print(
            f"CITY: "
            f"{config['city']}",
            flush=True,
        )


        # ----------------------------------------------------
        # POINT FORECAST
        # ----------------------------------------------------

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
            ].append(
                error
            )

            continue


        # ----------------------------------------------------
        # ENSEMBLE DATA
        # ----------------------------------------------------

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


        # ----------------------------------------------------
        # KALSHI MARKETS
        # ----------------------------------------------------

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


        # ----------------------------------------------------
        # GROUP MARKETS BY DATE
        # ----------------------------------------------------

        markets_by_date = {}


        for market in markets:

            results[
                "markets_checked"
            ] += 1


            market_date = (
                get_market_date(
                    market
                )
            )


            if not market_date:

                print(
                    f"Could not parse date: "
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
            ].append(
                market
            )


        # ----------------------------------------------------
        # PROCESS EACH WEATHER DATE ONCE
        # ----------------------------------------------------

        for market_date, date_markets in (
            markets_by_date.items()
        ):


            point_forecast = (
                point_forecasts.get(
                    market_date
                )
            )


            if not point_forecast:

                print(
                    f"No point forecast "
                    f"for {market_date}",
                    flush=True,
                )

                continue


            forecast_high = (
                point_forecast.get(
                    "high_temperature"
                )
            )


            if forecast_high is None:

                continue


            change = (
                detect_forecast_change(
                    series_ticker,
                    market_date,
                    forecast_high,
                )
            )


            member_highs = (
                daily_ensemble_highs.get(
                    market_date,
                    []
                )
            )


            print(
                f"\nDATE: "
                f"{market_date}",
                flush=True,
            )

            print(
                f"Point forecast high: "
                f"{forecast_high:.1f}°F",
                flush=True,
            )

            print(
                f"Forecast change: "
                f"{change['delta']:+.1f}°F",
                flush=True,
            )

            print(
                f"Ensemble members: "
                f"{len(member_highs)}",
                flush=True,
            )


            # ------------------------------------------------
            # Only generate trade signals
            # after a meaningful forecast change.
            # ------------------------------------------------

            if change[
                "first_observation"
            ]:

                print(
                    "First observation. "
                    "Baseline stored.",
                    flush=True,
                )

                continue


            if not change[
                "changed"
            ]:

                continue


            if (
                abs(
                    change["delta"]
                )
                < TEMPERATURE_ALERT_CHANGE_F
            ):

                print(
                    "Forecast changed but "
                    "below alert threshold.",
                    flush=True,
                )

                continue


            results[
                "forecast_changes"
            ] += 1


            # ------------------------------------------------
            # ANALYZE MARKETS
            # ------------------------------------------------

            signals = []


            for market in date_markets:

                analysis = (
                    analyze_market(
                        market,
                        member_highs,
                    )
                )


                if not analysis:

                    continue


                edge_points = (
                    analysis["edge"]
                    * 100
                )


                print(
                    f"{market.get('ticker')} | "
                    f"Model: "
                    f"{analysis['probability'] * 100:.1f}% | "
                    f"Ask: "
                    f"{analysis['yes_ask_cents']:.1f}¢ | "
                    f"Edge: "
                    f"{edge_points:+.1f}",
                    flush=True,
                )


                if (
                    edge_points
                    >= MIN_EDGE_PERCENTAGE_POINTS
                ):

                    signals.append(
                        analysis
                    )


            if not signals:

                print(
                    "No preliminary "
                    "paper signals found.",
                    flush=True,
                )

                continue


            # ------------------------------------------------
            # SORT BY BEST EDGE
            # ------------------------------------------------

            signals.sort(
                key=lambda item:
                item["edge"],
                reverse=True,
            )


            top_signals = signals[
                :MAX_ALERTS_PER_CITY_PER_SCAN
            ]


            results[
                "signals_found"
            ] += len(
                top_signals
            )


            # ------------------------------------------------
            # DISCORD ALERT
            # ------------------------------------------------

            alert_key = (
                f"{series_ticker}|"
                f"{market_date}|"
                f"{forecast_high:.1f}"
            )


            if not can_send_alert(
                alert_key
            ):

                print(
                    "Alert cooldown active.",
                    flush=True,
                )

                continue


            message = (
                build_discord_message(
                    config,
                    market_date,
                    forecast_high,
                    change,
                    member_highs,
                    top_signals,
                )
            )


            print(
                "\nDISCORD SIGNAL:",
                flush=True,
            )

            print(
                message,
                flush=True,
            )


            send_discord_alert(
                message
            )


    last_scan_results = (
        results
    )


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
        f"Paper signals: "
        f"{results['signals_found']}",
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
                f"BACKGROUND ERROR: "
                f"{error}",
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
# FLASK ROUTES
# ============================================================

@app.route("/")
def home():

    return jsonify(
        {
            "status": "running",
            "name": (
                "Weather Kalshi "
                "Research Bot"
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

            "series": list(
                WEATHER_SERIES.keys()
            ),

            "utc_time": (
                utc_timestamp()
            ),
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
        "🧪 **WEATHER BOT TEST**\n\n"
        "Discord connection is working."
    )


    success = (
        send_discord_alert(
            message
        )
    )


    return jsonify(
        {
            "success": success
        }
    )


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
        f"Starting server "
        f"on port {port}",
        flush=True,
    )


    app.run(
        host="0.0.0.0",
        port=port,
    )
