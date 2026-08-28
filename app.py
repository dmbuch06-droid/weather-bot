import os
import re
import time
import math
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify


# ============================================================
# WEATHER + KALSHI RESEARCH BOT
# DIAGNOSTIC VERSION
#
# PAPER TRADING / RESEARCH ONLY
# ============================================================

app = Flask(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

KALSHI_API_URL = "https://external-api.kalshi.com/trade-api/v2"

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

REQUEST_TIMEOUT = 20
SCAN_INTERVAL_SECONDS = 300

TEMPERATURE_ALERT_CHANGE_F = 1.0
MIN_EDGE_PERCENTAGE_POINTS = 5.0
MAX_ALERTS_PER_CITY_DATE = 3
ALERT_COOLDOWN_SECONDS = 1800
MAX_FORECAST_DAYS_AHEAD = 7


# ============================================================
# WEATHER SERIES
# ============================================================

WEATHER_SERIES = {
    "KXHIGHNY": {
        "city": "New York",
        "lat": 40.7128,
        "lon": -74.0060,
    },
    "KXHIGHCHI": {
        "city": "Chicago",
        "lat": 41.8781,
        "lon": -87.6298,
    },
    "KXHIGHMIA": {
        "city": "Miami",
        "lat": 25.7617,
        "lon": -80.1918,
    },
    "KXHIGHAUS": {
        "city": "Austin",
        "lat": 30.2672,
        "lon": -97.7431,
    },
}


# ============================================================
# MEMORY
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
    values = [v for v in values if v is not None]

    if not values:
        return None

    return sum(values) / len(values)


def standard_deviation(values):
    values = [v for v in values if v is not None]

    if len(values) < 2:
        return None

    mean_value = average(values)

    variance = sum(
        (value - mean_value) ** 2
        for value in values
    ) / len(values)

    return math.sqrt(variance)


# ============================================================
# DISCORD
# ============================================================

def send_discord_alert(message):
    if not DISCORD_WEBHOOK_URL:
        print("DISCORD_WEBHOOK_URL is not configured.", flush=True)
        return False

    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": message},
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
                    response.text[:1000],
                    flush=True,
                )
                break

            data = response.json()

            markets = data.get("markets", [])

            all_markets.extend(markets)

            cursor = data.get("cursor")

            if not cursor:
                break

            if len(all_markets) >= 5000:
                break

        except Exception as error:
            print(
                f"Kalshi error for {series_ticker}: {error}",
                flush=True,
            )
            break

    return all_markets


# ============================================================
# DATE PARSING
#
# Example:
# KXHIGHCHI-26AUG28-T87
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

    year = 2000 + int(match.group(1))
    month = months.get(match.group(2))
    day = int(match.group(3))

    if not month:
        return None

    try:
        return datetime(
            year,
            month,
            day,
        ).strftime("%Y-%m-%d")

    except ValueError:
        return None


def get_market_date(market):
    return parse_date_from_ticker(
        market.get("ticker", "")
    )


# ============================================================
# DATE FILTER
# ============================================================

def is_relevant_forecast_date(date_string):

    try:
        market_date = datetime.strptime(
            date_string,
            "%Y-%m-%d",
        ).date()

        days_ahead = (
            market_date - today_utc()
        ).days

        return (
            days_ahead >= 0
            and days_ahead <= MAX_FORECAST_DAYS_AHEAD
        )

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

        value = safe_float(yes_ask)

        if value is not None:

            if value <= 1:
                return value * 100

            return value

    return None


# ============================================================
# RAW KALSHI DIAGNOSTICS
# ============================================================

def print_raw_market_data(market):

    print(
        "",
        flush=True,
    )

    print(
        "RAW KALSHI MARKET DATA",
        flush=True,
    )

    print(
        f"Ticker: {market.get('ticker')}",
        flush=True,
    )

    print(
        f"Title: {market.get('title')}",
        flush=True,
    )

    print(
        f"Subtitle: {market.get('subtitle')}",
        flush=True,
    )

    print(
        f"Yes subtitle: {market.get('yes_sub_title')}",
        flush=True,
    )

    print(
        f"No subtitle: {market.get('no_sub_title')}",
        flush=True,
    )

    print(
        f"Strike type: {market.get('strike_type')}",
        flush=True,
    )

    print(
        f"Floor strike: {market.get('floor_strike')}",
        flush=True,
    )

    print(
        f"Cap strike: {market.get('cap_strike')}",
        flush=True,
    )

    print(
        f"Yes ask: {market.get('yes_ask')}",
        flush=True,
    )

    print(
        f"Yes ask dollars: "
        f"{market.get('yes_ask_dollars')}",
        flush=True,
    )

    print(
        f"Yes bid: {market.get('yes_bid')}",
        flush=True,
    )

    print(
        f"Last price: {market.get('last_price')}",
        flush=True,
    )


# ============================================================
# WEATHER POINT FORECAST
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
                response.text[:1000],
                flush=True,
            )

            return None


        data = response.json()

        daily = data.get("daily", {})

        dates = daily.get("time", [])

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

            high_value = (
                highs[index]
                if index < len(highs)
                else None
            )

            precipitation_value = (
                precipitation[index]
                if index < len(precipitation)
                else None
            )


            forecasts[date_value] = {
                "high_temperature": high_value,
                "precipitation": precipitation_value,
            }


        return forecasts


    except Exception as error:

        print(
            f"Point forecast error: {error}",
            flush=True,
        )

        return None


# ============================================================
# ENSEMBLE API
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
                response.text[:1000],
                flush=True,
            )

            return None


        return response.json()


    except Exception as error:

        print(
            f"Ensemble API error: {error}",
            flush=True,
        )

        return None


# ============================================================
# BUILD DAILY ENSEMBLE HIGHS
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
            key.startswith("temperature_2m")
            and key != "time"
        ):
            member_keys.append(key)


    print(
        f"Ensemble temperature keys found: "
        f"{len(member_keys)}",
        flush=True,
    )


    print(
        f"First ensemble keys: "
        f"{member_keys[:10]}",
        flush=True,
    )


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


            current_max = (
                daily_members[
                    date_value
                ].get(member_key)
            )


            if (
                current_max is None
                or temperature > current_max
            ):

                daily_members[
                    date_value
                ][member_key] = temperature


    result = {}


    for date_value, members in (
        daily_members.items()
    ):

        result[date_value] = list(
            members.values()
        )


    return result


# ============================================================
# ENSEMBLE DIAGNOSTICS
# ============================================================

def print_ensemble_distribution(
    member_highs
):

    if not member_highs:

        print(
            "NO ENSEMBLE MEMBERS AVAILABLE",
            flush=True,
        )

        return


    values = sorted(
        [
            value
            for value in member_highs
            if value is not None
        ]
    )


    if not values:

        print(
            "NO VALID ENSEMBLE VALUES",
            flush=True,
        )

        return


    mean_value = average(values)

    std_value = standard_deviation(values)


    print(
        "",
        flush=True,
    )

    print(
        "ENSEMBLE HIGH DISTRIBUTION",
        flush=True,
    )

    print(
        f"Members: {len(values)}",
        flush=True,
    )

    print(
        f"Minimum: {min(values):.2f}°F",
        flush=True,
    )

    print(
        f"Maximum: {max(values):.2f}°F",
        flush=True,
    )

    print(
        f"Mean: {mean_value:.2f}°F",
        flush=True,
    )


    if std_value is not None:

        print(
            f"Standard deviation: "
            f"{std_value:.2f}°F",
            flush=True,
        )


    print(
        "Sorted member highs:",
        flush=True,
    )


    formatted_values = [
        f"{value:.1f}"
        for value in values
    ]


    for start in range(
        0,
        len(formatted_values),
        10,
    ):

        chunk = formatted_values[
            start:start + 10
        ]


        print(
            "  " + ", ".join(chunk),
            flush=True,
        )


# ============================================================
# FORECAST CHANGE
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
            forecast_value != old_value
        ),
        "old": old_value,
        "new": forecast_value,
        "delta": delta,
    }


# ============================================================
# TEMPORARY STRIKE INTERPRETATION
#
# This is intentionally conservative.
#
# We will verify this against the raw Kalshi
# fields printed in the logs.
# ============================================================

def get_strike_info(market):

    return {

        "ticker": market.get(
            "ticker",
            "",
        ),

        "strike_type": market.get(
            "strike_type",
            "",
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

    }


# ============================================================
# PROBABILITY MODEL
#
# DIAGNOSTIC ONLY
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


    elif floor_value is not None:

        for value in valid_members:

            if value >= floor_value:
                favorable += 1


    elif cap_value is not None:

        for value in valid_members:

            if value <= cap_value:
                favorable += 1


    else:
        return None


    return favorable / len(valid_members)


# ============================================================
# CONTRACT DESCRIPTION
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
            f"{floor_value:g}°F "
            f"to {cap_value:g}°F"
        )


    if floor_value is not None:
        return (
            f"{floor_value:g}°F "
            f"or higher"
        )


    if cap_value is not None:
        return (
            f"{cap_value:g}°F "
            f"or lower"
        )


    return "Unknown strike"


# ============================================================
# ANALYZE MARKET
# ============================================================

def analyze_market(
    market,
    member_highs,
):

    yes_ask_cents = get_yes_ask_cents(
        market
    )


    if yes_ask_cents is None:
        return None


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
        yes_ask_cents / 100.0
    )


    edge = (
        probability
        - market_probability
    )


    return {

        "market": market,

        "probability": probability,

        "yes_ask_cents": yes_ask_cents,

        "edge_points": edge * 100,

        "strike_info": strike_info,

    }


# ============================================================
# PRINT ANALYSIS
# ============================================================

def print_market_analysis(
    analysis
):

    market = analysis["market"]

    description = describe_contract(
        analysis["strike_info"]
    )


    print(
        f"Contract: {market.get('ticker')}",
        flush=True,
    )

    print(
        f"Interpretation: {description}",
        flush=True,
    )

    print(
        f"Model probability: "
        f"{analysis['probability'] * 100:.1f}%",
        flush=True,
    )

    print(
        f"YES ask: "
        f"{analysis['yes_ask_cents']:.1f}¢",
        flush=True,
    )

    print(
        f"Edge: "
        f"{analysis['edge_points']:+.1f} points",
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
        "STARTING WEATHER MARKET DIAGNOSTIC SCAN",
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


    for series_ticker, config in (
        WEATHER_SERIES.items()
    ):

        results["series_checked"] += 1


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

            results["errors"].append(
                f"No point forecast for "
                f"{config['city']}"
            )

            continue


        # ----------------------------------------------------
        # ENSEMBLE
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
        # KALSHI
        # ----------------------------------------------------

        markets = fetch_kalshi_markets(
            series_ticker
        )


        print(
            f"Markets found: {len(markets)}",
            flush=True,
        )


        markets_by_date = {}


        for market in markets:

            results["markets_checked"] += 1


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


            markets_by_date.setdefault(
                market_date,
                [],
            ).append(market)


        # ----------------------------------------------------
        # PROCESS EACH DATE
        # ----------------------------------------------------

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
                    "No point forecast for date.",
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
                    [],
                )
            )


            print(
                f"POINT FORECAST HIGH: "
                f"{forecast_high:.2f}°F",
                flush=True,
            )


            if precipitation is not None:

                print(
                    f"POINT FORECAST PRECIPITATION: "
                    f"{precipitation:.2f} inches",
                    flush=True,
                )


            # ------------------------------------------------
            # PRINT ENSEMBLE DIAGNOSTICS
            # ------------------------------------------------

            print_ensemble_distribution(
                member_highs
            )


            # ------------------------------------------------
            # FORECAST HISTORY
            # ------------------------------------------------

            change = detect_forecast_change(
                series_ticker,
                market_date,
                forecast_high,
            )


            if change["first_observation"]:

                print(
                    "FORECAST STATUS: "
                    "FIRST OBSERVATION",
                    flush=True,
                )

            else:

                print(
                    f"PREVIOUS FORECAST: "
                    f"{change['old']:.2f}°F",
                    flush=True,
                )

                print(
                    f"FORECAST CHANGE: "
                    f"{change['delta']:+.2f}°F",
                    flush=True,
                )


            # ------------------------------------------------
            # RAW KALSHI DATA
            #
            # Print every market so we can verify
            # strike interpretation.
            # ------------------------------------------------

            print(
                "",
                flush=True,
            )

            print(
                "==================================================",
                flush=True,
            )

            print(
                "RAW KALSHI CONTRACT INSPECTION",
                flush=True,
            )

            print(
                "==================================================",
                flush=True,
            )


            analyses = []


            for market in date_markets:

                print_raw_market_data(
                    market
                )


                analysis = analyze_market(
                    market,
                    member_highs,
                )


                if analysis:

                    analyses.append(
                        analysis
                    )


                    print(
                        "CURRENT DIAGNOSTIC MODEL:",
                        flush=True,
                    )


                    print_market_analysis(
                        analysis
                    )


            # ------------------------------------------------
            # TOP RESULTS
            # ------------------------------------------------

            analyses.sort(
                key=lambda item:
                item["edge_points"],
                reverse=True,
            )


            print(
                "",
                flush=True,
            )

            print(
                "==================================================",
                flush=True,
            )

            print(
                "TOP DIAGNOSTIC RESULTS",
                flush=True,
            )

            print(
                "==================================================",
                flush=True,
            )


            for analysis in analyses[:5]:

                description = describe_contract(
                    analysis["strike_info"]
                )


                print(

                    f"{description} | "
                    f"Model: "
                    f"{analysis['probability'] * 100:.1f}% | "
                    f"Ask: "
                    f"{analysis['yes_ask_cents']:.1f}¢ | "
                    f"Edge: "
                    f"{analysis['edge_points']:+.1f} points | "
                    f"{analysis['market'].get('ticker')}",

                    flush=True,

                )


            positive_signals = [

                analysis

                for analysis in analyses

                if (
                    analysis["edge_points"]
                    >= MIN_EDGE_PERCENTAGE_POINTS
                )

            ]


            results[
                "signals_found"
            ] += len(
                positive_signals
            )


    last_scan_results = results


    print(
        "\n"
        "==================================================",
        flush=True,
    )

    print(
        "DIAGNOSTIC SCAN COMPLETE",
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
        f"Diagnostic positive signals: "
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
        "mode": "diagnostic",
        "paper_trading": True,
    })


@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "discord_configured": bool(
            DISCORD_WEBHOOK_URL
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

    success = send_discord_alert(
        "🧪 **WEATHER BOT TEST**\n\n"
        "Discord connection is working."
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
            "results": last_scan_results,
        })

    except Exception as error:

        return jsonify({
            "success": False,
            "error": str(error),
        }), 500


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
        f"Starting server on port {port}",
        flush=True,
    )


    app.run(
        host="0.0.0.0",
        port=port,
    )
