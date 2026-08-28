import os
import time
import json
import threading
import statistics
from datetime import datetime, timezone, date
from collections import defaultdict

import requests
from flask import Flask, jsonify


# ==========================================================
# CONFIGURATION
# ==========================================================

app = Flask(__name__)

KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

SCAN_INTERVAL_SECONDS = int(
    os.environ.get("SCAN_INTERVAL_SECONDS", "300")
)

MIN_FORECAST_CHANGE_F = float(
    os.environ.get("MIN_FORECAST_CHANGE_F", "1.0")
)

MIN_EDGE_POINTS = float(
    os.environ.get("MIN_EDGE_POINTS", "5.0")
)

MAX_POINT_ENSEMBLE_GAP_F = float(
    os.environ.get("MAX_POINT_ENSEMBLE_GAP_F", "6.0")
)

STATE_FILE = os.environ.get(
    "STATE_FILE",
    "forecast_state.json"
)

REQUEST_TIMEOUT = 20


# ==========================================================
# CITY CONFIGURATION
# ==========================================================

CITIES = {
    "KXHIGHNY": {
        "name": "New York",
        "lat": 40.7128,
        "lon": -74.0060,
        "timezone": "America/New_York",
    },

    "KXHIGHCHI": {
        "name": "Chicago",
        "lat": 41.8781,
        "lon": -87.6298,
        "timezone": "America/Chicago",
    },

    "KXHIGHMIA": {
        "name": "Miami",
        "lat": 25.7617,
        "lon": -80.1918,
        "timezone": "America/New_York",
    },

    "KXHIGHAUS": {
        "name": "Austin",
        "lat": 30.2672,
        "lon": -97.7431,
        "timezone": "America/Chicago",
    },
}


# ==========================================================
# GLOBAL STATUS
# ==========================================================

bot_status = {
    "last_scan_utc": None,
    "last_scan_success": None,
    "series_checked": 0,
    "markets_checked": 0,
    "forecast_changes": 0,
    "positive_signals": 0,
    "discord_alerts": 0,
    "last_error": None,
}

forecast_state = {}
state_lock = threading.Lock()


# ==========================================================
# HELPER FUNCTIONS
# ==========================================================

def now_utc():
    return datetime.now(timezone.utc)


def safe_float(value, default=None):
    if value is None:
        return default

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def cents_from_market(market):
    """
    Kalshi may return prices in several possible fields.

    Prefer dollar-denominated ask fields if available.
    """

    dollar_fields = [
        "yes_ask_dollars",
        "yes_ask_price_dollars",
    ]

    for field in dollar_fields:
        value = safe_float(market.get(field))

        if value is not None:
            if 0 <= value <= 1:
                return value * 100.0

    cent_fields = [
        "yes_ask",
        "yes_ask_price",
    ]

    for field in cent_fields:
        value = safe_float(market.get(field))

        if value is not None:
            if 0 <= value <= 1:
                return value * 100.0

            return value

    return None


# ==========================================================
# STATE MANAGEMENT
# ==========================================================

def load_state():
    global forecast_state

    try:
        if not os.path.exists(STATE_FILE):
            print(
                "No previous state file found. Starting fresh.",
                flush=True
            )
            forecast_state = {}
            return

        with open(STATE_FILE, "r", encoding="utf-8") as file:
            forecast_state = json.load(file)

        print(
            f"Loaded {len(forecast_state)} forecast state entries.",
            flush=True
        )

    except Exception as error:
        print(
            f"State load error: {error}",
            flush=True
        )

        forecast_state = {}


def save_state():
    try:
        with state_lock:
            temp_file = STATE_FILE + ".tmp"

            with open(
                temp_file,
                "w",
                encoding="utf-8"
            ) as file:
                json.dump(
                    forecast_state,
                    file,
                    indent=2
                )

            os.replace(
                temp_file,
                STATE_FILE
            )

    except Exception as error:
        print(
            f"State save error: {error}",
            flush=True
        )


# ==========================================================
# DISCORD
# ==========================================================

def send_discord_alert(message):
    if not DISCORD_WEBHOOK_URL:
        print(
            "Discord webhook is not configured. "
            "Set DISCORD_WEBHOOK_URL in Render.",
            flush=True
        )
        return False

    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": message},
            timeout=REQUEST_TIMEOUT
        )

        print(
            f"Discord response: {response.status_code}",
            flush=True
        )

        if 200 <= response.status_code < 300:
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


# ==========================================================
# POINT FORECAST
# ==========================================================

def get_point_forecast(city):
    """
    Fetches daily maximum temperature and precipitation.

    Uses Open-Meteo forecast API.
    """

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": (
            "temperature_2m_max,"
            "precipitation_sum"
        ),
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "timezone": city["timezone"],
        "forecast_days": 7,
    }

    try:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        print(
            f"Point forecast status: "
            f"{response.status_code}",
            flush=True
        )

        if response.status_code != 200:
            print(
                response.text[:500],
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

        result = {}

        for index, forecast_date in enumerate(dates):

            high = (
                safe_float(highs[index])
                if index < len(highs)
                else None
            )

            precip = (
                safe_float(precipitation[index])
                if index < len(precipitation)
                else None
            )

            if high is None:
                continue

            result[forecast_date] = {
                "high": high,
                "precipitation": (
                    precip
                    if precip is not None
                    else 0.0
                ),
            }

        return result

    except Exception as error:
        print(
            f"Point forecast error: {error}",
            flush=True
        )

        return {}


# ==========================================================
# ENSEMBLE FORECAST
# ==========================================================

def get_ensemble_forecast(city):
    """
    Fetches ensemble hourly temperatures.

    Calculates a daily maximum for each ensemble member.

    The result is:

    {
        "YYYY-MM-DD": {
            "member_highs": [...]
        }
    }
    """

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "hourly": "temperature_2m",
        "temperature_unit": "fahrenheit",
        "timezone": city["timezone"],
        "forecast_days": 7,
    }

    url = (
        "https://ensemble-api.open-meteo.com/"
        "v1/ensemble"
    )

    try:
        response = requests.get(
            url,
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        print(
            f"Ensemble API status: "
            f"{response.status_code}",
            flush=True
        )

        if response.status_code != 200:
            print(
                response.text[:500],
                flush=True
            )
            return {}

        data = response.json()
        hourly = data.get("hourly", {})

        timestamps = hourly.get("time", [])

        member_keys = []

        for key in hourly.keys():

            if (
                key.startswith(
                    "temperature_2m_member"
                )
            ):
                member_keys.append(key)

        member_keys.sort()

        print(
            f"Ensemble member temperature keys found: "
            f"{len(member_keys)}",
            flush=True
        )

        if not timestamps or not member_keys:
            return {}

        # For each member, calculate the daily maximum.
        #
        # member_daily[date][member] = max temperature

        member_daily = defaultdict(
            lambda: defaultdict(list)
        )

        for member_key in member_keys:

            temperatures = hourly.get(
                member_key,
                []
            )

            for index, timestamp in enumerate(
                timestamps
            ):

                if index >= len(temperatures):
                    continue

                temperature = safe_float(
                    temperatures[index]
                )

                if temperature is None:
                    continue

                forecast_date = timestamp[:10]

                member_daily[
                    forecast_date
                ][member_key].append(
                    temperature
                )

        result = {}

        for forecast_date, members in (
            member_daily.items()
        ):

            member_highs = []

            for member_key in member_keys:

                values = members.get(
                    member_key,
                    []
                )

                if not values:
                    continue

                member_highs.append(
                    max(values)
                )

            if member_highs:
                result[forecast_date] = {
                    "member_highs": member_highs
                }

        print(
            f"Ensemble dates available: "
            f"{len(result)}",
            flush=True
        )

        return result

    except Exception as error:
        print(
            f"Ensemble forecast error: {error}",
            flush=True
        )

        return {}


# ==========================================================
# WEATHER VALIDATION
# ==========================================================

def validate_weather_alignment(
    point_high,
    ensemble_data
):
    """
    Safety check.

    We do NOT assume that raw ensemble frequency is
    trustworthy when the point forecast and ensemble mean
    are wildly different.

    This does not prove the data is correct.
    It simply blocks Discord alerts when there is a major
    inconsistency.
    """

    if point_high is None:
        return {
            "valid": False,
            "reason": "missing point forecast",
            "ensemble_mean": None,
            "gap_f": None,
        }

    if not ensemble_data:
        return {
            "valid": False,
            "reason": "missing ensemble data",
            "ensemble_mean": None,
            "gap_f": None,
        }

    member_highs = ensemble_data.get(
        "member_highs",
        []
    )

    if len(member_highs) < 5:
        return {
            "valid": False,
            "reason": "too few ensemble members",
            "ensemble_mean": None,
            "gap_f": None,
        }

    ensemble_mean = statistics.mean(
        member_highs
    )

    gap_f = abs(
        point_high - ensemble_mean
    )

    if gap_f > MAX_POINT_ENSEMBLE_GAP_F:

        return {
            "valid": False,
            "reason": (
                f"point/ensemble mean gap "
                f"{gap_f:.2f}F exceeds "
                f"{MAX_POINT_ENSEMBLE_GAP_F:.2f}F"
            ),
            "ensemble_mean": ensemble_mean,
            "gap_f": gap_f,
        }

    return {
        "valid": True,
        "reason": "passed sanity check",
        "ensemble_mean": ensemble_mean,
        "gap_f": gap_f,
    }


# ==========================================================
# KALSHI
# ==========================================================

def get_kalshi_markets(series_ticker):
    """
    Fetches open markets for one Kalshi series.
    """

    try:

        params = {
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 200,
        }

        response = requests.get(
            f"{KALSHI_API_URL}/markets",
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        print(
            f"Kalshi {series_ticker} status: "
            f"{response.status_code}",
            flush=True
        )

        if response.status_code != 200:
            print(
                response.text[:500],
                flush=True
            )
            return []

        data = response.json()

        markets = data.get(
            "markets",
            []
        )

        print(
            f"Markets found: "
            f"{len(markets)}",
            flush=True
        )

        return markets

    except Exception as error:

        print(
            f"Kalshi API error "
            f"for {series_ticker}: {error}",
            flush=True
        )

        return []


# ==========================================================
# MARKET DATE PARSING
# ==========================================================

def parse_market_date(ticker):
    """
    Examples:

    KXHIGHNY-26AUG28-T87
    KXHIGHCHI-26AUG28-B84.5

    Date portion:
    26AUG28

    Meaning:
    YYYY-MM-DD
    """

    try:

        parts = ticker.split("-")

        if len(parts) < 2:
            return None

        raw_date = parts[1]

        if len(raw_date) != 7:
            return None

        parsed = datetime.strptime(
            raw_date,
            "%y%b%d"
        )

        return parsed.strftime(
            "%Y-%m-%d"
        )

    except Exception:
        return None


# ==========================================================
# STRIKE INTERPRETATION
# ==========================================================

def get_market_strike(market):
    """
    Returns a normalized contract description.

    Possible types:

    less
    greater
    between
    """

    ticker = (
        market.get("ticker")
        or ""
    )

    title = (
        market.get("title")
        or ""
    ).lower()

    floor_strike = safe_float(
        market.get("floor_strike")
    )

    cap_strike = safe_float(
        market.get("cap_strike")
    )

    if floor_strike is not None and (
        cap_strike is None
    ):

        return {
            "type": "greater",
            "floor": floor_strike,
            "cap": None,
            "label": (
                f"{int(floor_strike) + 1}"
                f"°F or higher"
            ),
        }

    if cap_strike is not None and (
        floor_strike is None
    ):

        return {
            "type": "less",
            "floor": None,
            "cap": cap_strike,
            "label": (
                f"{int(cap_strike) - 1}"
                f"°F or lower"
            ),
        }

    if (
        floor_strike is not None
        and cap_strike is not None
    ):

        return {
            "type": "between",
            "floor": floor_strike,
            "cap": cap_strike,
            "label": (
                f"{int(floor_strike)}°F "
                f"to {int(cap_strike)}°F"
            ),
        }

    # Fallback based on ticker/title.

    if "-T" in ticker:

        try:

            strike_text = ticker.split(
                "-T"
            )[-1]

            strike = float(
                strike_text
            )

            if ">" in title:
                return {
                    "type": "greater",
                    "floor": strike,
                    "cap": None,
                    "label": (
                        f"{int(strike) + 1}"
                        f"°F or higher"
                    ),
                }

            if "<" in title:
                return {
                    "type": "less",
                    "floor": None,
                    "cap": strike,
                    "label": (
                        f"{int(strike) - 1}"
                        f"°F or lower"
                    ),
                }

        except Exception:
            pass

    return None


# ==========================================================
# ENSEMBLE PROBABILITY
# ==========================================================

def calculate_probability(
    member_highs,
    strike
):
    """
    IMPORTANT:

    This is a RAW ENSEMBLE FREQUENCY.

    It is NOT a calibrated production probability model.

    Example:

    30 ensemble members
    18 satisfy the condition

    Raw frequency = 60%
    """

    if not member_highs:
        return 0.0

    strike_type = strike["type"]

    matches = 0

    for temperature in member_highs:

        if strike_type == "greater":

            # Example:
            # >104 means settlement 105 or higher.
            if temperature > strike["floor"]:
                matches += 1

        elif strike_type == "less":

            # Example:
            # <97 means settlement 96 or lower.
            if temperature < strike["cap"]:
                matches += 1

        elif strike_type == "between":

            if (
                temperature >= strike["floor"]
                and temperature <= strike["cap"]
            ):
                matches += 1

    return (
        matches / len(member_highs)
    ) * 100.0


# ==========================================================
# FORECAST STATE
# ==========================================================

def get_state_key(
    series,
    forecast_date
):
    return (
        f"{series}|{forecast_date}"
    )


def check_forecast_change(
    series,
    forecast_date,
    point_high,
    precipitation
):
    """
    Returns:

    is_first_observation
    temperature_change
    precipitation_change
    """

    state_key = get_state_key(
        series,
        forecast_date
    )

    with state_lock:

        previous = forecast_state.get(
            state_key
        )

        current = {
            "point_high": point_high,
            "precipitation": precipitation,
            "updated_at": (
                now_utc().isoformat()
            ),
        }

        forecast_state[
            state_key
        ] = current

    if previous is None:

        save_state()

        return {
            "first": True,
            "temperature_change": 0.0,
            "precipitation_change": 0.0,
        }

    previous_high = safe_float(
        previous.get("point_high"),
        point_high
    )

    previous_precip = safe_float(
        previous.get("precipitation"),
        precipitation
    )

    temperature_change = (
        point_high - previous_high
    )

    precipitation_change = (
        precipitation - previous_precip
    )

    save_state()

    return {
        "first": False,
        "temperature_change": (
            temperature_change
        ),
        "precipitation_change": (
            precipitation_change
        ),
    }


# ==========================================================
# KALSHI LINK
# ==========================================================

def get_kalshi_link(
    series_ticker
):
    return (
        "https://kalshi.com/markets/"
        f"{series_ticker.lower()}"
    )


# ==========================================================
# DISCORD MESSAGE
# ==========================================================

def build_discord_message(
    city_name,
    forecast_date,
    point_high,
    precipitation,
    change_info,
    opportunity,
    series_ticker
):

    direction = "unchanged"

    temperature_change = (
        change_info[
            "temperature_change"
        ]
    )

    if temperature_change > 0:
        direction = (
            f"up {temperature_change:.1f}°F"
        )

    elif temperature_change < 0:
        direction = (
            f"down {abs(temperature_change):.1f}°F"
        )

    return (
        "🌦️ **WEATHER FORECAST CHANGE**\n\n"

        f"**{city_name} — "
        f"{forecast_date}**\n"

        f"Projected high: "
        f"**{point_high:.1f}°F**\n"

        f"Forecast change: "
        f"**{direction}**\n"

        f"Forecast precipitation: "
        f"**{precipitation:.2f} in**\n\n"

        f"🎯 **Potential paper-trade "
        f"opportunity**\n"

        f"Contract: "
        f"**{opportunity['label']}**\n"

        f"Raw ensemble frequency: "
        f"**{opportunity['probability']:.1f}%**\n"

        f"YES ask: "
        f"**{opportunity['ask']:.1f}¢**\n"

        f"Preliminary edge: "
        f"**+{opportunity['edge']:.1f} points**\n"

        f"Ticker: "
        f"`{opportunity['ticker']}`\n\n"

        f"Kalshi: "
        f"{get_kalshi_link(series_ticker)}\n\n"

        "⚠️ Paper-trading signal only. "
        "Raw ensemble frequency is not a "
        "guaranteed or calibrated probability."
    )


# ==========================================================
# ANALYZE ONE CITY
# ==========================================================

def analyze_city(
    series_ticker,
    city
):

    global bot_status

    city_name = city["name"]

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
        city
    )

    ensemble_forecasts = (
        get_ensemble_forecast(
            city
        )
    )

    markets = get_kalshi_markets(
        series_ticker
    )

    if not point_forecasts:
        print(
            "No point forecasts available.",
            flush=True
        )
        return

    if not ensemble_forecasts:
        print(
            "No ensemble forecasts available.",
            flush=True
        )
        return

    if not markets:
        print(
            "No Kalshi markets available.",
            flush=True
        )
        return

    today = date.today().isoformat()

    markets_by_date = defaultdict(
        list
    )

    for market in markets:

        ticker = (
            market.get("ticker")
            or ""
        )

        market_date = parse_market_date(
            ticker
        )

        if not market_date:
            continue

        if market_date < today:

            print(
                f"Ignoring stale date: "
                f"{market_date} | {ticker}",
                flush=True
            )

            continue

        if market_date not in point_forecasts:

            print(
                f"Ignoring date with no "
                f"point forecast: "
                f"{market_date} | {ticker}",
                flush=True
            )

            continue

        if market_date not in ensemble_forecasts:

            print(
                f"Ignoring date with no "
                f"ensemble forecast: "
                f"{market_date} | {ticker}",
                flush=True
            )

            continue

        markets_by_date[
            market_date
        ].append(
            market
        )

    for forecast_date in sorted(
        markets_by_date.keys()
    ):

        city_markets = (
            markets_by_date[
                forecast_date
            ]
        )

        point_data = (
            point_forecasts[
                forecast_date
            ]
        )

        ensemble_data = (
            ensemble_forecasts[
                forecast_date
            ]
        )

        point_high = (
            point_data.get("high")
        )

        precipitation = (
            point_data.get(
                "precipitation",
                0.0
            )
        )

        member_highs = (
            ensemble_data.get(
                "member_highs",
                []
            )
        )

        if point_high is None:
            continue

        print(
            "",
            flush=True
        )

        print(
            "..................................................",
            flush=True
        )

        print(
            f"DATE: {forecast_date}",
            flush=True
        )

        print(
            f"POINT FORECAST HIGH: "
            f"{point_high:.2f}°F",
            flush=True
        )

        print(
            f"POINT FORECAST PRECIPITATION: "
            f"{precipitation:.2f} inches",
            flush=True
        )

        print(
            f"ENSEMBLE MEMBERS: "
            f"{len(member_highs)}",
            flush=True
        )

        if member_highs:

            ensemble_min = min(
                member_highs
            )

            ensemble_max = max(
                member_highs
            )

            ensemble_mean = statistics.mean(
                member_highs
            )

            print(
                f"ENSEMBLE MINIMUM: "
                f"{ensemble_min:.2f}°F",
                flush=True
            )

            print(
                f"ENSEMBLE MAXIMUM: "
                f"{ensemble_max:.2f}°F",
                flush=True
            )

            print(
                f"ENSEMBLE MEAN: "
                f"{ensemble_mean:.2f}°F",
                flush=True
            )

        validation = (
            validate_weather_alignment(
                point_high,
                ensemble_data
            )
        )

        if validation["gap_f"] is not None:

            print(
                f"POINT/ENSEMBLE GAP: "
                f"{validation['gap_f']:.2f}°F",
                flush=True
            )

        print(
            f"WEATHER DATA VALIDATION: "
            f"{validation['reason']}",
            flush=True
        )

        if not validation["valid"]:

            print(
                "ALERT SAFETY GATE ACTIVE: "
                "ensemble probabilities will be "
                "logged for diagnostics, but "
                "Discord paper-trade alerts are "
                "blocked for this city/date.",
                flush=True
            )

        change_info = (
            check_forecast_change(
                series_ticker,
                forecast_date,
                point_high,
                precipitation
            )
        )

        if change_info["first"]:

            print(
                "FORECAST STATUS: "
                "FIRST OBSERVATION",
                flush=True
            )

            print(
                "Baseline stored.",
                flush=True
            )

        else:

            print(
                f"FORECAST CHANGE: "
                f"{change_info['temperature_change']:+.2f}°F",
                flush=True
            )

            print(
                f"PRECIPITATION CHANGE: "
                f"{change_info['precipitation_change']:+.2f} in",
                flush=True
            )

            if (
                abs(
                    change_info[
                        "temperature_change"
                    ]
                )
                >= MIN_FORECAST_CHANGE_F
            ):

                bot_status[
                    "forecast_changes"
                ] += 1

        opportunities = []

        for market in city_markets:

            bot_status[
                "markets_checked"
            ] += 1

            ticker = (
                market.get("ticker")
                or ""
            )

            strike = get_market_strike(
                market
            )

            if strike is None:
                continue

            ask = cents_from_market(
                market
            )

            if ask is None:
                continue

            probability = (
                calculate_probability(
                    member_highs,
                    strike
                )
            )

            edge = (
                probability - ask
            )

            opportunity = {
                "ticker": ticker,
                "label": strike["label"],
                "probability": probability,
                "ask": ask,
                "edge": edge,
            }

            opportunities.append(
                opportunity
            )

        opportunities.sort(
            key=lambda item: item["edge"],
            reverse=True
        )

        print(
            "",
            flush=True
        )

        print(
            f"TOP OPPORTUNITIES: "
            f"{city_name} | "
            f"{forecast_date}",
            flush=True
        )

        for index, opportunity in enumerate(
            opportunities[:5],
            start=1
        ):

            print(
                f"{index}. "
                f"{opportunity['label']} | "
                f"Raw ensemble: "
                f"{opportunity['probability']:.1f}% | "
                f"Ask: "
                f"{opportunity['ask']:.1f}¢ | "
                f"Edge: "
                f"{opportunity['edge']:+.1f} points | "
                f"{opportunity['ticker']}",
                flush=True
            )

        positive = [
            opportunity
            for opportunity in opportunities
            if opportunity["edge"]
            >= MIN_EDGE_POINTS
        ]

        bot_status[
            "positive_signals"
        ] += len(positive)

        # --------------------------------------------------
        # DISCORD ALERT RULES
        # --------------------------------------------------

        if change_info["first"]:

            print(
                "No Discord alert: "
                "first observation.",
                flush=True
            )

            continue

        temperature_changed = (
            abs(
                change_info[
                    "temperature_change"
                ]
            )
            >= MIN_FORECAST_CHANGE_F
        )

        if not temperature_changed:

            print(
                "No Discord alert: "
                "forecast change below threshold.",
                flush=True
            )

            continue

        if not validation["valid"]:

            print(
                "No Discord alert: "
                "weather validation failed.",
                flush=True
            )

            continue

        if not positive:

            print(
                "No Discord alert: "
                "no opportunity meets "
                "minimum edge threshold.",
                flush=True
            )

            continue

        best = positive[0]

        message = build_discord_message(
            city_name,
            forecast_date,
            point_high,
            precipitation,
            change_info,
            best,
            series_ticker
        )

        print(
            "",
            flush=True
        )

        print(
            "DISCORD PAPER SIGNAL:",
            flush=True
        )

        print(
            message,
            flush=True
        )

        sent = send_discord_alert(
            message
        )

        if sent:

            bot_status[
                "discord_alerts"
            ] += 1


# ==========================================================
# MAIN SCAN
# ==========================================================

def run_weather_scan():

    global bot_status

    bot_status["last_scan_utc"] = (
        now_utc().isoformat()
    )

    bot_status["series_checked"] = 0
    bot_status["markets_checked"] = 0
    bot_status["forecast_changes"] = 0
    bot_status["positive_signals"] = 0
    bot_status["discord_alerts"] = 0
    bot_status["last_error"] = None

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
        f"UTC: "
        f"{now_utc().isoformat()}",
        flush=True
    )

    print(
        "==================================================",
        flush=True
    )

    for series_ticker, city in (
        CITIES.items()
    ):

        try:

            bot_status[
                "series_checked"
            ] += 1

            analyze_city(
                series_ticker,
                city
            )

        except Exception as error:

            print(
                f"ERROR ANALYZING "
                f"{series_ticker}: {error}",
                flush=True
            )

            bot_status[
                "last_error"
            ] = str(error)

    bot_status[
        "last_scan_success"
    ] = now_utc().isoformat()

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
        f"{bot_status['series_checked']}",
        flush=True
    )

    print(
        f"Markets checked: "
        f"{bot_status['markets_checked']}",
        flush=True
    )

    print(
        f"Forecast changes: "
        f"{bot_status['forecast_changes']}",
        flush=True
    )

    print(
        f"Positive preliminary signals: "
        f"{bot_status['positive_signals']}",
        flush=True
    )

    print(
        f"Discord alerts sent: "
        f"{bot_status['discord_alerts']}",
        flush=True
    )

    print(
        "==================================================",
        flush=True
    )


# ==========================================================
# BACKGROUND SCANNER
# ==========================================================

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
                f"Background scanner error: "
                f"{error}",
                flush=True
            )

            bot_status[
                "last_error"
            ] = str(error)

        print(
            f"Waiting "
            f"{SCAN_INTERVAL_SECONDS} seconds...",
            flush=True
        )

        time.sleep(
            SCAN_INTERVAL_SECONDS
        )


# ==========================================================
# FLASK ROUTES
# ==========================================================

@app.route("/")
def home():

    return (
        "Weather + Kalshi paper-trading "
        "monitor is running."
    )


@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "bot": bot_status,
        "discord_configured": bool(
            DISCORD_WEBHOOK_URL
        ),
        "cities": list(
            CITIES.keys()
        ),
    })


@app.route("/status")
def status():

    return jsonify(
        bot_status
    )


@app.route("/test-alert")
def test_alert():

    if not DISCORD_WEBHOOK_URL:

        return (
            "Discord webhook is not configured. "
            "Set DISCORD_WEBHOOK_URL in Render.",
            500
        )

    message = (
        "🧪 **WEATHER BOT TEST ALERT**\n\n"
        "Your Discord webhook is working.\n"
        "This is a test message only.\n"
        "No trade signal was generated."
    )

    success = send_discord_alert(
        message
    )

    if success:

        return (
            "Test Discord alert sent successfully!"
        )

    return (
        "Discord alert failed. "
        "Check Render logs.",
        500
    )


# ==========================================================
# START APPLICATION
# ==========================================================

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
