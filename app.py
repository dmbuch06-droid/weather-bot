```python
import os
import time
import json
import threading
import statistics
import hashlib
from datetime import datetime, timezone
from collections import defaultdict

import requests
from flask import Flask, jsonify


# ==========================================================
# CONFIGURATION
# ==========================================================

app = Flask(__name__)

KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))
POINT_REFRESH_SECONDS = int(os.environ.get("POINT_REFRESH_SECONDS", "1800"))
ENSEMBLE_REFRESH_SECONDS = int(os.environ.get("ENSEMBLE_REFRESH_SECONDS", "1800"))
KALSHI_REFRESH_SECONDS = int(os.environ.get("KALSHI_REFRESH_SECONDS", "300"))

MIN_FORECAST_CHANGE_F = float(os.environ.get("MIN_FORECAST_CHANGE_F", "1.0"))
MIN_EDGE_POINTS = float(os.environ.get("MIN_EDGE_POINTS", "5.0"))
MAX_POINT_ENSEMBLE_GAP_F = float(
    os.environ.get("MAX_POINT_ENSEMBLE_GAP_F", "6.0")
)
MAX_POINT_CACHE_AGE_SECONDS = int(
    os.environ.get("MAX_POINT_CACHE_AGE_SECONDS", "21600")
)
MAX_ENSEMBLE_CACHE_AGE_SECONDS = int(
    os.environ.get("MAX_ENSEMBLE_CACHE_AGE_SECONDS", "21600")
)

STATE_FILE = os.environ.get("STATE_FILE", "forecast_state.json")
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))
FORECAST_DAYS = int(os.environ.get("FORECAST_DAYS", "7"))

# Render's local filesystem can disappear on restart/redeploy.
# This JSON state is still useful during a running instance and for persistent
# disks, but should eventually be replaced by Postgres/Redis for production.
STATE_VERSION = 2


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
# GLOBAL STATE
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
    "state_persistence_warning": None,
}

state_lock = threading.RLock()

# Persisted:
# {
#   "version": 2,
#   "forecasts": {...},
#   "cache": {...},
#   "cooldowns": {...},
#   "alerts": {...},
#   "paper_trades": [...]
# }
persistent_state = {}


# ==========================================================
# BASIC HELPERS
# ==========================================================

def now_utc():
    return datetime.now(timezone.utc)


def utc_iso():
    return now_utc().isoformat()


def safe_float(value, default=None):
    if value is None:
        return default

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_iso_datetime(value):
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.astimezone(timezone.utc)

    except Exception:
        return None


def age_seconds(timestamp):
    parsed = parse_iso_datetime(timestamp)

    if parsed is None:
        return None

    return max(
        0.0,
        (now_utc() - parsed).total_seconds()
    )


def today_for_city(city):
    # Open-Meteo returns daily dates in the requested local timezone.
    # Avoid server-local date comparisons.
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(
            ZoneInfo(city["timezone"])
        ).date().isoformat()

    except Exception:
        return now_utc().date().isoformat()


def ensure_state_shape():
    global persistent_state

    if not isinstance(persistent_state, dict):
        persistent_state = {}

    persistent_state.setdefault(
        "version",
        STATE_VERSION
    )
    persistent_state.setdefault(
        "forecasts",
        {}
    )
    persistent_state.setdefault(
        "cache",
        {}
    )
    persistent_state.setdefault(
        "cooldowns",
        {}
    )
    persistent_state.setdefault(
        "alerts",
        {}
    )
    persistent_state.setdefault(
        "paper_trades",
        []
    )

    # Migrate the old flat state format automatically.
    old_keys = [
        key
        for key in list(persistent_state.keys())
        if "|" in str(key)
        and isinstance(
            persistent_state.get(key),
            dict
        )
    ]

    if old_keys:
        for key in old_keys:
            persistent_state["forecasts"][key] = (
                persistent_state[key]
            )

            del persistent_state[key]

        persistent_state["version"] = STATE_VERSION


def cache_key(source, series_ticker):
    return f"{source}|{series_ticker}"


def cooldown_key(source, series_ticker):
    return f"{source}|{series_ticker}"


def log_json(prefix, value):
    try:
        print(
            f"{prefix}: "
            f"{json.dumps(value, default=str)[:2000]}",
            flush=True,
        )
    except Exception:
        print(
            f"{prefix}: {value}",
            flush=True,
        )


# ==========================================================
# STATE MANAGEMENT
# ==========================================================

def load_state():
    global persistent_state

    try:
        if not os.path.exists(STATE_FILE):
            persistent_state = {}

            ensure_state_shape()

            print(
                "No previous state file found. Starting fresh.",
                flush=True,
            )

            return

        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as file:
            persistent_state = json.load(file)

        ensure_state_shape()

        print(
            f"Loaded state: "
            f"{len(persistent_state['forecasts'])} "
            f"forecast entries, "
            f"{len(persistent_state['cache'])} "
            f"cache entries, "
            f"{len(persistent_state['paper_trades'])} "
            f"paper trades.",
            flush=True,
        )

    except Exception as error:
        print(
            f"State load error: {error}",
            flush=True,
        )

        persistent_state = {}

        ensure_state_shape()


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
                    persistent_state,
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
            flush=True,
        )

        bot_status[
            "state_persistence_warning"
        ] = str(error)


# ==========================================================
# HTTP / RATE LIMIT HELPERS
# ==========================================================

def source_in_cooldown(
    source,
    series_ticker
):
    key = cooldown_key(
        source,
        series_ticker
    )

    entry = persistent_state[
        "cooldowns"
    ].get(
        key,
        {}
    )

    until = parse_iso_datetime(
        entry.get("until")
    )

    if until and now_utc() < until:
        remaining = int(
            (until - now_utc()).total_seconds()
        )

        return True, remaining, entry

    if key in persistent_state["cooldowns"]:
        persistent_state[
            "cooldowns"
        ].pop(key, None)

        save_state()

    return False, 0, {}


def set_cooldown(
    source,
    series_ticker,
    seconds,
    reason
):
    from datetime import timedelta

    key = cooldown_key(
        source,
        series_ticker
    )

    previous = persistent_state[
        "cooldowns"
    ].get(
        key,
        {}
    )

    failures = int(
        previous.get("failures", 0)
    ) + 1

    persistent_state[
        "cooldowns"
    ][key] = {
        "until": (
            now_utc()
            + timedelta(seconds=seconds)
        ).isoformat(),
        "reason": reason,
        "failures": failures,
        "updated_at": utc_iso(),
    }

    save_state()


def clear_cooldown(
    source,
    series_ticker
):
    key = cooldown_key(
        source,
        series_ticker
    )

    if key in persistent_state["cooldowns"]:
        persistent_state[
            "cooldowns"
        ].pop(key, None)

        save_state()


def get_cache(
    source,
    series_ticker
):
    entry = persistent_state[
        "cache"
    ].get(
        cache_key(
            source,
            series_ticker
        )
    )

    if not isinstance(entry, dict):
        return None

    return entry


def set_cache(
    source,
    series_ticker,
    data,
    metadata=None
):
    persistent_state[
        "cache"
    ][
        cache_key(
            source,
            series_ticker
        )
    ] = {
        "source": source,
        "series": series_ticker,
        "retrieved_at": utc_iso(),
        "data": data,
        "metadata": metadata or {},
    }

    save_state()


def cache_is_fresh(
    entry,
    refresh_seconds
):
    if not entry:
        return False

    age = age_seconds(
        entry.get("retrieved_at")
    )

    return (
        age is not None
        and age < refresh_seconds
    )


def cache_is_usable(
    entry,
    max_age_seconds
):
    if not entry:
        return False

    age = age_seconds(
        entry.get("retrieved_at")
    )

    return (
        age is not None
        and age <= max_age_seconds
    )


def cached_result(
    entry,
    status
):
    if not entry:
        return {}, {
            "status": "missing",
            "retrieved_at": None,
            "age_seconds": None,
            "metadata": {},
        }

    return entry.get("data", {}), {
        "status": status,
        "retrieved_at": entry.get(
            "retrieved_at"
        ),
        "age_seconds": age_seconds(
            entry.get("retrieved_at")
        ),
        "metadata": entry.get(
            "metadata",
            {}
        ),
    }


# ==========================================================
# DISCORD
# ==========================================================

def send_discord_alert(message):
    if not DISCORD_WEBHOOK_URL:
        print(
            "Discord webhook is not configured.",
            flush=True,
        )

        return False

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "WeatherMarketMonitor/1.0",
    }

    payload = {
        "content": message,
    }

    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

        print(
            f"Discord response status: "
            f"{response.status_code}",
            flush=True,
        )

        print(
            f"Discord response content-type: "
            f"{response.headers.get('content-type')}",
            flush=True,
        )

        if 200 <= response.status_code < 300:
            print(
                "Discord alert sent successfully.",
                flush=True,
            )

            return True

        # Discord's normal rate-limit response.
        if response.status_code == 429:
            content_type = response.headers.get(
                "content-type",
                ""
            ).lower()

            print(
                "Discord returned HTTP 429.",
                flush=True,
            )

            if "application/json" in content_type:
                try:
                    error_data = response.json()

                    retry_after = error_data.get(
                        "retry_after"
                    )

                    print(
                        f"Discord rate limit details: "
                        f"{error_data}",
                        flush=True,
                    )

                    if retry_after is not None:
                        print(
                            f"Retry after: "
                            f"{retry_after} seconds",
                            flush=True,
                        )

                except Exception as error:
                    print(
                        f"Could not parse "
                        f"Discord 429 JSON: {error}",
                        flush=True,
                    )

            else:
                print(
                    "HTTP 429 did not return Discord JSON. "
                    "This may be a Cloudflare/network block.",
                    flush=True,
                )

                print(
                    f"Response body: "
                    f"{response.text[:1000]}",
                    flush=True,
                )

            return False

        print(
            f"Discord error body: "
            f"{response.text[:1000]}",
            flush=True,
        )

        return False

    except requests.exceptions.Timeout:
        print(
            "Discord webhook request timed out.",
            flush=True,
        )

        return False

    except requests.exceptions.RequestException as error:
        print(
            f"Discord webhook request error: "
            f"{error}",
            flush=True,
        )

        return False

    except Exception as error:
        print(
            f"Unexpected Discord webhook error: "
            f"{error}",
            flush=True,
        )

        return False


# ==========================================================
# POINT FORECAST
# ==========================================================

def fetch_point_forecast(city):
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
        "forecast_days": FORECAST_DAYS,
    }

    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params=params,
        timeout=REQUEST_TIMEOUT,
    )

    print(
        f"Point forecast status: "
        f"{response.status_code}",
        flush=True,
    )

    if response.status_code == 429:
        raise RuntimeError(
            "RATE_LIMIT_429|"
            + response.text[:500]
        )

    if response.status_code != 200:
        raise RuntimeError(
            f"HTTP_{response.status_code}|"
            f"{response.text[:500]}"
        )

    data = response.json()

    daily = data.get(
        "daily",
        {}
    )

    dates = daily.get(
        "time",
        []
    )

    highs = daily.get(
        "temperature_2m_max",
        []
    )

    precipitation = daily.get(
        "precipitation_sum",
        []
    )

    result = {}

    for index, forecast_date in enumerate(
        dates
    ):
        high = (
            safe_float(highs[index])
            if index < len(highs)
            else None
        )

        precip = (
            safe_float(
                precipitation[index]
            )
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

    metadata = {
        "api": "open-meteo-forecast",
        "timezone": city["timezone"],
        "latitude": city["lat"],
        "longitude": city["lon"],
        "forecast_days": FORECAST_DAYS,
    }

    return result, metadata


def get_point_forecast(
    series_ticker,
    city
):
    source = "point"

    entry = get_cache(
        source,
        series_ticker
    )

    if cache_is_fresh(
        entry,
        POINT_REFRESH_SECONDS
    ):
        data, info = cached_result(
            entry,
            "fresh_cache"
        )

        print(
            f"Point forecast: using scheduled cache "
            f"({info['age_seconds']:.0f}s old).",
            flush=True,
        )

        return data, info

    cooling, remaining, cooldown = (
        source_in_cooldown(
            source,
            series_ticker
        )
    )

    if cooling:
        if cache_is_usable(
            entry,
            MAX_POINT_CACHE_AGE_SECONDS
        ):
            data, info = cached_result(
                entry,
                "cooldown_cache"
            )

            info[
                "cooldown_remaining_seconds"
            ] = remaining

            print(
                f"Point forecast cooldown active "
                f"({remaining}s remaining). "
                f"Using cached data.",
                flush=True,
            )

            return data, info

        print(
            f"Point forecast cooldown active "
            f"({remaining}s remaining) "
            f"and no safe cache.",
            flush=True,
        )

        return {}, {
            "status": "rate_limited_no_cache",
            "retrieved_at": None,
            "age_seconds": None,
            "metadata": {},
        }

    try:
        data, metadata = fetch_point_forecast(
            city
        )

        set_cache(
            source,
            series_ticker,
            data,
            metadata
        )

        clear_cooldown(
            source,
            series_ticker
        )

        entry = get_cache(
            source,
            series_ticker
        )

        data, info = cached_result(
            entry,
            "fresh_api"
        )

        return data, info

    except Exception as error:
        message = str(error)

        print(
            f"Point forecast error: {message}",
            flush=True,
        )

        if message.startswith(
            "RATE_LIMIT_429|"
        ):
            set_cooldown(
                source,
                series_ticker,
                6 * 3600,
                (
                    "HTTP 429 from point "
                    "forecast provider"
                ),
            )

        else:
            set_cooldown(
                source,
                series_ticker,
                15 * 60,
                (
                    f"point forecast error: "
                    f"{message[:120]}"
                ),
            )

        if cache_is_usable(
            entry,
            MAX_POINT_CACHE_AGE_SECONDS
        ):
            data, info = cached_result(
                entry,
                "stale_cache_after_error"
            )

            info["error"] = message

            return data, info

        return {}, {
            "status": "failed_no_cache",
            "retrieved_at": None,
            "age_seconds": None,
            "metadata": {},
            "error": message,
        }


# ==========================================================
# ENSEMBLE FORECAST
# ==========================================================

def fetch_ensemble_forecast(city):
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "hourly": "temperature_2m",
        "temperature_unit": "fahrenheit",
        "timezone": city["timezone"],
        "forecast_days": FORECAST_DAYS,
    }

    url = (
        "https://ensemble-api.open-meteo.com/"
        "v1/ensemble"
    )

    response = requests.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )

    print(
        f"Ensemble API status: "
        f"{response.status_code}",
        flush=True,
    )

    if response.status_code == 429:
        raise RuntimeError(
            "RATE_LIMIT_429|"
            + response.text[:500]
        )

    if response.status_code != 200:
        raise RuntimeError(
            f"HTTP_{response.status_code}|"
            f"{response.text[:500]}"
        )

    data = response.json()

    hourly = data.get(
        "hourly",
        {}
    )

    timestamps = hourly.get(
        "time",
        []
    )

    member_keys = sorted(
        key
        for key in hourly.keys()
        if key.startswith(
            "temperature_2m_member"
        )
    )

    print(
        f"Ensemble member temperature keys found: "
        f"{len(member_keys)}",
        flush=True,
    )

    if not timestamps or not member_keys:
        return {}, {
            "api": "open-meteo-ensemble",
            "warning": (
                "no timestamps or "
                "ensemble members"
            ),
        }

    # Each timestamp is already returned in the requested city timezone.
    # Group by YYYY-MM-DD and calculate each member's local-calendar daily max.
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

            forecast_date = str(
                timestamp
            )[:10]

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

            if values:
                member_highs.append(
                    max(values)
                )

        if member_highs:
            result[forecast_date] = {
                "member_highs": member_highs,
                "member_count": len(
                    member_highs
                ),
                "mean": statistics.mean(
                    member_highs
                ),
                "median": statistics.median(
                    member_highs
                ),
                "minimum": min(
                    member_highs
                ),
                "maximum": max(
                    member_highs
                ),
            }

    print(
        f"Ensemble dates available: "
        f"{len(result)}",
        flush=True,
    )

    metadata = {
        "api": "open-meteo-ensemble",
        "timezone": city["timezone"],
        "latitude": city["lat"],
        "longitude": city["lon"],
        "forecast_days": FORECAST_DAYS,
        "member_keys_found": len(
            member_keys
        ),
        "retrieved_utc": utc_iso(),
        "model_run_time": None,
    }

    return result, metadata


def get_ensemble_forecast(
    series_ticker,
    city
):
    source = "ensemble"

    entry = get_cache(
        source,
        series_ticker
    )

    if cache_is_fresh(
        entry,
        ENSEMBLE_REFRESH_SECONDS
    ):
        data, info = cached_result(
            entry,
            "fresh_cache"
        )

        print(
            f"Ensemble forecast: using scheduled "
            f"cache ({info['age_seconds']:.0f}s old).",
            flush=True,
        )

        return data, info

    cooling, remaining, cooldown = (
        source_in_cooldown(
            source,
            series_ticker
        )
    )

    if cooling:
        if cache_is_usable(
            entry,
            MAX_ENSEMBLE_CACHE_AGE_SECONDS
        ):
            data, info = cached_result(
                entry,
                "cooldown_cache"
            )

            info[
                "cooldown_remaining_seconds"
            ] = remaining

            print(
                f"Ensemble cooldown active "
                f"({remaining}s remaining). "
                f"Using cached data.",
                flush=True,
            )

            return data, info

        return {}, {
            "status": "rate_limited_no_cache",
            "retrieved_at": None,
            "age_seconds": None,
            "metadata": {},
        }

    try:
        data, metadata = fetch_ensemble_forecast(
            city
        )

        set_cache(
            source,
            series_ticker,
            data,
            metadata
        )

        clear_cooldown(
            source,
            series_ticker
        )

        entry = get_cache(
            source,
            series_ticker
        )

        return cached_result(
            entry,
            "fresh_api"
        )

    except Exception as error:
        message = str(error)

        print(
            f"Ensemble forecast error: "
            f"{message}",
            flush=True,
        )

        if message.startswith(
            "RATE_LIMIT_429|"
        ):
            set_cooldown(
                source,
                series_ticker,
                6 * 3600,
                (
                    "HTTP 429 from ensemble "
                    "provider"
                ),
            )

        else:
            set_cooldown(
                source,
                series_ticker,
                15 * 60,
                (
                    f"ensemble error: "
                    f"{message[:120]}"
                ),
            )

        if cache_is_usable(
            entry,
            MAX_ENSEMBLE_CACHE_AGE_SECONDS
        ):
            data, info = cached_result(
                entry,
                "stale_cache_after_error"
            )

            info["error"] = message

            return data, info

        return {}, {
            "status": "failed_no_cache",
            "retrieved_at": None,
            "age_seconds": None,
            "metadata": {},
            "error": message,
        }


# ==========================================================
# WEATHER VALIDATION
# ==========================================================

def validate_weather_alignment(
    point_high,
    ensemble_data,
    point_info,
    ensemble_info
):
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
            "reason": (
                "too few ensemble members"
            ),
            "ensemble_mean": None,
            "gap_f": None,
        }

    ensemble_mean = statistics.mean(
        member_highs
    )

    gap_f = abs(
        point_high
        - ensemble_mean
    )

    # Stale data may be used for diagnostics but should not create new alerts.
    if point_info.get(
        "status"
    ) not in (
        "fresh_api",
        "fresh_cache"
    ):
        return {
            "valid": False,
            "reason": (
                f"point forecast not fresh "
                f"({point_info.get('status')})"
            ),
            "ensemble_mean": ensemble_mean,
            "gap_f": gap_f,
        }

    if ensemble_info.get(
        "status"
    ) not in (
        "fresh_api",
        "fresh_cache"
    ):
        return {
            "valid": False,
            "reason": (
                f"ensemble forecast not fresh "
                f"({ensemble_info.get('status')})"
            ),
            "ensemble_mean": ensemble_mean,
            "gap_f": gap_f,
        }

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

def get_kalshi_markets(
    series_ticker
):
    try:
        params = {
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 200,
        }

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

            return []

        data = response.json()

        markets = data.get(
            "markets",
            []
        )

        print(
            f"Markets found: "
            f"{len(markets)}",
            flush=True,
        )

        return markets

    except Exception as error:
        print(
            f"Kalshi API error for "
            f"{series_ticker}: {error}",
            flush=True,
        )

        return []


def cents_from_market(market):
    for field in (
        "yes_ask_dollars",
        "yes_ask_price_dollars"
    ):
        value = safe_float(
            market.get(field)
        )

        if (
            value is not None
            and 0 <= value <= 1
        ):
            return value * 100.0

    for field in (
        "yes_ask",
        "yes_ask_price"
    ):
        value = safe_float(
            market.get(field)
        )

        if value is not None:
            if 0 <= value <= 1:
                return value * 100.0

            return value

    return None


# ==========================================================
# MARKET PARSING
# ==========================================================

def parse_market_date(ticker):
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


def get_market_strike(market):
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

    if (
        floor_strike is not None
        and cap_strike is None
    ):
        return {
            "type": "greater",
            "floor": floor_strike,
            "cap": None,
            "label": (
                f">{floor_strike:g}°F"
            ),
        }

    if (
        cap_strike is not None
        and floor_strike is None
    ):
        return {
            "type": "less",
            "floor": None,
            "cap": cap_strike,
            "label": (
                f"<{cap_strike:g}°F"
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
                f"{floor_strike:g}°F to "
                f"{cap_strike:g}°F"
            ),
        }

    # Keep fallback conservative. Do not guess settlement semantics.
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
                    "label": f">{strike:g}°F",
                }

            if "<" in title:
                return {
                    "type": "less",
                    "floor": None,
                    "cap": strike,
                    "label": f"<{strike:g}°F",
                }

        except Exception:
            pass

    return None


# ==========================================================
# RAW ENSEMBLE FREQUENCY
# ==========================================================

def calculate_probability(
    member_highs,
    strike
):
    if not member_highs:
        return 0.0

    matches = 0

    for temperature in member_highs:
        if strike["type"] == "greater":
            if (
                temperature
                > strike["floor"]
            ):
                matches += 1

        elif strike["type"] == "less":
            if (
                temperature
                < strike["cap"]
            ):
                matches += 1

        elif strike["type"] == "between":
            if (
                temperature
                >= strike["floor"]
                and temperature
                <= strike["cap"]
            ):
                matches += 1

    return (
        matches
        / len(member_highs)
    ) * 100.0


# ==========================================================
# FORECAST CHANGE STATE
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
    precipitation,
    ensemble_data,
    point_info,
    ensemble_info,
):
    state_key = get_state_key(
        series,
        forecast_date
    )

    ensemble_mean = safe_float(
        ensemble_data.get("mean")
    )

    if ensemble_mean is None:
        highs = ensemble_data.get(
            "member_highs",
            []
        )

        ensemble_mean = (
            statistics.mean(highs)
            if highs
            else None
        )

    current = {
        "point_high": point_high,
        "precipitation": precipitation,
        "ensemble_mean": ensemble_mean,
        "ensemble_member_count": len(
            ensemble_data.get(
                "member_highs",
                []
            )
        ),
        "point_retrieved_at": point_info.get(
            "retrieved_at"
        ),
        "ensemble_retrieved_at": ensemble_info.get(
            "retrieved_at"
        ),
        "point_status": point_info.get(
            "status"
        ),
        "ensemble_status": ensemble_info.get(
            "status"
        ),
        "updated_at": utc_iso(),
    }

    with state_lock:
        previous = persistent_state[
            "forecasts"
        ].get(
            state_key
        )

        persistent_state[
            "forecasts"
        ][state_key] = current

    save_state()

    if previous is None:
        return {
            "first": True,
            "temperature_change": 0.0,
            "precipitation_change": 0.0,
            "ensemble_mean_change": 0.0,
            "previous": None,
        }

    previous_high = safe_float(
        previous.get("point_high"),
        point_high,
    )

    previous_precip = safe_float(
        previous.get("precipitation"),
        precipitation,
    )

    previous_ensemble_mean = safe_float(
        previous.get("ensemble_mean"),
        ensemble_mean,
    )

    return {
        "first": False,
        "temperature_change": (
            point_high
            - previous_high
        ),
        "precipitation_change": (
            precipitation
            - previous_precip
        ),
        "ensemble_mean_change": (
            (
                ensemble_mean
                - previous_ensemble_mean
            )
            if ensemble_mean is not None
            and previous_ensemble_mean is not None
            else 0.0
        ),
        "previous": previous,
    }


# ==========================================================
# PAPER TRADES / ALERT DEDUPLICATION
# ==========================================================

def opportunity_fingerprint(
    series_ticker,
    forecast_date,
    opportunity,
    point_high,
    ensemble_mean,
):
    raw = "|".join([
        series_ticker,
        forecast_date,
        opportunity["ticker"],
        f"{point_high:.2f}",
        (
            f"{ensemble_mean:.2f}"
            if ensemble_mean is not None
            else "none"
        ),
        f"{opportunity['ask']:.2f}",
        f"{opportunity['probability']:.2f}",
    ])

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()[:20]


def was_recently_alerted(
    fingerprint
):
    entry = persistent_state[
        "alerts"
    ].get(
        fingerprint
    )

    if not entry:
        return False

    return True


def record_alert(
    fingerprint
):
    persistent_state[
        "alerts"
    ][fingerprint] = {
        "sent_at": utc_iso(),
    }

    # Keep alert history bounded.
    if (
        len(
            persistent_state["alerts"]
        )
        > 1000
    ):
        ordered = sorted(
            persistent_state[
                "alerts"
            ].items(),
            key=lambda item: item[1].get(
                "sent_at",
                ""
            ),
        )

        persistent_state[
            "alerts"
        ] = dict(
            ordered[-800:]
        )

    save_state()


def record_paper_trade(
    city_name,
    series_ticker,
    forecast_date,
    point_high,
    precipitation,
    change_info,
    ensemble_data,
    opportunity,
    point_info,
    ensemble_info,
):
    trade = {
        "timestamp_utc": utc_iso(),
        "city": city_name,
        "series": series_ticker,
        "forecast_date": forecast_date,
        "contract_ticker": opportunity[
            "ticker"
        ],
        "contract_interpretation": opportunity[
            "label"
        ],
        "forecast_before": (
            change_info.get(
                "previous",
                {}
            )
            or {}
        ).get(
            "point_high"
        ),
        "forecast_after": point_high,
        "precipitation": precipitation,
        "ensemble_mean": ensemble_data.get(
            "mean"
        ),
        "ensemble_median": ensemble_data.get(
            "median"
        ),
        "ensemble_member_count": len(
            ensemble_data.get(
                "member_highs",
                []
            )
        ),
        "yes_ask_cents": opportunity[
            "ask"
        ],
        "raw_ensemble_probability": opportunity[
            "probability"
        ],
        "preliminary_edge_points": opportunity[
            "edge"
        ],
        "point_source_status": point_info.get(
            "status"
        ),
        "ensemble_source_status": ensemble_info.get(
            "status"
        ),
        "point_retrieved_at": point_info.get(
            "retrieved_at"
        ),
        "ensemble_retrieved_at": ensemble_info.get(
            "retrieved_at"
        ),
        "settlement_result": None,
        "profit_loss_cents": None,
    }

    persistent_state[
        "paper_trades"
    ].append(trade)

    # Keep the JSON state from growing forever.
    if (
        len(
            persistent_state[
                "paper_trades"
            ]
        )
        > 2000
    ):
        persistent_state[
            "paper_trades"
        ] = persistent_state[
            "paper_trades"
        ][-1500:]

    save_state()


# ==========================================================
# DISCORD MESSAGE
# ==========================================================

def get_kalshi_link(
    series_ticker
):
    return (
        "https://kalshi.com/markets/"
        f"{series_ticker.lower()}"
    )


def build_discord_message(
    city_name,
    forecast_date,
    point_high,
    precipitation,
    change_info,
    opportunity,
    validation,
    series_ticker,
):
    temperature_change = (
        change_info[
            "temperature_change"
        ]
    )

    if temperature_change > 0:
        direction = (
            f"up "
            f"{temperature_change:.1f}°F"
        )

    elif temperature_change < 0:
        direction = (
            f"down "
            f"{abs(temperature_change):.1f}°F"
        )

    else:
        direction = "unchanged"

    return (
        "🌦️ **WEATHER FORECAST CHANGE — "
        "PAPER SIGNAL**\n\n"

        f"**{city_name} — "
        f"{forecast_date}**\n"

        f"Point forecast high: "
        f"**{point_high:.1f}°F**\n"

        f"Change since previous observation: "
        f"**{direction}**\n"

        f"Precipitation forecast: "
        f"**{precipitation:.2f} in**\n"

        f"Ensemble mean: "
        f"**{validation['ensemble_mean']:.1f}°F**\n"

        f"Point/ensemble gap: "
        f"**{validation['gap_f']:.1f}°F**\n\n"

        "🎯 **Potential opportunity**\n"

        f"Contract: "
        f"**{opportunity['label']}**\n"

        f"Ticker: "
        f"`{opportunity['ticker']}`\n"

        f"Raw ensemble frequency: "
        f"**{opportunity['probability']:.1f}%**\n"

        f"YES ask: "
        f"**{opportunity['ask']:.1f}¢**\n"

        f"Preliminary edge: "
        f"**+{opportunity['edge']:.1f} points**\n\n"

        f"Kalshi: "
        f"{get_kalshi_link(series_ticker)}\n\n"

        "⚠️ Paper-trading only. Raw ensemble "
        "frequency is not a calibrated probability "
        "and contract settlement rules must be validated."
    )


# ==========================================================
# ANALYZE ONE CITY
# ==========================================================

def analyze_city(
    series_ticker,
    city
):
    city_name = city["name"]

    print(
        "\n--------------------------------------------------",
        flush=True,
    )

    print(
        f"SERIES: {series_ticker}",
        flush=True,
    )

    print(
        f"CITY: {city_name}",
        flush=True,
    )

    # Weather sources now have independent refresh schedules and caches.
    point_forecasts, point_info = (
        get_point_forecast(
            series_ticker,
            city,
        )
    )

    ensemble_forecasts, ensemble_info = (
        get_ensemble_forecast(
            series_ticker,
            city,
        )
    )

    # Kalshi remains live on every scanner pass.
    markets = get_kalshi_markets(
        series_ticker
    )

    print(
        f"POINT SOURCE STATUS: "
        f"{point_info.get('status')} | "
        f"age={point_info.get('age_seconds')}",
        flush=True,
    )

    print(
        f"ENSEMBLE SOURCE STATUS: "
        f"{ensemble_info.get('status')} | "
        f"age={ensemble_info.get('age_seconds')}",
        flush=True,
    )

    if not markets:
        print(
            "No Kalshi markets available.",
            flush=True,
        )

        return

    if not point_forecasts:
        print(
            "No usable point forecast. Kalshi markets "
            "were fetched, but new temperature analysis "
            "is blocked safely.",
            flush=True,
        )

        return

    if not ensemble_forecasts:
        print(
            "No usable ensemble forecast. Kalshi markets "
            "were fetched, but probability analysis "
            "is blocked safely.",
            flush=True,
        )

        return

    today = today_for_city(city)

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
            continue

        if market_date not in point_forecasts:
            print(
                f"Ignoring date with no point forecast: "
                f"{market_date} | {ticker}",
                flush=True,
            )

            continue

        if (
            market_date
            not in ensemble_forecasts
        ):
            print(
                f"Ignoring date with no ensemble forecast: "
                f"{market_date} | {ticker}",
                flush=True,
            )

            continue

        markets_by_date[
            market_date
        ].append(market)

    if not markets_by_date:
        print(
            "No open Kalshi market dates overlap "
            "the current weather data.",
            flush=True,
        )

        return

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

        point_high = point_data.get(
            "high"
        )

        precipitation = point_data.get(
            "precipitation",
            0.0
        )

        member_highs = ensemble_data.get(
            "member_highs",
            []
        )

        if (
            point_high is None
            or not member_highs
        ):
            continue

        print(
            "\n..................................................",
            flush=True,
        )

        print(
            f"DATE: {forecast_date}",
            flush=True,
        )

        print(
            f"POINT FORECAST HIGH: "
            f"{point_high:.2f}°F",
            flush=True,
        )

        print(
            f"POINT FORECAST PRECIPITATION: "
            f"{precipitation:.2f} inches",
            flush=True,
        )

        print(
            f"ENSEMBLE MEMBERS: "
            f"{len(member_highs)}",
            flush=True,
        )

        print(
            f"ENSEMBLE MINIMUM: "
            f"{min(member_highs):.2f}°F",
            flush=True,
        )

        print(
            f"ENSEMBLE MAXIMUM: "
            f"{max(member_highs):.2f}°F",
            flush=True,
        )

        print(
            f"ENSEMBLE MEAN: "
            f"{statistics.mean(member_highs):.2f}°F",
            flush=True,
        )

        print(
            f"ENSEMBLE MEDIAN: "
            f"{statistics.median(member_highs):.2f}°F",
            flush=True,
        )

        validation = validate_weather_alignment(
            point_high,
            ensemble_data,
            point_info,
            ensemble_info,
        )

        if (
            validation["gap_f"]
            is not None
        ):
            print(
                f"POINT/ENSEMBLE GAP: "
                f"{validation['gap_f']:.2f}°F",
                flush=True,
            )

        print(
            f"WEATHER DATA VALIDATION: "
            f"{validation['reason']}",
            flush=True,
        )

        change_info = check_forecast_change(
            series_ticker,
            forecast_date,
            point_high,
            precipitation,
            ensemble_data,
            point_info,
            ensemble_info,
        )

        if change_info["first"]:
            print(
                "FORECAST STATUS: "
                "FIRST OBSERVATION",
                flush=True,
            )

            print(
                "Baseline stored.",
                flush=True,
            )

        else:
            print(
                f"POINT FORECAST CHANGE: "
                f"{change_info['temperature_change']:+.2f}°F",
                flush=True,
            )

            print(
                f"ENSEMBLE MEAN CHANGE: "
                f"{change_info['ensemble_mean_change']:+.2f}°F",
                flush=True,
            )

            print(
                f"PRECIPITATION CHANGE: "
                f"{change_info['precipitation_change']:+.2f} in",
                flush=True,
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
                print(
                    f"Skipping unparsed strike: "
                    f"{ticker}",
                    flush=True,
                )

                continue

            ask = cents_from_market(
                market
            )

            if ask is None:
                continue

            probability = (
                calculate_probability(
                    member_highs,
                    strike,
                )
            )

            edge = (
                probability
                - ask
            )

            opportunities.append({
                "ticker": ticker,
                "label": strike["label"],
                "probability": probability,
                "ask": ask,
                "edge": edge,
            })

        opportunities.sort(
            key=lambda item: item["edge"],
            reverse=True,
        )

        print(
            f"\nTOP OPPORTUNITIES: "
            f"{city_name} | "
            f"{forecast_date}",
            flush=True,
        )

        if not opportunities:
            print(
                "No contracts could be safely interpreted.",
                flush=True,
            )

        for index, opportunity in enumerate(
            opportunities[:5],
            start=1,
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
                flush=True,
            )

        positive = [
            opportunity
            for opportunity in opportunities
            if (
                opportunity["edge"]
                >= MIN_EDGE_POINTS
            )
        ]

        bot_status[
            "positive_signals"
        ] += len(positive)

        # ---------------- DISCORD SAFETY RULES ----------------

        if change_info["first"]:
            print(
                "No Discord alert: "
                "first observation.",
                flush=True,
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
                "No Discord alert: point forecast "
                "change below threshold.",
                flush=True,
            )

            continue

        if not validation["valid"]:
            print(
                "No Discord alert: weather "
                "validation/freshness failed.",
                flush=True,
            )

            continue

        if not positive:
            print(
                "No Discord alert: no opportunity "
                "meets minimum edge.",
                flush=True,
            )

            continue

        best = positive[0]

        fingerprint = opportunity_fingerprint(
            series_ticker,
            forecast_date,
            best,
            point_high,
            validation[
                "ensemble_mean"
            ],
        )

        if was_recently_alerted(
            fingerprint
        ):
            print(
                "No Discord alert: duplicate "
                "opportunity already alerted.",
                flush=True,
            )

            continue

        message = build_discord_message(
            city_name,
            forecast_date,
            point_high,
            precipitation,
            change_info,
            best,
            validation,
            series_ticker,
        )

        print(
            "\nDISCORD PAPER SIGNAL:",
            flush=True,
        )

        print(
            message,
            flush=True,
        )

        sent = send_discord_alert(
            message
        )

        if sent:
            bot_status[
                "discord_alerts"
            ] += 1

            record_alert(
                fingerprint
            )

            record_paper_trade(
                city_name,
                series_ticker,
                forecast_date,
                point_high,
                precipitation,
                change_info,
                ensemble_data,
                best,
                point_info,
                ensemble_info,
            )


# ==========================================================
# MAIN SCAN
# ==========================================================

def run_weather_scan():
    bot_status[
        "last_scan_utc"
    ] = utc_iso()

    bot_status[
        "series_checked"
    ] = 0

    bot_status[
        "markets_checked"
    ] = 0

    bot_status[
        "forecast_changes"
    ] = 0

    bot_status[
        "positive_signals"
    ] = 0

    bot_status[
        "discord_alerts"
    ] = 0

    bot_status[
        "last_error"
    ] = None

    print(
        "\n==================================================",
        flush=True,
    )

    print(
        "STARTING WEATHER MARKET SCAN",
        flush=True,
    )

    print(
        f"UTC: {utc_iso()}",
        flush=True,
    )

    print(
        "==================================================",
        flush=True,
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
                flush=True,
            )

            bot_status[
                "last_error"
            ] = str(error)

    bot_status[
        "last_scan_success"
    ] = utc_iso()

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
        f"{bot_status['series_checked']}",
        flush=True,
    )

    print(
        f"Markets checked: "
        f"{bot_status['markets_checked']}",
        flush=True,
    )

    print(
        f"Forecast changes: "
        f"{bot_status['forecast_changes']}",
        flush=True,
    )

    print(
        f"Positive preliminary signals: "
        f"{bot_status['positive_signals']}",
        flush=True,
    )

    print(
        f"Discord alerts sent: "
        f"{bot_status['discord_alerts']}",
        flush=True,
    )

    print(
        "==================================================",
        flush=True,
    )


# ==========================================================
# BACKGROUND SCANNER
# ==========================================================

def background_scanner():
    print(
        "Background scanner started.",
        flush=True,
    )

    while True:
        try:
            run_weather_scan()

        except Exception as error:
            print(
                f"Background scanner error: "
                f"{error}",
                flush=True,
            )

            bot_status[
                "last_error"
            ] = str(error)

        print(
            f"Waiting "
            f"{SCAN_INTERVAL_SECONDS} seconds...",
            flush=True,
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
        "Weather + Kalshi paper-trading monitor is running. "
        "Use /health or /status."
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
        "state_file": STATE_FILE,
        "state_version": STATE_VERSION,
        "cache_entries": len(
            persistent_state.get(
                "cache",
                {}
            )
        ),
        "forecast_entries": len(
            persistent_state.get(
                "forecasts",
                {}
            )
        ),
        "paper_trade_count": len(
            persistent_state.get(
                "paper_trades",
                []
            )
        ),
    })


@app.route("/status")
def status():
    return jsonify(
        bot_status
    )


@app.route("/paper-trades")
def paper_trades():
    return jsonify(
        persistent_state.get(
            "paper_trades",
            []
        )[-100:]
    )


@app.route("/debug-state")
def debug_state():
    # Useful while testing. Do not expose secrets here.
    return jsonify({
        "cache": persistent_state.get(
            "cache",
            {}
        ),
        "cooldowns": persistent_state.get(
            "cooldowns",
            {}
        ),
        "forecasts": persistent_state.get(
            "forecasts",
            {}
        ),
    })


@app.route(
    "/test-alert",
    methods=["GET"]
)
def test_alert():
    if not DISCORD_WEBHOOK_URL:
        return (
            "Discord webhook is not configured. "
            "Set DISCORD_WEBHOOK_URL in Render.",
            500,
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
            "Test Discord alert sent successfully!",
            200,
        )

    return (
        "Discord alert failed. "
        "Check Render logs for the detailed response.",
        500,
    )


# ==========================================================
# START APPLICATION
# ==========================================================

if __name__ == "__main__":
    load_state()

    scanner_thread = threading.Thread(
        target=background_scanner,
        daemon=True,
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
        flush=True,
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )
```
