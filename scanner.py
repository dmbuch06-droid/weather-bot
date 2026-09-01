import hashlib
import json
import logging
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

try:
    import psycopg2
    from psycopg2.extras import Json, execute_values
except ImportError:
    psycopg2 = None
    Json = None
    execute_values = None

import requests


# ==========================================================
# CONFIGURATION
# ==========================================================

KALSHI_API_URL = os.environ.get(
    "KALSHI_API_URL",
    "https://external-api.kalshi.com/trade-api/v2",
).rstrip("/")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "",
).strip()

DISCORD_RELAY_URL = os.environ.get(
    "DISCORD_RELAY_URL",
    "",
).strip()

DISCORD_RELAY_SECRET = os.environ.get(
    "DISCORD_RELAY_SECRET",
    "",
).strip()

REQUEST_TIMEOUT = int(
    os.environ.get("REQUEST_TIMEOUT", "15")
)

FORECAST_DAYS = int(
    os.environ.get("FORECAST_DAYS", "3")
)

SCAN_WORKERS = int(
    os.environ.get("SCAN_WORKERS", "6")
)

MIN_FORECAST_PROBABILITY_CHANGE_POINTS = float(
    os.environ.get(
        "MIN_FORECAST_PROBABILITY_CHANGE_POINTS",
        "20",
    )
)

MIN_TEMPERATURE_CHANGE_F = float(
    os.environ.get(
        "MIN_TEMPERATURE_CHANGE_F",
        "1",
    )
)

MIN_MARKET_LAG_POINTS = float(
    os.environ.get(
        "MIN_MARKET_LAG_POINTS",
        "10",
    )
)

MIN_PRELIMINARY_EDGE_POINTS = float(
    os.environ.get(
        "MIN_PRELIMINARY_EDGE_POINTS",
        "10",
    )
)

MIN_ENTRY_PRICE_CENTS = float(
    os.environ.get(
        "MIN_ENTRY_PRICE_CENTS",
        "5",
    )
)

MAX_ENTRY_PRICE_CENTS = float(
    os.environ.get(
        "MAX_ENTRY_PRICE_CENTS",
        "95",
    )
)

PAPER_RISK_DOLLARS = float(
    os.environ.get(
        "PAPER_RISK_DOLLARS",
        "10",
    )
)

ALLOW_UNVERIFIED_SIGNALS = (
    os.environ.get(
        "ALLOW_UNVERIFIED_SIGNALS",
        "false",
    ).lower()
    in {"1", "true", "yes"}
)

# We do not claim a model-run timestamp unless Open-Meteo actually
# returns one in the response.
WEATHER_MODELS = (
    "hrrr",
    "nbm",
    "gfs_seamless",
    "ecmwf_ifs025",
)

ENSEMBLE_MODEL = "gfs_seamless"


# ==========================================================
# CITY / STATION MAPPINGS
# ==========================================================
#
# Forecast points for the first four cities are aligned to the
# current Kalshi daily-temperature station descriptions:
# NYC = Central Park
# Chicago = Chicago Midway
# Miami = Miami International Airport
# Austin = Austin Bergstrom
#
# The other locations are monitored, but paper signals are
# disabled by default until their exact current settlement
# station/forecast-point mapping is independently verified.
#

CITIES = {
    "New York": {
        "aliases": ("new york", "new york city", "nyc"),
        "lat": 40.78,
        "lon": -73.97,
        "timezone": "America/New_York",
        "rain_code": "NYC",
        "signal_eligible": True,
        "station": "Central Park",
    },
    "Chicago": {
        "aliases": ("chicago",),
        "lat": 41.7870,
        "lon": -87.7717,
        "timezone": "America/Chicago",
        "rain_code": "CHI",
        "signal_eligible": True,
        "station": "Chicago Midway",
    },
    "Miami": {
        "aliases": ("miami",),
        "lat": 25.7959,
        "lon": -80.2870,
        "timezone": "America/New_York",
        "rain_code": "MIA",
        "signal_eligible": True,
        "station": "Miami International Airport",
    },
    "Austin": {
        "aliases": ("austin",),
        "lat": 30.1975,
        "lon": -97.6663,
        "timezone": "America/Chicago",
        "rain_code": "AUS",
        "signal_eligible": True,
        "station": "Austin Bergstrom",
    },
    "Los Angeles": {
        "aliases": ("los angeles", "la"),
        "lat": 33.9425,
        "lon": -118.4081,
        "timezone": "America/Los_Angeles",
        "rain_code": "LAX",
        "signal_eligible": False,
        "station": "LAX proxy",
    },
    "Dallas": {
        "aliases": ("dallas",),
        "lat": 32.8998,
        "lon": -97.0403,
        "timezone": "America/Chicago",
        "rain_code": "DAL",
        "signal_eligible": False,
        "station": "DFW proxy",
    },
    "Seattle": {
        "aliases": ("seattle",),
        "lat": 47.4502,
        "lon": -122.3088,
        "timezone": "America/Los_Angeles",
        "rain_code": "SEA",
        "signal_eligible": False,
        "station": "SEA proxy",
    },
    "Houston": {
        "aliases": ("houston",),
        "lat": 29.6454,
        "lon": -95.2789,
        "timezone": "America/Chicago",
        "rain_code": "HOU",
        "signal_eligible": False,
        "station": "Houston proxy",
    },
    "Oklahoma City": {
        "aliases": ("oklahoma city",),
        "lat": 35.3931,
        "lon": -97.6007,
        "timezone": "America/Chicago",
        "rain_code": "OKC",
        "signal_eligible": False,
        "station": "OKC proxy",
    },
    "Philadelphia": {
        "aliases": ("philadelphia", "philly"),
        "lat": 39.8744,
        "lon": -75.2424,
        "timezone": "America/New_York",
        "rain_code": "PHIL",
        "signal_eligible": False,
        "station": "PHL proxy",
    },
    "Phoenix": {
        "aliases": ("phoenix",),
        "lat": 33.4342,
        "lon": -112.0116,
        "timezone": "America/Phoenix",
        "rain_code": "PHX",
        "signal_eligible": False,
        "station": "PHX proxy",
    },
    "San Francisco": {
        "aliases": ("san francisco", "sf"),
        "lat": 37.6213,
        "lon": -122.3790,
        "timezone": "America/Los_Angeles",
        "rain_code": "SFO",
        "signal_eligible": False,
        "station": "SFO proxy",
    },
    "Las Vegas": {
        "aliases": ("las vegas", "vegas"),
        "lat": 36.0840,
        "lon": -115.1537,
        "timezone": "America/Los_Angeles",
        "rain_code": "LV",
        "signal_eligible": False,
        "station": "LAS proxy",
    },
    "Minneapolis": {
        "aliases": ("minneapolis",),
        "lat": 44.8848,
        "lon": -93.2223,
        "timezone": "America/Chicago",
        "rain_code": "MIN",
        "signal_eligible": False,
        "station": "MSP proxy",
    },
    "New Orleans": {
        "aliases": ("new orleans",),
        "lat": 30.0424,
        "lon": -90.0289,
        "timezone": "America/Chicago",
        "rain_code": "NOLA",
        "signal_eligible": False,
        "station": "MSY proxy",
    },
    "Denver": {
        "aliases": ("denver",),
        "lat": 39.8561,
        "lon": -104.6737,
        "timezone": "America/Denver",
        "rain_code": "DEN",
        "signal_eligible": False,
        "station": "DEN proxy",
    },
    "Trenton": {
        "aliases": ("trenton",),
        "lat": 40.2767,
        "lon": -74.8135,
        "timezone": "America/New_York",
        "rain_code": "TTN",
        "signal_eligible": False,
        "station": "TTN proxy",
    },
    "Newark": {
        "aliases": ("newark",),
        "lat": 40.6895,
        "lon": -74.1745,
        "timezone": "America/New_York",
        "rain_code": "EWR",
        "signal_eligible": False,
        "station": "EWR proxy",
    },
    "Washington DC": {
        "aliases": (
            "washington dc",
            "washington d.c.",
            "washington",
        ),
        "lat": 38.8512,
        "lon": -77.0402,
        "timezone": "America/New_York",
        "rain_code": "DC",
        "signal_eligible": False,
        "station": "DCA proxy",
    },
    "Boston": {
        "aliases": ("boston",),
        "lat": 42.3656,
        "lon": -71.0096,
        "timezone": "America/New_York",
        "rain_code": "BOS",
        "signal_eligible": False,
        "station": "BOS proxy",
    },
    "Atlanta": {
        "aliases": ("atlanta",),
        "lat": 33.6407,
        "lon": -84.4277,
        "timezone": "America/New_York",
        "rain_code": "ATL",
        "signal_eligible": False,
        "station": "ATL proxy",
    },
    "San Antonio": {
        "aliases": ("san antonio",),
        "lat": 29.5337,
        "lon": -98.4698,
        "timezone": "America/Chicago",
        "rain_code": "SATX",
        "signal_eligible": False,
        "station": "SATX proxy",
    },
}

RAIN_CODE_TO_CITY = {
    item["rain_code"]: name
    for name, item in CITIES.items()
}


# ==========================================================
# LOGGING / HTTP
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(
    "weather-kalshi-scanner"
)


def utc_now():
    return datetime.now(timezone.utc)


def safe_float(
    value: Any,
    default: Optional[float] = None,
):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def request_json(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
):
    last_error = None

    for attempt in range(3):
        try:
            response = session.get(
                url,
                params=params,
                headers={
                    "User-Agent": (
                        "WeatherKalshiResearchBot/4.0"
                    ),
                    "Accept": "application/json",
                },
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                retry_after = safe_float(
                    response.headers.get(
                        "Retry-After"
                    ),
                    2.0,
                ) or 2.0

                time.sleep(
                    min(
                        max(
                            retry_after,
                            0.5,
                        ),
                        10.0,
                    )
                )
                continue

            if response.status_code != 200:
                raise RuntimeError(
                    f"HTTP {response.status_code}: "
                    f"{response.text[:500]}"
                )

            payload = response.json()

            if not isinstance(payload, dict):
                raise RuntimeError(
                    "Expected a JSON object."
                )

            if payload.get("error"):
                raise RuntimeError(
                    f"API error: {payload['error']}"
                )

            return payload

        except Exception as error:
            last_error = error

            if attempt < 2:
                time.sleep(
                    1.5 * (attempt + 1)
                )

    raise RuntimeError(
        f"Request failed after retries: {last_error}"
    )


# ==========================================================
# DATABASE
# ==========================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_runs (
    run_id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    success BOOLEAN,
    error TEXT
);

CREATE TABLE IF NOT EXISTS forecast_observations (
    id BIGSERIAL PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    city TEXT NOT NULL,
    source TEXT NOT NULL,
    variable TEXT NOT NULL,
    forecast_date DATE NOT NULL,
    model_run_time TEXT,
    value DOUBLE PRECISION,
    probability_proxy DOUBLE PRECISION,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    UNIQUE(
        city,
        source,
        variable,
        forecast_date,
        payload_hash
    )
);

CREATE INDEX IF NOT EXISTS idx_forecast_latest
ON forecast_observations(
    city,
    source,
    variable,
    forecast_date,
    observed_at DESC
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    id BIGSERIAL PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ticker TEXT NOT NULL,
    event_ticker TEXT,
    series_ticker TEXT,
    market_date DATE,
    city TEXT,
    market_kind TEXT NOT NULL,
    yes_bid_cents DOUBLE PRECISION,
    yes_ask_cents DOUBLE PRECISION,
    no_bid_cents DOUBLE PRECISION,
    no_ask_cents DOUBLE PRECISION,
    last_price_cents DOUBLE PRECISION,
    status TEXT,
    result TEXT,
    raw_market JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_market_latest
ON market_snapshots(ticker, observed_at DESC);

CREATE TABLE IF NOT EXISTS paper_trades (
    id BIGSERIAL PRIMARY KEY,
    signal_fingerprint TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settled_at TIMESTAMPTZ,
    city TEXT NOT NULL,
    forecast_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    market_kind TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('YES','NO')),
    entry_price_cents DOUBLE PRECISION NOT NULL,
    risk_dollars DOUBLE PRECISION NOT NULL,
    contracts DOUBLE PRECISION NOT NULL,
    model_probability_proxy DOUBLE PRECISION NOT NULL,
    preliminary_edge_points DOUBLE PRECISION NOT NULL,
    forecast_change_points DOUBLE PRECISION NOT NULL,
    market_change_points DOUBLE PRECISION,
    market_lag_points DOUBLE PRECISION NOT NULL,
    forecast_temperature_change_f DOUBLE PRECISION,
    reason JSONB NOT NULL,
    result TEXT,
    profit_loss_dollars DOUBLE PRECISION,
    status TEXT NOT NULL DEFAULT 'open'
);

CREATE INDEX IF NOT EXISTS idx_paper_status
ON paper_trades(status, created_at DESC);

CREATE TABLE IF NOT EXISTS alert_log (
    fingerprint TEXT PRIMARY KEY,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    message JSONB NOT NULL
);
"""


def db_connect():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not configured."
        )
    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2-binary is not installed."
        )

    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,
    )


def ensure_schema():
    with db_connect() as conn:
        with conn.cursor() as cur:
            for statement in SCHEMA.split(";"):
                statement = statement.strip()

                if statement:
                    cur.execute(
                        statement
                    )


def begin_run():
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_runs(success)
                VALUES(NULL)
                RETURNING run_id
                """
            )
            return int(
                cur.fetchone()[0]
            )


def finish_run(
    run_id,
    success,
    error=None,
):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bot_runs
                SET finished_at=NOW(),
                    success=%s,
                    error=%s
                WHERE run_id=%s
                """,
                (
                    success,
                    error,
                    run_id,
                ),
            )


def read_previous_forecasts(
    conn,
    keys,
):
    """
    keys: list of (city, source, variable, forecast_date)
    """
    result = {}

    if not keys:
        return result

    with conn.cursor() as cur:
        for (
            city,
            source,
            variable,
            forecast_date,
        ) in keys:
            cur.execute(
                """
                SELECT
                    observed_at,
                    value,
                    probability_proxy,
                    model_run_time,
                    payload
                FROM forecast_observations
                WHERE city=%s
                  AND source=%s
                  AND variable=%s
                  AND forecast_date=%s
                ORDER BY observed_at DESC
                LIMIT 1
                """,
                (
                    city,
                    source,
                    variable,
                    forecast_date,
                ),
            )

            row = cur.fetchone()

            if row is not None:
                result[
                    (
                        city,
                        source,
                        variable,
                        forecast_date,
                    )
                ] = row

    return result


def read_previous_markets(
    conn,
    tickers,
):
    result = {}

    if not tickers:
        return result

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (ticker)
                ticker,
                observed_at,
                yes_bid_cents,
                yes_ask_cents,
                no_bid_cents,
                no_ask_cents,
                last_price_cents,
                status,
                result
            FROM market_snapshots
            WHERE ticker = ANY(%s)
            ORDER BY ticker, observed_at DESC
            """,
            (
                list(tickers),
            ),
        )

        for row in cur.fetchall():
            result[
                row[0]
            ] = row[1:]

    return result


def store_forecasts(
    conn,
    rows,
):
    if not rows:
        return

    execute_values(
        conn.cursor(),
        """
        INSERT INTO forecast_observations(
            observed_at,
            city,
            source,
            variable,
            forecast_date,
            model_run_time,
            value,
            probability_proxy,
            payload,
            payload_hash
        )
        VALUES(
            NOW(), %s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT(
            city,
            source,
            variable,
            forecast_date,
            payload_hash
        )
        DO NOTHING
        """,
        rows,
        template=None,
        page_size=500,
    )


def store_markets(
    conn,
    rows,
):
    if not rows:
        return

    execute_values(
        conn.cursor(),
        """
        INSERT INTO market_snapshots(
            observed_at,
            ticker,
            event_ticker,
            series_ticker,
            market_date,
            city,
            market_kind,
            yes_bid_cents,
            yes_ask_cents,
            no_bid_cents,
            no_ask_cents,
            last_price_cents,
            status,
            result,
            raw_market
        )
        VALUES(
            NOW(),%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        rows,
        page_size=500,
    )


# ==========================================================
# WEATHER
# ==========================================================

def location_lists():
    names = list(
        CITIES.keys()
    )

    latitudes = ",".join(
        str(
            CITIES[name]["lat"]
        )
        for name in names
    )

    longitudes = ",".join(
        str(
            CITIES[name]["lon"]
        )
        for name in names
    )

    return names, latitudes, longitudes


def parse_open_meteo_location_response(
    response,
    names,
):
    locations = (
        response
        if isinstance(response, list)
        else [response]
    )

    if len(locations) != len(names):
        raise RuntimeError(
            "Open-Meteo returned "
            f"{len(locations)} locations; "
            f"expected {len(names)}."
        )

    return locations


def aggregate_hourly_daily(
    location_data,
    city_name,
):
    hourly = location_data.get(
        "hourly",
        {},
    )

    times = hourly.get(
        "time",
        [],
    )

    temperatures = hourly.get(
        "temperature_2m",
        [],
    )

    precipitation = hourly.get(
        "precipitation",
        [],
    )

    timezone_name = CITIES[
        city_name
    ]["timezone"]

    local_tz = ZoneInfo(
        timezone_name
    )

    grouped = {}

    for index, timestamp in enumerate(
        times
    ):
        try:
            dt = datetime.fromisoformat(
                str(timestamp).replace(
                    "Z",
                    "+00:00",
                )
            )
        except ValueError:
            continue

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        forecast_date = (
            dt.astimezone(
                local_tz
            ).date().isoformat()
        )

        bucket = grouped.setdefault(
            forecast_date,
            {
                "temperatures": [],
                "precipitation": [],
            },
        )

        if index < len(
            temperatures
        ):
            value = safe_float(
                temperatures[index]
            )
            if value is not None:
                bucket[
                    "temperatures"
                ].append(value)

        if index < len(
            precipitation
        ):
            value = safe_float(
                precipitation[index]
            )
            if value is not None:
                bucket[
                    "precipitation"
                ].append(value)

    daily = {}

    for forecast_date, bucket in grouped.items():
        if not bucket[
            "temperatures"
        ]:
            continue

        daily[
            forecast_date
        ] = {
            "high": max(
                bucket["temperatures"]
            ),
            "precipitation_sum": sum(
                bucket[
                    "precipitation"
                ]
            ),
        }

    model_run_time = (
        location_data.get(
            "model_run_time"
        )
        or location_data.get(
            "model_run_id"
        )
    )

    return {
        "daily": daily,
        "model_run_time": (
            str(model_run_time)
            if model_run_time is not None
            else None
        ),
    }


def fetch_model(
    session,
    model,
):
    names, latitudes, longitudes = (
        location_lists()
    )

    if model in {
        "hrrr",
        "nbm",
    }:
        url = (
            "https://api.open-meteo.com/v1/gfs"
        )
    else:
        url = (
            "https://api.open-meteo.com/v1/forecast"
        )

    payload = request_json(
        session,
        url,
        {
            "latitude": latitudes,
            "longitude": longitudes,
            "models": model,
            "hourly": (
                "temperature_2m,"
                "precipitation"
            ),
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "timezone": "UTC",
            "forecast_days": FORECAST_DAYS,
        },
    )

    locations = (
        parse_open_meteo_location_response(
            payload,
            names,
        )
    )

    return {
        city_name: aggregate_hourly_daily(
            location_data,
            city_name,
        )
        for city_name, location_data
        in zip(
            names,
            locations,
        )
    }


def fetch_ensemble(
    session,
):
    names, latitudes, longitudes = (
        location_lists()
    )

    payload = request_json(
        session,
        "https://ensemble-api.open-meteo.com/v1/ensemble",
        {
            "latitude": latitudes,
            "longitude": longitudes,
            "models": ENSEMBLE_MODEL,
            "hourly": (
                "temperature_2m,"
                "precipitation"
            ),
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "timezone": "UTC",
            "forecast_days": FORECAST_DAYS,
        },
    )

    locations = (
        parse_open_meteo_location_response(
            payload,
            names,
        )
    )

    output = {}

    for city_name, location_data in zip(
        names,
        locations,
    ):
        hourly = location_data.get(
            "hourly",
            {},
        )

        times = hourly.get(
            "time",
            [],
        )

        temp_keys = sorted(
            key
            for key in hourly
            if key.startswith(
                "temperature_2m_member"
            )
        )

        rain_keys = sorted(
            key
            for key in hourly
            if key.startswith(
                "precipitation_member"
            )
        )

        if not temp_keys:
            raise RuntimeError(
                f"{city_name}: no ensemble "
                "temperature members returned."
            )

        if not rain_keys:
            raise RuntimeError(
                f"{city_name}: no ensemble "
                "precipitation members returned."
            )

        local_tz = ZoneInfo(
            CITIES[city_name][
                "timezone"
            ]
        )

        grouped = {}

        for index, timestamp in enumerate(
            times
        ):
            try:
                dt = datetime.fromisoformat(
                    str(timestamp).replace(
                        "Z",
                        "+00:00",
                    )
                )
            except ValueError:
                continue

            if dt.tzinfo is None:
                dt = dt.replace(
                    tzinfo=timezone.utc
                )

            forecast_date = (
                dt.astimezone(
                    local_tz
                ).date().isoformat()
            )

            day = grouped.setdefault(
                forecast_date,
                {
                    "temperature": {
                        key: []
                        for key in temp_keys
                    },
                    "rain": {
                        key: 0.0
                        for key in rain_keys
                    },
                },
            )

            for key in temp_keys:
                values = hourly.get(
                    key,
                    [],
                )

                if index >= len(values):
                    continue

                value = safe_float(
                    values[index]
                )

                if value is not None:
                    day[
                        "temperature"
                    ][key].append(
                        value
                    )

            for key in rain_keys:
                values = hourly.get(
                    key,
                    [],
                )

                if index >= len(values):
                    continue

                value = safe_float(
                    values[index]
                )

                if value is not None:
                    day[
                        "rain"
                    ][key] += value

        daily = {}

        for forecast_date, day in grouped.items():
            highs = []

            for key in temp_keys:
                values = day[
                    "temperature"
                ][key]

                if values:
                    highs.append(
                        max(values)
                    )

            rains = [
                day["rain"].get(
                    key,
                    0.0,
                )
                for key in rain_keys
            ]

            if highs and rains:
                daily[
                    forecast_date
                ] = {
                    "member_highs": highs,
                    "member_rain_totals": rains,
                }

        model_run_time = (
            location_data.get(
                "model_run_time"
            )
            or location_data.get(
                "model_run_id"
            )
        )

        output[
            city_name
        ] = {
            "daily": daily,
            "model_run_time": (
                str(model_run_time)
                if model_run_time is not None
                else None
            ),
            "temperature_member_count": len(
                temp_keys
            ),
            "rain_member_count": len(
                rain_keys
            ),
        }

    return output


def fetch_weather():
    session = requests.Session()

    deterministic = {}

    with ThreadPoolExecutor(
        max_workers=len(WEATHER_MODELS)
    ) as executor:
        jobs = {
            executor.submit(
                fetch_model,
                session,
                model,
            ): model
            for model in WEATHER_MODELS
        }

        for future in as_completed(
            jobs
        ):
            model = jobs[
                future
            ]
            deterministic[
                model
            ] = future.result()

    ensemble = fetch_ensemble(
        session
    )

    return deterministic, ensemble


# ==========================================================
# KALSHI
# ==========================================================

def discover_temperature_series(
    session,
):
    payload = request_json(
        session,
        f"{KALSHI_API_URL}/series",
        {
            "category": "Climate and Weather",
        },
    )

    output = []

    for item in payload.get(
        "series",
        [],
    ):
        title = (
            item.get("title")
            or ""
        )

        frequency = (
            item.get("frequency")
            or ""
        )

        if (
            "highest temperature"
            not in title.lower()
        ):
            continue

        if (
            "daily"
            not in frequency.lower()
            and "daily temperature"
            not in title.lower()
        ):
            continue

        city_name = match_city_in_title(
            title
        )

        ticker = item.get(
            "ticker"
        )

        if (
            city_name is None
            or not ticker
        ):
            continue

        output.append(
            {
                "ticker": ticker,
                "city": city_name,
                "metadata": item,
            }
        )

    # One series per city is enough. If duplicates exist,
    # prefer an exact city mapping and the first returned entry.
    unique = {}

    for item in output:
        unique.setdefault(
            item["city"],
            item,
        )

    return list(
        unique.values()
    )


def match_city_in_title(
    title,
):
    lowered = title.lower()

    ordered = sorted(
        CITIES.items(),
        key=lambda item: max(
            len(alias)
            for alias in item[1][
                "aliases"
            ]
        ),
        reverse=True,
    )

    for city_name, data in ordered:
        for alias in data[
            "aliases"
        ]:
            if alias in lowered:
                return city_name

    return None


def fetch_markets_for_series(
    session,
    series_ticker,
):
    markets = []
    cursor = None

    for _ in range(10):
        params = {
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 1000,
        }

        if cursor:
            params["cursor"] = cursor

        payload = request_json(
            session,
            f"{KALSHI_API_URL}/markets",
            params,
        )

        page = payload.get(
            "markets",
            [],
        )

        markets.extend(
            page
        )

        cursor = payload.get(
            "cursor"
        )

        if not cursor:
            break

    return markets


def parse_market_date(
    ticker,
):
    parts = (
        ticker or ""
    ).split("-")

    if len(parts) < 2:
        return None

    try:
        return datetime.strptime(
            parts[1],
            "%y%b%d",
        ).date().isoformat()
    except ValueError:
        return None


def temperature_strike(
    market,
):
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

    if strike_type == "between":
        if floor is None or cap is None:
            return None

        return (
            "between",
            floor,
            cap,
            f"{floor:g}°F to {cap:g}°F",
        )

    if strike_type == "greater":
        if floor is None:
            return None

        return (
            "greater",
            floor,
            None,
            f">{floor:g}°F",
        )

    if strike_type == "less":
        if cap is None:
            return None

        return (
            "less",
            None,
            cap,
            f"<{cap:g}°F",
        )

    return None


def temperature_probability(
    member_highs,
    strike,
):
    values = list(
        member_highs
    )

    if not values:
        return None

    kind, floor, cap, _ = (
        strike
    )

    if kind == "between":
        matches = sum(
            floor
            <= value
            <= cap
            for value in values
        )

    elif kind == "greater":
        matches = sum(
            value > floor
            for value in values
        )

    elif kind == "less":
        matches = sum(
            value < cap
            for value in values
        )

    else:
        return None

    return (
        matches
        / len(values)
        * 100.0
    )


def rain_probability(
    member_rain_totals,
):
    values = list(
        member_rain_totals
    )

    if not values:
        return None

    matches = sum(
        value > 0.0
        for value in values
    )

    return (
        matches
        / len(values)
        * 100.0
    )


def market_ask(
    market,
    side,
):
    field = (
        "yes_ask_dollars"
        if side == "YES"
        else "no_ask_dollars"
    )

    value = safe_float(
        market.get(field)
    )

    if value is None:
        return None

    return value * 100.0


def previous_market_price(
    previous_row,
    side,
):
    if previous_row is None:
        return None

    index = (
        2
        if side == "YES"
        else 4
    )

    return safe_float(
        previous_row[index]
    )


def market_change_points(
    current,
    previous_row,
    side,
):
    previous = previous_market_price(
        previous_row,
        side,
    )

    if previous is None:
        return None

    return (
        current
        - previous
    )


def candidate_is_strong(
    candidate,
):
    if candidate is None:
        return False

    return (
        abs(
            candidate[
                "forecast_change_points"
            ]
        )
        >= MIN_FORECAST_PROBABILITY_CHANGE_POINTS
        and candidate[
            "market_lag_points"
        ]
        >= MIN_MARKET_LAG_POINTS
        and candidate[
            "preliminary_edge_points"
        ]
        >= MIN_PRELIMINARY_EDGE_POINTS
        and MIN_ENTRY_PRICE_CENTS
        <= candidate[
            "entry_price_cents"
        ]
        <= MAX_ENTRY_PRICE_CENTS
    )


def make_candidate(
    city_name,
    forecast_date,
    market,
    side,
    current_probability,
    previous_probability,
    market_change,
    temp_change,
    market_kind,
):
    ask = market_ask(
        market,
        side,
    )

    if ask is None:
        return None

    if not (
        MIN_ENTRY_PRICE_CENTS
        <= ask
        <= MAX_ENTRY_PRICE_CENTS
    ):
        return None

    probability_change = (
        current_probability
        - previous_probability
    )

    same_direction = (
        probability_change
        * market_change
        > 0
    )

    if same_direction:
        lag = (
            abs(probability_change)
            - abs(market_change)
        )
    else:
        # A flat or counter-moving market does not reduce
        # the forecast shock; it is treated as full lag.
        lag = abs(
            probability_change
        )

    return {
        "city": city_name,
        "forecast_date": forecast_date,
        "ticker": market[
            "ticker"
        ],
        "market_kind": market_kind,
        "side": side,
        "entry_price_cents": ask,
        "model_probability_proxy": (
            current_probability
        ),
        "forecast_change_points": (
            probability_change
        ),
        "market_change_points": (
            market_change
        ),
        "market_lag_points": lag,
        "preliminary_edge_points": (
            current_probability
            - ask
        ),
        "forecast_temperature_change_f": (
            temp_change
        ),
    }


# ==========================================================
# PAPER TRADING
# ==========================================================

def signal_fingerprint(
    signal,
):
    raw = "|".join(
        [
            signal["ticker"],
            signal["side"],
            signal["city"],
            signal["forecast_date"],
            f"{signal['entry_price_cents']:.2f}",
            f"{signal['model_probability_proxy']:.2f}",
            f"{signal['forecast_change_points']:.2f}",
        ]
    )

    return hashlib.sha256(
        raw.encode(
            "utf-8"
        )
    ).hexdigest()[:32]


def open_paper_trade(
    signal,
    reason,
):
    fp = signal_fingerprint(
        signal
    )

    price = (
        signal["entry_price_cents"]
        / 100.0
    )

    if price <= 0:
        return False, fp

    contracts = (
        PAPER_RISK_DOLLARS
        / price
    )

    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO paper_trades(
                    signal_fingerprint,
                    city,
                    forecast_date,
                    ticker,
                    market_kind,
                    side,
                    entry_price_cents,
                    risk_dollars,
                    contracts,
                    model_probability_proxy,
                    preliminary_edge_points,
                    forecast_change_points,
                    market_change_points,
                    market_lag_points,
                    forecast_temperature_change_f,
                    reason,
                    status
                )
                VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,'open'
                )
                ON CONFLICT(
                    signal_fingerprint
                )
                DO NOTHING
                """,
                (
                    fp,
                    signal["city"],
                    signal["forecast_date"],
                    signal["ticker"],
                    signal["market_kind"],
                    signal["side"],
                    signal[
                        "entry_price_cents"
                    ],
                    PAPER_RISK_DOLLARS,
                    contracts,
                    signal[
                        "model_probability_proxy"
                    ],
                    signal[
                        "preliminary_edge_points"
                    ],
                    signal[
                        "forecast_change_points"
                    ],
                    signal[
                        "market_change_points"
                    ],
                    signal[
                        "market_lag_points"
                    ],
                    signal.get(
                        "forecast_temperature_change_f"
                    ),
                    Json(reason),
                ),
            )

            created = (
                cur.rowcount == 1
            )

    return created, fp


def send_discord(
    message,
):
    if (
        not DISCORD_RELAY_URL
        or not DISCORD_RELAY_SECRET
    ):
        logger.warning(
            "Discord relay is not configured."
        )
        return False

    try:
        response = requests.post(
            DISCORD_RELAY_URL,
            json={
                "secret": DISCORD_RELAY_SECRET,
                "message": message,
            },
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=REQUEST_TIMEOUT,
        )

        if not (
            200
            <= response.status_code
            < 300
        ):
            logger.error(
                "Discord relay failed: "
                "%s %s",
                response.status_code,
                response.text[:500],
            )
            return False

        try:
            payload = response.json()
        except Exception:
            payload = {}

        if payload.get(
            "success"
        ) is False:
            logger.error(
                "Discord relay reported failure: %s",
                payload,
            )
            return False

        return True

    except Exception as error:
        logger.error(
            "Discord relay exception: %s",
            error,
        )
        return False


def send_signal_alert(
    signal,
    fp,
):
    direction = (
        "up"
        if signal[
            "forecast_change_points"
        ] > 0
        else "down"
    )

    message = (
        "🌦️ **WEATHER FORECAST SHOCK — "
        "PAPER TRADE**\n\n"
        f"**{signal['city']} — "
        f"{signal['forecast_date']}**\n"
        f"Market: `{signal['ticker']}`\n"
        f"Type: **{signal['market_kind']}**\n"
        f"Side: **{signal['side']}**\n"
        f"Entry ask: **"
        f"{signal['entry_price_cents']:.1f}¢**\n\n"
        f"Raw ensemble probability proxy: "
        f"**{signal['model_probability_proxy']:.1f}%**\n"
        f"Forecast probability change: "
        f"**{direction} "
        f"{abs(signal['forecast_change_points']):.1f} pts**\n"
        f"Market price change: "
        f"**{signal['market_change_points']:+.1f} pts**\n"
        f"Estimated market lag: "
        f"**{signal['market_lag_points']:.1f} pts**\n"
        f"Preliminary edge: "
        f"**{signal['preliminary_edge_points']:+.1f} pts**\n"
    )

    if (
        signal.get(
            "forecast_temperature_change_f"
        )
        is not None
    ):
        message += (
            f"HRRR high change: "
            f"**{signal['forecast_temperature_change_f']:+.1f}°F**\n"
        )

    message += (
        f"\nPaper risk: "
        f"**${PAPER_RISK_DOLLARS:.2f}**\n"
        f"Signal fingerprint: `{fp}`\n\n"
        "⚠️ **Paper trading only.** "
        "The probability is an uncalibrated "
        "ensemble-frequency proxy, not a proven "
        "fair probability."
    )

    return send_discord(
        message
    )


def settle_paper_trades():
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    ticker,
                    side,
                    risk_dollars,
                    contracts
                FROM paper_trades
                WHERE status='open'
                ORDER BY created_at
                LIMIT 500
                """
            )

            rows = cur.fetchall()

        if not rows:
            return

        tickers = sorted(
            {
                row[1]
                for row in rows
            }
        )

        session = requests.Session()

        payload = request_json(
            session,
            f"{KALSHI_API_URL}/markets",
            {
                "tickers": ",".join(
                    tickers
                ),
                "limit": 1000,
            },
        )

        markets = {
            market.get("ticker"): market
            for market in payload.get(
                "markets",
                [],
            )
        }

        for (
            trade_id,
            ticker,
            side,
            risk_dollars,
            contracts,
        ) in rows:
            market = markets.get(
                ticker
            )

            if not market:
                continue

            result = (
                market.get("result")
                or ""
            ).lower()

            if result not in {
                "yes",
                "no",
            }:
                continue

            won = (
                result == side.lower()
            )

            if won:
                pnl = (
                    float(contracts)
                    - float(risk_dollars)
                )
            else:
                pnl = -float(
                    risk_dollars
                )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_trades
                    SET settled_at=NOW(),
                        result=%s,
                        profit_loss_dollars=%s,
                        status='settled'
                    WHERE id=%s
                      AND status='open'
                    """,
                    (
                        result,
                        pnl,
                        trade_id,
                    ),
                )

            logger.info(
                "PAPER TRADE SETTLED | "
                "%s | %s | result=%s | P/L=$%.2f",
                ticker,
                side,
                result,
                pnl,
            )


# ==========================================================
# SCAN
# ==========================================================

def scan():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is missing."
        )

    ensure_schema()

    run_id = begin_run()
    started = time.monotonic()

    try:
        logger.info(
            "=================================================="
        )
        logger.info(
            "STARTING WEATHER MARKET SCAN"
        )
        logger.info(
            "UTC: %s",
            utc_now().isoformat(),
        )
        logger.info(
            "=================================================="
        )

        session = requests.Session()

        deterministic, ensemble = (
            fetch_weather()
        )

        series = (
            discover_temperature_series(
                session
            )
        )

        temperature_results = []

        with ThreadPoolExecutor(
            max_workers=SCAN_WORKERS
        ) as executor:
            futures = {
                executor.submit(
                    fetch_markets_for_series,
                    session,
                    item["ticker"],
                ): item
                for item in series
            }

            for future in as_completed(
                futures
            ):
                item = futures[
                    future
                ]

                try:
                    temperature_results.append(
                        (
                            item,
                            future.result(),
                        )
                    )
                except Exception as error:
                    logger.error(
                        "Temperature market fetch failed "
                        "for %s: %s",
                        item["ticker"],
                        error,
                    )

        try:
            rain_markets = (
                fetch_markets_for_series(
                    session,
                    "KXRAIN",
                )
            )
        except Exception as error:
            logger.error(
                "KXRAIN fetch failed: %s",
                error,
            )
            rain_markets = []

        # ------------------------------------------------------
        # Build all market rows first.
        # ------------------------------------------------------

        market_entries = []

        for series_info, markets in (
            temperature_results
        ):
            for market in markets:
                ticker = (
                    market.get("ticker")
                    or ""
                )

                city_name = series_info[
                    "city"
                ]

                market_date = parse_market_date(
                    ticker
                )

                if market_date is None:
                    continue

                if market_date < today_for_city(
                    CITIES[city_name]
                ):
                    continue

                market_entries.append(
                    {
                        "market": market,
                        "city": city_name,
                        "date": market_date,
                        "kind": "temperature",
                        "series_info": series_info,
                    }
                )

        for market in rain_markets:
            ticker = (
                market.get("ticker")
                or ""
            )

            city_name = rain_city_from_ticker(
                ticker
            )

            if city_name is None:
                continue

            market_date = parse_market_date(
                ticker
            )

            if market_date is None:
                continue

            if market_date < today_for_city(
                CITIES[city_name]
            ):
                continue

            market_entries.append(
                {
                    "market": market,
                    "city": city_name,
                    "date": market_date,
                    "kind": "rain",
                    "series_info": {
                        "ticker": "KXRAIN",
                        "metadata": {},
                    },
                }
            )

        tickers = sorted(
            {
                entry["market"].get(
                    "ticker"
                )
                for entry in market_entries
                if entry["market"].get(
                    "ticker"
                )
            }
        )

        # ------------------------------------------------------
        # Read previous history BEFORE writing current history.
        # This ordering is intentional.
        # ------------------------------------------------------

        forecast_keys = set()

        for entry in market_entries:
            city_name = entry[
                "city"
            ]
            market_date = entry[
                "date"
            ]

            forecast_keys.add(
                (
                    city_name,
                    "ensemble",
                    (
                        "temperature_distribution"
                        if entry["kind"]
                        == "temperature"
                        else "rain_distribution"
                    ),
                    market_date,
                )
            )

            if entry["kind"] == "temperature":
                forecast_keys.add(
                    (
                        city_name,
                        "hrrr",
                        "temperature_high",
                        market_date,
                    )
                )

        with db_connect() as conn:
            previous_forecasts = (
                read_previous_forecasts(
                    conn,
                    sorted(
                        forecast_keys
                    ),
                )
            )

            previous_markets = (
                read_previous_markets(
                    conn,
                    tickers,
                )
            )

        candidates = []

        # ------------------------------------------------------
        # Temperature signals.
        # ------------------------------------------------------

        for entry in market_entries:
            if entry["kind"] != "temperature":
                continue

            market = entry[
                "market"
            ]

            city_name = entry[
                "city"
            ]

            forecast_date = entry[
                "date"
            ]

            if not (
                CITIES[city_name][
                    "signal_eligible"
                ]
                or ALLOW_UNVERIFIED_SIGNALS
            ):
                continue

            ensemble_day = (
                ensemble
                .get(city_name, {})
                .get("daily", {})
                .get(forecast_date)
            )

            if not ensemble_day:
                continue

            current_members = (
                ensemble_day[
                    "member_highs"
                ]
            )

            prev_row = (
                previous_forecasts.get(
                    (
                        city_name,
                        "ensemble",
                        "temperature_distribution",
                        forecast_date,
                    )
                )
            )

            if prev_row is None:
                continue

            previous_members = (
                (prev_row[4] or {})
                .get(
                    "member_highs",
                    [],
                )
            )

            if not previous_members:
                continue

            strike = temperature_strike(
                market
            )

            if strike is None:
                continue

            current_yes = (
                temperature_probability(
                    current_members,
                    strike,
                )
            )

            previous_yes = (
                temperature_probability(
                    previous_members,
                    strike,
                )
            )

            if (
                current_yes is None
                or previous_yes is None
            ):
                continue

            hrrr_prev = previous_forecasts.get(
                (
                    city_name,
                    "hrrr",
                    "temperature_high",
                    forecast_date,
                )
            )

            current_hrrr = (
                deterministic
                .get("hrrr", {})
                .get(city_name, {})
                .get("daily", {})
                .get(forecast_date)
            )

            hrrr_change = None

            if (
                hrrr_prev is not None
                and current_hrrr is not None
            ):
                old_high = safe_float(
                    hrrr_prev[1]
                )

                if old_high is not None:
                    hrrr_change = (
                        current_hrrr["high"]
                        - old_high
                    )

            previous_market = (
                previous_markets.get(
                    market.get("ticker")
                )
            )

            if previous_market is None:
                continue

            for side in (
                "YES",
                "NO",
            ):
                ask = market_ask(
                    market,
                    side,
                )

                if ask is None:
                    continue

                current_probability = (
                    current_yes
                    if side == "YES"
                    else 100.0
                    - current_yes
                )

                previous_probability = (
                    previous_yes
                    if side == "YES"
                    else 100.0
                    - previous_yes
                )

                mchange = (
                    market_change_points(
                        ask,
                        previous_market,
                        side,
                    )
                )

                if mchange is None:
                    continue

                candidate = make_candidate(
                    city_name,
                    forecast_date,
                    market,
                    side,
                    current_probability,
                    previous_probability,
                    mchange,
                    hrrr_change,
                    "temperature",
                )

                if candidate_is_strong(
                    candidate
                ):
                    candidates.append(
                        candidate
                    )

        # ------------------------------------------------------
        # Rain signals.
        # ------------------------------------------------------

        for entry in market_entries:
            if entry["kind"] != "rain":
                continue

            city_name = entry[
                "city"
            ]

            forecast_date = entry[
                "date"
            ]

            if not (
                CITIES[city_name][
                    "signal_eligible"
                ]
                or ALLOW_UNVERIFIED_SIGNALS
            ):
                continue

            market = entry[
                "market"
            ]

            ensemble_day = (
                ensemble
                .get(city_name, {})
                .get("daily", {})
                .get(forecast_date)
            )

            if not ensemble_day:
                continue

            current_members = (
                ensemble_day[
                    "member_rain_totals"
                ]
            )

            prev_row = (
                previous_forecasts.get(
                    (
                        city_name,
                        "ensemble",
                        "rain_distribution",
                        forecast_date,
                    )
                )
            )

            if prev_row is None:
                continue

            previous_members = (
                (prev_row[4] or {})
                .get(
                    "member_rain_totals",
                    [],
                )
            )

            current_yes = (
                rain_probability(
                    current_members
                )
            )

            previous_yes = (
                rain_probability(
                    previous_members
                )
            )

            if (
                current_yes is None
                or previous_yes is None
            ):
                continue

            previous_market = (
                previous_markets.get(
                    market.get("ticker")
                )
            )

            if previous_market is None:
                continue

            for side in (
                "YES",
                "NO",
            ):
                ask = market_ask(
                    market,
                    side,
                )

                if ask is None:
                    continue

                current_probability = (
                    current_yes
                    if side == "YES"
                    else 100.0
                    - current_yes
                )

                previous_probability = (
                    previous_yes
                    if side == "YES"
                    else 100.0
                    - previous_yes
                )

                mchange = (
                    market_change_points(
                        ask,
                        previous_market,
                        side,
                    )
                )

                if mchange is None:
                    continue

                candidate = make_candidate(
                    city_name,
                    forecast_date,
                    market,
                    side,
                    current_probability,
                    previous_probability,
                    mchange,
                    None,
                    "rain",
                )

                if candidate_is_strong(
                    candidate
                ):
                    candidates.append(
                        candidate
                    )

        # ------------------------------------------------------
        # Store CURRENT weather observations after the
        # previous-vs-current comparisons above.
        # ------------------------------------------------------

        forecast_rows = []

        for model, cities in (
            deterministic.items()
        ):
            for city_name, city_data in (
                cities.items()
            ):
                model_run_time = (
                    city_data.get(
                        "model_run_time"
                    )
                )

                for forecast_date, daily in (
                    city_data[
                        "daily"
                    ].items()
                ):
                    daily_high = daily.get(
                        "high"
                    )

                    daily_rain = daily.get(
                        "precipitation_sum"
                    )

                    high_payload = {
                        "high": daily_high
                    }

                    rain_payload = {
                        "precipitation_sum": daily_rain
                    }

                    high_hash = hashlib.sha256(
                        json.dumps(
                            high_payload,
                            sort_keys=True,
                        ).encode(
                            "utf-8"
                        )
                    ).hexdigest()

                    rain_hash = hashlib.sha256(
                        json.dumps(
                            rain_payload,
                            sort_keys=True,
                        ).encode(
                            "utf-8"
                        )
                    ).hexdigest()

                    forecast_rows.append(
                        (
                            city_name,
                            model,
                            "temperature_high",
                            forecast_date,
                            model_run_time,
                            daily_high,
                            None,
                            Json(
                                high_payload
                            ),
                            high_hash,
                        )
                    )

                    forecast_rows.append(
                        (
                            city_name,
                            model,
                            "precipitation_sum",
                            forecast_date,
                            model_run_time,
                            daily_rain,
                            None,
                            Json(
                                rain_payload
                            ),
                            rain_hash,
                        )
                    )

        for city_name, city_data in (
            ensemble.items()
        ):
            for forecast_date, daily in (
                city_data[
                    "daily"
                ].items()
            ):
                temp_payload = {
                    "member_highs": daily[
                        "member_highs"
                    ]
                }

                rain_payload = {
                    "member_rain_totals": daily[
                        "member_rain_totals"
                    ]
                }

                temp_hash = hashlib.sha256(
                    json.dumps(
                        temp_payload,
                        sort_keys=True,
                    ).encode(
                        "utf-8"
                    )
                ).hexdigest()

                rain_hash = hashlib.sha256(
                    json.dumps(
                        rain_payload,
                        sort_keys=True,
                    ).encode(
                        "utf-8"
                    )
                ).hexdigest()

                forecast_rows.append(
                    (
                        city_name,
                        "ensemble",
                        "temperature_distribution",
                        forecast_date,
                        city_data.get(
                            "model_run_time"
                        ),
                        statistics.mean(
                            daily[
                                "member_highs"
                            ]
                        ),
                        None,
                        Json(
                            temp_payload
                        ),
                        temp_hash,
                    )
                )

                current_rain_probability = (
                    rain_probability(
                        daily[
                            "member_rain_totals"
                        ]
                    )
                )

                forecast_rows.append(
                    (
                        city_name,
                        "ensemble",
                        "rain_distribution",
                        forecast_date,
                        city_data.get(
                            "model_run_time"
                        ),
                        statistics.mean(
                            daily[
                                "member_rain_totals"
                            ]
                        ),
                        current_rain_probability,
                        Json(
                            rain_payload
                        ),
                        rain_hash,
                    )
                )

        market_rows = []

        for entry in market_entries:
            market = entry[
                "market"
            ]

            def d2c(value):
                number = safe_float(
                    value
                )
                return (
                    number * 100.0
                    if number is not None
                    else None
                )

            market_rows.append(
                (
                    market.get(
                        "ticker"
                    ),
                    market.get(
                        "event_ticker"
                    ),
                    market.get(
                        "series_ticker"
                    ),
                    entry["date"],
                    entry["city"],
                    entry["kind"],
                    d2c(
                        market.get(
                            "yes_bid_dollars"
                        )
                    ),
                    d2c(
                        market.get(
                            "yes_ask_dollars"
                        )
                    ),
                    d2c(
                        market.get(
                            "no_bid_dollars"
                        )
                    ),
                    d2c(
                        market.get(
                            "no_ask_dollars"
                        )
                    ),
                    d2c(
                        market.get(
                            "last_price_dollars"
                        )
                    ),
                    market.get(
                        "status"
                    ),
                    market.get(
                        "result"
                    ),
                    Json(market),
                )
            )

        with db_connect() as conn:
            store_forecasts(
                conn,
                forecast_rows,
            )
            store_markets(
                conn,
                market_rows,
            )

        # ------------------------------------------------------
        # One strongest signal per city/date.
        # ------------------------------------------------------

        selected = {}

        for candidate in candidates:
            key = (
                candidate["city"],
                candidate["forecast_date"],
            )

            if key not in selected:
                selected[
                    key
                ] = candidate
                continue

            old = selected[
                key
            ]

            new_score = (
                candidate[
                    "market_lag_points"
                ],
                candidate[
                    "preliminary_edge_points"
                ],
            )

            old_score = (
                old[
                    "market_lag_points"
                ],
                old[
                    "preliminary_edge_points"
                ],
            )

            if new_score > old_score:
                selected[
                    key
                ] = candidate

        # ------------------------------------------------------
        # Paper trades and alerts.
        # A paper trade is recorded independently of Discord.
        # Discord failure must not erase a qualifying paper trade.
        # ------------------------------------------------------

        for signal in selected.values():
            reason = {
                "strategy": (
                    "large forecast probability "
                    "shock plus insufficient market "
                    "response"
                ),
                "location_station": CITIES[
                    signal["city"]
                ]["station"],
                "raw_ensemble_probability": True,
                "calibrated_probability": False,
                "paper_only": True,
                "allow_unverified_signals": (
                    ALLOW_UNVERIFIED_SIGNALS
                ),
            }

            created, fp = (
                open_paper_trade(
                    signal,
                    reason,
                )
            )

            if not created:
                continue

            logger.info(
                "PAPER TRADE OPENED | "
                "%s | %s | %s | %.1f¢",
                signal["ticker"],
                signal["side"],
                signal["city"],
                signal[
                    "entry_price_cents"
                ],
            )

            alert_payload = {
                "signal": signal,
                "fingerprint": fp,
            }

            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM alert_log
                        WHERE fingerprint=%s
                        LIMIT 1
                        """,
                        (fp,),
                    )
                    already_alerted = (
                        cur.fetchone()
                        is not None
                    )

            if already_alerted:
                continue

            delivered = (
                send_signal_alert(
                    signal,
                    fp,
                )
            )

            if delivered:
                with db_connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO alert_log(
                                fingerprint,
                                message
                            )
                            VALUES(
                                %s,%s
                            )
                            ON CONFLICT(
                                fingerprint
                            )
                            DO NOTHING
                            """,
                            (
                                fp,
                                Json(
                                    alert_payload
                                ),
                            ),
                        )

        settle_paper_trades()

        elapsed = (
            time.monotonic()
            - started
        )

        logger.info(
            "SCAN COMPLETE | "
            "cities=%d | "
            "temp_series=%d | "
            "temp_markets=%d | "
            "rain_markets=%d | "
            "candidate_signals=%d | "
            "selected_signals=%d | "
            "elapsed=%.1fs",
            len(CITIES),
            len(temperature_results),
            sum(
                len(markets)
                for _, markets
                in temperature_results
            ),
            len(rain_markets),
            len(candidates),
            len(selected),
            elapsed,
        )

        logger.info(
            "This scan used raw ensemble-frequency "
            "proxies only; no calibrated probability "
            "claim is being made."
        )

        finish_run(
            run_id,
            True,
            None,
        )

    except Exception as error:
        finish_run(
            run_id,
            False,
            str(error),
        )
        raise


def today_for_city(
    city_data,
):
    return datetime.now(
        ZoneInfo(
            city_data["timezone"]
        )
    ).date().isoformat()


def rain_city_from_ticker(
    ticker,
):
    parts = (
        ticker or ""
    ).split("-")

    if len(parts) < 3:
        return None

    return RAIN_CODE_TO_CITY.get(
        parts[-1]
    )


if __name__ == "__main__":
    start = time.monotonic()

    try:
        logger.info(
            "Starting one-shot weather/Kalshi scan."
        )

        scan()

        logger.info(
            "One-shot scan completed successfully "
            "in %.1f seconds.",
            time.monotonic() - start,
        )

    except Exception:
        logger.exception(
            "One-shot scan FAILED after %.1f seconds.",
            time.monotonic() - start,
        )
        raise
