import hashlib
import json
import logging
import os
import re
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

try:
    import psycopg2
except ImportError:
    psycopg2 = None

try:
    from psycopg2.extras import Json, execute_values
except ImportError:
    Json = None
    execute_values = None


# ==========================================================
# CONFIG
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

WEATHER_REFRESH_SECONDS = int(
    os.environ.get("WEATHER_REFRESH_SECONDS", "1800")
)

WEATHER_WORKERS = int(
    os.environ.get("WEATHER_WORKERS", "4")
)

MIN_FORECAST_PROBABILITY_CHANGE_POINTS = float(
    os.environ.get(
        "MIN_FORECAST_PROBABILITY_CHANGE_POINTS",
        "20",
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

# We keep these model names explicit. If an API rejects a model, that
# failure is surfaced in the scan rather than silently substituting another.
DETERMINISTIC_MODELS = (
    "hrrr",
    "nbm",
    "gfs_seamless",
    "ecmwf_ifs025",
)

ENSEMBLE_MODEL = "gfs_seamless"

# IMPORTANT:
# Only city/station mappings we have verified are eligible for automatic
# paper signals. Every other discovered city is still monitored and stored.
VERIFIED_SIGNAL_CODES = {
    "NYC",
    "CHI",
    "MIA",
    "AUS",
}

CITY_MAP = {
    "NYC": {
        "name": "New York",
        "lat": 40.7789,
        "lon": -73.9692,
        "timezone": "America/New_York",
        "signal_eligible": True,
        "station": "Central Park",
    },
    "CHI": {
        "name": "Chicago",
        "lat": 41.7870,
        "lon": -87.7522,
        "timezone": "America/Chicago",
        "signal_eligible": True,
        "station": "Chicago Midway",
    },
    "MIA": {
        "name": "Miami",
        "lat": 25.7959,
        "lon": -80.2870,
        "timezone": "America/New_York",
        "signal_eligible": True,
        "station": "Miami International Airport",
    },
    "AUS": {
        "name": "Austin",
        "lat": 30.1975,
        "lon": -97.6663,
        "timezone": "America/Chicago",
        "signal_eligible": True,
        "station": "Austin Bergstrom",
    },
    "LAX": {"name": "Los Angeles", "lat": 33.9425, "lon": -118.4081, "timezone": "America/Los_Angeles", "signal_eligible": False, "station": "LAX proxy"},
    "DAL": {"name": "Dallas", "lat": 32.8998, "lon": -97.0403, "timezone": "America/Chicago", "signal_eligible": False, "station": "DFW proxy"},
    "SEA": {"name": "Seattle", "lat": 47.4502, "lon": -122.3088, "timezone": "America/Los_Angeles", "signal_eligible": False, "station": "SEA proxy"},
    "HOU": {"name": "Houston", "lat": 29.6454, "lon": -95.2789, "timezone": "America/Chicago", "signal_eligible": False, "station": "Houston proxy"},
    "OKC": {"name": "Oklahoma City", "lat": 35.3931, "lon": -97.6007, "timezone": "America/Chicago", "signal_eligible": False, "station": "OKC proxy"},
    "PHIL": {"name": "Philadelphia", "lat": 39.8744, "lon": -75.2424, "timezone": "America/New_York", "signal_eligible": False, "station": "PHL proxy"},
    "PHX": {"name": "Phoenix", "lat": 33.4342, "lon": -112.0116, "timezone": "America/Phoenix", "signal_eligible": False, "station": "PHX proxy"},
    "SFO": {"name": "San Francisco", "lat": 37.6213, "lon": -122.3790, "timezone": "America/Los_Angeles", "signal_eligible": False, "station": "SFO proxy"},
    "LV": {"name": "Las Vegas", "lat": 36.0840, "lon": -115.1537, "timezone": "America/Los_Angeles", "signal_eligible": False, "station": "LAS proxy"},
    "MIN": {"name": "Minneapolis", "lat": 44.8848, "lon": -93.2223, "timezone": "America/Chicago", "signal_eligible": False, "station": "MSP proxy"},
    "NOLA": {"name": "New Orleans", "lat": 30.0424, "lon": -90.0289, "timezone": "America/Chicago", "signal_eligible": False, "station": "MSY proxy"},
    "DEN": {"name": "Denver", "lat": 39.8561, "lon": -104.6737, "timezone": "America/Denver", "signal_eligible": False, "station": "DEN proxy"},
    "TTN": {"name": "Trenton", "lat": 40.2767, "lon": -74.8135, "timezone": "America/New_York", "signal_eligible": False, "station": "TTN proxy"},
    "EWR": {"name": "Newark", "lat": 40.6895, "lon": -74.1745, "timezone": "America/New_York", "signal_eligible": False, "station": "EWR proxy"},
    "DC": {"name": "Washington DC", "lat": 38.8512, "lon": -77.0402, "timezone": "America/New_York", "signal_eligible": False, "station": "DCA proxy"},
    "BOS": {"name": "Boston", "lat": 42.3656, "lon": -71.0096, "timezone": "America/New_York", "signal_eligible": False, "station": "BOS proxy"},
    "ATL": {"name": "Atlanta", "lat": 33.6407, "lon": -84.4277, "timezone": "America/New_York", "signal_eligible": False, "station": "ATL proxy"},
    "SATX": {"name": "San Antonio", "lat": 29.5337, "lon": -98.4698, "timezone": "America/Chicago", "signal_eligible": False, "station": "SATX proxy"},
}

ALIASES = {
    "NEW YORK": "NYC",
    "NEW YORK CITY": "NYC",
    "CHICAGO": "CHI",
    "MIAMI": "MIA",
    "AUSTIN": "AUS",
    "LOS ANGELES": "LAX",
    "DALLAS": "DAL",
    "SEATTLE": "SEA",
    "HOUSTON": "HOU",
    "OKLAHOMA CITY": "OKC",
    "PHILADELPHIA": "PHIL",
    "PHOENIX": "PHX",
    "SAN FRANCISCO": "SFO",
    "LAS VEGAS": "LV",
    "MINNEAPOLIS": "MIN",
    "NEW ORLEANS": "NOLA",
    "DENVER": "DEN",
    "TRENTON": "TTN",
    "NEWARK": "EWR",
    "WASHINGTON DC": "DC",
    "BOSTON": "BOS",
    "ATLANTA": "ATL",
    "SAN ANTONIO": "SATX",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger(
    "weather-kalshi-scanner"
)


# ==========================================================
# GENERAL HELPERS
# ==========================================================

def utc_now():
    return datetime.now(timezone.utc)


def safe_float(value, default=None):
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def local_date(timestamp, timezone_name):
    parsed = datetime.fromisoformat(
        str(timestamp).replace(
            "Z",
            "+00:00",
        )
    )

    if parsed.tzinfo is None:
        parsed = parsed.replace(
            tzinfo=timezone.utc
        )

    return parsed.astimezone(
        ZoneInfo(timezone_name)
    ).date().isoformat()


def today_for_code(code):
    return datetime.now(
        ZoneInfo(
            CITY_MAP[code]["timezone"]
        )
    ).date().isoformat()


def parse_market_date(market):
    event_ticker = (
        market.get("event_ticker")
        or market.get("ticker")
        or ""
    )

    for part in event_ticker.split("-"):
        try:
            return datetime.strptime(
                part,
                "%y%b%d",
            ).date().isoformat()
        except ValueError:
            continue

    return None


def code_from_title_or_ticker(series):
    title = (
        series.get("title")
        or ""
    ).upper()

    ticker = (
        series.get("ticker")
        or ""
    ).upper()

    for code in CITY_MAP:
        if re.search(
            rf"\b{re.escape(code)}\b",
            title,
        ):
            return code

    if ticker.startswith(
        "KXHIGH"
    ):
        suffix = ticker[
            len("KXHIGH"):
        ]

        if suffix in CITY_MAP:
            return suffix

    for alias, code in ALIASES.items():
        if alias in title:
            return code

    return None


def rain_code_from_ticker(ticker):
    suffix = (
        ticker
        or ""
    ).rsplit(
        "-",
        1,
    )[-1].upper()

    return (
        suffix
        if suffix in CITY_MAP
        else None
    )


def payload_hash(payload):
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


# ==========================================================
# HTTP
# ==========================================================

def http_json(session, url, params=None):
    last_error = None

    for attempt in range(3):
        try:
            response = session.get(
                url,
                params=params,
                headers={
                    "User-Agent": "WeatherKalshiResearchBot/5.0",
                    "Accept": "application/json",
                },
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                retry_after = safe_float(
                    response.headers.get("Retry-After"),
                    2.0,
                ) or 2.0

                time.sleep(
                    min(
                        max(
                            retry_after,
                            1.0,
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

            data = response.json()

            if not isinstance(data, dict):
                raise RuntimeError(
                    "Expected a JSON object."
                )

            if data.get("error"):
                raise RuntimeError(
                    f"API error: {data['error']}"
                )

            return data

        except Exception as exc:
            last_error = exc

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
CREATE TABLE IF NOT EXISTS scan_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    stats JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT
);

CREATE TABLE IF NOT EXISTS series_registry (
    series_ticker TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    category TEXT,
    tags JSONB NOT NULL,
    settlement_sources JSONB NOT NULL,
    contract_terms_url TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    raw_series JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS forecast_observations (
    id BIGSERIAL PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL,
    city TEXT NOT NULL,
    variable TEXT NOT NULL,
    model TEXT NOT NULL,
    forecast_date DATE NOT NULL,
    scalar_value DOUBLE PRECISION,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    UNIQUE(
        city,
        variable,
        model,
        forecast_date,
        payload_hash
    )
);

CREATE INDEX IF NOT EXISTS idx_forecast_lookup
ON forecast_observations(
    city,
    variable,
    model,
    forecast_date,
    observed_at DESC
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    id BIGSERIAL PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    event_ticker TEXT,
    series_ticker TEXT,
    market_date DATE,
    city TEXT,
    market_kind TEXT NOT NULL,
    strike_type TEXT,
    floor_strike DOUBLE PRECISION,
    cap_strike DOUBLE PRECISION,
    yes_bid_cents DOUBLE PRECISION,
    yes_ask_cents DOUBLE PRECISION,
    no_bid_cents DOUBLE PRECISION,
    no_ask_cents DOUBLE PRECISION,
    last_price_cents DOUBLE PRECISION,
    status TEXT,
    result TEXT
);

CREATE INDEX IF NOT EXISTS idx_market_lookup
ON market_snapshots(
    ticker,
    observed_at DESC
);

CREATE TABLE IF NOT EXISTS paper_trades (
    id BIGSERIAL PRIMARY KEY,
    signal_fingerprint TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    settled_at TIMESTAMPTZ,
    city TEXT NOT NULL,
    forecast_date DATE NOT NULL,
    market_ticker TEXT NOT NULL,
    market_kind TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('YES','NO')),
    entry_price_cents DOUBLE PRECISION NOT NULL,
    stake_dollars DOUBLE PRECISION NOT NULL,
    contracts DOUBLE PRECISION NOT NULL,
    model_probability_proxy DOUBLE PRECISION NOT NULL,
    preliminary_edge_points DOUBLE PRECISION NOT NULL,
    forecast_probability_change_points DOUBLE PRECISION NOT NULL,
    market_price_change_points DOUBLE PRECISION NOT NULL,
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
    sent_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);
"""


def require_db():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is missing."
        )

    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2-binary is not installed."
        )


def ensure_schema():
    require_db()

    with psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,
    ) as conn:
        with conn.cursor() as cur:
            for statement in SCHEMA.split(";"):
                statement = statement.strip()

                if statement:
                    cur.execute(
                        statement
                    )


def db_latest_forecast(
    conn,
    city,
    variable,
    model,
    forecast_date,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                observed_at,
                scalar_value,
                payload
            FROM forecast_observations
            WHERE city=%s
              AND variable=%s
              AND model=%s
              AND forecast_date=%s
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            (
                city,
                variable,
                model,
                forecast_date,
            ),
        )

        return cur.fetchone()


def db_latest_market(
    conn,
    ticker,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                observed_at,
                yes_bid_cents,
                yes_ask_cents,
                no_bid_cents,
                no_ask_cents,
                last_price_cents
            FROM market_snapshots
            WHERE ticker=%s
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            (ticker,),
        )

        return cur.fetchone()


def db_latest_weather_refresh(
    conn,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(completed_at)
            FROM scan_runs
            WHERE status='success'
              AND COALESCE(
                  (stats->>'weather_refreshed')::boolean,
                  false
              ) = true
            """
        )

        row = cur.fetchone()

    return row[0] if row else None


def start_scan(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scan_runs(
                started_at,
                status,
                stats
            )
            VALUES(
                NOW(),
                'running',
                '{}'::jsonb
            )
            RETURNING id
            """
        )

        return cur.fetchone()[0]


def finish_scan(
    conn,
    scan_id,
    status,
    stats,
    error=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE scan_runs
            SET completed_at=NOW(),
                status=%s,
                stats=%s,
                error=%s
            WHERE id=%s
            """,
            (
                status,
                Json(stats),
                error,
                scan_id,
            ),
        )


# ==========================================================
# KALSHI
# ==========================================================

def get_series_list(session):
    output = []
    cursor = None

    for _ in range(20):
        params = {
            "category": "Climate and Weather",
        }

        if cursor:
            params["cursor"] = cursor

        data = http_json(
            session,
            f"{KALSHI_API_URL}/series",
            params,
        )

        output.extend(
            data.get("series", [])
        )

        cursor = data.get(
            "cursor"
        )

        if not cursor:
            break

    return output


def save_series_registry(
    conn,
    series_list,
):
    rows = []

    for series in series_list:
        ticker = (
            series.get("ticker")
            or ""
        )

        if not ticker:
            continue

        rows.append(
            (
                ticker,
                series.get("title", ""),
                series.get("category"),
                Json(
                    series.get(
                        "tags",
                        [],
                    )
                ),
                Json(
                    series.get(
                        "settlement_sources",
                        [],
                    )
                ),
                series.get(
                    "contract_terms_url"
                ),
                Json(series),
            )
        )

    if not rows:
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO series_registry(
                series_ticker,
                title,
                category,
                tags,
                settlement_sources,
                contract_terms_url,
                updated_at,
                raw_series
            )
            VALUES(
                %s,%s,%s,%s,%s,%s,NOW(),%s
            )
            ON CONFLICT(series_ticker)
            DO UPDATE SET
                title=EXCLUDED.title,
                category=EXCLUDED.category,
                tags=EXCLUDED.tags,
                settlement_sources=EXCLUDED.settlement_sources,
                contract_terms_url=EXCLUDED.contract_terms_url,
                updated_at=NOW(),
                raw_series=EXCLUDED.raw_series
            """,
            rows,
            page_size=500,
        )


def get_markets_for_series(
    session,
    series_ticker,
):
    output = []
    cursor = None

    for _ in range(20):
        params = {
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 1000,
        }

        if cursor:
            params["cursor"] = cursor

        data = http_json(
            session,
            f"{KALSHI_API_URL}/markets",
            params,
        )

        output.extend(
            data.get("markets", [])
        )

        cursor = data.get(
            "cursor"
        )

        if not cursor:
            break

    return output


def is_temperature_series(series):
    title = (
        series.get("title")
        or ""
    ).lower()

    frequency = (
        series.get("frequency")
        or ""
    ).lower()

    return (
        frequency == "daily"
        and (
            "temperature"
            in title
        )
        and (
            "high"
            in title
            or "highest"
            in title
            or "maximum"
            in title
        )
    )


def is_rain_series(series):
    ticker = (
        series.get("ticker")
        or ""
    ).upper()

    title = (
        series.get("title")
        or ""
    ).lower()

    return (
        ticker == "KXRAIN"
        or (
            frequency_is_daily(series)
            and (
                "rain" in title
                or "precipitation" in title
            )
        )
    )


def frequency_is_daily(series):
    return (
        (
            series.get(
                "frequency"
            )
            or ""
        ).lower()
        == "daily"
    )


# ==========================================================
# WEATHER
# ==========================================================

def weather_params(city_codes):
    return {
        "latitude": ",".join(
            str(CITY_MAP[
                code
            ]["lat"])
            for code in city_codes
        ),
        "longitude": ",".join(
            str(CITY_MAP[
                code
            ]["lon"])
            for code in city_codes
        ),
        "hourly": (
            "temperature_2m,"
            "precipitation"
        ),
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "timezone": "UTC",
        "forecast_days": FORECAST_DAYS,
    }


def aggregate_deterministic(
    location_data,
    code,
):
    hourly = location_data.get(
        "hourly",
        {},
    )

    timestamps = hourly.get(
        "time",
        [],
    )

    temps = hourly.get(
        "temperature_2m",
        [],
    )

    rains = hourly.get(
        "precipitation",
        [],
    )

    buckets = {}

    for index, timestamp in enumerate(
        timestamps
    ):
        date_key = local_date(
            timestamp,
            CITY_MAP[code][
                "timezone"
            ],
        )

        bucket = buckets.setdefault(
            date_key,
            {
                "temps": [],
                "rain": [],
            },
        )

        if index < len(temps):
            value = safe_float(
                temps[index]
            )
            if value is not None:
                bucket[
                    "temps"
                ].append(value)

        if index < len(rains):
            value = safe_float(
                rains[index]
            )
            if value is not None:
                bucket[
                    "rain"
                ].append(value)

    result = {}

    for date_key, bucket in (
        buckets.items()
    ):
        if not bucket["temps"]:
            continue

        result[
            date_key
        ] = {
            "high": max(
                bucket["temps"]
            ),
            "precipitation_sum": sum(
                bucket["rain"]
            ),
        }

    return result


def fetch_deterministic(
    session,
    model,
    city_codes,
):
    params = weather_params(
        city_codes
    )

    params[
        "models"
    ] = model

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

    data = http_json(
        session,
        url,
        params,
    )

    locations = (
        data
        if isinstance(data, list)
        else [data]
    )

    if len(locations) != len(
        city_codes
    ):
        raise RuntimeError(
            f"{model}: expected "
            f"{len(city_codes)} locations, "
            f"got {len(locations)}."
        )

    return {
        code: {
            "daily": aggregate_deterministic(
                location,
                code,
            ),
            "model_run": (
                location.get(
                    "model_run"
                )
                or location.get(
                    "model_run_id"
                )
                or location.get(
                    "model_run_time"
                )
            ),
        }
        for code, location
        in zip(
            city_codes,
            locations,
        )
    }


def aggregate_ensemble(
    location_data,
    code,
):
    hourly = location_data.get(
        "hourly",
        {},
    )

    timestamps = hourly.get(
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
            f"{code}: ensemble temperature "
            "members were not returned."
        )

    if not rain_keys:
        raise RuntimeError(
            f"{code}: ensemble precipitation "
            "members were not returned."
        )

    local_tz = ZoneInfo(
        CITY_MAP[code][
            "timezone"
        ]
    )

    buckets = {}

    for index, timestamp in enumerate(
        timestamps
    ):
        parsed = datetime.fromisoformat(
            str(timestamp).replace(
                "Z",
                "+00:00",
            )
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(
                tzinfo=timezone.utc
            )

        date_key = (
            parsed.astimezone(
                local_tz
            ).date().isoformat()
        )

        day = buckets.setdefault(
            date_key,
            {
                "temps": {
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
                    "temps"
                ][key].append(value)

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

    result = {}

    for date_key, day in (
        buckets.items()
    ):
        member_highs = []

        for key in temp_keys:
            values = day[
                "temps"
            ][key]

            if values:
                member_highs.append(
                    max(values)
                )

        member_rain = [
            day[
                "rain"
            ].get(
                key,
                0.0,
            )
            for key in rain_keys
        ]

        if member_highs and member_rain:
            result[
                date_key
            ] = {
                "member_highs": member_highs,
                "member_rain_totals": member_rain,
            }

    return result


def fetch_ensemble(
    session,
    city_codes,
):
    params = weather_params(
        city_codes
    )

    params[
        "models"
    ] = ENSEMBLE_MODEL

    data = http_json(
        session,
        "https://ensemble-api.open-meteo.com/v1/ensemble",
        params,
    )

    locations = (
        data
        if isinstance(data, list)
        else [data]
    )

    if len(locations) != len(
        city_codes
    ):
        raise RuntimeError(
            "Ensemble response returned "
            f"{len(locations)} locations; "
            f"expected {len(city_codes)}."
        )

    output = {}

    for code, location in zip(
        city_codes,
        locations,
    ):
        output[
            code
        ] = {
            "daily": aggregate_ensemble(
                location,
                code,
            ),
            "model_run": (
                location.get(
                    "model_run"
                )
                or location.get(
                    "model_run_id"
                )
                or location.get(
                    "model_run_time"
                )
            ),
        }

    return output


# ==========================================================
# SIGNAL MATH
# ==========================================================

def temperature_probability(
    member_highs,
    market,
):
    if not member_highs:
        return None

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

    if (
        strike_type
        == "greater"
        and floor is not None
    ):
        hits = sum(
            value > floor
            for value in member_highs
        )

    elif (
        strike_type
        == "less"
        and cap is not None
    ):
        hits = sum(
            value < cap
            for value in member_highs
        )

    elif (
        strike_type
        == "between"
        and floor is not None
        and cap is not None
    ):
        hits = sum(
            floor
            <= value
            <= cap
            for value in member_highs
        )

    else:
        return None

    return (
        100.0
        * hits
        / len(member_highs)
    )


def rain_probability(
    member_rain_totals,
):
    if not member_rain_totals:
        return None

    # Research proxy for the current KXRAIN YES definition:
    # total precipitation > 0 inches.
    hits = sum(
        value > 0.0
        for value in member_rain_totals
    )

    return (
        100.0
        * hits
        / len(member_rain_totals)
    )


def side_ask_cents(
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

    return (
        None
        if value is None
        else value * 100.0
    )


def side_market_change(
    current_ask,
    previous_market,
    side,
):
    if previous_market is None:
        return None

    previous_ask = (
        safe_float(
            previous_market[2]
            if side == "YES"
            else previous_market[4]
        )
    )

    if previous_ask is None:
        return None

    return (
        current_ask
        - previous_ask
    )


def make_candidate(
    city,
    forecast_date,
    market,
    kind,
    current_probability,
    previous_probability,
    previous_market,
    temperature_change,
):
    if (
        current_probability is None
        or previous_probability is None
        or previous_market is None
    ):
        return None

    probability_change = (
        current_probability
        - previous_probability
    )

    if (
        abs(probability_change)
        < MIN_FORECAST_PROBABILITY_CHANGE_POINTS
    ):
        return None

    best = None

    for side in (
        "YES",
        "NO",
    ):
        ask = side_ask_cents(
            market,
            side,
        )

        if ask is None:
            continue

        if not (
            MIN_ENTRY_PRICE_CENTS
            <= ask
            <= MAX_ENTRY_PRICE_CENTS
        ):
            continue

        current_side_probability = (
            current_probability
            if side == "YES"
            else 100.0
            - current_probability
        )

        previous_side_probability = (
            previous_probability
            if side == "YES"
            else 100.0
            - previous_probability
        )

        side_probability_change = (
            current_side_probability
            - previous_side_probability
        )

        market_change = (
            side_market_change(
                ask,
                previous_market,
                side,
            )
        )

        if market_change is None:
            continue

        # A useful lag requires the market to have moved in the same
        # direction as the forecast signal, but not by as much.
        if (
            side_probability_change
            == 0
        ):
            continue

        if (
            side_probability_change
            * market_change
            > 0
        ):
            lag = (
                abs(
                    side_probability_change
                )
                - abs(
                    market_change
                )
            )

            lag = max(
                0.0,
                lag,
            )
        else:
            # A flat/counter-moving market is treated as having
            # received essentially none of the forecast shock.
            lag = abs(
                side_probability_change
            )

        edge = (
            current_side_probability
            - ask
        )

        if (
            lag
            < MIN_MARKET_LAG_POINTS
            or edge
            < MIN_PRELIMINARY_EDGE_POINTS
        ):
            continue

        candidate = {
            "city": city,
            "forecast_date": forecast_date,
            "market_ticker": market[
                "ticker"
            ],
            "market_kind": kind,
            "side": side,
            "entry_price_cents": ask,
            "model_probability_proxy": (
                current_side_probability
            ),
            "preliminary_edge_points": edge,
            "forecast_probability_change_points": (
                side_probability_change
            ),
            "market_price_change_points": (
                market_change
            ),
            "market_lag_points": lag,
            "forecast_temperature_change_f": (
                temperature_change
            ),
            "contract_label": (
                market.get("title")
                or ""
            ),
        }

        if best is None or (
            candidate[
                "market_lag_points"
            ],
            candidate[
                "preliminary_edge_points"
            ],
        ) > (
            best[
                "market_lag_points"
            ],
            best[
                "preliminary_edge_points"
            ],
        ):
            best = candidate

    return best


# ==========================================================
# PAPER TRADING / DISCORD
# ==========================================================

def signal_fingerprint(
    signal,
):
    raw = "|".join(
        [
            signal["market_ticker"],
            signal["side"],
            signal["forecast_date"],
            f"{signal['entry_price_cents']:.2f}",
            f"{signal['model_probability_proxy']:.2f}",
            f"{signal['forecast_probability_change_points']:.2f}",
            f"{signal['market_price_change_points']:.2f}",
        ]
    )

    return hashlib.sha256(
        raw.encode()
    ).hexdigest()[:32]


def open_paper_trade(
    conn,
    signal,
    reason,
):
    fp = signal_fingerprint(
        signal
    )

    entry = (
        signal["entry_price_cents"]
        / 100.0
    )

    if entry <= 0:
        return False, fp

    contracts = (
        PAPER_RISK_DOLLARS
        / entry
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO paper_trades(
                signal_fingerprint,
                created_at,
                city,
                forecast_date,
                market_ticker,
                market_kind,
                side,
                entry_price_cents,
                stake_dollars,
                contracts,
                model_probability_proxy,
                preliminary_edge_points,
                forecast_probability_change_points,
                market_price_change_points,
                market_lag_points,
                forecast_temperature_change_f,
                reason,
                status
            )
            VALUES(
                %s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,'open'
            )
            ON CONFLICT(signal_fingerprint)
            DO NOTHING
            """,
            (
                fp,
                signal["city"],
                signal["forecast_date"],
                signal["market_ticker"],
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
                    "forecast_probability_change_points"
                ],
                signal[
                    "market_price_change_points"
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


def already_alerted(
    conn,
    fp,
):
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

        return cur.fetchone() is not None


def record_alert(
    conn,
    fp,
    signal,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alert_log(
                fingerprint,
                sent_at,
                payload
            )
            VALUES(
                %s,NOW(),%s
            )
            ON CONFLICT(fingerprint)
            DO NOTHING
            """,
            (
                fp,
                Json(signal),
            ),
        )


def send_discord(
    message,
):
    if (
        not DISCORD_RELAY_URL
        or not DISCORD_RELAY_SECRET
    ):
        log.warning(
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
                "User-Agent": (
                    "WeatherKalshiResearchBot/5.0"
                ),
                "Accept": "application/json",
            },
            timeout=REQUEST_TIMEOUT,
        )

        log.info(
            "Discord relay response: %s",
            response.status_code,
        )

        if not (
            200
            <= response.status_code
            < 300
        ):
            log.error(
                "Discord relay error: %s",
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
            log.error(
                "Discord relay reported failure: %s",
                payload,
            )
            return False

        return True

    except Exception as exc:
        log.error(
            "Discord relay exception: %s",
            exc,
        )
        return False


def signal_message(
    signal,
):
    direction = (
        "up"
        if signal[
            "forecast_probability_change_points"
        ] > 0
        else "down"
    )

    temp = signal.get(
        "forecast_temperature_change_f"
    )

    temp_line = (
        f"HRRR high change: **{temp:+.1f}°F**\n"
        if temp is not None
        else ""
    )

    return (
        "🌦️ **WEATHER FORECAST SHOCK — PAPER TRADE**\n\n"
        f"**{signal['city']} — "
        f"{signal['forecast_date']}**\n"
        f"Market: `{signal['market_ticker']}`\n"
        f"Type: **{signal['market_kind']}**\n"
        f"Side: **{signal['side']}**\n"
        f"Entry ask: **{signal['entry_price_cents']:.1f}¢**\n\n"
        f"Raw ensemble probability proxy: "
        f"**{signal['model_probability_proxy']:.1f}%**\n"
        f"Forecast probability change: "
        f"**{direction} "
        f"{abs(signal['forecast_probability_change_points']):.1f} pts**\n"
        f"Market ask change: "
        f"**{signal['market_price_change_points']:+.1f} pts**\n"
        f"Estimated market lag: "
        f"**{signal['market_lag_points']:.1f} pts**\n"
        f"Preliminary edge: "
        f"**{signal['preliminary_edge_points']:+.1f} pts**\n"
        f"{temp_line}\n"
        f"Paper risk: "
        f"**${PAPER_RISK_DOLLARS:.2f}**\n\n"
        "⚠️ Paper trading only. "
        "The ensemble frequency is an uncalibrated "
        "research proxy, not a proven fair probability."
    )


def settle_paper_trades(
    conn,
    session,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                market_ticker,
                side,
                stake_dollars,
                contracts
            FROM paper_trades
            WHERE status='open'
            ORDER BY created_at
            LIMIT 500
            """
        )

        trades = cur.fetchall()

    if not trades:
        return 0

    tickers = ",".join(
        sorted(
            {
                row[1]
                for row in trades
            }
        )
    )

    try:
        data = http_json(
            session,
            f"{KALSHI_API_URL}/markets",
            {
                "tickers": tickers,
                "limit": 1000,
            },
        )
    except Exception as exc:
        log.warning(
            "Paper-trade settlement check failed: %s",
            exc,
        )
        return 0

    markets = {
        market.get("ticker"): market
        for market in data.get(
            "markets",
            [],
        )
    }

    settled = 0

    with conn.cursor() as cur:
        for (
            trade_id,
            ticker,
            side,
            stake,
            contracts,
        ) in trades:
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
                result
                == side.lower()
            )

            pnl = (
                float(contracts)
                - float(stake)
                if won
                else -float(stake)
            )

            cur.execute(
                """
                UPDATE paper_trades
                SET
                    settled_at=NOW(),
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

            settled += (
                1
                if cur.rowcount
                else 0
            )

    return settled


# ==========================================================
# SNAPSHOT WRITES
# ==========================================================

def forecast_rows(
    deterministic,
    ensemble,
):
    rows = []

    for model, cities in (
        deterministic.items()
    ):
        for code, data in (
            cities.items()
        ):
            city = CITY_MAP[
                code
            ]["name"]

            for date_key, daily in (
                data["daily"].items()
            ):
                high_payload = {
                    "high": daily[
                        "high"
                    ]
                }

                rain_payload = {
                    "precipitation_sum": daily[
                        "precipitation_sum"
                    ]
                }

                rows.append(
                    (
                        city,
                        "temperature_high",
                        model,
                        date_key,
                        daily["high"],
                        Json(high_payload),
                        payload_hash(
                            high_payload
                        ),
                    )
                )

                rows.append(
                    (
                        city,
                        "precipitation_sum",
                        model,
                        date_key,
                        daily[
                            "precipitation_sum"
                        ],
                        Json(rain_payload),
                        payload_hash(
                            rain_payload
                        ),
                    )
                )

    for code, data in (
        ensemble.items()
    ):
        city = CITY_MAP[
            code
        ]["name"]

        for date_key, daily in (
            data["daily"].items()
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

            rows.append(
                (
                    city,
                    "ensemble_temperature_distribution",
                    ENSEMBLE_MODEL,
                    date_key,
                    statistics.mean(
                        daily[
                            "member_highs"
                        ]
                    ),
                    Json(
                        temp_payload
                    ),
                    payload_hash(
                        temp_payload
                    ),
                )
            )

            rows.append(
                (
                    city,
                    "ensemble_rain_distribution",
                    ENSEMBLE_MODEL,
                    date_key,
                    statistics.mean(
                        daily[
                            "member_rain_totals"
                        ]
                    ),
                    Json(
                        rain_payload
                    ),
                    payload_hash(
                        rain_payload
                    ),
                )
            )

    return rows


def write_forecasts(
    conn,
    rows,
):
    if not rows:
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO forecast_observations(
                observed_at,
                city,
                variable,
                model,
                forecast_date,
                scalar_value,
                payload,
                payload_hash
            )
            VALUES(
                NOW(),%s,%s,%s,%s,%s,%s,%s
            )
            ON CONFLICT(
                city,
                variable,
                model,
                forecast_date,
                payload_hash
            )
            DO NOTHING
            """,
            rows,
            page_size=500,
        )


def market_rows(
    entries,
):
    rows = []

    for entry in entries:
        market = entry[
            "market"
        ]

        def cents(value):
            number = safe_float(
                value
            )
            return (
                None
                if number is None
                else number * 100.0
            )

        rows.append(
            (
                market.get(
                    "ticker"
                )
                or "",
                market.get(
                    "event_ticker"
                ),
                market.get(
                    "series_ticker"
                ),
                entry["date"],
                CITY_MAP[
                    entry["code"]
                ]["name"],
                entry["kind"],
                market.get(
                    "strike_type"
                ),
                safe_float(
                    market.get(
                        "floor_strike"
                    )
                ),
                safe_float(
                    market.get(
                        "cap_strike"
                    )
                ),
                cents(
                    market.get(
                        "yes_bid_dollars"
                    )
                ),
                cents(
                    market.get(
                        "yes_ask_dollars"
                    )
                ),
                cents(
                    market.get(
                        "no_bid_dollars"
                    )
                ),
                cents(
                    market.get(
                        "no_ask_dollars"
                    )
                ),
                cents(
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
            )
        )

    return rows


def write_markets(
    conn,
    rows,
):
    if not rows:
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO market_snapshots(
                observed_at,
                ticker,
                event_ticker,
                series_ticker,
                market_date,
                city,
                market_kind,
                strike_type,
                floor_strike,
                cap_strike,
                yes_bid_cents,
                yes_ask_cents,
                no_bid_cents,
                no_ask_cents,
                last_price_cents,
                status,
                result
            )
            VALUES(
                NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s
            )
            """,
            rows,
            page_size=500,
        )


# ==========================================================
# SIGNAL PROCESSING
# ==========================================================

def process_temperature_signals(
    conn,
    temp_entries,
    ensemble,
    deterministic,
    stats,
):
    candidates = []

    for entry in temp_entries:
        code = entry[
            "code"
        ]

        if not CITY_MAP[
            code
        ]["signal_eligible"]:
            continue

        market = entry[
            "market"
        ]

        forecast_date = entry[
            "date"
        ]

        daily = (
            ensemble
            .get(code, {})
            .get("daily", {})
            .get(forecast_date)
        )

        if not daily:
            continue

        city = CITY_MAP[
            code
        ]["name"]

        previous = db_latest_forecast(
            conn,
            city,
            "ensemble_temperature_distribution",
            ENSEMBLE_MODEL,
            forecast_date,
        )

        if previous is None:
            continue

        previous_members = (
            (previous[2] or {})
            .get(
                "member_highs",
                [],
            )
        )

        current_members = (
            daily[
                "member_highs"
            ]
        )

        if not previous_members:
            continue

        current_hrrr = (
            deterministic
            .get("hrrr", {})
            .get(code, {})
            .get("daily", {})
            .get(forecast_date, {})
            .get("high")
        )

        previous_hrrr = db_latest_forecast(
            conn,
            city,
            "temperature_high",
            "hrrr",
            forecast_date,
        )

        hrrr_change = None

        if (
            current_hrrr is not None
            and previous_hrrr is not None
        ):
            old_hrrr = safe_float(
                previous_hrrr[1]
            )

            if old_hrrr is not None:
                hrrr_change = (
                    current_hrrr
                    - old_hrrr
                )

        strike = temperature_probability(
            current_members,
            market,
        )

        previous_probability = (
            temperature_probability(
                previous_members,
                market,
            )
        )

        if (
            strike is None
            or previous_probability is None
        ):
            continue

        previous_market = (
            db_latest_market(
                conn,
                market[
                    "ticker"
                ],
            )
        )

        candidate = make_candidate(
            city,
            forecast_date,
            market,
            "temperature",
            strike,
            previous_probability,
            previous_market,
            hrrr_change,
        )

        if candidate:
            candidates.append(
                candidate
            )

    candidates.sort(
        key=lambda item: (
            item[
                "market_lag_points"
            ],
            item[
                "preliminary_edge_points"
            ],
        ),
        reverse=True,
    )

    return candidates


def process_rain_signals(
    conn,
    rain_entries,
    ensemble,
):
    candidates = []

    for entry in rain_entries:
        code = entry[
            "code"
        ]

        if not CITY_MAP[
            code
        ]["signal_eligible"]:
            continue

        market = entry[
            "market"
        ]

        forecast_date = entry[
            "date"
        ]

        daily = (
            ensemble
            .get(code, {})
            .get("daily", {})
            .get(forecast_date)
        )

        if not daily:
            continue

        city = CITY_MAP[
            code
        ]["name"]

        previous = db_latest_forecast(
            conn,
            city,
            "ensemble_rain_distribution",
            ENSEMBLE_MODEL,
            forecast_date,
        )

        if previous is None:
            continue

        previous_rain = (
            (previous[2] or {})
            .get(
                "member_rain_totals",
                [],
            )
        )

        current_rain = daily[
            "member_rain_totals"
        ]

        if not previous_rain:
            continue

        current_probability = (
            rain_probability(
                current_rain
            )
        )

        previous_probability = (
            rain_probability(
                previous_rain
            )
        )

        previous_market = (
            db_latest_market(
                conn,
                market[
                    "ticker"
                ],
            )
        )

        candidate = make_candidate(
            city,
            forecast_date,
            market,
            "rain",
            current_probability,
            previous_probability,
            previous_market,
            None,
        )

        if candidate:
            candidates.append(
                candidate
            )

    candidates.sort(
        key=lambda item: (
            item[
                "market_lag_points"
            ],
            item[
                "preliminary_edge_points"
            ],
        ),
        reverse=True,
    )

    return candidates


# ==========================================================
# SCANNER
# ==========================================================

def run_scan():
    require_db()

    ensure_schema()

    session = requests.Session()

    with psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,
    ) as conn:

        scan_id = start_scan(
            conn
        )

        stats = {
            "temperature_series": 0,
            "temperature_markets": 0,
            "rain_series": 0,
            "rain_markets": 0,
            "weather_refreshed": False,
            "forecast_candidates": 0,
            "paper_trades_created": 0,
            "discord_alerts": 0,
            "settled_trades": 0,
        }

        started = time.monotonic()

        try:
            log.info(
                "=================================================="
            )
            log.info(
                "STARTING WEATHER MARKET SCAN"
            )
            log.info(
                "UTC: %s",
                utc_now().isoformat(),
            )
            log.info(
                "=================================================="
            )

            all_series = get_series_list(
                session
            )

            save_series_registry(
                conn,
                all_series,
            )

            temp_series = [
                series
                for series in all_series
                if is_temperature_series(
                    series
                )
            ]

            rain_series = [
                series
                for series in all_series
                if is_rain_series(
                    series
                )
            ]

            stats[
                "temperature_series"
            ] = len(
                temp_series
            )

            stats[
                "rain_series"
            ] = len(
                rain_series
            )

            # Fetch KXRAIN exactly once this scan.
            rain_markets = []

            for series in rain_series:
                if (
                    (
                        series.get("ticker")
                        or ""
                    ).upper()
                    == "KXRAIN"
                ):
                    rain_markets = (
                        get_markets_for_series(
                            session,
                            "KXRAIN",
                        )
                    )
                    break

            stats[
                "rain_markets"
            ] = len(
                rain_markets
            )

            # Map all currently observed weather cities to forecast points.
            codes = set()

            for series in temp_series:
                code = code_from_title_or_ticker(
                    series
                )

                if code:
                    codes.add(
                        code
                    )

            for market in rain_markets:
                code = rain_code_from_ticker(
                    market.get(
                        "ticker"
                    )
                )

                if code:
                    codes.add(
                        code
                    )

            city_codes = sorted(
                codes
            )

            if not city_codes:
                raise RuntimeError(
                    "No current Kalshi weather markets "
                    "could be mapped to forecast coordinates."
                )

            last_weather_refresh = (
                db_latest_weather_refresh(
                    conn
                )
            )

            refresh_weather = (
                last_weather_refresh is None
                or (
                    utc_now()
                    - last_weather_refresh
                ).total_seconds()
                >= WEATHER_REFRESH_SECONDS
            )

            deterministic = {}
            ensemble = {}

            if refresh_weather:
                stats[
                    "weather_refreshed"
                ] = True

                log.info(
                    "Refreshing weather models for %d cities.",
                    len(city_codes),
                )

                with ThreadPoolExecutor(
                    max_workers=WEATHER_WORKERS
                ) as executor:

                    jobs = {
                        executor.submit(
                            fetch_deterministic,
                            session,
                            model,
                            city_codes,
                        ): model
                        for model in (
                            DETERMINISTIC_MODELS
                        )
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

                        log.info(
                            "Weather model fetched: %s",
                            model,
                        )

                ensemble = fetch_ensemble(
                    session,
                    city_codes,
                )

                temp_entries = []

                for series in temp_series:
                    code = (
                        code_from_title_or_ticker(
                            series
                        )
                    )

                    if not code:
                        continue

                    markets = (
                        get_markets_for_series(
                            session,
                            series[
                                "ticker"
                            ],
                        )
                    )

                    stats[
                        "temperature_markets"
                    ] += len(markets)

                    for market in markets:
                        date_key = parse_market_date(
                            market
                        )

                        if not date_key:
                            continue

                        if date_key < today_for_code(
                            code
                        ):
                            continue

                        temp_entries.append(
                            {
                                "market": market,
                                "code": code,
                                "date": date_key,
                            }
                        )

                rain_entries = []

                for market in rain_markets:
                    code = rain_code_from_ticker(
                        market.get(
                            "ticker"
                        )
                    )

                    if not code:
                        continue

                    date_key = parse_market_date(
                        market
                    )

                    if not date_key:
                        continue

                    if date_key < today_for_code(
                        code
                    ):
                        continue

                    rain_entries.append(
                        {
                            "market": market,
                            "code": code,
                            "date": date_key,
                        }
                    )

                # Crucial ordering:
                # previous forecasts/market snapshots are read during
                # signal evaluation BEFORE current snapshots are written.
                temp_candidates = (
                    process_temperature_signals(
                        conn,
                        temp_entries,
                        ensemble,
                        deterministic,
                        stats,
                    )
                )

                rain_candidates = (
                    process_rain_signals(
                        conn,
                        rain_entries,
                        ensemble,
                    )
                )

                all_candidates = (
                    temp_candidates
                    + rain_candidates
                )

                stats[
                    "forecast_candidates"
                ] = len(
                    all_candidates
                )

                # One strongest candidate per city/date.
                selected = {}

                for candidate in all_candidates:
                    key = (
                        candidate[
                            "city"
                        ],
                        candidate[
                            "forecast_date"
                        ],
                    )

                    old = selected.get(
                        key
                    )

                    if (
                        old is None
                        or (
                            candidate[
                                "market_lag_points"
                            ],
                            candidate[
                                "preliminary_edge_points"
                            ],
                        )
                        > (
                            old[
                                "market_lag_points"
                            ],
                            old[
                                "preliminary_edge_points"
                            ],
                        )
                    ):
                        selected[
                            key
                        ] = candidate

                for signal in selected.values():
                    reason = {
                        "strategy": (
                            "large forecast probability "
                            "shock plus insufficient "
                            "market response"
                        ),
                        "raw_ensemble_probability": True,
                        "calibrated_probability": False,
                        "paper_only": True,
                        "settlement_station": CITY_MAP[
                            next(
                                code
                                for code, item in CITY_MAP.items()
                                if item["name"]
                                == signal["city"]
                            )
                        ]["station"],
                    }

                    created, fp = (
                        open_paper_trade(
                            conn,
                            signal,
                            reason,
                        )
                    )

                    if not created:
                        continue

                    stats[
                        "paper_trades_created"
                    ] += 1

                    if already_alerted(
                        conn,
                        fp,
                    ):
                        continue

                    if send_discord(
                        signal_message(
                            signal
                        )
                    ):
                        record_alert(
                            conn,
                            fp,
                            signal,
                        )

                        stats[
                            "discord_alerts"
                        ] += 1

                        log.info(
                            "DISCORD ALERT SENT | %s | %s",
                            signal[
                                "market_ticker"
                            ],
                            signal[
                                "side"
                            ],
                        )
                    else:
                        log.error(
                            "Discord alert failed; "
                            "paper trade remains recorded."
                        )

                forecast_rows_to_write = (
                    forecast_rows(
                        deterministic,
                        ensemble,
                    )
                )

                write_forecasts(
                    conn,
                    forecast_rows_to_write,
                )

            # Collect current market prices EVERY 5-minute run.
            market_entries = []

            for series in temp_series:
                code = (
                    code_from_title_or_ticker(
                        series
                    )
                )

                if not code:
                    continue

                try:
                    markets = (
                        get_markets_for_series(
                            session,
                            series[
                                "ticker"
                            ],
                        )
                    )

                    stats[
                        "temperature_markets"
                    ] += (
                        0
                        if refresh_weather
                        else len(markets)
                    )

                    for market in markets:
                        date_key = parse_market_date(
                            market
                        )

                        if not date_key:
                            continue

                        if date_key < today_for_code(
                            code
                        ):
                            continue

                        market_entries.append(
                            {
                                "market": market,
                                "code": code,
                                "date": date_key,
                                "kind": "temperature",
                            }
                        )

                except Exception as exc:
                    log.warning(
                        "Temperature market snapshot fetch failed "
                        "for %s: %s",
                        series.get(
                            "ticker"
                        ),
                        exc,
                    )

            for market in rain_markets:
                code = rain_code_from_ticker(
                    market.get(
                        "ticker"
                    )
                )

                if not code:
                    continue

                date_key = parse_market_date(
                    market
                )

                if not date_key:
                    continue

                if date_key < today_for_code(
                    code
                ):
                    continue

                market_entries.append(
                    {
                        "market": market,
                        "code": code,
                        "date": date_key,
                        "kind": "rain",
                    }
                )

            if not refresh_weather:
                # We did not count these above because the exact current
                # list is only assembled for snapshot storage here.
                stats[
                    "rain_markets"
                ] = max(
                    stats[
                        "rain_markets"
                    ],
                    len(rain_markets),
                )

            write_markets(
                conn,
                market_rows(
                    market_entries
                ),
            )

            stats[
                "settled_trades"
            ] = settle_paper_trades(
                conn,
                session,
            )

            finish_scan(
                conn,
                scan_id,
                "success",
                stats,
                None,
            )

            conn.commit()

            log.info(
                "SCAN COMPLETE | %s | runtime=%.1fs",
                json.dumps(
                    stats,
                    default=str,
                ),
                time.monotonic()
                - started,
            )

        except Exception as exc:
            conn.rollback()

            try:
                finish_scan(
                    conn,
                    scan_id,
                    "failed",
                    stats,
                    str(exc),
                )
                conn.commit()
            except Exception:
                conn.rollback()

            log.exception(
                "SCAN FAILED"
            )

            raise


if __name__ == "__main__":
    try:
        run_scan()
    except Exception:
        raise
