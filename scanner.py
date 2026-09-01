import hashlib
import json
import logging
import os
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

ALLOW_UNVERIFIED_LOCATION_SIGNALS = (
    os.environ.get(
        "ALLOW_UNVERIFIED_LOCATION_SIGNALS",
        "false",
    ).lower()
    in {"1", "true", "yes"}
)

DETERMINISTIC_MODELS = (
    "hrrr",
    "nbm",
    "gfs_seamless",
    "ecmwf_ifs025",
)

ENSEMBLE_MODEL = "gfs_seamless"


# ==========================================================
# VERIFIED LOCATION MAP
# ==========================================================
#
# These coordinates are forecast proxies. The four automatic
# signal mappings below are the only ones enabled by default.
# Other rain/temp markets may still be observed and stored.
#

LOCATION_MAP = {
    "NYC": {
        "name": "New York",
        "lat": 40.7789,
        "lon": -73.9692,
        "timezone": "America/New_York",
        "station": "Central Park",
        "signal_eligible": True,
    },
    "CHI": {
        "name": "Chicago",
        "lat": 41.7868,
        "lon": -87.7522,
        "timezone": "America/Chicago",
        "station": "Chicago Midway",
        "signal_eligible": True,
    },
    "MIA": {
        "name": "Miami",
        "lat": 25.7959,
        "lon": -80.2870,
        "timezone": "America/New_York",
        "station": "Miami International Airport",
        "signal_eligible": True,
    },
    "AUS": {
        "name": "Austin",
        "lat": 30.1975,
        "lon": -97.6663,
        "timezone": "America/Chicago",
        "station": "Austin Bergstrom",
        "signal_eligible": True,
    },
    "LAX": {
        "name": "Los Angeles",
        "lat": 33.9425,
        "lon": -118.4081,
        "timezone": "America/Los_Angeles",
        "station": "LAX proxy",
        "signal_eligible": False,
    },
    "DAL": {
        "name": "Dallas",
        "lat": 32.8998,
        "lon": -97.0403,
        "timezone": "America/Chicago",
        "station": "DFW proxy",
        "signal_eligible": False,
    },
    "SEA": {
        "name": "Seattle",
        "lat": 47.4502,
        "lon": -122.3088,
        "timezone": "America/Los_Angeles",
        "station": "SEA proxy",
        "signal_eligible": False,
    },
    "HOU": {
        "name": "Houston",
        "lat": 29.6454,
        "lon": -95.2789,
        "timezone": "America/Chicago",
        "station": "Houston proxy",
        "signal_eligible": False,
    },
    "OKC": {
        "name": "Oklahoma City",
        "lat": 35.3931,
        "lon": -97.6007,
        "timezone": "America/Chicago",
        "station": "OKC proxy",
        "signal_eligible": False,
    },
    "PHIL": {
        "name": "Philadelphia",
        "lat": 39.8744,
        "lon": -75.2424,
        "timezone": "America/New_York",
        "station": "PHL proxy",
        "signal_eligible": False,
    },
    "PHX": {
        "name": "Phoenix",
        "lat": 33.4342,
        "lon": -112.0116,
        "timezone": "America/Phoenix",
        "station": "PHX proxy",
        "signal_eligible": False,
    },
    "SFO": {
        "name": "San Francisco",
        "lat": 37.6213,
        "lon": -122.3790,
        "timezone": "America/Los_Angeles",
        "station": "SFO proxy",
        "signal_eligible": False,
    },
    "LV": {
        "name": "Las Vegas",
        "lat": 36.0840,
        "lon": -115.1537,
        "timezone": "America/Los_Angeles",
        "station": "LAS proxy",
        "signal_eligible": False,
    },
    "MIN": {
        "name": "Minneapolis",
        "lat": 44.8848,
        "lon": -93.2223,
        "timezone": "America/Chicago",
        "station": "MSP proxy",
        "signal_eligible": False,
    },
    "NOLA": {
        "name": "New Orleans",
        "lat": 30.0424,
        "lon": -90.0289,
        "timezone": "America/Chicago",
        "station": "MSY proxy",
        "signal_eligible": False,
    },
    "DEN": {
        "name": "Denver",
        "lat": 39.8561,
        "lon": -104.6737,
        "timezone": "America/Denver",
        "station": "DEN proxy",
        "signal_eligible": False,
    },
    "TTN": {
        "name": "Trenton",
        "lat": 40.2767,
        "lon": -74.8135,
        "timezone": "America/New_York",
        "station": "TTN proxy",
        "signal_eligible": False,
    },
    "EWR": {
        "name": "Newark",
        "lat": 40.6895,
        "lon": -74.1745,
        "timezone": "America/New_York",
        "station": "EWR proxy",
        "signal_eligible": False,
    },
    "DC": {
        "name": "Washington DC",
        "lat": 38.8512,
        "lon": -77.0402,
        "timezone": "America/New_York",
        "station": "DCA proxy",
        "signal_eligible": False,
    },
    "BOS": {
        "name": "Boston",
        "lat": 42.3656,
        "lon": -71.0096,
        "timezone": "America/New_York",
        "station": "BOS proxy",
        "signal_eligible": False,
    },
    "ATL": {
        "name": "Atlanta",
        "lat": 33.6407,
        "lon": -84.4277,
        "timezone": "America/New_York",
        "station": "ATL proxy",
        "signal_eligible": False,
    },
    "SATX": {
        "name": "San Antonio",
        "lat": 29.5337,
        "lon": -98.4698,
        "timezone": "America/Chicago",
        "station": "SATX proxy",
        "signal_eligible": False,
    },
}


ALIASES = {
    "NEW YORK CITY": "NYC",
    "NEW YORK": "NYC",
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


# ==========================================================
# LOGGING / HELPERS
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger(
    "weather-kalshi-scanner"
)


def safe_float(value, default=None):
    try:
        return (
            float(value)
            if value is not None
            else default
        )
    except (TypeError, ValueError):
        return default


def utc_now():
    return datetime.now(timezone.utc)


def local_date_from_timestamp(
    timestamp,
    timezone_name,
):
    dt = datetime.fromisoformat(
        str(timestamp).replace(
            "Z",
            "+00:00",
        )
    )

    if dt.tzinfo is None:
        dt = dt.replace(
            tzinfo=timezone.utc
        )

    return (
        dt.astimezone(
            ZoneInfo(timezone_name)
        )
        .date()
        .isoformat()
    )


def today_for_code(code):
    return datetime.now(
        ZoneInfo(
            LOCATION_MAP[
                code
            ]["timezone"]
        )
    ).date().isoformat()


def payload_hash(payload):
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode(
            "utf-8"
        )
    ).hexdigest()


# ==========================================================
# HTTP
# ==========================================================

def http_json(
    session,
    url,
    params=None,
):
    last_error = None

    for attempt in range(3):
        try:
            response = session.get(
                url,
                params=params,
                headers={
                    "User-Agent": (
                        "WeatherKalshiResearchBot/6.0"
                    ),
                    "Accept": "application/json",
                },
                timeout=REQUEST_TIMEOUT,
            )

            log.info(
                "HTTP %s %s -> %s",
                response.request.method,
                response.url,
                response.status_code,
            )

            if response.status_code == 429:
                retry_after = safe_float(
                    response.headers.get(
                        "Retry-After"
                    ),
                    2.0,
                ) or 2.0

                if attempt == 2:
                    raise RuntimeError(
                        "HTTP 429 after retries"
                    )

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
                    "API returned non-object JSON"
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
        f"Request failed after retries: "
        f"{last_error}"
    )


# ==========================================================
# DATABASE
# ==========================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS scan_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
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
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
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
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settled_at TIMESTAMPTZ,
    city TEXT NOT NULL,
    forecast_date DATE NOT NULL,
    market_ticker TEXT NOT NULL,
    market_kind TEXT NOT NULL,
    side TEXT NOT NULL CHECK(
        side IN ('YES','NO')
    ),
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
ON paper_trades(
    status,
    created_at DESC
);

CREATE TABLE IF NOT EXISTS alert_log (
    fingerprint TEXT PRIMARY KEY,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL
);
"""


def require_database():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is missing."
        )

    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2-binary is not installed."
        )


def db_connect():
    require_database()

    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,
    )


def ensure_schema(
    conn,
):
    with conn.cursor() as cur:
        for statement in SCHEMA.split(";"):
            statement = statement.strip()

            if statement:
                cur.execute(
                    statement
                )

    conn.commit()


def start_scan(
    conn,
):
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

        scan_id = cur.fetchone()[0]

    conn.commit()

    return int(scan_id)


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
            SET
                completed_at=NOW(),
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

    conn.commit()


def latest_forecast(
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


def latest_market(
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
            (
                ticker,
            ),
        )

        return cur.fetchone()


def latest_weather_refresh(
    conn,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(completed_at)
            FROM scan_runs
            WHERE status='success'
              AND stats->>'weather_refreshed'='true'
            """
        )

        row = cur.fetchone()

    return (
        row[0]
        if row and row[0]
        else None
    )


# ==========================================================
# KALSHI
# ==========================================================

def get_series_list(
    session,
):
    items = []
    cursor = None

    for _ in range(20):
        params = {
            "category": "Climate and Weather",
            "limit": 1000,
        }

        if cursor:
            params[
                "cursor"
            ] = cursor

        data = http_json(
            session,
            f"{KALSHI_API_URL}/series",
            params,
        )

        items.extend(
            data.get(
                "series",
                [],
            )
        )

        cursor = data.get(
            "cursor"
        )

        if not cursor:
            break

    return items


def series_location_code(
    series,
):
    title = (
        series.get(
            "title"
        )
        or ""
    ).upper()

    ticker = (
        series.get(
            "ticker"
        )
        or ""
    ).upper()

    for code in LOCATION_MAP:
        if (
            ticker.startswith(
                "KXHIGH"
            )
            and ticker[
                6:
            ]
            == code
        ):
            return code

        if (
            f" {code} "
            in f" {title} "
        ):
            return code

    for alias, code in (
        ALIASES.items()
    ):
        if alias in title:
            return code

    return None


def is_daily_high_series(
    series,
):
    title = (
        series.get(
            "title"
        )
        or ""
    ).lower()

    frequency = (
        series.get(
            "frequency"
        )
        or ""
    ).lower()

    return (
        frequency == "daily"
        and "temperature" in title
        and (
            "highest" in title
            or "high" in title
            or "maximum" in title
        )
    )


def is_kxrain_series(
    series,
):
    return (
        (
            series.get(
                "ticker"
            )
            or ""
        ).upper()
        == "KXRAIN"
    )


def get_series_markets(
    session,
    ticker,
):
    output = []
    cursor = None

    for _ in range(20):
        params = {
            "series_ticker": ticker,
            "status": "open",
            "limit": 1000,
        }

        if cursor:
            params[
                "cursor"
            ] = cursor

        data = http_json(
            session,
            f"{KALSHI_API_URL}/markets",
            params,
        )

        output.extend(
            data.get(
                "markets",
                [],
            )
        )

        cursor = data.get(
            "cursor"
        )

        if not cursor:
            break

    return output


def parse_market_date(
    market,
):
    candidates = [
        market.get(
            "event_ticker"
        ),
        market.get(
            "ticker"
        ),
    ]

    for candidate in candidates:
        for part in (
            str(candidate or "")
            .split("-")
        ):
            try:
                return datetime.strptime(
                    part,
                    "%y%b%d",
                ).date().isoformat()
            except ValueError:
                continue

    return None


def cents(
    value,
):
    number = safe_float(
        value
    )

    return (
        None
        if number is None
        else number * 100.0
    )


# ==========================================================
# WEATHER
# ==========================================================

def weather_params(
    city_codes,
):
    return {
        "latitude": ",".join(
            str(
                LOCATION_MAP[
                    code
                ]["lat"]
            )
            for code in city_codes
        ),
        "longitude": ",".join(
            str(
                LOCATION_MAP[
                    code
                ]["lon"]
            )
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


def aggregate_daily(
    location,
    code,
):
    hourly = location.get(
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

    daily = {}

    for i, timestamp in enumerate(
        times
    ):
        date_key = (
            local_date_from_timestamp(
                timestamp,
                LOCATION_MAP[
                    code
                ]["timezone"],
            )
        )

        bucket = daily.setdefault(
            date_key,
            {
                "temps": [],
                "rain": [],
            },
        )

        if i < len(
            temperatures
        ):
            value = safe_float(
                temperatures[i]
            )

            if value is not None:
                bucket[
                    "temps"
                ].append(
                    value
                )

        if i < len(
            precipitation
        ):
            value = safe_float(
                precipitation[i]
            )

            if value is not None:
                bucket[
                    "rain"
                ].append(
                    value
                )

    result = {}

    for date_key, values in (
        daily.items()
    ):
        if not values[
            "temps"
        ]:
            continue

        result[
            date_key
        ] = {
            "high": max(
                values[
                    "temps"
                ]
            ),
            "precipitation_sum": sum(
                values[
                    "rain"
                ]
            ),
        }

    return {
        "daily": result,
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


def fetch_deterministic_model(
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

    payload = http_json(
        session,
        url,
        params,
    )

    locations = (
        payload
        if isinstance(payload, list)
        else [payload]
    )

    if len(locations) != len(
        city_codes
    ):
        raise RuntimeError(
            f"{model}: expected "
            f"{len(city_codes)} locations, "
            f"received {len(locations)}."
        )

    return {
        code: aggregate_daily(
            location,
            code,
        )
        for code, location in zip(
            city_codes,
            locations,
        )
    }


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

    payload = http_json(
        session,
        "https://ensemble-api.open-meteo.com/v1/ensemble",
        params,
    )

    locations = (
        payload
        if isinstance(payload, list)
        else [payload]
    )

    if len(locations) != len(
        city_codes
    ):
        raise RuntimeError(
            "Ensemble location count mismatch."
        )

    result = {}

    for code, location in zip(
        city_codes,
        locations,
    ):
        hourly = location.get(
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
                f"{code}: no temperature "
                "ensemble member keys."
            )

        if not rain_keys:
            raise RuntimeError(
                f"{code}: no precipitation "
                "ensemble member keys."
            )

        local_tz = ZoneInfo(
            LOCATION_MAP[
                code
            ]["timezone"]
        )

        grouped = {}

        for i, timestamp in enumerate(
            times
        ):
            dt = datetime.fromisoformat(
                str(timestamp).replace(
                    "Z",
                    "+00:00",
                )
            )

            if dt.tzinfo is None:
                dt = dt.replace(
                    tzinfo=timezone.utc
                )

            date_key = (
                dt.astimezone(
                    local_tz
                )
                .date()
                .isoformat()
            )

            bucket = grouped.setdefault(
                date_key,
                {
                    "temp": {
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

                if i < len(values):
                    value = safe_float(
                        values[i]
                    )

                    if value is not None:
                        bucket[
                            "temp"
                        ][key].append(
                            value
                        )

            for key in rain_keys:
                values = hourly.get(
                    key,
                    [],
                )

                if i < len(values):
                    value = safe_float(
                        values[i]
                    )

                    if value is not None:
                        bucket[
                            "rain"
                        ][key] += value

        city_daily = {}

        for date_key, bucket in (
            grouped.items()
        ):
            member_highs = []

            for key in temp_keys:
                values = bucket[
                    "temp"
                ][key]

                if values:
                    member_highs.append(
                        max(values)
                    )

            member_rain = [
                bucket[
                    "rain"
                ][key]
                for key in rain_keys
            ]

            if (
                member_highs
                and member_rain
            ):
                city_daily[
                    date_key
                ] = {
                    "member_highs": (
                        member_highs
                    ),
                    "member_rain_totals": (
                        member_rain
                    ),
                }

        result[
            code
        ] = {
            "daily": city_daily,
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
            "temperature_member_count": len(
                temp_keys
            ),
            "precipitation_member_count": len(
                rain_keys
            ),
        }

    return result


def fetch_weather(
    session,
    city_codes,
):
    deterministic = {}

    with ThreadPoolExecutor(
        max_workers=len(
            DETERMINISTIC_MODELS
        )
    ) as executor:
        futures = {
            executor.submit(
                fetch_deterministic_model,
                session,
                model,
                city_codes,
            ): model
            for model in (
                DETERMINISTIC_MODELS
            )
        }

        for future in as_completed(
            futures
        ):
            model = futures[
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

    log.info(
        "Ensemble fetched: model=%s, cities=%d",
        ENSEMBLE_MODEL,
        len(ensemble),
    )

    return deterministic, ensemble


# ==========================================================
# PROBABILITY / SIGNALS
# ==========================================================

def temperature_probability(
    member_highs,
    market,
):
    if not member_highs:
        return None

    kind = (
        market.get(
            "strike_type"
        )
        or ""
    ).lower()

    floor = safe_float(
        market.get(
            "floor_strike"
        )
    )

    cap = safe_float(
        market.get(
            "cap_strike"
        )
    )

    if (
        kind == "greater"
        and floor is not None
    ):
        hits = sum(
            value > floor
            for value in member_highs
        )

    elif (
        kind == "less"
        and cap is not None
    ):
        hits = sum(
            value < cap
            for value in member_highs
        )

    elif (
        kind == "between"
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

    return (
        100.0
        * sum(
            value > 0.0
            for value in member_rain_totals
        )
        / len(member_rain_totals)
    )


def ask_cents(
    market,
    side,
):
    field = (
        "yes_ask_dollars"
        if side == "YES"
        else "no_ask_dollars"
    )

    return cents(
        market.get(
            field
        )
    )


def market_change_points(
    current_ask,
    previous_market,
    side,
):
    if previous_market is None:
        return None

    previous_ask = (
        previous_market[2]
        if side == "YES"
        else previous_market[4]
    )

    previous_ask = safe_float(
        previous_ask
    )

    if previous_ask is None:
        return None

    return (
        current_ask
        - previous_ask
    )


def build_candidate(
    conn,
    city,
    forecast_date,
    market,
    kind,
    current_probability,
    previous_probability,
    temperature_change,
):
    if (
        current_probability is None
        or previous_probability is None
    ):
        return None

    forecast_change = (
        current_probability
        - previous_probability
    )

    if (
        abs(forecast_change)
        < MIN_FORECAST_PROBABILITY_CHANGE_POINTS
    ):
        return None

    previous_market = latest_market(
        conn,
        market.get(
            "ticker"
        )
        or "",
    )

    if previous_market is None:
        return None

    best = None

    for side in (
        "YES",
        "NO",
    ):
        ask = ask_cents(
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

        side_probability = (
            current_probability
            if side == "YES"
            else 100.0
            - current_probability
        )

        prior_side_probability = (
            previous_probability
            if side == "YES"
            else 100.0
            - previous_probability
        )

        side_change = (
            side_probability
            - prior_side_probability
        )

        market_change = (
            market_change_points(
                ask,
                previous_market,
                side,
            )
        )

        if market_change is None:
            continue

        if (
            side_change
            * market_change
            > 0
        ):
            lag = max(
                0.0,
                abs(
                    side_change
                )
                - abs(
                    market_change
                ),
            )
        else:
            lag = abs(
                side_change
            )

        edge = (
            side_probability
            - ask
        )

        if (
            abs(side_change)
            < MIN_FORECAST_PROBABILITY_CHANGE_POINTS
            or lag < MIN_MARKET_LAG_POINTS
            or edge < MIN_PRELIMINARY_EDGE_POINTS
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
                side_probability
            ),
            "preliminary_edge_points": edge,
            "forecast_probability_change_points": (
                side_change
            ),
            "market_price_change_points": (
                market_change
            ),
            "market_lag_points": lag,
            "forecast_temperature_change_f": (
                temperature_change
            ),
            "title": (
                market.get(
                    "title"
                )
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
# PAPER TRADES / ALERTS
# ==========================================================

def signal_fingerprint(
    signal,
):
    raw = "|".join(
        [
            signal[
                "market_ticker"
            ],
            signal[
                "side"
            ],
            signal[
                "forecast_date"
            ],
            f"{signal['entry_price_cents']:.2f}",
            f"{signal['model_probability_proxy']:.2f}",
            f"{signal['forecast_probability_change_points']:.2f}",
            f"{signal['market_price_change_points']:.2f}",
        ]
    )

    return hashlib.sha256(
        raw.encode(
            "utf-8"
        )
    ).hexdigest()[:32]


def open_paper_trade(
    conn,
    signal,
    reason,
):
    fp = signal_fingerprint(
        signal
    )

    price = (
        signal[
            "entry_price_cents"
        ]
        / 100.0
    )

    if price <= 0:
        return False, fp

    contracts = (
        PAPER_RISK_DOLLARS
        / price
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO paper_trades(
                signal_fingerprint,
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
                signal[
                    "forecast_date"
                ],
                signal[
                    "market_ticker"
                ],
                signal[
                    "market_kind"
                ],
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
                    "WeatherKalshiResearchBot/6.0"
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
                "Discord relay returned: %s",
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
                "Relay reported failure: %s",
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


def alert_message(
    signal,
):
    direction = (
        "up"
        if signal[
            "forecast_probability_change_points"
        ]
        > 0
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
        f"Entry ask: **"
        f"{signal['entry_price_cents']:.1f}¢**\n\n"
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


def alert_if_needed(
    conn,
    signal,
):
    fp = signal_fingerprint(
        signal
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM alert_log
            WHERE fingerprint=%s
            LIMIT 1
            """,
            (
                fp,
            ),
        )

        if cur.fetchone():
            return False

    sent = send_discord(
        alert_message(
            signal
        )
    )

    if not sent:
        return False

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
            ON CONFLICT(
                fingerprint
            )
            DO NOTHING
            """,
            (
                fp,
                Json(signal),
            ),
        )

    return True


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

    ticker_list = sorted(
        {
            row[1]
            for row in trades
        }
    )

    try:
        payload = http_json(
            session,
            f"{KALSHI_API_URL}/markets",
            {
                "status": "open",
                "limit": 1000,
            },
        )
    except Exception:
        # Settled markets are not necessarily returned by status=open.
        # Leave open trades untouched; a later reconciliation can use
        # historical/settled-market data after the trade closes.
        return 0

    by_ticker = {
        market.get(
            "ticker"
        ): market
        for market in payload.get(
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
            market = by_ticker.get(
                ticker
            )

            if market is None:
                continue

            result = (
                market.get(
                    "result"
                )
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
                cur.rowcount
            )

    return settled


# ==========================================================
# SNAPSHOT WRITES
# ==========================================================

def write_forecast_snapshots(
    conn,
    deterministic,
    ensemble,
):
    rows = []

    for model, city_data in (
        deterministic.items()
    ):
        for code, data in (
            city_data.items()
        ):
            city = LOCATION_MAP[
                code
            ]["name"]

            for date_key, daily in (
                data[
                    "daily"
                ].items()
            ):
                high_payload = {
                    "high": daily[
                        "high"
                    ],
                    "model_run": data.get(
                        "model_run"
                    ),
                }

                rain_payload = {
                    "precipitation_sum": daily[
                        "precipitation_sum"
                    ],
                    "model_run": data.get(
                        "model_run"
                    ),
                }

                rows.append(
                    (
                        city,
                        "temperature_high",
                        model,
                        date_key,
                        daily[
                            "high"
                        ],
                        Json(
                            high_payload
                        ),
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
                        Json(
                            rain_payload
                        ),
                        payload_hash(
                            rain_payload
                        ),
                    )
                )

    for code, data in (
        ensemble.items()
    ):
        city = LOCATION_MAP[
            code
        ]["name"]

        for date_key, daily in (
            data[
                "daily"
            ].items()
        ):
            temp_payload = {
                "member_highs": daily[
                    "member_highs"
                ],
                "model_run": data.get(
                    "model_run"
                ),
            }

            rain_payload = {
                "member_rain_totals": (
                    daily[
                        "member_rain_totals"
                    ]
                ),
                "model_run": data.get(
                    "model_run"
                ),
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


def write_market_snapshots(
    conn,
    entries,
):
    rows = []

    for entry in entries:
        market = entry[
            "market"
        ]

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
                entry[
                    "date"
                ],
                LOCATION_MAP[
                    entry["code"]
                ]["name"],
                entry[
                    "kind"
                ],
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
# MAIN
# ==========================================================

def run_scan():
    require_database()

    started = time.monotonic()

    stats = {
        "temperature_series": 0,
        "temperature_markets": 0,
        "rain_markets": 0,
        "weather_refreshed": False,
        "forecast_shocks": 0,
        "paper_trades_created": 0,
        "discord_alerts": 0,
        "settled_trades": 0,
        "errors": [],
    }

    conn = db_connect()
    session = requests.Session()

    try:
        ensure_schema(conn)
        scan_id = start_scan(conn)

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
            "KALSHI_API_URL: %s",
            KALSHI_API_URL,
        )
        log.info(
            "FORECAST_DAYS: %s | WEATHER_REFRESH_SECONDS: %s",
            FORECAST_DAYS,
            WEATHER_REFRESH_SECONDS,
        )
        log.info(
            "=================================================="
        )

        all_series = get_series_list(
            session
        )

        log.info(
            "Climate/weather series returned: %d",
            len(all_series),
        )

        with conn.cursor() as cur:
            for series in all_series:
                ticker = (
                    series.get(
                        "ticker"
                    )
                    or ""
                )

                if not ticker:
                    continue

                cur.execute(
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
                    ON CONFLICT(
                        series_ticker
                    )
                    DO UPDATE SET
                        title=EXCLUDED.title,
                        category=EXCLUDED.category,
                        tags=EXCLUDED.tags,
                        settlement_sources=EXCLUDED.settlement_sources,
                        contract_terms_url=EXCLUDED.contract_terms_url,
                        updated_at=NOW(),
                        raw_series=EXCLUDED.raw_series
                    """,
                    (
                        ticker,
                        series.get(
                            "title",
                            "",
                        ),
                        series.get(
                            "category"
                        ),
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
                    ),
                )

        conn.commit()

        temperature_series = [
            series
            for series in all_series
            if is_daily_high_series(
                series
            )
        ]

        rain_series = [
            series
            for series in all_series
            if is_kxrain_series(
                series
            )
        ]

        stats[
            "temperature_series"
        ] = len(
            temperature_series
        )

        log.info(
            "Daily temperature series discovered: %d",
            len(temperature_series),
        )

        log.info(
            "KXRAIN series discovered: %d",
            len(rain_series),
        )

        # Fetch KXRAIN one time so it can both discover rain cities
        # and be snapshotted later.
        rain_markets = []

        if rain_series:
            rain_markets = (
                get_series_markets(
                    session,
                    "KXRAIN",
                )
            )

        stats[
            "rain_markets"
        ] = len(
            rain_markets
        )

        city_codes = set()

        for series in temperature_series:
            code = series_location_code(
                series
            )

            if code:
                city_codes.add(
                    code
                )

        for market in rain_markets:
            ticker = (
                market.get(
                    "ticker"
                )
                or ""
            )

            suffix = (
                ticker.rsplit(
                    "-",
                    1,
                )[-1]
                .upper()
            )

            if suffix in LOCATION_MAP:
                city_codes.add(
                    suffix
                )

        if not city_codes:
            raise RuntimeError(
                "No weather markets could be mapped "
                "to known forecast coordinates."
            )

        city_codes = sorted(
            city_codes
        )

        log.info(
            "Forecast cities: %s",
            ", ".join(
                city_codes
            ),
        )

        last_weather = (
            latest_weather_refresh(
                conn
            )
        )

        refresh_weather = (
            last_weather is None
            or (
                (
                    utc_now()
                    - last_weather
                ).total_seconds()
                >= WEATHER_REFRESH_SECONDS
            )
        )

        deterministic = {}
        ensemble = {}

        if refresh_weather:
            stats[
                "weather_refreshed"
            ] = True

            log.info(
                "Weather refresh required."
            )

            deterministic, ensemble = (
                fetch_weather(
                    session,
                    city_codes,
                )
            )

            # SIGNAL COMPARISON HAPPENS BEFORE CURRENT SNAPSHOT WRITES.
            for series in temperature_series:
                code = series_location_code(
                    series
                )

                if (
                    code is None
                    or (
                        not LOCATION_MAP[
                            code
                        ][
                            "signal_eligible"
                        ]
                        and not ALLOW_UNVERIFIED_LOCATION_SIGNALS
                    )
                ):
                    continue

                markets = get_series_markets(
                    session,
                    series[
                        "ticker"
                    ],
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

                    current_day = (
                        ensemble
                        .get(code, {})
                        .get("daily", {})
                        .get(date_key)
                    )

                    if not current_day:
                        continue

                    city = LOCATION_MAP[
                        code
                    ]["name"]

                    previous = latest_forecast(
                        conn,
                        city,
                        "ensemble_temperature_distribution",
                        ENSEMBLE_MODEL,
                        date_key,
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

                    if not previous_members:
                        continue

                    current_probability = (
                        temperature_probability(
                            current_day[
                                "member_highs"
                            ],
                            market,
                        )
                    )

                    previous_probability = (
                        temperature_probability(
                            previous_members,
                            market,
                        )
                    )

                    hrrr_current = (
                        deterministic
                        .get("hrrr", {})
                        .get(code, {})
                        .get("daily", {})
                        .get(date_key, {})
                        .get("high")
                    )

                    hrrr_previous = latest_forecast(
                        conn,
                        city,
                        "temperature_high",
                        "hrrr",
                        date_key,
                    )

                    temperature_change = None

                    if (
                        hrrr_current is not None
                        and hrrr_previous is not None
                    ):
                        old_value = safe_float(
                            hrrr_previous[1]
                        )

                        if old_value is not None:
                            temperature_change = (
                                hrrr_current
                                - old_value
                            )

                    candidate = build_candidate(
                        conn,
                        city,
                        date_key,
                        market,
                        "temperature",
                        current_probability,
                        previous_probability,
                        temperature_change,
                    )

                    if candidate:
                        stats[
                            "forecast_shocks"
                        ] += 1

                        reason = {
                            "strategy": (
                                "large forecast "
                                "probability shock plus "
                                "insufficient market response"
                            ),
                            "calibrated_probability": False,
                            "raw_ensemble_proxy": True,
                            "paper_only": True,
                            "station": LOCATION_MAP[
                                code
                            ]["station"],
                        }

                        created, _ = open_paper_trade(
                            conn,
                            candidate,
                            reason,
                        )

                        if created:
                            stats[
                                "paper_trades_created"
                            ] += 1

                            if alert_if_needed(
                                conn,
                                candidate,
                            ):
                                stats[
                                    "discord_alerts"
                                ] += 1

            # Rain signals.
            for market in rain_markets:
                ticker = (
                    market.get(
                        "ticker"
                    )
                    or ""
                )

                suffix = (
                    ticker.rsplit(
                        "-",
                        1,
                    )[-1]
                    .upper()
                )

                if suffix not in LOCATION_MAP:
                    continue

                code = suffix

                if (
                    not LOCATION_MAP[
                        code
                    ][
                        "signal_eligible"
                    ]
                    and not ALLOW_UNVERIFIED_LOCATION_SIGNALS
                ):
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

                current_day = (
                    ensemble
                    .get(code, {})
                    .get("daily", {})
                    .get(date_key)
                )

                if not current_day:
                    continue

                city = LOCATION_MAP[
                    code
                ]["name"]

                previous = latest_forecast(
                    conn,
                    city,
                    "ensemble_rain_distribution",
                    ENSEMBLE_MODEL,
                    date_key,
                )

                if previous is None:
                    continue

                previous_members = (
                    (previous[2] or {})
                    .get(
                        "member_rain_totals",
                        [],
                    )
                )

                current_probability = (
                    rain_probability(
                        current_day[
                            "member_rain_totals"
                        ]
                    )
                )

                previous_probability = (
                    rain_probability(
                        previous_members
                    )
                )

                candidate = build_candidate(
                    conn,
                    city,
                    date_key,
                    market,
                    "rain",
                    current_probability,
                    previous_probability,
                    None,
                )

                if candidate:
                    stats[
                        "forecast_shocks"
                    ] += 1

                    reason = {
                        "strategy": (
                            "large precipitation "
                            "probability shock plus "
                            "insufficient market response"
                        ),
                        "calibrated_probability": False,
                        "raw_ensemble_proxy": True,
                        "paper_only": True,
                        "station": LOCATION_MAP[
                            code
                        ]["station"],
                    }

                    created, _ = open_paper_trade(
                        conn,
                        candidate,
                        reason,
                    )

                    if created:
                        stats[
                            "paper_trades_created"
                        ] += 1

                        if alert_if_needed(
                            conn,
                            candidate,
                        ):
                            stats[
                                "discord_alerts"
                            ] += 1

            write_forecast_snapshots(
                conn,
                deterministic,
                ensemble,
            )

        # Current market prices are always recorded.
        market_entries = []

        for series in temperature_series:
            code = series_location_code(
                series
            )

            if code is None:
                continue

            try:
                markets = get_series_markets(
                    session,
                    series[
                        "ticker"
                    ],
                )
            except Exception as exc:
                stats[
                    "errors"
                ].append(
                    f"{series.get('ticker')}: {exc}"
                )
                log.error(
                    "Market fetch failed for %s: %s",
                    series.get(
                        "ticker"
                    ),
                    exc,
                )
                continue

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

        for market in rain_markets:
            ticker = (
                market.get(
                    "ticker"
                )
                or ""
            )

            suffix = (
                ticker.rsplit(
                    "-",
                    1,
                )[-1]
                .upper()
            )

            if suffix not in LOCATION_MAP:
                continue

            code = suffix
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

        stats[
            "temperature_markets"
        ] = sum(
            1
            for entry in market_entries
            if entry["kind"]
            == "temperature"
        )

        write_market_snapshots(
            conn,
            market_entries,
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

        log.info(
            "=================================================="
        )
        log.info(
            "SCAN COMPLETE | %s",
            json.dumps(
                stats,
                default=str,
            ),
        )
        log.info(
            "Runtime: %.1fs",
            time.monotonic()
            - started,
        )
        log.info(
            "=================================================="
        )

    except Exception as exc:
        log.exception(
            "SCAN FAILED"
        )

        try:
            finish_scan(
                conn,
                scan_id,
                "failed",
                stats,
                str(exc),
            )
        except Exception:
            conn.rollback()

        raise

    finally:
        conn.close()


if __name__ == "__main__":
    run_scan()
