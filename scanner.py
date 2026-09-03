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
    from psycopg2.extras import Json, execute_values
except ImportError:
    psycopg2 = None
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
    os.environ.get(
        "WEATHER_REFRESH_SECONDS",
        "1800",
    )
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

# IMPORTANT: these are Open-Meteo's actual current model identifiers.
# Do not replace them with "hrrr" / "nbm".
DETERMINISTIC_MODELS = (
    "hrrr_conus",
    "nbm_conus",
    "gfs_seamless",
    "ecmwf_ifs025",
)

ENSEMBLE_MODEL = "gfs_seamless"


# ==========================================================
# VERIFIED / MONITORED CITIES
# ==========================================================

CITIES = {
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
        "lat": 41.7868,
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
}

ALIASES = {
    "NEW YORK CITY": "NYC",
    "NEW YORK": "NYC",
    "CHICAGO": "CHI",
    "MIAMI": "MIA",
    "AUSTIN": "AUS",
}

RAIN_CODES = {
    "NYC": "NYC",
    "CHI": "CHI",
    "MIA": "MIA",
    "AUS": "AUS",
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger(
    "weather-kalshi-scanner"
)


# ==========================================================
# BASIC HELPERS
# ==========================================================

def utc_now():
    return datetime.now(timezone.utc)


def safe_float(
    value,
    default=None,
):
    try:
        return (
            float(value)
            if value is not None
            else default
        )
    except (TypeError, ValueError):
        return default


def cents_from_dollars(
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


def payload_hash(
    payload,
):
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


def today_for_city(
    code,
):
    return datetime.now(
        ZoneInfo(
            CITIES[code]["timezone"]
        )
    ).date().isoformat()


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


def parse_market_date(
    market,
):
    source = (
        market.get("event_ticker")
        or market.get("ticker")
        or ""
    )

    for part in str(source).split("-"):
        try:
            return datetime.strptime(
                part,
                "%y%b%d",
            ).date().isoformat()
        except ValueError:
            continue

    return None


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
                        "WeatherKalshiResearchBot/8.0"
                    ),
                    "Accept": "application/json",
                },
                timeout=REQUEST_TIMEOUT,
            )

            log.info(
                "HTTP GET %s -> %s",
                response.url,
                response.status_code,
            )

            if response.status_code == 429:
                if attempt == 2:
                    raise RuntimeError(
                        "HTTP 429 after retries"
                    )

                wait = safe_float(
                    response.headers.get(
                        "Retry-After"
                    ),
                    2,
                ) or 2

                time.sleep(
                    min(
                        max(wait, 1),
                        10,
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


def start_run(
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

        run_id = cur.fetchone()[0]

    conn.commit()
    return run_id


def finish_run(
    conn,
    run_id,
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
                run_id,
            ),
        )

    conn.commit()


# ==========================================================
# KALSHI
# ==========================================================

def get_all_weather_series(
    session,
):
    output = []
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

        output.extend(
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

    return output


def series_city_code(
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

    if ticker.startswith(
        "KXHIGH"
    ):
        suffix = ticker[
            6:
        ]

        if suffix in CITIES:
            return suffix

    for alias, code in (
        ALIASES.items()
    ):
        if alias in title:
            return code

    return None


def is_daily_temperature_series(
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


def get_series_markets(
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


# ==========================================================
# OPEN-METEO
# ==========================================================

def location_params(
    codes,
):
    return {
        "latitude": ",".join(
            str(
                CITIES[code]["lat"]
            )
            for code in codes
        ),
        "longitude": ",".join(
            str(
                CITIES[code]["lon"]
            )
            for code in codes
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

    grouped = {}

    for index, timestamp in enumerate(
        times
    ):
        date_key = (
            local_date_from_timestamp(
                timestamp,
                CITIES[
                    code
                ]["timezone"],
            )
        )

        day = grouped.setdefault(
            date_key,
            {
                "temps": [],
                "rain": [],
            },
        )

        if index < len(temperatures):
            value = safe_float(
                temperatures[index]
            )
            if value is not None:
                day[
                    "temps"
                ].append(value)

        if index < len(precipitation):
            value = safe_float(
                precipitation[index]
            )
            if value is not None:
                day[
                    "rain"
                ].append(value)

    result = {}

    for date_key, day in (
        grouped.items()
    ):
        if not day[
            "temps"
        ]:
            continue

        result[
            date_key
        ] = {
            "high": max(
                day[
                    "temps"
                ]
            ),
            "precipitation_sum": sum(
                day[
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


def fetch_deterministic(
    session,
    model,
    codes,
):
    params = location_params(
        codes
    )
    params[
        "models"
    ] = model

    data = http_json(
        session,
        "https://api.open-meteo.com/v1/gfs",
        params,
    )

    locations = (
        data
        if isinstance(data, list)
        else [data]
    )

    if len(locations) != len(
        codes
    ):
        raise RuntimeError(
            f"{model}: Open-Meteo returned "
            f"{len(locations)} locations; "
            f"expected {len(codes)}."
        )

    return {
        code: aggregate_daily(
            location,
            code,
        )
        for code, location in zip(
            codes,
            locations,
        )
    }


def aggregate_ensemble(
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

    temperature_keys = sorted(
        key
        for key in hourly
        if key.startswith(
            "temperature_2m_member"
        )
    )

    precipitation_keys = sorted(
        key
        for key in hourly
        if key.startswith(
            "precipitation_member"
        )
    )

    if not temperature_keys:
        raise RuntimeError(
            f"{code}: no ensemble "
            "temperature members returned."
        )

    if not precipitation_keys:
        raise RuntimeError(
            f"{code}: no ensemble "
            "precipitation members returned."
        )

    tz = ZoneInfo(
        CITIES[
            code
        ]["timezone"]
    )

    grouped = {}

    for index, timestamp in enumerate(
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
                tz
            )
            .date()
            .isoformat()
        )

        day = grouped.setdefault(
            date_key,
            {
                "temperatures": {
                    key: []
                    for key in temperature_keys
                },
                "precipitation": {
                    key: 0.0
                    for key in precipitation_keys
                },
            },
        )

        for key in temperature_keys:
            values = hourly.get(
                key,
                [],
            )

            if index < len(values):
                value = safe_float(
                    values[index]
                )
                if value is not None:
                    day[
                        "temperatures"
                    ][key].append(
                        value
                    )

        for key in precipitation_keys:
            values = hourly.get(
                key,
                [],
            )

            if index < len(values):
                value = safe_float(
                    values[index]
                )

                if value is not None:
                    day[
                        "precipitation"
                    ][key] += value

    result = {}

    for date_key, day in (
        grouped.items()
    ):
        highs = []

        for key in temperature_keys:
            values = day[
                "temperatures"
            ][key]

            if values:
                highs.append(
                    max(values)
                )

        rain = [
            day[
                "precipitation"
            ][key]
            for key in precipitation_keys
        ]

        if highs and rain:
            result[
                date_key
            ] = {
                "member_highs": highs,
                "member_rain_totals": rain,
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


def fetch_ensemble(
    session,
    codes,
):
    params = location_params(
        codes
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
        codes
    ):
        raise RuntimeError(
            "Ensemble location count mismatch."
        )

    return {
        code: aggregate_ensemble(
            location,
            code,
        )
        for code, location in zip(
            codes,
            locations,
        )
    }


def fetch_weather(
    session,
    codes,
):
    deterministic = {}

    # IMPORTANT:
    # An optional deterministic model can fail without killing the
    # entire run. The ensemble is required because it powers signals.
    with ThreadPoolExecutor(
        max_workers=len(
            DETERMINISTIC_MODELS
        )
    ) as executor:
        futures = {
            executor.submit(
                fetch_deterministic,
                session,
                model,
                codes,
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

            try:
                deterministic[
                    model
                ] = future.result()

                log.info(
                    "DETERMINISTIC MODEL OK: %s",
                    model,
                )

            except Exception as exc:
                log.error(
                    "DETERMINISTIC MODEL FAILED: %s | %s",
                    model,
                    exc,
                )

    if not deterministic:
        raise RuntimeError(
            "All deterministic weather models failed."
        )

    ensemble = fetch_ensemble(
        session,
        codes,
    )

    log.info(
        "ENSEMBLE OK: %d cities, %s",
        len(ensemble),
        ENSEMBLE_MODEL,
    )

    return deterministic, ensemble


# ==========================================================
# PROBABILITIES
# ==========================================================

def temperature_probability(
    members,
    market,
):
    if not members:
        return None

    strike_type = (
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
        strike_type == "greater"
        and floor is not None
    ):
        hits = sum(
            value > floor
            for value in members
        )

    elif (
        strike_type == "less"
        and cap is not None
    ):
        hits = sum(
            value < cap
            for value in members
        )

    elif (
        strike_type == "between"
        and floor is not None
        and cap is not None
    ):
        hits = sum(
            floor
            <= value
            <= cap
            for value in members
        )

    else:
        return None

    return (
        100.0
        * hits
        / len(members)
    )


def rain_probability(
    members,
):
    if not members:
        return None

    return (
        100.0
        * sum(
            value > 0.0
            for value in members
        )
        / len(members)
    )


# ==========================================================
# SIGNAL / PAPER TRADE
# ==========================================================

def side_ask(
    market,
    side,
):
    field = (
        "yes_ask_dollars"
        if side == "YES"
        else "no_ask_dollars"
    )

    return cents_from_dollars(
        market.get(field)
    )


def previous_side_ask(
    previous_market,
    side,
):
    if previous_market is None:
        return None

    return (
        safe_float(
            previous_market[2]
            if side == "YES"
            else previous_market[4]
        )
    )


def build_signal(
    conn,
    city,
    forecast_date,
    market,
    market_kind,
    current_probability,
    previous_probability,
    temperature_change=None,
):
    if (
        current_probability is None
        or previous_probability is None
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

    candidates = []

    for side in (
        "YES",
        "NO",
    ):
        ask = side_ask(
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

        current_side_prob = (
            current_probability
            if side == "YES"
            else 100.0
            - current_probability
        )

        previous_side_prob = (
            previous_probability
            if side == "YES"
            else 100.0
            - previous_probability
        )

        forecast_change = (
            current_side_prob
            - previous_side_prob
        )

        if (
            abs(forecast_change)
            < MIN_FORECAST_PROBABILITY_CHANGE_POINTS
        ):
            continue

        prior_ask = (
            previous_side_ask(
                previous_market,
                side,
            )
        )

        if prior_ask is None:
            continue

        market_change = (
            ask
            - prior_ask
        )

        if (
            forecast_change
            * market_change
            > 0
        ):
            lag = max(
                0.0,
                abs(
                    forecast_change
                )
                - abs(
                    market_change
                ),
            )
        else:
            lag = abs(
                forecast_change
            )

        edge = (
            current_side_prob
            - ask
        )

        if (
            lag
            < MIN_MARKET_LAG_POINTS
            or edge
            < MIN_PRELIMINARY_EDGE_POINTS
        ):
            continue

        candidates.append(
            {
                "city": city,
                "forecast_date": forecast_date,
                "market_ticker": market[
                    "ticker"
                ],
                "market_kind": market_kind,
                "side": side,
                "entry_price_cents": ask,
                "model_probability_proxy": current_side_prob,
                "preliminary_edge_points": edge,
                "forecast_probability_change_points": forecast_change,
                "market_price_change_points": market_change,
                "market_lag_points": lag,
                "forecast_temperature_change_f": temperature_change,
            }
        )

    if not candidates:
        return None

    return max(
        candidates,
        key=lambda item: (
            item[
                "market_lag_points"
            ],
            item[
                "preliminary_edge_points"
            ],
        ),
    )


def trade_fingerprint(
    signal,
):
    raw = "|".join(
        [
            signal[
                "market_ticker"
            ],
            signal["side"],
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
        raw.encode()
    ).hexdigest()[:32]


def open_paper_trade(
    conn,
    signal,
    reason,
):
    fp = trade_fingerprint(
        signal
    )

    price_dollars = (
        signal[
            "entry_price_cents"
        ]
        / 100.0
    )

    contracts = (
        PAPER_RISK_DOLLARS
        / price_dollars
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


def discord_message(
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
        f"HRRR high change: "
        f"**{temp:+.1f}°F**\n"
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
        "The ensemble frequency is an "
        "uncalibrated research proxy."
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
                "Accept": "application/json",
            },
            timeout=REQUEST_TIMEOUT,
        )

        log.info(
            "Discord relay status: %s",
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
            result = response.json()
        except Exception:
            result = {}

        if result.get(
            "success"
        ) is False:
            return False

        return True

    except Exception as exc:
        log.error(
            "Discord relay exception: %s",
            exc,
        )
        return False


def alert_once(
    conn,
    signal,
):
    fp = trade_fingerprint(
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
            (fp,),
        )

        if cur.fetchone():
            return False

    sent = send_discord(
        discord_message(
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


# ==========================================================
# SNAPSHOTS
# ==========================================================

def write_weather(
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
            city = CITIES[
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

                rows.extend(
                    [
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
                        ),
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
                        ),
                    ]
                )

    for code, data in (
        ensemble.items()
    ):
        city = CITIES[
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
                ]
            }

            rain_payload = {
                "member_rain_totals": daily[
                    "member_rain_totals"
                ]
            }

            rows.extend(
                [
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
                    ),
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
                    ),
                ]
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
                CITIES[
                    entry[
                        "code"
                    ]
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
                cents_from_dollars(
                    market.get(
                        "yes_bid_dollars"
                    )
                ),
                cents_from_dollars(
                    market.get(
                        "yes_ask_dollars"
                    )
                ),
                cents_from_dollars(
                    market.get(
                        "no_bid_dollars"
                    )
                ),
                cents_from_dollars(
                    market.get(
                        "no_ask_dollars"
                    )
                ),
                cents_from_dollars(
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
# SCAN
# ==========================================================

def run_scan():
    require_database()

    conn = db_connect()
    session = requests.Session()

    try:
        ensure_schema(
            conn
        )

        run_id = start_run(
            conn
        )

        stats = {
            "weather_refreshed": False,
            "temperature_series": 0,
            "temperature_markets": 0,
            "rain_markets": 0,
            "forecast_shocks": 0,
            "paper_trades_created": 0,
            "discord_alerts": 0,
            "settled_trades": 0,
            "errors": [],
        }

        start = time.monotonic()

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
                "KALSHI_API_URL: %s",
                KALSHI_API_URL,
            )
            log.info(
                "DETERMINISTIC_MODELS: %s",
                ", ".join(
                    DETERMINISTIC_MODELS
                ),
            )
            log.info(
                "ENSEMBLE_MODEL: %s",
                ENSEMBLE_MODEL,
            )
            log.info(
                "=================================================="
            )

            series = get_all_weather_series(
                session
            )

            temperature_series = [
                item
                for item in series
                if is_daily_temperature_series(
                    item
                )
            ]

            rain_series = [
                item
                for item in series
                if (
                    item.get(
                        "ticker"
                    )
                    or ""
                ).upper()
                == "KXRAIN"
            ]

            # Only map the locations whose exact forecast proxy we
            # deliberately maintain.
            mapped_temp_series = [
                item
                for item in temperature_series
                if series_city_code(
                    item
                ) in CITIES
            ]

            stats[
                "temperature_series"
            ] = len(
                mapped_temp_series
            )

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

            city_codes = sorted(
                {
                    series_city_code(
                        item
                    )
                    for item in mapped_temp_series
                    if series_city_code(
                        item
                    )
                }
                | {
                    (
                        market.get(
                            "ticker"
                        )
                        or ""
                    ).rsplit(
                        "-",
                        1,
                    )[-1].upper()
                    for market in rain_markets
                    if (
                        market.get(
                            "ticker"
                        )
                        or ""
                    ).rsplit(
                        "-",
                        1,
                    )[-1].upper()
                    in CITIES
                }
            )

            if not city_codes:
                raise RuntimeError(
                    "No supported city markets found."
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

            current_market_entries = []

            # Always fetch current open markets.
            for item in mapped_temp_series:
                code = series_city_code(
                    item
                )

                try:
                    markets = (
                        get_series_markets(
                            session,
                            item[
                                "ticker"
                            ],
                        )
                    )
                except Exception as exc:
                    stats[
                        "errors"
                    ].append(
                        f"{item.get('ticker')}: {exc}"
                    )
                    log.error(
                        "Temperature markets failed for %s: %s",
                        item.get(
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

                    if date_key < today_for_city(
                        code
                    ):
                        continue

                    current_market_entries.append(
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

                code = ticker.rsplit(
                    "-",
                    1,
                )[-1].upper()

                if code not in CITIES:
                    continue

                date_key = parse_market_date(
                    market
                )

                if not date_key:
                    continue

                if date_key < today_for_city(
                    code
                ):
                    continue

                current_market_entries.append(
                    {
                        "market": market,
                        "code": code,
                        "date": date_key,
                        "kind": "rain",
                    }
                )

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

                # FIRST establish signals from prior history,
                # THEN write the current weather observations.
                # This prevents current=previous self-comparisons.
                for entry in current_market_entries:
                    code = entry[
                        "code"
                    ]

                    if not CITIES[
                        code
                    ][
                        "signal_eligible"
                    ]:
                        continue

                    date_key = entry[
                        "date"
                    ]

                    city = CITIES[
                        code
                    ]["name"]

                    day = (
                        ensemble
                        .get(code, {})
                        .get("daily", {})
                        .get(date_key)
                    )

                    if not day:
                        continue

                    market = entry[
                        "market"
                    ]

                    if entry[
                        "kind"
                    ] == "temperature":
                        previous = (
                            latest_forecast(
                                conn,
                                city,
                                "ensemble_temperature_distribution",
                                ENSEMBLE_MODEL,
                                date_key,
                            )
                        )

                        if previous:
                            old_members = (
                                (previous[2] or {})
                                .get(
                                    "member_highs",
                                    [],
                                )
                            )

                            if old_members:
                                current_probability = (
                                    temperature_probability(
                                        day[
                                            "member_highs"
                                        ],
                                        market,
                                    )
                                )

                                previous_probability = (
                                    temperature_probability(
                                        old_members,
                                        market,
                                    )
                                )

                                hrrr_current = (
                                    deterministic
                                    .get(
                                        "hrrr_conus",
                                        {},
                                    )
                                    .get(
                                        code,
                                        {},
                                    )
                                    .get(
                                        "daily",
                                        {},
                                    )
                                    .get(
                                        date_key,
                                        {},
                                    )
                                    .get(
                                        "high"
                                    )
                                )

                                hrrr_previous = (
                                    latest_forecast(
                                        conn,
                                        city,
                                        "temperature_high",
                                        "hrrr_conus",
                                        date_key,
                                    )
                                )

                                temp_change = None

                                if (
                                    hrrr_current is not None
                                    and hrrr_previous
                                ):
                                    old_hrrr = safe_float(
                                        hrrr_previous[
                                            1
                                        ]
                                    )

                                    if old_hrrr is not None:
                                        temp_change = (
                                            hrrr_current
                                            - old_hrrr
                                        )

                                signal = build_signal(
                                    conn,
                                    city,
                                    date_key,
                                    market,
                                    "temperature",
                                    current_probability,
                                    previous_probability,
                                    temp_change,
                                )

                                if signal:
                                    stats[
                                        "forecast_shocks"
                                    ] += 1

                                    reason = {
                                        "paper_only": True,
                                        "calibrated_probability": False,
                                        "raw_ensemble_proxy": True,
                                        "station": CITIES[
                                            code
                                        ][
                                            "station"
                                        ],
                                    }

                                    created, _ = (
                                        open_paper_trade(
                                            conn,
                                            signal,
                                            reason,
                                        )
                                    )

                                    if created:
                                        stats[
                                            "paper_trades_created"
                                        ] += 1

                                        if alert_once(
                                            conn,
                                            signal,
                                        ):
                                            stats[
                                                "discord_alerts"
                                            ] += 1

                    else:
                        previous = (
                            latest_forecast(
                                conn,
                                city,
                                "ensemble_rain_distribution",
                                ENSEMBLE_MODEL,
                                date_key,
                            )
                        )

                        if previous:
                            old_rain = (
                                (previous[2] or {})
                                .get(
                                    "member_rain_totals",
                                    [],
                                )
                            )

                            if old_rain:
                                current_probability = (
                                    rain_probability(
                                        day[
                                            "member_rain_totals"
                                        ]
                                    )
                                )

                                previous_probability = (
                                    rain_probability(
                                        old_rain
                                    )
                                )

                                signal = build_signal(
                                    conn,
                                    city,
                                    date_key,
                                    market,
                                    "rain",
                                    current_probability,
                                    previous_probability,
                                    None,
                                )

                                if signal:
                                    stats[
                                        "forecast_shocks"
                                    ] += 1

                                    reason = {
                                        "paper_only": True,
                                        "calibrated_probability": False,
                                        "raw_ensemble_proxy": True,
                                        "station": CITIES[
                                            code
                                        ][
                                            "station"
                                        ],
                                    }

                                    created, _ = (
                                        open_paper_trade(
                                            conn,
                                            signal,
                                            reason,
                                        )
                                    )

                                    if created:
                                        stats[
                                            "paper_trades_created"
                                        ] += 1

                                        if alert_once(
                                            conn,
                                            signal,
                                        ):
                                            stats[
                                                "discord_alerts"
                                            ] += 1

                write_weather(
                    conn,
                    deterministic,
                    ensemble,
                )

            # Current market prices are written every scan.
            write_market_snapshots(
                conn,
                current_market_entries,
            )

            finish_run(
                conn,
                run_id,
                "success",
                stats,
                None,
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
                - start,
            )

        except Exception as exc:
            log.exception(
                "SCAN FAILED"
            )

            try:
                finish_run(
                    conn,
                    run_id,
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
