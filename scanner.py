import hashlib
import json
import logging
import os
import re
import statistics
import time
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    psycopg2 = None
    Json = None


KALSHI_API_URL = os.environ.get(
    "KALSHI_API_URL",
    "https://external-api.kalshi.com/trade-api/v2",
).rstrip("/")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
DISCORD_RELAY_URL = os.environ.get("DISCORD_RELAY_URL", "").strip()
DISCORD_RELAY_SECRET = os.environ.get("DISCORD_RELAY_SECRET", "").strip()
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))
FORECAST_DAYS = int(os.environ.get("FORECAST_DAYS", "3"))
WEATHER_REFRESH_SECONDS = int(
    os.environ.get("WEATHER_REFRESH_SECONDS", "1800")
)
MIN_FORECAST_PROBABILITY_CHANGE_POINTS = float(
    os.environ.get("MIN_FORECAST_PROBABILITY_CHANGE_POINTS", "20")
)
MIN_MARKET_LAG_POINTS = float(
    os.environ.get("MIN_MARKET_LAG_POINTS", "10")
)
MIN_PRELIMINARY_EDGE_POINTS = float(
    os.environ.get("MIN_PRELIMINARY_EDGE_POINTS", "10")
)
MIN_ENTRY_PRICE_CENTS = float(
    os.environ.get("MIN_ENTRY_PRICE_CENTS", "5")
)
MAX_ENTRY_PRICE_CENTS = float(
    os.environ.get("MAX_ENTRY_PRICE_CENTS", "95")
)
PAPER_RISK_DOLLARS = float(
    os.environ.get("PAPER_RISK_DOLLARS", "10")
)
ALLOW_UNVERIFIED_LOCATION_SIGNALS = os.environ.get(
    "ALLOW_UNVERIFIED_LOCATION_SIGNALS", "false"
).lower() in {"1", "true", "yes"}
ENSEMBLE_MODEL = "gfs_seamless"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("weather-kalshi-scanner")


# Forecast proxy coordinates. These are not asserted to be legal Kalshi
# settlement coordinates. Only entries marked True may signal by default.
LOCATION_MAP = {
    "NYC": ("New York City", 40.7789, -73.9692, "America/New_York", True),
    "CHI": ("Chicago", 41.9742, -87.9073, "America/Chicago", False),
    "MIA": ("Miami", 25.7959, -80.2870, "America/New_York", False),
    "AUS": ("Austin", 30.1975, -97.6663, "America/Chicago", False),
    "LAX": ("Los Angeles", 33.9425, -118.4081, "America/Los_Angeles", False),
    "DAL": ("Dallas", 32.8998, -97.0403, "America/Chicago", False),
    "SEA": ("Seattle", 47.4502, -122.3088, "America/Los_Angeles", False),
    "HOU": ("Houston", 29.6454, -95.2789, "America/Chicago", False),
    "OKC": ("Oklahoma City", 35.3931, -97.6007, "America/Chicago", False),
    "PHIL": ("Philadelphia", 39.8744, -75.2424, "America/New_York", False),
    "PHX": ("Phoenix", 33.4342, -112.0116, "America/Phoenix", False),
    "SFO": ("San Francisco", 37.6213, -122.3790, "America/Los_Angeles", False),
    "LV": ("Las Vegas", 36.0840, -115.1537, "America/Los_Angeles", False),
    "MIN": ("Minneapolis", 44.8848, -93.2223, "America/Chicago", False),
    "NOLA": ("New Orleans", 30.0424, -90.0289, "America/Chicago", False),
    "DEN": ("Denver", 39.8561, -104.6737, "America/Denver", False),
    "TTN": ("Trenton", 40.2767, -74.8135, "America/New_York", False),
    "EWR": ("Newark", 40.6895, -74.1745, "America/New_York", False),
    "DC": ("Washington DC", 38.8512, -77.0402, "America/New_York", False),
    "BOS": ("Boston", 42.3656, -71.0096, "America/New_York", False),
    "ATL": ("Atlanta", 33.6407, -84.4277, "America/New_York", False),
    "SATX": ("San Antonio", 29.5337, -98.4698, "America/Chicago", False),
}


def utc_now():
    return datetime.now(timezone.utc)


def safe_float(value, default=None):
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    if psycopg2 is None:
        raise RuntimeError("psycopg2-binary is not installed")
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


def db_execute(sql, params=None, fetch=False, fetchone=False):
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if fetchone:
                result = cur.fetchone()
            elif fetch:
                result = cur.fetchall()
            else:
                result = None
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_schema():
    statements = [
        """
        CREATE TABLE IF NOT EXISTS scan_runs (
            id BIGSERIAL PRIMARY KEY,
            started_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ,
            status TEXT NOT NULL,
            stats JSONB NOT NULL DEFAULT '{}'::jsonb,
            error TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS series_registry (
            series_ticker TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            category TEXT,
            tags JSONB NOT NULL,
            settlement_sources JSONB NOT NULL,
            contract_terms_url TEXT,
            updated_at TIMESTAMPTZ NOT NULL,
            raw_series JSONB NOT NULL
        )
        """,
        """
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
            UNIQUE(city, variable, model, forecast_date, payload_hash)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_forecast_lookup
        ON forecast_observations(city, variable, model, forecast_date, observed_at DESC)
        """,
        """
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
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_market_lookup
        ON market_snapshots(ticker, observed_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS paper_trades (
            id BIGSERIAL PRIMARY KEY,
            signal_fingerprint TEXT UNIQUE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            settled_at TIMESTAMPTZ,
            city TEXT NOT NULL,
            forecast_date DATE NOT NULL,
            market_ticker TEXT NOT NULL,
            market_kind TEXT NOT NULL,
            side TEXT NOT NULL,
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
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_paper_status
        ON paper_trades(status, created_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS alert_log (
            fingerprint TEXT PRIMARY KEY,
            sent_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL
        )
        """,
    ]
    for sql in statements:
        db_execute(sql)


def get_latest_weather_fetch_time():
    row = db_execute(
        """
        SELECT MAX(started_at)
        FROM scan_runs
        WHERE status='success'
          AND (stats->>'weather_refreshed')='true'
        """,
        fetchone=True,
    )
    return row[0] if row and row[0] else None


def payload_hash(payload):
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def insert_forecast(city, variable, model, date_key, scalar, payload):
    db_execute(
        """
        INSERT INTO forecast_observations(
            observed_at,city,variable,model,forecast_date,
            scalar_value,payload,payload_hash
        ) VALUES(NOW(),%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(city,variable,model,forecast_date,payload_hash)
        DO NOTHING
        """,
        (
            city,
            variable,
            model,
            date_key,
            scalar,
            Json(payload),
            payload_hash(payload),
        ),
    )


def latest_forecast(city, variable, model, date_key):
    return db_execute(
        """
        SELECT observed_at, scalar_value, payload
        FROM forecast_observations
        WHERE city=%s AND variable=%s AND model=%s AND forecast_date=%s
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        (city, variable, model, date_key),
        fetchone=True,
    )


def latest_market(ticker):
    return db_execute(
        """
        SELECT observed_at, yes_bid_cents, yes_ask_cents,
               no_bid_cents, no_ask_cents, last_price_cents
        FROM market_snapshots
        WHERE ticker=%s
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        (ticker,),
        fetchone=True,
    )


def parse_market_date(market):
    event_ticker = market.get("event_ticker") or ""
    for part in event_ticker.split("-"):
        try:
            return datetime.strptime(part, "%y%b%d").date().isoformat()
        except ValueError:
            continue
    return None


def get_connectionless_market_fields(market):
    def cents(value):
        value = safe_float(value)
        return None if value is None else value * 100.0

    return (
        cents(market.get("yes_bid_dollars")),
        cents(market.get("yes_ask_dollars")),
        cents(market.get("no_bid_dollars")),
        cents(market.get("no_ask_dollars")),
        cents(market.get("last_price_dollars")),
    )


def insert_market_snapshot(market, city, kind):
    yes_bid, yes_ask, no_bid, no_ask, last_price = (
        get_connectionless_market_fields(market)
    )
    db_execute(
        """
        INSERT INTO market_snapshots(
            observed_at,ticker,event_ticker,series_ticker,market_date,
            city,market_kind,strike_type,floor_strike,cap_strike,
            yes_bid_cents,yes_ask_cents,no_bid_cents,no_ask_cents,
            last_price_cents,status,result
        ) VALUES(
            NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            market.get("ticker") or "",
            market.get("event_ticker"),
            market.get("series_ticker"),
            parse_market_date(market),
            city,
            kind,
            market.get("strike_type"),
            safe_float(market.get("floor_strike")),
            safe_float(market.get("cap_strike")),
            yes_bid,
            yes_ask,
            no_bid,
            no_ask,
            last_price,
            market.get("status"),
            market.get("result"),
        ),
    )


# ==========================================================
# KALSHI
# ==========================================================

def get_series_list():
    items = []
    cursor = None
    while True:
        params = {
            "category": "Climate and Weather",
            "include_product_metadata": "true",
        }
        if cursor:
            params["cursor"] = cursor
        data = http_json(
            f"{KALSHI_API_URL}/series",
            params=params,
        )
        items.extend(data.get("series", []))
        cursor = data.get("cursor")
        if not cursor:
            return items


def save_series_registry(series_list):
    for series in series_list:
        db_execute(
            """
            INSERT INTO series_registry(
                series_ticker,title,category,tags,settlement_sources,
                contract_terms_url,updated_at,raw_series
            ) VALUES(%s,%s,%s,%s,%s,%s,NOW(),%s)
            ON CONFLICT(series_ticker) DO UPDATE SET
                title=EXCLUDED.title,
                category=EXCLUDED.category,
                tags=EXCLUDED.tags,
                settlement_sources=EXCLUDED.settlement_sources,
                contract_terms_url=EXCLUDED.contract_terms_url,
                updated_at=NOW(),
                raw_series=EXCLUDED.raw_series
            """,
            (
                series.get("ticker"),
                series.get("title", ""),
                series.get("category"),
                Json(series.get("tags", [])),
                Json(series.get("settlement_sources", [])),
                series.get("contract_terms_url"),
                Json(series),
            ),
        )


def get_series_markets(series_ticker):
    items = []
    cursor = None
    while True:
        params = {
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 1000,
        }
        if cursor:
            params["cursor"] = cursor
        data = http_json(
            f"{KALSHI_API_URL}/markets",
            params=params,
        )
        items.extend(data.get("markets", []))
        cursor = data.get("cursor")
        if not cursor:
            return items


def get_series_metadata(ticker):
    data = http_json(
        f"{KALSHI_API_URL}/series/{ticker}"
    )
    return data.get("series", {})


def temperature_series(series):
    title = (series.get("title") or "").lower()
    tags = " ".join(str(x).lower() for x in series.get("tags", []))
    return (
        series.get("frequency") == "daily"
        and "temperature" in (title + " " + tags)
        and ("highest" in title or "high" in title or "maximum" in title)
    )


def is_rain_series(series):
    ticker = (series.get("ticker") or "").upper()
    title = (series.get("title") or "").lower()
    return ticker == "KXRAIN" or (
        series.get("frequency") == "daily"
        and ("rain" in title or "precipitation" in title)
    )


def location_from_title_or_ticker(series):
    title = (series.get("title") or "").upper()
    ticker = (series.get("ticker") or "").upper()
    for code in LOCATION_MAP:
        if re.search(rf"\b{re.escape(code)}\b", title):
            return LOCATION_MAP[code]
        if ticker.startswith("KXHIGH") and ticker[6:] == code:
            return LOCATION_MAP[code]
    # Common spelled-out names that don't use abbreviations in the title.
    aliases = {
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
    for alias, code in aliases.items():
        if alias in title:
            return LOCATION_MAP[code]
    return None


def rain_city_from_ticker(ticker):
    suffix = (ticker or "").rsplit("-", 1)[-1].upper()
    return LOCATION_MAP.get(suffix)


# ==========================================================
# HTTP / OPEN-METEO
# ==========================================================

def http_json(url, params=None):
    response = requests.get(
        url,
        params=params,
        headers={
            "User-Agent": "WeatherKalshiResearchBot/4.0",
            "Accept": "application/json",
        },
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"HTTP {response.status_code}: {response.text[:500]}"
        )
    return response.json()


def location_params(city_names):
    return {
        "latitude": ",".join(str(LOCATION_MAP[x][1]) for x in city_names),
        "longitude": ",".join(str(LOCATION_MAP[x][2]) for x in city_names),
    }


def local_date(timestamp, timezone_name):
    dt = datetime.fromisoformat(
        str(timestamp).replace("Z", "+00:00")
    )
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(
        ZoneInfo(timezone_name)
    ).date().isoformat()


def aggregate_hourly(location_data, city_name):
    hourly = location_data.get("hourly", {})
    timestamps = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    precip = hourly.get("precipitation", [])
    grouped = defaultdict(lambda: {"temps": [], "precip": []})
    tz = LOCATION_MAP[city_name][3]
    for i, ts in enumerate(timestamps):
        date_key = local_date(ts, tz)
        if i < len(temps):
            value = safe_float(temps[i])
            if value is not None:
                grouped[date_key]["temps"].append(value)
        if i < len(precip):
            value = safe_float(precip[i])
            if value is not None:
                grouped[date_key]["precip"].append(value)
    result = {}
    for date_key, values in grouped.items():
        if not values["temps"]:
            continue
        result[date_key] = {
            "high": max(values["temps"]),
            "precipitation_sum": sum(values["precip"]),
        }
    return result


def fetch_deterministic_model(model, city_names):
    params = location_params(city_names)
    params.update({
        "models": model,
        "hourly": "temperature_2m,precipitation",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "timezone": "UTC",
        "forecast_days": FORECAST_DAYS,
    })
    url = (
        "https://api.open-meteo.com/v1/gfs"
        if model in {"hrrr", "nbm"}
        else "https://api.open-meteo.com/v1/forecast"
    )
    data = http_json(url, params)
    locations = data if isinstance(data, list) else [data]
    if len(locations) != len(city_names):
        raise RuntimeError(
            f"{model}: expected {len(city_names)} locations, got {len(locations)}"
        )
    return {
        city: {
            "daily": aggregate_hourly(loc, city),
            "model_run": (
                loc.get("model_run")
                or loc.get("model_run_id")
                or loc.get("model_run_time")
            ),
        }
        for city, loc in zip(city_names, locations)
    }


def fetch_ensemble(city_names):
    params = location_params(city_names)
    params.update({
        "models": ENSEMBLE_MODEL,
        "hourly": "temperature_2m,precipitation",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "timezone": "UTC",
        "forecast_days": FORECAST_DAYS,
    })
    data = http_json(
        "https://ensemble-api.open-meteo.com/v1/ensemble",
        params,
    )
    locations = data if isinstance(data, list) else [data]
    if len(locations) != len(city_names):
        raise RuntimeError(
            f"ensemble: expected {len(city_names)} locations, got {len(locations)}"
        )
    output = {}
    for city, loc in zip(city_names, locations):
        hourly = loc.get("hourly", {})
        timestamps = hourly.get("time", [])
        temp_keys = sorted(
            key for key in hourly
            if key.startswith("temperature_2m_member")
        )
        precip_keys = sorted(
            key for key in hourly
            if key.startswith("precipitation_member")
        )
        if not temp_keys:
            raise RuntimeError(
                f"ensemble: no temperature member keys for {city}"
            )
        if not precip_keys:
            raise RuntimeError(
                f"ensemble: no precipitation member keys for {city}"
            )
        tz = LOCATION_MAP[city][3]
        day = defaultdict(lambda: {
            "temp": defaultdict(list),
            "precip": defaultdict(float),
        })
        for i, ts in enumerate(timestamps):
            date_key = local_date(ts, tz)
            for key in temp_keys:
                values = hourly.get(key, [])
                if i < len(values):
                    value = safe_float(values[i])
                    if value is not None:
                        day[date_key]["temp"][key].append(value)
            for key in precip_keys:
                values = hourly.get(key, [])
                if i < len(values):
                    value = safe_float(values[i])
                    if value is not None:
                        day[date_key]["precip"][key] += value
        city_daily = {}
        for date_key, values in day.items():
            highs = [
                max(v) for v in values["temp"].values()
                if v
            ]
            rain = list(values["precip"].values())
            if highs and rain:
                city_daily[date_key] = {
                    "member_highs": highs,
                    "member_precip_totals": rain,
                    "temperature_mean": statistics.mean(highs),
                    "temperature_median": statistics.median(highs),
                }
        output[city] = {
            "daily": city_daily,
            "temperature_member_count": len(temp_keys),
            "precipitation_member_count": len(precip_keys),
        }
    return output


def save_weather_forecasts(deterministic, ensemble):
    for model, cities in deterministic.items():
        for city, data in cities.items():
            for date_key, daily in data["daily"].items():
                insert_forecast(
                    city,
                    "temperature_high",
                    model,
                    date_key,
                    daily["high"],
                    {
                        **daily,
                        "model_run": data.get("model_run"),
                    },
                )
                insert_forecast(
                    city,
                    "precipitation_sum",
                    model,
                    date_key,
                    daily["precipitation_sum"],
                    {
                        **daily,
                        "model_run": data.get("model_run"),
                    },
                )
    for city, data in ensemble.items():
        for date_key, daily in data["daily"].items():
            insert_forecast(
                city,
                "ensemble_temperature_distribution",
                ENSEMBLE_MODEL,
                date_key,
                daily["temperature_mean"],
                {"member_highs": daily["member_highs"]},
            )
            insert_forecast(
                city,
                "ensemble_rain_distribution",
                ENSEMBLE_MODEL,
                date_key,
                statistics.mean(daily["member_precip_totals"]),
                {"member_precip_totals": daily["member_precip_totals"]},
            )


# ==========================================================
# PROBABILITIES / SIGNALS
# ==========================================================

def temperature_probability(member_highs, market):
    if not member_highs:
        return None
    strike_type = (market.get("strike_type") or "").lower()
    floor = safe_float(market.get("floor_strike"))
    cap = safe_float(market.get("cap_strike"))
    if strike_type == "greater" and floor is not None:
        hits = sum(x > floor for x in member_highs)
    elif strike_type == "less" and cap is not None:
        hits = sum(x < cap for x in member_highs)
    elif strike_type == "between" and floor is not None and cap is not None:
        hits = sum(floor <= x <= cap for x in member_highs)
    else:
        return None
    return 100.0 * hits / len(member_highs)


def rain_probability(member_precip):
    if not member_precip:
        return None
    return 100.0 * sum(value > 0.0 for value in member_precip) / len(member_precip)


def get_side_ask_cents(market, side):
    field = "yes_ask_dollars" if side == "YES" else "no_ask_dollars"
    value = safe_float(market.get(field))
    return None if value is None else value * 100.0


def prior_member_data(city, date_key, variable):
    row = latest_forecast(
        city,
        variable,
        ENSEMBLE_MODEL,
        date_key,
    )
    if not row:
        return None
    return row[2] or {}


def prior_hrrr_high(city, date_key):
    row = latest_forecast(
        city,
        "temperature_high",
        "hrrr",
        date_key,
    )
    return safe_float(row[1]) if row else None


def location_signal_allowed(entry):
    return bool(entry[4]) or ALLOW_UNVERIFIED_LOCATION_SIGNALS


def build_candidate(
    city,
    date_key,
    market,
    current_probability,
    previous_probability,
    hrrr_change_f,
    kind,
):
    if current_probability is None or previous_probability is None:
        return None

    probability_change = current_probability - previous_probability
    if abs(probability_change) < MIN_FORECAST_PROBABILITY_CHANGE_POINTS:
        return None

    ticker = market.get("ticker") or ""
    previous_market = latest_market(ticker)
    if not previous_market:
        return None

    best = None
    for side in ("YES", "NO"):
        ask = get_side_ask_cents(market, side)
        if ask is None or not (MIN_ENTRY_PRICE_CENTS <= ask <= MAX_ENTRY_PRICE_CENTS):
            continue

        side_probability = current_probability if side == "YES" else 100.0 - current_probability
        side_probability_change = probability_change if side == "YES" else -probability_change
        previous_ask = safe_float(
            previous_market[2] if side == "YES" else previous_market[4]
        )
        if previous_ask is None:
            continue

        market_change = ask - previous_ask
        market_lag = side_probability_change - market_change
        preliminary_edge = side_probability - ask

        if market_lag < MIN_MARKET_LAG_POINTS:
            continue
        if preliminary_edge < MIN_PRELIMINARY_EDGE_POINTS:
            continue

        candidate = {
            "city": city,
            "forecast_date": date_key,
            "market_ticker": ticker,
            "market_kind": kind,
            "side": side,
            "entry_price_cents": ask,
            "model_probability_proxy": side_probability,
            "preliminary_edge_points": preliminary_edge,
            "forecast_probability_change_points": side_probability_change,
            "market_price_change_points": market_change,
            "market_lag_points": market_lag,
            "forecast_temperature_change_f": hrrr_change_f,
            "contract_label": market.get("title") or "",
        }
        if best is None or (
            candidate["market_lag_points"],
            candidate["preliminary_edge_points"],
        ) > (
            best["market_lag_points"],
            best["preliminary_edge_points"],
        ):
            best = candidate

    return best


def signal_fingerprint(signal):
    raw = "|".join([
        signal["market_ticker"],
        signal["side"],
        signal["forecast_date"],
        f"{signal['entry_price_cents']:.2f}",
        f"{signal['model_probability_proxy']:.2f}",
        f"{signal['forecast_probability_change_points']:.2f}",
        f"{signal['market_price_change_points']:.2f}",
    ])
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def alert_exists(fp):
    return db_execute(
        "SELECT 1 FROM alert_log WHERE fingerprint=%s",
        (fp,),
        fetchone=True,
    ) is not None


def paper_trade_exists(fp):
    return db_execute(
        "SELECT 1 FROM paper_trades WHERE signal_fingerprint=%s",
        (fp,),
        fetchone=True,
    ) is not None


def open_paper_trade(signal, reason):
    fp = signal_fingerprint(signal)
    if paper_trade_exists(fp):
        return fp

    entry = signal["entry_price_cents"] / 100.0
    if entry <= 0:
        return fp

    contracts = PAPER_RISK_DOLLARS / entry
    db_execute(
        """
        INSERT INTO paper_trades(
            signal_fingerprint,created_at,city,forecast_date,market_ticker,
            market_kind,side,entry_price_cents,stake_dollars,contracts,
            model_probability_proxy,preliminary_edge_points,
            forecast_probability_change_points,market_price_change_points,
            market_lag_points,forecast_temperature_change_f,reason,status
        ) VALUES(%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open')
        ON CONFLICT(signal_fingerprint) DO NOTHING
        """,
        (
            fp,
            signal["city"],
            signal["forecast_date"],
            signal["market_ticker"],
            signal["market_kind"],
            signal["side"],
            signal["entry_price_cents"],
            PAPER_RISK_DOLLARS,
            contracts,
            signal["model_probability_proxy"],
            signal["preliminary_edge_points"],
            signal["forecast_probability_change_points"],
            signal["market_price_change_points"],
            signal["market_lag_points"],
            signal.get("forecast_temperature_change_f"),
            Json(reason),
        ),
    )
    return fp


def send_discord(message):
    if not DISCORD_RELAY_URL or not DISCORD_RELAY_SECRET:
        log.error("Discord relay is not configured.")
        return False
    try:
        response = requests.post(
            DISCORD_RELAY_URL,
            json={
                "secret": DISCORD_RELAY_SECRET,
                "message": message,
            },
            headers={
                "User-Agent": "WeatherKalshiResearchBot/4.0",
                "Accept": "application/json",
            },
            timeout=REQUEST_TIMEOUT,
        )
        log.info("Discord relay response: %s", response.status_code)
        if not (200 <= response.status_code < 300):
            log.error("Discord relay error: %s", response.text[:1000])
            return False
        try:
            payload = response.json()
        except Exception:
            payload = {}
        if payload.get("success") is False:
            log.error("Discord relay reported failure: %s", payload)
            return False
        return True
    except Exception as exc:
        log.error("Discord relay exception: %s", exc)
        return False


def signal_message(signal):
    temp_line = ""
    temp = signal.get("forecast_temperature_change_f")
    if temp is not None:
        temp_line = f"HRRR high change: **{temp:+.1f}°F**\n"
    return (
        "🌦️ **WEATHER FORECAST SHOCK — PAPER TRADE**\n\n"
        f"**{signal['city']} — {signal['forecast_date']}**\n"
        f"Market: `{signal['market_ticker']}`\n"
        f"Side: **{signal['side']}**\n"
        f"Entry ask: **{signal['entry_price_cents']:.1f}¢**\n\n"
        f"Model probability proxy: **{signal['model_probability_proxy']:.1f}%**\n"
        f"Forecast probability change: **{signal['forecast_probability_change_points']:+.1f} pts**\n"
        f"Market ask change: **{signal['market_price_change_points']:+.1f} pts**\n"
        f"Estimated market lag: **{signal['market_lag_points']:+.1f} pts**\n"
        f"Preliminary edge: **{signal['preliminary_edge_points']:+.1f} pts**\n"
        f"{temp_line}\n"
        f"Paper risk: **${PAPER_RISK_DOLLARS:.2f}**\n\n"
        "⚠️ Research only. Model probability is an uncalibrated ensemble-frequency proxy; "
        "settlement source/location still requires validation."
    )


def record_alert(fp, signal):
    db_execute(
        """
        INSERT INTO alert_log(fingerprint,sent_at,payload)
        VALUES(%s,NOW(),%s)
        ON CONFLICT(fingerprint) DO NOTHING
        """,
        (fp, Json(signal)),
    )


# ==========================================================
# PAPER SETTLEMENT
# ==========================================================

def settle_paper_trades():
    rows = db_execute(
        """
        SELECT id,market_ticker,side,stake_dollars,contracts
        FROM paper_trades
        WHERE status='open'
        ORDER BY created_at ASC
        LIMIT 200
        """,
        fetch=True,
    )
    for trade_id, ticker, side, stake, contracts in rows:
        try:
            data = http_json(
                f"{KALSHI_API_URL}/markets/{ticker}"
            )
            market = data.get("market", {})
            result = (market.get("result") or "").lower()
            if result not in {"yes", "no"}:
                continue
            won = result == side.lower()
            pnl = contracts - stake if won else -stake
            db_execute(
                """
                UPDATE paper_trades
                SET settled_at=NOW(),result=%s,profit_loss_dollars=%s,status='settled'
                WHERE id=%s
                """,
                (result, pnl, trade_id),
            )
            log.info(
                "PAPER TRADE SETTLED | %s | %s | %s | P/L=$%.2f",
                ticker,
                side,
                result,
                pnl,
            )
        except Exception as exc:
            log.warning(
                "Could not settle %s: %s",
                ticker,
                exc,
            )


# ==========================================================
# SCANNER
# ==========================================================

def start_scan_run():
    row = db_execute(
        """
        INSERT INTO scan_runs(started_at,status,stats)
        VALUES(NOW(),'running','{}'::jsonb)
        RETURNING id
        """,
        fetchone=True,
    )
    return row[0]


def finish_scan_run(scan_id, status, stats, error=None):
    db_execute(
        """
        UPDATE scan_runs
        SET completed_at=NOW(),status=%s,stats=%s,error=%s
        WHERE id=%s
        """,
        (
            status,
            Json(stats),
            error,
            scan_id,
        ),
    )


def run_scan():
    scan_id = None
    stats = {
        "temperature_series": 0,
        "temperature_markets": 0,
        "rain_series": 0,
        "rain_markets": 0,
        "weather_refreshed": False,
        "forecast_shocks": 0,
        "paper_trades_created": 0,
        "discord_alerts": 0,
        "settled_trades": 0,
    }
    started = time.time()

    try:
        ensure_schema()
        scan_id = start_scan_run()
        settle_paper_trades()

        all_series = get_series_list()
        save_series_registry(all_series)
        temp_series = [s for s in all_series if temperature_series(s)]
        rain_series_list = [s for s in all_series if is_rain_series(s)]
        stats["temperature_series"] = len(temp_series)
        stats["rain_series"] = len(rain_series_list)

        # Build the union of all city proxies needed by both the discovered
        # daily temperature series and the current KXRAIN multi-city event.
        # Rain-only cities therefore receive weather forecasts too.
        city_locations = {}
        for series in temp_series:
            location = location_from_title_or_ticker(series)
            if location is not None:
                city_locations[location[0]] = location

        try:
            rain_markets_for_city_discovery = get_series_markets("KXRAIN")
        except Exception as exc:
            rain_markets_for_city_discovery = []
            log.warning(
                "KXRAIN city discovery failed: %s",
                exc,
            )

        for market in rain_markets_for_city_discovery:
            location = rain_city_from_ticker(
                market.get("ticker")
            )
            if location is not None:
                city_locations[location[0]] = location

        if not city_locations:
            raise RuntimeError(
                "No current Kalshi weather markets could be mapped to forecast coordinates."
            )

        city_names = sorted(city_locations)

        last_weather = get_latest_weather_fetch_time()
        refresh_weather = (
            last_weather is None
            or (utc_now() - last_weather).total_seconds() >= WEATHER_REFRESH_SECONDS
        )

        deterministic = {}
        ensemble = {}

        if refresh_weather:
            stats["weather_refreshed"] = True
            for model in (
                "hrrr",
                "nbm",
                "gfs_seamless",
                "ecmwf_ifs025",
            ):
                deterministic[model] = fetch_deterministic_model(
                    model,
                    city_names,
                )
                log.info(
                    "Weather model fetched: %s",
                    model,
                )
            ensemble = fetch_ensemble(city_names)

            # IMPORTANT: signals are calculated against the previous database
            # observations BEFORE this fresh forecast is inserted.
            process_temperature_signals(
                temp_series,
                deterministic,
                ensemble,
                stats,
            )
            process_rain_signals(
                rain_series_list,
                ensemble,
                stats,
            )

            # Only after signal comparison is complete do we write the new
            # forecast observations as the latest baseline.
            save_weather_forecasts(
                deterministic,
                ensemble,
            )

        # Market snapshots are collected every five-minute cron run,
        # regardless of whether a new weather forecast was fetched.
        collect_market_snapshots(
            temp_series,
            rain_series_list,
            stats,
        )

        settle_paper_trades()
        finish_scan_run(
            scan_id,
            "success",
            stats,
            None,
        )
        log.info(
            "SCAN COMPLETE | %s | runtime=%.1fs",
            json.dumps(stats, default=str),
            time.time() - started,
        )

    except Exception as exc:
        log.exception("SCAN FAILED")
        if scan_id is not None:
            try:
                finish_scan_run(
                    scan_id,
                    "failed",
                    stats,
                    str(exc),
                )
            except Exception:
                log.exception("Could not record scan failure")
        raise


def process_temperature_signals(temp_series, deterministic, ensemble, stats):
    for series in temp_series:
        location = location_from_title_or_ticker(series)
        if location is None:
            log.warning(
                "Unmapped temperature series: %s | %s",
                series.get("ticker"),
                series.get("title"),
            )
            continue

        city, _, _, tz, verified = location
        if not location_signal_allowed(location):
            log.info(
                "Monitoring %s, but paper signals are blocked because settlement location is unverified.",
                city,
            )

        try:
            markets = get_series_markets(
                series["ticker"]
            )
        except Exception as exc:
            log.error(
                "Could not fetch %s: %s",
                series.get("ticker"),
                exc,
            )
            continue

        stats["temperature_markets"] += len(markets)
        city_ensemble = (
            ensemble.get(city, {}).get("daily", {})
        )

        for market in markets:
            date_key = parse_market_date(market)
            if not date_key:
                continue
            today = datetime.now(
                ZoneInfo(tz)
            ).date().isoformat()
            if date_key < today:
                continue

            current_daily = city_ensemble.get(date_key)
            if not current_daily:
                continue

            previous = prior_member_data(
                city,
                date_key,
                "ensemble_temperature_distribution",
            )
            if not previous:
                continue

            old_members = previous.get(
                "member_highs",
                [],
            )
            current_members = current_daily[
                "member_highs"
            ]
            current_prob = temperature_probability(
                current_members,
                market,
            )
            previous_prob = temperature_probability(
                old_members,
                market,
            )

            hrrr_old = prior_hrrr_high(
                city,
                date_key,
            )
            hrrr_current = (
                deterministic.get("hrrr", {})
                .get(city, {})
                .get("daily", {})
                .get(date_key, {})
                .get("high")
            )
            hrrr_change = (
                hrrr_current - hrrr_old
                if hrrr_current is not None and hrrr_old is not None
                else None
            )

            if location_signal_allowed(location):
                signal = build_candidate(
                    city,
                    date_key,
                    market,
                    current_prob,
                    previous_prob,
                    hrrr_change,
                    "temperature",
                )
                if signal:
                    stats["forecast_shocks"] += 1
                    emit_signal(signal, series, stats)


def process_rain_signals(rain_series_list, ensemble, stats):
    for series in rain_series_list:
        ticker = (series.get("ticker") or "").upper()
        if ticker != "KXRAIN":
            continue

        markets = get_series_markets("KXRAIN")
        for market in markets:
            location = rain_city_from_ticker(
                market.get("ticker")
            )
            if not location:
                continue

            city, _, _, tz, _ = location
            if not ALLOW_UNVERIFIED_LOCATION_SIGNALS:
                continue

            date_key = parse_market_date(market)
            if not date_key:
                continue
            today = datetime.now(
                ZoneInfo(tz)
            ).date().isoformat()
            if date_key < today:
                continue

            current_daily = (
                ensemble.get(city, {})
                .get("daily", {})
                .get(date_key)
            )
            if not current_daily:
                continue

            previous = prior_member_data(
                city,
                date_key,
                "ensemble_rain_distribution",
            )
            if not previous:
                continue

            current_prob = rain_probability(
                current_daily["member_precip_totals"]
            )
            previous_prob = rain_probability(
                previous.get("member_precip_totals", [])
            )

            signal = build_candidate(
                city,
                date_key,
                market,
                current_prob,
                previous_prob,
                None,
                "rain",
            )
            if signal:
                stats["forecast_shocks"] += 1
                emit_signal(
                    signal,
                    series,
                    stats,
                )


def collect_market_snapshots(temp_series, rain_series_list, stats):
    for series in temp_series:
        location = location_from_title_or_ticker(series)
        city = location[0] if location else None
        try:
            markets = get_series_markets(series["ticker"])
        except Exception as exc:
            log.warning(
                "Market snapshot fetch failed for %s: %s",
                series.get("ticker"),
                exc,
            )
            continue
        for market in markets:
            insert_market_snapshot(
                market,
                city,
                "temperature",
            )

    for series in rain_series_list:
        if (series.get("ticker") or "").upper() != "KXRAIN":
            continue
        try:
            markets = get_series_markets("KXRAIN")
        except Exception as exc:
            log.warning(
                "KXRAIN snapshot fetch failed: %s",
                exc,
            )
            continue
        for market in markets:
            location = rain_city_from_ticker(
                market.get("ticker")
            )
            city = location[0] if location else None
            insert_market_snapshot(
                market,
                city,
                "rain",
            )
        break


def emit_signal(signal, series, stats):
    reason = {
        "strategy": "large weather forecast shock plus insufficient market response",
        "model_probability_is_calibrated": False,
        "ensemble_model": ENSEMBLE_MODEL,
        "paper_risk_dollars": PAPER_RISK_DOLLARS,
        "series_settlement_sources": series.get("settlement_sources", []),
        "contract_terms_url": series.get("contract_terms_url"),
        "location_signal_gate": "verified_only" if not ALLOW_UNVERIFIED_LOCATION_SIGNALS else "proxy_allowed",
    }

    fp = open_paper_trade(
        signal,
        reason,
    )

    # A paper trade is persistent even if Discord is temporarily unavailable.
    # The alert log is recorded ONLY after Discord successfully accepts it.
    if not alert_exists(fp):
        if send_discord(signal_message(signal)):
            record_alert(
                fp,
                {
                    "signal": signal,
                    "reason": reason,
                },
            )
            stats["discord_alerts"] += 1
            log.info(
                "DISCORD ALERT SENT | %s | %s | %s",
                signal["city"],
                signal["market_ticker"],
                signal["side"],
            )
        else:
            log.error(
                "Discord alert failed; paper trade remains recorded for retry."
            )

    stats["paper_trades_created"] += 1


if __name__ == "__main__":
    run_scan()
