import hashlib
import json
import logging
import os
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
NWS_API_URL = "https://api.weather.gov"
NWS_USER_AGENT = os.environ.get("NWS_USER_AGENT", "WeatherKalshiResearchBot/1.0")
FORECAST_DAYS = int(os.environ.get("FORECAST_DAYS", "3"))
MIN_FORECAST_PROBABILITY_CHANGE_POINTS = float(
    os.environ.get("MIN_FORECAST_PROBABILITY_CHANGE_POINTS", "20")
)
MIN_MARKET_LAG_POINTS = float(os.environ.get("MIN_MARKET_LAG_POINTS", "10"))
MIN_PRELIMINARY_EDGE_POINTS = float(
    os.environ.get("MIN_PRELIMINARY_EDGE_POINTS", "10")
)
MIN_ENTRY_PRICE_CENTS = float(os.environ.get("MIN_ENTRY_PRICE_CENTS", "5"))
MAX_ENTRY_PRICE_CENTS = float(os.environ.get("MAX_ENTRY_PRICE_CENTS", "95"))
PAPER_RISK_DOLLARS = float(os.environ.get("PAPER_RISK_DOLLARS", "10"))
RESEARCH_MIN_FORECAST_CHANGE_POINTS = float(os.environ.get("RESEARCH_MIN_FORECAST_CHANGE_POINTS", "3"))
RAIN_SIGNAL_CODES = {"NYC", "CHI", "MIA", "AUS"}

_DB_CONN = None

# We deliberately do NOT enable automatic signals for unverified settlement
# locations. All four cities are monitored; only verified entries can signal.
ALLOW_UNVERIFIED_LOCATION_SIGNALS = os.environ.get(
    "ALLOW_UNVERIFIED_LOCATION_SIGNALS", "false"
).lower() in {"1", "true", "yes"}

# The forecast-shock probability source is the GFS ensemble. Deterministic
# GFS is used only as a diagnostic. HRRR/NBM are deliberately optional and
# are not allowed to make a scan fail.
ENSEMBLE_MODEL = "gfs_seamless"
DETERMINISTIC_MODEL = "gfs_seamless"

LOCATION_MAP = {
    "NYC": ("New York City", 40.7789, -73.9692, "America/New_York", True),
    "CHI": ("Chicago", 41.9742, -87.9073, "America/Chicago", False),
    "MIA": ("Miami", 25.7959, -80.2870, "America/New_York", False),
    "AUS": ("Austin", 30.1975, -97.6663, "America/Chicago", False),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("weather-kalshi-scanner")


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
    global _DB_CONN
    if _DB_CONN is None or _DB_CONN.closed:
        _DB_CONN = db_conn()
    try:
        with _DB_CONN.cursor() as cur:
            cur.execute(sql, params or ())
            if fetchone:
                result = cur.fetchone()
            elif fetch:
                result = cur.fetchall()
            else:
                result = None
        _DB_CONN.commit()
        return result
    except Exception:
        _DB_CONN.rollback()
        raise


def db_close():
    global _DB_CONN
    if _DB_CONN is not None and not _DB_CONN.closed:
        _DB_CONN.close()
    _DB_CONN = None


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
        CREATE TABLE IF NOT EXISTS weather_service_state (
            city_code TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            last_update_at TIMESTAMPTZ NOT NULL,
            checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
        """
        CREATE TABLE IF NOT EXISTS forecast_research_events (
            id BIGSERIAL PRIMARY KEY,
            event_fingerprint TEXT UNIQUE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            city TEXT NOT NULL,
            forecast_date DATE NOT NULL,
            variable TEXT NOT NULL,
            market_ticker TEXT NOT NULL,
            side TEXT NOT NULL,
            previous_probability DOUBLE PRECISION NOT NULL,
            current_probability DOUBLE PRECISION NOT NULL,
            forecast_probability_change_points DOUBLE PRECISION NOT NULL,
            pre_forecast_ask_cents DOUBLE PRECISION,
            event_ask_cents DOUBLE PRECISION,
            initial_market_change_points DOUBLE PRECISION,
            initial_market_lag_points DOUBLE PRECISION,
            initial_preliminary_edge_points DOUBLE PRECISION,
            first_response_at TIMESTAMPTZ,
            milestone_25_at TIMESTAMPTZ,
            milestone_50_at TIMESTAMPTZ,
            milestone_75_at TIMESTAMPTZ,
            milestone_90_at TIMESTAMPTZ,
            latest_observation_at TIMESTAMPTZ,
            latest_ask_cents DOUBLE PRECISION,
            latest_market_move_points DOUBLE PRECISION,
            latest_lag_remaining_points DOUBLE PRECISION,
            max_market_move_points DOUBLE PRECISION DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'open',
            closed_at TIMESTAMPTZ,
            settlement_result TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS forecast_research_updates (
            id BIGSERIAL PRIMARY KEY,
            event_id BIGINT NOT NULL REFERENCES forecast_research_events(id) ON DELETE CASCADE,
            observed_at TIMESTAMPTZ NOT NULL,
            market_ask_cents DOUBLE PRECISION,
            market_move_points DOUBLE PRECISION,
            lag_remaining_points DOUBLE PRECISION,
            market_response_fraction DOUBLE PRECISION
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_research_events_open
        ON forecast_research_events(status, created_at DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_research_updates_event
        ON forecast_research_updates(event_id, observed_at DESC)
        """,
    ]
    for sql in statements:
        db_execute(sql)


def payload_hash(payload):
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def insert_forecast(city, variable, model, date_key, scalar, payload):
    db_execute(
        """
        INSERT INTO forecast_observations(
            observed_at,city,variable,model,forecast_date,
            scalar_value,payload,payload_hash
        ) VALUES(NOW(),%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(city,variable,model,forecast_date,payload_hash) DO NOTHING
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
    ticker = market.get("ticker") or ""
    for source in (event_ticker, ticker):
        for part in source.split("-"):
            try:
                return datetime.strptime(part, "%y%b%d").date().isoformat()
            except ValueError:
                pass
    return None


def location_from_title_or_ticker(series):
    title = (series.get("title") or "").upper()
    ticker = (series.get("ticker") or "").upper()

    # Current Kalshi weather series use tickers such as KXHIGHNY,
    # KXHIGHCHI, KXHIGHMIA, and KXHIGHAUS. Older/duplicate series
    # such as HIGHCHI may also appear in the API response. Prefer the
    # explicit ticker mapping before falling back to title matching.
    ticker_map = (
        ("KXHIGHNY", "NYC"),
        ("HIGHNY", "NYC"),
        ("KXHIGHCHI", "CHI"),
        ("HIGHCHI", "CHI"),
        ("KXHIGHMIA", "MIA"),
        ("HIGHMIA", "MIA"),
        ("KXHIGHAUS", "AUS"),
        ("HIGHAUS", "AUS"),
    )
    for prefix, code in ticker_map:
        if ticker == prefix or ticker.startswith(prefix + "-") or prefix in ticker:
            return LOCATION_MAP[code]

    aliases = {
        "NEW YORK CITY": "NYC",
        "NEW YORK": "NYC",
        "NYC": "NYC",
        "CHICAGO": "CHI",
        "MIAMI": "MIA",
        "AUSTIN": "AUS",
    }
    for alias, code in aliases.items():
        if alias in title:
            return LOCATION_MAP[code]
    return None


def temperature_series(series):
    title = (series.get("title") or "").lower()
    tags = " ".join(str(x).lower() for x in (series.get("tags") or []))
    text = title + " " + tags
    return (
        series.get("frequency") == "daily"
        and "temperature" in text
        and any(word in title for word in ("highest", "high", "maximum"))
    )


def is_rain_series(series):
    ticker = (series.get("ticker") or "").upper()
    title = (series.get("title") or "").lower()
    return ticker == "KXRAIN" or (
        series.get("frequency") == "daily"
        and ("rain" in title or "precipitation" in title)
    )


def http_json(url, params=None):
    response = requests.get(
        url,
        params=params,
        headers={
            "User-Agent": "WeatherKalshiResearchBot/5.0",
            "Accept": "application/json",
        },
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:800]}")
    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError("API returned invalid JSON") from exc


def nws_json(url):
    response = requests.get(
        url,
        headers={
            "User-Agent": NWS_USER_AGENT,
            "Accept": "application/geo+json, application/ld+json, application/json",
        },
        timeout=REQUEST_TIMEOUT,
    )
    if not 200 <= response.status_code < 300:
        raise RuntimeError(f"NWS {response.status_code}: {response.text[:500]}")
    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError("NWS API returned invalid JSON") from exc


def fetch_nws_updates(city_names):
    """Return each city's current NWS forecast update timestamp.

    We use NWS only as the publication/update trigger and provenance reference.
    GFS ensemble remains the probability source used by the existing strategy.
    """
    updates = {}
    for code in city_names:
        _, lat, lon, _, _ = LOCATION_MAP[code]
        point = nws_json(f"{NWS_API_URL}/points/{lat},{lon}")
        forecast_url = (point.get("properties") or {}).get("forecastHourly")
        if not forecast_url:
            raise RuntimeError(f"NWS: no hourly forecast URL for {code}")
        forecast = nws_json(forecast_url)
        properties = forecast.get("properties") or {}
        updated = properties.get("updated") or properties.get("updateTime")
        if not updated:
            raise RuntimeError(f"NWS: no update timestamp for {code}")
        dt = datetime.fromisoformat(str(updated).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        updates[code] = dt.astimezone(timezone.utc)
    return updates


def get_nws_state(code):
    row = db_execute(
        "SELECT last_update_at FROM weather_service_state WHERE city_code=%s AND source='nws'",
        (code,),
        fetchone=True,
    )
    return row[0] if row else None


def save_nws_state(updates):
    for code, updated_at in updates.items():
        db_execute(
            """
            INSERT INTO weather_service_state(city_code,source,last_update_at,checked_at)
            VALUES(%s,'nws',%s,NOW())
            ON CONFLICT(city_code) DO UPDATE SET
                source='nws',
                last_update_at=EXCLUDED.last_update_at,
                checked_at=NOW()
            """,
            (code, updated_at),
        )


def detect_nws_updates(city_names):
    """Check whether NWS has published a newer forecast for any monitored city."""
    updates = fetch_nws_updates(city_names)
    changed = {}
    for code, updated_at in updates.items():
        last_update = get_nws_state(code)
        if last_update is None or updated_at > last_update:
            changed[code] = updated_at
    return updates, changed


def location_params(city_names):
    return {
        "latitude": ",".join(str(LOCATION_MAP[x][1]) for x in city_names),
        "longitude": ",".join(str(LOCATION_MAP[x][2]) for x in city_names),
    }


def normalize_locations(data, city_names, label):
    if isinstance(data, list):
        locations = data
    elif isinstance(data, dict):
        locations = [data]
    else:
        raise RuntimeError(f"{label}: unexpected JSON type {type(data).__name__}")
    if len(locations) != len(city_names):
        raise RuntimeError(
            f"{label}: expected {len(city_names)} locations, got {len(locations)}"
        )
    return list(zip(city_names, locations))


def local_date(timestamp, timezone_name):
    dt = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ZoneInfo(timezone_name)).date().isoformat()


def aggregate_hourly(location_data, city_name):
    hourly = location_data.get("hourly") or {}
    timestamps = hourly.get("time") or []
    temps = hourly.get("temperature_2m") or []
    precip = hourly.get("precipitation") or []
    grouped = defaultdict(lambda: {"temps": [], "precip": []})
    tz_name = LOCATION_MAP[city_name][3]
    for i, timestamp in enumerate(timestamps):
        date_key = local_date(timestamp, tz_name)
        if i < len(temps):
            temp = safe_float(temps[i])
            if temp is not None:
                grouped[date_key]["temps"].append(temp)
        if i < len(precip):
            rain = safe_float(precip[i])
            if rain is not None:
                grouped[date_key]["precip"].append(rain)
    result = {}
    for date_key, values in grouped.items():
        if not values["temps"]:
            continue
        result[date_key] = {
            "high": max(values["temps"]),
            "precipitation_sum": sum(values["precip"]),
        }
    return result


def fetch_gfs_deterministic(city_names):
    params = location_params(city_names)
    params.update(
        {
            "models": DETERMINISTIC_MODEL,
            "hourly": "temperature_2m,precipitation",
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "timezone": "UTC",
            "forecast_days": FORECAST_DAYS,
        }
    )
    log.info("Fetching deterministic GFS: %s", DETERMINISTIC_MODEL)
    data = http_json("https://api.open-meteo.com/v1/gfs", params)
    output = {}
    for city, location_data in normalize_locations(data, city_names, "deterministic GFS"):
        output[city] = {
            "daily": aggregate_hourly(location_data, city),
            "model_run": (
                location_data.get("model_run")
                or location_data.get("model_run_id")
                or location_data.get("model_run_time")
            ),
        }
    return output


def fetch_ensemble(city_names):
    params = location_params(city_names)
    params.update(
        {
            "models": ENSEMBLE_MODEL,
            "hourly": "temperature_2m,precipitation",
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "timezone": "UTC",
            "forecast_days": FORECAST_DAYS,
        }
    )
    log.info("Fetching ensemble: %s", ENSEMBLE_MODEL)
    data = http_json("https://ensemble-api.open-meteo.com/v1/ensemble", params)
    output = {}
    for city, location_data in normalize_locations(data, city_names, "ensemble"):
        hourly = location_data.get("hourly") or {}
        timestamps = hourly.get("time") or []
        temp_keys = sorted(k for k in hourly if k.startswith("temperature_2m_member"))
        precip_keys = sorted(k for k in hourly if k.startswith("precipitation_member"))
        if not temp_keys:
            raise RuntimeError(f"ensemble: no temperature ensemble members for {city}")

        day = defaultdict(lambda: {"temp": defaultdict(list), "precip": defaultdict(float)})
        tz_name = LOCATION_MAP[city][3]
        for i, timestamp in enumerate(timestamps):
            date_key = local_date(timestamp, tz_name)
            for key in temp_keys:
                values = hourly.get(key) or []
                if i < len(values):
                    value = safe_float(values[i])
                    if value is not None:
                        day[date_key]["temp"][key].append(value)
            for key in precip_keys:
                values = hourly.get(key) or []
                if i < len(values):
                    value = safe_float(values[i])
                    if value is not None:
                        day[date_key]["precip"][key] += value

        city_daily = {}
        for date_key, values in day.items():
            highs = [max(v) for v in values["temp"].values() if v]
            rain = list(values["precip"].values())
            if highs:
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
                    {**daily, "model_run": data.get("model_run")},
                )
                insert_forecast(
                    city,
                    "precipitation_sum",
                    model,
                    date_key,
                    daily["precipitation_sum"],
                    {**daily, "model_run": data.get("model_run")},
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
            if daily["member_precip_totals"]:
                insert_forecast(
                    city,
                    "ensemble_rain_distribution",
                    ENSEMBLE_MODEL,
                    date_key,
                    statistics.mean(daily["member_precip_totals"]),
                    {"member_precip_totals": daily["member_precip_totals"]},
                )


def temperature_probability(member_highs, market):
    if not member_highs:
        return None
    strike_type = (market.get("strike_type") or "").lower()
    floor = safe_float(market.get("floor_strike"))
    cap = safe_float(market.get("cap_strike"))
    if strike_type == "greater" and floor is not None:
        hits = sum(value > floor for value in member_highs)
    elif strike_type == "less" and cap is not None:
        hits = sum(value < cap for value in member_highs)
    elif strike_type == "between" and floor is not None and cap is not None:
        hits = sum(floor <= value <= cap for value in member_highs)
    else:
        return None
    return 100.0 * hits / len(member_highs)


def rain_probability(member_precip_totals):
    """Probability proxy for KXRAIN: total daily precipitation > 0 inches."""
    if not member_precip_totals:
        return None
    hits = sum(value > 0.0 for value in member_precip_totals)
    return 100.0 * hits / len(member_precip_totals)


def rain_city_from_ticker(market):
    ticker = (market.get("ticker") or "").upper()
    if not ticker.startswith("KXRAIN-"):
        return None
    parts = ticker.split("-")
    if len(parts) < 3:
        return None
    code = parts[-1]
    return code if code in LOCATION_MAP else None


def prior_rain_member_data(city, date_key):
    row = latest_forecast(city, "ensemble_rain_distribution", ENSEMBLE_MODEL, date_key)
    return (row[2] or {}) if row else None


def build_rain_candidate(city, date_key, market, current_probability, previous_probability):
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
        previous_ask = safe_float(previous_market[2] if side == "YES" else previous_market[4])
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
            "market_kind": "precipitation",
            "side": side,
            "entry_price_cents": ask,
            "model_probability_proxy": side_probability,
            "preliminary_edge_points": preliminary_edge,
            "forecast_probability_change_points": side_probability_change,
            "market_price_change_points": market_change,
            "market_lag_points": market_lag,
            "forecast_temperature_change_f": None,
            "contract_label": market.get("title") or "",
        }
        if best is None or (candidate["market_lag_points"], candidate["preliminary_edge_points"]) > (best["market_lag_points"], best["preliminary_edge_points"]):
            best = candidate
    return best


def get_side_ask_cents(market, side):
    field = "yes_ask_dollars" if side == "YES" else "no_ask_dollars"
    value = safe_float(market.get(field))
    return None if value is None else value * 100.0


def location_signal_allowed(location):
    return bool(location[4]) or ALLOW_UNVERIFIED_LOCATION_SIGNALS


def prior_member_data(city, date_key, variable):
    row = latest_forecast(city, variable, ENSEMBLE_MODEL, date_key)
    return (row[2] or {}) if row else None


def build_candidate(city, date_key, market, current_probability, previous_probability):
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
            "market_kind": "temperature",
            "side": side,
            "entry_price_cents": ask,
            "model_probability_proxy": side_probability,
            "preliminary_edge_points": preliminary_edge,
            "forecast_probability_change_points": side_probability_change,
            "market_price_change_points": market_change,
            "market_lag_points": market_lag,
            "forecast_temperature_change_f": None,
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
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def open_paper_trade(signal, reason):
    fp = signal_fingerprint(signal)
    existing = db_execute(
        "SELECT 1 FROM paper_trades WHERE signal_fingerprint=%s",
        (fp,),
        fetchone=True,
    )
    if existing:
        return fp, False

    entry = signal["entry_price_cents"] / 100.0
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
    return fp, True


def send_discord(message):
    if not DISCORD_RELAY_URL or not DISCORD_RELAY_SECRET:
        log.error("Discord relay is not configured")
        return False
    try:
        response = requests.post(
            DISCORD_RELAY_URL,
            json={"secret": DISCORD_RELAY_SECRET, "message": message},
            headers={"User-Agent": "WeatherKalshiResearchBot/5.0", "Accept": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        log.info("Discord relay response: %s", response.status_code)
        if not 200 <= response.status_code < 300:
            log.error("Discord relay error: %s", response.text[:1000])
            return False
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if payload.get("success") is False:
            log.error("Discord relay reported failure: %s", payload)
            return False
        return True
    except Exception as exc:
        log.error("Discord relay exception: %s", exc)
        return False


def signal_message(signal):
    kind_label = "PRECIPITATION" if signal.get("market_kind") == "precipitation" else "TEMPERATURE"
    return (
        f"🌦️ **{kind_label} FORECAST SHOCK — PAPER TRADE**\n\n"
        f"**{signal['city']} — {signal['forecast_date']}**\n"
        f"Market: `{signal['market_ticker']}`\n"
        f"Side: **{signal['side']}**\n"
        f"Entry ask: **{signal['entry_price_cents']:.1f}¢**\n\n"
        f"Ensemble probability proxy: **{signal['model_probability_proxy']:.1f}%**\n"
        f"Forecast probability change: **{signal['forecast_probability_change_points']:+.1f} pts**\n"
        f"Market ask change: **{signal['market_price_change_points']:+.1f} pts**\n"
        f"Estimated market lag: **{signal['market_lag_points']:+.1f} pts**\n"
        f"Preliminary edge: **{signal['preliminary_edge_points']:+.1f} pts**\n\n"
        f"Paper risk: **${PAPER_RISK_DOLLARS:.2f}**\n\n"
        "⚠️ Research only. The ensemble value is an uncalibrated frequency proxy. "
        "For precipitation, Kalshi settles using the official Weather Company data, so Open-Meteo is a forecast proxy, not the settlement feed."
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
    settled = 0
    for trade_id, ticker, side, stake, contracts in rows:
        try:
            data = http_json(f"{KALSHI_API_URL}/markets/{ticker}")
            market = data.get("market", {}) if isinstance(data, dict) else {}
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
            settled += 1
            log.info("PAPER TRADE SETTLED | %s | %s | result=%s | P/L=$%.2f", ticker, side, result, pnl)
        except Exception as exc:
            log.warning("Could not settle %s: %s", ticker, exc)
    return settled


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
        (status, Json(stats), error, scan_id),
    )


def get_series_list():
    items = []
    cursor = None
    while True:
        params = {"category": "Climate and Weather", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        data = http_json(f"{KALSHI_API_URL}/series", params)
        items.extend(data.get("series", []))
        cursor = data.get("cursor")
        if not cursor:
            return items


def save_series_registry(series_list):
    # Save all discovered series in one database transaction. The previous
    # implementation opened a fresh PostgreSQL connection for every series,
    # which made a 300+ series response unnecessarily slow.
    rows = []
    for series in series_list:
        ticker = series.get("ticker")
        if not ticker:
            continue
        rows.append(
            (
                ticker,
                series.get("title", ""),
                series.get("category"),
                Json(series.get("tags") or []),
                Json(series.get("settlement_sources") or []),
                series.get("contract_terms_url"),
                Json(series),
            )
        )
    if not rows:
        return
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            sql = """
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
            """
            cur.executemany(sql, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_series_markets(series_ticker):
    items = []
    cursor = None
    while True:
        params = {"series_ticker": series_ticker, "status": "open", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        data = http_json(f"{KALSHI_API_URL}/markets", params)
        items.extend(data.get("markets", []))
        cursor = data.get("cursor")
        if not cursor:
            return items


def select_relevant_temperature_series(all_series):
    # There can be duplicate/legacy series for the same city. For signal
    # generation and market snapshots, keep one canonical series per city,
    # preferring the current KXHIGH* ticker family.
    candidates = defaultdict(list)
    for series in all_series:
        if not temperature_series(series):
            continue
        location = location_from_title_or_ticker(series)
        if location is None:
            continue
        city = location[0]
        candidates[city].append(series)

    result = []
    for city, series_list in sorted(candidates.items()):
        def rank(series):
            ticker = (series.get("ticker") or "").upper()
            return (0 if ticker.startswith("KXHIGH") else 1, ticker)
        chosen = sorted(series_list, key=rank)[0]
        result.append(chosen)
        if len(series_list) > 1:
            discarded = [s.get("ticker") for s in series_list if s is not chosen]
            log.info(
                "Using canonical temperature series for %s: %s | ignoring duplicates: %s",
                city, chosen.get("ticker"), ", ".join(x or "?" for x in discarded),
            )
    return result


def insert_market_snapshot(market, city, kind):
    def cents(value):
        value = safe_float(value)
        return None if value is None else value * 100.0

    db_execute(
        """
        INSERT INTO market_snapshots(
            observed_at,ticker,event_ticker,series_ticker,market_date,
            city,market_kind,strike_type,floor_strike,cap_strike,
            yes_bid_cents,yes_ask_cents,no_bid_cents,no_ask_cents,
            last_price_cents,status,result
        ) VALUES(NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            cents(market.get("yes_bid_dollars")),
            cents(market.get("yes_ask_dollars")),
            cents(market.get("no_bid_dollars")),
            cents(market.get("no_ask_dollars")),
            cents(market.get("last_price_dollars")),
            market.get("status"),
            market.get("result"),
        ),
    )


def research_event_fingerprint(city, date_key, variable, ticker, side, forecast_signature):
    raw = "|".join([city, date_key, variable, ticker, side, forecast_signature])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def record_research_event(city, date_key, variable, market, side, previous_probability, current_probability, previous_market_ask, current_market_ask, forecast_signature, stats):
    if current_probability is None or previous_probability is None:
        return
    forecast_change = current_probability - previous_probability
    side_forecast_change = forecast_change if side == "YES" else -forecast_change
    if abs(side_forecast_change) < RESEARCH_MIN_FORECAST_CHANGE_POINTS:
        return
    if previous_market_ask is None or current_market_ask is None:
        return

    ticker = market.get("ticker") or ""
    side_probability = current_probability if side == "YES" else 100.0 - current_probability
    initial_market_change = current_market_ask - previous_market_ask
    initial_lag = side_forecast_change - initial_market_change
    initial_edge = side_probability - current_market_ask
    status = "open"
    fp = research_event_fingerprint(city, date_key, variable, ticker, side, forecast_signature)

    row = db_execute(
        """
        INSERT INTO forecast_research_events(
            event_fingerprint,created_at,city,forecast_date,variable,market_ticker,side,
            previous_probability,current_probability,forecast_probability_change_points,
            pre_forecast_ask_cents,event_ask_cents,initial_market_change_points,
            initial_market_lag_points,initial_preliminary_edge_points,status
        ) VALUES(%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(event_fingerprint) DO NOTHING
        RETURNING id
        """,
        (
            fp, city, date_key, variable, ticker, side,
            previous_probability if side == "YES" else 100.0 - previous_probability,
            side_probability, side_forecast_change, previous_market_ask, current_market_ask,
            initial_market_change, initial_lag, initial_edge, status,
        ),
        fetchone=True,
    )
    if row:
        stats["research_events_created"] += 1
        log.info(
            "RESEARCH EVENT | %s | %s | %s | %s | prob %.1f -> %.1f | market %.1f -> %.1f | lag %.1f pts | edge %.1f pts",
            variable, city, date_key, ticker,
            previous_probability if side == "YES" else 100.0 - previous_probability,
            side_probability, previous_market_ask, current_market_ask, initial_lag, initial_edge,
        )


def record_research_temperature(temp_series, ensemble, stats):
    for series in temp_series:
        location = location_from_title_or_ticker(series)
        if location is None:
            continue
        city, _, _, tz_name, _ = location
        code = next(code for code, entry in LOCATION_MAP.items() if entry == location)
        markets = get_series_markets(series.get("ticker"))
        for market in markets:
            stats["research_markets_considered"] += 1
            date_key = parse_market_date(market)
            if not date_key or date_key < datetime.now(ZoneInfo(tz_name)).date().isoformat():
                continue
            daily = ensemble.get(code, {}).get("daily", {}).get(date_key)
            previous = prior_member_data(code, date_key, "ensemble_temperature_distribution")
            if not daily:
                continue
            if not previous:
                stats["research_missing_previous_forecast"] += 1
                continue
            current_members = daily.get("member_highs") or []
            previous_members = previous.get("member_highs") or []
            current_prob = temperature_probability(current_members, market)
            previous_prob = temperature_probability(previous_members, market)
            if current_prob is None or previous_prob is None:
                continue
            stats["research_current_forecasts"] += 1
            stats["research_previous_forecasts"] += 1
            prev_market = latest_market(market.get("ticker") or "")
            for side in ("YES", "NO"):
                current_ask = get_side_ask_cents(market, side)
                previous_ask = None if not prev_market else safe_float(prev_market[2] if side == "YES" else prev_market[4])
                record_research_event(
                    city, date_key, "temperature", market, side,
                    previous_prob, current_prob, previous_ask, current_ask,
                    payload_hash({"member_highs": current_members}), stats,
                )


def record_research_rain(city_names, ensemble, stats):
    for market in get_series_markets("KXRAIN"):
        code = rain_city_from_ticker(market)
        if code not in city_names:
            continue
        city, _, _, tz_name, _ = LOCATION_MAP[code]
        date_key = parse_market_date(market)
        if not date_key or date_key < datetime.now(ZoneInfo(tz_name)).date().isoformat():
            continue
        stats["research_markets_considered"] += 1
        daily = ensemble.get(code, {}).get("daily", {}).get(date_key)
        previous = prior_rain_member_data(code, date_key)
        if not daily:
            continue
        if not previous:
            stats["research_missing_previous_forecast"] += 1
            continue
        current_members = daily.get("member_precip_totals") or []
        previous_members = previous.get("member_precip_totals") or []
        current_prob = rain_probability(current_members)
        previous_prob = rain_probability(previous_members)
        if current_prob is None or previous_prob is None:
            continue
        stats["research_current_forecasts"] += 1
        stats["research_previous_forecasts"] += 1
        prev_market = latest_market(market.get("ticker") or "")
        for side in ("YES", "NO"):
            current_ask = get_side_ask_cents(market, side)
            previous_ask = None if not prev_market else safe_float(prev_market[2] if side == "YES" else prev_market[4])
            record_research_event(
                city, date_key, "precipitation", market, side,
                previous_prob, current_prob, previous_ask, current_ask,
                payload_hash({"member_precip_totals": current_members}), stats,
            )


def research_observe_market_progress(stats, scan_started_at):
    rows = db_execute(
        """
        SELECT id,city,forecast_date,variable,market_ticker,side,created_at,event_ask_cents,
               initial_market_lag_points,first_response_at,milestone_25_at,milestone_50_at,
               milestone_75_at,milestone_90_at,max_market_move_points,latest_observation_at
        FROM forecast_research_events
        WHERE status='open'
          AND created_at < %s
        ORDER BY created_at ASC
        LIMIT 1000
        """,
        (scan_started_at,),
        fetch=True,
    ) or []
    for row in rows:
        (event_id, city, forecast_date, variable, ticker, side, created_at, event_ask,
         initial_lag, first_response_at, m25, m50, m75, m90, max_move, latest_observed_at) = row
        if initial_lag is None or event_ask is None:
            continue
        after = latest_observed_at or created_at
        snapshot = db_execute(
            """
            SELECT observed_at,
                   CASE WHEN %s='YES' THEN yes_ask_cents ELSE no_ask_cents END
            FROM market_snapshots
            WHERE ticker=%s AND observed_at>%s AND observed_at<%s
            ORDER BY observed_at ASC
            LIMIT 1
            """,
            (side, ticker, after, scan_started_at), fetchone=True,
        )
        if not snapshot or snapshot[1] is None:
            continue
        observed_at, current_ask = snapshot
        market_move = current_ask - event_ask
        # Measure response in the forecast-change direction. Initial lag may be
        # zero/negative; such events remain in the research table but do not
        # get milestone timing until a positive initial gap exists.
        response_fraction = (market_move / initial_lag) if initial_lag > 0 else 0.0
        lag_remaining = initial_lag - market_move
        max_move_new = max(max_move or 0.0, market_move)
        db_execute(
            """
            INSERT INTO forecast_research_updates(
                event_id,observed_at,market_ask_cents,market_move_points,
                lag_remaining_points,market_response_fraction
            ) VALUES(%s,%s,%s,%s,%s,%s)
            """,
            (event_id, observed_at, current_ask, market_move, lag_remaining, response_fraction),
        )
        updates = {
            "latest_observation_at": observed_at,
            "latest_ask_cents": current_ask,
            "latest_market_move_points": market_move,
            "latest_lag_remaining_points": lag_remaining,
            "max_market_move_points": max_move_new,
        }
        # First response is directional: a YES event responds when YES ask rises;
        # a NO event responds when NO ask rises.
        if first_response_at is None and market_move > 0:
            updates["first_response_at"] = observed_at
        if initial_lag > 0:
            for fraction, column, existing in (
                (0.25, "milestone_25_at", m25),
                (0.50, "milestone_50_at", m50),
                (0.75, "milestone_75_at", m75),
                (0.90, "milestone_90_at", m90),
            ):
                if existing is None and market_move >= initial_lag * fraction:
                    updates[column] = observed_at
                    if fraction == 0.90:
                        log.info(
                            "RESEARCH ADAPTED 90%% | %s | %s | %s | %.1fs",
                            variable, city, ticker, (observed_at-created_at).total_seconds()
                        )
        assignments = [f"{k}=%s" for k in updates]
        db_execute(
            f"UPDATE forecast_research_events SET {', '.join(assignments)} WHERE id=%s",
            tuple(list(updates.values()) + [event_id]),
        )
        stats["research_events_observed"] += 1


def process_temperature_signals(temp_series, ensemble, stats):
    for series in temp_series:
        location = location_from_title_or_ticker(series)
        if location is None:
            continue
        city, _, _, tz_name, _ = location
        code = next(code for code, entry in LOCATION_MAP.items() if entry == location)
        if not location_signal_allowed(location):
            continue

        markets = get_series_markets(series.get("ticker"))
        for market in markets:
            date_key = parse_market_date(market)
            if not date_key:
                continue
            today = datetime.now(ZoneInfo(tz_name)).date().isoformat()
            if date_key < today:
                continue

            current_daily = ensemble.get(code, {}).get("daily", {}).get(date_key)
            if not current_daily:
                continue
            current_members = current_daily.get("member_highs") or []
            previous = prior_member_data(code, date_key, "ensemble_temperature_distribution")
            previous_members = (previous or {}).get("member_highs") or []
            if not previous_members:
                continue

            current_prob = temperature_probability(current_members, market)
            previous_prob = temperature_probability(previous_members, market)
            signal = build_candidate(city, date_key, market, current_prob, previous_prob)
            if not signal:
                continue

            stats["forecast_shocks"] += 1
            reason = {
                "contract": market.get("title") or market.get("ticker"),
                "ensemble_model": ENSEMBLE_MODEL,
                "current_member_count": len(current_members),
                "previous_member_count": len(previous_members),
                "settlement_source_note": "Verify active Kalshi market rules before treating coordinates as settlement-equivalent.",
            }
            fp, created = open_paper_trade(signal, reason)
            if not created:
                continue
            stats["paper_trades_created"] += 1

            if not db_execute(
                "SELECT 1 FROM alert_log WHERE fingerprint=%s", (fp,), fetchone=True
            ):
                if send_discord(signal_message(signal)):
                    record_alert(fp, signal)
                    stats["discord_alerts"] += 1
                else:
                    log.error("Paper trade %s created but Discord alert failed", fp)

            log.info(
                "FORECAST SHOCK | %s | %s | %s | prob %.1f -> %.1f | market lag %.1f pts | edge %.1f pts",
                city,
                date_key,
                market.get("ticker"),
                previous_prob,
                current_prob,
                signal["market_lag_points"],
                signal["preliminary_edge_points"],
            )


def process_rain_signals(city_names, ensemble, stats):
    """Evaluate KXRAIN daily >0.00 inch outcomes for forecast shocks."""
    markets_by_city = defaultdict(list)
    for market in get_series_markets("KXRAIN"):
        code = rain_city_from_ticker(market)
        if code in city_names:
            markets_by_city[code].append(market)

    for code in city_names:
        if code not in RAIN_SIGNAL_CODES:
            continue
        location = LOCATION_MAP[code]
        city, _, _, tz_name, _ = location
        for market in markets_by_city.get(code, []):
            date_key = parse_market_date(market)
            if not date_key:
                continue
            if date_key < datetime.now(ZoneInfo(tz_name)).date().isoformat():
                continue

            current_daily = ensemble.get(code, {}).get("daily", {}).get(date_key)
            if not current_daily:
                continue
            current_members = current_daily.get("member_precip_totals") or []
            previous = prior_rain_member_data(code, date_key)
            previous_members = (previous or {}).get("member_precip_totals") or []
            if not previous_members:
                continue

            current_prob = rain_probability(current_members)
            previous_prob = rain_probability(previous_members)
            signal = build_rain_candidate(city, date_key, market, current_prob, previous_prob)
            if not signal:
                continue

            stats["forecast_shocks"] += 1
            stats["rain_forecast_shocks"] += 1
            reason = {
                "contract": market.get("title") or market.get("ticker"),
                "market_kind": "precipitation",
                "rain_rule": "YES if total daily precipitation is strictly greater than 0 inches; trace and missing daily values count as 0.",
                "ensemble_model": ENSEMBLE_MODEL,
                "current_member_count": len(current_members),
                "previous_member_count": len(previous_members),
                "current_precip_mean_inches": statistics.mean(current_members),
                "previous_precip_mean_inches": statistics.mean(previous_members),
                "settlement_source_note": "Kalshi weather rules use the official Weather Company source; Open-Meteo is a forecast proxy and is not settlement-equivalent.",
            }
            fp, created = open_paper_trade(signal, reason)
            if not created:
                continue
            stats["paper_trades_created"] += 1

            if not db_execute("SELECT 1 FROM alert_log WHERE fingerprint=%s", (fp,), fetchone=True):
                if send_discord(signal_message(signal)):
                    record_alert(fp, signal)
                    stats["discord_alerts"] += 1
                else:
                    log.error("Paper trade %s created but Discord alert failed", fp)

            log.info(
                "RAIN FORECAST SHOCK | %s | %s | %s | prob %.1f -> %.1f | market lag %.1f pts | edge %.1f pts",
                city, date_key, market.get("ticker"), previous_prob, current_prob,
                signal["market_lag_points"], signal["preliminary_edge_points"],
            )


def collect_market_snapshots(temp_series, stats, city_names):
    for series in temp_series:
        location = location_from_title_or_ticker(series)
        if location is None:
            continue
        city = location[0]
        markets = get_series_markets(series.get("ticker"))
        stats["temperature_markets"] += len(markets)
        for market in markets:
            insert_market_snapshot(market, city, "temperature")

    for market in get_series_markets("KXRAIN"):
        code = rain_city_from_ticker(market)
        if not code or code not in city_names:
            continue
        city = LOCATION_MAP[code][0]
        stats["rain_markets"] += 1
        insert_market_snapshot(market, city, "precipitation")


def run_scan():
    scan_id = None
    stats = {
        "temperature_series": 0,
        "temperature_markets": 0,
        "weather_refreshed": False,
        "forecast_shocks": 0,
        "paper_trades_created": 0,
        "discord_alerts": 0,
        "settled_trades": 0,
        "deterministic_gfs_ok": False,
        "ensemble_ok": False,
        "rain_signals_enabled": True,
        "rain_markets": 0,
        "rain_forecast_shocks": 0,
        "research_events_created": 0,
        "research_events_observed": 0,
        "research_markets_considered": 0,
        "research_current_forecasts": 0,
        "research_previous_forecasts": 0,
        "research_missing_previous_forecast": 0,
        "nws_updates_detected": 0,
    }
    started = time.time()
    scan_started_at = utc_now()

    log.info("=" * 50)
    log.info("STARTING WEATHER MARKET SCAN")
    log.info("UTC: %s", utc_now().isoformat())
    log.info("ENSEMBLE_MODEL: %s", ENSEMBLE_MODEL)
    log.info("DETERMINISTIC_MODEL: %s", DETERMINISTIC_MODEL)
    log.info("=" * 50)

    try:
        ensure_schema()
        scan_id = start_scan_run()
        stats["settled_trades"] = settle_paper_trades()

        all_series = get_series_list()
        registry_started = time.time()
        save_series_registry(all_series)
        log.info("Series registry saved: %d series in %.1fs", len(all_series), time.time() - registry_started)
        temp_series = select_relevant_temperature_series(all_series)
        stats["temperature_series"] = len(temp_series)
        log.info("Relevant temperature series: %d", len(temp_series))

        city_names = sorted({location_from_title_or_ticker(s)[0] for s in temp_series if location_from_title_or_ticker(s)})
        if not city_names:
            raise RuntimeError("No monitored temperature market locations were discovered")
        # Convert display names back to LOCATION_MAP keys.
        city_names = sorted({next(code for code, entry in LOCATION_MAP.items() if entry[0] == name) for name in city_names})
        log.info("Forecast cities: %s", ", ".join(city_names))

        try:
            nws_updates, changed_updates = detect_nws_updates(city_names)
            stats["nws_updates_detected"] = len(changed_updates)
            for code, updated_at in nws_updates.items():
                log.info("NWS %s forecast update: %s%s", code, updated_at.isoformat(), " (NEW)" if code in changed_updates else "")
        except Exception as exc:
            raise RuntimeError(f"NWS update check failed: {exc}") from exc

        refresh_weather = bool(changed_updates)
        log.info("New NWS forecast detected: %s", refresh_weather)

        if refresh_weather:
            stats["weather_refreshed"] = True

            deterministic = {}
            try:
                deterministic[DETERMINISTIC_MODEL] = fetch_gfs_deterministic(city_names)
                stats["deterministic_gfs_ok"] = True
                log.info("Deterministic GFS fetched successfully")
            except Exception as exc:
                log.warning("Deterministic GFS unavailable; continuing with ensemble: %s", exc)

            ensemble = fetch_ensemble(city_names)
            stats["ensemble_ok"] = True

            process_temperature_signals(temp_series, ensemble, stats)
            process_rain_signals(city_names, ensemble, stats)
            record_research_temperature(temp_series, ensemble, stats)
            record_research_rain(city_names, ensemble, stats)

            save_weather_forecasts(deterministic, ensemble)
            save_nws_state(nws_updates)
        else:
            ensemble = {}

        # Market snapshots happen every scan, including scans without a new weather run.
        collect_market_snapshots(temp_series, stats, city_names)
        research_observe_market_progress(stats, scan_started_at)
        stats["settled_trades"] += settle_paper_trades()
        finish_scan_run(scan_id, "success", stats)
        log.info(
            "RESEARCH | markets=%d | current_forecasts=%d | previous_forecasts=%d | events_created=%d | events_observed=%d | missing_previous=%d",
            stats["research_markets_considered"],
            stats["research_current_forecasts"],
            stats["research_previous_forecasts"],
            stats["research_events_created"],
            stats["research_events_observed"],
            stats["research_missing_previous_forecast"],
        )
        log.info("SCAN COMPLETE | %s | runtime=%.1fs", json.dumps(stats, default=str), time.time() - started)
    except Exception as exc:
        log.exception("SCAN FAILED")
        if scan_id is not None:
            try:
                finish_scan_run(scan_id, "failed", stats, str(exc))
            except Exception:
                log.exception("Could not record scan failure")
        raise
    finally:
        db_close()


if __name__ == "__main__":
    run_scan()
