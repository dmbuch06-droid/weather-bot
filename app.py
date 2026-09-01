import os
from datetime import datetime, timezone

try:
    import psycopg2
except ImportError:
    psycopg2 = None

from flask import Flask, jsonify

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
PAPER_TRADING_MODE = True


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured.")
    if psycopg2 is None:
        raise RuntimeError("psycopg2-binary is not installed.")
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,
    )


def ensure_schema():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
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
            )


@app.route("/")
def home():
    return (
        "Weather + Kalshi research bot is running. "
        "Use /health, /status, or /paper-trades."
    )


@app.route("/health")
def health():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                db_ok = cur.fetchone() == (1,)
        error = None
    except Exception as exc:
        db_ok = False
        error = str(exc)

    return jsonify(
        {
            "status": "ok" if db_ok else "degraded",
            "database_connected": db_ok,
            "database_error": error,
            "paper_trading_mode": PAPER_TRADING_MODE,
            "utc": datetime.now(timezone.utc).isoformat(),
        }
    )


@app.route("/status")
def status():
    result = {
        "paper_trading_mode": PAPER_TRADING_MODE,
    }

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM bot_runs),
                        (SELECT COUNT(*) FROM forecast_observations),
                        (SELECT COUNT(*) FROM market_snapshots),
                        (SELECT COUNT(*) FROM paper_trades),
                        (SELECT COUNT(*) FROM paper_trades WHERE status='open'),
                        (SELECT COALESCE(SUM(profit_loss_dollars), 0)
                         FROM paper_trades
                         WHERE status='settled'),
                        (SELECT MAX(finished_at) FROM bot_runs),
                        (SELECT MAX(finished_at)
                         FROM bot_runs
                         WHERE success IS TRUE)
                    """
                )

                (
                    result["scan_count"],
                    result["forecast_observations"],
                    result["market_snapshots"],
                    result["paper_trades"],
                    result["open_paper_trades"],
                    result["settled_paper_pnl_dollars"],
                    result["last_scan_finished_utc"],
                    result["last_successful_scan_finished_utc"],
                ) = cur.fetchone()

    except Exception as exc:
        result["database_error"] = str(exc)

    return jsonify(result)


@app.route("/paper-trades")
def paper_trades():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        created_at,
                        settled_at,
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
                        result,
                        profit_loss_dollars,
                        status
                    FROM paper_trades
                    ORDER BY created_at DESC
                    LIMIT 200
                    """
                )
                rows = cur.fetchall()

        keys = [
            "id",
            "created_at",
            "settled_at",
            "city",
            "forecast_date",
            "ticker",
            "market_kind",
            "side",
            "entry_price_cents",
            "risk_dollars",
            "contracts",
            "model_probability_proxy",
            "preliminary_edge_points",
            "forecast_change_points",
            "market_change_points",
            "market_lag_points",
            "forecast_temperature_change_f",
            "result",
            "profit_loss_dollars",
            "status",
        ]

        return jsonify(
            [dict(zip(keys, row)) for row in rows]
        )

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    try:
        ensure_schema()
    except Exception as exc:
        print(
            f"Database schema initialization warning: {exc}",
            flush=True,
        )

    port = int(
        os.environ.get("PORT", "10000")
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
    )
