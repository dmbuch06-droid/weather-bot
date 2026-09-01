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
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


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
        db_error = None
    except Exception as exc:
        db_ok = False
        db_error = str(exc)

    return jsonify({
        "status": "ok" if db_ok else "degraded",
        "database_connected": db_ok,
        "database_error": db_error,
        "paper_trading_mode": PAPER_TRADING_MODE,
        "utc": datetime.now(timezone.utc).isoformat(),
    })


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
                        (SELECT COUNT(*) FROM scan_runs),
                        (SELECT COUNT(*) FROM forecast_observations),
                        (SELECT COUNT(*) FROM market_snapshots),
                        (SELECT COUNT(*) FROM paper_trades),
                        (SELECT COUNT(*) FROM paper_trades WHERE status='open'),
                        (SELECT COALESCE(SUM(profit_loss_dollars), 0)
                         FROM paper_trades WHERE status='settled'),
                        (SELECT MAX(completed_at) FROM scan_runs),
                        (SELECT MAX(completed_at)
                         FROM scan_runs WHERE status='success')
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
            "id", "created_at", "settled_at", "city", "forecast_date",
            "market_ticker", "market_kind", "side", "entry_price_cents",
            "stake_dollars", "contracts", "model_probability_proxy",
            "preliminary_edge_points", "forecast_probability_change_points",
            "market_price_change_points", "market_lag_points",
            "forecast_temperature_change_f", "result",
            "profit_loss_dollars", "status",
        ]
        return jsonify([dict(zip(keys, row)) for row in rows])
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    # Web service only. The scanner is scheduled by GitHub Actions.
    port = int(os.environ.get("PORT", "10000"))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
    )
