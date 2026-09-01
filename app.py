import os
from flask import Flask, jsonify

try:
    import psycopg2
except ImportError:
    psycopg2 = None

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
DISCORD_RELAY_URL = os.environ.get("DISCORD_RELAY_URL", "").strip()
DISCORD_RELAY_SECRET = os.environ.get("DISCORD_RELAY_SECRET", "").strip()


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured.")
    if psycopg2 is None:
        raise RuntimeError("psycopg2-binary is not installed.")
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


@app.get("/")
def home():
    return (
        "Weather + Kalshi research bot is running. "
        "The scanner runs separately as a Render Cron Job."
    )


@app.get("/health")
def health():
    database_connected = False
    database_error = None
    if DATABASE_URL and psycopg2 is not None:
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                database_connected = cur.fetchone()[0] == 1
            conn.close()
        except Exception as exc:
            database_error = str(exc)

    return jsonify({
        "status": "ok" if database_connected else "degraded",
        "database_configured": bool(DATABASE_URL),
        "database_connected": database_connected,
        "database_error": database_error,
        "discord_relay_configured": bool(
            DISCORD_RELAY_URL and DISCORD_RELAY_SECRET
        ),
        "scanner_architecture": "Render Cron Job -> scanner.py",
        "paper_trading_mode": True,
    })


@app.get("/status")
def status():
    result = {"paper_trading_mode": True}
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT started_at, completed_at, status, stats
                FROM scan_runs
                ORDER BY started_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                result["last_scan"] = {
                    "started_at": row[0],
                    "completed_at": row[1],
                    "status": row[2],
                    "stats": row[3],
                }
            cur.execute("SELECT COUNT(*) FROM forecast_observations")
            result["forecast_observations"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM market_snapshots")
            result["market_snapshots"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM paper_trades")
            result["paper_trades"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM paper_trades WHERE status='open'")
            result["open_paper_trades"] = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*), COALESCE(SUM(profit_loss_dollars), 0)
                FROM paper_trades
                WHERE status='settled'
            """)
            count, pnl = cur.fetchone()
            result["settled_paper_trades"] = count
            result["settled_paper_pnl_dollars"] = float(pnl or 0)
        conn.close()
        result["database"] = "ok"
    except Exception as exc:
        result["database"] = "error"
        result["database_error"] = str(exc)
    return jsonify(result)


@app.get("/paper-trades")
def paper_trades():
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, created_at, settled_at, city, forecast_date,
                       market_ticker, market_kind, side, entry_price_cents,
                       stake_dollars, contracts, model_probability_proxy,
                       preliminary_edge_points, forecast_probability_change_points,
                       market_price_change_points, market_lag_points,
                       forecast_temperature_change_f, result,
                       profit_loss_dollars, status
                FROM paper_trades
                ORDER BY created_at DESC
                LIMIT 200
            """)
            rows = cur.fetchall()
        conn.close()
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
    port = int(os.environ.get("PORT", "10000"))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
    )
