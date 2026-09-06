import csv
import io
import os
from datetime import datetime, timezone

try:
    import psycopg2
except ImportError:
    psycopg2 = None

from flask import Flask, jsonify, Response, render_template_string

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
PAPER_TRADING_MODE = True


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured.")
    if psycopg2 is None:
        raise RuntimeError("psycopg2-binary is not installed.")
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


def fetch_one(sql, params=()):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def fetch_all(sql, params=()):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def safe_number(value, digits=2):
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def iso_value(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


def build_research_report():
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "summary": {},
        "by_city": [],
        "by_variable": [],
        "response_buckets": [],
        "recent_events": [],
        "paper_trading": {},
        "scan_health": {},
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE status='open'),
                    COUNT(*) FILTER (WHERE status<>'open'),
                    AVG(initial_market_lag_points)
                        FILTER (WHERE initial_market_lag_points IS NOT NULL),
                    AVG(initial_preliminary_edge_points)
                        FILTER (WHERE initial_preliminary_edge_points IS NOT NULL),
                    AVG(
                        EXTRACT(EPOCH FROM (first_response_at - created_at))
                    ) FILTER (WHERE first_response_at IS NOT NULL),
                    AVG(
                        EXTRACT(EPOCH FROM (milestone_25_at - created_at))
                    ) FILTER (WHERE milestone_25_at IS NOT NULL),
                    AVG(
                        EXTRACT(EPOCH FROM (milestone_50_at - created_at))
                    ) FILTER (WHERE milestone_50_at IS NOT NULL),
                    AVG(
                        EXTRACT(EPOCH FROM (milestone_75_at - created_at))
                    ) FILTER (WHERE milestone_75_at IS NOT NULL),
                    AVG(
                        EXTRACT(EPOCH FROM (milestone_90_at - created_at))
                    ) FILTER (WHERE milestone_90_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE first_response_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE milestone_25_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE milestone_50_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE milestone_75_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE milestone_90_at IS NOT NULL),
                    AVG(max_market_move_points)
                        FILTER (WHERE max_market_move_points IS NOT NULL)
                FROM forecast_research_events
                """
            )
            (
                total_events,
                open_events,
                closed_events,
                avg_initial_lag,
                avg_initial_edge,
                avg_first_response_seconds,
                avg_25_seconds,
                avg_50_seconds,
                avg_75_seconds,
                avg_90_seconds,
                first_response_count,
                milestone_25_count,
                milestone_50_count,
                milestone_75_count,
                milestone_90_count,
                avg_max_move,
            ) = cur.fetchone()

            cur.execute(
                """
                SELECT
                    city,
                    COUNT(*) AS events,
                    COUNT(*) FILTER (WHERE first_response_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE milestone_50_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE milestone_90_at IS NOT NULL),
                    AVG(initial_market_lag_points),
                    AVG(initial_preliminary_edge_points),
                    AVG(max_market_move_points),
                    AVG(
                        EXTRACT(EPOCH FROM (first_response_at - created_at))
                    ) FILTER (WHERE first_response_at IS NOT NULL)
                FROM forecast_research_events
                GROUP BY city
                ORDER BY events DESC, city
                """
            )
            city_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    variable,
                    COUNT(*) AS events,
                    COUNT(*) FILTER (WHERE first_response_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE milestone_50_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE milestone_90_at IS NOT NULL),
                    AVG(initial_market_lag_points),
                    AVG(initial_preliminary_edge_points),
                    AVG(max_market_move_points),
                    AVG(
                        EXTRACT(EPOCH FROM (first_response_at - created_at))
                    ) FILTER (WHERE first_response_at IS NOT NULL)
                FROM forecast_research_events
                GROUP BY variable
                ORDER BY events DESC, variable
                """
            )
            variable_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    CASE
                        WHEN ABS(forecast_probability_change_points) < 5 THEN '<5'
                        WHEN ABS(forecast_probability_change_points) < 10 THEN '5-10'
                        WHEN ABS(forecast_probability_change_points) < 20 THEN '10-20'
                        WHEN ABS(forecast_probability_change_points) < 30 THEN '20-30'
                        ELSE '30+'
                    END AS bucket,
                    COUNT(*),
                    AVG(initial_market_lag_points),
                    AVG(initial_preliminary_edge_points),
                    COUNT(*) FILTER (WHERE first_response_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE milestone_50_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE milestone_90_at IS NOT NULL)
                FROM forecast_research_events
                GROUP BY 1
                ORDER BY MIN(ABS(forecast_probability_change_points))
                """
            )
            bucket_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    id,
                    created_at,
                    city,
                    forecast_date,
                    variable,
                    market_ticker,
                    side,
                    previous_probability,
                    current_probability,
                    forecast_probability_change_points,
                    pre_forecast_ask_cents,
                    event_ask_cents,
                    initial_market_change_points,
                    initial_market_lag_points,
                    initial_preliminary_edge_points,
                    first_response_at,
                    milestone_25_at,
                    milestone_50_at,
                    milestone_75_at,
                    milestone_90_at,
                    max_market_move_points,
                    latest_lag_remaining_points,
                    status,
                    settlement_result
                FROM forecast_research_events
                ORDER BY created_at DESC
                LIMIT 100
                """
            )
            event_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE status='open'),
                    COUNT(*) FILTER (WHERE status='settled'),
                    COUNT(*) FILTER (WHERE status='settled' AND result='yes'
                                      AND side='YES'),
                    COUNT(*) FILTER (WHERE status='settled' AND result='no'
                                      AND side='NO'),
                    COALESCE(SUM(profit_loss_dollars), 0),
                    COALESCE(SUM(profit_loss_dollars)
                             FILTER (WHERE status='settled'), 0),
                    AVG(profit_loss_dollars)
                        FILTER (WHERE status='settled'),
                    AVG(model_probability_proxy)
                        FILTER (WHERE status='settled'),
                    AVG(preliminary_edge_points)
                        FILTER (WHERE status='settled'),
                    AVG(market_lag_points)
                        FILTER (WHERE status='settled')
                FROM paper_trades
                """
            )
            (
                paper_total,
                paper_open,
                paper_settled,
                paper_yes_wins,
                paper_no_wins,
                paper_all_pnl,
                paper_settled_pnl,
                paper_avg_settled_pnl,
                paper_avg_probability,
                paper_avg_edge,
                paper_avg_lag,
            ) = cur.fetchone()

            cur.execute(
                """
                SELECT
                    market_kind,
                    COUNT(*),
                    COUNT(*) FILTER (WHERE status='settled'),
                    COUNT(*) FILTER (
                        WHERE status='settled'
                        AND profit_loss_dollars > 0
                    ),
                    COALESCE(SUM(profit_loss_dollars)
                             FILTER (WHERE status='settled'), 0),
                    AVG(entry_price_cents)
                        FILTER (WHERE status='settled'),
                    AVG(preliminary_edge_points)
                        FILTER (WHERE status='settled'),
                    AVG(market_lag_points)
                        FILTER (WHERE status='settled')
                FROM paper_trades
                GROUP BY market_kind
                ORDER BY market_kind
                """
            )
            paper_kind_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE status='success'),
                    COUNT(*) FILTER (WHERE status='failed'),
                    MAX(completed_at),
                    MAX(completed_at) FILTER (WHERE status='success'),
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))
                        FILTER (WHERE completed_at IS NOT NULL)
                FROM scan_runs
                """
            )
            (
                scan_count,
                successful_scans,
                failed_scans,
                last_scan,
                last_success,
                avg_scan_seconds,
            ) = cur.fetchone()

    total_events = int(total_events or 0)
    report["summary"] = {
        "total_research_events": total_events,
        "open_research_events": int(open_events or 0),
        "closed_research_events": int(closed_events or 0),
        "first_response_count": int(first_response_count or 0),
        "milestone_25_count": int(milestone_25_count or 0),
        "milestone_50_count": int(milestone_50_count or 0),
        "milestone_75_count": int(milestone_75_count or 0),
        "milestone_90_count": int(milestone_90_count or 0),
        "first_response_rate_pct": safe_number(
            (first_response_count or 0) * 100 / total_events if total_events else 0
        ),
        "response_50_rate_pct": safe_number(
            (milestone_50_count or 0) * 100 / total_events if total_events else 0
        ),
        "response_90_rate_pct": safe_number(
            (milestone_90_count or 0) * 100 / total_events if total_events else 0
        ),
        "avg_initial_lag_points": safe_number(avg_initial_lag),
        "avg_initial_edge_points": safe_number(avg_initial_edge),
        "avg_max_market_move_points": safe_number(avg_max_move),
        "avg_first_response_minutes": safe_number(
            (avg_first_response_seconds or 0) / 60
            if avg_first_response_seconds is not None
            else None
        ),
        "avg_25_response_minutes": safe_number(
            (avg_25_seconds or 0) / 60
            if avg_25_seconds is not None
            else None
        ),
        "avg_50_response_minutes": safe_number(
            (avg_50_seconds or 0) / 60
            if avg_50_seconds is not None
            else None
        ),
        "avg_75_response_minutes": safe_number(
            (avg_75_seconds or 0) / 60
            if avg_75_seconds is not None
            else None
        ),
        "avg_90_response_minutes": safe_number(
            (avg_90_seconds or 0) / 60
            if avg_90_seconds is not None
            else None
        ),
    }

    for row in city_rows:
        (
            city,
            events,
            responded,
            m50,
            m90,
            lag,
            edge,
            max_move,
            response_seconds,
        ) = row
        n = int(events or 0)
        report["by_city"].append(
            {
                "city": city,
                "events": n,
                "first_response_rate_pct": safe_number(
                    (responded or 0) * 100 / n if n else 0
                ),
                "response_50_rate_pct": safe_number(
                    (m50 or 0) * 100 / n if n else 0
                ),
                "response_90_rate_pct": safe_number(
                    (m90 or 0) * 100 / n if n else 0
                ),
                "avg_initial_lag_points": safe_number(lag),
                "avg_initial_edge_points": safe_number(edge),
                "avg_max_market_move_points": safe_number(max_move),
                "avg_first_response_minutes": safe_number(
                    (response_seconds or 0) / 60
                    if response_seconds is not None
                    else None
                ),
            }
        )

    for row in variable_rows:
        (
            variable,
            events,
            responded,
            m50,
            m90,
            lag,
            edge,
            max_move,
            response_seconds,
        ) = row
        n = int(events or 0)
        report["by_variable"].append(
            {
                "variable": variable,
                "events": n,
                "first_response_rate_pct": safe_number(
                    (responded or 0) * 100 / n if n else 0
                ),
                "response_50_rate_pct": safe_number(
                    (m50 or 0) * 100 / n if n else 0
                ),
                "response_90_rate_pct": safe_number(
                    (m90 or 0) * 100 / n if n else 0
                ),
                "avg_initial_lag_points": safe_number(lag),
                "avg_initial_edge_points": safe_number(edge),
                "avg_max_market_move_points": safe_number(max_move),
                "avg_first_response_minutes": safe_number(
                    (response_seconds or 0) / 60
                    if response_seconds is not None
                    else None
                ),
            }
        )

    for row in bucket_rows:
        bucket, events, lag, edge, responded, m50, m90 = row
        n = int(events or 0)
        report["response_buckets"].append(
            {
                "forecast_change_bucket": bucket,
                "events": n,
                "avg_initial_lag_points": safe_number(lag),
                "avg_initial_edge_points": safe_number(edge),
                "first_response_rate_pct": safe_number(
                    (responded or 0) * 100 / n if n else 0
                ),
                "response_50_rate_pct": safe_number(
                    (m50 or 0) * 100 / n if n else 0
                ),
                "response_90_rate_pct": safe_number(
                    (m90 or 0) * 100 / n if n else 0
                ),
            }
        )

    event_keys = [
        "id", "created_at", "city", "forecast_date", "variable",
        "market_ticker", "side", "previous_probability",
        "current_probability", "forecast_probability_change_points",
        "pre_forecast_ask_cents", "event_ask_cents",
        "initial_market_change_points", "initial_market_lag_points",
        "initial_preliminary_edge_points", "first_response_at",
        "milestone_25_at", "milestone_50_at", "milestone_75_at",
        "milestone_90_at", "max_market_move_points",
        "latest_lag_remaining_points", "status", "settlement_result",
    ]
    for row in event_rows:
        item = dict(zip(event_keys, row))
        for key, value in list(item.items()):
            if hasattr(value, "isoformat"):
                item[key] = value.isoformat()
            elif isinstance(value, float):
                item[key] = round(value, 4)
        report["recent_events"].append(item)

    settled_wins = int(paper_yes_wins or 0) + int(paper_no_wins or 0)
    report["paper_trading"] = {
        "total_trades": int(paper_total or 0),
        "open_trades": int(paper_open or 0),
        "settled_trades": int(paper_settled or 0),
        "settled_wins": settled_wins,
        "settled_win_rate_pct": safe_number(
            settled_wins * 100 / paper_settled if paper_settled else 0
        ),
        "settled_pnl_dollars": safe_number(paper_settled_pnl),
        "average_settled_pnl_dollars": safe_number(paper_avg_settled_pnl),
        "average_probability_proxy_pct": safe_number(paper_avg_probability),
        "average_preliminary_edge_points": safe_number(paper_avg_edge),
        "average_market_lag_points": safe_number(paper_avg_lag),
        "by_market_kind": [
            {
                "market_kind": row[0],
                "trades": int(row[1] or 0),
                "settled": int(row[2] or 0),
                "wins": int(row[3] or 0),
                "settled_pnl_dollars": safe_number(row[4]),
                "avg_entry_price_cents": safe_number(row[5]),
                "avg_edge_points": safe_number(row[6]),
                "avg_lag_points": safe_number(row[7]),
            }
            for row in paper_kind_rows
        ],
    }

    report["scan_health"] = {
        "scan_count": int(scan_count or 0),
        "successful_scans": int(successful_scans or 0),
        "failed_scans": int(failed_scans or 0),
        "success_rate_pct": safe_number(
            (successful_scans or 0) * 100 / scan_count if scan_count else 0
        ),
        "last_scan_finished_utc": iso_value(last_scan),
        "last_successful_scan_finished_utc": iso_value(last_success),
        "average_scan_runtime_seconds": safe_number(avg_scan_seconds),
    }

    return report


def format_value(value, suffix=""):
    if value is None:
        return "—"
    return f"{value}{suffix}"


DASHBOARD_HTML = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Weather + Kalshi Research Dashboard</title>
<style>
body { font-family: Arial, sans-serif; margin: 0; background: #f4f6f8; color: #17202a; }
header { background: #17202a; color: white; padding: 24px; }
main { max-width: 1400px; margin: 0 auto; padding: 20px; }
h1, h2 { margin-top: 0; }
.small { color: #68737d; font-size: 13px; }
.cards { display: grid; grid-template-columns: repeat(auto-fit,minmax(180px,1fr)); gap: 12px; margin: 18px 0 24px; }
.card { background: white; border-radius: 10px; padding: 16px; box-shadow: 0 1px 4px rgba(0,0,0,.12); }
.card .value { font-size: 26px; font-weight: 700; margin-top: 8px; }
section { background: white; border-radius: 10px; padding: 18px; margin: 18px 0; box-shadow: 0 1px 4px rgba(0,0,0,.10); overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
th, td { border-bottom: 1px solid #e3e7ea; padding: 8px; text-align: left; white-space: nowrap; }
th { background: #f0f3f5; }
a { color: inherit; }
.note { background: #fff7df; border-left: 4px solid #d39e00; padding: 12px; margin: 16px 0; }
.good { font-weight: 700; }
@media (max-width: 700px) {
  main { padding: 10px; }
  .card .value { font-size: 21px; }
}
</style>
</head>
<body>
<header>
  <h1>Weather + Kalshi Research Dashboard</h1>
  <div>Historical forecast-shock and market-response research</div>
  <div class="small">Generated UTC: {{ report.generated_at_utc }}</div>
</header>
<main>

<div class="note">
  <strong>Important:</strong> This dashboard measures the bot's research hypothesis:
  when a forecast changes, how quickly and how far does the Kalshi market respond?
  It is not a claim that the ensemble probability is calibrated or that historical
  paper-trade results guarantee future performance.
</div>

<h2>Research Overview</h2>
<div class="cards">
  <div class="card"><div>Research events</div><div class="value">{{ report.summary.total_research_events }}</div></div>
  <div class="card"><div>First market response</div><div class="value">{{ report.summary.first_response_rate_pct }}%</div></div>
  <div class="card"><div>Reached 50% response</div><div class="value">{{ report.summary.response_50_rate_pct }}%</div></div>
  <div class="card"><div>Reached 90% response</div><div class="value">{{ report.summary.response_90_rate_pct }}%</div></div>
  <div class="card"><div>Avg initial lag</div><div class="value">{{ report.summary.avg_initial_lag_points }} pp</div></div>
  <div class="card"><div>Avg initial edge</div><div class="value">{{ report.summary.avg_initial_edge_points }} pp</div></div>
  <div class="card"><div>Avg max market move</div><div class="value">{{ report.summary.avg_max_market_move_points }} pp</div></div>
  <div class="card"><div>Avg first response</div><div class="value">{{ report.summary.avg_first_response_minutes }} min</div></div>
</div>

<section>
<h2>Market Response Timing</h2>
<table>
<tr><th>Milestone</th><th>Events reaching it</th><th>Rate</th><th>Average time</th></tr>
<tr><td>Any directional response</td><td>{{ report.summary.first_response_count if report.summary.first_response_count is defined else "—" }}</td><td>{{ report.summary.first_response_rate_pct }}%</td><td>{{ report.summary.avg_first_response_minutes }} min</td></tr>
<tr><td>25% of initial lag</td><td>{{ report.summary.milestone_25_count }}</td><td>{{ report.summary.milestone_25_count * 100 / report.summary.total_research_events if report.summary.total_research_events else 0 }}%</td><td>{{ report.summary.avg_25_response_minutes }} min</td></tr>
<tr><td>50% of initial lag</td><td>{{ report.summary.milestone_50_count }}</td><td>{{ report.summary.response_50_rate_pct }}%</td><td>{{ report.summary.avg_50_response_minutes }} min</td></tr>
<tr><td>75% of initial lag</td><td>{{ report.summary.milestone_75_count }}</td><td>{{ report.summary.milestone_75_count * 100 / report.summary.total_research_events if report.summary.total_research_events else 0 }}%</td><td>{{ report.summary.avg_75_response_minutes }} min</td></tr>
<tr><td>90% of initial lag</td><td>{{ report.summary.milestone_90_count }}</td><td>{{ report.summary.response_90_rate_pct }}%</td><td>{{ report.summary.avg_90_response_minutes }} min</td></tr>
</table>
<div class="small">The 25% and 75% counts are available in the database and the JSON endpoint; the main summary emphasizes the 50% and 90% milestones.</div>
</section>

<section>
<h2>By City</h2>
<table>
<tr><th>City</th><th>Events</th><th>First response</th><th>50% response</th><th>90% response</th><th>Avg lag</th><th>Avg edge</th><th>Avg max move</th><th>Avg response</th></tr>
{% for r in report.by_city %}
<tr>
<td>{{ r.city }}</td><td>{{ r.events }}</td><td>{{ r.first_response_rate_pct }}%</td>
<td>{{ r.response_50_rate_pct }}%</td><td>{{ r.response_90_rate_pct }}%</td>
<td>{{ r.avg_initial_lag_points }}</td><td>{{ r.avg_initial_edge_points }}</td>
<td>{{ r.avg_max_market_move_points }}</td><td>{{ r.avg_first_response_minutes }} min</td>
</tr>
{% endfor %}
</table>
</section>

<section>
<h2>Temperature vs Precipitation</h2>
<table>
<tr><th>Variable</th><th>Events</th><th>First response</th><th>50% response</th><th>90% response</th><th>Avg lag</th><th>Avg edge</th><th>Avg max move</th><th>Avg response</th></tr>
{% for r in report.by_variable %}
<tr>
<td>{{ r.variable }}</td><td>{{ r.events }}</td><td>{{ r.first_response_rate_pct }}%</td>
<td>{{ r.response_50_rate_pct }}%</td><td>{{ r.response_90_rate_pct }}%</td>
<td>{{ r.avg_initial_lag_points }}</td><td>{{ r.avg_initial_edge_points }}</td>
<td>{{ r.avg_max_market_move_points }}</td><td>{{ r.avg_first_response_minutes }} min</td>
</tr>
{% endfor %}
</table>
</section>

<section>
<h2>Forecast Shock Size</h2>
<table>
<tr><th>Forecast probability change</th><th>Events</th><th>Avg lag</th><th>Avg edge</th><th>First response</th><th>50% response</th><th>90% response</th></tr>
{% for r in report.response_buckets %}
<tr>
<td>{{ r.forecast_change_bucket }} pp</td><td>{{ r.events }}</td>
<td>{{ r.avg_initial_lag_points }}</td><td>{{ r.avg_initial_edge_points }}</td>
<td>{{ r.first_response_rate_pct }}%</td><td>{{ r.response_50_rate_pct }}%</td><td>{{ r.response_90_rate_pct }}%</td>
</tr>
{% endfor %}
</table>
</section>

<section>
<h2>Paper Trading</h2>
<div class="cards">
  <div class="card"><div>Total trades</div><div class="value">{{ report.paper_trading.total_trades }}</div></div>
  <div class="card"><div>Open</div><div class="value">{{ report.paper_trading.open_trades }}</div></div>
  <div class="card"><div>Settled</div><div class="value">{{ report.paper_trading.settled_trades }}</div></div>
  <div class="card"><div>Win rate</div><div class="value">{{ report.paper_trading.settled_win_rate_pct }}%</div></div>
  <div class="card"><div>Settled P/L</div><div class="value">${{ report.paper_trading.settled_pnl_dollars }}</div></div>
</div>
<table>
<tr><th>Market kind</th><th>Trades</th><th>Settled</th><th>Wins</th><th>Settled P/L</th><th>Avg entry</th><th>Avg edge</th><th>Avg lag</th></tr>
{% for r in report.paper_trading.by_market_kind %}
<tr>
<td>{{ r.market_kind }}</td><td>{{ r.trades }}</td><td>{{ r.settled }}</td><td>{{ r.wins }}</td>
<td>${{ r.settled_pnl_dollars }}</td><td>{{ r.avg_entry_price_cents }}¢</td>
<td>{{ r.avg_edge_points }} pp</td><td>{{ r.avg_lag_points }} pp</td>
</tr>
{% endfor %}
</table>
</section>

<section>
<h2>Scanner Health</h2>
<table>
<tr><th>Total scans</th><th>Successful</th><th>Failed</th><th>Success rate</th><th>Avg runtime</th><th>Last successful scan</th></tr>
<tr>
<td>{{ report.scan_health.scan_count }}</td><td>{{ report.scan_health.successful_scans }}</td>
<td>{{ report.scan_health.failed_scans }}</td><td>{{ report.scan_health.success_rate_pct }}%</td>
<td>{{ report.scan_health.average_scan_runtime_seconds }} sec</td>
<td>{{ report.scan_health.last_successful_scan_finished_utc or "—" }}</td>
</tr>
</table>
</section>

<section>
<h2>Recent Research Events</h2>
<table>
<tr>
<th>Created UTC</th><th>City</th><th>Date</th><th>Variable</th><th>Ticker</th><th>Side</th>
<th>Forecast change</th><th>Market change</th><th>Initial lag</th><th>Initial edge</th>
<th>First response</th><th>50%</th><th>90%</th><th>Max move</th><th>Lag remaining</th><th>Status</th>
</tr>
{% for r in report.recent_events %}
<tr>
<td>{{ r.created_at }}</td><td>{{ r.city }}</td><td>{{ r.forecast_date }}</td><td>{{ r.variable }}</td>
<td>{{ r.market_ticker }}</td><td>{{ r.side }}</td>
<td>{{ r.forecast_probability_change_points }}</td>
<td>{{ r.initial_market_change_points }}</td>
<td>{{ r.initial_market_lag_points }}</td>
<td>{{ r.initial_preliminary_edge_points }}</td>
<td>{{ r.first_response_at or "—" }}</td>
<td>{{ r.milestone_50_at or "—" }}</td>
<td>{{ r.milestone_90_at or "—" }}</td>
<td>{{ r.max_market_move_points }}</td>
<td>{{ r.latest_lag_remaining_points }}</td>
<td>{{ r.status }}</td>
</tr>
{% endfor %}
</table>
<div class="small">Showing the latest 100 research events. Use <a href="/research.json">/research.json</a> for the full aggregated report and <a href="/research.csv">/research.csv</a> for event-level CSV export.</div>
</section>

</main>
</body>
</html>
"""


@app.route("/")
def home():
    return (
        "Weather + Kalshi research bot is running. "
        "Use /health, /status, /research, /research.json, /research.csv, or /paper-trades."
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
                        (SELECT COUNT(*) FROM forecast_research_events),
                        (SELECT COUNT(*) FROM forecast_research_updates),
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
                    result["research_events"],
                    result["research_updates"],
                    result["paper_trades"],
                    result["open_paper_trades"],
                    result["settled_paper_pnl_dollars"],
                    result["last_scan_finished_utc"],
                    result["last_successful_scan_finished_utc"],
                ) = cur.fetchone()
    except Exception as exc:
        result["database_error"] = str(exc)

    return jsonify(result)


@app.route("/research")
def research():
    try:
        report = build_research_report()
        return render_template_string(DASHBOARD_HTML, report=report)
    except Exception as exc:
        return (
            "<h1>Research dashboard error</h1>"
            f"<pre>{str(exc)}</pre>"
        ), 500


@app.route("/research.json")
def research_json():
    try:
        return jsonify(build_research_report())
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/research.csv")
def research_csv():
    try:
        rows = fetch_all(
            """
            SELECT
                created_at, city, forecast_date, variable, market_ticker, side,
                previous_probability, current_probability,
                forecast_probability_change_points,
                pre_forecast_ask_cents, event_ask_cents,
                initial_market_change_points, initial_market_lag_points,
                initial_preliminary_edge_points, first_response_at,
                milestone_25_at, milestone_50_at, milestone_75_at,
                milestone_90_at, max_market_move_points,
                latest_lag_remaining_points, status, settlement_result
            FROM forecast_research_events
            ORDER BY created_at ASC
            """
        )

        headers = [
            "created_at", "city", "forecast_date", "variable", "market_ticker",
            "side", "previous_probability", "current_probability",
            "forecast_probability_change_points", "pre_forecast_ask_cents",
            "event_ask_cents", "initial_market_change_points",
            "initial_market_lag_points", "initial_preliminary_edge_points",
            "first_response_at", "milestone_25_at", "milestone_50_at",
            "milestone_75_at", "milestone_90_at", "max_market_move_points",
            "latest_lag_remaining_points", "status", "settlement_result",
        ]

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([iso_value(value) for value in row])

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=weather_kalshi_research.csv"
            },
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


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
        output = []
        for row in rows:
            item = dict(zip(keys, row))
            for key, value in list(item.items()):
                if hasattr(value, "isoformat"):
                    item[key] = value.isoformat()
            output.append(item)
        return jsonify(output)
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
