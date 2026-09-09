import os
import re
import requests
from datetime import datetime

KALSHI_API_URL = os.environ.get(
    "KALSHI_API_URL",
    "https://external-api.kalshi.com/trade-api/v2"
).rstrip("/")

TIMEOUT = 20


def get(url, params=None):
    r = requests.get(
        url,
        params=params,
        timeout=TIMEOUT,
        headers={
            "User-Agent": "WeatherKalshiMarketAudit/1.0",
            "Accept": "application/json",
        },
    )
    r.raise_for_status()
    return r.json()


def get_all_series():
    rows = []
    cursor = None

    while True:
        params = {
            "category": "Climate and Weather",
            "limit": 1000,
        }

        if cursor:
            params["cursor"] = cursor

        data = get(KALSHI_API_URL + "/series", params)

        rows.extend(data.get("series", []))
        cursor = data.get("cursor")

        if not cursor:
            return rows


def get_markets(series_ticker):
    rows = []
    cursor = None

    while True:
        params = {
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 1000,
        }

        if cursor:
            params["cursor"] = cursor

        data = get(KALSHI_API_URL + "/markets", params)

        rows.extend(data.get("markets", []))
        cursor = data.get("cursor")

        if not cursor:
            return rows


def is_daily_high_temperature(series):
    title = (series.get("title") or "").lower()
    ticker = (series.get("ticker") or "").upper()
    frequency = (series.get("frequency") or "").lower()

    return (
        (not frequency or frequency == "daily")
        and "lowest temperature" not in title
        and (
            ticker.startswith("KXHIGH")
            or "highest temperature" in title
            or "high temperature" in title
            or "maximum temperature" in title
        )
    )


def city_from_series(series):
    title = " ".join((series.get("title") or "").split())

    match = re.search(
        r"(?:temperature)\s+in\s+(.+?)(?:\s+today\??|\s+on\s+.+?\??$|\?$|$)",
        title,
        re.I,
    )

    return match.group(1).strip(" ?.") if match else "UNKNOWN"


def market_date(market):
    for value in (
        market.get("event_ticker", ""),
        market.get("ticker", ""),
    ):
        for part in value.split("-"):
            try:
                return datetime.strptime(part, "%y%b%d").date().isoformat()
            except ValueError:
                pass

    return None


def label(market):
    strike_type = (market.get("strike_type") or "").lower()
    floor = market.get("floor_strike")
    cap = market.get("cap_strike")

    if strike_type == "between":
        return f"{floor}° to {cap}°"

    if strike_type == "greater":
        return f"{floor}° or above"

    if strike_type == "less":
        return f"{cap}° or below"

    return f"UNKNOWN ({strike_type})"


def main():
    print("=" * 80)
    print("KALSHI WEATHER MARKET AUDIT")
    print("=" * 80)
    print()

    series = get_all_series()

    candidates = [
        s for s in series
        if is_daily_high_temperature(s)
    ]

    # Match the scanner's preference:
    # KXHIGH... series first, then ticker alphabetically.
    candidates.sort(
        key=lambda s: (
            0 if (s.get("ticker") or "").upper().startswith("KXHIGH") else 1,
            s.get("ticker") or "",
        )
    )

    print(f"Daily-high temperature series found: {len(candidates)}")
    print()

    selected = {}

    for s in candidates:
        ticker = s.get("ticker")
        city = city_from_series(s)

        if ticker:
            # One selected series per city, matching the scanner's intent.
            key = city.lower()

            if key not in selected:
                selected[key] = s

    print("SELECTED SERIES")
    print("-" * 80)

    for city, s in sorted(selected.items()):
        print(
            f"{city.title():25} "
            f"{s.get('ticker'):20} "
            f"{s.get('title')}"
        )

    print()
    print("=" * 80)
    print("OPEN MARKET CONTRACT AUDIT")
    print("=" * 80)
    print()

    total_markets = 0
    errors = 0

    for city, s in sorted(selected.items()):
        series_ticker = s.get("ticker")
        markets = get_markets(series_ticker)

        print()
        print(f"{city.upper()} — {series_ticker}")
        print("-" * 80)

        # Group by event/date.
        events = {}

        for m in markets:
            date = market_date(m)
            events.setdefault(date, []).append(m)

        for date in sorted(events):
            ms = events[date]

            print(f"\nDATE: {date}")
            print(f"Markets: {len(ms)}")

            # Sort by floor/cap for easier inspection.
            ms.sort(
                key=lambda m: (
                    float(m.get("floor_strike") or -9999),
                    float(m.get("cap_strike") or -9999),
                )
            )

            labels = []

            for m in ms:
                ticker = m.get("ticker")
                event = m.get("event_ticker")
                strike_type = (m.get("strike_type") or "").lower()
                floor = m.get("floor_strike")
                cap = m.get("cap_strike")
                title = m.get("title") or ""
                yes_ask = m.get("yes_ask_dollars")
                no_ask = m.get("no_ask_dollars")
                last = m.get("last_price_dollars")

                total_markets += 1

                problems = []

                if not ticker:
                    problems.append("NO TICKER")

                if not event:
                    problems.append("NO EVENT")

                if not date:
                    problems.append("NO DATE")

                if "lowest temperature" in title.lower():
                    problems.append("LOW-TEMP MARKET")

                if not series_ticker.upper().startswith("KXHIGH"):
                    problems.append("NOT KXHIGH SERIES")

                if strike_type == "between":
                    if floor is None or cap is None:
                        problems.append("BETWEEN WITHOUT FLOOR/CAP")
                    elif float(cap) < float(floor):
                        problems.append("CAP < FLOOR")

                elif strike_type not in {"greater", "less"}:
                    problems.append("UNKNOWN STRIKE TYPE")

                print(
                    f"  {ticker}\n"
                    f"    Contract: {label(m)}\n"
                    f"    strike_type={strike_type} "
                    f"floor={floor} cap={cap}\n"
                    f"    event={event}\n"
                    f"    title={title}\n"
                    f"    YES ask={yes_ask} "
                    f"NO ask={no_ask} "
                    f"last={last}"
                )

                if problems:
                    errors += len(problems)
                    print(
                        "    !!! PROBLEM: "
                        + ", ".join(problems)
                    )

                labels.append(label(m))

    print()
    print("=" * 80)
    print("AUDIT SUMMARY")
    print("=" * 80)
    print(f"Selected daily-high series: {len(selected)}")
    print(f"Open markets inspected:     {total_markets}")
    print(f"Problems found:             {errors}")
    print()

    if errors == 0:
        print("RESULT: PASS")
        print()
        print(
            "The live Kalshi market structure matches the "
            "scanner's expected daily-high temperature structure."
        )
    else:
        print("RESULT: REVIEW REQUIRED")
        print()
        print(
            "One or more market/contract mismatches were found."
        )


if __name__ == "__main__":
    main()
