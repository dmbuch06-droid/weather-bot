from datetime import datetime
import math
import os
import threading
import time
from flask import Flask
import requests

app = Flask(__name__)

DISCORD_WEBHOOK_URL = os.getenv(
    "DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1534781924252323891/7hm54rbQchA2idRvqEoi6j6grqqk7Wx48CBMWqbKwRJdn2vxkfZ9II1d1pCX1IXNbD2R",
)

PAPER_PORTFOLIO = {
    "starting_balance_usd": 1000.00,
    "cash_balance_usd": 1000.00,
    "open_positions": [],
    "settled_history": [],
}

CITIES = {
    "Chicago": {"lat": 41.8781, "lon": -87.6298, "series": "KXHIGHCHI"},
    "New York City": {"lat": 40.7128, "lon": -74.0060, "series": "KXHIGHNY"},
    "Miami": {"lat": 25.7617, "lon": -80.1918, "series": "KXHIGHMIA"},
    "Austin": {"lat": 30.2672, "lon": -97.7431, "series": "KXHIGHAUT"},
    "Los Angeles": {"lat": 34.0522, "lon": -118.2437, "series": "KXHIGHLA"},
}

MIN_EDGE_PERCENT = 0.05
MIN_LIQUIDITY = 5


def get_hrrr_forecast(lat, lon):
  url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m&models=best_match&temperature_unit=fahrenheit"
  try:
    response = requests.get(url, timeout=10)
    data = response.json()
    if "hourly" not in data:
      return None
    return max(data["hourly"]["temperature_2m"][:24])
  except Exception as e:
    print(f"HRRR Fetch Error: {e}")
    return None


def get_active_kalshi_markets(series_ticker):
  url = f"https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker={series_ticker}&status=open"
  try:
    data = requests.get(url, timeout=10).json()
    markets = data.get("markets", [])
    active_markets = []
    for m in markets:
      active_markets.append({
          "ticker": m.get("ticker"),
          "yes_ask": m.get("yes_ask", 50),
          "yes_ask_volume": m.get("yes_ask_volume", 0),
          "floor_strike": m.get("floor_strike", 0.0),
      })
    return active_markets
  except Exception as e:
    print(f"Kalshi API Error for {series_ticker}: {e}")
    return []


def calculate_edge(model_temp, strike, market_price_cents):
  sigma = 1.2
  z = (strike - model_temp) / (sigma * math.sqrt(2))
  model_prob = 0.5 * (1.0 + math.erf(z))
  market_prob = market_price_cents / 100.0
  edge = model_prob - market_prob
  expected_value_dollar = (model_prob * 1.0) - (market_price_cents / 100.0)
  return model_prob * 100, edge * 100, expected_value_dollar


def execute_paper_trade(city, market_data, model_p, edge, ev_dollar):
  contract_count = 5
  entry_cents = market_data["yes_ask"]
  total_cost = (entry_cents * contract_count) / 100.0

  if PAPER_PORTFOLIO["cash_balance_usd"] >= total_cost:
    PAPER_PORTFOLIO["cash_balance_usd"] -= total_cost
    trade = {
        "city": city,
        "ticker": market_data["ticker"],
        "contracts": contract_count,
        "entry_price_cents": entry_cents,
        "cost_usd": total_cost,
        "projected_ev_dollar": ev_dollar,
        "model_prob": model_p,
        "edge": edge,
        "time_placed": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    PAPER_PORTFOLIO["open_positions"].append(trade)
    print(
        f"🟢 [PAPER TRADE EXECUTED] {city} ({market_data['ticker']}) | 5x @"
        f" {entry_cents}¢ | Proj EV: ${ev_dollar:+.2f}"
    )
    return True
  return False


def send_discord_alert(message):
  try:
    requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=5)
  except Exception as e:
    print(f"Discord Error: {e}")


def background_weather_loop():
  """Runs the continuous background polling loop safely."""
  print("🚀 Cloud Background Weather Monitor Loop Started!")
  while True:
    try:
      for city, info in CITIES.items():
        forecast_temp = get_hrrr_forecast(info["lat"], info["lon"])
        markets = get_active_kalshi_markets(info["series"])

        if not forecast_temp or not markets:
          continue

        for market_data in markets:
          strike = market_data["floor_strike"]
          ask = market_data["yes_ask"]
          liquidity = market_data["yes_ask_volume"]

          if ask <= 0 or ask >= 100 or strike <= 0:
            continue

          model_p, edge, ev_dollar = calculate_edge(forecast_temp, strike, ask)

          if edge >= (MIN_EDGE_PERCENT * 100) and liquidity >= MIN_LIQUIDITY:
            already_traded = any(
                t["ticker"] == market_data["ticker"]
                for t in PAPER_PORTFOLIO["open_positions"]
            )
            if not already_traded:
              success = execute_paper_trade(
                  city, market_data, model_p, edge, ev_dollar
              )
              if success:
                msg = (
                    f"📐 **High-EV Paper Trade Logged (Cloud)** 📐\n"
                    f"🏙️ **City:** {city} | 📈 **Model Peak:**"
                    f" **{forecast_temp}°F** (Strike: {strike}°F)\n"
                    f"🎟️ **Ticker:** `{market_data['ticker']}`\n"
                    f"📊 **Model Prob:** `{model_p:.1f}%` | **Edge:**"
                    f" `+{edge:.1f}%`\n"
                    f"💵 **Projected EV:** `+${ev_dollar:.2f} per contract`\n"
                    f"💻 *Virtual Cash Left:"
                    f" ${PAPER_PORTFOLIO['cash_balance_usd']:.2f}*"
                )
                send_discord_alert(msg)
    except Exception as e:
      print(f"Loop Exception: {e}")

    time.sleep(900)  # Poll every 15 minutes


@app.route("/")
def home():
  cash = PAPER_PORTFOLIO["cash_balance_usd"]
  open_count = len(PAPER_PORTFOLIO["open_positions"])
  return (
      f"Weather Arbitrage Bot is LIVE 🚀 | Cash Balance: ${cash:.2f} | Open"
      f" Positions: {open_count}"
  )


# Start the background polling thread when Flask boots up
threading.Thread(target=background_weather_loop, daemon=True).start()

if __name__ == "__main__":
  port = int(os.environ.get("PORT", 5000))
  app.run(host="0.0.0.0", port=port)
