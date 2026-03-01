"""WX_SUM market maker.

Fair value derived from Open-Meteo 15-minute weather data for London.
Refreshes weather every 15 minutes (configurable).
Fixed spread of 9 with position-linear skew (1 tick per unit of net exposure).
"""

import math
import time
from datetime import datetime

import requests

from market_making import MarketMaker
from bot_template import OrderRequest, Side

# ── Exchange config ──────────────────────────────────────────────────
CMI_URL = "http://ec2-52-19-74-159.eu-west-1.compute.amazonaws.com"
USERNAME = "BERT_trading"
PASSWORD = "bertbertbert"

# ── Product config ───────────────────────────────────────────────────
PRODUCT = "WX_SUM"
SPREAD = 9          # total ask - bid width
ORDER_SIZE = 1
MAX_POSITION = 100

# ── Weather config ───────────────────────────────────────────────────
LONDON_LAT, LONDON_LON = 51.5074, -0.1278
WEATHER_REFRESH_SECS = 900  # 15 minutes

# Competition window (London time, naive datetimes to match API output)
WINDOW_START = datetime(2026, 2, 28, 12, 0)
WINDOW_END = datetime(2026, 3, 1, 12, 0)


def fetch_wx_sum(past_steps: int = 96, forecast_steps: int = 96) -> float:
    """Fetch 15-min weather for London and compute WX_SUM fair value.

    WX_SUM = sum over competition window of (temp_F * humidity / 100)
    where temp_F = temp_C * 9/5 + 32.
    """
    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": LONDON_LAT,
            "longitude": LONDON_LON,
            "minutely_15": "temperature_2m,relative_humidity_2m",
            "past_minutely_15": past_steps,
            "forecast_minutely_15": forecast_steps,
            "timezone": "Europe/London",
        },
    )
    resp.raise_for_status()
    m = resp.json()["minutely_15"]

    wx_sum = 0.0
    for t_str, temp_c, hum in zip(m["time"], m["temperature_2m"], m["relative_humidity_2m"]):
        if temp_c is None or hum is None:
            continue
        dt = datetime.fromisoformat(t_str)
        if WINDOW_START <= dt < WINDOW_END:
            temp_f = temp_c * 9 / 5 + 32
            wx_sum += temp_f * hum / 100

    return wx_sum


class WxSumMarketMaker(MarketMaker):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._last_weather_fetch: float = 0.0
        self._wx_sum: float | None = None

    def _refresh_weather(self) -> None:
        now = time.time()
        if self._wx_sum is None or now - self._last_weather_fetch >= WEATHER_REFRESH_SECS:
            try:
                self._wx_sum = fetch_wx_sum()
                self._last_weather_fetch = now
                print(f"[WEATHER] WX_SUM fair value: {self._wx_sum:.2f}")
            except Exception as exc:
                print(f"[WEATHER] Fetch failed: {exc}")

    # ── Fair value ───────────────────────────────────────────────────

    def compute_fair_value(self, product: str) -> float | None:
        if product == PRODUCT:
            self._refresh_weather()
            return self._wx_sum
        return super().compute_fair_value(product)

    # ── Quoting ──────────────────────────────────────────────────────

    def calculate_quotes(
        self, product: str
    ) -> tuple[OrderRequest | None, OrderRequest | None]:
        fv = self.fair_values.get(product)
        if fv is None:
            return None, None

        position = self.positions.get(product, 0)

        # Skew: shift fair value by 2 per unit of net position.
        #   Long  → adjusted_fv lower → encourages selling
        #   Short → adjusted_fv higher → encourages buying
        adjusted_fv = fv - position * 2

        # Fixed spread of 9 centred on adjusted fair value
        bid_price = round(adjusted_fv - SPREAD / 2)
        ask_price = bid_price + SPREAD  # guarantees exactly 9 wide

        # Prices must be positive
        bid_price = max(1, bid_price)
        ask_price = max(bid_price + 1, ask_price)

        # Position-limit gating
        bid = (
            OrderRequest(product=product, price=bid_price, side=Side.BUY, volume=self.order_size)
            if position < self.max_position
            else None
        )
        ask = (
            OrderRequest(product=product, price=ask_price, side=Side.SELL, volume=self.order_size)
            if position > -self.max_position
            else None
        )

        return bid, ask


if __name__ == "__main__":
    bot = WxSumMarketMaker(
        cmi_url=CMI_URL,
        username=USERNAME,
        password=PASSWORD,
        products=[PRODUCT],
        spread=SPREAD,
        max_position=MAX_POSITION,
        order_size=ORDER_SIZE,
        requote_interval=5.0,
        min_request_interval=1.0,
    )
    bot.run_loop()
