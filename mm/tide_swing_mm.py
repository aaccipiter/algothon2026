"""TIDE_SWING market maker.

Fair value derived from Thames tidal level SARIMA forecast.
FV = window['price'].sum() * 100  (straddle of absolute level diffs).
Refreshes tidal data every 15 minutes.
Fixed spread of 5 with symmetric skew: 3 per unit of net position.
"""

import time

import pandas as pd
import requests
from statsmodels.tsa.statespace.sarimax import SARIMAX

from market_making import MarketMaker
from bot_template import OrderRequest, Side

# ── Exchange config ──────────────────────────────────────────────────
CMI_URL = "http://ec2-52-19-74-159.eu-west-1.compute.amazonaws.com"
USERNAME = "BERT_trading"
PASSWORD = "bertbertbert"

# ── Product config ───────────────────────────────────────────────────
PRODUCT = "TIDE_SWING"
SPREAD = 5
ORDER_SIZE = 1
MAX_POSITION = 100

# ── Tide config ──────────────────────────────────────────────────────
THAMES_MEASURE = "0006-level-tidal_level-i-15_min-mAOD"
TIDE_REFRESH_SECS = 900  # 15 minutes

WINDOW_START = pd.Timestamp("2026-02-28 12:00:00")
WINDOW_END = pd.Timestamp("2026-03-01 12:00:00")


def _straddle(value: float) -> float:
    if value < 0.2:
        return 0.2 - value
    elif value > 0.25:
        return value - 0.25
    return 0.0


def fetch_tide_fair_value() -> float:
    """Fetch Thames tidal data, fit SARIMA, and compute TIDE_SWING fair value."""
    resp = requests.get(
        f"https://environment.data.gov.uk/flood-monitoring/id/measures/{THAMES_MEASURE}/readings",
        params={"_sorted": "", "_limit": 500},
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])

    df = pd.DataFrame(items)[["dateTime", "value"]].rename(
        columns={"dateTime": "time", "value": "level"}
    )
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.sort_values("time").reset_index(drop=True)

    df["time"] = pd.to_datetime(df["time"])
    df = df[df["time"] >= pd.to_datetime("2026-02-01 00:00:00+00:00")]
    df["time"] = df["time"].dt.tz_convert(None)
    df = df.set_index("time").sort_index().asfreq("15min")

    # Fit SARIMA
    model = SARIMAX(
        df["level"],
        order=(1, 0, 1),
        seasonal_order=(1, 0, 1, 50),
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    results = model.fit(disp=False)

    # Forecast to target time
    last_time = df.index[-1]
    steps = max(1, int((WINDOW_END - last_time).total_seconds() / 900))
    forecast = results.get_forecast(steps=steps)

    # Combine in-sample + out-of-sample
    insample_pred = results.get_prediction(start=0).predicted_mean
    oos_pred = forecast.predicted_mean
    all_pred = pd.concat([insample_pred, oos_pred])

    sarima_pred_df = pd.DataFrame({"pred_mean": all_pred})
    sarima_pred_df["level"] = df["level"].reindex(sarima_pred_df.index)

    # Slice window & compute price
    window = sarima_pred_df.loc[WINDOW_START:WINDOW_END].copy()
    window["level_diff"] = window["pred_mean"].diff().abs()
    window["price"] = window["level_diff"].apply(_straddle)

    fv = window["price"].sum() * 100
    return fv


class TideSwingMarketMaker(MarketMaker):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._last_tide_fetch: float = 0.0
        self._tide_fv: float | None = None

    def _refresh_tide(self) -> None:
        now = time.time()
        if self._tide_fv is None or now - self._last_tide_fetch >= TIDE_REFRESH_SECS:
            try:
                self._tide_fv = fetch_tide_fair_value()
                self._last_tide_fetch = now
                print(f"[TIDE] TIDE_SWING fair value: {self._tide_fv:.2f}")
            except Exception as exc:
                print(f"[TIDE] Fetch failed: {exc}")

    # ── Fair value ───────────────────────────────────────────────────

    def compute_fair_value(self, product: str) -> float | None:
        if product == PRODUCT:
            self._refresh_tide()
            return self._tide_fv
        return super().compute_fair_value(product)

    # ── Quoting ──────────────────────────────────────────────────────

    def calculate_quotes(
        self, product: str
    ) -> tuple[OrderRequest | None, OrderRequest | None]:
        fv = self.fair_values.get(product)
        if fv is None:
            return None, None

        position = self.positions.get(product, 0)

        # Symmetric skew: shift fair value by 3 per unit of net position.
        #   Long  → adjusted_fv lower → encourages selling
        #   Short → adjusted_fv higher → encourages buying
        adjusted_fv = fv - position * 3

        # Fixed spread of 5 centred on adjusted fair value
        bid_price = round(adjusted_fv - SPREAD / 2)
        ask_price = bid_price + SPREAD

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
    bot = TideSwingMarketMaker(
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
