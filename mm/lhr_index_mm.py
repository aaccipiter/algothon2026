"""LHR_INDEX market maker.

Fair value derived from LHR flight imbalance (AeroDataBox API).
Refreshes flight data every 15 minutes.
Fixed spread of 5 with asymmetric skew: 2 per unit short, 1 per unit long.
"""

import math
import time
from datetime import datetime, timedelta

import requests

from market_making import MarketMaker
from bot_template import OrderRequest, Side

# ── Exchange config ──────────────────────────────────────────────────
CMI_URL = "http://ec2-52-19-74-159.eu-west-1.compute.amazonaws.com"
USERNAME = "BERT_trading"
PASSWORD = "bertbertbert"

# ── Product config ───────────────────────────────────────────────────
PRODUCT = "LHR_INDEX"
SPREAD = 5          # total ask - bid width
ORDER_SIZE = 1
MAX_POSITION = 100

# ── Flight data config ───────────────────────────────────────────────
AERODATABOX_KEY = "a70a952394msh6b78ca787f2c543p19188cjsn507f0843caaa"
AERODATABOX_HOST = "aerodatabox.p.rapidapi.com"
FLIGHT_HEADERS = {"x-rapidapi-host": AERODATABOX_HOST, "x-rapidapi-key": AERODATABOX_KEY}
AIRPORT = "LHR"
RATE_LIMIT_PAUSE = 1.2
FLIGHT_REFRESH_SECS = 900  # 15 minutes

WINDOW_START = "2026-02-28 12:00"
WINDOW_END = "2026-03-01 12:00"


def _fetch_flight_window(from_local: str, to_local: str) -> dict:
    url = (
        f"https://{AERODATABOX_HOST}/flights/airports/iata/{AIRPORT}"
        f"/{from_local}/{to_local}"
        f"?direction=Both&withCancelled=true&withCodeshared=true"
        f"&withLocation=false&withCargo=true"
    )
    resp = requests.get(url, headers=FLIGHT_HEADERS, timeout=30)
    if resp.status_code == 204:
        return {"arrivals": [], "departures": []}
    resp.raise_for_status()
    return resp.json()


def _fetch_flight_range(from_dt: datetime, to_dt: datetime) -> dict:
    all_arrivals, all_departures = [], []
    current = from_dt
    while current < to_dt:
        chunk_end = min(current + timedelta(hours=12), to_dt)
        data = _fetch_flight_window(
            current.strftime("%Y-%m-%dT%H:%M"),
            chunk_end.strftime("%Y-%m-%dT%H:%M"),
        )
        all_arrivals.extend(data.get("arrivals", []))
        all_departures.extend(data.get("departures", []))
        current = chunk_end
        if current < to_dt:
            time.sleep(RATE_LIMIT_PAUSE)
    return {"arrivals": all_arrivals, "departures": all_departures}


def _dedup(flights: list[dict]) -> list[dict]:
    groups: dict[str, tuple[int, dict]] = {}
    for f in flights:
        movement = f.get("movement", {}) or {}
        scheduled = (movement.get("scheduledTime", {}) or {}).get("utc", "")
        other_iata = (movement.get("airport", {}) or {}).get("iata", "")
        key = f"{scheduled}|{other_iata}"
        cs_status = f.get("codeshareStatus", "")
        rank = 0 if cs_status == "IsOperator" else (1 if cs_status == "" else 2)
        if key not in groups or rank < groups[key][0]:
            groups[key] = (rank, f)
    return [v[1] for v in groups.values()]


def fetch_lhr_imbalance() -> float:
    """Fetch LHR flights and compute the absolute imbalance sum fair value."""
    start = datetime.strptime(WINDOW_START, "%Y-%m-%d %H:%M")
    end = datetime.strptime(WINDOW_END, "%Y-%m-%d %H:%M")

    data = _fetch_flight_range(start, end)

    # Remove cancellations
    def not_cancelled(f):
        return not ((f.get("status") or "").lower().startswith("cancel"))

    arrivals = _dedup([f for f in data["arrivals"] if not_cancelled(f)])
    departures = _dedup([f for f in data["departures"] if not_cancelled(f)])

    # Build 30-min buckets
    buckets: dict[datetime, dict[str, int]] = {}
    current = start
    while current < end:
        buckets[current] = {"arrivals": 0, "departures": 0}
        current += timedelta(minutes=30)

    def _get_scheduled_local(f):
        movement = f.get("movement", {}) or {}
        st = (movement.get("scheduledTime", {}) or {})
        t = st.get("local") or st.get("utc")
        if t:
            return datetime.fromisoformat(t.replace("Z", "+00:00").replace("+00:00", ""))
        return None

    def assign(flights, direction):
        for f in flights:
            t = _get_scheduled_local(f)
            if t is None or t < start or t >= end:
                continue
            bucket_key = start + timedelta(minutes=int((t - start).total_seconds() / 60 // 30) * 30)
            if bucket_key in buckets:
                buckets[bucket_key][direction] += 1

    assign(arrivals, "arrivals")
    assign(departures, "departures")

    # Compute imbalance metric per bucket and sum
    running_sum = 0.0
    for t in sorted(buckets):
        a = buckets[t]["arrivals"]
        d = buckets[t]["departures"]
        running_sum += 100 * ((a - d) / max(a + d, 1))

    return abs(running_sum)


class LhrIndexMarketMaker(MarketMaker):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._last_flight_fetch: float = 0.0
        self._lhr_fv: float | None = None

    def _refresh_flights(self) -> None:
        now = time.time()
        if self._lhr_fv is None or now - self._last_flight_fetch >= FLIGHT_REFRESH_SECS:
            try:
                self._lhr_fv = fetch_lhr_imbalance()
                self._last_flight_fetch = now
                print(f"[FLIGHTS] LHR_INDEX fair value: {self._lhr_fv:.2f}")
            except Exception as exc:
                print(f"[FLIGHTS] Fetch failed: {exc}")

    def compute_fair_value(self, product: str) -> float | None:
        if product == PRODUCT:
            self._refresh_flights()
            return self._lhr_fv
        return super().compute_fair_value(product)

    def calculate_quotes(
        self, product: str
    ) -> tuple[OrderRequest | None, OrderRequest | None]:
        fv = self.fair_values.get(product)
        if fv is None:
            return None, None

        position = self.positions.get(product, 0)

        # Asymmetric skew:
        #   Short (position < 0) → adjust theo UP by 2 per unit
        #   Long  (position > 0) → adjust theo DOWN by 1 per unit
        if position < 0:
            skew = position * 2  # negative * 2 → negative, subtracted below → shifts up
        else:
            skew = position * 1  # positive * 1 → positive, subtracted below → shifts down
        adjusted_fv = fv - skew

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
    bot = LhrIndexMarketMaker(
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
