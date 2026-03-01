"""LHR_INDEX market maker.

Hard-coded fair value of 170.
Fixed spread of 5 with asymmetric skew: 2 per unit short, 1 per unit long.
"""

from market_making import MarketMaker
from bot_template import OrderRequest, Side

# ── Exchange config ──────────────────────────────────────────────────
CMI_URL = "http://ec2-52-19-74-159.eu-west-1.compute.amazonaws.com"
USERNAME = "BERT_trading"
PASSWORD = "bertbertbert"

# ── Product config ───────────────────────────────────────────────────
PRODUCT = "LHR_INDEX"
FAIR_VALUE = 170
SPREAD = 5          # total ask - bid width
ORDER_SIZE = 1
MAX_POSITION = 100


class LhrIndexMarketMaker(MarketMaker):

    def compute_fair_value(self, product: str) -> float | None:
        if product == PRODUCT:
            return FAIR_VALUE
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
