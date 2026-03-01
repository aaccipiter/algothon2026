import math
import time
import threading

from market_making import MarketMaker

from bot_template import (
    BaseBot,
    Order,
    OrderBook,
    OrderRequest,
    OrderResponse,
    Side,
    Trade,
)

CMI_URL = "http://ec2-52-19-74-159.eu-west-1.compute.amazonaws.com"
USERNAME = "BERT_trading"
PASSWORD = "bertbertbert"

# --- which products to quote ---
PRODUCTS = ["LHR_COUNT"]

class MyMarketMaker(MarketMaker):
    def compute_fair_value(self, product: str) -> float | None:
        fair_value = 1322
        return fair_value

    def calculate_quotes(self, product: str) -> tuple[OrderRequest | None, OrderRequest | None]:
        penalty_scale = 0.2
        mid = self.get_mid_price(product)
        fv = self.fair_values.get(product)

        if mid is not None and fv is not None:
            divergence = abs(mid - fv)
            penalty = divergence * penalty_scale
        else:
            penalty = 0.0

        original_spread = self._spread_cfg
        self._spread_cfg = {product: self.get_spread(product) + round(penalty)}
        
        bid, ask = super().calculate_quotes(product)
        
        self._spread_cfg = original_spread 
        return bid, ask       
            

bot = MyMarketMaker(
    cmi_url=CMI_URL,
    username=USERNAME,
    password=PASSWORD,
    products=PRODUCTS,
    spread=100,           # ticks each side of fv
    edge=0,             # additional buffer beyond spread
    max_position=50,    # hard limit per product
    order_size=5,       # base order size (contracts)
    skew_aggr=0.8,      # how aggressively to lean against inventory
    requote_interval=5.0,
    min_request_interval=1.0,  # 1 req/s rate limit
)
bot.run_loop()