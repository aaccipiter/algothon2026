import math
import time
import threading

from bot_template import (
    BaseBot,
    Order,
    OrderBook,
    OrderRequest,
    OrderResponse,
    Side,
    Trade,
)

class RateLimiter:
    """Thread-safe token-bucket enforcing a minimum gap between API calls."""

    def __init__(self, min_interval: float = 1.0):
        self.min_interval = min_interval
        self._last_call: float = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            gap = self.min_interval - (now - self._last_call)
            if gap > 0:
                time.sleep(gap)
            self._last_call = time.monotonic()


class MarketMaker(BaseBot):

    def __init__(
        self,
        cmi_url: str,
        username: str,
        password: str,
        products: list[str],
        *,
        # -- quoting params --
        spread: int | dict[str, int] = 2,
        edge: float = 0.0,
        max_position: int = 100,
        order_size: int = 10,
        skew_aggr: float = 0.8,
        size_reduction_factor: float = 0.5,
        # -- timing --
        requote_interval: float = 5.0,
        min_request_interval: float = 1.0,
        # -- housekeeping --
        position_sync_cycles: int = 10,
    ):
        super().__init__(cmi_url, username, password)
        self.products_to_quote = products

        # Quoting parameters
        self._spread_cfg = spread
        self.edge = edge
        self.max_position = max_position
        self.order_size = order_size
        self.skew_aggr = skew_aggr
        self.size_reduction_factor = size_reduction_factor

        # Timing
        self.requote_interval = requote_interval
        self.position_sync_cycles = position_sync_cycles

        # Live state (updated by SSE callbacks + API syncs)
        self.books: dict[str, OrderBook] = {}
        self.positions: dict[str, int] = {p: 0 for p in products}
        self.fair_values: dict[str, float | None] = {p: None for p in products}
        self.active_order_ids: dict[str, list[str]] = {p: [] for p in products}
        self.last_quotes: dict[str, tuple[OrderRequest | None, OrderRequest | None]] = {}
        self.fills: list[Trade] = []
        self.realized_pnl: float = 0.0

        # Rate limiter wrapping every REST call
        self._limiter = RateLimiter(min_request_interval)
        self._running = False
        self._fill_event = threading.Event()  # set by on_trades to wake main loop

    def get_spread(self, product: str) -> int:
        """Return the configured spread (ticks) for *product*."""
        if isinstance(self._spread_cfg, dict):
            return self._spread_cfg.get(product, 2)
        return self._spread_cfg

    def get_mid_price(self, product: str) -> float | None:
        book = self.books.get(product)
        if book and book.buy_orders and book.sell_orders:
            return (book.buy_orders[0].price + book.sell_orders[0].price) / 2
        return None
    
    # ------------------------------------------------------------------
    # SSE callbacks  (called from the background SSE thread)
    # ------------------------------------------------------------------

    def on_orderbook(self, book: OrderBook) -> None:
        self.books[book.product] = book

    def on_trades(self, trade: Trade) -> None:
        is_buyer = trade.buyer == self.username
        is_seller = trade.seller == self.username
        if not (is_buyer or is_seller):
            return

        # Update position
        sign = 1 if is_buyer else -1
        self.positions[trade.product] = (
            self.positions.get(trade.product, 0) + sign * trade.volume
        )

        # Track P&L (cost basis accounting)
        cost = trade.price * trade.volume
        self.realized_pnl += -cost if is_buyer else cost

        self.fills.append(trade)

        # Invalidate cached quotes so next requote cycle will re-place
        self.last_quotes.pop(trade.product, None)

        side_str = "BOT" if is_buyer else "SLD"
        pos = self.positions[trade.product]
        print(
            f"  [FILL] {side_str} {trade.volume} {trade.product} "
            f"@ {trade.price} | pos={pos} | pnl={self.realized_pnl:.0f}"
        )

        # Wake the main loop so it requotes immediately
        self._fill_event.set()

        # Hook for subclass-specific fill logic
        self.on_fill(trade)

    def on_fill(self, trade: Trade) -> None:
        """Called after every processed fill. Override for custom behaviour."""


    def compute_fair_value(self, product: str) -> float | None:
        """Return your fair value estimate for *product*, or None to skip.

        **Override this method with your model.**  Examples::

            def compute_fair_value(self, product):
                if product == "TIDE_SPOT":
                    return my_tide_model.predict_abs_height_mm()
                if product == "WX_SPOT":
                    return forecast_temp_f * forecast_humidity_pct
                return super().compute_fair_value(product)   # fallback

        Default implementation: orderbook mid-price (provides zero edge).
        """
        book = self.books.get(product)
        if book and book.buy_orders and book.sell_orders:
            return (book.buy_orders[0].price + book.sell_orders[0].price) / 2
        return None

    # ------------------------------------------------------------------
    # Quote calculation  (ported from val_mm calculate_quotes)
    # ------------------------------------------------------------------

    def calculate_quotes(
        self, product: str
    ) -> tuple[OrderRequest | None, OrderRequest | None]:
        """Derive a bid and ask for *product* from fair value + inventory.

        Implements:
        - Half-spread on each side of fair value
        - Additional ``edge`` buffer
        - Inventory skew that shifts both quotes to discourage adding
          to a growing position  (val_mm ``SKEW_AGGRESSIVENESS``)
        - Order size reduction as position approaches ``max_position``
        - Hard stop: no bid when at +max_position, no ask at -max_position
        """
        fv = self.fair_values.get(product)
        if fv is None:
            return None, None

        spread = self.get_spread(product)
        position = self.positions.get(product, 0)

        # Inventory ratio in [-1, 1]
        inv_ratio = (
            max(-1.0, min(1.0, position / self.max_position))
            if self.max_position > 0
            else 0.0
        )

        # Skew: when long (inv_ratio > 0) shift quotes DOWN to encourage
        # selling; when short shift UP to encourage buying.
        skew = (spread / 2) * inv_ratio * self.skew_aggr

        # Price levels -- floor/ceil ensure at least ``spread`` ticks wide
        bid_price = math.floor(fv - spread / 2 - self.edge - skew)
        ask_price = math.ceil(fv + spread / 2 + self.edge - skew)

        # Safety: ask must be strictly above bid
        if ask_price <= bid_price:
            ask_price = bid_price + 1

        # Prices must be positive
        bid_price = max(1, bid_price)
        ask_price = max(bid_price + 1, ask_price)

        # Size scaling: shrink as position grows toward limit
        size_factor = 1.0 - abs(inv_ratio) * self.size_reduction_factor
        bid_size = max(1, round(self.order_size * size_factor))
        ask_size = max(1, round(self.order_size * size_factor))

        # Position-limit gating
        bid = (
            OrderRequest(product=product, price=bid_price, side=Side.BUY, volume=bid_size)
            if position < self.max_position
            else None
        )
        ask = (
            OrderRequest(product=product, price=ask_price, side=Side.SELL, volume=ask_size)
            if position > -self.max_position
            else None
        )

        return bid, ask

    # ------------------------------------------------------------------
    # Rate-limited order management
    # ------------------------------------------------------------------

    def _api_send(self, order: OrderRequest) -> OrderResponse | None:
        self._limiter.wait()
        resp = super().send_order(order)
        return resp

    def _api_cancel(self, order_id: str) -> None:
        self._limiter.wait()
        super().cancel_order(order_id)

    def _cancel_product_orders(self, product: str) -> None:
        """Cancel every tracked resting order for *product*."""
        for oid in self.active_order_ids.get(product, []):
            try:
                self._api_cancel(oid)
            except Exception:
                pass  # order already filled / cancelled
        self.active_order_ids[product] = []

    # ------------------------------------------------------------------
    # Requote: the core cancel-replace cycle for one product
    # ------------------------------------------------------------------

    def _quotes_match(
        self,
        product: str,
        bid: OrderRequest | None,
        ask: OrderRequest | None,
    ) -> bool:
        """Return True if bid/ask are identical to the last placed quotes."""
        prev = self.last_quotes.get(product)
        if prev is None:
            return False
        prev_bid, prev_ask = prev

        def _eq(a: OrderRequest | None, b: OrderRequest | None) -> bool:
            if a is None and b is None:
                return True
            if a is None or b is None:
                return False
            return a.price == b.price and a.volume == b.volume and a.side == b.side

        return _eq(bid, prev_bid) and _eq(ask, prev_ask)

    def requote(self, product: str) -> None:
        """Full cancel → compute → place cycle for one product.

        Skips the cancel-replace if the new quotes are identical to the
        ones already resting on the exchange.
        """

        # 1. Recompute fair value
        self.fair_values[product] = self.compute_fair_value(product)

        # 2. Derive new quotes
        bid, ask = self.calculate_quotes(product)

        # 3. Skip if quotes unchanged and we still have resting orders
        if self._quotes_match(product, bid, ask) and self.active_order_ids.get(product):
            return

        # 4. Pull stale quotes
        self._cancel_product_orders(product)

        # 5. Place new orders
        if bid:
            resp = self._api_send(bid)
            if resp:
                self.active_order_ids[product].append(resp.id)

        if ask:
            resp = self._api_send(ask)
            if resp:
                self.active_order_ids[product].append(resp.id)

        self.last_quotes[product] = (bid, ask)

        # Log
        fv = self.fair_values.get(product)
        pos = self.positions.get(product, 0)
        b = f"bid={bid.price}x{bid.volume}" if bid else "no-bid"
        a = f"ask={ask.price}x{ask.volume}" if ask else "no-ask"
        print(f"[QUOTE] {product}: fv={fv} pos={pos} {b} {a}")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run_loop(self) -> None:
        """Start quoting. Blocks until Ctrl-C."""
        self.start()  # launch SSE thread
        self._running = True

        print(f"Market maker started for {self.products_to_quote}")
        print(
            f"  spread={self._spread_cfg}  edge={self.edge}  "
            f"max_pos={self.max_position}  size={self.order_size}  "
            f"skew={self.skew_aggr}  interval={self.requote_interval}s"
        )

        # Let the SSE stream populate initial orderbooks
        time.sleep(2)

        # Seed positions from the API so we don't start from zero
        self._sync_positions()

        cycle = 0
        try:
            while self._running:
                cycle += 1

                for product in self.products_to_quote:
                    if not self._running:
                        break
                    try:
                        self.requote(product)
                    except Exception as exc:
                        print(f"[ERROR] requote({product}): {exc}")

                # Periodic position reconciliation with the API
                if self.position_sync_cycles and cycle % self.position_sync_cycles == 0:
                    self._sync_positions()

                # Sleep for requote_interval, but wake early if a fill arrives
                self._fill_event.wait(timeout=self.requote_interval)
                self._fill_event.clear()

        except KeyboardInterrupt:
            pass
        finally:
            self.shutdown()

    def _sync_positions(self) -> None:
        """Fetch authoritative positions from the REST API."""
        self._limiter.wait()
        try:
            api_pos = self.get_positions()
            self.positions.update(api_pos)
            print(f"[SYNC] positions: {self.positions}")
        except Exception as exc:
            print(f"[WARN] position sync failed: {exc}")

    def shutdown(self) -> None:
        """Cancel all orders and tear down."""
        self._running = False
        print("\nShutting down -- cancelling all orders ...")

        # Best-effort cancel via REST (catches orders we may have lost track of)
        try:
            self.cancel_all_orders()
        except Exception:
            pass

        self.stop()  # kill SSE thread
        self._print_summary()

    def _print_summary(self) -> None:
        print()
        print("=" * 55)
        print("  SESSION SUMMARY")
        print("=" * 55)
        print(f"  Fills        : {len(self.fills)}")
        print(f"  Realized P&L : {self.realized_pnl:.2f}")
        print(f"  Positions    : {self.positions}")
        print("=" * 55)
