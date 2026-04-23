from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal

from binance_trade.strategy_runtime import StopStrategy, StrategyContext, StrategyEvent


@dataclass(slots=True)
class SpotMeanReversionStrategy:
    symbol: str = "BTCUSDT"
    lookback: int = 20
    threshold_pct: Decimal = Decimal("0.002")
    quote_order_qty: Decimal = Decimal("25")
    _prices: deque[Decimal] = field(init=False, repr=False)
    needs_user_stream: bool = False

    def __post_init__(self) -> None:
        self.threshold_pct = Decimal(str(self.threshold_pct))
        self.quote_order_qty = Decimal(str(self.quote_order_qty))
        self._prices = deque(maxlen=self.lookback)

    def market_streams(self) -> list[str]:
        return [f"{self.symbol.lower()}@miniTicker"]

    async def on_start(self, ctx: StrategyContext):
        return None

    async def on_market_event(self, ctx: StrategyContext, event: StrategyEvent):
        close_price = event.payload.get("data", {}).get("c")
        if close_price is None:
            return None

        price = Decimal(str(close_price))
        self._prices.append(price)
        if len(self._prices) < self.lookback:
            return None

        average = sum(self._prices, Decimal("0")) / Decimal(len(self._prices))
        if price <= average * (Decimal("1") - self.threshold_pct):
            return [
                ctx.market_buy(self.symbol, quote_order_qty=self.quote_order_qty),
                StopStrategy("signal fired"),
            ]
        return None

    async def on_user_event(self, ctx: StrategyContext, event: StrategyEvent):
        return None


def create_strategy(**kwargs):
    return SpotMeanReversionStrategy(**kwargs)
