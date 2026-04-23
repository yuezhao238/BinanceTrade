from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .strategy_runtime import StopStrategy, StrategyContext, StrategyEvent
from .types import MarketType


@dataclass(slots=True)
class DipBuyStrategy:
    symbol: str
    quote_order_qty: Decimal | None = Decimal("25")
    quantity: Decimal | None = None
    lookback: int = 30
    trigger_pct: Decimal = Decimal("0.003")
    market_type: MarketType = MarketType.SPOT
    _prices: deque[Decimal] = field(init=False, repr=False)
    needs_user_stream: bool = False

    def __post_init__(self) -> None:
        if self.quote_order_qty is not None:
            self.quote_order_qty = Decimal(str(self.quote_order_qty))
        if self.quantity is not None:
            self.quantity = Decimal(str(self.quantity))
        self.trigger_pct = Decimal(str(self.trigger_pct))
        self._prices = deque(maxlen=self.lookback)

    def market_streams(self) -> list[str]:
        return [f"{self.symbol.lower()}@miniTicker"]

    async def on_start(self, ctx: StrategyContext) -> None:
        ctx.logger.info("strategy started symbol=%s market=%s", self.symbol, ctx.service.market_type.value)
        return None

    async def on_market_event(self, ctx: StrategyContext, event: StrategyEvent) -> Any:
        data = event.payload.get("data", {})
        close_price = data.get("c")
        if close_price is None:
            return None

        price = Decimal(str(close_price))
        self._prices.append(price)
        if len(self._prices) < self.lookback:
            return None

        rolling_average = sum(self._prices, Decimal("0")) / Decimal(len(self._prices))
        trigger_price = rolling_average * (Decimal("1") - self.trigger_pct)
        if price > trigger_price:
            return None

        if ctx.service.market_type is MarketType.SPOT:
            if self.quote_order_qty is None:
                raise ValueError("spot demo strategy requires quote_order_qty")
            order = ctx.market_buy(self.symbol, quote_order_qty=self.quote_order_qty)
        else:
            if self.quantity is None:
                raise ValueError("futures demo strategy requires quantity")
            order = ctx.market_buy(self.symbol, quantity=self.quantity)

        return [order, StopStrategy(f"triggered below rolling average at {price}")]

    async def on_user_event(self, ctx: StrategyContext, event: StrategyEvent) -> None:
        return None
