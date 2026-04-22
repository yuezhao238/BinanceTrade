from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from .types import SubmissionMode

if TYPE_CHECKING:
    from .service import TradingService


@dataclass(slots=True)
class DipBuyStrategy:
    symbol: str
    quote_order_qty: Decimal = Decimal("25")
    lookback: int = 30
    trigger_pct: Decimal = Decimal("0.003")

    async def run(self, service: "TradingService", *, submission_mode: SubmissionMode) -> dict[str, Any]:
        prices: deque[Decimal] = deque(maxlen=self.lookback)
        async for message in service.market_messages(self.symbol, "miniTicker"):
            data = message.get("data", {})
            close_price = data.get("c")
            if close_price is None:
                continue

            price = Decimal(str(close_price))
            prices.append(price)
            if len(prices) < self.lookback:
                continue

            rolling_average = sum(prices, Decimal("0")) / Decimal(len(prices))
            trigger_price = rolling_average * (Decimal("1") - self.trigger_pct)
            if price <= trigger_price:
                return await service.buy_market(
                    self.symbol,
                    quote_order_qty=self.quote_order_qty,
                    submission_mode=submission_mode,
                )

        raise RuntimeError("market stream ended before the strategy fired")
