from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .config import Settings
from .filters import SymbolRules
from .state import SQLiteStateStore
from .types import OrderRequest


@dataclass(slots=True)
class RiskDecision:
    allowed: bool
    reasons: list[str]
    estimated_notional: Decimal | None


class RiskGate:
    def __init__(self, settings: Settings, state_store: SQLiteStateStore) -> None:
        self.settings = settings
        self.state_store = state_store

    def evaluate(
        self,
        order: OrderRequest,
        rules: SymbolRules,
        *,
        reference_price: Decimal | None = None,
    ) -> RiskDecision:
        reasons: list[str] = []
        reasons.extend(rules.validate(order, reference_price=reference_price))

        if self.settings.allowed_symbols and order.symbol not in self.settings.allowed_symbols:
            reasons.append(f"symbol {order.symbol} is not in ALLOWED_SYMBOLS")

        estimated_notional = rules.estimate_notional(order, reference_price)
        max_order_notional = Decimal(str(self.settings.max_order_notional))
        if estimated_notional is not None and estimated_notional > max_order_notional:
            reasons.append(f"notional {estimated_notional} exceeds MAX_ORDER_NOTIONAL {max_order_notional}")

        open_orders = self.state_store.count_open_orders(order.symbol)
        if open_orders >= self.settings.max_open_orders_per_symbol:
            reasons.append(
                f"open order count {open_orders} exceeds MAX_OPEN_ORDERS_PER_SYMBOL {self.settings.max_open_orders_per_symbol}"
            )

        last_update = self.state_store.last_order_update(order.symbol)
        if self.settings.order_cooldown_seconds > 0 and last_update:
            last_timestamp = datetime.fromisoformat(last_update)
            elapsed = (datetime.now(last_timestamp.tzinfo) - last_timestamp).total_seconds()
            if elapsed < self.settings.order_cooldown_seconds:
                reasons.append(
                    f"cooldown active: {elapsed:.2f}s elapsed, need {self.settings.order_cooldown_seconds}s"
                )

        return RiskDecision(
            allowed=not reasons,
            reasons=reasons,
            estimated_notional=estimated_notional,
        )
