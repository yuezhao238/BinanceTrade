from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .filters import SymbolRules
from .state import SQLiteStateStore
from .types import MarketType, OrderRequest


@dataclass(slots=True)
class RiskProfile:
    market_type: MarketType
    allowed_symbols: list[str]
    max_order_notional: Decimal
    max_open_orders_per_symbol: int
    order_cooldown_seconds: int


@dataclass(slots=True)
class RiskDecision:
    allowed: bool
    reasons: list[str]
    estimated_notional: Decimal | None


class RiskGate:
    def __init__(self, profile: RiskProfile, state_store: SQLiteStateStore) -> None:
        self.profile = profile
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

        if self.profile.allowed_symbols and order.symbol not in self.profile.allowed_symbols:
            reasons.append(f"symbol {order.symbol} is not in ALLOWED_SYMBOLS")

        estimated_notional = rules.estimate_notional(order, reference_price)
        risk_reducing = bool(order.reduce_only or order.close_position)
        if not risk_reducing and estimated_notional is not None and self.profile.max_order_notional > 0 and estimated_notional > self.profile.max_order_notional:
            reasons.append(f"notional {estimated_notional} exceeds MAX_ORDER_NOTIONAL {self.profile.max_order_notional}")

        open_orders = self.state_store.count_open_orders(order.symbol, self.profile.market_type)
        if not risk_reducing and open_orders >= self.profile.max_open_orders_per_symbol:
            reasons.append(
                f"open order count {open_orders} exceeds MAX_OPEN_ORDERS_PER_SYMBOL {self.profile.max_open_orders_per_symbol}"
            )

        last_update = self.state_store.last_order_update(order.symbol, self.profile.market_type)
        if not risk_reducing and self.profile.order_cooldown_seconds > 0 and last_update:
            last_timestamp = datetime.fromisoformat(last_update)
            elapsed = (datetime.now(last_timestamp.tzinfo) - last_timestamp).total_seconds()
            if elapsed < self.profile.order_cooldown_seconds:
                reasons.append(
                    f"cooldown active: {elapsed:.2f}s elapsed, need {self.profile.order_cooldown_seconds}s"
                )

        return RiskDecision(
            allowed=not reasons,
            reasons=reasons,
            estimated_notional=estimated_notional,
        )
