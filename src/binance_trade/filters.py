from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any, Mapping

from .types import MarketType, OrderRequest, OrderType


def _to_decimal(value: str | int | float | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _snap(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _is_multiple(value: Decimal, step: Decimal) -> bool:
    if step == 0:
        return True
    return _snap(value, step) == value


@dataclass(slots=True)
class QuantityFilter:
    min_qty: Decimal
    max_qty: Decimal
    step_size: Decimal

    def adjust(self, value: Decimal) -> Decimal:
        return _snap(value, self.step_size)

    def validate(self, value: Decimal) -> list[str]:
        reasons: list[str] = []
        if value < self.min_qty:
            reasons.append(f"quantity {value} is below minQty {self.min_qty}")
        if self.max_qty > 0 and value > self.max_qty:
            reasons.append(f"quantity {value} is above maxQty {self.max_qty}")
        if not _is_multiple(value, self.step_size):
            reasons.append(f"quantity {value} is not aligned to stepSize {self.step_size}")
        return reasons


@dataclass(slots=True)
class PriceFilter:
    min_price: Decimal
    max_price: Decimal
    tick_size: Decimal

    def adjust(self, value: Decimal) -> Decimal:
        return _snap(value, self.tick_size)

    def validate(self, value: Decimal) -> list[str]:
        reasons: list[str] = []
        if self.min_price > 0 and value < self.min_price:
            reasons.append(f"price {value} is below minPrice {self.min_price}")
        if self.max_price > 0 and value > self.max_price:
            reasons.append(f"price {value} is above maxPrice {self.max_price}")
        if not _is_multiple(value, self.tick_size):
            reasons.append(f"price {value} is not aligned to tickSize {self.tick_size}")
        return reasons


@dataclass(slots=True)
class MinNotionalFilter:
    min_notional: Decimal
    apply_to_market: bool


@dataclass(slots=True)
class NotionalFilter:
    min_notional: Decimal
    max_notional: Decimal
    apply_min_to_market: bool
    apply_max_to_market: bool


@dataclass(slots=True)
class PercentPriceFilter:
    multiplier_up: Decimal
    multiplier_down: Decimal
    multiplier_decimal: int | None = None


@dataclass(slots=True)
class SymbolRules:
    symbol: str
    market_type: MarketType
    status: str
    base_asset: str
    quote_asset: str
    margin_asset: str | None
    contract_type: str | None
    order_types: set[str]
    time_in_force_values: set[str]
    quote_order_qty_market_allowed: bool
    price_filter: PriceFilter | None
    lot_size: QuantityFilter | None
    market_lot_size: QuantityFilter | None
    min_notional: MinNotionalFilter | None
    notional: NotionalFilter | None
    percent_price: PercentPriceFilter | None
    max_num_orders: int | None
    max_position: Decimal | None
    trigger_protect: Decimal | None

    def adjust_price(self, price: Decimal) -> Decimal:
        if not self.price_filter:
            return price
        return self.price_filter.adjust(price)

    def adjust_quantity(self, quantity: Decimal, *, market: bool) -> Decimal:
        chosen_filter = self.market_lot_size if market and self.market_lot_size else self.lot_size
        if not chosen_filter:
            return quantity
        return chosen_filter.adjust(quantity)

    def estimate_notional(self, order: OrderRequest, reference_price: Decimal | None) -> Decimal | None:
        if order.quote_order_qty is not None:
            return order.quote_order_qty
        if order.price is not None and order.quantity is not None:
            return order.price * order.quantity
        if order.quantity is not None and reference_price is not None:
            return order.quantity * reference_price
        return None

    def validate(self, order: OrderRequest, reference_price: Decimal | None = None) -> list[str]:
        reasons: list[str] = []
        if self.status != "TRADING":
            reasons.append(f"symbol {self.symbol} is not TRADING")
        if order.order_type.value not in self.order_types:
            reasons.append(f"order type {order.order_type.value} is not enabled on {self.symbol}")
        if order.time_in_force is not None and self.time_in_force_values and order.time_in_force.value not in self.time_in_force_values:
            reasons.append(f"timeInForce {order.time_in_force.value} is not enabled on {self.symbol}")
        if order.quote_order_qty is not None and order.order_type is OrderType.MARKET and not self.quote_order_qty_market_allowed:
            reasons.append(f"symbol {self.symbol} does not allow quoteOrderQty market orders")

        if order.price is not None and self.price_filter:
            reasons.extend(self.price_filter.validate(order.price))

        quantity_filter = self.market_lot_size if order.order_type is OrderType.MARKET and self.market_lot_size else self.lot_size
        if order.quantity is not None and quantity_filter:
            reasons.extend(quantity_filter.validate(order.quantity))

        notional = self.estimate_notional(order, reference_price)
        if notional is not None:
            if self.min_notional and (order.order_type is not OrderType.MARKET or self.min_notional.apply_to_market):
                if notional < self.min_notional.min_notional:
                    reasons.append(f"notional {notional} is below MIN_NOTIONAL {self.min_notional.min_notional}")

            if self.notional:
                if (order.order_type is not OrderType.MARKET or self.notional.apply_min_to_market) and notional < self.notional.min_notional:
                    reasons.append(f"notional {notional} is below NOTIONAL min {self.notional.min_notional}")
                if self.notional.max_notional > 0 and (order.order_type is not OrderType.MARKET or self.notional.apply_max_to_market) and notional > self.notional.max_notional:
                    reasons.append(f"notional {notional} is above NOTIONAL max {self.notional.max_notional}")

        return reasons

    def summary(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "market_type": self.market_type.value,
            "status": self.status,
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "margin_asset": self.margin_asset,
            "contract_type": self.contract_type,
            "order_types": sorted(self.order_types),
            "time_in_force": sorted(self.time_in_force_values),
            "quote_order_qty_market_allowed": self.quote_order_qty_market_allowed,
            "price_filter": None if not self.price_filter else {
                "minPrice": str(self.price_filter.min_price),
                "maxPrice": str(self.price_filter.max_price),
                "tickSize": str(self.price_filter.tick_size),
            },
            "lot_size": None if not self.lot_size else {
                "minQty": str(self.lot_size.min_qty),
                "maxQty": str(self.lot_size.max_qty),
                "stepSize": str(self.lot_size.step_size),
            },
            "market_lot_size": None if not self.market_lot_size else {
                "minQty": str(self.market_lot_size.min_qty),
                "maxQty": str(self.market_lot_size.max_qty),
                "stepSize": str(self.market_lot_size.step_size),
            },
            "min_notional": None if not self.min_notional else {
                "minNotional": str(self.min_notional.min_notional),
                "applyToMarket": self.min_notional.apply_to_market,
            },
            "notional": None if not self.notional else {
                "minNotional": str(self.notional.min_notional),
                "maxNotional": str(self.notional.max_notional),
                "applyMinToMarket": self.notional.apply_min_to_market,
                "applyMaxToMarket": self.notional.apply_max_to_market,
            },
            "percent_price": None if not self.percent_price else {
                "multiplierUp": str(self.percent_price.multiplier_up),
                "multiplierDown": str(self.percent_price.multiplier_down),
                "multiplierDecimal": self.percent_price.multiplier_decimal,
            },
            "max_num_orders": self.max_num_orders,
            "max_position": None if self.max_position is None else str(self.max_position),
            "trigger_protect": None if self.trigger_protect is None else str(self.trigger_protect),
        }


def parse_symbol_rules(symbol_info: Mapping[str, Any], market_type: MarketType = MarketType.SPOT) -> SymbolRules:
    filters = {item["filterType"]: item for item in symbol_info.get("filters", [])}

    price_filter = filters.get("PRICE_FILTER")
    lot_size = filters.get("LOT_SIZE")
    market_lot_size = filters.get("MARKET_LOT_SIZE")
    min_notional = filters.get("MIN_NOTIONAL")
    notional = filters.get("NOTIONAL")
    percent_price = filters.get("PERCENT_PRICE")
    max_num_orders = filters.get("MAX_NUM_ORDERS")
    max_position = filters.get("MAX_POSITION")

    return SymbolRules(
        symbol=str(symbol_info["symbol"]).upper(),
        market_type=market_type,
        status=str(symbol_info.get("status", "")),
        base_asset=str(symbol_info.get("baseAsset", "")),
        quote_asset=str(symbol_info.get("quoteAsset", "")),
        margin_asset=None if symbol_info.get("marginAsset") is None else str(symbol_info.get("marginAsset")),
        contract_type=None if symbol_info.get("contractType") is None else str(symbol_info.get("contractType")),
        order_types=set(symbol_info.get("orderTypes", [])),
        time_in_force_values=set(symbol_info.get("timeInForce", [])),
        quote_order_qty_market_allowed=bool(symbol_info.get("quoteOrderQtyMarketAllowed", False)),
        price_filter=None if not price_filter else PriceFilter(
            min_price=_to_decimal(price_filter["minPrice"]) or Decimal("0"),
            max_price=_to_decimal(price_filter["maxPrice"]) or Decimal("0"),
            tick_size=_to_decimal(price_filter["tickSize"]) or Decimal("0"),
        ),
        lot_size=None if not lot_size else QuantityFilter(
            min_qty=_to_decimal(lot_size["minQty"]) or Decimal("0"),
            max_qty=_to_decimal(lot_size["maxQty"]) or Decimal("0"),
            step_size=_to_decimal(lot_size["stepSize"]) or Decimal("0"),
        ),
        market_lot_size=None if not market_lot_size else QuantityFilter(
            min_qty=_to_decimal(market_lot_size["minQty"]) or Decimal("0"),
            max_qty=_to_decimal(market_lot_size["maxQty"]) or Decimal("0"),
            step_size=_to_decimal(market_lot_size["stepSize"]) or Decimal("0"),
        ),
        min_notional=None if not min_notional else MinNotionalFilter(
            min_notional=_to_decimal(min_notional.get("minNotional", min_notional.get("notional"))) or Decimal("0"),
            apply_to_market=bool(min_notional.get("applyToMarket", False)),
        ),
        notional=None if not notional else NotionalFilter(
            min_notional=_to_decimal(notional["minNotional"]) or Decimal("0"),
            max_notional=_to_decimal(notional["maxNotional"]) or Decimal("0"),
            apply_min_to_market=bool(notional.get("applyMinToMarket", False)),
            apply_max_to_market=bool(notional.get("applyMaxToMarket", False)),
        ),
        percent_price=None if not percent_price else PercentPriceFilter(
            multiplier_up=_to_decimal(percent_price["multiplierUp"]) or Decimal("0"),
            multiplier_down=_to_decimal(percent_price["multiplierDown"]) or Decimal("0"),
            multiplier_decimal=None if percent_price.get("multiplierDecimal") is None else int(percent_price["multiplierDecimal"]),
        ),
        max_num_orders=None if not max_num_orders else int(max_num_orders.get("maxNumOrders", max_num_orders.get("limit", 0))),
        max_position=_to_decimal(max_position["maxPosition"]) if max_position else None,
        trigger_protect=_to_decimal(symbol_info.get("triggerProtect")),
    )
