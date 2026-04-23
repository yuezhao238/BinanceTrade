from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

from .utils import decimal_to_str


class Environment(str, Enum):
    TESTNET = "testnet"
    MAINNET = "mainnet"


class ApiKeyType(str, Enum):
    HMAC = "HMAC"
    RSA = "RSA"
    ED25519 = "ED25519"


class MarketType(str, Enum):
    SPOT = "spot"
    FUTURES = "futures"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"
    TRAILING_STOP_MARKET = "TRAILING_STOP_MARKET"


class TimeInForce(str, Enum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    GTX = "GTX"
    GTD = "GTD"


class PositionSide(str, Enum):
    BOTH = "BOTH"
    LONG = "LONG"
    SHORT = "SHORT"


class SubmissionMode(str, Enum):
    DRY_RUN = "DRY_RUN"
    TEST = "TEST"
    LIVE = "LIVE"


@dataclass(slots=True)
class OrderRequest:
    symbol: str
    side: OrderSide
    order_type: OrderType
    market_type: MarketType = MarketType.SPOT
    quantity: Decimal | None = None
    quote_order_qty: Decimal | None = None
    price: Decimal | None = None
    time_in_force: TimeInForce | None = None
    new_client_order_id: str | None = None
    new_order_resp_type: str | None = "FULL"
    position_side: PositionSide | None = None
    reduce_only: bool | None = None
    close_position: bool | None = None
    price_match: str | None = None
    self_trade_prevention_mode: str | None = None
    good_till_date: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def normalized(self) -> "OrderRequest":
        return OrderRequest(
            symbol=self.symbol.upper(),
            side=OrderSide(self.side),
            order_type=OrderType(self.order_type),
            market_type=MarketType(self.market_type),
            quantity=self.quantity,
            quote_order_qty=self.quote_order_qty,
            price=self.price,
            time_in_force=self.time_in_force,
            new_client_order_id=self.new_client_order_id,
            new_order_resp_type=self.new_order_resp_type,
            position_side=None if self.position_side is None else PositionSide(self.position_side),
            reduce_only=self.reduce_only,
            close_position=self.close_position,
            price_match=self.price_match,
            self_trade_prevention_mode=self.self_trade_prevention_mode,
            good_till_date=self.good_till_date,
            metadata=dict(self.metadata),
        )

    def validate(self) -> None:
        if self.market_type is MarketType.SPOT:
            self._validate_spot()
        else:
            self._validate_futures()

        if self.quantity is not None and self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.quote_order_qty is not None and self.quote_order_qty <= 0:
            raise ValueError("quote_order_qty must be positive")
        if self.price is not None and self.price <= 0:
            raise ValueError("price must be positive")
        if self.good_till_date is not None and self.good_till_date <= 0:
            raise ValueError("good_till_date must be positive")

    def _validate_spot(self) -> None:
        if self.order_type is OrderType.MARKET:
            if self.price is not None:
                raise ValueError("MARKET orders must not include price")
            if self.quantity is None and self.quote_order_qty is None:
                raise ValueError("MARKET orders require quantity or quote_order_qty")
            if self.side is OrderSide.SELL and self.quote_order_qty is not None:
                raise ValueError("SELL MARKET orders must use base quantity")
            if self.time_in_force is not None:
                raise ValueError("MARKET orders must not include time_in_force")
        elif self.order_type is OrderType.LIMIT:
            if self.price is None or self.quantity is None:
                raise ValueError("LIMIT orders require price and quantity")
            if self.quote_order_qty is not None:
                raise ValueError("LIMIT orders do not support quote_order_qty")
            if self.time_in_force is None:
                raise ValueError("LIMIT orders require time_in_force")
        else:
            raise ValueError(f"SPOT order type {self.order_type.value} is not supported by this starter")

        unsupported_fields = {
            "position_side": self.position_side,
            "reduce_only": self.reduce_only,
            "close_position": self.close_position,
            "price_match": self.price_match,
            "good_till_date": self.good_till_date,
        }
        for field_name, value in unsupported_fields.items():
            if value is not None:
                raise ValueError(f"{field_name} is futures-only")

    def _validate_futures(self) -> None:
        if self.quote_order_qty is not None:
            raise ValueError("FUTURES orders do not support quote_order_qty")

        if self.order_type is OrderType.MARKET:
            if self.quantity is None:
                raise ValueError("FUTURES MARKET orders require quantity")
            if self.price is not None:
                raise ValueError("FUTURES MARKET orders must not include price")
        elif self.order_type is OrderType.LIMIT:
            if self.quantity is None or self.price is None:
                raise ValueError("FUTURES LIMIT orders require quantity and price")
            if self.time_in_force is None:
                raise ValueError("FUTURES LIMIT orders require time_in_force")
        else:
            raise ValueError(f"FUTURES order type {self.order_type.value} is not supported by this starter")

        if self.reduce_only and self.position_side in {PositionSide.LONG, PositionSide.SHORT}:
            raise ValueError("reduce_only cannot be sent in hedge mode orders with LONG or SHORT positionSide")
        if self.good_till_date is not None and self.time_in_force is not TimeInForce.GTD:
            raise ValueError("good_till_date requires time_in_force=GTD")

    def to_rest_params(self) -> dict[str, Any]:
        self.validate()
        params: dict[str, Any] = {
            "symbol": self.symbol.upper(),
            "side": self.side.value,
            "type": self.order_type.value,
        }
        if self.quantity is not None:
            params["quantity"] = decimal_to_str(self.quantity)
        if self.quote_order_qty is not None:
            params["quoteOrderQty"] = decimal_to_str(self.quote_order_qty)
        if self.price is not None:
            params["price"] = decimal_to_str(self.price)
        if self.time_in_force is not None:
            params["timeInForce"] = self.time_in_force.value
        if self.new_client_order_id:
            params["newClientOrderId"] = self.new_client_order_id
        if self.new_order_resp_type:
            params["newOrderRespType"] = self.new_order_resp_type
        if self.position_side is not None:
            params["positionSide"] = self.position_side.value
        if self.reduce_only is not None:
            params["reduceOnly"] = "true" if self.reduce_only else "false"
        if self.close_position is not None:
            params["closePosition"] = "true" if self.close_position else "false"
        if self.price_match is not None:
            params["priceMatch"] = self.price_match
        if self.self_trade_prevention_mode is not None:
            params["selfTradePreventionMode"] = self.self_trade_prevention_mode
        if self.good_till_date is not None:
            params["goodTillDate"] = self.good_till_date
        params.update(self.metadata)
        return params
