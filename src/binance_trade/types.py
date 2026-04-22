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


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class TimeInForce(str, Enum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"


class SubmissionMode(str, Enum):
    DRY_RUN = "DRY_RUN"
    TEST = "TEST"
    LIVE = "LIVE"


@dataclass(slots=True)
class OrderRequest:
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal | None = None
    quote_order_qty: Decimal | None = None
    price: Decimal | None = None
    time_in_force: TimeInForce | None = None
    new_client_order_id: str | None = None
    new_order_resp_type: str | None = "FULL"
    metadata: dict[str, Any] = field(default_factory=dict)

    def normalized(self) -> "OrderRequest":
        return OrderRequest(
            symbol=self.symbol.upper(),
            side=OrderSide(self.side),
            order_type=OrderType(self.order_type),
            quantity=self.quantity,
            quote_order_qty=self.quote_order_qty,
            price=self.price,
            time_in_force=self.time_in_force,
            new_client_order_id=self.new_client_order_id,
            new_order_resp_type=self.new_order_resp_type,
            metadata=dict(self.metadata),
        )

    def validate(self) -> None:
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

        if self.quantity is not None and self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.quote_order_qty is not None and self.quote_order_qty <= 0:
            raise ValueError("quote_order_qty must be positive")
        if self.price is not None and self.price <= 0:
            raise ValueError("price must be positive")

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
        params.update(self.metadata)
        return params
