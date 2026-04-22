from decimal import Decimal

from binance_trade.filters import parse_symbol_rules
from binance_trade.types import OrderRequest, OrderSide, OrderType, TimeInForce


def _sample_symbol() -> dict:
    return {
        "symbol": "BTCUSDT",
        "status": "TRADING",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "orderTypes": ["LIMIT", "MARKET"],
        "quoteOrderQtyMarketAllowed": True,
        "filters": [
            {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "100.00000000", "stepSize": "0.00001000"},
            {"filterType": "MARKET_LOT_SIZE", "minQty": "0.00000000", "maxQty": "10.00000000", "stepSize": "0.00001000"},
            {"filterType": "MIN_NOTIONAL", "minNotional": "5.00000000", "applyToMarket": True},
            {
                "filterType": "NOTIONAL",
                "minNotional": "5.00000000",
                "maxNotional": "1000000.00000000",
                "applyMinToMarket": True,
                "applyMaxToMarket": True,
            },
            {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 200},
        ],
    }


def test_symbol_rules_adjust_limit_order_to_exchange_grid() -> None:
    rules = parse_symbol_rules(_sample_symbol())
    order = OrderRequest(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.00123456"),
        price=Decimal("63123.4567"),
        time_in_force=TimeInForce.GTC,
    )
    adjusted_quantity = rules.adjust_quantity(order.quantity or Decimal("0"), market=False)
    adjusted_price = rules.adjust_price(order.price or Decimal("0"))
    assert adjusted_quantity == Decimal("0.00123")
    assert adjusted_price == Decimal("63123.45")


def test_symbol_rules_validate_market_buy_notional() -> None:
    rules = parse_symbol_rules(_sample_symbol())
    order = OrderRequest(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quote_order_qty=Decimal("2"),
    )
    reasons = rules.validate(order)
    assert any("MIN_NOTIONAL" in reason for reason in reasons)
