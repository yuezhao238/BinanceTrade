from decimal import Decimal

from binance_trade.filters import parse_symbol_rules
from binance_trade.types import MarketType, OrderRequest, OrderSide, OrderType, PositionSide, TimeInForce


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


def test_parse_futures_symbol_rules_uses_notional_and_limit_keys() -> None:
    rules = parse_symbol_rules(
        {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "contractType": "PERPETUAL",
            "orderTypes": ["LIMIT", "MARKET", "STOP"],
            "timeInForce": ["GTC", "IOC", "FOK", "GTX"],
            "triggerProtect": "0.15",
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.1", "maxPrice": "1000000", "tickSize": "0.1"},
                {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
                {"filterType": "MIN_NOTIONAL", "notional": "5"},
                {"filterType": "MAX_NUM_ORDERS", "limit": 200},
                {"filterType": "PERCENT_PRICE", "multiplierUp": "1.15", "multiplierDown": "0.85", "multiplierDecimal": "4"},
            ],
        },
        market_type=MarketType.FUTURES,
    )
    assert rules.market_type is MarketType.FUTURES
    assert rules.min_notional is not None and rules.min_notional.min_notional == Decimal("5")
    assert rules.max_num_orders == 200
    assert rules.trigger_protect == Decimal("0.15")


def test_futures_order_validation_rejects_quote_order_qty_and_reduce_only_in_hedge_mode() -> None:
    invalid_quote = OrderRequest(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        market_type=MarketType.FUTURES,
        quote_order_qty=Decimal("25"),
        quantity=Decimal("0.001"),
    )
    try:
        invalid_quote.validate()
    except ValueError as exc:
        assert "do not support quote_order_qty" in str(exc)
    else:
        raise AssertionError("expected quote_order_qty validation failure")

    invalid_reduce_only = OrderRequest(
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        market_type=MarketType.FUTURES,
        quantity=Decimal("0.001"),
        position_side=PositionSide.SHORT,
        reduce_only=True,
    )
    try:
        invalid_reduce_only.validate()
    except ValueError as exc:
        assert "reduce_only cannot be sent" in str(exc)
    else:
        raise AssertionError("expected reduce_only validation failure")
