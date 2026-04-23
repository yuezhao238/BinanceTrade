from decimal import Decimal
from pathlib import Path

from binance_trade.config import Settings
from binance_trade.filters import parse_symbol_rules
from binance_trade.risk import RiskGate, RiskProfile
from binance_trade.state import SQLiteStateStore
from binance_trade.types import MarketType, OrderRequest, OrderSide, OrderType


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        STATE_DB_PATH=tmp_path / "state.db",
        ALLOWED_SYMBOLS="BTCUSDT",
        MAX_ORDER_NOTIONAL=50,
        MAX_OPEN_ORDERS_PER_SYMBOL=5,
        ORDER_COOLDOWN_SECONDS=0,
    )


def _rules() -> dict:
    return {
        "symbol": "BTCUSDT",
        "status": "TRADING",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "orderTypes": ["LIMIT", "MARKET"],
        "quoteOrderQtyMarketAllowed": True,
        "filters": [
            {"filterType": "LOT_SIZE", "minQty": "0.00001", "maxQty": "100", "stepSize": "0.00001"},
            {"filterType": "MARKET_LOT_SIZE", "minQty": "0", "maxQty": "100", "stepSize": "0.00001"},
            {"filterType": "MIN_NOTIONAL", "minNotional": "5", "applyToMarket": True},
        ],
    }


def test_risk_gate_rejects_notional_over_limit(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    state = SQLiteStateStore(settings.state_db_path)
    gate = RiskGate(
        RiskProfile(
            market_type=MarketType.SPOT,
            allowed_symbols=settings.allowed_symbols,
            max_order_notional=Decimal(str(settings.max_order_notional)),
            max_open_orders_per_symbol=settings.max_open_orders_per_symbol,
            order_cooldown_seconds=settings.order_cooldown_seconds,
        ),
        state,
    )
    order = OrderRequest(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quote_order_qty=Decimal("60"),
    )
    decision = gate.evaluate(order, parse_symbol_rules(_rules()))
    assert not decision.allowed
    assert any("MAX_ORDER_NOTIONAL" in reason for reason in decision.reasons)


def test_reduce_only_order_bypasses_max_notional(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    state = SQLiteStateStore(settings.state_db_path)
    gate = RiskGate(
        RiskProfile(
            market_type=MarketType.FUTURES,
            allowed_symbols=["BTCUSDT"],
            max_order_notional=Decimal("10"),
            max_open_orders_per_symbol=1,
            order_cooldown_seconds=60,
        ),
        state,
    )
    futures_rules = parse_symbol_rules(
        {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "orderTypes": ["LIMIT", "MARKET"],
            "timeInForce": ["GTC", "IOC", "FOK", "GTX"],
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.1", "maxPrice": "1000000", "tickSize": "0.1"},
                {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
                {"filterType": "MIN_NOTIONAL", "notional": "5"},
                {"filterType": "MAX_NUM_ORDERS", "limit": 200},
            ],
        },
        market_type=MarketType.FUTURES,
    )
    order = OrderRequest(
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        market_type=MarketType.FUTURES,
        quantity=Decimal("1"),
        reduce_only=True,
    )
    decision = gate.evaluate(order, futures_rules, reference_price=Decimal("50000"))
    assert decision.allowed
