from decimal import Decimal
from pathlib import Path

from binance_trade.config import Settings
from binance_trade.filters import parse_symbol_rules
from binance_trade.risk import RiskGate
from binance_trade.state import SQLiteStateStore
from binance_trade.types import OrderRequest, OrderSide, OrderType


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
    gate = RiskGate(settings, state)
    order = OrderRequest(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quote_order_qty=Decimal("60"),
    )
    decision = gate.evaluate(order, parse_symbol_rules(_rules()))
    assert not decision.allowed
    assert any("MAX_ORDER_NOTIONAL" in reason for reason in decision.reasons)
