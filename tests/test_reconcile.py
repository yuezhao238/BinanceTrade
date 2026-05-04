import asyncio
from decimal import Decimal
from pathlib import Path

from binance_trade.config import Settings
from binance_trade.exceptions import BinanceAPIError
from binance_trade.service import SpotTradingService
from binance_trade.types import MarketType, OrderRequest, OrderSide, OrderType, SubmissionMode


class FakeSpotRest:
    async def get_open_orders(self, symbol=None):
        return []

    async def get_order(self, symbol, *, client_order_id=None, order_id=None):
        raise BinanceAPIError(
            http_status=400,
            code=-2013,
            message="Order does not exist.",
        )


class FakeSubmitFailureRest:
    async def get_symbol_rules(self, symbol):
        from binance_trade.filters import parse_symbol_rules

        return parse_symbol_rules(
            {
                "symbol": symbol,
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "orderTypes": ["MARKET"],
                "quoteOrderQtyMarketAllowed": True,
                "filters": [
                    {"filterType": "LOT_SIZE", "minQty": "0.00001", "maxQty": "100", "stepSize": "0.00001"},
                    {"filterType": "MARKET_LOT_SIZE", "minQty": "0", "maxQty": "100", "stepSize": "0.00001"},
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5", "applyToMarket": True},
                ],
            }
        )

    async def get_price(self, symbol):
        return Decimal("100000")

    async def place_order(self, order):
        raise BinanceAPIError(http_status=400, code=-1021, message="Timestamp outside recvWindow")


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        BINANCE_ENV="mainnet",
        STATE_DB_PATH=tmp_path / "state.db",
        RUNTIME_DIR=tmp_path / "runtime",
        DRY_RUN=True,
    )


def test_spot_reconcile_marks_missing_local_pending_orders_terminal(tmp_path: Path) -> None:
    service = SpotTradingService(_settings(tmp_path))
    service.rest = FakeSpotRest()
    service.state.record_order_request(
        OrderRequest(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quote_order_qty=Decimal("25"),
            new_client_order_id="missing-local-order",
        ),
        SubmissionMode.LIVE,
    )

    assert service.state.count_open_orders("BTCUSDT", MarketType.SPOT) == 1

    payload = asyncio.run(service.reconcile("BTCUSDT"))

    assert payload["open_orders"] == []
    assert payload["checked_local_orders"][0]["status"] == "RECONCILED_MISSING"
    assert service.state.count_open_orders("BTCUSDT", MarketType.SPOT) == 0


def test_spot_live_submit_failure_marks_order_terminal(tmp_path: Path) -> None:
    service = SpotTradingService(_settings(tmp_path))
    service.authenticator = object()
    service.rest = FakeSubmitFailureRest()

    async def _run() -> None:
        try:
            await service.buy_market("BTCUSDT", quote_order_qty=Decimal("25"), submission_mode=SubmissionMode.LIVE)
        except BinanceAPIError:
            return
        raise AssertionError("expected BinanceAPIError")

    asyncio.run(_run())

    orders = service.state.list_orders(limit=1)
    assert orders[0]["status"] == "SUBMIT_FAILED"
    assert service.state.count_open_orders("BTCUSDT", MarketType.SPOT) == 0
