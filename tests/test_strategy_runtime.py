from decimal import Decimal
from pathlib import Path

from binance_trade.strategy_runtime import StopStrategy, StrategyContext, StrategyEvent, StrategyRunner, load_strategy
from binance_trade.types import MarketType, OrderRequest, SubmissionMode


class FakeService:
    market_type = MarketType.SPOT

    def __init__(self) -> None:
        self.orders: list[OrderRequest] = []

    async def submit_order(self, order: OrderRequest, *, submission_mode: SubmissionMode):
        self.orders.append(order)
        return {"status": submission_mode.value, "clientOrderId": order.new_client_order_id}

    async def get_klines(self, symbol: str, interval: str, *, limit: int = 500):
        return []

    async def raw_market_messages(self, streams: list[str], *, reconnect: bool = True):
        yield {"stream": streams[0], "data": {"c": "99"}}

    async def user_messages(self, *, reconnect: bool = True):
        if False:
            yield {}


class OneShotStrategy:
    needs_user_stream = False

    def market_streams(self) -> list[str]:
        return ["btcusdt@miniTicker"]

    async def on_start(self, ctx: StrategyContext):
        return None

    async def on_market_event(self, ctx: StrategyContext, event: StrategyEvent):
        return [ctx.market_buy("BTCUSDT", quote_order_qty=Decimal("25")), StopStrategy("done")]

    async def on_user_event(self, ctx: StrategyContext, event: StrategyEvent):
        return None


def test_strategy_runner_executes_returned_order_intent() -> None:
    service = FakeService()
    runner = StrategyRunner(service=service, strategy=OneShotStrategy(), submission_mode=SubmissionMode.DRY_RUN)
    result = __import__("asyncio").run(runner.run())
    assert result["status"] == "STOPPED"
    assert len(service.orders) == 1
    assert service.orders[0].quote_order_qty == Decimal("25")


def test_load_strategy_from_file_path() -> None:
    strategy = load_strategy(
        str(Path("examples/strategies/spot_mean_reversion.py")) + ":create_strategy",
        {"symbol": "ETHUSDT", "quote_order_qty": "20"},
    )
    assert strategy.market_streams() == ["ethusdt@miniTicker"]
