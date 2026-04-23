from dataclasses import dataclass, field

from binance_trade.builtin_strategies import BaseKlineSignalStrategy, Candle
from binance_trade.research import BacktestConfig, run_walkforward_analysis
from binance_trade.types import MarketType


def _candles(open_close_pairs: list[tuple[float, float]]) -> list[Candle]:
    result: list[Candle] = []
    for index, (open_price, close_price) in enumerate(open_close_pairs):
        result.append(
            Candle(
                open_time=index * 60_000,
                close_time=(index + 1) * 60_000 - 1,
                open=open_price,
                high=max(open_price, close_price) + 0.5,
                low=min(open_price, close_price) - 0.5,
                close=close_price,
                volume=1000.0,
                quote_volume=100_000.0,
                trade_count=100,
            )
        )
    return result


@dataclass(slots=True)
class IndexedSignalStrategy(BaseKlineSignalStrategy):
    signal_schedule: dict[int, int] = field(default_factory=dict)

    def required_bars(self) -> int:
        return 3

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        direction = self.signal_schedule.get(len(candles) - 1)
        if direction is None:
            return None
        return direction, f"signal {direction}"


def test_walkforward_analysis_builds_multiple_folds() -> None:
    candles = _candles([(100 + i, 100 + i + (1 if i % 3 else -1)) for i in range(18)])
    strategy = IndexedSignalStrategy(
        symbol="BTCUSDT",
        interval="1m",
        trade_side="both",
        signal_schedule={2: 1, 4: -1, 6: 1, 8: -1, 10: 1, 12: -1, 14: 1, 16: -1},
    )
    report = run_walkforward_analysis(
        strategy,
        candles,
        BacktestConfig(market_type=MarketType.SPOT, initial_capital=1000.0, fee_bps=0.0, slippage_bps=0.0),
        train_bars=8,
        test_bars=4,
        step_bars=2,
    )

    assert report["fold_count"] == 4
    assert len(report["folds"]) == 4
    assert "avg_test_return_pct" in report["summary"]
