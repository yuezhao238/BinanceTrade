from dataclasses import dataclass, field

from binance_trade.builtin_strategies import BaseKlineSignalStrategy, Candle
from binance_trade.research import BacktestConfig, interval_to_minutes, run_backtest
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


def test_interval_to_minutes_supports_common_crypto_bars() -> None:
    assert interval_to_minutes("15m") == 15
    assert interval_to_minutes("4h") == 240
    assert interval_to_minutes("1d") == 1440


def test_spot_backtest_enters_on_next_open_and_exits_flat() -> None:
    candles = _candles(
        [
            (100, 100),
            (101, 101),
            (102, 102),
            (103, 104),
            (105, 106),
            (107, 108),
        ]
    )
    strategy = IndexedSignalStrategy(
        symbol="BTCUSDT",
        interval="1m",
        trade_side="both",
        signal_schedule={2: 1, 4: -1},
    )

    result = run_backtest(
        strategy,
        candles,
        BacktestConfig(market_type=MarketType.SPOT, initial_capital=1000.0, fee_bps=0.0, slippage_bps=0.0),
    )

    assert result["metrics"]["trade_count"] == 1
    assert result["trades"][0]["entry_price"] == 103
    assert result["trades"][0]["exit_price"] == 107
    assert result["metrics"]["final_equity"] > 1000


def test_futures_backtest_can_reverse_long_and_short() -> None:
    candles = _candles(
        [
            (100, 100),
            (101, 101),
            (102, 102),
            (103, 104),
            (105, 106),
            (104, 103),
            (102, 101),
            (103, 104),
        ]
    )
    strategy = IndexedSignalStrategy(
        symbol="BTCUSDT",
        interval="1m",
        trade_side="both",
        signal_schedule={2: 1, 4: -1, 6: 1},
    )

    result = run_backtest(
        strategy,
        candles,
        BacktestConfig(market_type=MarketType.FUTURES, initial_capital=1000.0, fee_bps=0.0, slippage_bps=0.0),
    )

    assert result["metrics"]["trade_count"] == 3
    assert [trade["side"] for trade in result["trades"]] == ["LONG", "SHORT", "LONG"]
    assert result["metrics"]["final_equity"] > 1000
