from binance_trade.builtin_strategies import Candle, builtin_strategy_names, create_strategy, list_strategies


def test_builtin_strategy_catalog_has_at_least_twenty_five_items() -> None:
    assert len(builtin_strategy_names()) >= 25
    assert len(list_strategies()) >= 25


def test_builtin_strategy_factory_creates_known_strategy() -> None:
    strategy = create_strategy(name="sma_crossover", symbol="ethusdt", fast_period=5, slow_period=10)
    assert strategy.symbol == "ETHUSDT"
    assert strategy.market_streams() == ["ethusdt@kline_1m"]


def test_all_builtin_strategies_can_compute_on_standard_candles() -> None:
    candles = [
        Candle(
            open_time=index * 60_000,
            close_time=(index + 1) * 60_000 - 1,
            open=100 + (index * 0.1),
            high=101 + (index * 0.1),
            low=99 + (index * 0.1),
            close=100 + (index * 0.1) + ((index % 7) * 0.05),
            volume=1000 + (index % 11) * 50,
            quote_volume=100_000,
            trade_count=100,
        )
        for index in range(180)
    ]

    for name in builtin_strategy_names():
        strategy = create_strategy(name=name, symbol="BTCUSDT", interval="15m")
        assert strategy.required_bars() > 0
        result = strategy.compute_signal(candles)
        assert result is None or result[0] in {-1, 1}
