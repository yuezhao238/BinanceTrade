from binance_trade.builtin_strategies import builtin_strategy_names, create_strategy, list_strategies


def test_builtin_strategy_catalog_has_at_least_twenty_items() -> None:
    assert len(builtin_strategy_names()) >= 20
    assert len(list_strategies()) >= 20


def test_builtin_strategy_factory_creates_known_strategy() -> None:
    strategy = create_strategy(name="sma_crossover", symbol="ethusdt", fast_period=5, slow_period=10)
    assert strategy.symbol == "ETHUSDT"
    assert strategy.market_streams() == ["ethusdt@kline_1m"]
