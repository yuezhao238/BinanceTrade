from binance_trade.presets import get_preset, list_presets


def test_research_presets_are_available() -> None:
    presets = list_presets()
    assert len(presets) >= 5
    assert any(item["name"] == "binance_us_spot_ema_btc_15m" for item in presets)


def test_get_preset_returns_expected_market_and_strategy() -> None:
    preset = get_preset("global_futures_dmi_btc_15m")
    assert preset.market.value == "futures"
    assert preset.strategy_name == "dmi_adx_trend"
