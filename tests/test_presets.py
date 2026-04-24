from binance_trade.presets import get_preset, list_presets


def test_research_presets_are_available() -> None:
    presets = list_presets()
    assert len(presets) >= 5
    assert any(item["name"] == "global_spot_ema_btc_15m" for item in presets)


def test_get_preset_returns_expected_market_and_strategy() -> None:
    preset = get_preset("global_futures_dmi_btc_15m")
    assert preset.market.value == "futures"
    assert preset.strategy_name == "dmi_adx_trend"


def test_old_binance_us_preset_name_resolves_to_global_alias() -> None:
    preset = get_preset("binance_us_spot_ema_btc_15m")
    assert preset.name == "global_spot_ema_btc_15m"
