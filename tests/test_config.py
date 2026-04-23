import pytest

from binance_trade.config import Settings
from binance_trade.exceptions import ConfigError


def test_binance_us_spot_endpoints_are_resolved() -> None:
    settings = Settings(BINANCE_ENV="binance_us")

    assert settings.resolved_rest_base_url == "https://api.binance.us"
    assert settings.resolved_market_ws_url == "wss://stream.binance.us:9443/stream"
    assert settings.resolved_ws_api_url == "wss://ws-api.binance.us:443/ws-api/v3"


def test_binance_us_rejects_futures_endpoints() -> None:
    settings = Settings(BINANCE_ENV="binance_us")

    with pytest.raises(ConfigError):
        settings.assert_futures_supported()

