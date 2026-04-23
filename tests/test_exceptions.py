from binance_trade.exceptions import with_restricted_location_hint


def test_restricted_location_hint_for_global_spot_points_to_binance_us() -> None:
    message = with_restricted_location_hint(
        "Service unavailable from a restricted location according to 'b. Eligibility'.",
        environment="testnet",
        market_type="spot",
    )

    assert "BINANCE_ENV=binance_us" in message
    assert "cannot bypass exchange eligibility" in message


def test_restricted_location_hint_for_futures_mentions_spot_only_binance_us() -> None:
    message = with_restricted_location_hint(
        "Service unavailable from a restricted location according to 'b. Eligibility'.",
        environment="mainnet",
        market_type="futures",
    )

    assert "spot-only" in message
    assert "USDⓈ-M Futures" in message
