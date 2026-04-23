from pathlib import Path

from binance_trade.runtime_profiles import load_runtime_profile


def test_load_runtime_profile_from_toml(tmp_path: Path) -> None:
    profile_path = tmp_path / "spot.toml"
    profile_path.write_text(
        """
name = "spot-ema"
market = "spot"
strategy_ref = "examples/strategies/spot_ema_persistent.py:create_strategy"
submission_mode = "dry_run"
description = "Persistent profile."
notes = ["one", "two"]

[params]
symbol = "BTCUSDT"
interval = "1h"

[daemon]
reconcile_on_start = true
reconcile_interval_seconds = 120
heartbeat_interval_seconds = 15
auto_restart = true
restart_initial_delay_seconds = 3
restart_max_delay_seconds = 30
stop_on_strategy_exit = false
stale_after_seconds = 45
""",
        encoding="utf-8",
    )

    profile = load_runtime_profile(profile_path)

    assert profile.name == "spot-ema"
    assert profile.market.value == "spot"
    assert profile.strategy_ref.endswith(":create_strategy")
    assert profile.submission_mode is not None
    assert profile.params["symbol"] == "BTCUSDT"
    assert profile.daemon.heartbeat_interval_seconds == 15
    assert profile.daemon.stale_after_seconds == 45
