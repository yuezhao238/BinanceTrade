from pathlib import Path

from binance_trade.runtime_profiles import load_runtime_profile, load_runtime_stack


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


def test_load_runtime_stack_from_toml(tmp_path: Path) -> None:
    (tmp_path / "btc.toml").write_text(
        """
name = "btc"
market = "spot"
strategy_ref = "examples/strategies/spot_ema_persistent.py:create_strategy"
[params]
symbol = "BTCUSDT"
""",
        encoding="utf-8",
    )
    (tmp_path / "eth.toml").write_text(
        """
name = "eth"
market = "spot"
strategy_ref = "examples/strategies/spot_ema_persistent.py:create_strategy"
[params]
symbol = "ETHUSDT"
""",
        encoding="utf-8",
    )
    stack_path = tmp_path / "stack.toml"
    stack_path.write_text(
        """
name = "core"
profiles = ["btc.toml", "eth.toml"]
[settings]
heartbeat_interval_seconds = 20
stale_after_seconds = 60
stop_on_member_exit = false
stop_on_member_failure = true
""",
        encoding="utf-8",
    )

    stack = load_runtime_stack(stack_path)

    assert stack.name == "core"
    assert len(stack.profiles) == 2
    assert stack.profiles[0].name == "btc"
    assert stack.profiles[1].params["symbol"] == "ETHUSDT"
    assert stack.settings.stop_on_member_exit is False
