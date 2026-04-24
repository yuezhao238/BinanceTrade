import json
from decimal import Decimal
from pathlib import Path

from binance_trade.config import Settings
from binance_trade.dashboard import DashboardConfig, DashboardControlPlane, DashboardDataService, render_dashboard_html
from binance_trade.state import SQLiteStateStore
from binance_trade.types import MarketType, OrderRequest, OrderSide, OrderType, SubmissionMode


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        BINANCE_ENV="mainnet",
        STATE_DB_PATH=tmp_path / "state.db",
        RUNTIME_DIR=tmp_path / "runtime",
        DRY_RUN=True,
    )


def test_dashboard_snapshot_aggregates_runtime_state(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStateStore(settings.state_db_path)

    run_id = store.start_runtime_session(
        service_name="spot-btc",
        market_type=MarketType.SPOT,
        strategy_ref="examples/strategies/spot_ema_persistent.py:create_strategy",
        submission_mode=SubmissionMode.DRY_RUN,
        profile={"name": "spot-btc"},
        restart_count=0,
        stale_after_seconds=90,
    )
    store.update_runtime_status(
        service_name="spot-btc",
        run_id=run_id,
        market_type=MarketType.SPOT,
        strategy_ref="examples/strategies/spot_ema_persistent.py:create_strategy",
        submission_mode=SubmissionMode.DRY_RUN,
        status="RUNNING",
        restart_count=0,
        stale_after_seconds=90,
        summary={"actions": 2},
        ctx_state={"position_qty": "0.001"},
        strategy_state={
            "symbol": "BTCUSDT",
            "interval": "1h",
            "fast_period": 20,
            "slow_period": 50,
            "candles": [
                {"close_time": 1, "high": "1.2", "low": "0.9", "close": "1.0"},
                {"close_time": 2, "high": "1.3", "low": "1.0", "close": "1.1"},
                {"close_time": 3, "high": "1.4", "low": "1.1", "close": "1.2"},
            ],
        },
    )

    stack_run_id = store.start_runtime_stack_session(
        stack_name="spot-core",
        profile_count=1,
        stale_after_seconds=90,
        config={"name": "spot-core"},
    )
    store.update_runtime_stack_status(
        stack_name="spot-core",
        run_id=stack_run_id,
        status="RUNNING",
        profile_count=1,
        healthy_profile_count=1,
        stale_after_seconds=90,
        summary={
            "members": {
                "spot-btc": {
                    "status": "RUNNING",
                    "healthy": True,
                    "restart_count": 0,
                    "updated_at": "2026-04-24T00:00:00+00:00",
                    "summary": {"actions": 2},
                    "strategy_state": {"candles": [1, 2, 3]},
                }
            }
        },
    )

    order = OrderRequest(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quote_order_qty=Decimal("25"),
        new_client_order_id="cid-1",
    )
    store.record_order_request(order, SubmissionMode.DRY_RUN)
    store.record_order_result("cid-1", {"status": "DRY_RUN", "clientOrderId": "cid-1"}, fallback_status="DRY_RUN")
    store.record_event(
        market_type=MarketType.SPOT,
        channel="user_stream",
        payload={"e": "executionReport", "X": "FILLED"},
        event_type="executionReport",
        symbol="BTCUSDT",
        client_order_id="cid-1",
    )

    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    (settings.runtime_dir / "spot-btc.json").write_text(
        json.dumps({"status": "RUNNING", "updated_at": "2026-04-24T00:00:00+00:00"}),
        encoding="utf-8",
    )
    control_dir = settings.runtime_dir / "control"
    control_dir.mkdir(parents=True, exist_ok=True)
    (control_dir / "research_state.json").write_text(
        json.dumps(
            {
                "summary": {"headline": "EMA is strongest", "capital": 1000, "deployable_budget": 850},
                "allocations": [{"title": "EMA Crossover", "allocation_pct": 100, "budget_amount": 850}],
                "candidates": [{"title": "EMA Crossover"}],
            }
        ),
        encoding="utf-8",
    )

    service = DashboardDataService(
        settings=settings,
        config=DashboardConfig(include_portfolio=False, order_limit=10, event_limit=10),
        state_store=store,
    )
    snapshot = service.build_snapshot()

    assert snapshot["summary"]["stack_count"] == 1
    assert snapshot["summary"]["service_count"] == 1
    assert snapshot["summary"]["order_count"] == 1
    assert snapshot["summary"]["event_count"] == 1
    assert snapshot["portfolio"]["enabled"] is False
    assert snapshot["services"][0]["strategy_state"]["candle_count"] == 3
    assert snapshot["services"][0]["strategy_label"] == "spot ema persistent"
    assert snapshot["strategy_charts"][0]["symbol"] == "BTCUSDT"
    assert snapshot["strategy_charts"][0]["series"]["close"] == [1.0, 1.1, 1.2]
    assert snapshot["strategy_charts"][0]["fast_period"] == 20
    assert snapshot["research"]["latest"]["summary"]["headline"] == "EMA is strongest"
    assert snapshot["stacks"][0]["members"][0]["service_name"] == "spot-btc"
    assert snapshot["runtime_files"][0]["name"] == "spot-btc.json"


def test_render_dashboard_html_embeds_refresh_interval() -> None:
    html = render_dashboard_html(refresh_seconds=12)

    assert "Local Ops Dashboard" in html
    assert "Live Strategy Charts" in html
    assert "Strategy Lab" in html
    assert "Earn to Spot" in html
    assert "const REFRESH_SECONDS = 12;" in html
    assert "/api/snapshot" in html


def test_dashboard_control_plane_discovers_runtime_files_and_starts_profile(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    workspace_root = tmp_path / "workspace"
    runtime_dir = workspace_root / "examples" / "runtime"
    strategy_dir = workspace_root / "examples" / "strategies"
    runtime_dir.mkdir(parents=True)
    strategy_dir.mkdir(parents=True)
    (strategy_dir / "dummy.py").write_text("def create_strategy(**kwargs):\n    return None\n", encoding="utf-8")
    (runtime_dir / "demo_profile.toml").write_text(
        """
name = "demo-profile"
market = "spot"
strategy_ref = "examples/strategies/dummy.py:create_strategy"

[params]
symbol = "BTCUSDT"
        """.strip(),
        encoding="utf-8",
    )
    (runtime_dir / "demo_stack.toml").write_text(
        """
name = "demo-stack"
profiles = ["demo_profile.toml"]
        """.strip(),
        encoding="utf-8",
    )

    control = DashboardControlPlane(settings=settings, workspace_root=workspace_root)
    described = control.describe()
    assert described["available_profiles"][0]["name"] == "demo-profile"
    assert described["available_stacks"][0]["name"] == "demo-stack"
    assert described["research_categories"][0]["key"] == "all"

    class FakeProcess:
        pid = 4242

    def fake_popen(*args, **kwargs):
        return FakeProcess()

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    result = control.handle_action({"action": "start_profile", "path": str((runtime_dir / "demo_profile.toml").resolve())})
    assert result["status"] == "STARTED"
    assert result["pid"] == 4242


def test_candidate_row_exposes_window_and_full_sample_metrics(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    control = DashboardControlPlane(settings=settings, workspace_root=tmp_path)

    row = control._candidate_row(
        {
            "name": "keltner_breakout",
            "title": "Keltner Breakout",
            "category": "volatility",
            "description": "test",
            "metrics": {
                "total_return_pct": 128.17,
                "max_drawdown_pct": 34.08,
                "profit_factor": 2.39,
                "sharpe": 0.76,
                "trade_count": 7,
                "win_rate_pct": 57.14,
                "fees_paid": 12.3,
                "turnover_multiple": 27.32,
                "exposure_pct": 55.6,
                "sample_days": 1500.0,
            },
            "trades": [
                {
                    "side": "LONG",
                    "entry_time": 2,
                    "entry_price": 96971.17,
                    "exit_time": 3,
                    "exit_price": 78741.1,
                    "return_pct": -18.8,
                    "entry_reason": "entry",
                    "exit_reason": "exit",
                    "bars_held": 1,
                }
            ],
            "equity_curve": [
                {"time": 1, "equity": 10000},
                {"time": 2, "equity": 23000},
                {"time": 3, "equity": 22817},
            ],
        },
        candle_snapshot=[
            {"close_time": 1, "high": "90000", "low": "85000", "close": "88000"},
            {"close_time": 2, "high": "98000", "low": "92000", "close": "96000"},
            {"close_time": 3, "high": "80000", "low": "77000", "close": "77602"},
        ],
    )

    assert row["metrics"]["window_return_pct"] == 128.17
    assert set(row["chart"].keys()) == {"recent", "full"}
    assert row["chart"]["recent"]["price"]["close"] == [88000.0, 96000.0, 77602.0]
    assert row["chart"]["full"]["price"]["close"] == [88000.0, 96000.0, 77602.0]
    assert "recent 3 bars" in row["ops_summary"]["window_scope"]
    assert "complete simulated path across 3 bars" in row["ops_summary"]["full_scope"]
    assert "full simulated sample" in row["ops_summary"]["metric_scope"]
