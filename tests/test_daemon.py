import asyncio
from decimal import Decimal
from pathlib import Path

import pytest

from binance_trade.config import Settings
from binance_trade.daemon import StrategyDaemon, is_runtime_status_healthy
from binance_trade.runtime_profiles import DaemonSettings, RuntimeProfile
from binance_trade.state import SQLiteStateStore
from binance_trade.strategy_runtime import StopStrategy, StrategyContext, StrategyEvent
from binance_trade.types import MarketType, OrderRequest, SubmissionMode


class FakeService:
    market_type = MarketType.SPOT
    authenticator = None

    def __init__(self, orders: list[OrderRequest]) -> None:
        self.orders = orders

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def submit_order(self, order: OrderRequest, *, submission_mode: SubmissionMode):
        self.orders.append(order)
        return {"status": submission_mode.value, "clientOrderId": order.new_client_order_id or "cid-1"}

    async def get_klines(self, symbol: str, interval: str, *, limit: int = 500, start_time=None, end_time=None):
        return []

    async def raw_market_messages(self, streams: list[str], *, reconnect: bool = True):
        yield {"stream": streams[0], "data": {"c": "100"}}

    async def user_messages(self, *, reconnect: bool = True):
        if False:
            yield {}

    async def reconcile(self, symbol: str | None = None):
        return {"open_orders": []}


class OneShotStrategy:
    needs_user_stream = False

    def market_streams(self) -> list[str]:
        return ["btcusdt@miniTicker"]

    async def on_start(self, ctx: StrategyContext):
        return None

    async def on_market_event(self, ctx: StrategyContext, event: StrategyEvent):
        return [ctx.market_buy("BTCUSDT", quote_order_qty=Decimal("25")), StopStrategy("done")]

    async def on_user_event(self, ctx: StrategyContext, event: StrategyEvent):
        return None


class CrashOnStartStrategy:
    needs_user_stream = False

    def market_streams(self) -> list[str]:
        return ["btcusdt@miniTicker"]

    async def on_start(self, ctx: StrategyContext):
        raise RuntimeError("boom")

    async def on_market_event(self, ctx: StrategyContext, event: StrategyEvent):
        return None

    async def on_user_event(self, ctx: StrategyContext, event: StrategyEvent):
        return None


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        STATE_DB_PATH=tmp_path / "state.db",
        RUNTIME_DIR=tmp_path / "runtime",
        DRY_RUN=True,
        BINANCE_ENV="binance_us",
    )


def test_daemon_persists_runtime_status_on_clean_stop(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStateStore(settings.state_db_path)
    orders: list[OrderRequest] = []
    profile = RuntimeProfile(
        name="spot-oneshot",
        market=MarketType.SPOT,
        strategy_ref="ignored",
        daemon=DaemonSettings(auto_restart=False, stop_on_strategy_exit=True),
    )

    daemon = StrategyDaemon(
        settings=settings,
        profile=profile,
        service_factory=lambda: FakeService(orders),
        strategy_loader=lambda strategy_ref, params=None: OneShotStrategy(),
        state_store=store,
    )

    result = asyncio.run(daemon.run())

    assert result["status"] == "STOPPED"
    assert len(orders) == 1
    status = store.get_runtime_status("spot-oneshot")
    assert status is not None
    assert status["status"] == "STOPPED"
    assert status["summary"]["actions"] == 1
    assert (settings.runtime_dir / "spot-oneshot.json").exists()


def test_daemon_restarts_after_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStateStore(settings.state_db_path)
    orders: list[OrderRequest] = []
    profile = RuntimeProfile(
        name="spot-restart",
        market=MarketType.SPOT,
        strategy_ref="ignored",
        daemon=DaemonSettings(
            auto_restart=True,
            stop_on_strategy_exit=True,
            restart_initial_delay_seconds=1,
            restart_max_delay_seconds=1,
        ),
    )

    calls = {"count": 0}

    def _loader(strategy_ref: str, params=None):
        if calls["count"] == 0:
            calls["count"] += 1
            return CrashOnStartStrategy()
        return OneShotStrategy()

    async def _skip_restart_wait(self, *, run_id: str, restart_count: int, delay_seconds: int, reason: str) -> None:
        summary = self._build_summary(
            status="RESTARTING",
            run_id=run_id,
            restart_count=restart_count,
            reason=reason,
            extra={"next_restart_delay_seconds": delay_seconds},
        )
        self.state_store.update_runtime_status(
            service_name=self.profile.name,
            run_id=run_id,
            market_type=self.profile.market,
            strategy_ref=self.profile.strategy_ref,
            submission_mode=self.profile.resolve_submission_mode(None, default_dry_run=self.settings.dry_run),
            status="RESTARTING",
            restart_count=restart_count,
            stale_after_seconds=self.profile.daemon.stale_after_seconds,
            summary=summary,
        )

    monkeypatch.setattr(StrategyDaemon, "_transition_to_restart", _skip_restart_wait)

    daemon = StrategyDaemon(
        settings=settings,
        profile=profile,
        service_factory=lambda: FakeService(orders),
        strategy_loader=_loader,
        state_store=store,
    )

    result = asyncio.run(daemon.run())

    assert result["status"] == "STOPPED"
    assert result["restart_count"] == 1
    assert len(orders) == 1
    status = store.get_runtime_status("spot-restart")
    assert status is not None
    assert status["status"] == "STOPPED"
    assert status["restart_count"] == 1


def test_runtime_health_is_false_for_stopped_payload() -> None:
    payload = {
        "status": "STOPPED",
        "last_heartbeat_at": "2026-04-24T00:00:00+00:00",
        "stale_after_seconds": 30,
    }

    assert is_runtime_status_healthy(payload) is False
