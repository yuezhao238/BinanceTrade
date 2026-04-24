from __future__ import annotations

import asyncio
import json
import logging
import signal
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Protocol

from .config import Settings
from .state import SQLiteStateStore
from .strategy_runtime import StrategyRunner, StrategyProtocol, load_strategy
from .types import MarketType, SubmissionMode
from .utils import json_dumps, utc_now_iso

LOGGER = logging.getLogger(__name__)


class ServiceFactory(Protocol):
    def __call__(self) -> Any:
        ...


StrategyLoader = Callable[[str, dict[str, Any] | None], StrategyProtocol]


def is_runtime_status_healthy(payload: dict[str, Any], *, now: datetime | None = None) -> bool:
    status = str(payload.get("status", "")).upper()
    if status not in {"STARTING", "RUNNING", "RESTARTING"}:
        return False
    heartbeat_text = payload.get("last_heartbeat_at")
    if not heartbeat_text:
        return False
    stale_after_seconds = int(payload.get("stale_after_seconds", 90))
    current_time = now or datetime.now(UTC)
    heartbeat_time = datetime.fromisoformat(str(heartbeat_text))
    age_seconds = (current_time - heartbeat_time).total_seconds()
    return age_seconds <= stale_after_seconds


def is_runtime_stack_status_healthy(payload: dict[str, Any], *, now: datetime | None = None) -> bool:
    status = str(payload.get("status", "")).upper()
    if status not in {"STARTING", "RUNNING"}:
        return False
    heartbeat_text = payload.get("last_heartbeat_at")
    if not heartbeat_text:
        return False
    stale_after_seconds = int(payload.get("stale_after_seconds", 90))
    current_time = now or datetime.now(UTC)
    heartbeat_time = datetime.fromisoformat(str(heartbeat_text))
    age_seconds = (current_time - heartbeat_time).total_seconds()
    if age_seconds > stale_after_seconds:
        return False
    return int(payload.get("healthy_profile_count", 0)) >= int(payload.get("profile_count", 0))


class StrategyDaemon:
    def __init__(
        self,
        *,
        settings: Settings,
        profile: Any,
        service_factory: ServiceFactory,
        strategy_loader: StrategyLoader = load_strategy,
        state_store: SQLiteStateStore | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        self.settings = settings
        self.profile = profile
        self.service_factory = service_factory
        self.strategy_loader = strategy_loader
        self.state_store = state_store or SQLiteStateStore(settings.state_db_path)
        self.install_signal_handlers = install_signal_handlers
        self.stop_event = asyncio.Event()
        self._last_reconcile_summary: dict[str, Any] | None = None
        self._last_error: str | None = None

    async def run(self) -> dict[str, Any]:
        if self.install_signal_handlers:
            self._install_signal_handlers()

        restart_count = 0
        restart_delay = self.profile.daemon.restart_initial_delay_seconds
        while not self.stop_event.is_set():
            run_id = self.state_store.start_runtime_session(
                service_name=self.profile.name,
                market_type=self.profile.market,
                strategy_ref=self.profile.strategy_ref,
                submission_mode=self.profile.resolve_submission_mode(None, default_dry_run=self.settings.dry_run),
                profile=self.profile.to_dict(),
                restart_count=restart_count,
                stale_after_seconds=self.profile.daemon.stale_after_seconds,
            )
            try:
                result = await self._run_once(run_id=run_id, restart_count=restart_count)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = str(exc)
                summary = self._build_summary(
                    status="FAILED",
                    run_id=run_id,
                    restart_count=restart_count,
                    reason="daemon cycle failed",
                    error_text=str(exc),
                )
                self.state_store.update_runtime_status(
                    service_name=self.profile.name,
                    run_id=run_id,
                    market_type=self.profile.market,
                    strategy_ref=self.profile.strategy_ref,
                    submission_mode=self.profile.resolve_submission_mode(None, default_dry_run=self.settings.dry_run),
                    status="FAILED",
                    restart_count=restart_count,
                    stale_after_seconds=self.profile.daemon.stale_after_seconds,
                    summary=summary,
                )
                self.state_store.stop_runtime_session(
                    run_id=run_id,
                    service_name=self.profile.name,
                    status="FAILED",
                    reason="daemon cycle failed",
                    error_text=str(exc),
                    summary=summary,
                )
                self._write_status_file(summary)
                self.state_store.record_event(
                    market_type=self.profile.market,
                    channel="runtime",
                    event_type="daemon_failed",
                    symbol=self.profile.params.get("symbol"),
                    payload={"service_name": self.profile.name, "run_id": run_id, "error": str(exc)},
                )
                if not self.profile.daemon.auto_restart or self.stop_event.is_set():
                    raise
                await self._transition_to_restart(run_id=run_id, restart_count=restart_count, delay_seconds=restart_delay, reason=str(exc))
                restart_count += 1
                restart_delay = min(restart_delay * 2, self.profile.daemon.restart_max_delay_seconds)
                continue

            summary = result["summary"]
            final_status = result["status"]
            self.state_store.stop_runtime_session(
                run_id=run_id,
                service_name=self.profile.name,
                status=final_status,
                reason=result.get("reason"),
                error_text=result.get("error"),
                summary=summary,
            )
            if not result.get("restartable", False):
                return summary

            if not self.profile.daemon.auto_restart or self.profile.daemon.stop_on_strategy_exit:
                return summary

            await self._transition_to_restart(
                run_id=run_id,
                restart_count=restart_count,
                delay_seconds=restart_delay,
                reason=result.get("reason") or "strategy exited",
            )
            restart_count += 1
            restart_delay = min(restart_delay * 2, self.profile.daemon.restart_max_delay_seconds)

        return self._build_summary(
            status="STOPPED",
            run_id="",
            restart_count=restart_count,
            reason="shutdown requested before cycle start",
        )

    def request_stop(self) -> None:
        self.stop_event.set()

    async def _run_once(self, *, run_id: str, restart_count: int) -> dict[str, Any]:
        submission_mode = self.profile.resolve_submission_mode(None, default_dry_run=self.settings.dry_run)
        async with self.service_factory() as service:
            strategy = self.strategy_loader(self.profile.strategy_ref, dict(self.profile.params))
            runner = StrategyRunner(service=service, strategy=strategy, submission_mode=submission_mode)

            restored = self._restore_runtime_snapshot(strategy, runner)
            if restored is not None:
                self.state_store.record_event(
                    market_type=self.profile.market,
                    channel="runtime",
                    event_type="state_restored",
                    symbol=self.profile.params.get("symbol"),
                    payload={"service_name": self.profile.name, "run_id": run_id},
                )

            startup_summary: dict[str, Any] = {"restored": restored}
            if self.profile.daemon.reconcile_on_start and getattr(service, "authenticator", None):
                reconcile_symbol = self.profile.params.get("symbol")
                reconcile_payload = await service.reconcile(reconcile_symbol)
                self._last_reconcile_summary = _summarize_reconcile_payload(reconcile_payload)
                startup_summary["startup_reconcile"] = self._last_reconcile_summary

            self._persist_runtime_snapshot(
                run_id=run_id,
                runner=runner,
                strategy=strategy,
                status="RUNNING",
                restart_count=restart_count,
                extra_summary=startup_summary,
            )

            runner_task = asyncio.create_task(runner.run(), name=f"{self.profile.name}-runner")
            heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(run_id=run_id, runner=runner, strategy=strategy, restart_count=restart_count),
                name=f"{self.profile.name}-heartbeat",
            )
            reconcile_task = None
            if self.profile.daemon.reconcile_interval_seconds > 0 and getattr(service, "authenticator", None):
                reconcile_task = asyncio.create_task(
                    self._reconcile_loop(service=service, run_id=run_id, runner=runner, strategy=strategy, restart_count=restart_count),
                    name=f"{self.profile.name}-reconcile",
                )
            stop_task = asyncio.create_task(self.stop_event.wait(), name=f"{self.profile.name}-stop")

            watched_tasks = {runner_task, heartbeat_task, stop_task}
            if reconcile_task is not None:
                watched_tasks.add(reconcile_task)

            done, pending = await asyncio.wait(watched_tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            for task in pending:
                with suppress(asyncio.CancelledError):
                    await task

            if stop_task in done and self.stop_event.is_set():
                if not runner_task.done():
                    runner_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await runner_task
                summary = self._build_summary(
                    status="STOPPED",
                    run_id=run_id,
                    restart_count=restart_count,
                    reason="shutdown requested",
                    runner=runner,
                )
                self._persist_runtime_snapshot(
                    run_id=run_id,
                    runner=runner,
                    strategy=strategy,
                    status="STOPPED",
                    restart_count=restart_count,
                    extra_summary=summary,
                )
                return {"status": "STOPPED", "reason": "shutdown requested", "summary": summary, "restartable": False}

            if heartbeat_task in done:
                exc = heartbeat_task.exception()
                raise RuntimeError("heartbeat task stopped unexpectedly") if exc is None else exc

            if reconcile_task is not None and reconcile_task in done:
                exc = reconcile_task.exception()
                raise RuntimeError("reconcile task stopped unexpectedly") if exc is None else exc

            result = await runner_task
            summary = self._build_summary(
                status="STOPPED",
                run_id=run_id,
                restart_count=restart_count,
                reason=result.get("reason"),
                runner=runner,
                extra=result,
            )
            self._persist_runtime_snapshot(
                run_id=run_id,
                runner=runner,
                strategy=strategy,
                status="STOPPED",
                restart_count=restart_count,
                extra_summary=summary,
            )
            return {
                "status": "STOPPED",
                "reason": result.get("reason"),
                "summary": summary,
                "restartable": True,
            }

    async def _heartbeat_loop(
        self,
        *,
        run_id: str,
        runner: StrategyRunner,
        strategy: StrategyProtocol,
        restart_count: int,
    ) -> None:
        while True:
            await asyncio.sleep(self.profile.daemon.heartbeat_interval_seconds)
            self._persist_runtime_snapshot(
                run_id=run_id,
                runner=runner,
                strategy=strategy,
                status="RUNNING",
                restart_count=restart_count,
            )

    async def _reconcile_loop(
        self,
        *,
        service: Any,
        run_id: str,
        runner: StrategyRunner,
        strategy: StrategyProtocol,
        restart_count: int,
    ) -> None:
        reconcile_symbol = self.profile.params.get("symbol")
        while True:
            await asyncio.sleep(self.profile.daemon.reconcile_interval_seconds)
            payload = await service.reconcile(reconcile_symbol)
            self._last_reconcile_summary = _summarize_reconcile_payload(payload)
            self.state_store.record_event(
                market_type=self.profile.market,
                channel="runtime",
                event_type="reconcile",
                symbol=reconcile_symbol,
                payload={"service_name": self.profile.name, "run_id": run_id, "summary": self._last_reconcile_summary},
            )
            self._persist_runtime_snapshot(
                run_id=run_id,
                runner=runner,
                strategy=strategy,
                status="RUNNING",
                restart_count=restart_count,
            )

    async def _transition_to_restart(self, *, run_id: str, restart_count: int, delay_seconds: int, reason: str) -> None:
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
        self.state_store.record_event(
            market_type=self.profile.market,
            channel="runtime",
            event_type="restarting",
            symbol=self.profile.params.get("symbol"),
            payload={"service_name": self.profile.name, "run_id": run_id, "reason": reason, "delay_seconds": delay_seconds},
        )
        self._write_status_file(summary)
        await asyncio.sleep(delay_seconds)

    def _persist_runtime_snapshot(
        self,
        *,
        run_id: str,
        runner: StrategyRunner,
        strategy: StrategyProtocol,
        status: str,
        restart_count: int,
        extra_summary: dict[str, Any] | None = None,
    ) -> None:
        summary = self._build_summary(
            status=status,
            run_id=run_id,
            restart_count=restart_count,
            runner=runner,
            extra=extra_summary,
        )
        self.state_store.update_runtime_status(
            service_name=self.profile.name,
            run_id=run_id,
            market_type=self.profile.market,
            strategy_ref=self.profile.strategy_ref,
            submission_mode=runner.ctx.submission_mode,
            status=status,
            restart_count=restart_count,
            stale_after_seconds=self.profile.daemon.stale_after_seconds,
            summary=summary,
            ctx_state=_safe_json_dict(runner.ctx.state),
            strategy_state=self._snapshot_strategy(strategy),
        )
        self._write_status_file(summary)

    def _build_summary(
        self,
        *,
        status: str,
        run_id: str,
        restart_count: int,
        reason: str | None = None,
        error_text: str | None = None,
        runner: StrategyRunner | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "service_name": self.profile.name,
            "status": status,
            "market": self.profile.market.value,
            "strategy_ref": self.profile.strategy_ref,
            "run_id": run_id,
            "restart_count": restart_count,
            "profile_path": None if self.profile.path is None else str(self.profile.path),
            "submission_mode": self.profile.resolve_submission_mode(None, default_dry_run=self.settings.dry_run).value,
            "updated_at": utc_now_iso(),
        }
        if reason:
            payload["reason"] = reason
        if error_text:
            payload["error"] = error_text
        if self._last_error and "error" not in payload:
            payload["error"] = self._last_error
        if self._last_reconcile_summary is not None:
            payload["last_reconcile"] = self._last_reconcile_summary
        if runner is not None:
            payload["actions"] = runner.action_count
            if "last_order_result" in runner.ctx.state:
                payload["last_order_result"] = _safe_json_value(runner.ctx.state["last_order_result"])
        if extra:
            payload.update(_safe_json_dict(extra))
        return payload

    def _restore_runtime_snapshot(self, strategy: StrategyProtocol, runner: StrategyRunner) -> dict[str, Any] | None:
        snapshot = self.state_store.get_runtime_status(self.profile.name)
        if snapshot is None:
            return None
        ctx_state = snapshot.get("ctx_state")
        if isinstance(ctx_state, dict):
            runner.ctx.state.update(ctx_state)
        strategy_state = snapshot.get("strategy_state")
        if strategy_state is not None and hasattr(strategy, "restore_state"):
            restore_state = getattr(strategy, "restore_state")
            restore_state(strategy_state)
        return snapshot

    def _snapshot_strategy(self, strategy: StrategyProtocol) -> dict[str, Any] | None:
        if not hasattr(strategy, "snapshot_state"):
            return None
        snapshot = getattr(strategy, "snapshot_state")()
        if snapshot is None:
            return None
        if not isinstance(snapshot, dict):
            raise ValueError("strategy snapshot_state() must return a dict or None")
        return _safe_json_dict(snapshot)

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.request_stop)

    def _write_status_file(self, payload: dict[str, Any]) -> None:
        self.settings.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._status_file_path().write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")

    def _status_file_path(self) -> Path:
        return self.settings.runtime_dir / f"{self.profile.name}.json"


class StrategyDaemonStack:
    def __init__(
        self,
        *,
        settings: Settings,
        stack: Any,
        service_factory_resolver: Callable[[Any], ServiceFactory],
        strategy_loader: StrategyLoader = load_strategy,
        state_store: SQLiteStateStore | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        self.settings = settings
        self.stack = stack
        self.service_factory_resolver = service_factory_resolver
        self.strategy_loader = strategy_loader
        self.state_store = state_store or SQLiteStateStore(settings.state_db_path)
        self.install_signal_handlers = install_signal_handlers
        self.stop_event = asyncio.Event()

    async def run(self) -> dict[str, Any]:
        if self.install_signal_handlers:
            self._install_signal_handlers()

        run_id = self.state_store.start_runtime_stack_session(
            stack_name=self.stack.name,
            profile_count=len(self.stack.profiles),
            stale_after_seconds=self.stack.settings.stale_after_seconds,
            config=self.stack.to_dict(),
        )
        daemons = {
            profile.name: StrategyDaemon(
                settings=self.settings,
                profile=profile,
                service_factory=self.service_factory_resolver(profile),
                strategy_loader=self.strategy_loader,
                state_store=self.state_store,
                install_signal_handlers=False,
            )
            for profile in self.stack.profiles
        }
        member_results: dict[str, dict[str, Any]] = {}
        tasks = {name: asyncio.create_task(daemon.run(), name=f"{name}-stack-member") for name, daemon in daemons.items()}
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(run_id=run_id, member_results=member_results), name=f"{self.stack.name}-heartbeat")
        stop_task = asyncio.create_task(self.stop_event.wait(), name=f"{self.stack.name}-stop")

        try:
            self._persist_stack_status(run_id=run_id, status="RUNNING", member_results=member_results)
            while tasks:
                watched = set(tasks.values()) | {heartbeat_task, stop_task}
                done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)

                if stop_task in done and self.stop_event.is_set():
                    await self._stop_member_tasks(daemons, tasks)
                    summary = self._build_stack_summary(
                        run_id=run_id,
                        status="STOPPED",
                        member_results=member_results,
                        reason="shutdown requested",
                    )
                    self.state_store.stop_runtime_stack_session(
                        stack_name=self.stack.name,
                        run_id=run_id,
                        status="STOPPED",
                        reason="shutdown requested",
                        summary=summary,
                    )
                    self._write_status_file(summary)
                    return summary

                if heartbeat_task in done:
                    exc = heartbeat_task.exception()
                    raise RuntimeError("stack heartbeat stopped unexpectedly") if exc is None else exc

                completed_members = [name for name, task in tasks.items() if task in done]
                for name in completed_members:
                    task = tasks.pop(name)
                    exc = task.exception()
                    if exc is not None:
                        member_results[name] = {"status": "FAILED", "error": str(exc)}
                        self._persist_stack_status(run_id=run_id, status="FAILED", member_results=member_results, error_text=str(exc))
                        if self.stack.settings.stop_on_member_failure:
                            await self._stop_member_tasks(daemons, tasks)
                            summary = self._build_stack_summary(
                                run_id=run_id,
                                status="FAILED",
                                member_results=member_results,
                                reason=f"member failed: {name}",
                                error_text=str(exc),
                            )
                            self.state_store.stop_runtime_stack_session(
                                stack_name=self.stack.name,
                                run_id=run_id,
                                status="FAILED",
                                reason=f"member failed: {name}",
                                error_text=str(exc),
                                summary=summary,
                            )
                            self._write_status_file(summary)
                            raise RuntimeError(f"runtime stack {self.stack.name} failed because {name} failed: {exc}") from exc
                    else:
                        result = task.result()
                        member_results[name] = result
                        self._persist_stack_status(run_id=run_id, status="RUNNING", member_results=member_results)
                        if self.stack.settings.stop_on_member_exit:
                            await self._stop_member_tasks(daemons, tasks)
                            summary = self._build_stack_summary(
                                run_id=run_id,
                                status="STOPPED",
                                member_results=member_results,
                                reason=f"member exited: {name}",
                            )
                            self.state_store.stop_runtime_stack_session(
                                stack_name=self.stack.name,
                                run_id=run_id,
                                status="STOPPED",
                                reason=f"member exited: {name}",
                                summary=summary,
                            )
                            self._write_status_file(summary)
                            return summary

                if tasks:
                    self._persist_stack_status(run_id=run_id, status="RUNNING", member_results=member_results)

            summary = self._build_stack_summary(
                run_id=run_id,
                status="STOPPED",
                member_results=member_results,
                reason="all members exited",
            )
            self.state_store.stop_runtime_stack_session(
                stack_name=self.stack.name,
                run_id=run_id,
                status="STOPPED",
                reason="all members exited",
                summary=summary,
            )
            self._write_status_file(summary)
            return summary
        finally:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task

    def request_stop(self) -> None:
        self.stop_event.set()

    async def _heartbeat_loop(self, *, run_id: str, member_results: dict[str, dict[str, Any]]) -> None:
        while True:
            await asyncio.sleep(self.stack.settings.heartbeat_interval_seconds)
            self._persist_stack_status(run_id=run_id, status="RUNNING", member_results=member_results)

    async def _stop_member_tasks(self, daemons: dict[str, StrategyDaemon], tasks: dict[str, asyncio.Task[dict[str, Any]]]) -> None:
        for daemon in daemons.values():
            daemon.request_stop()
        if not tasks:
            return
        done, pending = await asyncio.wait(set(tasks.values()), timeout=5)
        for task in done:
            with suppress(asyncio.CancelledError):
                await task
        for task in pending:
            task.cancel()
        for task in pending:
            with suppress(asyncio.CancelledError):
                await task

    def _persist_stack_status(
        self,
        *,
        run_id: str,
        status: str,
        member_results: dict[str, dict[str, Any]],
        reason: str | None = None,
        error_text: str | None = None,
    ) -> None:
        summary = self._build_stack_summary(
            run_id=run_id,
            status=status,
            member_results=member_results,
            reason=reason,
            error_text=error_text,
        )
        healthy_profile_count = sum(1 for item in summary["members"].values() if item.get("healthy"))
        self.state_store.update_runtime_stack_status(
            stack_name=self.stack.name,
            run_id=run_id,
            status=status,
            profile_count=len(self.stack.profiles),
            healthy_profile_count=healthy_profile_count,
            stale_after_seconds=self.stack.settings.stale_after_seconds,
            summary=summary,
        )
        self._write_status_file(summary)

    def _build_stack_summary(
        self,
        *,
        run_id: str,
        status: str,
        member_results: dict[str, dict[str, Any]],
        reason: str | None = None,
        error_text: str | None = None,
    ) -> dict[str, Any]:
        members: dict[str, Any] = {}
        healthy_profile_count = 0
        for profile in self.stack.profiles:
            runtime_status = self.state_store.get_runtime_status(profile.name)
            member_payload: dict[str, Any] = {
                "market": profile.market.value,
                "profile_path": None if profile.path is None else str(profile.path),
                "strategy_ref": profile.strategy_ref,
            }
            if runtime_status is not None:
                member_payload.update(runtime_status)
                member_payload["healthy"] = is_runtime_status_healthy(runtime_status)
            else:
                member_payload["status"] = "UNKNOWN"
                member_payload["healthy"] = False
            if profile.name in member_results:
                member_payload["result"] = _safe_json_value(member_results[profile.name])
            if member_payload["healthy"]:
                healthy_profile_count += 1
            members[profile.name] = member_payload

        payload: dict[str, Any] = {
            "stack_name": self.stack.name,
            "status": status,
            "run_id": run_id,
            "profile_count": len(self.stack.profiles),
            "healthy_profile_count": healthy_profile_count,
            "stack_path": None if self.stack.path is None else str(self.stack.path),
            "updated_at": utc_now_iso(),
            "members": members,
        }
        if reason:
            payload["reason"] = reason
        if error_text:
            payload["error"] = error_text
        return payload

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.request_stop)

    def _write_status_file(self, payload: dict[str, Any]) -> None:
        self.settings.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._status_file_path().write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")

    def _status_file_path(self) -> Path:
        return self.settings.runtime_dir / f"stack-{self.stack.name}.json"


def _summarize_reconcile_payload(payload: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    if "open_orders" in payload:
        summary["open_order_count"] = len(payload.get("open_orders", []))
    if "positions" in payload:
        summary["position_count"] = len(payload.get("positions", []))
    if not summary:
        summary["keys"] = sorted(payload)
    summary["updated_at"] = utc_now_iso()
    return summary


def _safe_json_dict(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    return {str(key): _safe_json_value(value) for key, value in payload.items()}


def _safe_json_value(value: Any) -> Any:
    try:
        return _round_trip_json(value)
    except TypeError:
        if isinstance(value, dict):
            return {str(key): _safe_json_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_safe_json_value(item) for item in value]
        return str(value)


def _round_trip_json(value: Any) -> Any:
    import json

    return json.loads(json_dumps(value))
