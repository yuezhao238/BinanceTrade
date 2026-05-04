from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .builtin_strategies import create_strategy as create_builtin_strategy
from .builtin_strategies import ema_series, list_strategies
from .config import Settings
from .daemon import is_runtime_stack_status_healthy, is_runtime_status_healthy
from .exceptions import BinanceTradeError
from .presets import list_presets as list_research_presets
from .research import BacktestConfig, benchmark_builtin_strategies, fetch_recent_candles, interval_to_minutes, run_backtest_with_options
from .runtime_profiles import RuntimeProfile, RuntimeStack, load_runtime_profile, load_runtime_stack
from .service import FuturesTradingService, SpotTradingService
from .state import SQLiteStateStore
from .types import MarketType, SubmissionMode
from .utils import json_dumps, utc_now_iso

LOGGER = logging.getLogger(__name__)


def _decimal_or_zero(value: Any) -> Decimal:
    if value in (None, "", False):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _truncate_json(value: Any, *, limit: int = 180) -> str:
    text = json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1]}…"


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _strategy_label(strategy_ref: str) -> str:
    ref = str(strategy_ref).strip()
    if not ref:
        return "strategy"
    stem = Path(ref.split(":", 1)[0]).stem.replace("_", " ").strip()
    return stem or ref


def _csv_list(value: Any) -> list[str]:
    if value in (None, "", False):
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _parse_manual_weights(value: Any) -> dict[str, float]:
    if value in (None, "", False):
        return {}
    text = str(value).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None
    if isinstance(parsed, dict):
        rows = parsed.items()
    else:
        rows = []
        for chunk in text.split(","):
            if "=" not in chunk:
                continue
            key, raw = chunk.split("=", 1)
            rows.append((key.strip(), raw.strip()))
    weights: dict[str, float] = {}
    for key, raw in rows:
        try:
            weight = float(str(raw).strip())
        except Exception:
            continue
        if weight > 0:
            weights[str(key).strip().lower()] = weight
    return weights


def _max_drawdown(values: list[float]) -> float:
    if not values:
        return 0.0
    peak = values[0]
    drawdown = 0.0
    for value in values:
        peak = max(peak, value)
        if peak <= 0:
            continue
        drawdown = max(drawdown, (peak - value) / peak)
    return drawdown


def _series_return_pct(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    base = float(values[0])
    if base == 0:
        return None
    return round(((float(values[-1]) / base) - 1) * 100, 4)


@dataclass(slots=True)
class DashboardConfig:
    host: str = "127.0.0.1"
    port: int = 8765
    refresh_seconds: int = 10
    portfolio_cache_seconds: int = 15
    include_portfolio: bool = True
    order_limit: int = 25
    event_limit: int = 40


class DashboardDataService:
    def __init__(
        self,
        settings: Settings,
        config: DashboardConfig,
        state_store: SQLiteStateStore | None = None,
        control_plane: "DashboardControlPlane | None" = None,
    ) -> None:
        self.settings = settings
        self.config = config
        self.state_store = state_store or SQLiteStateStore(settings.state_db_path)
        self.control_plane = control_plane
        self.control_dir = settings.runtime_dir / "control"
        self.research_state_path = self.control_dir / "research_state.json"
        self.deployment_suggestions_path = self.control_dir / "deployment_suggestions.json"
        self._portfolio_lock = threading.Lock()
        self._portfolio_cache: dict[str, Any] | None = None
        self._portfolio_cached_at = 0.0

    def invalidate_portfolio_cache(self) -> None:
        with self._portfolio_lock:
            self._portfolio_cache = None
            self._portfolio_cached_at = 0.0

    def build_snapshot(self) -> dict[str, Any]:
        raw_services = self.state_store.list_runtime_statuses()
        services = [self._service_card(item) for item in raw_services]
        strategy_charts = [chart for item in raw_services if (chart := self._strategy_chart(item)) is not None]
        stacks = [self._stack_card(item) for item in self.state_store.list_runtime_stack_statuses()]
        orders = [self._order_row(item) for item in self.state_store.list_orders(limit=self.config.order_limit)]
        events = [self._event_row(item) for item in self.state_store.list_events(limit=self.config.event_limit)]
        portfolio = self._portfolio_section() if self.config.include_portfolio else {"enabled": False}
        research = self._research_section()
        runtime_files = self._runtime_files()
        controls = None if self.control_plane is None else self.control_plane.describe()
        deployment = self._deployment_section(
            controls=controls,
            raw_services=raw_services,
            strategy_charts=strategy_charts,
            portfolio=portfolio,
            orders=orders,
            events=events,
        )
        status_log = self._status_log(
            stacks=stacks,
            services=services,
            orders=orders,
            events=events,
            runtime_files=runtime_files,
        )
        return {
            "generated_at": utc_now_iso(),
            "environment": self.settings.binance_env.value,
            "summary": {
                "stack_count": len(stacks),
                "service_count": len(services),
                "healthy_stack_count": sum(1 for item in stacks if item["healthy"]),
                "healthy_service_count": sum(1 for item in services if item["healthy"]),
                "order_count": len(orders),
                "open_order_count": sum(1 for item in orders if item["is_open"]),
                "event_count": len(events),
            },
            "stacks": stacks,
            "services": services,
            "strategy_charts": strategy_charts,
            "orders": orders,
            "events": events,
            "portfolio": portfolio,
            "research": research,
            "deployment": deployment,
            "status_log": status_log,
            "runtime_files": runtime_files,
            "controls": controls,
        }

    def _portfolio_section(self) -> dict[str, Any]:
        with self._portfolio_lock:
            now = time.time()
            if self._portfolio_cache is not None and now - self._portfolio_cached_at < self.config.portfolio_cache_seconds:
                return self._portfolio_cache

            try:
                payload = asyncio.run(self._fetch_portfolio_snapshot())
            except Exception as exc:
                payload = {
                    "ok": False,
                    "error": str(exc),
                    "fetched_at": utc_now_iso(),
                }
            self._portfolio_cache = payload
            self._portfolio_cached_at = now
            return payload

    async def _fetch_portfolio_snapshot(self) -> dict[str, Any]:
        async with SpotTradingService(self.settings) as service:
            try:
                raw = await service.portfolio_overview()
            except BinanceTradeError as exc:
                return {
                    "ok": False,
                    "error": str(exc),
                    "fetched_at": utc_now_iso(),
                }

        wallet_rows = []
        for item in raw.get("wallet_balance", []) or []:
            balance = _decimal_or_zero(item.get("balance"))
            if balance <= 0:
                continue
            wallet_rows.append(
                {
                    "wallet_name": item.get("walletName"),
                    "balance": str(balance),
                    "active": bool(item.get("activate")),
                }
            )
        wallet_rows.sort(key=lambda item: _decimal_or_zero(item["balance"]), reverse=True)

        spot_balances = []
        for item in raw.get("spot_account", {}).get("balances", []) or []:
            total = _decimal_or_zero(item.get("free")) + _decimal_or_zero(item.get("locked"))
            if total <= 0:
                continue
            spot_balances.append(
                {
                    "asset": item.get("asset"),
                    "free": item.get("free"),
                    "locked": item.get("locked"),
                    "total": str(total),
                }
            )
        spot_balances.sort(key=lambda item: _decimal_or_zero(item["total"]), reverse=True)

        earn_positions = []
        for item in raw.get("simple_earn_flexible", {}).get("positions", []) or []:
            earn_positions.append(
                {
                    "asset": item.get("asset"),
                    "total_amount": item.get("totalAmount"),
                    "apr": item.get("latestAnnualPercentageRate"),
                    "product_id": item.get("productId"),
                }
            )

        return {
            "ok": True,
            "fetched_at": utc_now_iso(),
            "wallets": wallet_rows,
            "spot_balances": spot_balances,
            "earn_positions": earn_positions,
            "raw_summary": {
                "simple_earn_account": raw.get("simple_earn_account"),
                "summary": raw.get("summary"),
            },
        }

    def _service_card(self, item: dict[str, Any]) -> dict[str, Any]:
        summary = item.get("summary") or {}
        strategy_state = item.get("strategy_state") or {}
        ctx_state = item.get("ctx_state") or {}
        preview = {key: value for key, value in strategy_state.items() if key != "candles"}
        candle_count = len(strategy_state.get("candles", [])) if isinstance(strategy_state.get("candles"), list) else None
        if candle_count is not None:
            preview["candle_count"] = candle_count
        return {
            "service_name": item["service_name"],
            "market_type": item["market_type"],
            "strategy_ref": item["strategy_ref"],
            "strategy_label": _strategy_label(item["strategy_ref"]),
            "submission_mode": item["submission_mode"],
            "status": item["status"],
            "healthy": is_runtime_status_healthy(item),
            "restart_count": item["restart_count"],
            "started_at": item["started_at"],
            "last_heartbeat_at": item["last_heartbeat_at"],
            "updated_at": item["updated_at"],
            "symbol": strategy_state.get("symbol") or ctx_state.get("symbol"),
            "interval": strategy_state.get("interval"),
            "reason": summary.get("reason"),
            "error": summary.get("error"),
            "actions": summary.get("actions"),
            "ctx_state": ctx_state,
            "strategy_state": preview,
        }

    def _strategy_chart(self, item: dict[str, Any]) -> dict[str, Any] | None:
        strategy_state = item.get("strategy_state") or {}
        raw_candles = strategy_state.get("candles")
        if not isinstance(raw_candles, list) or len(raw_candles) < 2:
            return None

        times: list[int] = []
        closes: list[float] = []
        highs: list[float] = []
        lows: list[float] = []
        for candle in raw_candles:
            if not isinstance(candle, dict):
                continue
            close_time = _int_or_none(candle.get("close_time"))
            close_value = _float_or_none(candle.get("close"))
            high_value = _float_or_none(candle.get("high"))
            low_value = _float_or_none(candle.get("low"))
            if close_time is None or close_value is None:
                continue
            times.append(close_time)
            closes.append(close_value)
            highs.append(high_value if high_value is not None else close_value)
            lows.append(low_value if low_value is not None else close_value)

        if len(closes) < 2:
            return None

        fast_period = _int_or_none(strategy_state.get("fast_period"))
        slow_period = _int_or_none(strategy_state.get("slow_period"))
        fast_series = ema_series(closes, fast_period) if fast_period and fast_period > 0 else None
        slow_series = ema_series(closes, slow_period) if slow_period and slow_period > 0 else None

        trend = None
        signal = None
        if fast_series and slow_series:
            trend = "bullish" if fast_series[-1] >= slow_series[-1] else "bearish"
            if len(fast_series) >= 2 and len(slow_series) >= 2:
                if fast_series[-2] <= slow_series[-2] and fast_series[-1] > slow_series[-1]:
                    signal = "bullish_crossover"
                elif fast_series[-2] >= slow_series[-2] and fast_series[-1] < slow_series[-1]:
                    signal = "bearish_crossover"

        position_qty = str((item.get("ctx_state") or {}).get("position_qty") or strategy_state.get("position_qty") or "0")
        pending_client_order_id = (item.get("ctx_state") or {}).get("pending_client_order_id") or strategy_state.get("pending_client_order_id")
        last_close = closes[-1]
        chart_high = max(highs + closes + ([max(fast_series)] if fast_series else []) + ([max(slow_series)] if slow_series else []))
        chart_low = min(lows + closes + ([min(fast_series)] if fast_series else []) + ([min(slow_series)] if slow_series else []))
        midpoint = (chart_high + chart_low) / 2 if chart_high != chart_low else chart_high

        return {
            "service_name": item["service_name"],
            "strategy_ref": item["strategy_ref"],
            "strategy_label": _strategy_label(item["strategy_ref"]),
            "market_type": item["market_type"],
            "submission_mode": item["submission_mode"],
            "status": item["status"],
            "healthy": is_runtime_status_healthy(item),
            "symbol": strategy_state.get("symbol") or (item.get("ctx_state") or {}).get("symbol"),
            "interval": strategy_state.get("interval"),
            "bar_count": len(closes),
            "position_qty": position_qty,
            "pending_client_order_id": pending_client_order_id,
            "fast_period": fast_period,
            "slow_period": slow_period,
            "trend": trend,
            "signal": signal,
            "last_close": last_close,
            "last_close_time": times[-1],
            "chart_min": chart_low,
            "chart_mid": midpoint,
            "chart_max": chart_high,
            "series": {
                "times": times,
                "close": closes,
                "fast_ema": fast_series,
                "slow_ema": slow_series,
            },
        }

    def _stack_card(self, item: dict[str, Any]) -> dict[str, Any]:
        summary = item.get("summary") or {}
        raw_members = summary.get("members") if isinstance(summary, dict) else None
        members = []
        if isinstance(raw_members, dict):
            for name, member in raw_members.items():
                member_summary = member.get("summary") or {}
                members.append(
                    {
                        "service_name": name,
                        "status": member.get("status"),
                        "healthy": bool(member.get("healthy")),
                        "restart_count": member.get("restart_count"),
                        "updated_at": member.get("updated_at"),
                        "error": member_summary.get("error"),
                        "reason": member_summary.get("reason"),
                    }
                )
        members.sort(key=lambda row: row["service_name"])
        return {
            "stack_name": item["stack_name"],
            "status": item["status"],
            "healthy": is_runtime_stack_status_healthy(item),
            "profile_count": item["profile_count"],
            "healthy_profile_count": item["healthy_profile_count"],
            "started_at": item["started_at"],
            "last_heartbeat_at": item["last_heartbeat_at"],
            "updated_at": item["updated_at"],
            "reason": summary.get("reason"),
            "error": summary.get("error"),
            "members": members,
        }

    def _order_row(self, item: dict[str, Any]) -> dict[str, Any]:
        status = item["status"]
        return {
            "client_order_id": item["client_order_id"],
            "symbol": item["symbol"],
            "market_type": item["market_type"],
            "side": item["side"],
            "order_type": item["order_type"],
            "status": status,
            "submission_mode": item["submission_mode"],
            "price": item["price"],
            "quantity": item["quantity"],
            "quote_order_qty": item["quote_order_qty"],
            "exchange_order_id": item["exchange_order_id"],
            "created_at": item["created_at"],
            "updated_at": item["updated_at"],
            "is_open": status in {"LOCAL_PENDING", "NEW", "PARTIALLY_FILLED", "PENDING_UNKNOWN"},
            "request_preview": _truncate_json(item.get("request")),
            "response_preview": _truncate_json(item.get("response")),
        }

    def _event_row(self, item: dict[str, Any]) -> dict[str, Any]:
        payload = item.get("payload")
        return {
            "id": item["id"],
            "market_type": item["market_type"],
            "channel": item["channel"],
            "event_type": item["event_type"],
            "symbol": item["symbol"],
            "client_order_id": item["client_order_id"],
            "created_at": item["created_at"],
            "payload_preview": _truncate_json(payload),
        }

    def _status_log(
        self,
        *,
        stacks: list[dict[str, Any]],
        services: list[dict[str, Any]],
        orders: list[dict[str, Any]],
        events: list[dict[str, Any]],
        runtime_files: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in stacks[:3]:
            severity = "ok" if item["healthy"] else "error"
            detail = f"{item['healthy_profile_count']}/{item['profile_count']} profiles healthy"
            if item.get("error") or item.get("reason"):
                detail = str(item.get("error") or item.get("reason"))
            rows.append(
                {
                    "time": item.get("updated_at"),
                    "severity": severity,
                    "source": item["stack_name"],
                    "title": f"Stack {item['status']}",
                    "detail": detail,
                }
            )
        for item in services[:6]:
            severity = "ok" if item["healthy"] else ("warn" if item["status"] == "RUNNING" else "error")
            detail = str(item.get("error") or item.get("reason") or f"restart_count={item.get('restart_count', 0)}")
            rows.append(
                {
                    "time": item.get("updated_at"),
                    "severity": severity,
                    "source": item["service_name"],
                    "title": f"{item['status']} / {item['submission_mode']}",
                    "detail": detail,
                }
            )
        for item in orders[:8]:
            status = str(item.get("status") or "")
            severity = "ok" if status == "FILLED" else ("warn" if item.get("is_open") else ("error" if status in {"LOCAL_REJECTED", "REJECTED"} else "info"))
            amount = item.get("quote_order_qty") or item.get("quantity") or "-"
            rows.append(
                {
                    "time": item.get("updated_at"),
                    "severity": severity,
                    "source": item.get("symbol"),
                    "title": f"{item.get('side')} {status}",
                    "detail": f"{amount} {item.get('order_type')} {item.get('submission_mode')}",
                }
            )
        for item in events[:8]:
            payload = item.get("payload_preview") or ""
            event_type = item.get("event_type") or item.get("channel") or "event"
            severity = "info"
            if "error" in str(payload).lower() or "rejected" in str(payload).lower():
                severity = "warn"
            rows.append(
                {
                    "time": item.get("created_at"),
                    "severity": severity,
                    "source": item.get("symbol") or item.get("channel"),
                    "title": str(event_type),
                    "detail": payload,
                }
            )
        for item in runtime_files[:4]:
            if item.get("error"):
                rows.append(
                    {
                        "time": item.get("updated_at"),
                        "severity": "warn",
                        "source": item.get("name"),
                        "title": "runtime file warning",
                        "detail": item.get("error"),
                    }
                )
        rows.sort(key=lambda row: str(row.get("time") or ""), reverse=True)
        return rows[:18]

    def _runtime_files(self) -> list[dict[str, Any]]:
        rows = []
        for path in sorted(self.settings.runtime_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                rows.append(
                    {
                        "name": path.name,
                        "size_bytes": path.stat().st_size,
                        "error": str(exc),
                    }
                )
                continue
            rows.append(
                {
                    "name": path.name,
                    "size_bytes": path.stat().st_size,
                    "status": payload.get("status"),
                    "updated_at": payload.get("updated_at"),
                    "kind": "stack" if path.name.startswith("stack-") else "service",
                }
            )
        return rows

    def _deployment_section(
        self,
        *,
        controls: dict[str, Any] | None,
        raw_services: list[dict[str, Any]],
        strategy_charts: list[dict[str, Any]],
        portfolio: dict[str, Any],
        orders: list[dict[str, Any]],
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not controls:
            return {"enabled": False}

        stack_rows = controls.get("available_stacks") or []
        if not stack_rows:
            return {"enabled": True, "stacks": [], "default_total_budget": 0.0}

        suggestion_rows = self._load_or_compute_deployment_suggestions(stack_rows)
        suggestions_by_path = {row["profile_path"]: row for row in suggestion_rows}
        runtime_by_service = {item.get("service_name"): item for item in raw_services}
        chart_by_service = {item.get("service_name"): item for item in strategy_charts}
        default_total_budget = self._deployment_default_budget(portfolio, stack_rows)
        stacks: list[dict[str, Any]] = []

        for stack in stack_rows:
            profiles = []
            generated_stack = bool(stack.get("generated"))
            current_budget_total = sum(_float_or_none(profile.get("budget_value")) or 0.0 for profile in stack.get("profiles", []))
            positive_score_total = sum(
                max(0.0, float((suggestions_by_path.get(profile["path"]) or {}).get("score", 0.0)))
                for profile in stack.get("profiles", [])
            )
            for profile in stack.get("profiles", []):
                suggestion = suggestions_by_path.get(profile["path"], {})
                raw_score = max(0.0, float(suggestion.get("score", 0.0)))
                current_budget = _float_or_none(profile.get("budget_value")) or 0.0
                if generated_stack and current_budget_total > 0:
                    suggested_weight = (current_budget / current_budget_total) * 100
                else:
                    suggested_weight = (
                        (raw_score / positive_score_total) * 100
                        if positive_score_total > 0
                        else (100 / max(len(stack.get("profiles", [])), 1))
                    )
                runtime_item = runtime_by_service.get(profile["name"])
                chart_item = chart_by_service.get(profile["name"])
                profiles.append(
                    {
                        **profile,
                        "current_budget": current_budget,
                        "suggested_weight_pct": round(suggested_weight, 2),
                        "suggested_budget": round(default_total_budget * (suggested_weight / 100), 2),
                        "history_score": round(raw_score, 4) if raw_score else 0.0,
                        "history_metrics": suggestion.get("metrics"),
                        "history_basis": (
                            "Preserving Strategy Lab allocation ratios from the generated stack."
                            if generated_stack and current_budget_total > 0
                            else suggestion.get("basis")
                        ),
                        "status": None if runtime_item is None else runtime_item.get("status"),
                        "submission_mode": None if runtime_item is None else runtime_item.get("submission_mode"),
                        "has_live_chart": chart_item is not None,
                        "last_close": None if chart_item is None else chart_item.get("last_close"),
                        "last_close_time": None if chart_item is None else chart_item.get("last_close_time"),
                        "activity": self._profile_activity(
                            profile=profile,
                            runtime_item=runtime_item,
                            orders=orders,
                            events=events,
                        ),
                    }
                )
            stacks.append(
                {
                    "name": stack["name"],
                    "path": stack["path"],
                    "generated": generated_stack,
                    "profile_count": len(profiles),
                    "profiles": profiles,
                }
            )

        return {
            "enabled": True,
            "default_stack_path": stacks[0]["path"] if stacks else None,
            "default_total_budget": round(default_total_budget, 2),
            "stacks": stacks,
        }

    def _deployment_default_budget(self, portfolio: dict[str, Any], stack_rows: list[dict[str, Any]]) -> float:
        if portfolio.get("ok"):
            for item in portfolio.get("spot_balances", []):
                if str(item.get("asset", "")).upper() == "USDT":
                    total = _float_or_none(item.get("free"))
                    if total and total > 0:
                        return total
        fallback = 0.0
        first_stack = stack_rows[0] if stack_rows else {}
        for profile in first_stack.get("profiles", []):
            fallback += _float_or_none(profile.get("budget_value")) or 0.0
        return fallback

    def _load_or_compute_deployment_suggestions(self, stack_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        profile_paths = [profile["path"] for stack in stack_rows for profile in stack.get("profiles", [])]
        if not profile_paths:
            return []

        cache_payload: dict[str, Any] | None = None
        if self.deployment_suggestions_path.exists():
            try:
                cache_payload = json.loads(self.deployment_suggestions_path.read_text(encoding="utf-8"))
            except Exception:
                cache_payload = None
        if cache_payload:
            generated_at = _float_or_none(cache_payload.get("generated_at_epoch"))
            cached_profiles = cache_payload.get("profiles") or {}
            if (
                generated_at is not None
                and (time.time() - generated_at) < 1800
                and all(path in cached_profiles for path in profile_paths)
            ):
                return [cached_profiles[path] for path in profile_paths]

        try:
            suggestions = asyncio.run(self._compute_deployment_suggestions(profile_paths))
        except Exception as exc:
            LOGGER.warning("deployment suggestion refresh failed: %s", exc)
            if cache_payload:
                cached_profiles = cache_payload.get("profiles") or {}
                return [cached_profiles[path] for path in profile_paths if path in cached_profiles]
            return []

        cache_payload = {
            "generated_at": utc_now_iso(),
            "generated_at_epoch": time.time(),
            "profiles": {row["profile_path"]: row for row in suggestions},
        }
        self.deployment_suggestions_path.write_text(json_dumps(cache_payload), encoding="utf-8")
        return suggestions

    async def _compute_deployment_suggestions(self, profile_paths: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        async with SpotTradingService(self.settings) as service:
            for path in profile_paths:
                profile = load_runtime_profile(path)
                if profile.market is not MarketType.SPOT:
                    rows.append(
                        {
                            "profile_path": str(path),
                            "score": 1.0,
                            "basis": "No spot replay available; using equal-weight fallback.",
                            "metrics": None,
                        }
                    )
                    continue
                rows.append(await self._profile_spot_suggestion(service, profile))
        return rows

    async def _profile_spot_suggestion(self, service: SpotTradingService, profile: RuntimeProfile) -> dict[str, Any]:
        symbol = str(profile.params.get("symbol", "")).upper()
        interval = str(profile.params.get("interval", "1h")).strip()
        quote_order_qty = profile.params.get("quote_order_qty", "25")
        if not symbol or not interval:
            return {
                "profile_path": str(profile.path),
                "score": 1.0,
                "basis": "Missing symbol or interval; using equal-weight fallback.",
                "metrics": None,
            }

        bars = max(500, min(1200, int(_int_or_none(profile.params.get("warmup_bars")) or 250) * 4))
        candles = await fetch_recent_candles(service, symbol, interval, bars=bars)
        strategy_name = "ema_crossover"
        strategy_kwargs: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "quote_order_qty": str(quote_order_qty),
            "trade_side": str(profile.params.get("trade_side", "long") or "long"),
            "warmup_bars": int(_int_or_none(profile.params.get("warmup_bars")) or 250),
        }
        if str(profile.strategy_ref).endswith("spot_builtin_persistent.py:create_strategy"):
            strategy_name = str(profile.params.get("strategy_name", "")).strip().lower()
            if not strategy_name:
                return {
                    "profile_path": str(profile.path),
                    "score": 1.0,
                    "basis": "Missing built-in strategy name; using equal-weight fallback.",
                    "metrics": None,
                }
            for key, value in profile.params.items():
                if key == "strategy_name":
                    continue
                strategy_kwargs[key] = value
        else:
            fast_period = _int_or_none(profile.params.get("fast_period"))
            slow_period = _int_or_none(profile.params.get("slow_period"))
            if fast_period is None or slow_period is None:
                return {
                    "profile_path": str(profile.path),
                    "score": 1.0,
                    "basis": "Missing EMA parameters; using equal-weight fallback.",
                    "metrics": None,
                }
            strategy_kwargs["fast_period"] = fast_period
            strategy_kwargs["slow_period"] = slow_period

        strategy = create_builtin_strategy(name=strategy_name, **strategy_kwargs)
        result = run_backtest_with_options(
            strategy,
            candles,
            BacktestConfig(
                market_type=MarketType.SPOT,
                initial_capital=1000.0,
                fee_bps=10.0,
                slippage_bps=2.0,
                leverage=1.0,
                position_fraction=1.0,
            ),
            include_equity_curve=False,
        )
        metrics = result.get("metrics") or {}
        sharpe = float(metrics.get("sharpe") or 0.0)
        profit_factor = float(metrics.get("profit_factor") or 0.0)
        total_return = float(metrics.get("total_return_pct") or 0.0)
        drawdown = float(metrics.get("max_drawdown_pct") or 0.0)
        score = max(0.0, total_return - (drawdown * 0.55) + (sharpe * 12.0) + max(profit_factor - 1.0, 0.0) * 18.0)
        sample_days = metrics.get("sample_days")
        return {
            "profile_path": str(profile.path),
            "score": round(score, 4),
            "basis": (
                f"Historical {strategy_name.replace('_', ' ')} replay on {symbol} {interval}; "
                f"return {total_return:.2f}%, drawdown {drawdown:.2f}%, "
                f"Sharpe {sharpe:.2f}, sample {sample_days or '—'} days."
            ),
            "metrics": metrics,
        }

    def _profile_activity(
        self,
        *,
        profile: dict[str, Any],
        runtime_item: dict[str, Any] | None,
        orders: list[dict[str, Any]],
        events: list[dict[str, Any]],
    ) -> list[str]:
        symbol = str(profile.get("symbol") or "").upper()
        service_name = str(profile.get("name") or "")
        rows: list[str] = []

        if runtime_item:
            status = str(runtime_item.get("status") or "UNKNOWN")
            updated_at = runtime_item.get("updated_at") or runtime_item.get("last_heartbeat_at")
            rows.append(f"{service_name} is {status} as of {updated_at or '—'}.")

        for item in orders:
            if str(item.get("symbol") or "").upper() != symbol:
                continue
            rows.append(
                f"Order {item.get('side') or '—'} {item.get('order_type') or '—'} "
                f"{item.get('status') or '—'} at {item.get('updated_at') or item.get('created_at') or '—'}."
            )
            if len(rows) >= 4:
                return rows

        for item in events:
            event_symbol = str(item.get("symbol") or "").upper()
            payload = item.get("payload") or {}
            payload_service = str(payload.get("service_name") or payload.get("summary", {}).get("service_name") or "")
            if event_symbol and event_symbol != symbol and payload_service != service_name:
                continue
            label = item.get("event_type") or item.get("channel") or "event"
            rows.append(f"{label} recorded at {item.get('created_at') or '—'}.")
            if len(rows) >= 4:
                break

        if not rows:
            rows.append("No recent live actions yet. Start the daemon and wait for the next signal.")
        return rows

    def _research_section(self) -> dict[str, Any]:
        strategies = list_strategies()
        presets = [
            item for item in list_research_presets() if self.settings.binance_env.value in item.get("environments", [])
        ]
        category_counts: dict[str, int] = {}
        for item in strategies:
            category = str(item.get("category", "unknown"))
            category_counts[category] = category_counts.get(category, 0) + 1

        latest = None
        error = None
        if self.research_state_path.exists():
            try:
                latest = json.loads(self.research_state_path.read_text(encoding="utf-8"))
            except Exception as exc:
                error = str(exc)

        return {
            "catalog": {
                "builtin_strategy_count": len(strategies),
                "category_rows": [
                    {"category": category, "count": count}
                    for category, count in sorted(category_counts.items(), key=lambda item: item[0])
                ],
                "preset_count": len(presets),
                "preset_rows": presets[:6],
            },
            "latest": latest,
            "state_path": str(self.research_state_path),
            "error": error,
        }


class DashboardControlPlane:
    def __init__(self, settings: Settings, *, workspace_root: Path | None = None) -> None:
        self.settings = settings
        self.workspace_root = (workspace_root or Path.cwd()).resolve()
        self.runtime_examples_dir = self.workspace_root / "examples" / "runtime"
        self.generated_runtime_dir = settings.runtime_dir / "generated"
        self.generated_runtime_dir.mkdir(parents=True, exist_ok=True)
        self.control_dir = settings.runtime_dir / "control"
        self.control_dir.mkdir(parents=True, exist_ok=True)
        self.research_state_path = self.control_dir / "research_state.json"

    def describe(self) -> dict[str, Any]:
        return {
            "available_stacks": self._discover_stacks(),
            "available_profiles": self._discover_profiles(),
            "research_categories": self._research_categories(),
            "managed_processes": self._managed_processes(),
        }

    def handle_action(self, payload: dict[str, Any]) -> dict[str, Any]:
        action = str(payload.get("action", "")).strip()
        if not action:
            raise ValueError("action is required")

        if action == "start_stack":
            return self._start_stack(
                self._required_path(payload),
                budget_overrides=payload.get("budget_overrides"),
            )
        if action == "stop_stack":
            return self._stop_stack(self._required_identifier(payload, field="stack_name"), path=payload.get("path"))
        if action == "start_profile":
            return self._start_profile(self._required_path(payload))
        if action == "stop_profile":
            return self._stop_profile(self._required_identifier(payload, field="profile_name"), path=payload.get("path"))
        if action == "doctor_stack":
            return self._doctor_stack(self._required_path(payload))
        if action == "doctor_profile":
            return self._doctor_profile(self._required_path(payload))
        if action == "reconcile_spot":
            symbol = str(payload.get("symbol", "")).strip().upper()
            if not symbol:
                raise ValueError("symbol is required")
            return self._reconcile_spot(symbol)
        if action == "buy_market_spot":
            symbol = str(payload.get("symbol", "")).strip().upper()
            quote_order_qty = Decimal(str(payload.get("quote_order_qty", "")).strip())
            return self._buy_market_spot(symbol=symbol, quote_order_qty=quote_order_qty, submission_mode=self._submission_mode(payload))
        if action == "sell_market_spot":
            symbol = str(payload.get("symbol", "")).strip().upper()
            quantity = Decimal(str(payload.get("quantity", "")).strip())
            return self._sell_market_spot(symbol=symbol, quantity=quantity, submission_mode=self._submission_mode(payload))
        if action == "redeem_earn_flexible":
            asset = str(payload.get("asset", "")).strip().upper() or None
            product_id = str(payload.get("product_id", "")).strip() or None
            raw_amount = str(payload.get("amount", "")).strip()
            amount = None if not raw_amount else Decimal(raw_amount)
            redeem_all = str(payload.get("redeem_all", "false")).strip().lower() in {"1", "true", "yes", "on"}
            dest_account = str(payload.get("dest_account", "SPOT")).strip().upper() or "SPOT"
            confirmation_text = str(payload.get("confirmation_text", "")).strip()
            return self._redeem_earn_flexible(
                asset=asset,
                product_id=product_id,
                amount=amount,
                redeem_all=redeem_all,
                dest_account=dest_account,
                confirmation_text=confirmation_text,
            )
        if action == "research_scan":
            return self._research_scan(payload)
        if action == "generate_research_stack":
            return self._generate_research_stack(payload)
        if action == "update_profile_params":
            return self._update_profile_params(payload)
        if action == "refresh_portfolio":
            return {"status": "OK", "message": "Refresh portfolio by reloading the page or waiting for the next polling cycle."}
        raise ValueError(f"unsupported action {action!r}")

    def _required_path(self, payload: dict[str, Any]) -> str:
        path = str(payload.get("path", "")).strip()
        if not path:
            raise ValueError("path is required")
        return path

    def _required_identifier(self, payload: dict[str, Any], *, field: str) -> str:
        value = str(payload.get(field, "")).strip()
        if not value:
            raise ValueError(f"{field} is required")
        return value

    def _resolve_path(self, path: str | Path) -> Path:
        candidate = Path(path).expanduser()
        if not candidate.is_absolute():
            candidate = (self.workspace_root / candidate).resolve()
        if not candidate.exists():
            raise ValueError(f"path {candidate} does not exist")
        return candidate

    def _submission_mode(self, payload: dict[str, Any]) -> SubmissionMode:
        raw = str(payload.get("submission_mode", "DRY_RUN")).strip().upper()
        return SubmissionMode(raw)

    def _runtime_definition_paths(self) -> list[Path]:
        paths: list[Path] = []
        seen: set[Path] = set()
        for root in (self.runtime_examples_dir, self.generated_runtime_dir):
            if not root.exists():
                continue
            for path in sorted(root.rglob("*.toml")):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                paths.append(resolved)
        return paths

    def _is_generated_runtime_path(self, path: Path) -> bool:
        try:
            path.resolve().relative_to(self.generated_runtime_dir.resolve())
        except ValueError:
            return False
        return True

    def _discover_stacks(self) -> list[dict[str, Any]]:
        rows = []
        for path in self._runtime_definition_paths():
            try:
                stack = load_runtime_stack(path)
            except Exception:
                continue
            rows.append(
                {
                    "name": stack.name,
                    "path": str(path.resolve()),
                    "generated": self._is_generated_runtime_path(path),
                    "profile_count": len(stack.profiles),
                    "profiles": [self._profile_summary(profile) for profile in stack.profiles],
                }
            )
        return rows

    def _discover_profiles(self) -> list[dict[str, Any]]:
        rows = []
        for path in self._runtime_definition_paths():
            try:
                profile = load_runtime_profile(path)
            except Exception:
                continue
            rows.append(self._profile_summary(profile))
        return rows

    def _profile_summary(self, profile: RuntimeProfile) -> dict[str, Any]:
        budget_key = "quote_order_qty" if profile.market is MarketType.SPOT else "quantity"
        budget_value = profile.params.get(budget_key)
        return {
            "name": profile.name,
            "market": profile.market.value,
            "path": str((profile.path or Path(".")).resolve()),
            "generated": False if profile.path is None else self._is_generated_runtime_path(profile.path),
            "symbol": str(profile.params.get("symbol", "")).upper() or None,
            "interval": str(profile.params.get("interval", "")) or None,
            "strategy_ref": profile.strategy_ref,
            "strategy_label": _strategy_label(profile.strategy_ref),
            "budget_key": budget_key,
            "budget_value": None if budget_value is None else str(budget_value),
            "params": {str(key): str(value) for key, value in profile.params.items()},
        }

    def _research_categories(self) -> list[dict[str, Any]]:
        counts: dict[str, int] = {}
        for item in list_strategies():
            category = str(item.get("category", "unknown"))
            counts[category] = counts.get(category, 0) + 1
        rows = [{"key": "all", "label": f"All Strategies ({sum(counts.values())})"}]
        rows.extend(
            {"key": category, "label": f"{category.replace('_', ' ').title()} ({count})"}
            for category, count in sorted(counts.items(), key=lambda item: item[0])
        )
        return rows

    def _metadata_path(self, *, kind: str, name: str) -> Path:
        return self.control_dir / f"{kind}-{name}.json"

    def _managed_processes(self) -> list[dict[str, Any]]:
        rows = []
        for path in sorted(self.control_dir.glob("*.json")):
            if not (path.name.startswith("stack-") or path.name.startswith("profile-")):
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                rows.append({"metadata_file": path.name, "status": "INVALID", "error": str(exc)})
                continue
            pid = int(payload.get("pid", 0) or 0)
            running = self._pid_is_running(pid)
            payload["running"] = running
            payload["status"] = "RUNNING" if running else "STOPPED"
            payload["metadata_file"] = path.name
            rows.append(payload)
        return rows

    def _pid_is_running(self, pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def _start_process(self, *, kind: str, name: str, path: Path, command: list[str]) -> dict[str, Any]:
        metadata_path = self._metadata_path(kind=kind, name=name)
        if metadata_path.exists():
            existing = json.loads(metadata_path.read_text(encoding="utf-8"))
            pid = int(existing.get("pid", 0) or 0)
            if self._pid_is_running(pid):
                return {
                    "status": "ALREADY_RUNNING",
                    "kind": kind,
                    "name": name,
                    "pid": pid,
                    "path": str(path),
                    "log_path": existing.get("log_path"),
                }

        log_path = self.control_dir / f"{kind}-{name}.log"
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")
        with log_path.open("ab") as log_handle:
            process = subprocess.Popen(
                command,
                cwd=str(self.workspace_root),
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                env=env,
                start_new_session=True,
            )
        payload = {
            "kind": kind,
            "name": name,
            "path": str(path),
            "pid": process.pid,
            "started_at": utc_now_iso(),
            "log_path": str(log_path),
            "command": command,
        }
        metadata_path.write_text(json_dumps(payload), encoding="utf-8")
        return {"status": "STARTED", **payload}

    def _stop_process(self, *, kind: str, name: str) -> dict[str, Any]:
        metadata_path = self._metadata_path(kind=kind, name=name)
        if not metadata_path.exists():
            return {"status": "NOT_FOUND", "kind": kind, "name": name}
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        pid = int(payload.get("pid", 0) or 0)
        if not self._pid_is_running(pid):
            payload["status"] = "STOPPED"
            metadata_path.write_text(json_dumps(payload), encoding="utf-8")
            return {"status": "ALREADY_STOPPED", **payload}

        os.kill(pid, signal.SIGTERM)
        for _ in range(20):
            if not self._pid_is_running(pid):
                payload["status"] = "STOPPED"
                payload["stopped_at"] = utc_now_iso()
                metadata_path.write_text(json_dumps(payload), encoding="utf-8")
                return {"status": "STOPPED", **payload}
            time.sleep(0.25)
        payload["status"] = "STOP_SIGNAL_SENT"
        payload["stop_requested_at"] = utc_now_iso()
        metadata_path.write_text(json_dumps(payload), encoding="utf-8")
        return {"status": "STOP_SIGNAL_SENT", **payload}

    def _start_stack(self, path: str, *, budget_overrides: Any = None) -> dict[str, Any]:
        resolved = self._resolve_path(path)
        stack = load_runtime_stack(resolved)
        applied_budget_updates = self._apply_budget_overrides(stack=stack, budget_overrides=budget_overrides)
        command = [sys.executable, "-m", "binance_trade.cli", "run-daemon-stack", str(resolved)]
        result = self._start_process(kind="stack", name=stack.name, path=resolved, command=command)
        if applied_budget_updates:
            result["budget_updates"] = applied_budget_updates
        return result

    def _stop_stack(self, stack_name: str, *, path: Any = None) -> dict[str, Any]:
        if path:
            resolved = self._resolve_path(str(path))
            stack_name = load_runtime_stack(resolved).name
        return self._stop_process(kind="stack", name=stack_name)

    def _start_profile(self, path: str) -> dict[str, Any]:
        resolved = self._resolve_path(path)
        profile = load_runtime_profile(resolved)
        command = [sys.executable, "-m", "binance_trade.cli", "run-daemon", str(resolved)]
        return self._start_process(kind="profile", name=profile.name, path=resolved, command=command)

    def _stop_profile(self, profile_name: str, *, path: Any = None) -> dict[str, Any]:
        if path:
            resolved = self._resolve_path(str(path))
            profile_name = load_runtime_profile(resolved).name
        return self._stop_process(kind="profile", name=profile_name)

    def _apply_budget_overrides(self, *, stack: RuntimeStack, budget_overrides: Any) -> list[dict[str, Any]]:
        if not isinstance(budget_overrides, dict):
            return []
        updates: list[dict[str, Any]] = []
        for profile in stack.profiles:
            raw_value = budget_overrides.get(profile.name)
            if raw_value in (None, "", False):
                continue
            numeric = Decimal(str(raw_value))
            if numeric <= 0:
                continue
            key = "quote_order_qty" if profile.market is MarketType.SPOT else "quantity"
            if profile.path is None:
                continue
            rendered = format(numeric, "f")
            self._write_profile_param(profile.path, key, rendered)
            updates.append({"profile_name": profile.name, "param": key, "value": rendered})
        return updates

    def _update_profile_params(self, payload: dict[str, Any]) -> dict[str, Any]:
        path = self._resolve_path(self._required_identifier(payload, field="profile_path"))
        profile = load_runtime_profile(path)
        raw_params = payload.get("params")
        if not isinstance(raw_params, dict):
            raise ValueError("params must be an object")
        if len(raw_params) > 40:
            raise ValueError("too many params in one update")

        updates: list[dict[str, str]] = []
        for raw_key, raw_value in raw_params.items():
            key = str(raw_key).strip()
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                raise ValueError(f"invalid param key {key!r}")
            value = str(raw_value).strip()
            if value == "":
                continue
            quote = not re.fullmatch(r"-?\d+(\.\d+)?", value)
            if key in {"symbol", "interval", "strategy_name", "trade_side", "quote_order_qty", "quantity"}:
                quote = True
            self._write_profile_param(path, key, value, quote=quote)
            updates.append({"param": key, "value": value})

        return {
            "status": "OK",
            "profile_name": profile.name,
            "path": str(path),
            "updates": updates,
        }

    def _write_profile_param(self, path: Path, key: str, value: str, *, quote: bool = True) -> None:
        text = path.read_text(encoding="utf-8")
        lines = text.splitlines()
        params_start = None
        params_end = len(lines)
        for index, line in enumerate(lines):
            if line.strip() == "[params]":
                params_start = index
                continue
            if params_start is not None and index > params_start and line.strip().startswith("[") and line.strip().endswith("]"):
                params_end = index
                break
        if params_start is None:
            raise ValueError(f"{path} is missing a [params] section")

        key_pattern = re.compile(rf"^\s*{re.escape(key)}\s*=")
        rendered = json.dumps(value) if quote else value
        replacement = f"{key} = {rendered}"
        for index in range(params_start + 1, params_end):
            if key_pattern.match(lines[index]):
                lines[index] = replacement
                path.write_text("\n".join(lines) + ("\n" if text.endswith("\n") else ""), encoding="utf-8")
                return

        lines.insert(params_end, replacement)
        path.write_text("\n".join(lines) + ("\n" if text.endswith("\n") else ""), encoding="utf-8")

    def _generate_research_stack(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.research_state_path.exists():
            raise ValueError("run a Strategy Lab scan before generating a deployable stack")
        research_state = json.loads(self.research_state_path.read_text(encoding="utf-8"))
        request = research_state.get("request") or {}
        market = str(request.get("market", "spot")).strip().lower()
        if market != "spot":
            raise ValueError("Strategy Lab deployment generation currently supports spot research only")

        allocations = research_state.get("allocations") or []
        if not allocations:
            raise ValueError("the latest Strategy Lab scan has no funded allocations to deploy")

        stack_name = self._generated_stack_name(payload=payload, request=request)
        stack_dir = self.generated_runtime_dir / stack_name
        stack_dir.mkdir(parents=True, exist_ok=False)

        profile_refs: list[str] = []
        for item in allocations:
            profile_name = f"{stack_name}-{self._slugify(str(item.get('name', 'strategy')))}"
            profile_file = f"{profile_name}.toml"
            profile_path = stack_dir / profile_file
            profile_path.write_text(
                self._render_generated_profile(
                    profile_name=profile_name,
                    item=item,
                    request=request,
                ),
                encoding="utf-8",
            )
            profile_refs.append(profile_file)

        stack_path = stack_dir / f"{stack_name}.toml"
        stack_path.write_text(
            self._render_generated_stack(
                stack_name=stack_name,
                profile_refs=profile_refs,
                request=request,
                research_state=research_state,
            ),
            encoding="utf-8",
        )
        stack = load_runtime_stack(stack_path)
        generated = {
            "name": stack.name,
            "path": str(stack_path.resolve()),
            "profile_count": len(stack.profiles),
            "profiles": [self._profile_summary(profile) for profile in stack.profiles],
            "generated_at": utc_now_iso(),
        }
        research_state["generated_stack"] = generated
        self.research_state_path.write_text(json_dumps(research_state), encoding="utf-8")
        return {"status": "OK", "stack": generated}

    def _generated_stack_name(self, *, payload: dict[str, Any], request: dict[str, Any]) -> str:
        requested = str(payload.get("stack_name", "")).strip()
        if requested:
            base = requested
        else:
            symbol = str(request.get("symbol", "spot")).lower()
            interval = str(request.get("interval", "scan")).lower()
            base = f"lab-{symbol}-{interval}"
        slug = self._slugify(base) or "lab-stack"
        timestamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        return f"{slug}-{timestamp}"

    def _slugify(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")

    def _render_generated_profile(self, *, profile_name: str, item: dict[str, Any], request: dict[str, Any]) -> str:
        strategy_name = str(item.get("name", "")).strip().lower()
        if not strategy_name:
            raise ValueError("generated research allocation is missing a strategy name")
        symbol = str(request.get("symbol", "BTCUSDT")).strip().upper()
        interval = str(request.get("interval", "1h")).strip()
        budget_amount = Decimal(str(item.get("budget_amount", "0")))
        allocation_pct = float(item.get("allocation_pct", 0.0) or 0.0)
        warmup_bars = max(250, min(1500, int(_int_or_none(request.get("bars")) or 250)))
        title = str(item.get("title", strategy_name)).strip()
        description = str(item.get("description") or f"{title} generated from Strategy Lab.").strip()
        notes = [
            "Generated from Strategy Lab research output.",
            f"Historical allocation {allocation_pct:.2f}% of the deployable budget.",
            (
                f"Research return {float(item.get('metrics', {}).get('total_return_pct') or 0.0):.2f}% · "
                f"drawdown {float(item.get('metrics', {}).get('max_drawdown_pct') or 0.0):.2f}% · "
                f"score {float(item.get('score') or 0.0):.4f}."
            ),
        ]
        return "\n".join(
            [
                f"name = {json.dumps(profile_name, ensure_ascii=False)}",
                'market = "spot"',
                'strategy_ref = "examples/strategies/spot_builtin_persistent.py:create_strategy"',
                'submission_mode = "inherit"',
                f"description = {json.dumps(description, ensure_ascii=False)}",
                f"notes = {json.dumps(notes, ensure_ascii=False)}",
                "",
                "[params]",
                f"strategy_name = {json.dumps(strategy_name, ensure_ascii=False)}",
                f"symbol = {json.dumps(symbol, ensure_ascii=False)}",
                f"interval = {json.dumps(interval, ensure_ascii=False)}",
                f'quote_order_qty = "{format(budget_amount, "f")}"',
                'trade_side = "long"',
                f"warmup_bars = {warmup_bars}",
                "",
                "[daemon]",
                "reconcile_on_start = true",
                "reconcile_interval_seconds = 300",
                "heartbeat_interval_seconds = 30",
                "auto_restart = true",
                "restart_initial_delay_seconds = 5",
                "restart_max_delay_seconds = 60",
                "stop_on_strategy_exit = false",
                "stale_after_seconds = 90",
                "",
            ]
        )

    def _render_generated_stack(
        self,
        *,
        stack_name: str,
        profile_refs: list[str],
        request: dict[str, Any],
        research_state: dict[str, Any],
    ) -> str:
        summary = research_state.get("summary") or {}
        symbol = str(request.get("symbol", "BTCUSDT"))
        interval = str(request.get("interval", "1h"))
        notes = [
            "Generated automatically from Strategy Lab.",
            f"Market {request.get('market', 'spot')} · symbol {symbol} · interval {interval}.",
            f"Deployable budget in research: {summary.get('deployable_budget', 0)} USDT.",
        ]
        description = f"Strategy Lab generated stack for {symbol} {interval}."
        return "\n".join(
            [
                f"name = {json.dumps(stack_name, ensure_ascii=False)}",
                f"description = {json.dumps(description, ensure_ascii=False)}",
                f"notes = {json.dumps(notes, ensure_ascii=False)}",
                f"profiles = {json.dumps(profile_refs, ensure_ascii=False)}",
                "",
                "[settings]",
                "heartbeat_interval_seconds = 30",
                "stale_after_seconds = 90",
                "stop_on_member_exit = true",
                "stop_on_member_failure = true",
                "",
            ]
        )

    def _doctor_stack(self, path: str) -> dict[str, Any]:
        stack = load_runtime_stack(self._resolve_path(path))
        results = []
        for profile in stack.profiles:
            results.append({"profile_name": profile.name, "doctor": self._doctor_profile_loaded(profile)})
        return {"status": "OK", "runtime_stack": stack.to_dict(), "profiles": results}

    def _doctor_profile(self, path: str) -> dict[str, Any]:
        profile = load_runtime_profile(self._resolve_path(path))
        return {"status": "OK", "runtime_profile": profile.to_dict(), "doctor": self._doctor_profile_loaded(profile)}

    def _doctor_profile_loaded(self, profile: RuntimeProfile) -> dict[str, Any]:
        async def _run() -> dict[str, Any]:
            if profile.market is MarketType.SPOT:
                async with SpotTradingService(self.settings) as service:
                    payload = await service.doctor(profile.params.get("symbol"))
            else:
                async with FuturesTradingService(self.settings) as service:
                    payload = await service.doctor(profile.params.get("symbol"))
            payload["runtime_profile"] = profile.to_dict()
            return payload

        return asyncio.run(_run())

    def _reconcile_spot(self, symbol: str) -> dict[str, Any]:
        async def _run() -> dict[str, Any]:
            async with SpotTradingService(self.settings) as service:
                payload = await service.reconcile(symbol)
                payload["symbol"] = symbol
                return payload

        return {"status": "OK", "result": asyncio.run(_run())}

    def _buy_market_spot(self, *, symbol: str, quote_order_qty: Decimal, submission_mode: SubmissionMode) -> dict[str, Any]:
        async def _run() -> dict[str, Any]:
            async with SpotTradingService(self.settings) as service:
                return await service.buy_market(symbol, quote_order_qty=quote_order_qty, submission_mode=submission_mode)

        return {
            "status": "OK",
            "symbol": symbol,
            "submission_mode": submission_mode.value,
            "result": asyncio.run(_run()),
        }

    def _sell_market_spot(self, *, symbol: str, quantity: Decimal, submission_mode: SubmissionMode) -> dict[str, Any]:
        async def _run() -> dict[str, Any]:
            async with SpotTradingService(self.settings) as service:
                return await service.sell_market(symbol, quantity=quantity, submission_mode=submission_mode)

        return {
            "status": "OK",
            "symbol": symbol,
            "submission_mode": submission_mode.value,
            "result": asyncio.run(_run()),
        }

    def _redeem_earn_flexible(
        self,
        *,
        asset: str | None,
        product_id: str | None,
        amount: Decimal | None,
        redeem_all: bool,
        dest_account: str,
        confirmation_text: str,
    ) -> dict[str, Any]:
        async def _run() -> dict[str, Any]:
            async with SpotTradingService(self.settings) as service:
                return await service.redeem_simple_earn_flexible(
                    asset=asset,
                    product_id=product_id,
                    amount=amount,
                    redeem_all=redeem_all,
                    dest_account=dest_account,
                    confirmation_text=confirmation_text,
                )

        return {
            "status": "OK",
            "asset": asset,
            "product_id": product_id,
            "dest_account": dest_account,
            "result": asyncio.run(_run()),
        }

    def _research_scan(self, payload: dict[str, Any]) -> dict[str, Any]:
        market = str(payload.get("market", "spot")).strip().lower()
        symbol = str(payload.get("symbol", "BTCUSDT")).strip().upper()
        interval = str(payload.get("interval", "15m")).strip()
        bars = int(str(payload.get("bars", "1500")).strip())
        capital = float(str(payload.get("capital", "1000")).strip())
        fee_bps = float(str(payload.get("fee_bps", "10")).strip())
        slippage_bps = float(str(payload.get("slippage_bps", "2")).strip())
        leverage = float(str(payload.get("leverage", "1")).strip())
        position_fraction = float(str(payload.get("position_fraction", "1")).strip())
        category = str(payload.get("category", "all")).strip().lower()
        top_n = max(1, int(str(payload.get("top_n", "5")).strip()))
        allocation_mode = str(payload.get("allocation_mode", "auto")).strip().lower()
        manual_weights = _parse_manual_weights(payload.get("manual_weights"))
        requested_names = _csv_list(payload.get("strategies"))

        metadata = list_strategies()
        names = requested_names
        if not names:
            if category == "all":
                names = [item["name"] for item in metadata]
            else:
                names = [item["name"] for item in metadata if item.get("category") == category]
        if not names:
            raise ValueError("no strategies selected for this research scan")

        async def _run() -> dict[str, Any]:
            if market == "spot":
                async with SpotTradingService(self.settings) as service:
                    candles = await fetch_recent_candles(service, symbol, interval, bars=bars)
                    benchmark = await benchmark_builtin_strategies(
                        service,
                        market_type=MarketType.SPOT,
                        symbol=symbol,
                        interval=interval,
                        bars=bars,
                        initial_capital=capital,
                        fee_bps=fee_bps,
                        slippage_bps=slippage_bps,
                        leverage=1.0,
                        position_fraction=position_fraction,
                        strategy_names=names,
                        include_equity_curve=True,
                        workers=1,
                    )
                    return {
                        "benchmark": benchmark,
                        "candles": [
                            {
                                "open_time": candle.open_time,
                                "close_time": candle.close_time,
                                "open": candle.open,
                                "high": candle.high,
                                "low": candle.low,
                                "close": candle.close,
                            }
                            for candle in candles
                        ],
                    }
            if market == "futures":
                async with FuturesTradingService(self.settings) as service:
                    candles = await fetch_recent_candles(service, symbol, interval, bars=bars)
                    benchmark = await benchmark_builtin_strategies(
                        service,
                        market_type=MarketType.FUTURES,
                        symbol=symbol,
                        interval=interval,
                        bars=bars,
                        initial_capital=capital,
                        fee_bps=fee_bps,
                        slippage_bps=slippage_bps,
                        leverage=leverage,
                        position_fraction=position_fraction,
                        strategy_names=names,
                        include_equity_curve=True,
                        workers=1,
                    )
                    return {
                        "benchmark": benchmark,
                        "candles": [
                            {
                                "open_time": candle.open_time,
                                "close_time": candle.close_time,
                                "open": candle.open,
                                "high": candle.high,
                                "low": candle.low,
                                "close": candle.close,
                            }
                            for candle in candles
                        ],
                    }
            raise ValueError("market must be spot or futures")

        payload_bundle = asyncio.run(_run())
        benchmark = payload_bundle["benchmark"]
        research_state = self._build_research_state(
            benchmark=benchmark,
            candle_snapshot=payload_bundle["candles"],
            request={
                "market": market,
                "symbol": symbol,
                "interval": interval,
                "bars": bars,
                "capital": capital,
                "fee_bps": fee_bps,
                "slippage_bps": slippage_bps,
                "leverage": leverage,
                "position_fraction": position_fraction,
                "category": category,
                "top_n": top_n,
                "allocation_mode": allocation_mode,
                "manual_weights": manual_weights,
                "strategy_names": names,
            },
        )
        self.research_state_path.write_text(json_dumps(research_state), encoding="utf-8")
        return {
            "status": "OK",
            "summary": research_state["summary"],
            "top_allocations": research_state["allocations"][: min(3, len(research_state["allocations"]))],
            "candidate_count": len(research_state["candidates"]),
            "state_path": str(self.research_state_path),
        }

    def _build_research_state(self, *, benchmark: dict[str, Any], candle_snapshot: list[dict[str, Any]], request: dict[str, Any]) -> dict[str, Any]:
        capital = float(request["capital"])
        top_n = int(request["top_n"])
        completed = [item for item in benchmark.get("strategies", []) if item.get("status") != "ERROR"]
        candidates = [self._candidate_row(item, candle_snapshot=candle_snapshot) for item in completed]
        candidates.sort(key=lambda item: (item["score"], item["metrics"]["total_return_pct"]), reverse=True)

        allocations, allocation_context = self._allocation_rows(candidates=candidates, capital=capital, request=request)
        positive = [item for item in candidates if item["score"] > 0]
        deployable_budget = allocation_context["deployable_budget"]
        reserve_budget = capital - deployable_budget
        portfolio = self._portfolio_simulation(
            allocations=allocations,
            capital=capital,
            reserve_budget=reserve_budget,
            strategy_results=completed,
        )

        top_candidate = allocations[0] if allocations else (candidates[0] if candidates else None)
        watchlist = []
        for item in candidates[: min(max(top_n, 3), len(candidates))]:
            if allocations and any(item["name"] == allocated["name"] for allocated in allocations):
                continue
            watchlist.append(item)
        avoid = sorted(candidates, key=lambda item: item["metrics"]["total_return_pct"])[:3]

        summary = {
            "generated_at": utc_now_iso(),
            "headline": (
                f"{top_candidate['title']} is the strongest current candidate on {request['symbol']} {request['interval']}"
                if top_candidate
                else "No completed strategy candidates yet"
            ),
            "market": request["market"],
            "symbol": request["symbol"],
            "interval": request["interval"],
            "bars": request["bars"],
            "capital": round(capital, 2),
            "deployable_budget": round(deployable_budget, 2),
            "reserve_budget": round(reserve_budget, 2),
            "category": request["category"],
            "allocation_mode": request.get("allocation_mode", "auto"),
            "completed_count": len(completed),
            "failed_count": benchmark.get("benchmark", {}).get("failed_count", 0),
            "top_strategy_name": None if top_candidate is None else top_candidate["title"],
            "top_strategy_return_pct": None if top_candidate is None else top_candidate["metrics"]["total_return_pct"],
            "top_strategy_drawdown_pct": None if top_candidate is None else top_candidate["metrics"]["max_drawdown_pct"],
            "positive_candidate_count": len(positive),
            "allocation_model": allocation_context["description"],
            "interval_explanation": self._interval_explanation(str(request["interval"]), int(request["bars"])),
            "notes": [
                "This budget view is research guidance, not an automated live allocation command.",
                "Returns are net of the configured fee and slippage assumptions.",
                "These charts approximate long-running deployment by replaying historical Binance K-lines with next-bar execution.",
                "Strategies with non-positive scores stay in the watchlist or avoid list unless you explicitly fund them with manual weights.",
            ],
        }

        return {
            "generated_at": utc_now_iso(),
            "request": request,
            "benchmark": benchmark.get("benchmark", {}),
            "summary": summary,
            "portfolio": portfolio,
            "allocations": allocations,
            "candidates": candidates,
            "watchlist": watchlist[:5],
            "avoid": avoid,
        }

    def _allocation_rows(self, *, candidates: list[dict[str, Any]], capital: float, request: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        allocation_mode = str(request.get("allocation_mode", "auto")).lower()
        top_n = int(request.get("top_n", 5))

        if allocation_mode == "manual":
            weights = request.get("manual_weights") or {}
            total_weight = sum(float(value) for value in weights.values())
            if total_weight > 0:
                rows = []
                for candidate in candidates:
                    keys = {
                        str(candidate["name"]).lower(),
                        str(candidate["title"]).lower(),
                    }
                    matched_weight = next((float(weights[key]) for key in keys if key in weights), 0.0)
                    if matched_weight <= 0:
                        continue
                    weight = matched_weight / total_weight
                    row = dict(candidate)
                    row["rank"] = len(rows) + 1
                    row["allocation_pct"] = round(weight * 100, 2)
                    row["budget_amount"] = round(capital * weight, 2)
                    rows.append(row)
                return rows, {
                    "deployable_budget": capital if rows else 0.0,
                    "description": "manual weights across explicitly selected strategies",
                }

        positive = [item for item in candidates if item["score"] > 0]
        deployable_budget = capital * 0.85 if positive else 0.0
        rows = [dict(item) for item in positive[:top_n]]
        score_total = sum(item["score"] for item in rows)
        for index, item in enumerate(rows, start=1):
            weight = (item["score"] / score_total) if score_total > 0 else 0.0
            item["rank"] = index
            item["allocation_pct"] = round(weight * 100, 2)
            item["budget_amount"] = round(deployable_budget * weight, 2)
        return rows, {
            "deployable_budget": deployable_budget,
            "description": "score-weighted positive candidates with a 15% reserve",
        }

    def _portfolio_simulation(
        self,
        *,
        allocations: list[dict[str, Any]],
        capital: float,
        reserve_budget: float,
        strategy_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not allocations:
            return {
                "metrics": {
                    "final_equity": round(capital, 2),
                    "total_return_pct": 0.0,
                    "max_drawdown_pct": 0.0,
                    "funded_strategy_count": 0,
                },
                "equity_curve": [],
            }

        strategy_map = {str(item.get("name")): item for item in strategy_results}
        first_strategy = strategy_map.get(str(allocations[0].get("name")))
        first_curve = (first_strategy or {}).get("equity_curve", []) or []
        times = [int(point["time"]) for point in first_curve]
        if not times:
            return {
                "metrics": {
                    "final_equity": round(capital, 2),
                    "total_return_pct": 0.0,
                    "max_drawdown_pct": 0.0,
                    "funded_strategy_count": len(allocations),
                },
                "equity_curve": [],
            }

        normalized_curves: list[list[float]] = []
        for item in allocations:
            strategy_result = strategy_map.get(str(item.get("name")))
            values = [float(point["equity"]) for point in (strategy_result or {}).get("equity_curve", [])]
            if not values:
                continue
            usable = values[-len(times) :]
            budget_amount = float(item.get("budget_amount", 0.0))
            if not usable or budget_amount <= 0:
                continue
            base = capital if capital > 0 else usable[0]
            normalized_curves.append([(value / base) * budget_amount for value in usable])

        if not normalized_curves:
            return {
                "metrics": {
                    "final_equity": round(capital, 2),
                    "total_return_pct": 0.0,
                    "max_drawdown_pct": 0.0,
                    "funded_strategy_count": len(allocations),
                },
                "equity_curve": [],
            }

        combined: list[float] = []
        for index in range(len(times)):
            combined.append(round(reserve_budget + sum(curve[index] for curve in normalized_curves), 4))

        final_equity = combined[-1]
        total_return_pct = ((final_equity / capital) - 1) * 100 if capital > 0 else 0.0
        return {
            "metrics": {
                "final_equity": round(final_equity, 2),
                "total_return_pct": round(total_return_pct, 4),
                "max_drawdown_pct": round(_max_drawdown(combined) * 100, 4),
                "funded_strategy_count": len(allocations),
            },
            "equity_curve": [{"time": timestamp, "equity": value} for timestamp, value in zip(times, combined)],
        }

    def _interval_explanation(self, interval: str, bars: int) -> str:
        minutes = interval_to_minutes(interval)
        sample_days = round((minutes * bars) / (60 * 24), 2)
        if minutes < 60:
            return f"{interval} means each K-line covers {minutes} minutes; {bars} bars is about {sample_days} days of simulated history."
        hours = minutes / 60
        if hours < 24:
            return f"{interval} means each K-line covers {hours:g} hours; {bars} bars is about {sample_days} days of simulated history."
        days = hours / 24
        return f"{interval} means each K-line covers {days:g} days; {bars} bars is about {sample_days} days of simulated history."

    def _candidate_row(self, item: dict[str, Any], *, candle_snapshot: list[dict[str, Any]]) -> dict[str, Any]:
        metrics = item.get("metrics", {})
        recent_trades = item.get("trades", [])[-3:]
        score = self._candidate_score(metrics)
        chart = self._candidate_chart(candle_snapshot=candle_snapshot, strategy_result=item)
        window_return_pct = _series_return_pct(chart.get("recent", {}).get("equity", {}).get("values", []))
        window_bars = len(chart.get("recent", {}).get("price", {}).get("close", []))
        full_bars = len(chart.get("full", {}).get("price", {}).get("close", []))
        return {
            "name": item.get("name"),
            "title": item.get("title", item.get("name")),
            "category": item.get("category"),
            "description": item.get("description"),
            "score": round(score, 4),
            "chart": chart,
            "metrics": {
                "total_return_pct": metrics.get("total_return_pct"),
                "window_return_pct": window_return_pct,
                "max_drawdown_pct": metrics.get("max_drawdown_pct"),
                "profit_factor": metrics.get("profit_factor"),
                "sharpe": metrics.get("sharpe"),
                "trade_count": metrics.get("trade_count"),
                "win_rate_pct": metrics.get("win_rate_pct"),
                "fees_paid": metrics.get("fees_paid"),
                "turnover_multiple": metrics.get("turnover_multiple"),
                "exposure_pct": metrics.get("exposure_pct"),
                "sample_days": metrics.get("sample_days"),
            },
            "ops_summary": {
                "activity": (
                    f"{metrics.get('trade_count', 0)} trades over {metrics.get('sample_days', 0)}d · "
                    f"exposure {metrics.get('exposure_pct', 0)}% · turnover {metrics.get('turnover_multiple', 0)}x"
                ),
                "window_scope": (
                    f"Displayed chart shows the most recent {window_bars} bars only. "
                    "B/S markers and the mini equity curve belong to this window."
                ),
                "full_scope": (
                    f"Full sample view shows the complete simulated path across {full_bars} bars."
                ),
                "metric_scope": (
                    "Return, drawdown, Sharpe, win rate, and profit factor summarize the full simulated sample."
                ),
                "cost_pressure": (
                    f"fees {metrics.get('fees_paid', 0)} across {metrics.get('trade_count', 0)} trades"
                ),
                "recent_actions": [self._trade_action_text(trade) for trade in recent_trades],
            },
            "source_urls": item.get("source_urls", []),
        }

    def _candidate_chart(self, *, candle_snapshot: list[dict[str, Any]], strategy_result: dict[str, Any]) -> dict[str, Any]:
        if not candle_snapshot:
            return {
                "recent": {
                    "price": {"times": [], "close": [], "min": None, "max": None},
                    "equity": {"times": [], "values": [], "min": None, "max": None},
                    "markers": [],
                },
                "full": {
                    "price": {"times": [], "close": [], "min": None, "max": None},
                    "equity": {"times": [], "values": [], "min": None, "max": None},
                    "markers": [],
                },
            }

        equity_points = strategy_result.get("equity_curve", []) or []
        full_times = [int(item["close_time"]) for item in candle_snapshot]
        full_close = [float(item["close"]) for item in candle_snapshot]
        full_price_min = min(float(item["low"]) for item in candle_snapshot)
        full_price_max = max(float(item["high"]) for item in candle_snapshot)
        recent_candles = candle_snapshot[-120:]
        recent_times = [int(item["close_time"]) for item in recent_candles]
        recent_close = [float(item["close"]) for item in recent_candles]
        recent_price_min = min(float(item["low"]) for item in recent_candles)
        recent_price_max = max(float(item["high"]) for item in recent_candles)
        recent_equity = equity_points[-120:]
        full_equity_values = [float(item["equity"]) for item in equity_points] if equity_points else []
        recent_equity_values = [float(item["equity"]) for item in recent_equity] if recent_equity else []

        def _markers_for_window(start_time: int, end_time: int) -> list[dict[str, Any]]:
            markers = []
            for trade in strategy_result.get("trades", []) or []:
                side = str(trade.get("side", "LONG")).upper()
                entry_time = _int_or_none(trade.get("entry_time"))
                exit_time = _int_or_none(trade.get("exit_time"))
                entry_price = _float_or_none(trade.get("entry_price"))
                exit_price = _float_or_none(trade.get("exit_price"))
                return_pct = trade.get("return_pct")
                if entry_time is not None and entry_price is not None and start_time <= entry_time <= end_time:
                    markers.append(
                        {
                            "kind": "buy" if side == "LONG" else "sell",
                            "time": entry_time,
                            "price": entry_price,
                            "label": f"{side} entry",
                            "detail": str(trade.get("entry_reason", "")),
                        }
                    )
                if exit_time is not None and exit_price is not None and start_time <= exit_time <= end_time:
                    markers.append(
                        {
                            "kind": "sell" if side == "LONG" else "buy",
                            "time": exit_time,
                            "price": exit_price,
                            "label": f"{side} exit",
                            "detail": f"{trade.get('exit_reason', '')} · {return_pct}%",
                        }
                    )
            return markers

        return {
            "recent": {
                "price": {
                    "times": recent_times,
                    "close": recent_close,
                    "min": recent_price_min,
                    "max": recent_price_max,
                },
                "equity": {
                    "times": [int(item["time"]) for item in recent_equity] if recent_equity else [],
                    "values": recent_equity_values,
                    "min": min(recent_equity_values) if recent_equity_values else None,
                    "max": max(recent_equity_values) if recent_equity_values else None,
                },
                "markers": _markers_for_window(recent_times[0], recent_times[-1]),
            },
            "full": {
                "price": {
                    "times": full_times,
                    "close": full_close,
                    "min": full_price_min,
                    "max": full_price_max,
                },
                "equity": {
                    "times": [int(item["time"]) for item in equity_points] if equity_points else [],
                    "values": full_equity_values,
                    "min": min(full_equity_values) if full_equity_values else None,
                    "max": max(full_equity_values) if full_equity_values else None,
                },
                "markers": _markers_for_window(full_times[0], full_times[-1]),
            },
        }

    def _candidate_score(self, metrics: dict[str, Any]) -> float:
        total_return = max(float(metrics.get("total_return_pct") or 0.0), 0.0)
        profit_factor = float(metrics.get("profit_factor") or 0.0)
        sharpe = float(metrics.get("sharpe") or 0.0)
        max_drawdown = max(float(metrics.get("max_drawdown_pct") or 0.0), 0.0)
        turnover = max(float(metrics.get("turnover_multiple") or 0.0), 0.0)
        if total_return <= 0 or profit_factor <= 0:
            return 0.0
        drawdown_factor = max(0.05, 1 - (max_drawdown / 20))
        turnover_factor = max(0.2, 1 - (turnover / 120))
        quality = max(profit_factor, 1.0) * max(sharpe + 1.5, 0.5)
        return total_return * quality * drawdown_factor * turnover_factor

    def _trade_action_text(self, trade: dict[str, Any]) -> str:
        side = str(trade.get("side", "TRADE"))
        entry_reason = str(trade.get("entry_reason", "entry")).strip()
        exit_reason = str(trade.get("exit_reason", "exit")).strip()
        return_pct = trade.get("return_pct")
        bars_held = trade.get("bars_held")
        return f"{side} · {return_pct}% · {bars_held} bars · {entry_reason} -> {exit_reason}"


class DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        data_service: DashboardDataService,
        control_plane: DashboardControlPlane,
        html: str,
    ) -> None:
        self.data_service = data_service
        self.control_plane = control_plane
        self.html = html.encode("utf-8")
        super().__init__(server_address, DashboardRequestHandler)


class DashboardRequestHandler(BaseHTTPRequestHandler):
    server: DashboardHTTPServer

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_bytes(self.server.html, content_type="text/html; charset=utf-8")
            return
        if parsed.path == "/healthz":
            self._send_json({"ok": True, "generated_at": utc_now_iso()})
            return
        if parsed.path == "/api/snapshot":
            self._send_json(self.server.data_service.build_snapshot())
            return
        self._send_json({"error": "not_found", "path": parsed.path}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/actions":
            self._send_json({"error": "not_found", "path": parsed.path}, status=HTTPStatus.NOT_FOUND)
            return
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            payload = json.loads(body.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("request body must be a JSON object")
            if payload.get("action") in {"refresh_portfolio", "redeem_earn_flexible"}:
                self.server.data_service.invalidate_portfolio_cache()
            result = self.server.control_plane.handle_action(payload)
            self._send_json({"ok": True, "result": result})
        except Exception as exc:
            LOGGER.exception("dashboard action failed")
            self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def do_HEAD(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/healthz", "/api/snapshot", "/api/actions"}:
            self.send_response(HTTPStatus.OK)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        LOGGER.info("dashboard %s", format % args)

    def _send_json(self, payload: dict[str, Any], *, status: HTTPStatus = HTTPStatus.OK) -> None:
        self._send_bytes(json_dumps(payload).encode("utf-8"), content_type="application/json; charset=utf-8", status=status)

    def _send_bytes(self, payload: bytes, *, content_type: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)


def render_dashboard_html(*, refresh_seconds: int) -> str:
    return DASHBOARD_TEMPLATE.replace("__REFRESH_SECONDS__", str(refresh_seconds))


def run_dashboard_server(settings: Settings, config: DashboardConfig) -> None:
    control_plane = DashboardControlPlane(settings=settings)
    data_service = DashboardDataService(settings=settings, config=config, control_plane=control_plane)
    server = DashboardHTTPServer(
        (config.host, config.port),
        data_service=data_service,
        control_plane=control_plane,
        html=render_dashboard_html(refresh_seconds=config.refresh_seconds),
    )
    LOGGER.info("dashboard listening on http://%s:%s", config.host, config.port)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        LOGGER.info("dashboard shutdown requested")
    finally:
        server.server_close()


DASHBOARD_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BinanceTrade Ops Dashboard</title>
  <style>
    :root {
      --bg: #080b10;
      --panel: rgba(14,19,27,0.88);
      --panel-strong: rgba(18,25,35,0.96);
      --ink: #e8f0f2;
      --muted: #8ea0aa;
      --line: rgba(116,239,211,0.16);
      --gold: #ffd166;
      --teal: #54f0c8;
      --green: #45d483;
      --red: #ff5c7a;
      --amber: #ffb020;
      --shadow: 0 18px 70px rgba(0, 0, 0, 0.32);
      --mono: "SF Mono", "JetBrains Mono", "Menlo", "Cascadia Code", monospace;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "SF Pro Display", "Segoe UI", sans-serif;
      background:
        linear-gradient(rgba(84,240,200,0.045) 1px, transparent 1px),
        linear-gradient(90deg, rgba(84,240,200,0.045) 1px, transparent 1px),
        radial-gradient(circle at top left, rgba(84,240,200,0.16), transparent 30%),
        radial-gradient(circle at bottom right, rgba(255,209,102,0.12), transparent 30%),
        linear-gradient(180deg, #0b1018 0%, var(--bg) 100%);
      background-size: 28px 28px, 28px 28px, auto, auto, auto;
    }
    .shell {
      width: min(1480px, calc(100vw - 32px));
      margin: 24px auto 56px;
    }
    .hero {
      display: grid;
      gap: 18px;
      grid-template-columns: 1.4fr 1fr;
      align-items: stretch;
      margin-bottom: 20px;
    }
    .hero-card, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(16px);
    }
    .hero-card {
      padding: 26px 28px;
      position: relative;
      overflow: hidden;
    }
    .hero-card::after {
      content: "";
      position: absolute;
      inset: auto -60px -70px auto;
      width: 180px;
      height: 180px;
      border-radius: 999px;
      background: radial-gradient(circle, rgba(211,155,42,0.24), transparent 70%);
    }
    .eyebrow {
      font-size: 12px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 10px;
    }
    .hero h1 {
      font-family: var(--mono);
      font-size: clamp(32px, 4vw, 54px);
      line-height: 0.94;
      margin: 0 0 14px;
      letter-spacing: -0.04em;
    }
    .hero p {
      margin: 0;
      color: var(--muted);
      max-width: 58ch;
      line-height: 1.5;
    }
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(84,240,200,0.08);
      border: 1px solid rgba(84,240,200,0.18);
      font-family: var(--mono);
      font-size: 13px;
      margin-top: 18px;
    }
    .status-badge::before {
      content: "";
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: var(--gold);
      box-shadow: 0 0 0 0 rgba(211,155,42,0.5);
      animation: pulse 1.8s infinite;
    }
    @keyframes pulse {
      0% { box-shadow: 0 0 0 0 rgba(211,155,42,0.45); }
      70% { box-shadow: 0 0 0 14px rgba(211,155,42,0); }
      100% { box-shadow: 0 0 0 0 rgba(211,155,42,0); }
    }
    .summary-grid, .panel-grid {
      display: grid;
      gap: 16px;
    }
    .summary-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    .panel-grid { grid-template-columns: 1.1fr 1.1fr 1.2fr; }
    .metric {
      padding: 18px 18px 16px;
      background: var(--panel-strong);
      border-radius: 16px;
      border: 1px solid var(--line);
    }
    .metric-label {
      font-family: var(--mono);
      font-size: 12px;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 10px;
    }
    .metric-value {
      font-family: var(--mono);
      font-size: clamp(24px, 3vw, 36px);
      font-weight: 700;
      letter-spacing: -0.04em;
    }
    .metric-sub {
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
    }
    .panel {
      padding: 20px;
    }
    .panel h2 {
      margin: 0 0 14px;
      font-size: 18px;
      letter-spacing: -0.02em;
      font-family: var(--mono);
    }
    .subtle {
      color: var(--muted);
      font-size: 13px;
    }
    .list {
      display: grid;
      gap: 10px;
    }
    .service-card, .stack-card, .wallet-row, .runtime-file {
      padding: 14px;
      background: rgba(11,16,24,0.72);
      border: 1px solid var(--line);
      border-radius: 14px;
    }
    .service-head, .stack-head, .wallet-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      margin-bottom: 8px;
    }
    .title-row {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      padding: 4px 9px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      background: rgba(23,22,26,0.08);
      font-family: var(--mono);
    }
    .pill.ok { background: rgba(19,121,91,0.12); color: var(--green); }
    .pill.warn { background: rgba(155,106,0,0.14); color: var(--amber); }
    .pill.err { background: rgba(180,58,58,0.12); color: var(--red); }
    .meta-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 12px;
      font-size: 13px;
    }
    .meta-grid span {
      color: var(--muted);
      display: block;
      margin-bottom: 2px;
    }
    .wallet-bars {
      display: grid;
      gap: 10px;
      margin-top: 12px;
    }
    .wallet-bar-label {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 4px;
      font-size: 13px;
    }
    .track {
      height: 10px;
      border-radius: 999px;
      background: rgba(23,22,26,0.08);
      overflow: hidden;
    }
    .fill {
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(90deg, var(--teal), var(--gold));
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    th, td {
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }
    th {
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      font-weight: 600;
    }
    code {
      font-family: "SF Mono", "Menlo", monospace;
      font-size: 12px;
      color: var(--teal);
      word-break: break-all;
    }
    .empty {
      color: var(--muted);
      padding: 8px 0;
    }
    .wide { grid-column: span 3; }
    .split { display: grid; gap: 16px; grid-template-columns: 1fr 1fr; }
    .toolbar {
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: center;
      margin-bottom: 12px;
      flex-wrap: wrap;
    }
    .ops-grid {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }
    .ops-card {
      padding: 14px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(11,16,24,0.72);
    }
    .ops-card h3 {
      margin: 0 0 10px;
      font-size: 15px;
      font-family: var(--mono);
    }
    .ops-form {
      display: grid;
      gap: 10px;
    }
    .ops-form label {
      display: grid;
      gap: 6px;
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      font-family: var(--mono);
    }
    .ops-form input, .ops-form select, .ops-form button, .ops-row button {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 12px;
      font: inherit;
      background: rgba(5,9,14,0.88);
      color: var(--ink);
    }
    .ops-form button, .ops-row button {
      cursor: pointer;
      background: linear-gradient(180deg, rgba(84,240,200,0.18) 0%, rgba(84,240,200,0.08) 100%);
      font-weight: 600;
      font-family: var(--mono);
    }
    .ops-row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .lang-toggle, .panel-tab {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 10px 14px;
      font: inherit;
      background: rgba(11,16,24,0.86);
      color: var(--ink);
      cursor: pointer;
      transition: transform 140ms ease, background 140ms ease, border-color 140ms ease;
    }
    .lang-toggle:hover, .panel-tab:hover {
      transform: translateY(-1px);
      border-color: rgba(23,22,26,0.18);
    }
    .panel-tabs {
      display: flex;
      gap: 10px;
      margin: 0 0 18px;
      flex-wrap: wrap;
    }
    .panel-tab.active {
      background: linear-gradient(180deg, rgba(84,240,200,0.22) 0%, rgba(84,240,200,0.08) 100%);
      border-color: rgba(84,240,200,0.42);
      font-weight: 700;
    }
    .hero-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 18px;
    }
    .primary-action {
      border: 1px solid rgba(84,240,200,0.48);
      border-radius: 12px;
      padding: 12px 16px;
      cursor: pointer;
      color: #06100e;
      background: linear-gradient(180deg, var(--teal), #2fbd9d);
      font-family: var(--mono);
      font-weight: 800;
      box-shadow: 0 0 28px rgba(84,240,200,0.18);
    }
    .secondary-action {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px 16px;
      cursor: pointer;
      color: var(--ink);
      background: rgba(11,16,24,0.74);
      font-family: var(--mono);
      font-weight: 700;
    }
    .readiness-strip {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      margin-bottom: 16px;
    }
    .readiness-card {
      padding: 14px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(11,16,24,0.7);
    }
    .readiness-card span {
      display: block;
      color: var(--muted);
      font-family: var(--mono);
      font-size: 11px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      margin-bottom: 6px;
    }
    .readiness-card strong {
      font-family: var(--mono);
      font-size: 18px;
    }
    .deployment-param-grid {
      display: grid;
      gap: 8px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      margin-top: 10px;
    }
    .deployment-param-grid label {
      min-width: 0;
    }
    [data-panel].is-hidden {
      display: none !important;
    }
    .deployment-layout {
      align-items: start;
      grid-template-columns: 1.1fr 0.9fr;
    }
    .deployment-profile-grid {
      display: grid;
      gap: 10px;
      margin-top: 4px;
    }
    .deployment-profile-row {
      padding: 12px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.74);
    }
    .deployment-profile-head {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: start;
      margin-bottom: 8px;
    }
    .deployment-profile-meta {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 10px;
      font-size: 13px;
      margin-top: 8px;
    }
    .deployment-profile-meta span {
      color: var(--muted);
      display: block;
      margin-bottom: 2px;
    }
    .live-overview-grid {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .advanced-details > summary {
      cursor: pointer;
      font-weight: 600;
      color: var(--ink);
      list-style: none;
    }
    .advanced-details > summary::-webkit-details-marker {
      display: none;
    }
    .advanced-details > summary::after {
      content: "▾";
      margin-left: 8px;
      color: var(--muted);
    }
    .response-box {
      margin-top: 10px;
      padding: 12px;
      min-height: 72px;
      border-radius: 14px;
      background: rgba(84,240,200,0.06);
      border: 1px dashed rgba(23,22,26,0.15);
      font-family: "SF Mono", "Menlo", monospace;
      font-size: 12px;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .response-box h3,
    .research-block h3 {
      margin: 0 0 10px;
      font-size: 15px;
    }
    .response-box ul,
    .research-list {
      margin: 0;
      padding-left: 18px;
      display: grid;
      gap: 6px;
    }
    .status-log {
      display: grid;
      gap: 8px;
    }
    .log-row {
      display: grid;
      grid-template-columns: 86px 92px 1fr;
      gap: 12px;
      align-items: start;
      padding: 10px 0;
      border-bottom: 1px solid var(--line);
      font-size: 13px;
    }
    .log-row:last-child { border-bottom: 0; }
    .log-source {
      color: var(--muted);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .log-message strong {
      display: block;
      margin-bottom: 2px;
    }
    .log-message span {
      color: var(--muted);
      display: block;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .research-layout {
      display: grid;
      gap: 16px;
      grid-template-columns: 360px 1fr;
    }
    .research-block {
      padding: 16px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(11,16,24,0.72);
    }
    .research-summary-grid,
    .candidate-metrics {
      display: grid;
      gap: 10px 12px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }
    .mini-metric {
      padding: 10px 12px;
      border-radius: 14px;
      background: rgba(84,240,200,0.06);
      border: 1px solid rgba(23,22,26,0.07);
    }
    .mini-metric span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 4px;
    }
    .candidate-grid {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      margin-top: 16px;
    }
    .candidate-card {
      padding: 16px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(11,16,24,0.74);
    }
    .candidate-chart-wrap {
      margin-top: 12px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background:
        linear-gradient(180deg, rgba(211,155,42,0.06), transparent 45%),
        linear-gradient(180deg, rgba(16,23,32,0.98), rgba(10,15,22,0.88));
      overflow: hidden;
    }
    .candidate-chart-svg {
      width: 100%;
      height: auto;
      display: block;
    }
    .candidate-legend {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 10px;
      font-size: 12px;
      color: var(--muted);
    }
    .marker-label {
      display: inline-flex;
      align-items: center;
      gap: 7px;
    }
    .marker-dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      display: inline-block;
    }
    .allocation-track {
      height: 10px;
      border-radius: 999px;
      background: rgba(84,240,200,0.10);
      overflow: hidden;
      margin-top: 10px;
    }
    .allocation-fill {
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(90deg, var(--gold), var(--teal));
    }
    .candidate-notes {
      display: grid;
      gap: 8px;
      margin-top: 12px;
      font-size: 13px;
      color: var(--muted);
    }
    .catalog-grid {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .chart-grid {
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .chart-card {
      padding: 16px;
      border-radius: 20px;
      border: 1px solid var(--line);
      background: rgba(11,16,24,0.74);
    }
    .chart-meta {
      display: grid;
      gap: 8px 12px;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      margin-top: 12px;
      font-size: 13px;
    }
    .chart-meta span {
      color: var(--muted);
      display: block;
      margin-bottom: 2px;
    }
    .chart-wrap {
      margin-top: 14px;
      position: relative;
      border-radius: 18px;
      border: 1px solid var(--line);
      background:
        linear-gradient(180deg, rgba(24,125,120,0.05), transparent 38%),
        linear-gradient(180deg, rgba(16,23,32,0.96), rgba(10,15,22,0.84));
      overflow: hidden;
    }
    .chart-svg {
      width: 100%;
      height: auto;
      display: block;
    }
    .chart-card.placeholder {
      border-style: dashed;
    }
    .chart-empty-copy {
      position: absolute;
      inset: 0;
      display: grid;
      place-items: center;
      color: var(--muted);
      font-size: 13px;
      text-align: center;
      padding: 12px 48px;
      pointer-events: none;
    }
    .chart-legend {
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      margin-top: 12px;
      font-size: 12px;
      color: var(--muted);
    }
    .legend-key {
      display: inline-flex;
      gap: 8px;
      align-items: center;
    }
    .legend-line {
      width: 18px;
      height: 3px;
      border-radius: 999px;
      background: currentColor;
    }
    .activity-log {
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px solid var(--line);
    }
    .activity-log h4 {
      margin: 0 0 8px;
      font-size: 13px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .activity-list {
      margin: 0;
      padding-left: 18px;
      display: grid;
      gap: 6px;
      color: var(--ink);
      font-size: 13px;
    }
    @media (max-width: 1180px) {
      .hero, .panel-grid { grid-template-columns: 1fr; }
      .wide { grid-column: span 1; }
      .ops-grid { grid-template-columns: 1fr 1fr; }
      .readiness-strip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .deployment-layout,
      .research-layout,
      .chart-grid { grid-template-columns: 1fr; }
      .candidate-grid,
      .catalog-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 760px) {
      .shell { width: min(100vw - 20px, 100%); margin: 12px auto 28px; }
      .summary-grid, .split, .chart-meta, .research-summary-grid, .candidate-metrics { grid-template-columns: 1fr; }
      .readiness-strip, .deployment-param-grid { grid-template-columns: 1fr; }
      .ops-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="hero-card">
        <div class="toolbar" style="align-items:flex-start">
          <div>
            <div class="eyebrow" data-i18n="hero.eyebrow">BinanceTrade Runtime</div>
            <h1 data-i18n="hero.title">Local Ops Dashboard</h1>
            <p data-i18n="hero.copy">A two-panel workspace for research and live trading. Configure a deployable budget, launch a daemon, and watch strategy curves, metrics, and actions update from the same local state.</p>
            <div class="hero-actions">
              <button type="button" class="primary-action" onclick="switchPanel('automation')" data-i18n="hero.configure">Configure Automated Trading</button>
              <button type="button" class="secondary-action" onclick="switchPanel('status')" data-i18n="hero.status">View Current Status</button>
            </div>
          </div>
          <button type="button" class="lang-toggle" id="langToggle" onclick="toggleLocale()">中文</button>
        </div>
        <div class="status-badge"><span id="generatedAt">Waiting for first snapshot…</span></div>
      </div>
      <div class="summary-grid" id="summaryGrid"></div>
    </section>

    <nav class="panel-tabs" aria-label="Dashboard panels">
      <button type="button" id="tabStatus" class="panel-tab active" onclick="switchPanel('status')" data-i18n="tabs.status">Status</button>
      <button type="button" id="tabAutomation" class="panel-tab" onclick="switchPanel('automation')" data-i18n="tabs.automation">Automation</button>
      <button type="button" id="tabResearch" class="panel-tab" onclick="switchPanel('research')" data-i18n="tabs.research">Strategy Lab</button>
    </nav>

    <section class="panel-grid">
      <section class="panel wide" data-panel="status">
        <div class="toolbar">
          <h2 data-i18n="status.title">Current Status</h2>
          <div class="subtle" data-i18n="status.subtitle">Read-only launch state. Starting the dashboard does not start trading.</div>
        </div>
        <div class="readiness-strip" id="readinessStrip"></div>
      </section>

      <section class="panel" data-panel="status">
        <div class="toolbar">
          <h2 data-i18n="stacks.title">Stacks</h2>
          <div class="subtle" id="environmentLabel"></div>
        </div>
        <div class="list" id="stackList"></div>
      </section>

      <section class="panel" data-panel="status">
        <div class="toolbar">
          <h2 data-i18n="services.title">Services</h2>
          <div class="subtle" data-i18n="services.subtitle">Daemon members and latest heartbeat</div>
        </div>
        <div class="list" id="serviceList"></div>
      </section>

      <section class="panel wide" data-panel="automation">
        <div class="toolbar">
          <h2 data-i18n="deployment.title">Automation Config</h2>
          <div class="subtle" id="deploymentStatus" data-i18n="deployment.subtitle">Choose a stack, review the historical budget suggestion for each strategy, then start the daemon.</div>
        </div>
        <div class="split deployment-layout">
          <article class="ops-card">
            <h3 data-i18n="deployment.plan">Deployment Plan</h3>
            <div class="ops-form">
              <label data-i18n-label="deployment.stack">
                <span data-i18n="deployment.stack">Runtime Stack</span>
                <select id="liveStackPath"></select>
              </label>
              <label data-i18n-label="deployment.totalBudget">
                <span data-i18n="deployment.totalBudget">Total Budget (USDT)</span>
                <input id="deploymentTotalBudget" value="0" />
              </label>
              <div id="deploymentProfiles"></div>
              <div class="ops-row">
                <button type="button" onclick="applySuggestedBudgets()" data-i18n="deployment.useSuggested">Use Historical Defaults</button>
                <button type="button" onclick="startLiveDeployment()" data-i18n="deployment.start">Start Daemon</button>
                <button type="button" onclick="stopLiveDeployment()" data-i18n="deployment.stop">Stop Stack</button>
              </div>
              <div class="subtle" id="deploymentHint" data-i18n="deployment.hint">Each budget is editable. The default suggestion is computed from recent historical replay for the profile when possible.</div>
            </div>
          </article>
          <article class="ops-card" id="liveOverviewPanel"></article>
        </div>
      </section>

      <section class="panel wide" data-panel="automation status">
        <div class="toolbar">
          <h2 data-i18n="liveCharts.title">Live Strategy Charts</h2>
          <div class="subtle" data-i18n="liveCharts.subtitle">The chart stays as a blank coordinate frame until a daemon is running. Once started, each profile begins monitoring and the curve starts plotting live points.</div>
        </div>
        <div class="chart-grid" id="strategyChartList"></div>
      </section>

      <section class="panel wide" data-panel="status">
        <div class="toolbar">
          <h2 data-i18n="statusLog.title">Runtime Log</h2>
          <div class="subtle" data-i18n="statusLog.subtitle">Condensed daemon, order, reconcile, and user-stream status.</div>
        </div>
        <div class="status-log" id="statusLogPanel"></div>
      </section>

      <section class="panel wide" data-panel="status automation research">
        <div class="response-box" id="actionResponse"><h3 data-i18n="advanced.actionCenter">Action Center</h3><div class="subtle" data-i18n="advanced.actionEmpty">No control action executed yet.</div></div>
      </section>

      <section class="panel wide" data-panel="automation research">
        <div class="toolbar">
          <h2 data-i18n="research.title">Strategy Market</h2>
          <div class="subtle" id="researchStatus" data-i18n="research.statusEmpty">No strategy scan has been run from the dashboard yet.</div>
        </div>
        <div class="research-layout">
          <article class="research-block">
            <h3 data-i18n="research.scan">Research Scan</h3>
            <div class="ops-form">
              <label>
                <span data-i18n="research.market">Market</span>
                <select id="researchMarket">
                  <option value="spot">spot</option>
                  <option value="futures">futures</option>
                </select>
              </label>
              <label>
                <span data-i18n="research.universe">Universe</span>
                <select id="researchCategory"></select>
              </label>
              <label>
                <span data-i18n="research.symbol">Symbol</span>
                <input id="researchSymbol" value="BTCUSDT" />
              </label>
              <label>
                <span data-i18n="research.interval">Interval</span>
                <input id="researchInterval" value="15m" />
              </label>
              <label>
                <span data-i18n="research.bars">Bars</span>
                <input id="researchBars" value="1500" />
              </label>
              <label>
                <span data-i18n="research.budget">Budget</span>
                <input id="researchCapital" value="1000" />
              </label>
              <label>
                <span data-i18n="research.allocationMode">Allocation Mode</span>
                <select id="researchAllocationMode">
                  <option value="auto">auto from history</option>
                  <option value="manual">manual weights</option>
                </select>
              </label>
              <label>
                <span data-i18n="research.manualWeights">Manual Weights</span>
                <input id="researchManualWeights" value="" placeholder="sma_crossover=40, rsi_regime=35, ichimoku_trend=25" />
              </label>
              <label>
                <span data-i18n="research.fee">Fee (bps)</span>
                <input id="researchFeeBps" value="10" />
              </label>
              <label>
                <span data-i18n="research.slippage">Slippage (bps)</span>
                <input id="researchSlippageBps" value="2" />
              </label>
              <label>
                <span data-i18n="research.leverage">Leverage</span>
                <input id="researchLeverage" value="1" />
              </label>
              <label>
                <span data-i18n="research.positionFraction">Position Fraction</span>
                <input id="researchPositionFraction" value="1" />
              </label>
              <div class="ops-row">
                <button type="button" onclick="runResearchScan()" data-i18n="research.run">Run Strategy Scan</button>
              </div>
              <div class="subtle" id="intervalHint">15m means each K-line covers 15 minutes. Increase Bars to simulate a longer period.</div>
            </div>
          </article>

          <div>
            <div class="research-block" id="researchSummaryPanel"></div>
            <div class="candidate-grid" id="researchAllocations"></div>
            <div class="candidate-grid" id="researchSecondary"></div>
          </div>
        </div>
      </section>

      <section class="panel" data-panel="status">
        <div class="toolbar">
          <h2 data-i18n="portfolio.title">Portfolio</h2>
          <div class="subtle" id="portfolioTime"></div>
        </div>
        <div id="portfolioPanel"></div>
      </section>

      <section class="panel wide" data-panel="status">
        <div class="toolbar">
          <h2 data-i18n="advanced.title">Advanced Operations</h2>
          <div class="subtle" data-i18n="advanced.subtitle">Manual runtime controls and wallet actions are still available here, but the primary workflow lives in the Live Deployment panel.</div>
        </div>
        <details class="advanced-details">
          <summary data-i18n="advanced.open">Open advanced runtime controls</summary>
          <div class="ops-grid" style="margin-top:18px">
          <article class="ops-card">
            <h3 data-i18n="advanced.stackControl">Stack Control</h3>
            <div class="ops-form">
              <label>
                <span data-i18n="deployment.stack">Runtime Stack</span>
                <select id="stackPath"></select>
              </label>
              <div class="ops-row">
                <button type="button" onclick="runStackAction('start_stack')" data-i18n="advanced.startStack">Start Stack</button>
                <button type="button" onclick="runStackAction('stop_stack')" data-i18n="advanced.stopStack">Stop Stack</button>
                <button type="button" onclick="runStackAction('doctor_stack')" data-i18n="advanced.doctorStack">Doctor Stack</button>
              </div>
            </div>
          </article>

          <article class="ops-card">
            <h3 data-i18n="advanced.profileControl">Profile Control</h3>
            <div class="ops-form">
              <label>
                <span data-i18n="advanced.runtimeProfile">Runtime Profile</span>
                <select id="profilePath"></select>
              </label>
              <div class="ops-row">
                <button type="button" onclick="runProfileAction('start_profile')" data-i18n="advanced.startProfile">Start Profile</button>
                <button type="button" onclick="runProfileAction('stop_profile')" data-i18n="advanced.stopProfile">Stop Profile</button>
                <button type="button" onclick="runProfileAction('doctor_profile')" data-i18n="advanced.doctorProfile">Doctor Profile</button>
              </div>
            </div>
          </article>

          <article class="ops-card">
            <h3 data-i18n="advanced.spotOps">Spot Runtime Ops</h3>
            <div class="ops-form">
              <label>
                <span data-i18n="research.symbol">Symbol</span>
                <input id="reconcileSymbol" value="BTCUSDT" />
              </label>
              <div class="ops-row">
                <button type="button" onclick="reconcileSpot()" data-i18n="advanced.reconcileSpot">Reconcile Spot</button>
                <button type="button" onclick="refreshPortfolio()" data-i18n="advanced.refreshPortfolio">Refresh Portfolio Hint</button>
              </div>
            </div>
          </article>

          <article class="ops-card">
            <h3 data-i18n="advanced.manualOrder">Manual Spot Order</h3>
            <div class="ops-form">
              <label>
                <span data-i18n="research.symbol">Symbol</span>
                <input id="orderSymbol" value="BTCUSDT" />
              </label>
              <label>
                <span data-i18n="advanced.submissionMode">Submission Mode</span>
                <select id="orderMode">
                  <option value="DRY_RUN">DRY_RUN</option>
                  <option value="TEST">TEST</option>
                  <option value="LIVE">LIVE</option>
                </select>
              </label>
              <label>
                <span data-i18n="advanced.buyQuoteQty">Buy Quote Qty</span>
                <input id="buyQuoteQty" value="25" />
              </label>
              <label>
                <span data-i18n="advanced.sellQuantity">Sell Quantity</span>
                <input id="sellQuantity" value="0.001" />
              </label>
              <div class="ops-row">
                <button type="button" onclick="buySpotMarket()" data-i18n="advanced.buyMarket">Buy Market</button>
                <button type="button" onclick="sellSpotMarket()" data-i18n="advanced.sellMarket">Sell Market</button>
              </div>
            </div>
          </article>

          <article class="ops-card">
            <h3 data-i18n="advanced.earnToSpot">Earn to Spot</h3>
            <div class="ops-form">
              <label>
                <span data-i18n="advanced.asset">Asset</span>
                <input id="redeemAsset" value="USDT" />
              </label>
              <label>
                <span data-i18n="advanced.amount">Amount</span>
                <input id="redeemAmount" value="100" />
              </label>
              <label>
                <span data-i18n="advanced.productId">Product Id (optional)</span>
                <input id="redeemProductId" value="" placeholder="auto-resolve from flexible position" />
              </label>
              <label>
                <span data-i18n="advanced.destination">Destination</span>
                <input id="redeemDestAccount" value="SPOT" />
              </label>
              <label>
                <span data-i18n="advanced.confirmPhrase">Confirm Phrase</span>
                <input id="redeemConfirmText" value="" placeholder='type REDEEM to execute' />
              </label>
              <div class="ops-row">
                <button type="button" onclick="redeemEarnFlexible()" data-i18n="advanced.redeemFlexible">Redeem Flexible</button>
              </div>
              <div class="subtle" data-i18n="advanced.redeemHint">This is a real wallet action on mainnet. Enter <code>REDEEM</code> exactly to allow it.</div>
            </div>
          </article>
          </div>
        </details>
      </section>

      <section class="panel wide" data-panel="status">
        <div class="toolbar">
          <h2 data-i18n="orders.title">Recent Orders</h2>
          <div class="subtle" data-i18n="orders.subtitle">Newest first</div>
        </div>
        <div id="ordersPanel"></div>
      </section>

      <section class="panel wide" data-panel="status">
        <details class="advanced-details">
          <summary data-i18n="advanced.diagnostics">Open raw diagnostics</summary>
          <div class="split" style="margin-top:14px">
            <div>
              <div class="toolbar">
                <h2 data-i18n="events.title">Recent Events</h2>
                <div class="subtle" data-i18n="events.subtitle">User stream and reconciler activity</div>
              </div>
              <div id="eventsPanel"></div>
            </div>
            <div>
              <div class="toolbar">
                <h2 data-i18n="files.title">Runtime Files</h2>
                <div class="subtle" data-i18n="files.subtitle">Mirrored JSON heartbeat files</div>
              </div>
              <div class="list" id="runtimeFiles"></div>
            </div>
          </div>
        </details>
      </section>
    </section>
  </div>

  <script>
    const REFRESH_SECONDS = __REFRESH_SECONDS__;
    let currentControls = null;
    let currentSnapshot = null;
    let currentLocale = window.localStorage.getItem("dashboardLocale") || "en";
    let currentPanel = window.localStorage.getItem("dashboardPanel") || "status";
    let deploymentDraft = { stackPath: null, totalBudget: null, profileBudgets: {}, profileParams: {} };
    const sectionHashes = {};
    const I18N = {
      en: {
        "hero.eyebrow": "BinanceTrade Runtime",
        "hero.title": "Ops Terminal",
        "hero.copy": "Dashboard boot is passive: it only opens this local console and reads current state. Trading starts only after you explicitly configure and launch automation.",
        "hero.configure": "Configure Automated Trading",
        "hero.status": "View Current Status",
        "tabs.status": "Status",
        "tabs.automation": "Automation",
        "tabs.research": "Strategy Lab",
        "status.title": "Current Status",
        "status.subtitle": "Read-only launch state. Starting the dashboard does not start trading.",
        "stacks.title": "Stacks",
        "services.title": "Services",
        "services.subtitle": "Daemon members and latest heartbeat",
        "deployment.title": "Automation Config",
        "deployment.subtitle": "Tune strategy parameters and budgets first. The daemon starts only when you click Start Daemon.",
        "deployment.plan": "Strategy Parameter Panel",
        "deployment.stack": "Runtime Stack",
        "deployment.totalBudget": "Total Budget (USDT)",
        "deployment.useSuggested": "Use Historical Defaults",
        "deployment.start": "Start Daemon",
        "deployment.stop": "Stop Stack",
        "deployment.hint": "Each budget is editable. The default suggestion is computed from recent historical replay for the profile when possible.",
        "liveCharts.title": "Live Strategy Charts",
        "liveCharts.subtitle": "The chart stays as a blank coordinate frame until a daemon is running. Once started, each profile begins monitoring and the curve starts plotting live points.",
        "statusLog.title": "Runtime Log",
        "statusLog.subtitle": "Condensed daemon, order, reconcile, and user-stream status.",
        "research.title": "Strategy Market",
        "research.statusEmpty": "No strategy scan has been run from the dashboard yet.",
        "research.scan": "Research Scan",
        "research.market": "Market",
        "research.universe": "Universe",
        "research.symbol": "Symbol",
        "research.interval": "Interval",
        "research.bars": "Bars",
        "research.budget": "Budget",
        "research.allocationMode": "Allocation Mode",
        "research.manualWeights": "Manual Weights",
        "research.fee": "Fee (bps)",
        "research.slippage": "Slippage (bps)",
        "research.leverage": "Leverage",
        "research.positionFraction": "Position Fraction",
        "research.run": "Run Strategy Scan",
        "research.generateStack": "Generate Deployable Stack",
        "research.generatedStack": "Latest generated stack",
        "portfolio.title": "Portfolio",
        "advanced.title": "Advanced Operations",
        "advanced.subtitle": "Manual runtime controls and wallet actions are still available here, but the primary workflow lives in the Live Deployment panel.",
        "advanced.open": "Open advanced runtime controls",
        "advanced.diagnostics": "Open raw diagnostics",
        "advanced.stackControl": "Stack Control",
        "advanced.profileControl": "Profile Control",
        "advanced.runtimeProfile": "Runtime Profile",
        "advanced.startStack": "Start Stack",
        "advanced.stopStack": "Stop Stack",
        "advanced.doctorStack": "Doctor Stack",
        "advanced.startProfile": "Start Profile",
        "advanced.stopProfile": "Stop Profile",
        "advanced.doctorProfile": "Doctor Profile",
        "advanced.spotOps": "Spot Runtime Ops",
        "advanced.reconcileSpot": "Reconcile Spot",
        "advanced.refreshPortfolio": "Refresh Portfolio Hint",
        "advanced.manualOrder": "Manual Spot Order",
        "advanced.submissionMode": "Submission Mode",
        "advanced.buyQuoteQty": "Buy Quote Qty",
        "advanced.sellQuantity": "Sell Quantity",
        "advanced.buyMarket": "Buy Market",
        "advanced.sellMarket": "Sell Market",
        "advanced.earnToSpot": "Earn to Spot",
        "advanced.asset": "Asset",
        "advanced.amount": "Amount",
        "advanced.productId": "Product Id (optional)",
        "advanced.destination": "Destination",
        "advanced.confirmPhrase": "Confirm Phrase",
        "advanced.redeemFlexible": "Redeem Flexible",
        "advanced.redeemHint": "This is a real wallet action on mainnet. Enter REDEEM exactly to allow it.",
        "advanced.actionCenter": "Action Center",
        "advanced.actionEmpty": "No control action executed yet.",
        "orders.title": "Recent Orders",
        "orders.subtitle": "Newest first",
        "events.title": "Recent Events",
        "events.subtitle": "User stream and reconciler activity",
        "files.title": "Runtime Files",
        "files.subtitle": "Mirrored JSON heartbeat files",
        "live.placeholder": "Waiting for daemon start. This chart will begin plotting once the strategy is monitoring live data.",
        "live.activity": "Recent Strategy Actions",
        "live.overview": "Live Launch Summary",
        "live.overview.copy": "Use this view to budget each strategy, watch daemon health, and keep the active budget close to your spot balance.",
        "live.stackStatus": "Stack Status",
        "live.serviceCount": "Profiles",
        "live.spotBudget": "Spot Budget",
        "live.liveProfiles": "Live Profiles",
        "live.planBudget": "Planned Budget",
        "live.historicalWeight": "Historical Weight",
        "live.historyBasis": "Historical Basis",
        "live.interval": "Interval",
        "live.lastPrice": "Last Price",
        "live.position": "Position",
        "live.pendingOrder": "Pending Order",
        "live.lastUpdate": "Last Update",
        "live.state": "State",
        "live.noActivity": "No recent live actions yet. Start the daemon and wait for the next signal.",
        "live.mode": "Mode",
        "live.bars": "Bars",
        "live.chartRangeHigh": "Chart Max",
        "live.chartRangeMid": "Chart Mid",
        "live.chartRangeLow": "Chart Min",
        "live.lastCloseTime": "Last Close Time",
      },
      zh: {
        "hero.eyebrow": "BinanceTrade 运行时",
        "hero.title": "本地 Ops 终端",
        "hero.copy": "启动 dashboard 是被动的：它只打开本地网页并读取当前状态。只有你显式配置并点击启动自动化后，才会启动交易 daemon。",
        "hero.configure": "配置自动化交易",
        "hero.status": "查看当前状态",
        "tabs.status": "状态",
        "tabs.automation": "自动化交易",
        "tabs.research": "策略实验室",
        "status.title": "当前状态",
        "status.subtitle": "只读启动状态。运行 dashboard 命令本身不会开始交易。",
        "stacks.title": "组合栈",
        "services.title": "策略实例",
        "services.subtitle": "daemon 成员与最近心跳",
        "deployment.title": "自动化配置",
        "deployment.subtitle": "先调策略参数和预算。只有点击启动 Daemon 后才会进入自动监控/交易。",
        "deployment.plan": "策略参数面板",
        "deployment.stack": "运行栈",
        "deployment.totalBudget": "总预算（USDT）",
        "deployment.useSuggested": "使用历史建议",
        "deployment.start": "启动 Daemon",
        "deployment.stop": "停止 Stack",
        "deployment.hint": "每个策略额度都可以手动调整；默认建议会尽量根据该 profile 的近期历史回放自动生成。",
        "liveCharts.title": "实时策略曲线",
        "liveCharts.subtitle": "启动前这里会显示空白坐标图。启动 daemon 后，每个策略开始监控，曲线会随着实时状态逐步打点。",
        "statusLog.title": "运行日志",
        "statusLog.subtitle": "简洁汇总 daemon、订单、对账和用户流状态。",
        "research.title": "策略市场",
        "research.statusEmpty": "还没有从 dashboard 发起过策略扫描。",
        "research.scan": "研究扫描",
        "research.market": "市场",
        "research.universe": "策略集合",
        "research.symbol": "交易对",
        "research.interval": "周期",
        "research.bars": "K 线数量",
        "research.budget": "预算",
        "research.allocationMode": "分配方式",
        "research.manualWeights": "手动权重",
        "research.fee": "手续费（bps）",
        "research.slippage": "滑点（bps）",
        "research.leverage": "杠杆",
        "research.positionFraction": "仓位比例",
        "research.run": "运行策略扫描",
        "research.generateStack": "生成可部署 Stack",
        "research.generatedStack": "最近生成的 Stack",
        "portfolio.title": "资产总览",
        "advanced.title": "高级操作",
        "advanced.subtitle": "手动运行控制、钱包划转和临时下单仍然保留在这里，但日常主流程应该放在上面的实时部署面板。",
        "advanced.open": "展开高级运行控制",
        "advanced.diagnostics": "展开原始诊断信息",
        "advanced.stackControl": "Stack 控制",
        "advanced.profileControl": "Profile 控制",
        "advanced.runtimeProfile": "运行 Profile",
        "advanced.startStack": "启动 Stack",
        "advanced.stopStack": "停止 Stack",
        "advanced.doctorStack": "体检 Stack",
        "advanced.startProfile": "启动 Profile",
        "advanced.stopProfile": "停止 Profile",
        "advanced.doctorProfile": "体检 Profile",
        "advanced.spotOps": "现货运行维护",
        "advanced.reconcileSpot": "现货对账",
        "advanced.refreshPortfolio": "刷新资产提示",
        "advanced.manualOrder": "手动现货下单",
        "advanced.submissionMode": "提交模式",
        "advanced.buyQuoteQty": "买入金额",
        "advanced.sellQuantity": "卖出数量",
        "advanced.buyMarket": "市价买入",
        "advanced.sellMarket": "市价卖出",
        "advanced.earnToSpot": "理财转现货",
        "advanced.asset": "资产",
        "advanced.amount": "数量",
        "advanced.productId": "产品 ID（可选）",
        "advanced.destination": "目标账户",
        "advanced.confirmPhrase": "确认短语",
        "advanced.redeemFlexible": "赎回活期",
        "advanced.redeemHint": "这是主网真实钱包动作。只有完整输入 REDEEM 才会放行。",
        "advanced.actionCenter": "操作反馈",
        "advanced.actionEmpty": "还没有执行任何控制动作。",
        "orders.title": "最近订单",
        "orders.subtitle": "最新记录优先",
        "events.title": "最近事件",
        "events.subtitle": "用户流与对账活动",
        "files.title": "运行时文件",
        "files.subtitle": "镜像心跳 JSON 文件",
        "live.placeholder": "等待启动 daemon。策略一旦进入实时监控，这张图就会开始打点。",
        "live.activity": "最近策略动作",
        "live.overview": "实时部署摘要",
        "live.overview.copy": "在这里设置每个策略预算、查看 daemon 健康状态，并让计划预算与现货余额保持一致。",
        "live.stackStatus": "Stack 状态",
        "live.serviceCount": "策略数量",
        "live.spotBudget": "现货预算",
        "live.liveProfiles": "LIVE Profile 数",
        "live.planBudget": "计划额度",
        "live.historicalWeight": "历史建议权重",
        "live.historyBasis": "历史依据",
        "live.interval": "周期",
        "live.lastPrice": "最新价格",
        "live.position": "当前仓位",
        "live.pendingOrder": "待处理订单",
        "live.lastUpdate": "最近更新时间",
        "live.state": "状态",
        "live.noActivity": "暂时还没有实时操作记录。启动 daemon 后等待下一个信号即可。",
        "live.mode": "模式",
        "live.bars": "K 线数",
        "live.chartRangeHigh": "图上沿",
        "live.chartRangeMid": "图中位",
        "live.chartRangeLow": "图下沿",
        "live.lastCloseTime": "最近收盘时间",
      },
    };

    function t(key) {
      return (I18N[currentLocale] && I18N[currentLocale][key]) || I18N.en[key] || key;
    }

    function switchPanel(panel) {
      currentPanel = ["status", "automation", "research"].includes(panel) ? panel : "status";
      window.localStorage.setItem("dashboardPanel", currentPanel);
      document.querySelectorAll("[data-panel]").forEach((node) => {
        const panels = String(node.dataset.panel || "").split(/\\s+/);
        node.classList.toggle("is-hidden", !panels.includes(currentPanel));
      });
      document.getElementById("tabStatus").classList.toggle("active", currentPanel === "status");
      document.getElementById("tabAutomation").classList.toggle("active", currentPanel === "automation");
      document.getElementById("tabResearch").classList.toggle("active", currentPanel === "research");
    }

    function applyStaticTranslations() {
      document.querySelectorAll("[data-i18n]").forEach((node) => {
        node.innerHTML = escapeHtml(t(node.dataset.i18n)).replaceAll("&lt;code&gt;", "<code>").replaceAll("&lt;/code&gt;", "</code>");
      });
      const toggle = document.getElementById("langToggle");
      if (toggle) toggle.textContent = currentLocale === "en" ? "中文" : "EN";
    }

    function setLocale(locale) {
      currentLocale = locale === "zh" ? "zh" : "en";
      window.localStorage.setItem("dashboardLocale", currentLocale);
      applyStaticTranslations();
      if (currentSnapshot) {
        renderCurrentSnapshot(currentSnapshot, { force: true });
      } else {
        switchPanel(currentPanel);
      }
    }

    function toggleLocale() {
      setLocale(currentLocale === "en" ? "zh" : "en");
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function pillClass(status, healthy) {
      if (healthy === true) return "pill ok";
      if (String(status).includes("RUN")) return "pill warn";
      if (String(status).includes("STOP")) return "pill";
      return "pill err";
    }

    function formatNumber(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "—";
      const locale = currentLocale === "zh" ? "zh-CN" : "en-US";
      if (Math.abs(numeric) >= 1000) return numeric.toLocaleString(locale, { maximumFractionDigits: 2 });
      if (Math.abs(numeric) >= 1) return numeric.toLocaleString(locale, { maximumFractionDigits: 4 });
      return numeric.toLocaleString(locale, { maximumFractionDigits: 8 });
    }

    function formatPercent(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "—";
      return `${numeric.toFixed(2)}%`;
    }

    function formatMoney(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "—";
      const locale = currentLocale === "zh" ? "zh-CN" : "en-US";
      return `$${numeric.toLocaleString(locale, { maximumFractionDigits: 2 })}`;
    }

    function formatTime(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric) || numeric <= 0) return "—";
      const locale = currentLocale === "zh" ? "zh-CN" : "en-US";
      return new Date(numeric).toLocaleString(locale);
    }

    function lastValue(values) {
      if (!Array.isArray(values) || !values.length) return null;
      return values[values.length - 1];
    }

    function polylinePoints(values, minValue, maxValue, width, height, padX, padY) {
      if (!Array.isArray(values) || values.length < 2) return "";
      const span = maxValue - minValue || 1;
      const usableWidth = width - (padX * 2);
      const usableHeight = height - (padY * 2);
      return values.map((value, index) => {
        const x = padX + ((usableWidth * index) / Math.max(values.length - 1, 1));
        const y = padY + ((maxValue - Number(value)) / span) * usableHeight;
        return `${x.toFixed(2)},${y.toFixed(2)}`;
      }).join(" ");
    }

    function yGuide(value, minValue, maxValue, height, padY) {
      const span = maxValue - minValue || 1;
      const usableHeight = height - (padY * 2);
      const y = padY + ((maxValue - value) / span) * usableHeight;
      return y.toFixed(2);
    }

    function updateSection(key, data, renderer) {
      const next = JSON.stringify(data ?? null);
      if (sectionHashes[key] === next) return;
      sectionHashes[key] = next;
      renderer(data);
    }

    function metric(label, value, sub) {
      return `
        <article class="metric">
          <div class="metric-label">${escapeHtml(label)}</div>
          <div class="metric-value">${escapeHtml(value)}</div>
          <div class="metric-sub">${escapeHtml(sub)}</div>
        </article>
      `;
    }

    function renderSummary(summary) {
      document.getElementById("summaryGrid").innerHTML = [
        metric(t("stacks.title"), `${summary.healthy_stack_count}/${summary.stack_count}`, t("services.subtitle")),
        metric(t("services.title"), `${summary.healthy_service_count}/${summary.service_count}`, t("services.subtitle")),
        metric(t("orders.title"), `${summary.open_order_count}/${summary.order_count}`, t("orders.subtitle")),
        metric(t("events.title"), `${summary.event_count}`, t("events.subtitle")),
      ].join("");
    }

    function renderReadiness(data) {
      const node = document.getElementById("readinessStrip");
      if (!node) return;
      const summary = data?.summary || {};
      const portfolio = data?.portfolio || {};
      const spotUsdt = (portfolio.spot_balances || []).find((row) => String(row.asset || "").toUpperCase() === "USDT");
      const earnUsdt = (portfolio.earn_positions || []).find((row) => String(row.asset || "").toUpperCase() === "USDT");
      const running = Number(summary.healthy_service_count || 0);
      const openOrders = Number(summary.open_order_count || 0);
      const environment = data?.environment || "—";
      node.innerHTML = [
        `<div class="readiness-card"><span>mode</span><strong>${escapeHtml(environment)}</strong><div class="subtle">dashboard passive</div></div>`,
        `<div class="readiness-card"><span>daemon</span><strong>${escapeHtml(running ? `${running} running` : "idle")}</strong><div class="subtle">no auto-start from dashboard boot</div></div>`,
        `<div class="readiness-card"><span>spot usdt</span><strong>${escapeHtml(spotUsdt ? formatNumber(spotUsdt.free) : "—")}</strong><div class="subtle">available for direct spot orders</div></div>`,
        `<div class="readiness-card"><span>orders</span><strong>${escapeHtml(openOrders)}</strong><div class="subtle">${escapeHtml(earnUsdt ? `Earn USDT ${formatNumber(earnUsdt.total_amount)}` : "no open local orders")}</div></div>`,
      ].join("");
    }

    function setOptions(selectId, rows, valueKey = "path", labelKey = "name") {
      const select = document.getElementById(selectId);
      if (!select) return;
      const current = select.value;
      select.innerHTML = rows.map((row) => `
        <option value="${escapeHtml(row[valueKey])}">${escapeHtml(row[labelKey])}</option>
      `).join("");
      if (current && rows.some((row) => row[valueKey] === current)) {
        select.value = current;
      }
    }

    function renderControls(controls) {
      currentControls = controls;
      if (!controls) return;
      setOptions("stackPath", controls.available_stacks || []);
      setOptions("liveStackPath", controls.available_stacks || []);
      setOptions("profilePath", controls.available_profiles || []);
      setOptions("researchCategory", controls.research_categories || [], "key", "label");
      if (!deploymentDraft.stackPath) {
        deploymentDraft.stackPath = document.getElementById("liveStackPath")?.value || controls.available_stacks?.[0]?.path || null;
      }
      const liveStack = document.getElementById("liveStackPath");
      if (liveStack && deploymentDraft.stackPath) {
        liveStack.value = deploymentDraft.stackPath;
      }
    }

    function selectedStack(deployment) {
      const stacks = deployment?.stacks || [];
      if (!stacks.length) return null;
      const selectedPath = deploymentDraft.stackPath || deployment.default_stack_path || stacks[0].path;
      return stacks.find((item) => item.path === selectedPath) || stacks[0];
    }

    function applySuggestedBudgets() {
      if (!currentSnapshot?.deployment?.enabled) return;
      const deployment = currentSnapshot.deployment;
      const stack = selectedStack(deployment);
      if (!stack) return;
      const totalBudgetNode = document.getElementById("deploymentTotalBudget");
      const totalBudget = Number(totalBudgetNode?.value || deployment.default_total_budget || 0);
      deploymentDraft.totalBudget = totalBudget;
      deploymentDraft.profileBudgets = {};
      stack.profiles.forEach((profile) => {
        const suggested = (Number(profile.suggested_weight_pct || 0) / 100) * totalBudget;
        deploymentDraft.profileBudgets[profile.name] = Number.isFinite(suggested) ? suggested : Number(profile.current_budget || 0);
      });
      renderLiveWorkspace(currentSnapshot);
    }

    function updateDeploymentBudget(name, value) {
      deploymentDraft.profileBudgets[name] = Number(value || 0);
    }

    function updateDeploymentParam(profileName, key, value) {
      deploymentDraft.profileParams[profileName] = deploymentDraft.profileParams[profileName] || {};
      deploymentDraft.profileParams[profileName][key] = value;
    }

    function saveProfileParams(profileName, profilePath) {
      const params = deploymentDraft.profileParams?.[profileName] || {};
      if (!Object.keys(params).length) {
        postAction({ action: "update_profile_params", profile_path: profilePath, params: {} });
        return;
      }
      postAction({ action: "update_profile_params", profile_path: profilePath, params });
    }

    function blankChartSvg(width, height, padX, padY) {
      const topY = padY;
      const midY = height / 2;
      const bottomY = height - padY;
      return `
        <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="blank live chart">
          <line x1="${padX}" y1="${topY}" x2="${width - padX}" y2="${topY}" stroke="rgba(23,22,26,0.08)" stroke-width="1" />
          <line x1="${padX}" y1="${midY}" x2="${width - padX}" y2="${midY}" stroke="rgba(23,22,26,0.10)" stroke-width="1" stroke-dasharray="4 6" />
          <line x1="${padX}" y1="${bottomY}" x2="${width - padX}" y2="${bottomY}" stroke="rgba(23,22,26,0.08)" stroke-width="1" />
          <line x1="${padX}" y1="${topY}" x2="${padX}" y2="${bottomY}" stroke="rgba(23,22,26,0.08)" stroke-width="1" />
        </svg>
        <div class="chart-empty-copy">${escapeHtml(t("live.placeholder"))}</div>
      `;
    }

    function renderLiveOverview(deployment, stack) {
      const node = document.getElementById("liveOverviewPanel");
      if (!node) return;
      if (!deployment?.enabled || !stack) {
        node.innerHTML = `<h3>${escapeHtml(t("live.overview"))}</h3><div class="subtle">${escapeHtml(t("live.noActivity"))}</div>`;
        return;
      }
      const runningProfiles = stack.profiles.filter((item) => item.status === "RUNNING").length;
      const liveProfiles = stack.profiles.filter((item) => item.submission_mode === "LIVE").length;
      node.innerHTML = `
        <h3>${escapeHtml(t("live.overview"))}</h3>
        <div class="subtle" style="margin-bottom:12px">${escapeHtml(t("live.overview.copy"))}</div>
        <div class="live-overview-grid">
          <div class="mini-metric"><span>${escapeHtml(t("live.stackStatus"))}</span><strong>${escapeHtml(stack.name)}</strong></div>
          <div class="mini-metric"><span>${escapeHtml(t("live.serviceCount"))}</span><strong>${escapeHtml(stack.profile_count)}</strong></div>
          <div class="mini-metric"><span>${escapeHtml(t("live.spotBudget"))}</span><strong>${escapeHtml(formatMoney(deploymentDraft.totalBudget ?? deployment.default_total_budget ?? 0))}</strong></div>
          <div class="mini-metric"><span>${escapeHtml(t("live.liveProfiles"))}</span><strong>${escapeHtml(`${liveProfiles}/${runningProfiles}`)}</strong></div>
        </div>
      `;
    }

    function renderLiveWorkspace(snapshot) {
      const deployment = snapshot?.deployment;
      const charts = snapshot?.strategy_charts || [];
      const chartNode = document.getElementById("strategyChartList");
      const planNode = document.getElementById("deploymentProfiles");
      if (!chartNode || !planNode) return;
      if (!deployment?.enabled) {
        planNode.innerHTML = `<div class="empty">${escapeHtml(t("live.noActivity"))}</div>`;
        chartNode.innerHTML = `<div class="empty">${escapeHtml(t("live.placeholder"))}</div>`;
        return;
      }

      const stack = selectedStack(deployment);
      if (!stack) {
        planNode.innerHTML = `<div class="empty">${escapeHtml(t("live.noActivity"))}</div>`;
        chartNode.innerHTML = `<div class="empty">${escapeHtml(t("live.placeholder"))}</div>`;
        return;
      }

      if (deploymentDraft.stackPath !== stack.path) {
        deploymentDraft.stackPath = stack.path;
      }
      if (deploymentDraft.totalBudget === null || !Number.isFinite(Number(deploymentDraft.totalBudget))) {
        deploymentDraft.totalBudget = Number(deployment.default_total_budget || 0);
      }
      if (!Object.keys(deploymentDraft.profileBudgets || {}).length) {
        stack.profiles.forEach((profile) => {
          deploymentDraft.profileBudgets[profile.name] = Number(profile.suggested_budget || profile.current_budget || 0);
        });
      }
      stack.profiles.forEach((profile) => {
        deploymentDraft.profileParams[profile.name] = deploymentDraft.profileParams[profile.name] || { ...(profile.params || {}) };
      });

      const totalBudgetInput = document.getElementById("deploymentTotalBudget");
      if (totalBudgetInput && document.activeElement !== totalBudgetInput) {
        totalBudgetInput.value = String(deploymentDraft.totalBudget ?? deployment.default_total_budget ?? 0);
      }
      const deploymentStatus = document.getElementById("deploymentStatus");
      if (deploymentStatus) {
        deploymentStatus.textContent = `${stack.name} · ${stack.profile_count} profiles`;
      }

      planNode.innerHTML = `
        <div class="deployment-profile-grid">
          ${stack.profiles.map((profile) => {
            const params = deploymentDraft.profileParams[profile.name] || profile.params || {};
            const paramRows = Object.entries(params).filter(([key]) => !String(key).startsWith("_")).map(([key, value]) => `
              <label>
                <span>${escapeHtml(key)}</span>
                <input value="${escapeHtml(value)}" oninput="updateDeploymentParam('${escapeHtml(profile.name)}', '${escapeHtml(key)}', this.value)" />
              </label>
            `).join("");
            return `
            <article class="deployment-profile-row">
              <div class="deployment-profile-head">
                <div>
                  <strong>${escapeHtml(profile.name)}</strong>
                  <div class="subtle">${escapeHtml(profile.symbol || "—")} · ${escapeHtml(profile.interval || "—")} · ${escapeHtml(profile.strategy_label || "strategy")}</div>
                </div>
                <span class="${pillClass(profile.status || "STOPPED", profile.status === "RUNNING")}">${escapeHtml(profile.status || "STOPPED")}</span>
              </div>
              <label>
                <span>${escapeHtml(t("live.planBudget"))}</span>
                <input value="${escapeHtml(formatNumber(deploymentDraft.profileBudgets[profile.name] ?? profile.suggested_budget ?? 0))}" oninput="updateDeploymentBudget('${escapeHtml(profile.name)}', this.value)" />
              </label>
              <div class="deployment-profile-meta">
                <div><span>${escapeHtml(t("live.historicalWeight"))}</span>${escapeHtml(formatPercent(profile.suggested_weight_pct))}</div>
                <div><span>${escapeHtml(t("live.mode"))}</span>${escapeHtml(profile.submission_mode || "PLANNED")}</div>
              </div>
              <div class="deployment-param-grid">${paramRows}</div>
              <div class="ops-row" style="margin-top:10px">
                <button type="button" onclick="saveProfileParams('${escapeHtml(profile.name)}', '${escapeHtml(profile.path)}')">Save Params</button>
              </div>
              <div class="subtle" style="margin-top:8px">${escapeHtml(profile.history_basis || t("live.noActivity"))}</div>
            </article>
          `}).join("")}
        </div>
      `;

      const chartByService = Object.fromEntries(charts.map((item) => [item.service_name, item]));
      chartNode.innerHTML = stack.profiles.map((profile) => renderLiveStrategyCard(profile, chartByService[profile.name])).join("");
      renderLiveOverview(deployment, stack);
    }

    function renderStacks(stacks) {
      const node = document.getElementById("stackList");
      if (!stacks.length) {
        node.innerHTML = `<div class="empty">${escapeHtml(t("live.noActivity"))}</div>`;
        return;
      }
      node.innerHTML = stacks.map((item) => `
        <article class="stack-card">
          <div class="stack-head">
            <div class="title-row">
              <strong>${escapeHtml(item.stack_name)}</strong>
              <span class="${pillClass(item.status, item.healthy)}">${escapeHtml(item.status)}</span>
            </div>
            <div class="subtle">${escapeHtml(item.healthy_profile_count)} / ${escapeHtml(item.profile_count)} healthy</div>
          </div>
          <div class="meta-grid">
            <div><span>Started</span>${escapeHtml(item.started_at)}</div>
            <div><span>${escapeHtml(t("live.lastUpdate"))}</span>${escapeHtml(item.updated_at)}</div>
            <div><span>Heartbeat</span>${escapeHtml(item.last_heartbeat_at)}</div>
            <div><span>Reason</span>${escapeHtml(item.reason || item.error || "—")}</div>
          </div>
          ${item.members.length ? `<div class="list" style="margin-top:12px">${item.members.map((member) => `
            <div class="runtime-file">
              <div class="service-head">
                <div class="title-row">
                  <strong>${escapeHtml(member.service_name)}</strong>
                  <span class="${pillClass(member.status, member.healthy)}">${escapeHtml(member.status || "UNKNOWN")}</span>
                </div>
                <div class="subtle">restarts ${escapeHtml(member.restart_count ?? 0)}</div>
              </div>
              <div class="subtle">${escapeHtml(member.reason || member.error || member.updated_at || "—")}</div>
            </div>
          `).join("")}</div>` : ""}
        </article>
      `).join("");
    }

    function renderServices(services) {
      const node = document.getElementById("serviceList");
      if (!services.length) {
        node.innerHTML = `<div class="empty">${escapeHtml(t("live.noActivity"))}</div>`;
        return;
      }
      node.innerHTML = services.map((item) => `
        <article class="service-card">
          <div class="service-head">
            <div class="title-row">
              <strong>${escapeHtml(item.service_name)}</strong>
              <span class="${pillClass(item.status, item.healthy)}">${escapeHtml(item.status)}</span>
              <span class="pill">${escapeHtml(item.submission_mode)}</span>
            </div>
            <div class="subtle">restarts ${escapeHtml(item.restart_count)}</div>
          </div>
          <div class="meta-grid">
            <div><span>Strategy</span>${escapeHtml(item.strategy_label)}</div>
            <div><span>${escapeHtml(t("research.market"))}</span>${escapeHtml(item.market_type)}</div>
            <div><span>${escapeHtml(t("research.symbol"))}</span>${escapeHtml(item.symbol || "—")}</div>
            <div><span>${escapeHtml(t("live.interval"))}</span>${escapeHtml(item.interval || "—")}</div>
            <div><span>Heartbeat</span>${escapeHtml(item.last_heartbeat_at)}</div>
            <div><span>${escapeHtml(t("live.lastUpdate"))}</span>${escapeHtml(item.updated_at)}</div>
            <div><span>Actions</span>${escapeHtml(item.actions ?? 0)}</div>
            <div><span>${escapeHtml(t("live.position"))}</span>${escapeHtml(item.ctx_state?.position_qty ?? "—")}</div>
            <div><span>${escapeHtml(t("live.pendingOrder"))}</span>${escapeHtml(item.ctx_state?.pending_client_order_id ?? "—")}</div>
          </div>
          <div class="subtle" style="margin-top:10px">${escapeHtml(item.reason || item.error || item.strategy_ref || "No runtime errors recorded.")}</div>
        </article>
      `).join("");
    }

    function renderLiveStrategyCard(profile, item) {
      const width = 760;
      const height = 280;
      const padX = 20;
      const padY = 18;
      const plannedBudget = deploymentDraft.profileBudgets?.[profile.name] ?? profile.suggested_budget ?? profile.current_budget ?? 0;
      const activity = profile.activity || [t("live.noActivity")];
      if (!item) {
        return `
          <article class="chart-card placeholder">
            <div class="service-head">
              <div class="title-row">
                <strong>${escapeHtml(profile.name)}</strong>
                <span class="${pillClass("STOPPED", false)}">${escapeHtml(profile.status || "STOPPED")}</span>
                <span class="pill">${escapeHtml(profile.symbol || "—")}</span>
                <span class="pill">${escapeHtml(profile.interval || "—")}</span>
              </div>
              <div class="subtle">${escapeHtml(profile.strategy_label || "strategy")}</div>
            </div>
            <div class="chart-wrap">${blankChartSvg(width, height, padX, padY)}</div>
            <div class="chart-meta">
              <div><span>${escapeHtml(t("live.planBudget"))}</span>${escapeHtml(formatMoney(plannedBudget))}</div>
              <div><span>${escapeHtml(t("live.historicalWeight"))}</span>${escapeHtml(formatPercent(profile.suggested_weight_pct))}</div>
              <div><span>${escapeHtml(t("live.state"))}</span>${escapeHtml(profile.status || "PLANNED")}</div>
              <div><span>${escapeHtml(t("live.mode"))}</span>${escapeHtml(profile.submission_mode || "PLANNED")}</div>
              <div><span>${escapeHtml(t("live.position"))}</span>0</div>
              <div><span>${escapeHtml(t("live.pendingOrder"))}</span>—</div>
              <div><span>${escapeHtml(t("live.lastPrice"))}</span>—</div>
              <div><span>${escapeHtml(t("live.lastCloseTime"))}</span>—</div>
            </div>
            <div class="activity-log">
              <h4>${escapeHtml(t("live.activity"))}</h4>
              <ul class="activity-list">${activity.map((row) => `<li>${escapeHtml(row)}</li>`).join("")}</ul>
            </div>
          </article>
        `;
      }

      const minValue = Number(item.chart_min);
      const maxValue = Number(item.chart_max);
      const closePoints = polylinePoints(item.series?.close || [], minValue, maxValue, width, height, padX, padY);
      const fastPoints = polylinePoints(item.series?.fast_ema || [], minValue, maxValue, width, height, padX, padY);
      const slowPoints = polylinePoints(item.series?.slow_ema || [], minValue, maxValue, width, height, padX, padY);
      const topY = yGuide(maxValue, minValue, maxValue, height, padY);
      const midY = yGuide(Number(item.chart_mid), minValue, maxValue, height, padY);
      const bottomY = yGuide(minValue, minValue, maxValue, height, padY);
      const signalLabel = item.signal ? item.signal.replaceAll("_", " ") : (item.trend || "neutral");
      return `
        <article class="chart-card">
          <div class="service-head">
            <div class="title-row">
              <strong>${escapeHtml(item.service_name)}</strong>
              <span class="${pillClass(item.status, item.healthy)}">${escapeHtml(item.status)}</span>
              <span class="pill">${escapeHtml(item.symbol || "—")}</span>
              <span class="pill">${escapeHtml(item.interval || "—")}</span>
            </div>
            <div class="subtle">${escapeHtml(item.strategy_label)}</div>
          </div>
          <div class="chart-wrap">
            <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(item.service_name)} chart">
              <line x1="${padX}" y1="${topY}" x2="${width - padX}" y2="${topY}" stroke="rgba(23,22,26,0.08)" stroke-width="1" />
              <line x1="${padX}" y1="${midY}" x2="${width - padX}" y2="${midY}" stroke="rgba(23,22,26,0.10)" stroke-width="1" stroke-dasharray="4 6" />
              <line x1="${padX}" y1="${bottomY}" x2="${width - padX}" y2="${bottomY}" stroke="rgba(23,22,26,0.08)" stroke-width="1" />
              ${slowPoints ? `<polyline fill="none" stroke="#d39b2a" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" points="${slowPoints}" />` : ""}
              ${fastPoints ? `<polyline fill="none" stroke="#187d78" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" points="${fastPoints}" />` : ""}
              <polyline fill="none" stroke="#17161a" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" points="${closePoints}" />
            </svg>
          </div>
          <div class="chart-legend">
            <span class="legend-key" style="color:#17161a"><span class="legend-line"></span>Close ${escapeHtml(formatNumber(item.last_close))}</span>
            ${item.fast_period ? `<span class="legend-key" style="color:#187d78"><span class="legend-line"></span>EMA ${escapeHtml(item.fast_period)} ${escapeHtml(formatNumber(lastValue(item.series?.fast_ema)))}</span>` : ""}
            ${item.slow_period ? `<span class="legend-key" style="color:#d39b2a"><span class="legend-line"></span>EMA ${escapeHtml(item.slow_period)} ${escapeHtml(formatNumber(lastValue(item.series?.slow_ema)))}</span>` : ""}
          </div>
          <div class="chart-meta">
            <div><span>${escapeHtml(t("live.planBudget"))}</span>${escapeHtml(formatMoney(plannedBudget))}</div>
            <div><span>${escapeHtml(t("live.historicalWeight"))}</span>${escapeHtml(formatPercent(profile.suggested_weight_pct))}</div>
            <div><span>${escapeHtml(t("live.state"))}</span>${escapeHtml(signalLabel)}</div>
            <div><span>${escapeHtml(t("live.mode"))}</span>${escapeHtml(item.submission_mode || "—")}</div>
            <div><span>${escapeHtml(t("live.position"))}</span>${escapeHtml(item.position_qty || "0")}</div>
            <div><span>${escapeHtml(t("live.pendingOrder"))}</span>${escapeHtml(item.pending_client_order_id || "—")}</div>
            <div><span>${escapeHtml(t("live.lastCloseTime"))}</span>${escapeHtml(formatTime(item.last_close_time))}</div>
            <div><span>${escapeHtml(t("live.chartRangeHigh"))}</span>${escapeHtml(formatNumber(item.chart_max))}</div>
            <div><span>${escapeHtml(t("live.chartRangeMid"))}</span>${escapeHtml(formatNumber(item.chart_mid))}</div>
            <div><span>${escapeHtml(t("live.chartRangeLow"))}</span>${escapeHtml(formatNumber(item.chart_min))}</div>
          </div>
          <div class="activity-log">
            <h4>${escapeHtml(t("live.activity"))}</h4>
            <ul class="activity-list">${activity.map((row) => `<li>${escapeHtml(row)}</li>`).join("")}</ul>
          </div>
        </article>
      `;
    }

    function markerColor(kind) {
      return kind === "buy" ? "#13795b" : "#b43a3a";
    }

    function markerGlyph(kind) {
      return kind === "buy" ? "B" : "S";
    }

    function markerPosition(marker, times, minValue, maxValue, width, height, padX, padY) {
      if (!Array.isArray(times) || times.length < 2) return null;
      const timeIndex = times.findIndex((value) => Number(value) >= Number(marker.time));
      const index = timeIndex >= 0 ? timeIndex : (times.length - 1);
      const x = padX + (((width - (padX * 2)) * index) / Math.max(times.length - 1, 1));
      const span = maxValue - minValue || 1;
      const y = padY + ((maxValue - Number(marker.price)) / span) * (height - (padY * 2));
      return { x, y };
    }

    function renderResearchCandidateChartPanel(item, mode) {
      const width = 720;
      const height = 220;
      const miniHeight = 110;
      const padX = 20;
      const padY = 18;
      const series = item.chart?.[mode] || {};
      const priceSeries = series.price?.close || [];
      const priceTimes = series.price?.times || [];
      const minValue = Number(series.price?.min);
      const maxValue = Number(series.price?.max);
      const pricePoints = polylinePoints(priceSeries, minValue, maxValue, width, height, padX, padY);
      const equityValues = series.equity?.values || [];
      const equityMin = Number(series.equity?.min);
      const equityMax = Number(series.equity?.max);
      const equityPoints = polylinePoints(equityValues, equityMin, equityMax, width, miniHeight, padX, 14);
      const markers = series.markers || [];
      const markerSvg = markers.map((marker) => {
        const pos = markerPosition(marker, priceTimes, minValue, maxValue, width, height, padX, padY);
        if (!pos) return "";
        const color = markerColor(marker.kind);
        return `
          <g>
            <circle cx="${pos.x.toFixed(2)}" cy="${pos.y.toFixed(2)}" r="8" fill="${color}" stroke="white" stroke-width="2" />
            <text x="${pos.x.toFixed(2)}" y="${(pos.y + 3.8).toFixed(2)}" text-anchor="middle" font-size="9" font-weight="700" fill="white">${markerGlyph(marker.kind)}</text>
          </g>
        `;
      }).join("");
      const markerLegend = markers.slice(-4).map((marker) => `
        <span class="marker-label"><span class="marker-dot" style="background:${markerColor(marker.kind)}"></span>${escapeHtml(marker.label)} @ ${escapeHtml(formatNumber(marker.price))}</span>
      `).join("");
      return `
        <div class="subtle" style="margin-top:6px">${escapeHtml(mode === "recent" ? (item.ops_summary.window_scope || "") : (item.ops_summary.full_scope || ""))}</div>
        <div class="candidate-chart-wrap">
          ${priceSeries.length >= 2 ? `
            <svg class="candidate-chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(item.title)} price chart">
              <polyline fill="none" stroke="#17161a" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" points="${pricePoints}" />
              ${markerSvg}
            </svg>
          ` : `<div class="empty" style="padding:16px">No price history available.</div>`}
        </div>
        <div class="candidate-legend">
          <span class="legend-key" style="color:#17161a"><span class="legend-line"></span>Close ${escapeHtml(formatNumber(lastValue(priceSeries)))}</span>
          ${markerLegend}
        </div>
        <div class="candidate-chart-wrap" style="margin-top:10px">
          ${equityValues.length >= 2 ? `
            <svg class="candidate-chart-svg" viewBox="0 0 ${width} ${miniHeight}" role="img" aria-label="${escapeHtml(item.title)} equity curve">
              <polyline fill="none" stroke="#187d78" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" points="${equityPoints}" />
            </svg>
          ` : `<div class="empty" style="padding:16px">No equity curve available.</div>`}
        </div>
      `;
    }

    function renderResearchCandidateCard(item, tagLabel, tagTone, headerNote = "") {
      const chartTabs = `
        <div class="title-row" style="margin-top:10px;margin-bottom:4px">
          <span class="pill active" data-chart-mode="recent">Recent Window</span>
          <span class="pill" data-chart-mode="full">Full Sample</span>
        </div>
      `;
      return `
        <article class="candidate-card">
          <div class="service-head">
            <div class="title-row">
              <strong>${escapeHtml(item.title)}</strong>
              <span class="pill ${escapeHtml(tagTone)}">${escapeHtml(tagLabel)}</span>
              <span class="pill">${escapeHtml(item.category)}</span>
            </div>
            <div class="subtle">score ${escapeHtml(formatNumber(item.score))}</div>
          </div>
          <div class="subtle">key ${escapeHtml(item.name)}</div>
          ${headerNote ? `<div class="subtle">${escapeHtml(headerNote)}</div>` : ""}
          <div class="subtle">${escapeHtml(item.description || "")}</div>
          ${chartTabs}
          <div class="candidate-chart-panel" data-chart-panel="recent">${renderResearchCandidateChartPanel(item, "recent")}</div>
          <div class="candidate-chart-panel" data-chart-panel="full" style="display:none">${renderResearchCandidateChartPanel(item, "full")}</div>
          <div class="candidate-metrics" style="margin-top:12px">
            <div class="mini-metric"><span>Window Return</span><strong>${escapeHtml(formatPercent(item.metrics.window_return_pct))}</strong></div>
            <div class="mini-metric"><span>Full Return</span><strong>${escapeHtml(formatPercent(item.metrics.total_return_pct))}</strong></div>
            <div class="mini-metric"><span>Full Drawdown</span><strong>${escapeHtml(formatPercent(item.metrics.max_drawdown_pct))}</strong></div>
            <div class="mini-metric"><span>Profit Factor</span><strong>${escapeHtml(formatNumber(item.metrics.profit_factor))}</strong></div>
            <div class="mini-metric"><span>Sharpe</span><strong>${escapeHtml(formatNumber(item.metrics.sharpe))}</strong></div>
            <div class="mini-metric"><span>Win Rate</span><strong>${escapeHtml(formatPercent(item.metrics.win_rate_pct))}</strong></div>
            <div class="mini-metric"><span>Trades</span><strong>${escapeHtml(formatNumber(item.metrics.trade_count))}</strong></div>
          </div>
          <div class="candidate-notes">
            <div>${escapeHtml(item.ops_summary.activity)}</div>
            <div>${escapeHtml(item.ops_summary.metric_scope || "")}</div>
            <div>${escapeHtml(item.ops_summary.cost_pressure)}</div>
            ${(item.ops_summary.recent_actions || []).length ? `<ul class="research-list">${item.ops_summary.recent_actions.map((row) => `<li>${escapeHtml(row)}</li>`).join("")}</ul>` : `<div class="subtle">No completed recent trades in this sample.</div>`}
          </div>
        </article>
      `;
    }

    function renderPortfolioSimulation(portfolio) {
      const curve = portfolio?.equity_curve || [];
      if (!curve.length) {
        return `<div class="subtle">No funded portfolio simulation yet. Either switch to manual weights or find a scan with positive-score candidates.</div>`;
      }
      const width = 900;
      const height = 190;
      const values = curve.map((item) => Number(item.equity));
      const minValue = Math.min(...values);
      const maxValue = Math.max(...values);
      const points = polylinePoints(values, minValue, maxValue, width, height, 20, 18);
      const metrics = portfolio.metrics || {};
      return `
        <div class="candidate-chart-wrap" style="margin-top:14px">
          <svg class="candidate-chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="portfolio simulation">
            <polyline fill="none" stroke="#187d78" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round" points="${points}" />
          </svg>
        </div>
        <div class="candidate-metrics" style="margin-top:12px">
          <div class="mini-metric"><span>Portfolio Equity</span><strong>${escapeHtml(formatMoney(metrics.final_equity))}</strong></div>
          <div class="mini-metric"><span>Portfolio Return</span><strong>${escapeHtml(formatPercent(metrics.total_return_pct))}</strong></div>
          <div class="mini-metric"><span>Portfolio Drawdown</span><strong>${escapeHtml(formatPercent(metrics.max_drawdown_pct))}</strong></div>
          <div class="mini-metric"><span>Funded Strategies</span><strong>${escapeHtml(formatNumber(metrics.funded_strategy_count))}</strong></div>
        </div>
      `;
    }

    function renderResearch(research) {
      const statusNode = document.getElementById("researchStatus");
      const summaryNode = document.getElementById("researchSummaryPanel");
      const allocationsNode = document.getElementById("researchAllocations");
      const secondaryNode = document.getElementById("researchSecondary");
      const latest = research?.latest;
      const catalog = research?.catalog || {};

      if (!latest) {
        statusNode.textContent = "No strategy scan yet. Use the research form to benchmark the current universe.";
        const categories = (catalog.category_rows || []).map((item) => `
          <div class="mini-metric">
            <span>${escapeHtml(item.category.replaceAll("_", " "))}</span>
            <strong>${escapeHtml(item.count)} strategies</strong>
          </div>
        `).join("");
        const presets = (catalog.preset_rows || []).map((item) => `
          <div class="candidate-card">
            <div class="service-head">
              <div class="title-row">
                <strong>${escapeHtml(item.title)}</strong>
                <span class="pill">${escapeHtml(item.market)}</span>
              </div>
            </div>
            <div class="subtle">${escapeHtml(item.description)}</div>
          </div>
        `).join("");
        summaryNode.innerHTML = `
          <h3>Research Catalog</h3>
          <div class="subtle" style="margin-bottom:12px">Built-ins: ${escapeHtml(catalog.builtin_strategy_count || 0)} · Presets for this environment: ${escapeHtml(catalog.preset_count || 0)}</div>
          <div class="catalog-grid">${categories || `<div class="empty">No strategy categories found.</div>`}</div>
        `;
        allocationsNode.innerHTML = presets || `<div class="empty">No presets available for this environment.</div>`;
        secondaryNode.innerHTML = "";
        return;
      }

      const summary = latest.summary || {};
      statusNode.textContent = `Last scan ${summary.generated_at || latest.generated_at || "—"} · ${summary.market || "—"} · ${summary.symbol || "—"} · ${summary.interval || "—"}`;
      summaryNode.innerHTML = `
        <h3>${escapeHtml(summary.headline || "Strategy scan complete")}</h3>
        <div class="subtle" style="margin-bottom:12px">${escapeHtml(summary.allocation_model || "")}</div>
        <div class="subtle" style="margin-bottom:12px">${escapeHtml(summary.interval_explanation || "")}</div>
        <div class="research-summary-grid">
          <div class="mini-metric"><span>Budget</span><strong>${escapeHtml(formatMoney(summary.capital))}</strong></div>
          <div class="mini-metric"><span>Deployable</span><strong>${escapeHtml(formatMoney(summary.deployable_budget))}</strong></div>
          <div class="mini-metric"><span>Reserve</span><strong>${escapeHtml(formatMoney(summary.reserve_budget))}</strong></div>
          <div class="mini-metric"><span>Top Candidate</span><strong>${escapeHtml(summary.top_strategy_name || "—")}</strong></div>
          <div class="mini-metric"><span>Top Return</span><strong>${escapeHtml(formatPercent(summary.top_strategy_return_pct))}</strong></div>
          <div class="mini-metric"><span>Top Drawdown</span><strong>${escapeHtml(formatPercent(summary.top_strategy_drawdown_pct))}</strong></div>
        </div>
        <div class="ops-row" style="margin-top:14px">
          <button type="button" onclick="generateResearchStack()">${escapeHtml(t("research.generateStack"))}</button>
        </div>
        ${latest.generated_stack ? `
          <div class="subtle" style="margin-top:10px">
            ${escapeHtml(t("research.generatedStack"))}: ${escapeHtml(latest.generated_stack.name || "—")} ·
            ${escapeHtml(formatNumber(latest.generated_stack.profile_count || 0))} profiles
          </div>
        ` : ""}
        ${renderPortfolioSimulation(latest.portfolio)}
        <ul class="research-list" style="margin-top:12px">${(summary.notes || []).map((note) => `<li>${escapeHtml(note)}</li>`).join("")}</ul>
      `;

      allocationsNode.innerHTML = (latest.allocations || []).map((item) =>
        renderResearchCandidateCard(
          item,
          `alloc ${formatPercent(item.allocation_pct)}`,
          "ok",
          `${formatMoney(item.budget_amount)} allocated · ${formatPercent(item.allocation_pct)} of deployable budget`
        )
      ).join("") || `<div class="empty">No positive-score candidates were found under the current assumptions.</div>`;

      const watchCards = (latest.watchlist || []).map((item) => `
        ${renderResearchCandidateCard(item, "watch", "warn")}
      `).join("");
      const avoidCards = (latest.avoid || []).map((item) => `
        ${renderResearchCandidateCard(item, "avoid", "err")}
      `).join("");
      const candidateCards = (latest.candidates || []).map((item) => renderResearchCandidateCard(item, "candidate", "")).join("");
      secondaryNode.innerHTML = `
        ${candidateCards ? `<div class="research-block" style="grid-column:1/-1"><h3>Full Strategy Explorer</h3><div class="subtle" style="margin-bottom:12px">Every scanned strategy now has its own price chart, buy/sell markers, equity curve, and metric pack.</div></div>` : ""}
        ${candidateCards || `${watchCards}${avoidCards}`}
      `;
      installResearchCardInteractions();
    }

    function renderPortfolio(portfolio) {
      const node = document.getElementById("portfolioPanel");
      const time = document.getElementById("portfolioTime");
      if (!portfolio.enabled && portfolio.enabled !== undefined) {
        time.textContent = "Portfolio polling disabled";
        node.innerHTML = `<div class="empty">Portfolio polling is disabled for this dashboard.</div>`;
        return;
      }
      if (!portfolio.ok) {
        time.textContent = portfolio.fetched_at ? `Last attempt ${portfolio.fetched_at}` : "Unavailable";
        node.innerHTML = `<div class="empty">${escapeHtml(portfolio.error || "Portfolio snapshot unavailable.")}</div>`;
        return;
      }
      time.textContent = `Fetched ${portfolio.fetched_at}`;
      const wallets = portfolio.wallets || [];
      const maxBalance = wallets.reduce((best, row) => Math.max(best, Number(row.balance || 0)), 0);
      const bars = wallets.map((row) => {
        const width = maxBalance > 0 ? (Number(row.balance || 0) / maxBalance) * 100 : 0;
        return `
          <div>
            <div class="wallet-bar-label">
              <strong>${escapeHtml(row.wallet_name)}</strong>
              <span>${escapeHtml(row.balance)} USDT</span>
            </div>
            <div class="track"><div class="fill" style="width:${width.toFixed(2)}%"></div></div>
          </div>
        `;
      }).join("");
      const spotRows = (portfolio.spot_balances || []).map((row) => `
        <div class="wallet-row">
          <div class="wallet-head">
            <strong>${escapeHtml(row.asset)}</strong>
            <span>${escapeHtml(row.total)}</span>
          </div>
          <div class="subtle">free ${escapeHtml(row.free)} / locked ${escapeHtml(row.locked)}</div>
        </div>
      `).join("");
      const earnRows = (portfolio.earn_positions || []).map((row) => `
        <div class="runtime-file">
          <div class="wallet-head">
            <strong>${escapeHtml(row.asset)}</strong>
            <span>${escapeHtml(row.total_amount)}</span>
          </div>
          <div class="subtle">APR ${escapeHtml(row.apr || "—")} · ${escapeHtml(row.product_id || "—")}</div>
        </div>
      `).join("");
      node.innerHTML = `
        <div class="wallet-bars">${bars || `<div class="empty">No positive wallet balances found.</div>`}</div>
        <div class="split" style="margin-top:16px">
          <div>
            <h2 style="font-size:15px;margin:0 0 10px">Spot Balances</h2>
            <div class="list">${spotRows || `<div class="empty">No positive spot balances.</div>`}</div>
          </div>
          <div>
            <h2 style="font-size:15px;margin:0 0 10px">Simple Earn</h2>
            <div class="list">${earnRows || `<div class="empty">No positive Simple Earn positions.</div>`}</div>
          </div>
        </div>
      `;
    }

    function renderOrders(orders) {
      const node = document.getElementById("ordersPanel");
      if (!orders.length) {
        node.innerHTML = `<div class="empty">No orders recorded yet.</div>`;
        return;
      }
      node.innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Side</th>
              <th>Status</th>
              <th>Mode</th>
              <th>Qty</th>
              <th>Updated</th>
              <th>Client Order Id</th>
            </tr>
          </thead>
          <tbody>
            ${orders.map((item) => `
              <tr>
                <td>${escapeHtml(item.symbol)}</td>
                <td>${escapeHtml(item.side)} ${escapeHtml(item.order_type)}</td>
                <td><span class="${pillClass(item.status, item.is_open ? null : item.status === "FILLED")}">${escapeHtml(item.status)}</span></td>
                <td>${escapeHtml(item.submission_mode)}</td>
                <td>${escapeHtml(item.quote_order_qty || item.quantity || "—")}</td>
                <td>${escapeHtml(item.updated_at)}</td>
                <td><code>${escapeHtml(item.client_order_id)}</code></td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
    }

    function renderEvents(events) {
      const node = document.getElementById("eventsPanel");
      if (!events.length) {
        node.innerHTML = `<div class="empty">No events recorded yet.</div>`;
        return;
      }
      node.innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Channel</th>
              <th>Type</th>
              <th>Symbol</th>
              <th>Payload</th>
            </tr>
          </thead>
          <tbody>
            ${events.map((item) => `
              <tr>
                <td>${escapeHtml(item.created_at)}</td>
                <td>${escapeHtml(item.channel)}</td>
                <td>${escapeHtml(item.event_type || "—")}</td>
                <td>${escapeHtml(item.symbol || "—")}</td>
                <td><code>${escapeHtml(item.payload_preview)}</code></td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
    }

    function renderRuntimeFiles(files) {
      const node = document.getElementById("runtimeFiles");
      if (!files.length) {
        node.innerHTML = `<div class="empty">No runtime heartbeat files found.</div>`;
        return;
      }
      node.innerHTML = files.map((item) => `
        <article class="runtime-file">
          <div class="service-head">
            <div class="title-row">
              <strong>${escapeHtml(item.name)}</strong>
              ${item.status ? `<span class="pill">${escapeHtml(item.status)}</span>` : ""}
            </div>
            <div class="subtle">${escapeHtml(item.size_bytes)} bytes</div>
          </div>
          <div class="subtle">${escapeHtml(item.updated_at || item.error || item.kind || "—")}</div>
        </article>
      `).join("");
    }

    function renderStatusLog(rows) {
      const node = document.getElementById("statusLogPanel");
      if (!rows?.length) {
        node.innerHTML = `<div class="empty">No runtime activity recorded yet.</div>`;
        return;
      }
      node.innerHTML = rows.map((item) => `
        <div class="log-row">
          <span class="pill ${escapeHtml(item.severity || "info")}">${escapeHtml(item.severity || "info")}</span>
          <div class="log-source" title="${escapeHtml(item.source || "runtime")}">${escapeHtml(item.source || "runtime")}</div>
          <div class="log-message">
            <strong>${escapeHtml(item.title || "event")}</strong>
            <span title="${escapeHtml(item.detail || "")}">${escapeHtml(item.detail || "—")}</span>
            <div class="subtle">${escapeHtml(formatTime(item.time) || item.time || "—")}</div>
          </div>
        </div>
      `).join("");
    }

    function renderActionFeedback(payload, response) {
      const node = document.getElementById("actionResponse");
      if (!response?.ok) {
        node.innerHTML = `
          <h3>${escapeHtml(t("advanced.actionCenter"))}</h3>
          <div class="subtle">${escapeHtml(currentLocale === "zh" ? "最近一次操作失败。" : "The last action failed.")}</div>
          <ul class="research-list" style="margin-top:12px">
            <li>${escapeHtml(response?.error || "Unknown error")}</li>
          </ul>
        `;
        return;
      }
      const result = response.result || {};
      const action = payload.action;
      let title = currentLocale === "zh" ? "操作完成" : "Action Complete";
      let points = [];
      if (action === "start_stack" || action === "start_profile") {
        title = result.status === "ALREADY_RUNNING" ? "Already Running" : "Daemon Started";
        points = [
          `${result.name || "runtime"} on PID ${result.pid || "—"}`,
          `log file: ${result.log_path || "—"}`,
          `status: ${result.status || "OK"}`,
        ];
      } else if (action === "stop_stack" || action === "stop_profile") {
        title = "Stop Request Processed";
        points = [
          `${result.name || "runtime"} -> ${result.status || "OK"}`,
          result.stopped_at ? `stopped at ${result.stopped_at}` : `pid ${result.pid || "—"}`,
        ];
      } else if (action === "doctor_stack") {
        title = "Stack Doctor Complete";
        points = [
          `${(result.profiles || []).length} profiles checked`,
          ...(result.profiles || []).map((item) => `${item.profile_name}: ${item.doctor?.environment || "—"} / ${item.doctor?.symbol_rules?.symbol || "—"}`),
        ];
      } else if (action === "doctor_profile") {
        title = "Profile Doctor Complete";
        points = [
          `${result.doctor?.runtime_profile?.name || result.runtime_profile?.name || "profile"} checked`,
          `${result.doctor?.environment || "—"} / ${result.doctor?.symbol_rules?.symbol || "—"}`,
          `clock skew ${result.doctor?.clock_skew_ms ?? "—"} ms`,
        ];
      } else if (action === "reconcile_spot") {
        title = "Spot Reconcile Complete";
        points = [
          `${result.result?.symbol || payload.symbol || "symbol"} synced`,
          `${(result.result?.open_orders || []).length} open orders mirrored into local state`,
        ];
      } else if (action === "buy_market_spot" || action === "sell_market_spot") {
        title = "Spot Order Submitted";
        points = [
          `${action === "buy_market_spot" ? "BUY" : "SELL"} ${result.symbol || payload.symbol || "—"}`,
          `mode ${result.submission_mode || payload.submission_mode || "DRY_RUN"}`,
          `exchange status ${result.result?.status || "—"}`,
        ];
      } else if (action === "redeem_earn_flexible") {
        title = "Earn Redemption Submitted";
        points = [
          `${result.result?.asset || result.asset || payload.asset || "asset"} -> ${result.result?.dest_account || result.dest_account || "SPOT"}`,
          `product ${result.result?.product_id || result.product_id || "auto"}`,
          `amount ${result.result?.amount || "ALL"}`,
        ];
      } else if (action === "refresh_portfolio") {
        title = "Portfolio Refresh Requested";
        points = [`Portfolio cache invalidated. The next snapshot will pull fresh balances.`];
      } else if (action === "research_scan") {
        title = "Strategy Scan Complete";
        points = [
          result.summary?.headline || "Research snapshot updated.",
          `${result.candidate_count || 0} candidates analyzed`,
          `top allocation count ${(result.top_allocations || []).length}`,
        ];
      } else if (action === "generate_research_stack") {
        title = "Deployable Stack Generated";
        points = [
          `${result.stack?.name || "stack"} ready for deployment`,
          `${result.stack?.profile_count || 0} profiles written`,
          `path ${result.stack?.path || "—"}`,
        ];
      } else if (action === "update_profile_params") {
        title = "Strategy Params Saved";
        points = [
          `${result.profile_name || "profile"} updated`,
          `${(result.updates || []).length} params written`,
          `path ${result.path || payload.profile_path || "—"}`,
        ];
      } else {
        points = [result.status || "OK"];
      }
      node.innerHTML = `
        <h3>${escapeHtml(title)}</h3>
        <ul class="research-list">
          ${points.map((point) => `<li>${escapeHtml(point)}</li>`).join("")}
        </ul>
      `;
    }

    async function postAction(payload) {
      const responseNode = document.getElementById("actionResponse");
      responseNode.innerHTML = `<h3>${escapeHtml(t("advanced.actionCenter"))}</h3><div class="subtle">${escapeHtml(currentLocale === "zh" ? "执行中…" : "Working…")}</div>`;
      try {
        const response = await fetch("/api/actions", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await response.json();
      if (data?.ok && payload.action === "generate_research_stack" && data.result?.stack?.path) {
        deploymentDraft.stackPath = data.result.stack.path;
        deploymentDraft.profileBudgets = {};
        currentPanel = "automation";
        window.localStorage.setItem("dashboardPanel", currentPanel);
      }
        renderActionFeedback(payload, data);
        await refresh();
      } catch (error) {
        renderActionFeedback(payload, { ok: false, error: String(error) });
      }
    }

    function liveModeConfirmed(mode, message) {
      if (mode !== "LIVE") return true;
      return window.confirm(message);
    }

    function runStackAction(action) {
      const path = document.getElementById("stackPath").value;
      const payload = { action, path };
      if (action === "stop_stack" && currentControls) {
        const selected = (currentControls.available_stacks || []).find((item) => item.path === path);
        if (selected) payload.stack_name = selected.name;
      }
      postAction(payload);
    }

    function runProfileAction(action) {
      const path = document.getElementById("profilePath").value;
      const payload = { action, path };
      if (action === "stop_profile" && currentControls) {
        const selected = (currentControls.available_profiles || []).find((item) => item.path === path);
        if (selected) payload.profile_name = selected.name;
      }
      postAction(payload);
    }

    function reconcileSpot() {
      const symbol = document.getElementById("reconcileSymbol").value.trim().toUpperCase();
      postAction({ action: "reconcile_spot", symbol });
    }

    function refreshPortfolio() {
      postAction({ action: "refresh_portfolio" });
    }

    function startLiveDeployment() {
      const path = document.getElementById("liveStackPath").value;
      const budget_overrides = {};
      Object.entries(deploymentDraft.profileBudgets || {}).forEach(([name, value]) => {
        const numeric = Number(value);
        if (Number.isFinite(numeric) && numeric > 0) {
          budget_overrides[name] = numeric;
        }
      });
      postAction({ action: "start_stack", path, budget_overrides });
    }

    function stopLiveDeployment() {
      const path = document.getElementById("liveStackPath").value;
      const selected = (currentControls?.available_stacks || []).find((item) => item.path === path);
      postAction({ action: "stop_stack", path, stack_name: selected?.name });
    }

    function redeemEarnFlexible() {
      const asset = document.getElementById("redeemAsset").value.trim().toUpperCase();
      const amount = document.getElementById("redeemAmount").value.trim();
      const product_id = document.getElementById("redeemProductId").value.trim();
      const dest_account = document.getElementById("redeemDestAccount").value.trim().toUpperCase() || "SPOT";
      const confirmation_text = document.getElementById("redeemConfirmText").value.trim();
      if (!window.confirm(`Redeem ${amount || "ALL"} ${asset || "asset"} from Simple Earn flexible to ${dest_account}?`)) return;
      postAction({ action: "redeem_earn_flexible", asset, amount, product_id, dest_account, confirmation_text });
    }

    function runResearchScan() {
      postAction({
        action: "research_scan",
        market: document.getElementById("researchMarket").value,
        category: document.getElementById("researchCategory").value,
        symbol: document.getElementById("researchSymbol").value.trim().toUpperCase(),
        interval: document.getElementById("researchInterval").value.trim(),
        bars: document.getElementById("researchBars").value.trim(),
        capital: document.getElementById("researchCapital").value.trim(),
        allocation_mode: document.getElementById("researchAllocationMode").value,
        manual_weights: document.getElementById("researchManualWeights").value.trim(),
        fee_bps: document.getElementById("researchFeeBps").value.trim(),
        slippage_bps: document.getElementById("researchSlippageBps").value.trim(),
        leverage: document.getElementById("researchLeverage").value.trim(),
        position_fraction: document.getElementById("researchPositionFraction").value.trim(),
      });
    }

    function generateResearchStack() {
      postAction({ action: "generate_research_stack" });
    }

    function updateIntervalHint() {
      const interval = document.getElementById("researchInterval").value.trim().toLowerCase();
      const bars = Number(document.getElementById("researchBars").value.trim());
      const match = interval.match(/^(\\d+)([mhdw])$/);
      const hint = document.getElementById("intervalHint");
      if (!match) {
        hint.textContent = "Examples: 15m = 15 minutes per bar, 1h = 1 hour per bar, 1d = 1 day per bar.";
        return;
      }
      const count = Number(match[1]);
      const unit = match[2];
      const multipliers = { m: 1, h: 60, d: 1440, w: 10080 };
      const minutes = count * multipliers[unit];
      const sampleDays = Number.isFinite(bars) && bars > 0 ? ((minutes * bars) / 1440).toFixed(2) : "—";
      let unitText = `${minutes} minutes`;
      if (minutes >= 60 && minutes < 1440) unitText = `${minutes / 60} hours`;
      if (minutes >= 1440) unitText = `${minutes / 1440} days`;
      hint.textContent = `${interval} means each K-line covers ${unitText}; ${Number.isFinite(bars) ? bars : "?"} bars is about ${sampleDays} days of simulated history.`;
    }

    function installResearchCardInteractions() {
      document.querySelectorAll(".candidate-card").forEach((card) => {
        const tabs = Array.from(card.querySelectorAll("[data-chart-mode]"));
        const panels = Array.from(card.querySelectorAll("[data-chart-panel]"));
        tabs.forEach((tab) => {
          if (tab.dataset.bound === "1") return;
          tab.dataset.bound = "1";
          tab.addEventListener("click", () => {
            const mode = tab.dataset.chartMode;
            tabs.forEach((node) => node.classList.toggle("active", node.dataset.chartMode === mode));
            panels.forEach((panel) => {
              panel.style.display = panel.dataset.chartPanel === mode ? "" : "none";
            });
          });
        });
      });
    }

    function buySpotMarket() {
      const symbol = document.getElementById("orderSymbol").value.trim().toUpperCase();
      const submission_mode = document.getElementById("orderMode").value;
      const quote_order_qty = document.getElementById("buyQuoteQty").value.trim();
      if (!liveModeConfirmed(submission_mode, `Send a LIVE spot market BUY for ${symbol}?`)) return;
      postAction({ action: "buy_market_spot", symbol, submission_mode, quote_order_qty });
    }

    function sellSpotMarket() {
      const symbol = document.getElementById("orderSymbol").value.trim().toUpperCase();
      const submission_mode = document.getElementById("orderMode").value;
      const quantity = document.getElementById("sellQuantity").value.trim();
      if (!liveModeConfirmed(submission_mode, `Send a LIVE spot market SELL for ${symbol}?`)) return;
      postAction({ action: "sell_market_spot", symbol, submission_mode, quantity });
    }

    function renderCurrentSnapshot(data, { force = false } = {}) {
      currentSnapshot = data;
      document.getElementById("generatedAt").textContent = `Refreshed ${data.generated_at} · every ${REFRESH_SECONDS}s`;
      document.getElementById("environmentLabel").textContent = `Environment: ${data.environment}`;
      if (force) {
        Object.keys(sectionHashes).forEach((key) => delete sectionHashes[key]);
      }
      updateSection("readiness", data, renderReadiness);
      updateSection("summary", data.summary, renderSummary);
      updateSection("stacks", data.stacks, renderStacks);
      updateSection("services", data.services, renderServices);
      updateSection("research", data.research || {}, renderResearch);
      updateSection("portfolio", data.portfolio, renderPortfolio);
      updateSection("orders", data.orders, renderOrders);
      updateSection("events", data.events, renderEvents);
      updateSection("status_log", data.status_log || [], renderStatusLog);
      updateSection("runtime_files", data.runtime_files, renderRuntimeFiles);
      updateSection("controls", data.controls, renderControls);
      updateSection(
        "live_workspace",
        {
          deployment: data.deployment || {},
          strategy_charts: data.strategy_charts || [],
          orders: data.orders || [],
          events: data.events || [],
        },
        () => renderLiveWorkspace(data),
      );
      applyStaticTranslations();
      switchPanel(currentPanel);
    }

    async function refresh() {
      try {
        const response = await fetch("/api/snapshot", { cache: "no-store" });
        const data = await response.json();
        renderCurrentSnapshot(data);
      } catch (error) {
        document.getElementById("generatedAt").textContent = `Dashboard fetch failed: ${error}`;
      }
    }

    document.getElementById("liveStackPath").addEventListener("change", (event) => {
      deploymentDraft.stackPath = event.target.value;
      deploymentDraft.profileBudgets = {};
      if (currentSnapshot) renderLiveWorkspace(currentSnapshot);
    });
    document.getElementById("deploymentTotalBudget").addEventListener("change", (event) => {
      deploymentDraft.totalBudget = Number(event.target.value || 0);
      if (currentSnapshot?.deployment?.enabled) {
        applySuggestedBudgets();
      }
    });
    document.getElementById("researchInterval").addEventListener("input", updateIntervalHint);
    document.getElementById("researchBars").addEventListener("input", updateIntervalHint);
    updateIntervalHint();
    applyStaticTranslations();
    switchPanel(currentPanel);
    refresh();
    window.setInterval(refresh, REFRESH_SECONDS * 1000);
  </script>
</body>
</html>"""
