from __future__ import annotations

import asyncio
import json
import logging
import os
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

from .builtin_strategies import ema_series, list_strategies
from .config import Settings
from .daemon import is_runtime_stack_status_healthy, is_runtime_status_healthy
from .exceptions import BinanceTradeError
from .presets import list_presets as list_research_presets
from .research import benchmark_builtin_strategies, fetch_recent_candles, interval_to_minutes
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
            "runtime_files": runtime_files,
            "controls": None if self.control_plane is None else self.control_plane.describe(),
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
            return self._start_stack(self._required_path(payload))
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

    def _discover_stacks(self) -> list[dict[str, Any]]:
        rows = []
        for path in sorted(self.runtime_examples_dir.glob("*.toml")):
            try:
                stack = load_runtime_stack(path)
            except Exception:
                continue
            rows.append({"name": stack.name, "path": str(path.resolve())})
        return rows

    def _discover_profiles(self) -> list[dict[str, Any]]:
        rows = []
        for path in sorted(self.runtime_examples_dir.glob("*.toml")):
            try:
                profile = load_runtime_profile(path)
            except Exception:
                continue
            rows.append({"name": profile.name, "market": profile.market.value, "path": str(path.resolve())})
        return rows

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

    def _start_stack(self, path: str) -> dict[str, Any]:
        resolved = self._resolve_path(path)
        stack = load_runtime_stack(resolved)
        command = [sys.executable, "-m", "binance_trade.cli", "run-daemon-stack", str(resolved)]
        return self._start_process(kind="stack", name=stack.name, path=resolved, command=command)

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
      --bg: #f3efe7;
      --panel: rgba(255,255,255,0.84);
      --panel-strong: rgba(255,255,255,0.96);
      --ink: #17161a;
      --muted: #665f59;
      --line: rgba(23,22,26,0.1);
      --gold: #d39b2a;
      --teal: #187d78;
      --green: #13795b;
      --red: #b43a3a;
      --amber: #9b6a00;
      --shadow: 0 18px 50px rgba(39, 28, 15, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "SF Pro Display", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(211,155,42,0.18), transparent 34%),
        radial-gradient(circle at bottom right, rgba(24,125,120,0.14), transparent 28%),
        linear-gradient(180deg, #f8f4ec 0%, var(--bg) 100%);
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
      border-radius: 24px;
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
      font-family: "Avenir Next Condensed", "Helvetica Neue", sans-serif;
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
      background: rgba(23,22,26,0.06);
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
      border-radius: 20px;
      border: 1px solid var(--line);
    }
    .metric-label {
      font-size: 12px;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 10px;
    }
    .metric-value {
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
      background: rgba(255,255,255,0.74);
      border: 1px solid var(--line);
      border-radius: 16px;
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
      color: #56463a;
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
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.78);
    }
    .ops-card h3 {
      margin: 0 0 10px;
      font-size: 15px;
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
    }
    .ops-form input, .ops-form select, .ops-form button, .ops-row button {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      font: inherit;
      background: rgba(255,255,255,0.96);
      color: var(--ink);
    }
    .ops-form button, .ops-row button {
      cursor: pointer;
      background: linear-gradient(180deg, #fffaf1 0%, #f0e1c3 100%);
      font-weight: 600;
    }
    .ops-row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .response-box {
      margin-top: 10px;
      padding: 12px;
      min-height: 72px;
      border-radius: 14px;
      background: rgba(23,22,26,0.05);
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
    .research-layout {
      display: grid;
      gap: 16px;
      grid-template-columns: 360px 1fr;
    }
    .research-block {
      padding: 16px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.78);
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
      background: rgba(23,22,26,0.05);
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
      background: rgba(255,255,255,0.82);
    }
    .candidate-chart-wrap {
      margin-top: 12px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background:
        linear-gradient(180deg, rgba(211,155,42,0.06), transparent 45%),
        linear-gradient(180deg, rgba(255,255,255,0.98), rgba(255,255,255,0.86));
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
      background: rgba(23,22,26,0.08);
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
      background: rgba(255,255,255,0.82);
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
      border-radius: 18px;
      border: 1px solid var(--line);
      background:
        linear-gradient(180deg, rgba(24,125,120,0.05), transparent 38%),
        linear-gradient(180deg, rgba(255,255,255,0.96), rgba(255,255,255,0.84));
      overflow: hidden;
    }
    .chart-svg {
      width: 100%;
      height: auto;
      display: block;
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
    @media (max-width: 1180px) {
      .hero, .panel-grid { grid-template-columns: 1fr; }
      .wide { grid-column: span 1; }
      .ops-grid { grid-template-columns: 1fr 1fr; }
      .research-layout,
      .chart-grid { grid-template-columns: 1fr; }
      .candidate-grid,
      .catalog-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 760px) {
      .shell { width: min(100vw - 20px, 100%); margin: 12px auto 28px; }
      .summary-grid, .split, .chart-meta, .research-summary-grid, .candidate-metrics { grid-template-columns: 1fr; }
      .ops-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="hero-card">
        <div class="eyebrow">BinanceTrade Runtime</div>
        <h1>Local Ops Dashboard</h1>
        <p>Monitors supervised stacks, service heartbeats, recent orders, event flow, and wallet allocation from the same local workspace state that drives the daemon.</p>
        <div class="status-badge"><span id="generatedAt">Waiting for first snapshot…</span></div>
      </div>
      <div class="summary-grid" id="summaryGrid"></div>
    </section>

    <section class="panel-grid">
      <section class="panel">
        <div class="toolbar">
          <h2>Stacks</h2>
          <div class="subtle" id="environmentLabel"></div>
        </div>
        <div class="list" id="stackList"></div>
      </section>

      <section class="panel">
        <div class="toolbar">
          <h2>Services</h2>
          <div class="subtle">Daemon members and last heartbeat</div>
        </div>
        <div class="list" id="serviceList"></div>
      </section>

      <section class="panel wide">
        <div class="toolbar">
          <h2>Live Strategy Charts</h2>
          <div class="subtle">Current runtime strategy state, latest K-line history, and live indicator overlays</div>
        </div>
        <div class="chart-grid" id="strategyChartList"></div>
      </section>

      <section class="panel wide">
        <div class="toolbar">
          <h2>Strategy Lab</h2>
          <div class="subtle" id="researchStatus">No strategy scan has been run from the dashboard yet.</div>
        </div>
        <div class="research-layout">
          <article class="research-block">
            <h3>Research Scan</h3>
            <div class="ops-form">
              <label>Market
                <select id="researchMarket">
                  <option value="spot">spot</option>
                  <option value="futures">futures</option>
                </select>
              </label>
              <label>Universe
                <select id="researchCategory"></select>
              </label>
              <label>Symbol
                <input id="researchSymbol" value="BTCUSDT" />
              </label>
              <label>Interval
                <input id="researchInterval" value="15m" />
              </label>
              <label>Bars
                <input id="researchBars" value="1500" />
              </label>
              <label>Budget
                <input id="researchCapital" value="1000" />
              </label>
              <label>Allocation Mode
                <select id="researchAllocationMode">
                  <option value="auto">auto from history</option>
                  <option value="manual">manual weights</option>
                </select>
              </label>
              <label>Manual Weights
                <input id="researchManualWeights" value="" placeholder="sma_crossover=40, rsi_regime=35, ichimoku_trend=25" />
              </label>
              <label>Fee (bps)
                <input id="researchFeeBps" value="10" />
              </label>
              <label>Slippage (bps)
                <input id="researchSlippageBps" value="2" />
              </label>
              <label>Leverage
                <input id="researchLeverage" value="1" />
              </label>
              <label>Position Fraction
                <input id="researchPositionFraction" value="1" />
              </label>
              <div class="ops-row">
                <button type="button" onclick="runResearchScan()">Run Strategy Scan</button>
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

      <section class="panel">
        <div class="toolbar">
          <h2>Portfolio</h2>
          <div class="subtle" id="portfolioTime"></div>
        </div>
        <div id="portfolioPanel"></div>
      </section>

      <section class="panel wide">
        <div class="toolbar">
          <h2>Operations</h2>
          <div class="subtle">Local control plane with explicit white-listed actions</div>
        </div>
        <div class="ops-grid">
          <article class="ops-card">
            <h3>Stack Control</h3>
            <div class="ops-form">
              <label>Runtime Stack
                <select id="stackPath"></select>
              </label>
              <div class="ops-row">
                <button type="button" onclick="runStackAction('start_stack')">Start Stack</button>
                <button type="button" onclick="runStackAction('stop_stack')">Stop Stack</button>
                <button type="button" onclick="runStackAction('doctor_stack')">Doctor Stack</button>
              </div>
            </div>
          </article>

          <article class="ops-card">
            <h3>Profile Control</h3>
            <div class="ops-form">
              <label>Runtime Profile
                <select id="profilePath"></select>
              </label>
              <div class="ops-row">
                <button type="button" onclick="runProfileAction('start_profile')">Start Profile</button>
                <button type="button" onclick="runProfileAction('stop_profile')">Stop Profile</button>
                <button type="button" onclick="runProfileAction('doctor_profile')">Doctor Profile</button>
              </div>
            </div>
          </article>

          <article class="ops-card">
            <h3>Spot Runtime Ops</h3>
            <div class="ops-form">
              <label>Symbol
                <input id="reconcileSymbol" value="BTCUSDT" />
              </label>
              <div class="ops-row">
                <button type="button" onclick="reconcileSpot()">Reconcile Spot</button>
                <button type="button" onclick="refreshPortfolio()">Refresh Portfolio Hint</button>
              </div>
            </div>
          </article>

          <article class="ops-card">
            <h3>Manual Spot Order</h3>
            <div class="ops-form">
              <label>Symbol
                <input id="orderSymbol" value="BTCUSDT" />
              </label>
              <label>Submission Mode
                <select id="orderMode">
                  <option value="DRY_RUN">DRY_RUN</option>
                  <option value="TEST">TEST</option>
                  <option value="LIVE">LIVE</option>
                </select>
              </label>
              <label>Buy Quote Qty
                <input id="buyQuoteQty" value="25" />
              </label>
              <label>Sell Quantity
                <input id="sellQuantity" value="0.001" />
              </label>
              <div class="ops-row">
                <button type="button" onclick="buySpotMarket()">Buy Market</button>
                <button type="button" onclick="sellSpotMarket()">Sell Market</button>
              </div>
            </div>
          </article>

          <article class="ops-card">
            <h3>Earn to Spot</h3>
            <div class="ops-form">
              <label>Asset
                <input id="redeemAsset" value="USDT" />
              </label>
              <label>Amount
                <input id="redeemAmount" value="100" />
              </label>
              <label>Product Id (optional)
                <input id="redeemProductId" value="" placeholder="auto-resolve from flexible position" />
              </label>
              <label>Destination
                <input id="redeemDestAccount" value="SPOT" />
              </label>
              <label>Confirm Phrase
                <input id="redeemConfirmText" value="" placeholder='type REDEEM to execute' />
              </label>
              <div class="ops-row">
                <button type="button" onclick="redeemEarnFlexible()">Redeem Flexible</button>
              </div>
              <div class="subtle">This is a real wallet action on mainnet. Enter <code>REDEEM</code> exactly to allow it.</div>
            </div>
          </article>
        </div>
        <div class="response-box" id="actionResponse"><h3>Action Center</h3><div class="subtle">No control action executed yet.</div></div>
      </section>

      <section class="panel wide">
        <div class="toolbar">
          <h2>Recent Orders</h2>
          <div class="subtle">Newest first</div>
        </div>
        <div id="ordersPanel"></div>
      </section>

      <section class="panel wide">
        <div class="split">
          <div>
            <div class="toolbar">
              <h2>Recent Events</h2>
              <div class="subtle">User stream and reconciler activity</div>
            </div>
            <div id="eventsPanel"></div>
          </div>
          <div>
            <div class="toolbar">
              <h2>Runtime Files</h2>
              <div class="subtle">Mirrored JSON heartbeat files</div>
            </div>
            <div class="list" id="runtimeFiles"></div>
          </div>
        </div>
      </section>
    </section>
  </div>

  <script>
    const REFRESH_SECONDS = __REFRESH_SECONDS__;
    let currentControls = null;
    const sectionHashes = {};

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
      if (Math.abs(numeric) >= 1000) return numeric.toLocaleString(undefined, { maximumFractionDigits: 2 });
      if (Math.abs(numeric) >= 1) return numeric.toLocaleString(undefined, { maximumFractionDigits: 4 });
      return numeric.toLocaleString(undefined, { maximumFractionDigits: 8 });
    }

    function formatPercent(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "—";
      return `${numeric.toFixed(2)}%`;
    }

    function formatMoney(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "—";
      return `$${numeric.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
    }

    function formatTime(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric) || numeric <= 0) return "—";
      return new Date(numeric).toLocaleString();
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
        metric("Stacks", `${summary.healthy_stack_count}/${summary.stack_count}`, "Healthy stack supervisors"),
        metric("Services", `${summary.healthy_service_count}/${summary.service_count}`, "Healthy daemon members"),
        metric("Orders", `${summary.open_order_count}/${summary.order_count}`, "Open among most recent tracked orders"),
        metric("Events", `${summary.event_count}`, "Newest event records in local state"),
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
      setOptions("profilePath", controls.available_profiles || []);
      setOptions("researchCategory", controls.research_categories || [], "key", "label");
    }

    function renderStacks(stacks) {
      const node = document.getElementById("stackList");
      if (!stacks.length) {
        node.innerHTML = `<div class="empty">No runtime stacks have written state yet.</div>`;
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
            <div><span>Updated</span>${escapeHtml(item.updated_at)}</div>
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
        node.innerHTML = `<div class="empty">No daemon members have written runtime status yet.</div>`;
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
            <div><span>Market</span>${escapeHtml(item.market_type)}</div>
            <div><span>Symbol</span>${escapeHtml(item.symbol || "—")}</div>
            <div><span>Interval</span>${escapeHtml(item.interval || "—")}</div>
            <div><span>Heartbeat</span>${escapeHtml(item.last_heartbeat_at)}</div>
            <div><span>Updated</span>${escapeHtml(item.updated_at)}</div>
            <div><span>Actions</span>${escapeHtml(item.actions ?? 0)}</div>
            <div><span>Position</span>${escapeHtml(item.ctx_state?.position_qty ?? "—")}</div>
            <div><span>Pending Order</span>${escapeHtml(item.ctx_state?.pending_client_order_id ?? "—")}</div>
          </div>
          <div class="subtle" style="margin-top:10px">${escapeHtml(item.reason || item.error || item.strategy_ref || "No runtime errors recorded.")}</div>
        </article>
      `).join("");
    }

    function renderStrategyCharts(charts) {
      const node = document.getElementById("strategyChartList");
      if (!charts.length) {
        node.innerHTML = `<div class="empty">No chartable strategy runtime state is available yet. Start a daemon profile and wait for it to warm up enough K-lines.</div>`;
        return;
      }
      node.innerHTML = charts.map((item) => {
        const width = 760;
        const height = 280;
        const padX = 20;
        const padY = 18;
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
              <div><span>State</span>${escapeHtml(signalLabel)}</div>
              <div><span>Bars</span>${escapeHtml(item.bar_count)}</div>
              <div><span>Position</span>${escapeHtml(item.position_qty || "0")}</div>
              <div><span>Pending Order</span>${escapeHtml(item.pending_client_order_id || "—")}</div>
              <div><span>Last Close Time</span>${escapeHtml(formatTime(item.last_close_time))}</div>
              <div><span>Chart Max</span>${escapeHtml(formatNumber(item.chart_max))}</div>
              <div><span>Chart Mid</span>${escapeHtml(formatNumber(item.chart_mid))}</div>
              <div><span>Chart Min</span>${escapeHtml(formatNumber(item.chart_min))}</div>
            </div>
          </article>
        `;
      }).join("");
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

    function renderActionFeedback(payload, response) {
      const node = document.getElementById("actionResponse");
      if (!response?.ok) {
        node.innerHTML = `
          <h3>Action Center</h3>
          <div class="subtle">The last action failed.</div>
          <ul class="research-list" style="margin-top:12px">
            <li>${escapeHtml(response?.error || "Unknown error")}</li>
          </ul>
        `;
        return;
      }
      const result = response.result || {};
      const action = payload.action;
      let title = "Action Complete";
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
      responseNode.innerHTML = `<h3>Action Center</h3><div class="subtle">Working…</div>`;
      try {
        const response = await fetch("/api/actions", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await response.json();
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

    async function refresh() {
      try {
        const response = await fetch("/api/snapshot", { cache: "no-store" });
        const data = await response.json();
        document.getElementById("generatedAt").textContent = `Refreshed ${data.generated_at} · every ${REFRESH_SECONDS}s`;
        document.getElementById("environmentLabel").textContent = `Environment: ${data.environment}`;
        updateSection("summary", data.summary, renderSummary);
        updateSection("stacks", data.stacks, renderStacks);
        updateSection("services", data.services, renderServices);
        updateSection("strategy_charts", data.strategy_charts || [], renderStrategyCharts);
        updateSection("research", data.research || {}, renderResearch);
        updateSection("portfolio", data.portfolio, renderPortfolio);
        updateSection("orders", data.orders, renderOrders);
        updateSection("events", data.events, renderEvents);
        updateSection("runtime_files", data.runtime_files, renderRuntimeFiles);
        updateSection("controls", data.controls, renderControls);
      } catch (error) {
        document.getElementById("generatedAt").textContent = `Dashboard fetch failed: ${error}`;
      }
    }

    document.getElementById("researchInterval").addEventListener("input", updateIntervalHint);
    document.getElementById("researchBars").addEventListener("input", updateIntervalHint);
    updateIntervalHint();
    refresh();
    window.setInterval(refresh, REFRESH_SECONDS * 1000);
  </script>
</body>
</html>"""
