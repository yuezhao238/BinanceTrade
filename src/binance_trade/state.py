from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any

from .types import MarketType, OrderRequest, SubmissionMode
from .utils import json_dumps, utc_now_iso


class SQLiteStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    client_order_id TEXT PRIMARY KEY,
                    market_type TEXT NOT NULL DEFAULT 'spot',
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    price TEXT,
                    quantity TEXT,
                    quote_order_qty TEXT,
                    status TEXT NOT NULL,
                    exchange_order_id INTEGER,
                    submission_mode TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_type TEXT NOT NULL DEFAULT 'spot',
                    channel TEXT NOT NULL,
                    event_type TEXT,
                    symbol TEXT,
                    client_order_id TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runtime_sessions (
                    run_id TEXT PRIMARY KEY,
                    service_name TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    strategy_ref TEXT NOT NULL,
                    submission_mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    restart_count INTEGER NOT NULL DEFAULT 0,
                    profile_json TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    last_heartbeat_at TEXT,
                    stopped_at TEXT,
                    reason TEXT,
                    error_text TEXT
                );

                CREATE TABLE IF NOT EXISTS runtime_status (
                    service_name TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    strategy_ref TEXT NOT NULL,
                    submission_mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    restart_count INTEGER NOT NULL DEFAULT 0,
                    stale_after_seconds INTEGER NOT NULL DEFAULT 90,
                    summary_json TEXT NOT NULL,
                    ctx_state_json TEXT,
                    strategy_state_json TEXT,
                    started_at TEXT NOT NULL,
                    last_heartbeat_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runtime_stack_sessions (
                    run_id TEXT PRIMARY KEY,
                    stack_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    profile_count INTEGER NOT NULL,
                    config_json TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    last_heartbeat_at TEXT,
                    stopped_at TEXT,
                    reason TEXT,
                    error_text TEXT
                );

                CREATE TABLE IF NOT EXISTS runtime_stack_status (
                    stack_name TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    profile_count INTEGER NOT NULL,
                    healthy_profile_count INTEGER NOT NULL DEFAULT 0,
                    stale_after_seconds INTEGER NOT NULL DEFAULT 90,
                    summary_json TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    last_heartbeat_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_runtime_sessions_service_name ON runtime_sessions(service_name);
                CREATE INDEX IF NOT EXISTS idx_runtime_sessions_started_at ON runtime_sessions(started_at);
                CREATE INDEX IF NOT EXISTS idx_runtime_stack_sessions_stack_name ON runtime_stack_sessions(stack_name);
                """
            )
            self._ensure_column(connection, "orders", "market_type", "TEXT NOT NULL DEFAULT 'spot'")
            self._ensure_column(connection, "events", "market_type", "TEXT NOT NULL DEFAULT 'spot'")

    def _ensure_column(self, connection: sqlite3.Connection, table: str, name: str, ddl: str) -> None:
        columns = {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}
        if name not in columns:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")

    def record_order_request(self, order: OrderRequest, submission_mode: SubmissionMode) -> None:
        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO orders (
                    client_order_id, market_type, symbol, side, order_type, price, quantity,
                    quote_order_qty, status, submission_mode, request_json, response_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(client_order_id) DO UPDATE SET
                    market_type=excluded.market_type,
                    request_json=excluded.request_json,
                    submission_mode=excluded.submission_mode,
                    updated_at=excluded.updated_at
                """,
                (
                    order.new_client_order_id,
                    order.market_type.value,
                    order.symbol,
                    order.side.value,
                    order.order_type.value,
                    None if order.price is None else str(order.price),
                    None if order.quantity is None else str(order.quantity),
                    None if order.quote_order_qty is None else str(order.quote_order_qty),
                    "LOCAL_PENDING",
                    submission_mode.value,
                    json_dumps(order.to_rest_params()),
                    None,
                    now,
                    now,
                ),
            )

    def record_order_result(self, client_order_id: str, result: dict[str, Any], *, fallback_status: str) -> None:
        now = utc_now_iso()
        status = str(result.get("status", fallback_status))
        exchange_order_id = result.get("orderId")
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE orders
                SET status = ?, exchange_order_id = COALESCE(?, exchange_order_id),
                    response_json = ?, updated_at = ?
                WHERE client_order_id = ?
                """,
                (
                    status,
                    exchange_order_id,
                    json_dumps(result),
                    now,
                    client_order_id,
                ),
            )

    def record_event(
        self,
        *,
        market_type: MarketType,
        channel: str,
        payload: dict[str, Any],
        event_type: str | None = None,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO events (market_type, channel, event_type, symbol, client_order_id, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    market_type.value,
                    channel,
                    event_type,
                    symbol,
                    client_order_id,
                    json_dumps(payload),
                    utc_now_iso(),
                ),
            )

    def apply_exchange_order_snapshot(self, snapshot: dict[str, Any], *, market_type: MarketType) -> None:
        client_order_id = snapshot.get("clientOrderId")
        if not client_order_id:
            return
        self.record_order_result(client_order_id, snapshot, fallback_status="UNKNOWN")

    def apply_user_stream_message(self, message: dict[str, Any], *, market_type: MarketType) -> None:
        event = message.get("event", message)
        event_type = event.get("e")
        client_order_id = event.get("c")
        symbol = event.get("s")

        if market_type is MarketType.FUTURES and event_type == "ORDER_TRADE_UPDATE":
            order_event = event.get("o", {})
            client_order_id = order_event.get("c")
            symbol = order_event.get("s")
        elif market_type is MarketType.FUTURES and event_type == "TRADE_LITE":
            client_order_id = event.get("c")
            symbol = event.get("s")

        self.record_event(
            market_type=market_type,
            channel="user_stream",
            payload=message,
            event_type=event_type,
            symbol=symbol,
            client_order_id=client_order_id,
        )
        if market_type is MarketType.SPOT and event_type == "executionReport" and client_order_id:
            self.record_order_result(
                client_order_id,
                {
                    "status": event.get("X", "UNKNOWN"),
                    "orderId": event.get("i"),
                    "clientOrderId": client_order_id,
                    "symbol": symbol,
                    "event": event,
                },
                fallback_status="UNKNOWN",
            )
        elif market_type is MarketType.FUTURES and event_type == "ORDER_TRADE_UPDATE" and client_order_id:
            order_event = event.get("o", {})
            self.record_order_result(
                client_order_id,
                {
                    "status": order_event.get("X", "UNKNOWN"),
                    "orderId": order_event.get("i"),
                    "clientOrderId": client_order_id,
                    "symbol": symbol,
                    "event": order_event,
                },
                fallback_status="UNKNOWN",
            )

    def count_open_orders(self, symbol: str, market_type: MarketType) -> int:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM orders
                WHERE symbol = ?
                  AND market_type = ?
                  AND status IN ('LOCAL_PENDING', 'NEW', 'PARTIALLY_FILLED', 'PENDING_UNKNOWN')
                """,
                (symbol, market_type.value),
            ).fetchone()
        return int(row["count"]) if row else 0

    def last_order_update(self, symbol: str, market_type: MarketType) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT updated_at
                FROM orders
                WHERE symbol = ?
                  AND market_type = ?
                  AND submission_mode = 'LIVE'
                  AND status NOT IN ('LOCAL_REJECTED', 'REJECTED')
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (symbol, market_type.value),
            ).fetchone()
        return None if row is None else str(row["updated_at"])

    def start_runtime_session(
        self,
        *,
        service_name: str,
        market_type: MarketType,
        strategy_ref: str,
        submission_mode: SubmissionMode,
        profile: dict[str, Any],
        restart_count: int,
        stale_after_seconds: int,
    ) -> str:
        run_id = uuid.uuid4().hex
        now = utc_now_iso()
        profile_json = json_dumps(profile)
        empty_summary = json_dumps({"status": "STARTING"})
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO runtime_sessions (
                    run_id, service_name, market_type, strategy_ref, submission_mode, status,
                    restart_count, profile_json, started_at, last_heartbeat_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    service_name,
                    market_type.value,
                    strategy_ref,
                    submission_mode.value,
                    "STARTING",
                    restart_count,
                    profile_json,
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO runtime_status (
                    service_name, run_id, market_type, strategy_ref, submission_mode, status,
                    restart_count, stale_after_seconds, summary_json, ctx_state_json,
                    strategy_state_json, started_at, last_heartbeat_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(service_name) DO UPDATE SET
                    run_id=excluded.run_id,
                    market_type=excluded.market_type,
                    strategy_ref=excluded.strategy_ref,
                    submission_mode=excluded.submission_mode,
                    status=excluded.status,
                    restart_count=excluded.restart_count,
                    stale_after_seconds=excluded.stale_after_seconds,
                    summary_json=excluded.summary_json,
                    ctx_state_json=excluded.ctx_state_json,
                    strategy_state_json=excluded.strategy_state_json,
                    started_at=excluded.started_at,
                    last_heartbeat_at=excluded.last_heartbeat_at,
                    updated_at=excluded.updated_at
                """,
                (
                    service_name,
                    run_id,
                    market_type.value,
                    strategy_ref,
                    submission_mode.value,
                    "STARTING",
                    restart_count,
                    stale_after_seconds,
                    empty_summary,
                    None,
                    None,
                    now,
                    now,
                    now,
                ),
            )
        return run_id

    def update_runtime_status(
        self,
        *,
        service_name: str,
        run_id: str,
        market_type: MarketType,
        strategy_ref: str,
        submission_mode: SubmissionMode,
        status: str,
        restart_count: int,
        stale_after_seconds: int,
        summary: dict[str, Any],
        ctx_state: dict[str, Any] | None = None,
        strategy_state: dict[str, Any] | None = None,
    ) -> None:
        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE runtime_sessions
                SET status = ?, restart_count = ?, last_heartbeat_at = ?
                WHERE run_id = ?
                """,
                (status, restart_count, now, run_id),
            )
            connection.execute(
                """
                INSERT INTO runtime_status (
                    service_name, run_id, market_type, strategy_ref, submission_mode, status,
                    restart_count, stale_after_seconds, summary_json, ctx_state_json,
                    strategy_state_json, started_at, last_heartbeat_at, updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    COALESCE((SELECT started_at FROM runtime_status WHERE service_name = ?), ?),
                    ?, ?
                )
                ON CONFLICT(service_name) DO UPDATE SET
                    run_id=excluded.run_id,
                    market_type=excluded.market_type,
                    strategy_ref=excluded.strategy_ref,
                    submission_mode=excluded.submission_mode,
                    status=excluded.status,
                    restart_count=excluded.restart_count,
                    stale_after_seconds=excluded.stale_after_seconds,
                    summary_json=excluded.summary_json,
                    ctx_state_json=excluded.ctx_state_json,
                    strategy_state_json=excluded.strategy_state_json,
                    last_heartbeat_at=excluded.last_heartbeat_at,
                    updated_at=excluded.updated_at
                """,
                (
                    service_name,
                    run_id,
                    market_type.value,
                    strategy_ref,
                    submission_mode.value,
                    status,
                    restart_count,
                    stale_after_seconds,
                    json_dumps(summary),
                    None if ctx_state is None else json_dumps(ctx_state),
                    None if strategy_state is None else json_dumps(strategy_state),
                    service_name,
                    now,
                    now,
                    now,
                ),
            )

    def stop_runtime_session(
        self,
        *,
        run_id: str,
        service_name: str,
        status: str,
        reason: str | None = None,
        error_text: str | None = None,
        summary: dict[str, Any] | None = None,
    ) -> None:
        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE runtime_sessions
                SET status = ?, stopped_at = ?, reason = ?, error_text = ?, last_heartbeat_at = ?
                WHERE run_id = ?
                """,
                (status, now, reason, error_text, now, run_id),
            )
            if summary is not None:
                connection.execute(
                    """
                    UPDATE runtime_status
                    SET status = ?, summary_json = ?, last_heartbeat_at = ?, updated_at = ?
                    WHERE service_name = ? AND run_id = ?
                    """,
                    (status, json_dumps(summary), now, now, service_name, run_id),
                )

    def get_runtime_status(self, service_name: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM runtime_status
                WHERE service_name = ?
                """,
                (service_name,),
            ).fetchone()
        if row is None:
            return None
        return self._runtime_status_row_to_dict(row)

    def list_runtime_statuses(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM runtime_status
                ORDER BY updated_at DESC, service_name ASC
                """
            ).fetchall()
        return [self._runtime_status_row_to_dict(row) for row in rows]

    def start_runtime_stack_session(
        self,
        *,
        stack_name: str,
        profile_count: int,
        stale_after_seconds: int,
        config: dict[str, Any],
    ) -> str:
        run_id = uuid.uuid4().hex
        now = utc_now_iso()
        config_json = json_dumps(config)
        empty_summary = json_dumps({"status": "STARTING", "profile_count": profile_count, "healthy_profile_count": 0})
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO runtime_stack_sessions (
                    run_id, stack_name, status, profile_count, config_json, started_at, last_heartbeat_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, stack_name, "STARTING", profile_count, config_json, now, now),
            )
            connection.execute(
                """
                INSERT INTO runtime_stack_status (
                    stack_name, run_id, status, profile_count, healthy_profile_count,
                    stale_after_seconds, summary_json, started_at, last_heartbeat_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(stack_name) DO UPDATE SET
                    run_id=excluded.run_id,
                    status=excluded.status,
                    profile_count=excluded.profile_count,
                    healthy_profile_count=excluded.healthy_profile_count,
                    stale_after_seconds=excluded.stale_after_seconds,
                    summary_json=excluded.summary_json,
                    started_at=excluded.started_at,
                    last_heartbeat_at=excluded.last_heartbeat_at,
                    updated_at=excluded.updated_at
                """,
                (stack_name, run_id, "STARTING", profile_count, 0, stale_after_seconds, empty_summary, now, now, now),
            )
        return run_id

    def update_runtime_stack_status(
        self,
        *,
        stack_name: str,
        run_id: str,
        status: str,
        profile_count: int,
        healthy_profile_count: int,
        stale_after_seconds: int,
        summary: dict[str, Any],
    ) -> None:
        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE runtime_stack_sessions
                SET status = ?, last_heartbeat_at = ?
                WHERE run_id = ?
                """,
                (status, now, run_id),
            )
            connection.execute(
                """
                INSERT INTO runtime_stack_status (
                    stack_name, run_id, status, profile_count, healthy_profile_count,
                    stale_after_seconds, summary_json, started_at, last_heartbeat_at, updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?,
                    COALESCE((SELECT started_at FROM runtime_stack_status WHERE stack_name = ?), ?),
                    ?, ?
                )
                ON CONFLICT(stack_name) DO UPDATE SET
                    run_id=excluded.run_id,
                    status=excluded.status,
                    profile_count=excluded.profile_count,
                    healthy_profile_count=excluded.healthy_profile_count,
                    stale_after_seconds=excluded.stale_after_seconds,
                    summary_json=excluded.summary_json,
                    last_heartbeat_at=excluded.last_heartbeat_at,
                    updated_at=excluded.updated_at
                """,
                (
                    stack_name,
                    run_id,
                    status,
                    profile_count,
                    healthy_profile_count,
                    stale_after_seconds,
                    json_dumps(summary),
                    stack_name,
                    now,
                    now,
                    now,
                ),
            )

    def stop_runtime_stack_session(
        self,
        *,
        stack_name: str,
        run_id: str,
        status: str,
        reason: str | None = None,
        error_text: str | None = None,
        summary: dict[str, Any] | None = None,
    ) -> None:
        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE runtime_stack_sessions
                SET status = ?, stopped_at = ?, reason = ?, error_text = ?, last_heartbeat_at = ?
                WHERE run_id = ?
                """,
                (status, now, reason, error_text, now, run_id),
            )
            if summary is not None:
                connection.execute(
                    """
                    UPDATE runtime_stack_status
                    SET status = ?, summary_json = ?, last_heartbeat_at = ?, updated_at = ?
                    WHERE stack_name = ? AND run_id = ?
                    """,
                    (status, json_dumps(summary), now, now, stack_name, run_id),
                )

    def get_runtime_stack_status(self, stack_name: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM runtime_stack_status
                WHERE stack_name = ?
                """,
                (stack_name,),
            ).fetchone()
        if row is None:
            return None
        return self._runtime_stack_status_row_to_dict(row)

    def list_runtime_stack_statuses(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM runtime_stack_status
                ORDER BY updated_at DESC, stack_name ASC
                """
            ).fetchall()
        return [self._runtime_stack_status_row_to_dict(row) for row in rows]

    def _runtime_status_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "service_name": str(row["service_name"]),
            "run_id": str(row["run_id"]),
            "market_type": str(row["market_type"]),
            "strategy_ref": str(row["strategy_ref"]),
            "submission_mode": str(row["submission_mode"]),
            "status": str(row["status"]),
            "restart_count": int(row["restart_count"]),
            "stale_after_seconds": int(row["stale_after_seconds"]),
            "summary": self._loads_json(row["summary_json"]),
            "ctx_state": self._loads_json(row["ctx_state_json"]),
            "strategy_state": self._loads_json(row["strategy_state_json"]),
            "started_at": str(row["started_at"]),
            "last_heartbeat_at": str(row["last_heartbeat_at"]),
            "updated_at": str(row["updated_at"]),
        }

    def _runtime_stack_status_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "stack_name": str(row["stack_name"]),
            "run_id": str(row["run_id"]),
            "status": str(row["status"]),
            "profile_count": int(row["profile_count"]),
            "healthy_profile_count": int(row["healthy_profile_count"]),
            "stale_after_seconds": int(row["stale_after_seconds"]),
            "summary": self._loads_json(row["summary_json"]),
            "started_at": str(row["started_at"]),
            "last_heartbeat_at": str(row["last_heartbeat_at"]),
            "updated_at": str(row["updated_at"]),
        }

    def _loads_json(self, payload: Any) -> Any:
        if payload in (None, ""):
            return None
        if isinstance(payload, (dict, list)):
            return payload
        return json.loads(str(payload))
