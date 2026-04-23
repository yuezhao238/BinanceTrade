from __future__ import annotations

import sqlite3
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
