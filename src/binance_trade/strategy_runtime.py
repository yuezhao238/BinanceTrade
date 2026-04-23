from __future__ import annotations

import asyncio
import importlib
import importlib.util
import inspect
import json
import logging
import sys
from dataclasses import dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol, Sequence

from .types import MarketType, OrderRequest, OrderSide, OrderType, PositionSide, SubmissionMode, TimeInForce

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class StopStrategy:
    reason: str = "strategy requested stop"


@dataclass(slots=True)
class StrategyEvent:
    channel: str
    payload: dict[str, Any]
    stream: str | None = None


StrategyOutput = OrderRequest | StopStrategy | Sequence[OrderRequest | StopStrategy] | None


class RuntimeService(Protocol):
    market_type: MarketType

    async def submit_order(self, order: OrderRequest, *, submission_mode: SubmissionMode) -> dict[str, Any]:
        ...

    async def get_klines(self, symbol: str, interval: str, *, limit: int = 500) -> list[dict[str, Any]]:
        ...

    async def raw_market_messages(self, streams: list[str], *, reconnect: bool = True):
        ...

    async def user_messages(self, *, reconnect: bool = True):
        ...


class StrategyProtocol(Protocol):
    needs_user_stream: bool

    def market_streams(self) -> list[str]:
        ...

    async def on_start(self, ctx: "StrategyContext") -> StrategyOutput:
        ...

    async def on_market_event(self, ctx: "StrategyContext", event: StrategyEvent) -> StrategyOutput:
        ...

    async def on_user_event(self, ctx: "StrategyContext", event: StrategyEvent) -> StrategyOutput:
        ...


@dataclass(slots=True)
class StrategyContext:
    service: RuntimeService
    submission_mode: SubmissionMode
    state: dict[str, Any] = field(default_factory=dict)
    logger: logging.Logger = field(default_factory=lambda: LOGGER)

    async def get_klines(self, symbol: str, interval: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return await self.service.get_klines(symbol, interval, limit=limit)

    def market_buy(
        self,
        symbol: str,
        *,
        quantity: Decimal | None = None,
        quote_order_qty: Decimal | None = None,
        position_side: PositionSide | None = None,
        reduce_only: bool | None = None,
    ) -> OrderRequest:
        return OrderRequest(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            market_type=self.service.market_type,
            quantity=quantity,
            quote_order_qty=quote_order_qty,
            position_side=position_side,
            reduce_only=reduce_only,
            new_order_resp_type="ACK" if self.service.market_type is MarketType.FUTURES else "FULL",
        )

    def market_sell(
        self,
        symbol: str,
        *,
        quantity: Decimal,
        position_side: PositionSide | None = None,
        reduce_only: bool | None = None,
    ) -> OrderRequest:
        return OrderRequest(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            market_type=self.service.market_type,
            quantity=quantity,
            position_side=position_side,
            reduce_only=reduce_only,
            new_order_resp_type="ACK" if self.service.market_type is MarketType.FUTURES else "FULL",
        )

    def limit_buy(
        self,
        symbol: str,
        *,
        quantity: Decimal,
        price: Decimal,
        position_side: PositionSide | None = None,
        reduce_only: bool | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
    ) -> OrderRequest:
        return OrderRequest(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            market_type=self.service.market_type,
            quantity=quantity,
            price=price,
            time_in_force=time_in_force,
            position_side=position_side,
            reduce_only=reduce_only,
            new_order_resp_type="ACK" if self.service.market_type is MarketType.FUTURES else "FULL",
        )

    def limit_sell(
        self,
        symbol: str,
        *,
        quantity: Decimal,
        price: Decimal,
        position_side: PositionSide | None = None,
        reduce_only: bool | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
    ) -> OrderRequest:
        return OrderRequest(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            market_type=self.service.market_type,
            quantity=quantity,
            price=price,
            time_in_force=time_in_force,
            position_side=position_side,
            reduce_only=reduce_only,
            new_order_resp_type="ACK" if self.service.market_type is MarketType.FUTURES else "FULL",
        )


class StrategyRunner:
    def __init__(self, service: RuntimeService, strategy: StrategyProtocol, submission_mode: SubmissionMode) -> None:
        self.service = service
        self.strategy = strategy
        self.ctx = StrategyContext(service=service, submission_mode=submission_mode)
        self.action_count = 0

    async def run(self) -> dict[str, Any]:
        initial = await _maybe_await(self.strategy.on_start(self.ctx))
        stop = await self._consume_output(initial)
        if stop:
            return self._summary(stop.reason)

        streams = list(self.strategy.market_streams())
        if not streams and not getattr(self.strategy, "needs_user_stream", False):
            raise ValueError("strategy must declare at least one market stream or opt into user stream events")

        queue: asyncio.Queue[StrategyEvent] = asyncio.Queue()
        tasks: list[asyncio.Task[None]] = []
        try:
            if streams:
                tasks.append(asyncio.create_task(self._pump_market(streams, queue)))
            if getattr(self.strategy, "needs_user_stream", False):
                tasks.append(asyncio.create_task(self._pump_user(queue)))

            while True:
                event = await queue.get()
                if event.channel == "market":
                    output = await _maybe_await(self.strategy.on_market_event(self.ctx, event))
                else:
                    output = await _maybe_await(self.strategy.on_user_event(self.ctx, event))
                stop = await self._consume_output(output)
                if stop:
                    return self._summary(stop.reason)
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _pump_market(self, streams: list[str], queue: asyncio.Queue[StrategyEvent]) -> None:
        async for message in self.service.raw_market_messages(streams):
            await queue.put(StrategyEvent(channel="market", stream=message.get("stream"), payload=message))

    async def _pump_user(self, queue: asyncio.Queue[StrategyEvent]) -> None:
        async for message in self.service.user_messages():
            await queue.put(StrategyEvent(channel="user", payload=message))

    async def _consume_output(self, output: StrategyOutput) -> StopStrategy | None:
        if output is None:
            return None
        if isinstance(output, StopStrategy):
            return output
        if isinstance(output, OrderRequest):
            await self._execute_order(output)
            return None
        for item in output:
            if isinstance(item, StopStrategy):
                return item
            await self._execute_order(item)
        return None

    async def _execute_order(self, order: OrderRequest) -> dict[str, Any]:
        self.action_count += 1
        normalized = replace(order, market_type=self.service.market_type)
        result = await self.service.submit_order(normalized, submission_mode=self.ctx.submission_mode)
        self.ctx.state["last_order_result"] = result
        return result

    def _summary(self, reason: str) -> dict[str, Any]:
        payload = {
            "status": "STOPPED",
            "actions": self.action_count,
            "reason": reason,
        }
        if "last_order_result" in self.ctx.state:
            payload["last_order_result"] = self.ctx.state["last_order_result"]
        return payload


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def load_strategy(strategy_ref: str, params: dict[str, Any] | None = None) -> StrategyProtocol:
    params = params or {}
    location, _, attribute = strategy_ref.partition(":")
    if not attribute:
        attribute = "create_strategy"

    module = _load_module(location)
    if not hasattr(module, attribute):
        raise ValueError(f"strategy reference {strategy_ref!r} does not expose attribute {attribute!r}")

    target = getattr(module, attribute)
    strategy = target(**params) if callable(target) else target

    for method_name in ("market_streams", "on_start", "on_market_event", "on_user_event"):
        if not hasattr(strategy, method_name):
            raise ValueError(f"loaded strategy is missing {method_name}()")
    return strategy


def parse_strategy_params(params_json: str) -> dict[str, Any]:
    payload = json.loads(params_json) if params_json.strip() else {}
    if not isinstance(payload, dict):
        raise ValueError("strategy params must decode to a JSON object")
    return payload


def _load_module(location: str) -> ModuleType:
    path = Path(location)
    if path.exists():
        module_name = path.stem.replace("-", "_")
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ValueError(f"cannot import strategy from file {location}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    return importlib.import_module(location)
