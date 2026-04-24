from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from binance_trade.builtin_strategies import Candle, BaseKlineSignalStrategy, create_strategy as create_builtin_strategy
from binance_trade.strategy_runtime import StrategyContext, StrategyEvent

_TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}
_QUOTE_SUFFIXES = ("USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USD", "BTC", "ETH", "BNB")


def _split_symbol(symbol: str) -> tuple[str, str]:
    for suffix in _QUOTE_SUFFIXES:
        if symbol.endswith(suffix) and len(symbol) > len(suffix):
            return symbol[: -len(suffix)], suffix
    raise ValueError(f"unable to infer base asset from symbol {symbol!r}")


@dataclass(slots=True)
class SpotBuiltinPersistentStrategy:
    strategy_name: str = "ema_crossover"
    symbol: str = "BTCUSDT"
    interval: str = "1h"
    quote_order_qty: Decimal = Decimal("25")
    trade_side: str = "long"
    warmup_bars: int = 250
    needs_user_stream: bool = True
    _delegate: BaseKlineSignalStrategy = field(init=False, repr=False)
    _candles: deque[Candle] = field(init=False, repr=False)
    _last_close_time: int | None = field(default=None, init=False, repr=False)
    _position_qty: Decimal = field(default=Decimal("0"), init=False, repr=False)
    _pending_client_order_id: str | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        params = {
            "symbol": self.symbol,
            "interval": self.interval,
            "quote_order_qty": self.quote_order_qty,
            "trade_side": self.trade_side,
            "warmup_bars": self.warmup_bars,
        }
        self._delegate = create_builtin_strategy(name=self.strategy_name, **params)
        self.symbol = self._delegate.symbol
        self.interval = self._delegate.interval
        self.quote_order_qty = Decimal(str(self._delegate.quote_order_qty or self.quote_order_qty))
        self.trade_side = self._delegate.trade_side
        self.warmup_bars = self._delegate.warmup_bars
        self._candles = deque(maxlen=max(self.warmup_bars, self._delegate.required_bars() + 20))

    def market_streams(self) -> list[str]:
        return [f"{self.symbol.lower()}@kline_{self.interval}"]

    async def on_start(self, ctx: StrategyContext) -> None:
        history = await ctx.get_klines(self.symbol, self.interval, limit=max(self.warmup_bars, self._delegate.required_bars() + 20))
        for item in history:
            candle = Candle.from_rest(item)
            self._candles.append(candle)
            self._last_close_time = candle.close_time

        account_fn = getattr(ctx.service, "account", None)
        if callable(account_fn):
            account = await account_fn()
            self._position_qty = _extract_spot_position_qty(account, self.symbol)
        self._sync_context_state(ctx)
        ctx.logger.info(
            "persistent builtin strategy started strategy=%s symbol=%s interval=%s position_qty=%s warmup=%s",
            self.strategy_name,
            self.symbol,
            self.interval,
            self._position_qty,
            len(self._candles),
        )
        return None

    async def on_market_event(self, ctx: StrategyContext, event: StrategyEvent):
        self._capture_last_order_result(ctx)

        kline = event.payload.get("data", {}).get("k")
        if not kline or not kline.get("x"):
            return None

        candle = Candle.from_stream(kline)
        if self._last_close_time is not None and candle.close_time <= self._last_close_time:
            return None

        self._candles.append(candle)
        self._last_close_time = candle.close_time
        self._sync_context_state(ctx)

        if len(self._candles) < self._delegate.required_bars():
            return None
        if self._pending_client_order_id is not None:
            return None

        signal = self._delegate.compute_signal(list(self._candles))
        if signal is None:
            return None

        direction, reason = signal
        if direction > 0 and self._position_qty <= 0 and self.trade_side in {"long", "both"}:
            ctx.logger.info(
                "builtin strategy signal=%s bullish -> market buy symbol=%s quote=%s",
                self.strategy_name,
                self.symbol,
                self.quote_order_qty,
            )
            return ctx.market_buy(self.symbol, quote_order_qty=self.quote_order_qty)

        if direction < 0 and self._position_qty > 0:
            ctx.logger.info(
                "builtin strategy signal=%s bearish -> market sell symbol=%s quantity=%s reason=%s",
                self.strategy_name,
                self.symbol,
                self._position_qty,
                reason,
            )
            return ctx.market_sell(self.symbol, quantity=self._position_qty)

        return None

    async def on_user_event(self, ctx: StrategyContext, event: StrategyEvent) -> None:
        payload = event.payload.get("event", event.payload)
        if payload.get("e") != "executionReport":
            return None
        if payload.get("s") != self.symbol:
            return None

        client_order_id = payload.get("c")
        if client_order_id:
            self._pending_client_order_id = client_order_id

        status = payload.get("X")
        side = payload.get("S")
        filled_qty = Decimal(str(payload.get("z", "0")))
        orig_qty = Decimal(str(payload.get("q", "0")))

        if side == "BUY" and status in {"PARTIALLY_FILLED", "FILLED"}:
            self._position_qty = filled_qty
        elif side == "SELL" and status in {"PARTIALLY_FILLED", "FILLED"}:
            remaining = orig_qty - filled_qty
            self._position_qty = remaining if remaining > 0 else Decimal("0")

        if status in _TERMINAL_ORDER_STATUSES and client_order_id == self._pending_client_order_id:
            self._pending_client_order_id = None

        self._sync_context_state(ctx)
        return None

    def snapshot_state(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "interval": self.interval,
            "quote_order_qty": str(self.quote_order_qty),
            "trade_side": self.trade_side,
            "last_close_time": self._last_close_time,
            "position_qty": str(self._position_qty),
            "pending_client_order_id": self._pending_client_order_id,
            "candles": [
                {
                    "open_time": candle.open_time,
                    "close_time": candle.close_time,
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume,
                    "quote_volume": candle.quote_volume,
                    "trade_count": candle.trade_count,
                }
                for candle in self._candles
            ],
        }

    def restore_state(self, snapshot: dict[str, Any]) -> None:
        candles = snapshot.get("candles", [])
        self._candles.clear()
        for item in candles:
            self._candles.append(Candle(**item))
        self._last_close_time = snapshot.get("last_close_time")
        self._position_qty = Decimal(str(snapshot.get("position_qty", "0")))
        self._pending_client_order_id = snapshot.get("pending_client_order_id")

    def _capture_last_order_result(self, ctx: StrategyContext) -> None:
        result = ctx.state.get("last_order_result")
        if not isinstance(result, dict):
            return
        client_order_id = result.get("clientOrderId")
        if client_order_id:
            self._pending_client_order_id = str(client_order_id)

    def _sync_context_state(self, ctx: StrategyContext) -> None:
        ctx.state["strategy_name"] = self.strategy_name
        ctx.state["position_qty"] = str(self._position_qty)
        ctx.state["pending_client_order_id"] = self._pending_client_order_id
        ctx.state["last_close_time"] = self._last_close_time
        ctx.state["symbol"] = self.symbol


def _extract_spot_position_qty(account_payload: dict[str, Any], symbol: str) -> Decimal:
    base_asset, _ = _split_symbol(symbol)
    for balance in account_payload.get("balances", []):
        if balance.get("asset") != base_asset:
            continue
        free = Decimal(str(balance.get("free", "0")))
        locked = Decimal(str(balance.get("locked", "0")))
        return free + locked
    return Decimal("0")


def create_strategy(**kwargs: Any) -> SpotBuiltinPersistentStrategy:
    return SpotBuiltinPersistentStrategy(**kwargs)
