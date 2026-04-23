from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from statistics import fmean, pstdev
from typing import Any, Callable

from .strategy_runtime import StopStrategy, StrategyContext, StrategyEvent
from .types import MarketType


def _float(value: Any) -> float:
    return float(value)


@dataclass(slots=True)
class Candle:
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trade_count: int

    @classmethod
    def from_rest(cls, payload: dict[str, Any]) -> "Candle":
        return cls(
            open_time=int(payload["open_time"]),
            close_time=int(payload["close_time"]),
            open=_float(payload["open"]),
            high=_float(payload["high"]),
            low=_float(payload["low"]),
            close=_float(payload["close"]),
            volume=_float(payload["volume"]),
            quote_volume=_float(payload.get("quote_volume", 0)),
            trade_count=int(payload.get("trade_count", 0)),
        )

    @classmethod
    def from_stream(cls, payload: dict[str, Any]) -> "Candle":
        return cls(
            open_time=int(payload["t"]),
            close_time=int(payload["T"]),
            open=_float(payload["o"]),
            high=_float(payload["h"]),
            low=_float(payload["l"]),
            close=_float(payload["c"]),
            volume=_float(payload["v"]),
            quote_volume=_float(payload.get("q", 0)),
            trade_count=int(payload.get("n", 0)),
        )


def closes(candles: list[Candle]) -> list[float]:
    return [item.close for item in candles]


def highs(candles: list[Candle]) -> list[float]:
    return [item.high for item in candles]


def lows(candles: list[Candle]) -> list[float]:
    return [item.low for item in candles]


def volumes(candles: list[Candle]) -> list[float]:
    return [item.volume for item in candles]


def typical_prices(candles: list[Candle]) -> list[float]:
    return [(item.high + item.low + item.close) / 3 for item in candles]


def sma(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return fmean(values[-period:])


def ema_series(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    multiplier = 2 / (period + 1)
    result = [values[0]]
    for value in values[1:]:
        result.append((value - result[-1]) * multiplier + result[-1])
    return result


def ema(values: list[float], period: int) -> float | None:
    series = ema_series(values, period)
    return None if len(series) < period else series[-1]


def rolling_std(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return pstdev(values[-period:])


def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for index in range(1, period + 1):
        delta = values[index] - values[index - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    avg_gain = fmean(gains)
    avg_loss = fmean(losses)
    for index in range(period + 1, len(values)):
        delta = values[index] - values[index - 1]
        gain = max(delta, 0)
        loss = max(-delta, 0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def rsi_series(values: list[float], period: int = 14) -> list[float]:
    result: list[float] = []
    for index in range(period, len(values)):
        current = rsi(values[: index + 1], period)
        if current is not None:
            result.append(current)
    return result


def macd(values: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[float, float, float] | None:
    if len(values) < slow + signal:
        return None
    fast_series = ema_series(values, fast)
    slow_series = ema_series(values, slow)
    macd_line = [fast_value - slow_value for fast_value, slow_value in zip(fast_series, slow_series)]
    signal_series = ema_series(macd_line, signal)
    histogram = macd_line[-1] - signal_series[-1]
    return macd_line[-1], signal_series[-1], histogram


def stoch(high_values: list[float], low_values: list[float], close_values: list[float], k_period: int = 14, d_period: int = 3) -> tuple[float, float] | None:
    if len(close_values) < k_period + d_period - 1:
        return None
    raw_k_values: list[float] = []
    for index in range(k_period - 1, len(close_values)):
        highest = max(high_values[index - k_period + 1 : index + 1])
        lowest = min(low_values[index - k_period + 1 : index + 1])
        denominator = highest - lowest
        raw_k = 50.0 if denominator == 0 else 100 * ((close_values[index] - lowest) / denominator)
        raw_k_values.append(raw_k)
    if len(raw_k_values) < d_period:
        return None
    return raw_k_values[-1], fmean(raw_k_values[-d_period:])


def stoch_rsi(values: list[float], period: int = 14, smooth_k: int = 3, smooth_d: int = 3) -> tuple[float, float] | None:
    rsi_values = rsi_series(values, period)
    if len(rsi_values) < period + smooth_d:
        return None
    raw_values: list[float] = []
    for index in range(period - 1, len(rsi_values)):
        window = rsi_values[index - period + 1 : index + 1]
        highest = max(window)
        lowest = min(window)
        denominator = highest - lowest
        raw = 50.0 if denominator == 0 else 100 * ((rsi_values[index] - lowest) / denominator)
        raw_values.append(raw)
    if len(raw_values) < smooth_k + smooth_d - 1:
        return None
    smooth_k_values = [fmean(raw_values[index - smooth_k + 1 : index + 1]) for index in range(smooth_k - 1, len(raw_values))]
    if len(smooth_k_values) < smooth_d:
        return None
    return smooth_k_values[-1], fmean(smooth_k_values[-smooth_d:])


def bollinger(values: list[float], period: int = 20, stddev_mult: float = 2.0) -> tuple[float, float, float] | None:
    mid = sma(values, period)
    stddev = rolling_std(values, period)
    if mid is None or stddev is None:
        return None
    return mid + (stddev * stddev_mult), mid, mid - (stddev * stddev_mult)


def cci(candles: list[Candle], period: int = 20) -> float | None:
    tps = typical_prices(candles)
    if len(tps) < period:
        return None
    window = tps[-period:]
    mean = fmean(window)
    mean_dev = fmean([abs(value - mean) for value in window])
    if mean_dev == 0:
        return 0.0
    return (window[-1] - mean) / (0.015 * mean_dev)


def obv(close_values: list[float], volume_values: list[float]) -> list[float]:
    if not close_values:
        return []
    result = [0.0]
    for index in range(1, len(close_values)):
        if close_values[index] > close_values[index - 1]:
            result.append(result[-1] + volume_values[index])
        elif close_values[index] < close_values[index - 1]:
            result.append(result[-1] - volume_values[index])
        else:
            result.append(result[-1])
    return result


def cmf(candles: list[Candle], period: int = 20) -> float | None:
    if len(candles) < period:
        return None
    window = candles[-period:]
    mfv_sum = 0.0
    volume_sum = 0.0
    for candle in window:
        denominator = candle.high - candle.low
        multiplier = 0.0 if denominator == 0 else ((candle.close - candle.low) - (candle.high - candle.close)) / denominator
        mfv_sum += multiplier * candle.volume
        volume_sum += candle.volume
    if volume_sum == 0:
        return 0.0
    return mfv_sum / volume_sum


def mfi(candles: list[Candle], period: int = 14) -> float | None:
    if len(candles) <= period:
        return None
    positive = 0.0
    negative = 0.0
    tps = typical_prices(candles)
    for index in range(len(candles) - period, len(candles)):
        current = tps[index]
        prev = tps[index - 1]
        money_flow = current * candles[index].volume
        if current > prev:
            positive += money_flow
        elif current < prev:
            negative += money_flow
    if negative == 0:
        return 100.0
    ratio = positive / negative
    return 100 - (100 / (1 + ratio))


def true_ranges(candles: list[Candle]) -> list[float]:
    if not candles:
        return []
    result = [candles[0].high - candles[0].low]
    for index in range(1, len(candles)):
        current = candles[index]
        previous_close = candles[index - 1].close
        result.append(max(current.high - current.low, abs(current.high - previous_close), abs(current.low - previous_close)))
    return result


def atr(candles: list[Candle], period: int = 14) -> float | None:
    trs = true_ranges(candles)
    if len(trs) < period:
        return None
    value = fmean(trs[:period])
    for tr in trs[period:]:
        value = ((value * (period - 1)) + tr) / period
    return value


def aroon(candles: list[Candle], period: int = 25) -> tuple[float, float] | None:
    if len(candles) < period:
        return None
    window = candles[-period:]
    highest_index = max(range(len(window)), key=lambda idx: window[idx].high)
    lowest_index = min(range(len(window)), key=lambda idx: window[idx].low)
    up = 100 * (period - 1 - (len(window) - 1 - highest_index)) / (period - 1)
    down = 100 * (period - 1 - (len(window) - 1 - lowest_index)) / (period - 1)
    return up, down


def dmi_adx(candles: list[Candle], period: int = 14) -> tuple[float, float, float] | None:
    if len(candles) < period + 1:
        return None
    plus_dm: list[float] = []
    minus_dm: list[float] = []
    tr_values: list[float] = []
    for index in range(1, len(candles)):
        up_move = candles[index].high - candles[index - 1].high
        down_move = candles[index - 1].low - candles[index].low
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
        tr_values.append(max(
            candles[index].high - candles[index].low,
            abs(candles[index].high - candles[index - 1].close),
            abs(candles[index].low - candles[index - 1].close),
        ))
    if len(tr_values) < period:
        return None
    atr_value = fmean(tr_values[:period])
    plus_value = sum(plus_dm[:period])
    minus_value = sum(minus_dm[:period])
    dx_values: list[float] = []
    for index in range(period, len(tr_values)):
        atr_value = atr_value - (atr_value / period) + tr_values[index]
        plus_value = plus_value - (plus_value / period) + plus_dm[index]
        minus_value = minus_value - (minus_value / period) + minus_dm[index]
        plus_di = 0.0 if atr_value == 0 else 100 * (plus_value / atr_value)
        minus_di = 0.0 if atr_value == 0 else 100 * (minus_value / atr_value)
        total = plus_di + minus_di
        dx_values.append(0.0 if total == 0 else 100 * abs(plus_di - minus_di) / total)
    if not dx_values:
        plus_di = 0.0 if atr_value == 0 else 100 * (plus_value / atr_value)
        minus_di = 0.0 if atr_value == 0 else 100 * (minus_value / atr_value)
        adx_value = 0.0
    else:
        adx_value = fmean(dx_values[-period:]) if len(dx_values) >= period else fmean(dx_values)
        plus_di = 0.0 if atr_value == 0 else 100 * (plus_value / atr_value)
        minus_di = 0.0 if atr_value == 0 else 100 * (minus_value / atr_value)
    return plus_di, minus_di, adx_value


def keltner(candles: list[Candle], ema_period: int = 20, atr_period: int = 10, multiplier: float = 2.0) -> tuple[float, float, float] | None:
    close_values = closes(candles)
    center = ema(close_values, ema_period)
    atr_value = atr(candles, atr_period)
    if center is None or atr_value is None:
        return None
    return center + (atr_value * multiplier), center, center - (atr_value * multiplier)


def ichimoku(candles: list[Candle]) -> tuple[float, float, float, float] | None:
    if len(candles) < 52:
        return None
    conversion_window = candles[-9:]
    base_window = candles[-26:]
    span_b_window = candles[-52:]
    conversion = (max(item.high for item in conversion_window) + min(item.low for item in conversion_window)) / 2
    base = (max(item.high for item in base_window) + min(item.low for item in base_window)) / 2
    span_a = (conversion + base) / 2
    span_b = (max(item.high for item in span_b_window) + min(item.low for item in span_b_window)) / 2
    return conversion, base, span_a, span_b


def williams_r(candles: list[Candle], period: int = 14) -> float | None:
    if len(candles) < period:
        return None
    window = candles[-period:]
    highest = max(item.high for item in window)
    lowest = min(item.low for item in window)
    denominator = highest - lowest
    if denominator == 0:
        return 0.0
    return -100 * ((highest - window[-1].close) / denominator)


@dataclass(slots=True)
class BuiltinStrategySpec:
    name: str
    title: str
    description: str
    category: str
    source_urls: list[str]


@dataclass(slots=True)
class BaseKlineSignalStrategy:
    symbol: str = "BTCUSDT"
    interval: str = "1m"
    quote_order_qty: Decimal | str | None = Decimal("25")
    quantity: Decimal | str | None = Decimal("0.001")
    trade_side: str = "long"
    warmup_bars: int = 250
    needs_user_stream: bool = False
    _candles: deque[Candle] = field(init=False, repr=False)
    _last_close_time: int | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.symbol = self.symbol.upper()
        self.trade_side = self.trade_side.lower()
        self.quote_order_qty = None if self.quote_order_qty is None else Decimal(str(self.quote_order_qty))
        self.quantity = None if self.quantity is None else Decimal(str(self.quantity))
        self._candles = deque(maxlen=max(self.warmup_bars, self.required_bars() + 10))

    def market_streams(self) -> list[str]:
        return [f"{self.symbol.lower()}@kline_{self.interval}"]

    async def on_start(self, ctx: StrategyContext):
        history = await ctx.get_klines(self.symbol, self.interval, limit=max(self.warmup_bars, self.required_bars() + 10))
        for item in history:
            candle = Candle.from_rest(item)
            self._candles.append(candle)
            self._last_close_time = candle.close_time
        ctx.state["warmup_bars"] = len(self._candles)
        return None

    async def on_market_event(self, ctx: StrategyContext, event: StrategyEvent):
        kline = event.payload.get("data", {}).get("k")
        if not kline or not kline.get("x"):
            return None
        candle = Candle.from_stream(kline)
        if candle.close_time == self._last_close_time:
            return None
        self._last_close_time = candle.close_time
        self._candles.append(candle)
        if len(self._candles) < self.required_bars():
            return None
        signal = self.compute_signal(list(self._candles))
        if signal is None:
            return None
        direction, reason = signal
        order = self.build_order(ctx, direction)
        if order is None:
            return None
        return [order, StopStrategy(reason)]

    async def on_user_event(self, ctx: StrategyContext, event: StrategyEvent):
        return None

    def required_bars(self) -> int:
        return 100

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        raise NotImplementedError

    def build_order(self, ctx: StrategyContext, direction: int):
        if direction > 0 and self.trade_side not in {"long", "both"}:
            return None
        if direction < 0 and self.trade_side not in {"short", "both"}:
            return None

        if direction > 0:
            if ctx.service.market_type is MarketType.SPOT:
                if self.quote_order_qty is None:
                    raise ValueError("spot buy strategies require quote_order_qty")
                return ctx.market_buy(self.symbol, quote_order_qty=self.quote_order_qty)
            if self.quantity is None:
                raise ValueError("futures buy strategies require quantity")
            return ctx.market_buy(self.symbol, quantity=self.quantity)

        if ctx.service.market_type is MarketType.SPOT:
            if self.quantity is None:
                return None
            return ctx.market_sell(self.symbol, quantity=self.quantity)
        if self.quantity is None:
            raise ValueError("futures sell strategies require quantity")
        return ctx.market_sell(self.symbol, quantity=self.quantity)


@dataclass(slots=True)
class SmaCrossoverStrategy(BaseKlineSignalStrategy):
    fast_period: int = 20
    slow_period: int = 50

    def required_bars(self) -> int:
        return self.slow_period + 5

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        series = closes(candles)
        fast_now = sma(series, self.fast_period)
        slow_now = sma(series, self.slow_period)
        fast_prev = sma(series[:-1], self.fast_period)
        slow_prev = sma(series[:-1], self.slow_period)
        if None in {fast_now, slow_now, fast_prev, slow_prev}:
            return None
        if fast_prev <= slow_prev and fast_now > slow_now:
            return 1, f"sma bullish crossover {self.fast_period}/{self.slow_period}"
        if fast_prev >= slow_prev and fast_now < slow_now:
            return -1, f"sma bearish crossover {self.fast_period}/{self.slow_period}"
        return None


@dataclass(slots=True)
class EmaCrossoverStrategy(BaseKlineSignalStrategy):
    fast_period: int = 12
    slow_period: int = 26

    def required_bars(self) -> int:
        return self.slow_period + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        series = closes(candles)
        fast_now = ema(series, self.fast_period)
        slow_now = ema(series, self.slow_period)
        fast_prev = ema(series[:-1], self.fast_period)
        slow_prev = ema(series[:-1], self.slow_period)
        if None in {fast_now, slow_now, fast_prev, slow_prev}:
            return None
        if fast_prev <= slow_prev and fast_now > slow_now:
            return 1, f"ema bullish crossover {self.fast_period}/{self.slow_period}"
        if fast_prev >= slow_prev and fast_now < slow_now:
            return -1, f"ema bearish crossover {self.fast_period}/{self.slow_period}"
        return None


@dataclass(slots=True)
class MacdSignalStrategy(BaseKlineSignalStrategy):
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9

    def required_bars(self) -> int:
        return self.slow_period + self.signal_period + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        series = closes(candles)
        current = macd(series, self.fast_period, self.slow_period, self.signal_period)
        previous = macd(series[:-1], self.fast_period, self.slow_period, self.signal_period)
        if current is None or previous is None:
            return None
        line, signal_line, _ = current
        prev_line, prev_signal, _ = previous
        if prev_line <= prev_signal and line > signal_line:
            return 1, "macd crossed above signal"
        if prev_line >= prev_signal and line < signal_line:
            return -1, "macd crossed below signal"
        return None


@dataclass(slots=True)
class MacdZeroStrategy(BaseKlineSignalStrategy):
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9

    def required_bars(self) -> int:
        return self.slow_period + self.signal_period + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        series = closes(candles)
        current = macd(series, self.fast_period, self.slow_period, self.signal_period)
        previous = macd(series[:-1], self.fast_period, self.slow_period, self.signal_period)
        if current is None or previous is None:
            return None
        line, _, _ = current
        prev_line, _, _ = previous
        if prev_line <= 0 and line > 0:
            return 1, "macd crossed above zero"
        if prev_line >= 0 and line < 0:
            return -1, "macd crossed below zero"
        return None


@dataclass(slots=True)
class RsiMeanReversionStrategy(BaseKlineSignalStrategy):
    period: int = 14
    oversold: float = 30.0
    overbought: float = 70.0

    def required_bars(self) -> int:
        return self.period + 20

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        series = closes(candles)
        current = rsi(series, self.period)
        previous = rsi(series[:-1], self.period)
        if current is None or previous is None:
            return None
        if previous <= self.oversold and current > self.oversold:
            return 1, f"rsi recovered from oversold {self.oversold}"
        if previous >= self.overbought and current < self.overbought:
            return -1, f"rsi fell from overbought {self.overbought}"
        return None


@dataclass(slots=True)
class RsiTrendStrategy(BaseKlineSignalStrategy):
    period: int = 14
    bull_level: float = 60.0
    bear_level: float = 40.0

    def required_bars(self) -> int:
        return self.period + 20

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        series = closes(candles)
        current = rsi(series, self.period)
        previous = rsi(series[:-1], self.period)
        if current is None or previous is None:
            return None
        if previous <= self.bull_level and current > self.bull_level:
            return 1, f"rsi entered bullish regime above {self.bull_level}"
        if previous >= self.bear_level and current < self.bear_level:
            return -1, f"rsi entered bearish regime below {self.bear_level}"
        return None


@dataclass(slots=True)
class StochRsiStrategy(BaseKlineSignalStrategy):
    period: int = 14
    overbought: float = 80.0
    oversold: float = 20.0

    def required_bars(self) -> int:
        return (self.period * 2) + 20

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        series = closes(candles)
        current = stoch_rsi(series, self.period)
        previous = stoch_rsi(series[:-1], self.period)
        if current is None or previous is None:
            return None
        k_now, d_now = current
        k_prev, d_prev = previous
        if k_prev <= d_prev and k_now > d_now and k_now < self.oversold:
            return 1, "stochrsi bullish crossover in oversold zone"
        if k_prev >= d_prev and k_now < d_now and k_now > self.overbought:
            return -1, "stochrsi bearish crossover in overbought zone"
        return None


@dataclass(slots=True)
class SlowStochasticStrategy(BaseKlineSignalStrategy):
    k_period: int = 14
    d_period: int = 3
    oversold: float = 20.0
    overbought: float = 80.0

    def required_bars(self) -> int:
        return self.k_period + self.d_period + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        current = stoch(highs(candles), lows(candles), closes(candles), self.k_period, self.d_period)
        previous = stoch(highs(candles[:-1]), lows(candles[:-1]), closes(candles[:-1]), self.k_period, self.d_period)
        if current is None or previous is None:
            return None
        k_now, d_now = current
        k_prev, d_prev = previous
        if k_prev <= d_prev and k_now > d_now and k_now < self.oversold:
            return 1, "slow stochastic bullish crossover"
        if k_prev >= d_prev and k_now < d_now and k_now > self.overbought:
            return -1, "slow stochastic bearish crossover"
        return None


@dataclass(slots=True)
class BollingerMeanReversionStrategy(BaseKlineSignalStrategy):
    period: int = 20
    stddev_mult: float = 2.0

    def required_bars(self) -> int:
        return self.period + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        series = closes(candles)
        current = bollinger(series, self.period, self.stddev_mult)
        previous = bollinger(series[:-1], self.period, self.stddev_mult)
        if current is None or previous is None:
            return None
        upper_now, mid_now, lower_now = current
        _, _, lower_prev = previous
        prev_close = series[-2]
        close = series[-1]
        if prev_close <= lower_prev and close > lower_now and close < mid_now:
            return 1, "price reverted upward from lower bollinger band"
        if prev_close >= previous[0] and close < upper_now and close > mid_now:
            return -1, "price reverted downward from upper bollinger band"
        return None


@dataclass(slots=True)
class BollingerSqueezeBreakoutStrategy(BaseKlineSignalStrategy):
    period: int = 20
    stddev_mult: float = 2.0
    bandwidth_threshold: float = 0.05

    def required_bars(self) -> int:
        return self.period + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        series = closes(candles)
        current = bollinger(series, self.period, self.stddev_mult)
        previous = bollinger(series[:-1], self.period, self.stddev_mult)
        if current is None or previous is None:
            return None
        upper_now, mid_now, lower_now = current
        upper_prev, mid_prev, lower_prev = previous
        bandwidth_prev = 0.0 if mid_prev == 0 else (upper_prev - lower_prev) / mid_prev
        close = series[-1]
        if bandwidth_prev <= self.bandwidth_threshold and close > upper_now:
            return 1, "bollinger squeeze upside breakout"
        if bandwidth_prev <= self.bandwidth_threshold and close < lower_now:
            return -1, "bollinger squeeze downside breakout"
        return None


@dataclass(slots=True)
class CciBreakoutStrategy(BaseKlineSignalStrategy):
    period: int = 20
    threshold: float = 100.0

    def required_bars(self) -> int:
        return self.period + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        current = cci(candles, self.period)
        previous = cci(candles[:-1], self.period)
        if current is None or previous is None:
            return None
        if previous <= self.threshold and current > self.threshold:
            return 1, "cci broke above +100"
        if previous >= -self.threshold and current < -self.threshold:
            return -1, "cci broke below -100"
        return None


@dataclass(slots=True)
class CciMeanReversionStrategy(BaseKlineSignalStrategy):
    period: int = 20
    threshold: float = 100.0

    def required_bars(self) -> int:
        return self.period + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        current = cci(candles, self.period)
        previous = cci(candles[:-1], self.period)
        if current is None or previous is None:
            return None
        if previous <= -self.threshold and current > -self.threshold:
            return 1, "cci reverted upward through -100"
        if previous >= self.threshold and current < self.threshold:
            return -1, "cci reverted downward through +100"
        return None


@dataclass(slots=True)
class ObvConfirmationStrategy(BaseKlineSignalStrategy):
    lookback: int = 20
    price_ma_period: int = 20

    def required_bars(self) -> int:
        return max(self.lookback, self.price_ma_period) + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        close_values = closes(candles)
        volume_values = volumes(candles)
        obv_values = obv(close_values, volume_values)
        obv_ma = sma(obv_values, self.lookback)
        price_ma = sma(close_values, self.price_ma_period)
        if obv_ma is None or price_ma is None:
            return None
        if close_values[-1] > price_ma and obv_values[-1] > obv_ma and obv_values[-2] <= sma(obv_values[:-1], self.lookback):
            return 1, "obv confirmed upside trend"
        previous_obv_ma = sma(obv_values[:-1], self.lookback)
        if previous_obv_ma is None:
            return None
        if close_values[-1] < price_ma and obv_values[-1] < obv_ma and obv_values[-2] >= previous_obv_ma:
            return -1, "obv confirmed downside trend"
        return None


@dataclass(slots=True)
class CmfBreakoutStrategy(BaseKlineSignalStrategy):
    period: int = 20
    price_ma_period: int = 20

    def required_bars(self) -> int:
        return max(self.period, self.price_ma_period) + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        current = cmf(candles, self.period)
        previous = cmf(candles[:-1], self.period)
        price_ma = sma(closes(candles), self.price_ma_period)
        if current is None or previous is None or price_ma is None:
            return None
        close = candles[-1].close
        if previous <= 0 and current > 0 and close > price_ma:
            return 1, "cmf crossed above zero with price strength"
        if previous >= 0 and current < 0 and close < price_ma:
            return -1, "cmf crossed below zero with price weakness"
        return None


@dataclass(slots=True)
class MfiMeanReversionStrategy(BaseKlineSignalStrategy):
    period: int = 14
    oversold: float = 20.0
    overbought: float = 80.0

    def required_bars(self) -> int:
        return self.period + 20

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        current = mfi(candles, self.period)
        previous = mfi(candles[:-1], self.period)
        if current is None or previous is None:
            return None
        if previous <= self.oversold and current > self.oversold:
            return 1, "mfi recovered from oversold"
        if previous >= self.overbought and current < self.overbought:
            return -1, "mfi rolled over from overbought"
        return None


@dataclass(slots=True)
class AtrBreakoutStrategy(BaseKlineSignalStrategy):
    period: int = 14
    breakout_lookback: int = 20
    atr_mult: float = 0.5

    def required_bars(self) -> int:
        return max(self.period, self.breakout_lookback) + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        atr_value = atr(candles, self.period)
        if atr_value is None or len(candles) < self.breakout_lookback + 1:
            return None
        prev_window = candles[-self.breakout_lookback - 1 : -1]
        upper = max(item.high for item in prev_window) + (atr_value * self.atr_mult)
        lower = min(item.low for item in prev_window) - (atr_value * self.atr_mult)
        close = candles[-1].close
        if close > upper:
            return 1, "atr upside breakout"
        if close < lower:
            return -1, "atr downside breakout"
        return None


@dataclass(slots=True)
class DmiAdxTrendStrategy(BaseKlineSignalStrategy):
    period: int = 14
    adx_threshold: float = 20.0

    def required_bars(self) -> int:
        return (self.period * 3) + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        current = dmi_adx(candles, self.period)
        previous = dmi_adx(candles[:-1], self.period)
        if current is None or previous is None:
            return None
        plus_di, minus_di, adx_value = current
        prev_plus, prev_minus, prev_adx = previous
        if prev_plus <= prev_minus and plus_di > minus_di and adx_value >= self.adx_threshold and adx_value >= prev_adx:
            return 1, "dmi/adx bullish trend confirmation"
        if prev_plus >= prev_minus and plus_di < minus_di and adx_value >= self.adx_threshold and adx_value >= prev_adx:
            return -1, "dmi/adx bearish trend confirmation"
        return None


@dataclass(slots=True)
class AroonTrendStrategy(BaseKlineSignalStrategy):
    period: int = 25
    threshold: float = 70.0

    def required_bars(self) -> int:
        return self.period + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        current = aroon(candles, self.period)
        previous = aroon(candles[:-1], self.period)
        if current is None or previous is None:
            return None
        up, down = current
        prev_up, prev_down = previous
        if prev_up <= prev_down and up > down and up >= self.threshold:
            return 1, "aroon up trend signal"
        if prev_down <= prev_up and down > up and down >= self.threshold:
            return -1, "aroon down trend signal"
        return None


@dataclass(slots=True)
class KeltnerBreakoutStrategy(BaseKlineSignalStrategy):
    ema_period: int = 20
    atr_period: int = 10
    multiplier: float = 2.0

    def required_bars(self) -> int:
        return max(self.ema_period, self.atr_period) + 20

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        current = keltner(candles, self.ema_period, self.atr_period, self.multiplier)
        previous = keltner(candles[:-1], self.ema_period, self.atr_period, self.multiplier)
        if current is None or previous is None:
            return None
        upper_now, _, lower_now = current
        upper_prev, _, lower_prev = previous
        close = candles[-1].close
        prev_close = candles[-2].close
        if prev_close <= upper_prev and close > upper_now:
            return 1, "keltner upside breakout"
        if prev_close >= lower_prev and close < lower_now:
            return -1, "keltner downside breakout"
        return None


@dataclass(slots=True)
class IchimokuTrendStrategy(BaseKlineSignalStrategy):
    def required_bars(self) -> int:
        return 60

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        current = ichimoku(candles)
        previous = ichimoku(candles[:-1])
        if current is None or previous is None:
            return None
        conversion, base, span_a, span_b = current
        prev_conversion, prev_base, prev_span_a, prev_span_b = previous
        close = candles[-1].close
        prev_close = candles[-2].close
        cloud_top = max(span_a, span_b)
        cloud_bottom = min(span_a, span_b)
        prev_cloud_top = max(prev_span_a, prev_span_b)
        prev_cloud_bottom = min(prev_span_a, prev_span_b)
        if prev_conversion <= prev_base and conversion > base and prev_close <= prev_cloud_top and close > cloud_top:
            return 1, "ichimoku bullish cloud breakout"
        if prev_conversion >= prev_base and conversion < base and prev_close >= prev_cloud_bottom and close < cloud_bottom:
            return -1, "ichimoku bearish cloud breakdown"
        return None


@dataclass(slots=True)
class WilliamsRStrategy(BaseKlineSignalStrategy):
    period: int = 14
    oversold: float = -80.0
    overbought: float = -20.0

    def required_bars(self) -> int:
        return self.period + 10

    def compute_signal(self, candles: list[Candle]) -> tuple[int, str] | None:
        current = williams_r(candles, self.period)
        previous = williams_r(candles[:-1], self.period)
        if current is None or previous is None:
            return None
        if previous <= self.oversold and current > self.oversold:
            return 1, "williams %R recovered from oversold"
        if previous >= self.overbought and current < self.overbought:
            return -1, "williams %R turned down from overbought"
        return None


STRATEGY_SPECS: dict[str, BuiltinStrategySpec] = {
    "sma_crossover": BuiltinStrategySpec(
        name="sma_crossover",
        title="SMA Crossover",
        description="Fast SMA crosses slow SMA.",
        category="trend",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/sma",
            "https://www.jstor.org/stable/2328994",
        ],
    ),
    "ema_crossover": BuiltinStrategySpec(
        name="ema_crossover",
        title="EMA Crossover",
        description="Fast EMA crosses slow EMA.",
        category="trend",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/ema",
            "https://www.jstor.org/stable/2328994",
        ],
    ),
    "macd_signal": BuiltinStrategySpec(
        name="macd_signal",
        title="MACD Signal Cross",
        description="MACD line crosses the signal line.",
        category="trend",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/macd",
            "https://ta-lib.github.io/ta-doc/indicator/MACD.htm",
        ],
    ),
    "macd_zero": BuiltinStrategySpec(
        name="macd_zero",
        title="MACD Zero Cross",
        description="MACD line crosses the zero line.",
        category="trend",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/macd",
            "https://ta-lib.github.io/ta-doc/indicator/MACD.htm",
        ],
    ),
    "rsi_mean_reversion": BuiltinStrategySpec(
        name="rsi_mean_reversion",
        title="RSI Mean Reversion",
        description="RSI exits oversold or overbought zones.",
        category="mean_reversion",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/RSI",
            "https://ta-lib.github.io/ta-doc/indicator/RSI.htm",
        ],
    ),
    "rsi_regime": BuiltinStrategySpec(
        name="rsi_regime",
        title="RSI Regime",
        description="RSI enters bullish or bearish trend ranges.",
        category="trend",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/RSI",
            "https://ta-lib.github.io/ta-doc/indicator/RSI.htm",
        ],
    ),
    "stoch_rsi": BuiltinStrategySpec(
        name="stoch_rsi",
        title="StochRSI Reversal",
        description="StochRSI crosses in oversold or overbought zones.",
        category="momentum",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/stochrsi",
            "https://ta-lib.github.io/ta-doc/indicator/STOCHRSI.htm",
        ],
    ),
    "slow_stochastic": BuiltinStrategySpec(
        name="slow_stochastic",
        title="Slow Stochastic Reversal",
        description="Stochastic K crosses D in oversold or overbought zones.",
        category="momentum",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/stochastic-oscillator",
            "https://ta-lib.github.io/ta-doc/indicator/STOCH.htm",
        ],
    ),
    "bollinger_mean_reversion": BuiltinStrategySpec(
        name="bollinger_mean_reversion",
        title="Bollinger Mean Reversion",
        description="Price re-enters the Bollinger envelope after stretching outside it.",
        category="mean_reversion",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/bollinger-bands",
            "https://ta-lib.github.io/ta-doc/indicator/BBANDS.htm",
        ],
    ),
    "bollinger_squeeze_breakout": BuiltinStrategySpec(
        name="bollinger_squeeze_breakout",
        title="Bollinger Squeeze Breakout",
        description="Low bandwidth followed by a band breakout.",
        category="volatility",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/bollinger-bands",
            "https://ta-lib.github.io/ta-doc/indicator/BBANDS.htm",
        ],
    ),
    "cci_breakout": BuiltinStrategySpec(
        name="cci_breakout",
        title="CCI Breakout",
        description="CCI breaks above +100 or below -100.",
        category="momentum",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/cci",
            "https://ta-lib.github.io/ta-doc/indicator/CCI.htm",
        ],
    ),
    "cci_mean_reversion": BuiltinStrategySpec(
        name="cci_mean_reversion",
        title="CCI Mean Reversion",
        description="CCI returns from extreme territory.",
        category="mean_reversion",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/cci",
            "https://ta-lib.github.io/ta-doc/indicator/CCI.htm",
        ],
    ),
    "obv_confirmation": BuiltinStrategySpec(
        name="obv_confirmation",
        title="OBV Confirmation",
        description="Price trend confirmed by OBV strength or weakness.",
        category="volume",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/obv",
            "https://ta-lib.github.io/ta-doc/indicator/OBV.htm",
        ],
    ),
    "cmf_breakout": BuiltinStrategySpec(
        name="cmf_breakout",
        title="CMF Breakout",
        description="Chaikin Money Flow crosses zero with price confirmation.",
        category="volume",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/cmf",
        ],
    ),
    "mfi_mean_reversion": BuiltinStrategySpec(
        name="mfi_mean_reversion",
        title="MFI Mean Reversion",
        description="Money Flow Index exits oversold or overbought territory.",
        category="volume",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/mfi",
            "https://ta-lib.github.io/ta-doc/indicator/MFI.htm",
        ],
    ),
    "atr_breakout": BuiltinStrategySpec(
        name="atr_breakout",
        title="ATR Breakout",
        description="Price clears a breakout channel by an ATR buffer.",
        category="volatility",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/atr",
            "https://ta-lib.github.io/ta-doc/indicator/ATR.htm",
        ],
    ),
    "dmi_adx_trend": BuiltinStrategySpec(
        name="dmi_adx_trend",
        title="DMI/ADX Trend",
        description="Directional movement crossover with ADX trend-strength filter.",
        category="trend",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/dmi",
            "https://ta-lib.github.io/ta-doc/indicator/ADX.htm",
        ],
    ),
    "aroon_trend": BuiltinStrategySpec(
        name="aroon_trend",
        title="Aroon Trend",
        description="Aroon up/down crossover highlights fresh highs or lows.",
        category="trend",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/aroon-indicator",
            "https://ta-lib.github.io/ta-doc/indicator/AROON.htm",
        ],
    ),
    "keltner_breakout": BuiltinStrategySpec(
        name="keltner_breakout",
        title="Keltner Breakout",
        description="Price breaks outside an ATR-based Keltner channel.",
        category="volatility",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/keltner-bands",
        ],
    ),
    "ichimoku_trend": BuiltinStrategySpec(
        name="ichimoku_trend",
        title="Ichimoku Trend",
        description="Tenkan/Kijun crossover with cloud confirmation.",
        category="trend",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/ichimoku-cloud",
        ],
    ),
    "williams_r": BuiltinStrategySpec(
        name="williams_r",
        title="Williams %R Reversal",
        description="Williams %R exits oversold or overbought zones.",
        category="momentum",
        source_urls=[
            "https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/williams-r",
            "https://ta-lib.github.io/ta-doc/indicator/WILLR.htm",
        ],
    ),
}


STRATEGY_FACTORIES: dict[str, Callable[..., BaseKlineSignalStrategy]] = {
    "sma_crossover": SmaCrossoverStrategy,
    "ema_crossover": EmaCrossoverStrategy,
    "macd_signal": MacdSignalStrategy,
    "macd_zero": MacdZeroStrategy,
    "rsi_mean_reversion": RsiMeanReversionStrategy,
    "rsi_regime": RsiTrendStrategy,
    "stoch_rsi": StochRsiStrategy,
    "slow_stochastic": SlowStochasticStrategy,
    "bollinger_mean_reversion": BollingerMeanReversionStrategy,
    "bollinger_squeeze_breakout": BollingerSqueezeBreakoutStrategy,
    "cci_breakout": CciBreakoutStrategy,
    "cci_mean_reversion": CciMeanReversionStrategy,
    "obv_confirmation": ObvConfirmationStrategy,
    "cmf_breakout": CmfBreakoutStrategy,
    "mfi_mean_reversion": MfiMeanReversionStrategy,
    "atr_breakout": AtrBreakoutStrategy,
    "dmi_adx_trend": DmiAdxTrendStrategy,
    "aroon_trend": AroonTrendStrategy,
    "keltner_breakout": KeltnerBreakoutStrategy,
    "ichimoku_trend": IchimokuTrendStrategy,
    "williams_r": WilliamsRStrategy,
}


def builtin_strategy_names() -> list[str]:
    return sorted(STRATEGY_FACTORIES)


def list_strategies() -> list[dict[str, Any]]:
    return [
        {
            "name": spec.name,
            "title": spec.title,
            "category": spec.category,
            "description": spec.description,
            "source_urls": spec.source_urls,
        }
        for spec in sorted(STRATEGY_SPECS.values(), key=lambda item: item.name)
    ]


def create_strategy(name: str, **kwargs: Any) -> BaseKlineSignalStrategy:
    normalized = name.strip().lower()
    if normalized not in STRATEGY_FACTORIES:
        raise ValueError(f"unknown built-in strategy {name!r}; available: {', '.join(builtin_strategy_names())}")
    return STRATEGY_FACTORIES[normalized](**kwargs)
