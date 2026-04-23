from __future__ import annotations

import asyncio
import concurrent.futures
import math
import os
import re
from dataclasses import asdict, dataclass
from statistics import fmean, pstdev
from typing import Any, Protocol

from .builtin_strategies import BaseKlineSignalStrategy, Candle, builtin_strategy_names, create_strategy, list_strategies
from .types import MarketType

_INTERVAL_RE = re.compile(r"^(?P<count>\d+)(?P<unit>[mhdw])$")


class HistoricalKlineService(Protocol):
    async def get_klines(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int = 500,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[dict[str, Any]]:
        ...


@dataclass(slots=True)
class BacktestConfig:
    market_type: MarketType
    initial_capital: float = 10_000.0
    fee_bps: float = 10.0
    slippage_bps: float = 2.0
    leverage: float = 1.0
    position_fraction: float = 1.0

    def __post_init__(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        if self.fee_bps < 0 or self.slippage_bps < 0:
            raise ValueError("fee_bps and slippage_bps must be non-negative")
        if self.leverage <= 0:
            raise ValueError("leverage must be positive")
        if not (0 < self.position_fraction <= 1):
            raise ValueError("position_fraction must be in (0, 1]")


@dataclass(slots=True)
class BacktestTrade:
    side: str
    entry_time: int
    exit_time: int
    entry_price: float
    exit_price: float
    pnl: float
    return_pct: float
    fees: float
    bars_held: int
    entry_reason: str
    exit_reason: str


@dataclass(slots=True)
class _OpenPosition:
    side: int
    units: float
    entry_price: float
    entry_time: int
    entry_reason: str
    entry_index: int
    entry_notional: float
    capital_at_risk: float
    fees_paid: float


def interval_to_minutes(interval: str) -> int:
    match = _INTERVAL_RE.match(interval.strip().lower())
    if not match:
        raise ValueError(f"unsupported interval {interval!r}")
    count = int(match.group("count"))
    unit = match.group("unit")
    multipliers = {"m": 1, "h": 60, "d": 60 * 24, "w": 60 * 24 * 7}
    return count * multipliers[unit]


def annualization_factor(interval: str) -> float:
    minutes = interval_to_minutes(interval)
    return (365 * 24 * 60) / minutes


async def fetch_recent_candles(
    service: HistoricalKlineService,
    symbol: str,
    interval: str,
    *,
    bars: int,
    page_limit: int = 1000,
) -> list[Candle]:
    if bars <= 0:
        raise ValueError("bars must be positive")

    remaining = bars
    end_time: int | None = None
    chunks: list[list[Candle]] = []

    while remaining > 0:
        payload = await service.get_klines(
            symbol,
            interval,
            limit=min(page_limit, remaining),
            end_time=end_time,
        )
        if not payload:
            break

        chunk = [Candle.from_rest(item) for item in payload]
        chunks.insert(0, chunk)
        remaining -= len(chunk)

        if len(chunk) < min(page_limit, remaining + len(chunk)):
            break
        end_time = chunk[0].open_time - 1

    candles = [item for chunk in chunks for item in chunk]
    deduped: list[Candle] = []
    seen_open_times: set[int] = set()
    for candle in candles:
        if candle.open_time in seen_open_times:
            continue
        seen_open_times.add(candle.open_time)
        deduped.append(candle)
    return deduped[-bars:]


def run_backtest(strategy: BaseKlineSignalStrategy, candles: list[Candle], config: BacktestConfig) -> dict[str, Any]:
    return run_backtest_with_options(strategy, candles, config, include_equity_curve=False)


def run_backtest_with_options(
    strategy: BaseKlineSignalStrategy,
    candles: list[Candle],
    config: BacktestConfig,
    *,
    include_equity_curve: bool,
) -> dict[str, Any]:
    if len(candles) < strategy.required_bars() + 1:
        raise ValueError(
            f"not enough candles for {strategy.__class__.__name__}; need at least {strategy.required_bars() + 1}, "
            f"got {len(candles)}"
        )

    fee_rate = config.fee_bps / 10_000
    slippage_rate = config.slippage_bps / 10_000
    bars_factor = annualization_factor(strategy.interval)

    cash = config.initial_capital
    equity_curve: list[float] = []
    equity_times: list[int] = []
    trades: list[BacktestTrade] = []
    position: _OpenPosition | None = None
    pending_target: int | None = None
    pending_reason: str | None = None
    bars_in_market = 0
    total_fees = 0.0
    total_turnover = 0.0
    status = "OK"

    def current_equity(mark_price: float) -> float:
        if position is None:
            return cash
        if config.market_type is MarketType.SPOT:
            return cash + (position.units * mark_price)
        return cash + (position.units * (mark_price - position.entry_price))

    def _position_side() -> int:
        if position is None:
            return 0
        return 1 if position.units > 0 else -1

    def _spot_target(signal_direction: int) -> int | None:
        current = _position_side()
        if signal_direction > 0 and strategy.trade_side in {"long", "both"} and current == 0:
            return 1
        if signal_direction < 0 and current > 0:
            return 0
        return None

    def _futures_target(signal_direction: int) -> int | None:
        current = _position_side()
        if signal_direction > 0:
            if strategy.trade_side in {"long", "both"}:
                return 1
            return None
        if signal_direction < 0:
            if strategy.trade_side in {"short", "both"}:
                return -1
            if strategy.trade_side == "long" and current > 0:
                return 0
        return None

    def resolve_target(signal_direction: int) -> int | None:
        if config.market_type is MarketType.SPOT:
            return _spot_target(signal_direction)
        return _futures_target(signal_direction)

    def close_position(exec_price: float, timestamp: int, index: int, reason: str) -> None:
        nonlocal cash, position, total_fees, total_turnover
        if position is None:
            return

        if config.market_type is MarketType.SPOT:
            gross = abs(position.units) * exec_price
            fee = gross * fee_rate
            cash += gross - fee
            pnl = (exec_price - position.entry_price) * position.units - position.fees_paid - fee
            total_turnover += gross
        else:
            gross = abs(position.units) * exec_price
            fee = gross * fee_rate
            realized = position.units * (exec_price - position.entry_price)
            cash += realized - fee
            pnl = realized - position.fees_paid - fee
            total_turnover += gross

        total_fees += fee
        trades.append(
            BacktestTrade(
                side="LONG" if position.side > 0 else "SHORT",
                entry_time=position.entry_time,
                exit_time=timestamp,
                entry_price=position.entry_price,
                exit_price=exec_price,
                pnl=pnl,
                return_pct=0.0 if position.capital_at_risk == 0 else pnl / position.capital_at_risk,
                fees=position.fees_paid + fee,
                bars_held=max(index - position.entry_index, 1),
                entry_reason=position.entry_reason,
                exit_reason=reason,
            )
        )
        position = None

    def open_position(target_side: int, exec_price: float, timestamp: int, index: int, reason: str) -> None:
        nonlocal cash, position, total_fees, total_turnover
        equity = current_equity(exec_price)
        if equity <= 0:
            return

        if config.market_type is MarketType.SPOT:
            notional = cash * config.position_fraction
            if notional <= 0:
                return
            fee = notional * fee_rate
            spendable = notional - fee
            units = spendable / exec_price
            cash -= notional
            capital_at_risk = notional
        else:
            notional = equity * config.position_fraction * config.leverage
            if notional <= 0:
                return
            fee = notional * fee_rate
            units = (notional / exec_price) * target_side
            cash -= fee
            capital_at_risk = notional / config.leverage

        total_fees += fee
        total_turnover += notional
        position = _OpenPosition(
            side=target_side,
            units=units,
            entry_price=exec_price,
            entry_time=timestamp,
            entry_reason=reason,
            entry_index=index,
            entry_notional=notional,
            capital_at_risk=capital_at_risk,
            fees_paid=fee,
        )

    for index, candle in enumerate(candles):
        if pending_target is not None:
            execution_price = candle.open
            if pending_target > 0:
                execution_price *= 1 + slippage_rate
            elif pending_target < 0:
                execution_price *= 1 - slippage_rate

            current_side = _position_side()
            if current_side != 0 and pending_target != current_side:
                close_position(execution_price, candle.open_time, index, pending_reason or "signal reversal")
                current_side = _position_side()

            if pending_target != 0 and current_side != pending_target:
                open_position(pending_target, execution_price, candle.open_time, index, pending_reason or "signal entry")

            pending_target = None
            pending_reason = None

        equity = current_equity(candle.close)
        if _position_side() != 0:
            bars_in_market += 1
        equity_curve.append(equity)
        equity_times.append(candle.close_time)

        if equity <= 0:
            status = "DEPLETED"
            break

        if index >= len(candles) - 1:
            continue
        if index + 1 < strategy.required_bars():
            continue

        signal = strategy.compute_signal(candles[: index + 1])
        if signal is None:
            continue
        direction, reason = signal
        target = resolve_target(direction)
        if target is None:
            continue
        if target == _position_side():
            continue
        pending_target = target
        pending_reason = reason

    if position is not None:
        final_candle = candles[-1]
        close_position(final_candle.close, final_candle.close_time, len(candles), "final mark")
        equity_curve[-1] = current_equity(final_candle.close)

    returns = [
        (equity_curve[index] / equity_curve[index - 1]) - 1
        for index in range(1, len(equity_curve))
        if equity_curve[index - 1] > 0
    ]
    max_drawdown = _max_drawdown(equity_curve)
    avg_return = fmean(returns) if returns else 0.0
    volatility = pstdev(returns) if len(returns) > 1 else 0.0
    annualized_return = _annualized_return(config.initial_capital, equity_curve[-1], len(equity_curve), bars_factor)
    annualized_vol = volatility * math.sqrt(bars_factor) if volatility > 0 else 0.0
    sharpe = (avg_return / volatility) * math.sqrt(bars_factor) if volatility > 0 else None
    winning_trades = [trade for trade in trades if trade.pnl > 0]
    losing_trades = [trade for trade in trades if trade.pnl < 0]
    gross_profit = sum(trade.pnl for trade in winning_trades)
    gross_loss = abs(sum(trade.pnl for trade in losing_trades))

    payload = {
        "status": status,
        "market_type": config.market_type.value,
        "symbol": strategy.symbol,
        "interval": strategy.interval,
        "strategy_class": strategy.__class__.__name__,
        "bars": len(candles),
        "from_open_time": candles[0].open_time,
        "to_close_time": candles[-1].close_time,
        "execution_model": "signal on closed bar, execution on next bar open",
        "assumptions": {
            "initial_capital": round(config.initial_capital, 4),
            "fee_bps": config.fee_bps,
            "slippage_bps": config.slippage_bps,
            "leverage": config.leverage,
            "position_fraction": config.position_fraction,
        },
        "metrics": {
            "sample_days": round(len(candles) / (bars_factor / 365), 4),
            "final_equity": round(equity_curve[-1], 4),
            "net_profit": round(equity_curve[-1] - config.initial_capital, 4),
            "total_return_pct": round(((equity_curve[-1] / config.initial_capital) - 1) * 100, 4),
            "annualized_return_pct": None if annualized_return is None else round(annualized_return * 100, 4),
            "annualized_volatility_pct": round(annualized_vol * 100, 4),
            "sharpe": None if sharpe is None else round(sharpe, 4),
            "max_drawdown_pct": round(max_drawdown * 100, 4),
            "trade_count": len(trades),
            "win_rate_pct": round((len(winning_trades) / len(trades)) * 100, 4) if trades else 0.0,
            "profit_factor": None if gross_loss == 0 else round(gross_profit / gross_loss, 4),
            "avg_trade_return_pct": round(fmean(trade.return_pct for trade in trades) * 100, 4) if trades else 0.0,
            "exposure_pct": round((bars_in_market / len(candles)) * 100, 4),
            "fees_paid": round(total_fees, 4),
            "turnover_multiple": round(total_turnover / config.initial_capital, 4),
        },
        "trades": [
            {
                **asdict(trade),
                "pnl": round(trade.pnl, 4),
                "return_pct": round(trade.return_pct * 100, 4),
                "fees": round(trade.fees, 4),
            }
            for trade in trades[-20:]
        ],
    }
    if include_equity_curve:
        payload["equity_curve"] = [
            {"time": timestamp, "equity": round(equity, 4)}
            for timestamp, equity in zip(equity_times, equity_curve)
        ]
    return payload


def run_walkforward_analysis(
    strategy: BaseKlineSignalStrategy,
    candles: list[Candle],
    config: BacktestConfig,
    *,
    train_bars: int,
    test_bars: int,
    step_bars: int | None = None,
) -> dict[str, Any]:
    if train_bars <= 0 or test_bars <= 0:
        raise ValueError("train_bars and test_bars must be positive")
    if len(candles) < train_bars + test_bars:
        raise ValueError("not enough candles for walk-forward analysis")

    step = step_bars or test_bars
    if step <= 0:
        raise ValueError("step_bars must be positive")

    folds: list[dict[str, Any]] = []
    start = 0
    fold_index = 1
    while start + train_bars + test_bars <= len(candles):
        train_slice = candles[start : start + train_bars]
        test_slice = candles[start + train_bars : start + train_bars + test_bars]
        train_result = run_backtest(strategy, train_slice, config)
        test_result = run_backtest(strategy, test_slice, config)
        folds.append(
            {
                "fold": fold_index,
                "train_start": train_slice[0].open_time,
                "train_end": train_slice[-1].close_time,
                "test_start": test_slice[0].open_time,
                "test_end": test_slice[-1].close_time,
                "train_metrics": train_result["metrics"],
                "test_metrics": test_result["metrics"],
            }
        )
        fold_index += 1
        start += step

    test_returns = [fold["test_metrics"]["total_return_pct"] for fold in folds]
    train_returns = [fold["train_metrics"]["total_return_pct"] for fold in folds]
    positive_test_folds = [value for value in test_returns if value > 0]
    negative_test_folds = [value for value in test_returns if value < 0]

    return {
        "strategy_name": strategy.__class__.__name__,
        "symbol": strategy.symbol,
        "interval": strategy.interval,
        "market_type": config.market_type.value,
        "train_bars": train_bars,
        "test_bars": test_bars,
        "step_bars": step,
        "fold_count": len(folds),
        "assumptions": {
            "initial_capital": round(config.initial_capital, 4),
            "fee_bps": config.fee_bps,
            "slippage_bps": config.slippage_bps,
            "leverage": config.leverage,
            "position_fraction": config.position_fraction,
        },
        "summary": {
            "avg_train_return_pct": round(fmean(train_returns), 4) if train_returns else 0.0,
            "avg_test_return_pct": round(fmean(test_returns), 4) if test_returns else 0.0,
            "median_like_test_return_pct": round(sorted(test_returns)[len(test_returns) // 2], 4) if test_returns else 0.0,
            "positive_test_fold_pct": round((len(positive_test_folds) / len(folds)) * 100, 4) if folds else 0.0,
            "best_test_return_pct": round(max(test_returns), 4) if test_returns else 0.0,
            "worst_test_return_pct": round(min(test_returns), 4) if test_returns else 0.0,
            "avg_test_drawdown_pct": round(fmean(fold["test_metrics"]["max_drawdown_pct"] for fold in folds), 4) if folds else 0.0,
            "consistency_score": round(len(positive_test_folds) / max(len(negative_test_folds), 1), 4) if folds else 0.0,
        },
        "folds": folds,
    }


def _strategy_shared_params(market_type: MarketType, symbol: str, interval: str) -> dict[str, Any]:
    return {
        "symbol": symbol.upper(),
        "interval": interval,
        "trade_side": "long" if market_type is MarketType.SPOT else "both",
    }


def _default_worker_count(task_count: int) -> int:
    cpu_count = os.cpu_count() or 2
    return max(1, min(task_count, cpu_count, 8))


def _run_named_strategy_backtest(
    name: str,
    shared_params: dict[str, Any],
    candles: list[Candle],
    config: BacktestConfig,
    include_equity_curve: bool,
) -> dict[str, Any]:
    metadata = {item["name"]: item for item in list_strategies()}
    try:
        strategy = create_strategy(name=name, **shared_params)
        result = run_backtest_with_options(strategy, candles, config, include_equity_curve=include_equity_curve)
    except Exception as exc:
        return {
            "name": name,
            "title": metadata.get(name, {}).get("title", name),
            "category": metadata.get(name, {}).get("category", "unknown"),
            "description": metadata.get(name, {}).get("description", ""),
            "status": "ERROR",
            "error": str(exc),
        }

    return {
        "name": name,
        "title": metadata.get(name, {}).get("title", name),
        "category": metadata.get(name, {}).get("category", "unknown"),
        "description": metadata.get(name, {}).get("description", ""),
        "source_urls": metadata.get(name, {}).get("source_urls", []),
        "shared_params": shared_params,
        **result,
    }


def _benchmark_builtin_strategies_sync(
    names: list[str],
    market_type: MarketType,
    symbol: str,
    interval: str,
    candles: list[Candle],
    config: BacktestConfig,
    include_equity_curve: bool,
    workers: int,
) -> dict[str, Any]:
    shared_params = _strategy_shared_params(market_type, symbol, interval)
    benchmark_results: list[dict[str, Any]] = []

    if workers <= 1 or len(names) <= 1:
        benchmark_results = [
            _run_named_strategy_backtest(name, shared_params, candles, config, include_equity_curve)
            for name in names
        ]
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(_run_named_strategy_backtest, name, shared_params, candles, config, include_equity_curve)
                for name in names
            ]
            benchmark_results = [future.result() for future in futures]

    completed = [item for item in benchmark_results if item.get("status") != "ERROR"]
    ranked = sorted(completed, key=lambda item: item["metrics"]["total_return_pct"], reverse=True)
    worst = sorted(completed, key=lambda item: item["metrics"]["total_return_pct"])[:5]

    return {
        "benchmark": {
            "market_type": market_type.value,
            "symbol": symbol,
            "interval": interval,
            "bars": len(candles),
            "strategy_count": len(benchmark_results),
            "completed_count": len(completed),
            "failed_count": len(benchmark_results) - len(completed),
            "workers": workers,
            "assumptions": {
                "initial_capital": config.initial_capital,
                "fee_bps": config.fee_bps,
                "slippage_bps": config.slippage_bps,
                "leverage": config.leverage,
                "position_fraction": config.position_fraction,
            },
        },
        "top_strategies": [
            {
                "name": item["name"],
                "title": item["title"],
                "total_return_pct": item["metrics"]["total_return_pct"],
                "max_drawdown_pct": item["metrics"]["max_drawdown_pct"],
                "profit_factor": item["metrics"]["profit_factor"],
                "trade_count": item["metrics"]["trade_count"],
            }
            for item in ranked[:5]
        ],
        "worst_strategies": [
            {
                "name": item["name"],
                "title": item["title"],
                "total_return_pct": item["metrics"]["total_return_pct"],
                "max_drawdown_pct": item["metrics"]["max_drawdown_pct"],
                "profit_factor": item["metrics"]["profit_factor"],
                "trade_count": item["metrics"]["trade_count"],
            }
            for item in worst
        ],
        "strategies": benchmark_results,
    }


async def benchmark_builtin_strategies(
    service: HistoricalKlineService,
    *,
    market_type: MarketType,
    symbol: str,
    interval: str,
    bars: int,
    initial_capital: float,
    fee_bps: float,
    slippage_bps: float,
    leverage: float,
    position_fraction: float,
    strategy_names: list[str] | None = None,
    include_equity_curve: bool = True,
    workers: int | None = None,
) -> dict[str, Any]:
    names = strategy_names or builtin_strategy_names()
    symbol = symbol.upper()
    candles = await fetch_recent_candles(service, symbol, interval, bars=bars)
    config = BacktestConfig(
        market_type=market_type,
        initial_capital=initial_capital,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        leverage=leverage,
        position_fraction=position_fraction,
    )
    chosen_workers = workers or _default_worker_count(len(names))
    return await asyncio.to_thread(
        _benchmark_builtin_strategies_sync,
        names,
        market_type,
        symbol,
        interval,
        candles,
        config,
        include_equity_curve,
        chosen_workers,
    )


def _max_drawdown(equity_curve: list[float]) -> float:
    peak = equity_curve[0]
    max_drawdown = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak <= 0:
            continue
        max_drawdown = max(max_drawdown, (peak - value) / peak)
    return max_drawdown


def _annualized_return(initial_capital: float, final_equity: float, bars: int, bars_per_year: float) -> float | None:
    if initial_capital <= 0 or final_equity <= 0 or bars <= 1:
        return None
    years = bars / bars_per_year
    if years <= 0 or years < (30 / 365):
        return None
    log_return = math.log(final_equity / initial_capital) / years
    if abs(log_return) > 50:
        return None
    return math.exp(log_return) - 1
