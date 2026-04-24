from __future__ import annotations

import asyncio
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import typer

from .builtin_strategies import create_strategy as create_builtin_strategy
from .builtin_strategies import list_strategies as list_builtin_strategies
from .config import get_settings
from .daemon import StrategyDaemon, StrategyDaemonStack, is_runtime_stack_status_healthy, is_runtime_status_healthy
from .exceptions import BinanceTradeError
from .logging_utils import setup_logging
from .presets import get_preset, list_presets as list_research_presets
from .research import BacktestConfig, benchmark_builtin_strategies, fetch_recent_candles, run_backtest, run_walkforward_analysis
from .research_report import write_benchmark_report, write_walkforward_report
from .runtime_profiles import RuntimeProfile, RuntimeStack, load_runtime_profile, load_runtime_stack
from .service import FuturesTradingService, SpotTradingService
from .state import SQLiteStateStore
from .strategy_runtime import StrategyRunner, load_strategy, parse_strategy_params
from .types import MarketType, PositionSide, SubmissionMode

app = typer.Typer(no_args_is_help=True, help="Professional Binance Spot and USDⓈ-M Futures trading starter.")


def _print_json(payload: Any, *, pretty: bool = True) -> None:
    typer.echo(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2 if pretty else None,
            sort_keys=True,
            default=str,
        )
    )


def _run(coro: Any, *, pretty: bool = True) -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    try:
        result = asyncio.run(coro)
    except (BinanceTradeError, ValueError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    if result is not None:
        _print_json(result, pretty=pretty)


def _decimal(value: str) -> Decimal:
    return Decimal(value)


def _float_decimal(value: str) -> float:
    return float(Decimal(value))


def _csv_list(value: str | None) -> list[str] | None:
    if value is None or not value.strip():
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def _resolve_mode(live: bool, test_order: bool) -> SubmissionMode | None:
    if live and test_order:
        raise ValueError("--live and --test-order are mutually exclusive")
    if live:
        return SubmissionMode.LIVE
    if test_order:
        return SubmissionMode.TEST
    return None


def _position_side(value: str | None) -> PositionSide | None:
    if value is None:
        return None
    return PositionSide(value.upper())


async def _with_spot_service(callback: Any) -> Any:
    async with SpotTradingService(get_settings()) as service:
        return await callback(service)


async def _with_futures_service(callback: Any) -> Any:
    async with FuturesTradingService(get_settings()) as service:
        return await callback(service)


def _load_runtime_profile(path: str) -> RuntimeProfile:
    return load_runtime_profile(Path(path))


def _load_runtime_stack(path: str) -> RuntimeStack:
    return load_runtime_stack(Path(path))


def _service_factory_for_profile(settings: Any, profile: RuntimeProfile) -> Any:
    if profile.market is MarketType.SPOT:
        return lambda: SpotTradingService(settings)
    return lambda: FuturesTradingService(settings)


async def _profile_doctor(profile: RuntimeProfile, submission_mode: SubmissionMode | None) -> dict[str, Any]:
    settings = get_settings()
    selected_mode = profile.resolve_submission_mode(submission_mode, default_dry_run=settings.dry_run)
    symbol = profile.params.get("symbol")

    async def _spot() -> dict[str, Any]:
        async with SpotTradingService(settings) as service:
            payload = await service.doctor(symbol)
            payload["runtime_profile"] = profile.to_dict()
            payload["resolved_submission_mode"] = selected_mode.value
            return payload

    async def _futures() -> dict[str, Any]:
        async with FuturesTradingService(settings) as service:
            payload = await service.doctor(symbol)
            payload["runtime_profile"] = profile.to_dict()
            payload["resolved_submission_mode"] = selected_mode.value
            return payload

    if profile.market is MarketType.SPOT:
        return await _spot()
    return await _futures()


async def _run_daemon_profile(profile: RuntimeProfile, submission_mode: SubmissionMode | None) -> dict[str, Any]:
    settings = get_settings()
    selected_mode = profile.resolve_submission_mode(submission_mode, default_dry_run=settings.dry_run)
    store = SQLiteStateStore(settings.state_db_path)

    if selected_mode != profile.resolve_submission_mode(None, default_dry_run=settings.dry_run):
        profile = RuntimeProfile(
            name=profile.name,
            market=profile.market,
            strategy_ref=profile.strategy_ref,
            params=profile.params,
            submission_mode=selected_mode,
            description=profile.description,
            notes=profile.notes,
            daemon=profile.daemon,
            path=profile.path,
        )

    daemon = StrategyDaemon(
        settings=settings,
        profile=profile,
        state_store=store,
        service_factory=_service_factory_for_profile(settings, profile),
    )
    return await daemon.run()


async def _stack_doctor(stack: RuntimeStack, submission_mode: SubmissionMode | None) -> dict[str, Any]:
    results = []
    for profile in stack.profiles:
        payload = await _profile_doctor(profile, submission_mode)
        results.append({"profile_name": profile.name, "doctor": payload})
    return {"runtime_stack": stack.to_dict(), "profiles": results}


async def _run_daemon_stack(stack: RuntimeStack) -> dict[str, Any]:
    settings = get_settings()
    store = SQLiteStateStore(settings.state_db_path)
    daemon = StrategyDaemonStack(
        settings=settings,
        stack=stack,
        state_store=store,
        service_factory_resolver=lambda profile: _service_factory_for_profile(settings, profile),
    )
    return await daemon.run()


def _runtime_status_payload(service_name: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    store = SQLiteStateStore(settings.state_db_path)
    if service_name:
        payload = store.get_runtime_status(service_name)
        if payload is None:
            raise ValueError(f"runtime service {service_name!r} was not found")
        payload["healthy"] = is_runtime_status_healthy(payload)
        return payload
    statuses = store.list_runtime_statuses()
    for item in statuses:
        item["healthy"] = is_runtime_status_healthy(item)
    return {"services": statuses}


def _runtime_stack_status_payload(stack_name: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    store = SQLiteStateStore(settings.state_db_path)
    if stack_name:
        payload = store.get_runtime_stack_status(stack_name)
        if payload is None:
            raise ValueError(f"runtime stack {stack_name!r} was not found")
        payload["healthy"] = is_runtime_stack_status_healthy(payload)
        return payload
    stacks = store.list_runtime_stack_statuses()
    for item in stacks:
        item["healthy"] = is_runtime_stack_status_healthy(item)
    return {"stacks": stacks}


@app.command("doctor")
def doctor(symbol: str | None = typer.Argument(None, help="Spot symbol to validate, defaults to DEFAULT_SYMBOL.")) -> None:
    _run(_with_spot_service(lambda service: service.doctor(symbol)))


@app.command("price")
def price(symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT.")) -> None:
    _run(_with_spot_service(lambda service: service.price(symbol)))


@app.command("account")
def account() -> None:
    _run(_with_spot_service(lambda service: service.account()))


@app.command("buy-market")
def buy_market(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    quote: str = typer.Option(..., "--quote", help="Quote notional, for example 25 USDT."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_spot_service(
            lambda service: service.buy_market(
                symbol,
                quote_order_qty=_decimal(quote),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("sell-market")
def sell_market(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Base asset quantity."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_spot_service(
            lambda service: service.sell_market(
                symbol,
                quantity=_decimal(quantity),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("buy-limit")
def buy_limit(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Base asset quantity."),
    price: str = typer.Option(..., "--price", help="Limit price."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_spot_service(
            lambda service: service.buy_limit(
                symbol,
                quantity=_decimal(quantity),
                price=_decimal(price),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("sell-limit")
def sell_limit(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Base asset quantity."),
    price: str = typer.Option(..., "--price", help="Limit price."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_spot_service(
            lambda service: service.sell_limit(
                symbol,
                quantity=_decimal(quantity),
                price=_decimal(price),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("order-status")
def order_status(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    client_order_id: str | None = typer.Option(None, "--client-order-id", help="Client order id."),
    order_id: int | None = typer.Option(None, "--order-id", help="Exchange order id."),
) -> None:
    _run(_with_spot_service(lambda service: service.order_status(symbol, client_order_id=client_order_id, order_id=order_id)))


@app.command("cancel")
def cancel(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    client_order_id: str | None = typer.Option(None, "--client-order-id", help="Client order id."),
    order_id: int | None = typer.Option(None, "--order-id", help="Exchange order id."),
) -> None:
    _run(_with_spot_service(lambda service: service.cancel(symbol, client_order_id=client_order_id, order_id=order_id)))


@app.command("reconcile")
def reconcile(symbol: str | None = typer.Argument(None, help="Optional spot trading pair.")) -> None:
    _run(_with_spot_service(lambda service: service.reconcile(symbol)))


@app.command("watch-market")
def watch_market(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    stream: str = typer.Option("miniTicker", "--stream", help="trade, miniTicker, bookTicker, kline_1m, or full stream name."),
    reconnect: bool = typer.Option(True, "--reconnect/--no-reconnect", help="Reconnect on disconnect."),
) -> None:
    async def _watch() -> None:
        async with SpotTradingService(get_settings()) as service:
            async for message in service.market_messages(symbol, stream, reconnect=reconnect):
                _print_json(message, pretty=False)

    _run(_watch(), pretty=False)


@app.command("watch-user")
def watch_user(reconnect: bool = typer.Option(True, "--reconnect/--no-reconnect", help="Reconnect on disconnect.")) -> None:
    async def _watch() -> None:
        async with SpotTradingService(get_settings()) as service:
            async for message in service.user_messages(reconnect=reconnect):
                _print_json(message, pretty=False)

    _run(_watch(), pretty=False)


@app.command("run-demo-strategy")
def run_demo_strategy(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    quote: str = typer.Option("25", "--quote", help="Quote notional to buy when triggered."),
    lookback: int = typer.Option(30, "--lookback", help="Number of miniTicker points for the rolling average."),
    trigger_pct: str = typer.Option("0.003", "--trigger-pct", help="Buy when price is this far below the rolling average."),
    live: bool = typer.Option(False, "--live", help="Send a live order when the signal triggers."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test when the signal triggers."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_spot_service(
            lambda service: service.run_demo_strategy(
                symbol,
                quote_order_qty=_decimal(quote),
                lookback=lookback,
                trigger_pct=_decimal(trigger_pct),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("list-strategies")
def list_strategies() -> None:
    _print_json({"builtin_strategies": list_builtin_strategies()})


@app.command("list-presets")
def list_presets() -> None:
    _print_json({"presets": list_research_presets()})


@app.command("show-runtime-profile")
def show_runtime_profile(path: str = typer.Argument(..., help="Path to a runtime profile TOML or JSON file.")) -> None:
    profile = _load_runtime_profile(path)
    _print_json({"runtime_profile": profile.to_dict()})


@app.command("show-runtime-stack")
def show_runtime_stack(path: str = typer.Argument(..., help="Path to a runtime stack TOML or JSON file.")) -> None:
    stack = _load_runtime_stack(path)
    _print_json({"runtime_stack": stack.to_dict()})


@app.command("doctor-runtime-profile")
def doctor_runtime_profile(
    path: str = typer.Argument(..., help="Path to a runtime profile TOML or JSON file."),
    live: bool = typer.Option(False, "--live", help="Resolve this profile in live mode."),
    test_order: bool = typer.Option(False, "--test-order", help="Resolve this profile in exchange test mode."),
) -> None:
    profile = _load_runtime_profile(path)
    _run(_profile_doctor(profile, _resolve_mode(live, test_order)))


@app.command("doctor-runtime-stack")
def doctor_runtime_stack(
    path: str = typer.Argument(..., help="Path to a runtime stack TOML or JSON file."),
    live: bool = typer.Option(False, "--live", help="Resolve this stack in live mode."),
    test_order: bool = typer.Option(False, "--test-order", help="Resolve this stack in exchange test mode."),
) -> None:
    stack = _load_runtime_stack(path)
    _run(_stack_doctor(stack, _resolve_mode(live, test_order)))


@app.command("run-daemon")
def run_daemon(
    path: str = typer.Argument(..., help="Path to a runtime profile TOML or JSON file."),
    live: bool = typer.Option(False, "--live", help="Force live submission mode."),
    test_order: bool = typer.Option(False, "--test-order", help="Force exchange test submission mode."),
) -> None:
    profile = _load_runtime_profile(path)
    _run(_run_daemon_profile(profile, _resolve_mode(live, test_order)))


@app.command("run-daemon-stack")
def run_daemon_stack(
    path: str = typer.Argument(..., help="Path to a runtime stack TOML or JSON file."),
) -> None:
    stack = _load_runtime_stack(path)
    _run(_run_daemon_stack(stack))


@app.command("daemon-status")
def daemon_status(
    service_name: str | None = typer.Argument(None, help="Optional runtime service name. Omit to list all tracked services."),
) -> None:
    _print_json(_runtime_status_payload(service_name))


@app.command("daemon-health")
def daemon_health(
    service_name: str = typer.Argument(..., help="Runtime service name."),
) -> None:
    payload = _runtime_status_payload(service_name)
    if not payload.get("healthy", False):
        _print_json(payload)
        raise typer.Exit(code=1)
    _print_json(payload)


@app.command("daemon-stack-status")
def daemon_stack_status(
    stack_name: str | None = typer.Argument(None, help="Optional runtime stack name. Omit to list all tracked stacks."),
) -> None:
    _print_json(_runtime_stack_status_payload(stack_name))


@app.command("daemon-stack-health")
def daemon_stack_health(
    stack_name: str = typer.Argument(..., help="Runtime stack name."),
) -> None:
    payload = _runtime_stack_status_payload(stack_name)
    if not payload.get("healthy", False):
        _print_json(payload)
        raise typer.Exit(code=1)
    _print_json(payload)


@app.command("run-builtin-strategy")
def run_builtin_strategy(
    name: str = typer.Argument(..., help="Built-in strategy name."),
    market: str = typer.Option("spot", "--market", help="spot or futures."),
    params_json: str = typer.Option("{}", "--params-json", help="JSON object passed to the strategy constructor."),
    live: bool = typer.Option(False, "--live", help="Send live orders."),
    test_order: bool = typer.Option(False, "--test-order", help="Send exchange test orders."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    params = parse_strategy_params(params_json)

    async def _run_spot() -> dict[str, Any]:
        async with SpotTradingService(get_settings()) as service:
            runner = StrategyRunner(
                service=service,
                strategy=create_builtin_strategy(name=name, **params),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
            return await runner.run()

    async def _run_futures() -> dict[str, Any]:
        async with FuturesTradingService(get_settings()) as service:
            runner = StrategyRunner(
                service=service,
                strategy=create_builtin_strategy(name=name, **params),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
            return await runner.run()

    if market.lower() == "spot":
        _run(_run_spot())
        return
    if market.lower() == "futures":
        _run(_run_futures())
        return
    raise ValueError("--market must be spot or futures")


@app.command("backtest-builtin-strategy")
def backtest_builtin_strategy(
    name: str = typer.Argument(..., help="Built-in strategy name."),
    market: str = typer.Option("spot", "--market", help="spot or futures."),
    params_json: str = typer.Option("{}", "--params-json", help="JSON object passed to the strategy constructor."),
    bars: int = typer.Option(1500, "--bars", min=100, help="Number of recent klines to load."),
    capital: str = typer.Option("10000", "--capital", help="Initial research capital."),
    fee_bps: float = typer.Option(10.0, "--fee-bps", help="Round-trip modeling fee per side in basis points."),
    slippage_bps: float = typer.Option(2.0, "--slippage-bps", help="Execution slippage per side in basis points."),
    leverage: float = typer.Option(1.0, "--leverage", help="Research leverage assumption, mainly for futures."),
    position_fraction: float = typer.Option(1.0, "--position-fraction", min=0.01, max=1.0, help="Fraction of equity deployed per trade."),
) -> None:
    params = parse_strategy_params(params_json)

    async def _run_spot() -> dict[str, Any]:
        async with SpotTradingService(get_settings()) as service:
            strategy = create_builtin_strategy(name=name, **params)
            candles = await fetch_recent_candles(service, strategy.symbol, strategy.interval, bars=bars)
            return run_backtest(
                strategy,
                candles,
                BacktestConfig(
                    market_type=MarketType.SPOT,
                    initial_capital=_float_decimal(capital),
                    fee_bps=fee_bps,
                    slippage_bps=slippage_bps,
                    leverage=1.0,
                    position_fraction=position_fraction,
                ),
            )

    async def _run_futures() -> dict[str, Any]:
        async with FuturesTradingService(get_settings()) as service:
            strategy = create_builtin_strategy(name=name, **params)
            candles = await fetch_recent_candles(service, strategy.symbol, strategy.interval, bars=bars)
            return run_backtest(
                strategy,
                candles,
                BacktestConfig(
                    market_type=MarketType.FUTURES,
                    initial_capital=_float_decimal(capital),
                    fee_bps=fee_bps,
                    slippage_bps=slippage_bps,
                    leverage=leverage,
                    position_fraction=position_fraction,
                ),
            )

    if market.lower() == "spot":
        _run(_run_spot())
        return
    if market.lower() == "futures":
        _run(_run_futures())
        return
    raise ValueError("--market must be spot or futures")


@app.command("backtest-preset")
def backtest_preset(
    name: str = typer.Argument(..., help="Research preset name."),
    bars: int | None = typer.Option(None, "--bars", min=100, help="Override the preset history size."),
    capital: str = typer.Option("10000", "--capital", help="Initial research capital."),
    fee_bps: float | None = typer.Option(None, "--fee-bps", help="Override preset fee assumption."),
    slippage_bps: float | None = typer.Option(None, "--slippage-bps", help="Override preset slippage assumption."),
    leverage: float | None = typer.Option(None, "--leverage", help="Override preset leverage assumption."),
    position_fraction: float | None = typer.Option(None, "--position-fraction", min=0.01, max=1.0, help="Override preset position fraction."),
) -> None:
    preset = get_preset(name)
    chosen_bars = bars or preset.research_bars
    chosen_fee_bps = preset.fee_bps if fee_bps is None else fee_bps
    chosen_slippage_bps = preset.slippage_bps if slippage_bps is None else slippage_bps
    chosen_leverage = preset.leverage if leverage is None else leverage
    chosen_position_fraction = preset.position_fraction if position_fraction is None else position_fraction

    async def _run_spot() -> dict[str, Any]:
        async with SpotTradingService(get_settings()) as service:
            strategy = create_builtin_strategy(name=preset.strategy_name, **preset.params)
            candles = await fetch_recent_candles(service, strategy.symbol, strategy.interval, bars=chosen_bars)
            result = run_backtest(
                strategy,
                candles,
                BacktestConfig(
                    market_type=MarketType.SPOT,
                    initial_capital=_float_decimal(capital),
                    fee_bps=chosen_fee_bps,
                    slippage_bps=chosen_slippage_bps,
                    leverage=1.0,
                    position_fraction=chosen_position_fraction,
                ),
            )
            result["preset"] = name
            result["preset_notes"] = list(preset.notes)
            return result

    async def _run_futures() -> dict[str, Any]:
        async with FuturesTradingService(get_settings()) as service:
            strategy = create_builtin_strategy(name=preset.strategy_name, **preset.params)
            candles = await fetch_recent_candles(service, strategy.symbol, strategy.interval, bars=chosen_bars)
            result = run_backtest(
                strategy,
                candles,
                BacktestConfig(
                    market_type=MarketType.FUTURES,
                    initial_capital=_float_decimal(capital),
                    fee_bps=chosen_fee_bps,
                    slippage_bps=chosen_slippage_bps,
                    leverage=chosen_leverage,
                    position_fraction=chosen_position_fraction,
                ),
            )
            result["preset"] = name
            result["preset_notes"] = list(preset.notes)
            return result

    if preset.market is MarketType.SPOT:
        _run(_run_spot())
        return
    _run(_run_futures())


@app.command("benchmark-builtin-strategies")
def benchmark_all_strategies(
    market: str = typer.Option("spot", "--market", help="spot or futures."),
    symbol: str = typer.Option("BTCUSDT", "--symbol", help="Benchmark symbol."),
    interval: str = typer.Option("15m", "--interval", help="Common interval for all strategies."),
    bars: int = typer.Option(1500, "--bars", min=200, help="Number of recent klines."),
    capital: str = typer.Option("10000", "--capital", help="Initial research capital."),
    fee_bps: float = typer.Option(10.0, "--fee-bps", help="Modeled fee per side in basis points."),
    slippage_bps: float = typer.Option(2.0, "--slippage-bps", help="Modeled slippage per side in basis points."),
    leverage: float = typer.Option(1.0, "--leverage", help="Leverage assumption, mainly for futures."),
    position_fraction: float = typer.Option(1.0, "--position-fraction", min=0.01, max=1.0, help="Fraction of equity deployed per signal."),
    strategies: str | None = typer.Option(None, "--strategies", help="Optional comma-separated built-in strategy names."),
    out_dir: str | None = typer.Option(None, "--out-dir", help="Optional output directory for JSON/SVG/HTML files."),
    workers: int | None = typer.Option(None, "--workers", min=1, help="Optional worker count for parallel backtests."),
) -> None:
    chosen_market = market.lower()
    chosen_names = _csv_list(strategies)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    resolved_out_dir = Path(out_dir) if out_dir else Path("var/research_reports") / f"{chosen_market}_{symbol.upper()}_{interval}_{timestamp}"

    async def _run_spot() -> dict[str, Any]:
        async with SpotTradingService(get_settings()) as service:
            benchmark = await benchmark_builtin_strategies(
                service,
                market_type=MarketType.SPOT,
                symbol=symbol,
                interval=interval,
                bars=bars,
                initial_capital=_float_decimal(capital),
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
                leverage=1.0,
                position_fraction=position_fraction,
                strategy_names=chosen_names,
                workers=workers,
            )
            artifacts = write_benchmark_report(benchmark, resolved_out_dir.resolve())
            return {
                "benchmark": benchmark["benchmark"],
                "top_strategies": benchmark["top_strategies"],
                "worst_strategies": benchmark["worst_strategies"],
                "artifacts": artifacts,
            }

    async def _run_futures() -> dict[str, Any]:
        async with FuturesTradingService(get_settings()) as service:
            benchmark = await benchmark_builtin_strategies(
                service,
                market_type=MarketType.FUTURES,
                symbol=symbol,
                interval=interval,
                bars=bars,
                initial_capital=_float_decimal(capital),
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
                leverage=leverage,
                position_fraction=position_fraction,
                strategy_names=chosen_names,
                workers=workers,
            )
            artifacts = write_benchmark_report(benchmark, resolved_out_dir.resolve())
            return {
                "benchmark": benchmark["benchmark"],
                "top_strategies": benchmark["top_strategies"],
                "worst_strategies": benchmark["worst_strategies"],
                "artifacts": artifacts,
            }

    if chosen_market == "spot":
        _run(_run_spot())
        return
    if chosen_market == "futures":
        _run(_run_futures())
        return
    raise ValueError("--market must be spot or futures")


@app.command("walkforward-builtin-strategy")
def walkforward_builtin_strategy(
    name: str = typer.Argument(..., help="Built-in strategy name."),
    market: str = typer.Option("spot", "--market", help="spot or futures."),
    params_json: str = typer.Option("{}", "--params-json", help="JSON object passed to the strategy constructor."),
    bars: int = typer.Option(2000, "--bars", min=300, help="Number of recent klines to load."),
    train_bars: int = typer.Option(1000, "--train-bars", min=100, help="Number of bars in each in-sample fold."),
    test_bars: int = typer.Option(250, "--test-bars", min=50, help="Number of bars in each out-of-sample fold."),
    step_bars: int | None = typer.Option(None, "--step-bars", min=1, help="Fold step size; defaults to test-bars."),
    capital: str = typer.Option("10000", "--capital", help="Initial research capital."),
    fee_bps: float = typer.Option(10.0, "--fee-bps", help="Modeled fee per side in basis points."),
    slippage_bps: float = typer.Option(2.0, "--slippage-bps", help="Modeled slippage per side in basis points."),
    leverage: float = typer.Option(1.0, "--leverage", help="Leverage assumption, mainly for futures."),
    position_fraction: float = typer.Option(1.0, "--position-fraction", min=0.01, max=1.0, help="Fraction of equity deployed per signal."),
    out_dir: str | None = typer.Option(None, "--out-dir", help="Optional output directory for JSON/SVG/HTML files."),
) -> None:
    params = parse_strategy_params(params_json)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    resolved_out_dir = Path(out_dir) if out_dir else Path("var/walkforward_reports") / f"{market.lower()}_{name}_{timestamp}"

    async def _run_spot() -> dict[str, Any]:
        async with SpotTradingService(get_settings()) as service:
            strategy = create_builtin_strategy(name=name, **params)
            candles = await fetch_recent_candles(service, strategy.symbol, strategy.interval, bars=bars)
            report = run_walkforward_analysis(
                strategy,
                candles,
                BacktestConfig(
                    market_type=MarketType.SPOT,
                    initial_capital=_float_decimal(capital),
                    fee_bps=fee_bps,
                    slippage_bps=slippage_bps,
                    leverage=1.0,
                    position_fraction=position_fraction,
                ),
                train_bars=train_bars,
                test_bars=test_bars,
                step_bars=step_bars,
            )
            artifacts = write_walkforward_report(report, resolved_out_dir.resolve())
            return {"summary": report["summary"], "fold_count": report["fold_count"], "artifacts": artifacts}

    async def _run_futures() -> dict[str, Any]:
        async with FuturesTradingService(get_settings()) as service:
            strategy = create_builtin_strategy(name=name, **params)
            candles = await fetch_recent_candles(service, strategy.symbol, strategy.interval, bars=bars)
            report = run_walkforward_analysis(
                strategy,
                candles,
                BacktestConfig(
                    market_type=MarketType.FUTURES,
                    initial_capital=_float_decimal(capital),
                    fee_bps=fee_bps,
                    slippage_bps=slippage_bps,
                    leverage=leverage,
                    position_fraction=position_fraction,
                ),
                train_bars=train_bars,
                test_bars=test_bars,
                step_bars=step_bars,
            )
            artifacts = write_walkforward_report(report, resolved_out_dir.resolve())
            return {"summary": report["summary"], "fold_count": report["fold_count"], "artifacts": artifacts}

    if market.lower() == "spot":
        _run(_run_spot())
        return
    if market.lower() == "futures":
        _run(_run_futures())
        return
    raise ValueError("--market must be spot or futures")


@app.command("walkforward-preset")
def walkforward_preset(
    name: str = typer.Argument(..., help="Research preset name."),
    bars: int | None = typer.Option(None, "--bars", min=300, help="Override the preset history size."),
    train_bars: int = typer.Option(1000, "--train-bars", min=100, help="Number of bars in each in-sample fold."),
    test_bars: int = typer.Option(250, "--test-bars", min=50, help="Number of bars in each out-of-sample fold."),
    step_bars: int | None = typer.Option(None, "--step-bars", min=1, help="Fold step size; defaults to test-bars."),
    capital: str = typer.Option("10000", "--capital", help="Initial research capital."),
    fee_bps: float | None = typer.Option(None, "--fee-bps", help="Override preset fee assumption."),
    slippage_bps: float | None = typer.Option(None, "--slippage-bps", help="Override preset slippage assumption."),
    leverage: float | None = typer.Option(None, "--leverage", help="Override preset leverage assumption."),
    position_fraction: float | None = typer.Option(None, "--position-fraction", min=0.01, max=1.0, help="Override preset position fraction."),
    out_dir: str | None = typer.Option(None, "--out-dir", help="Optional output directory for JSON/SVG/HTML files."),
) -> None:
    preset = get_preset(name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    resolved_out_dir = Path(out_dir) if out_dir else Path("var/walkforward_reports") / f"{preset.market.value}_{name}_{timestamp}"
    chosen_bars = bars or max(preset.research_bars, train_bars + test_bars)
    chosen_fee_bps = preset.fee_bps if fee_bps is None else fee_bps
    chosen_slippage_bps = preset.slippage_bps if slippage_bps is None else slippage_bps
    chosen_leverage = preset.leverage if leverage is None else leverage
    chosen_position_fraction = preset.position_fraction if position_fraction is None else position_fraction

    async def _run_spot() -> dict[str, Any]:
        async with SpotTradingService(get_settings()) as service:
            strategy = create_builtin_strategy(name=preset.strategy_name, **preset.params)
            candles = await fetch_recent_candles(service, strategy.symbol, strategy.interval, bars=chosen_bars)
            report = run_walkforward_analysis(
                strategy,
                candles,
                BacktestConfig(
                    market_type=MarketType.SPOT,
                    initial_capital=_float_decimal(capital),
                    fee_bps=chosen_fee_bps,
                    slippage_bps=chosen_slippage_bps,
                    leverage=1.0,
                    position_fraction=chosen_position_fraction,
                ),
                train_bars=train_bars,
                test_bars=test_bars,
                step_bars=step_bars,
            )
            artifacts = write_walkforward_report(report, resolved_out_dir.resolve())
            return {"preset": name, "summary": report["summary"], "fold_count": report["fold_count"], "artifacts": artifacts}

    async def _run_futures() -> dict[str, Any]:
        async with FuturesTradingService(get_settings()) as service:
            strategy = create_builtin_strategy(name=preset.strategy_name, **preset.params)
            candles = await fetch_recent_candles(service, strategy.symbol, strategy.interval, bars=chosen_bars)
            report = run_walkforward_analysis(
                strategy,
                candles,
                BacktestConfig(
                    market_type=MarketType.FUTURES,
                    initial_capital=_float_decimal(capital),
                    fee_bps=chosen_fee_bps,
                    slippage_bps=chosen_slippage_bps,
                    leverage=chosen_leverage,
                    position_fraction=chosen_position_fraction,
                ),
                train_bars=train_bars,
                test_bars=test_bars,
                step_bars=step_bars,
            )
            artifacts = write_walkforward_report(report, resolved_out_dir.resolve())
            return {"preset": name, "summary": report["summary"], "fold_count": report["fold_count"], "artifacts": artifacts}

    if preset.market is MarketType.SPOT:
        _run(_run_spot())
        return
    _run(_run_futures())


@app.command("run-strategy")
def run_strategy(
    strategy_ref: str = typer.Argument(..., help="Module path or file path, e.g. examples/strategies/spot_mean_reversion.py:create_strategy"),
    market: str = typer.Option("spot", "--market", help="spot or futures."),
    params_json: str = typer.Option("{}", "--params-json", help="JSON object passed to the strategy factory."),
    live: bool = typer.Option(False, "--live", help="Send live orders."),
    test_order: bool = typer.Option(False, "--test-order", help="Send exchange test orders."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    params = parse_strategy_params(params_json)

    async def _run_spot() -> dict[str, Any]:
        async with SpotTradingService(get_settings()) as service:
            runner = StrategyRunner(
                service=service,
                strategy=load_strategy(strategy_ref, params),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
            return await runner.run()

    async def _run_futures() -> dict[str, Any]:
        async with FuturesTradingService(get_settings()) as service:
            runner = StrategyRunner(
                service=service,
                strategy=load_strategy(strategy_ref, params),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
            return await runner.run()

    if market.lower() == "spot":
        _run(_run_spot())
        return
    if market.lower() == "futures":
        _run(_run_futures())
        return
    raise ValueError("--market must be spot or futures")


@app.command("futures-doctor")
def futures_doctor(symbol: str | None = typer.Argument(None, help="Futures symbol to validate, defaults to FUTURES_DEFAULT_SYMBOL.")) -> None:
    _run(_with_futures_service(lambda service: service.doctor(symbol)))


@app.command("futures-price")
def futures_price(symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT.")) -> None:
    _run(_with_futures_service(lambda service: service.price(symbol)))


@app.command("futures-account")
def futures_account() -> None:
    _run(_with_futures_service(lambda service: service.account()))


@app.command("futures-positions")
def futures_positions(symbol: str | None = typer.Argument(None, help="Optional futures trading pair.")) -> None:
    _run(_with_futures_service(lambda service: service.positions(symbol)))


@app.command("futures-buy-market")
def futures_buy_market(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Contract quantity."),
    position_side: str | None = typer.Option(None, "--position-side", help="BOTH, LONG, or SHORT."),
    reduce_only: bool = typer.Option(False, "--reduce-only", help="Mark the order reduce-only."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_futures_service(
            lambda service: service.buy_market(
                symbol,
                quantity=_decimal(quantity),
                position_side=_position_side(position_side),
                reduce_only=reduce_only or None,
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("futures-sell-market")
def futures_sell_market(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Contract quantity."),
    position_side: str | None = typer.Option(None, "--position-side", help="BOTH, LONG, or SHORT."),
    reduce_only: bool = typer.Option(False, "--reduce-only", help="Mark the order reduce-only."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_futures_service(
            lambda service: service.sell_market(
                symbol,
                quantity=_decimal(quantity),
                position_side=_position_side(position_side),
                reduce_only=reduce_only or None,
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("futures-buy-limit")
def futures_buy_limit(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Contract quantity."),
    price: str = typer.Option(..., "--price", help="Limit price."),
    position_side: str | None = typer.Option(None, "--position-side", help="BOTH, LONG, or SHORT."),
    reduce_only: bool = typer.Option(False, "--reduce-only", help="Mark the order reduce-only."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_futures_service(
            lambda service: service.buy_limit(
                symbol,
                quantity=_decimal(quantity),
                price=_decimal(price),
                position_side=_position_side(position_side),
                reduce_only=reduce_only or None,
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("futures-sell-limit")
def futures_sell_limit(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Contract quantity."),
    price: str = typer.Option(..., "--price", help="Limit price."),
    position_side: str | None = typer.Option(None, "--position-side", help="BOTH, LONG, or SHORT."),
    reduce_only: bool = typer.Option(False, "--reduce-only", help="Mark the order reduce-only."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_futures_service(
            lambda service: service.sell_limit(
                symbol,
                quantity=_decimal(quantity),
                price=_decimal(price),
                position_side=_position_side(position_side),
                reduce_only=reduce_only or None,
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("futures-order-status")
def futures_order_status(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    client_order_id: str | None = typer.Option(None, "--client-order-id", help="Client order id."),
    order_id: int | None = typer.Option(None, "--order-id", help="Exchange order id."),
) -> None:
    _run(_with_futures_service(lambda service: service.order_status(symbol, client_order_id=client_order_id, order_id=order_id)))


@app.command("futures-cancel")
def futures_cancel(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    client_order_id: str | None = typer.Option(None, "--client-order-id", help="Client order id."),
    order_id: int | None = typer.Option(None, "--order-id", help="Exchange order id."),
) -> None:
    _run(_with_futures_service(lambda service: service.cancel(symbol, client_order_id=client_order_id, order_id=order_id)))


@app.command("futures-reconcile")
def futures_reconcile(symbol: str | None = typer.Argument(None, help="Optional futures trading pair.")) -> None:
    _run(_with_futures_service(lambda service: service.reconcile(symbol)))


@app.command("futures-watch-market")
def futures_watch_market(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    stream: str = typer.Option("miniTicker", "--stream", help="trade, miniTicker, bookTicker, kline_1m, or full stream name."),
    reconnect: bool = typer.Option(True, "--reconnect/--no-reconnect", help="Reconnect on disconnect."),
) -> None:
    async def _watch() -> None:
        async with FuturesTradingService(get_settings()) as service:
            async for message in service.market_messages(symbol, stream, reconnect=reconnect):
                _print_json(message, pretty=False)

    _run(_watch(), pretty=False)


@app.command("futures-watch-user")
def futures_watch_user(reconnect: bool = typer.Option(True, "--reconnect/--no-reconnect", help="Reconnect on disconnect.")) -> None:
    async def _watch() -> None:
        async with FuturesTradingService(get_settings()) as service:
            async for message in service.user_messages(reconnect=reconnect):
                _print_json(message, pretty=False)

    _run(_watch(), pretty=False)


@app.command("futures-set-leverage")
def futures_set_leverage(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    leverage: int = typer.Option(..., "--leverage", help="Target leverage, 1-125."),
) -> None:
    _run(_with_futures_service(lambda service: service.set_leverage(symbol, leverage)))


@app.command("futures-set-margin-type")
def futures_set_margin_type(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    margin_type: str = typer.Option(..., "--margin-type", help="ISOLATED or CROSSED."),
) -> None:
    _run(_with_futures_service(lambda service: service.set_margin_type(symbol, margin_type)))


@app.command("futures-run-demo-strategy")
def futures_run_demo_strategy(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    quantity: str = typer.Option("0.001", "--quantity", help="Contract quantity to buy when triggered."),
    lookback: int = typer.Option(30, "--lookback", help="Number of miniTicker points for the rolling average."),
    trigger_pct: str = typer.Option("0.003", "--trigger-pct", help="Buy when price is this far below the rolling average."),
    live: bool = typer.Option(False, "--live", help="Send a live order when the signal triggers."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test when the signal triggers."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_futures_service(
            lambda service: service.run_demo_strategy(
                symbol,
                quantity=_decimal(quantity),
                lookback=lookback,
                trigger_pct=_decimal(trigger_pct),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


if __name__ == "__main__":
    app()
