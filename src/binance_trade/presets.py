from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .types import MarketType


@dataclass(frozen=True, slots=True)
class StrategyPreset:
    name: str
    title: str
    strategy_name: str
    market: MarketType
    description: str
    environments: tuple[str, ...]
    params: dict[str, Any]
    research_bars: int
    fee_bps: float
    slippage_bps: float
    leverage: float = 1.0
    position_fraction: float = 1.0
    notes: tuple[str, ...] = ()


PRESETS: dict[str, StrategyPreset] = {
    "global_spot_ema_btc_15m": StrategyPreset(
        name="global_spot_ema_btc_15m",
        title="Global Spot EMA BTC 15m",
        strategy_name="ema_crossover",
        market=MarketType.SPOT,
        description="Long-only BTC spot trend follow using the classic 12/26 EMA crossover on 15m bars.",
        environments=("mainnet", "testnet"),
        params={
            "symbol": "BTCUSDT",
            "interval": "15m",
            "fast_period": 12,
            "slow_period": 26,
            "quote_order_qty": "25",
            "trade_side": "long",
        },
        research_bars=1500,
        fee_bps=10.0,
        slippage_bps=2.0,
        notes=(
            "Designed for spot accumulation, not for shorting.",
            "Use lower slippage assumptions only if your actual fills justify it.",
        ),
    ),
    "global_spot_bollinger_eth_5m": StrategyPreset(
        name="global_spot_bollinger_eth_5m",
        title="Global Spot Bollinger ETH 5m",
        strategy_name="bollinger_mean_reversion",
        market=MarketType.SPOT,
        description="Long-only ETH spot mean reversion after lower-band recovery on 5m bars.",
        environments=("mainnet", "testnet"),
        params={
            "symbol": "ETHUSDT",
            "interval": "5m",
            "period": 20,
            "stddev_mult": 2.0,
            "quote_order_qty": "25",
            "trade_side": "long",
        },
        research_bars=2000,
        fee_bps=10.0,
        slippage_bps=3.0,
        notes=(
            "Mean reversion weakens in strong directional trends.",
            "Prefer this on liquid symbols and tighter spreads.",
        ),
    ),
    "global_futures_dmi_btc_15m": StrategyPreset(
        name="global_futures_dmi_btc_15m",
        title="Global Futures DMI/ADX BTC 15m",
        strategy_name="dmi_adx_trend",
        market=MarketType.FUTURES,
        description="BTC perpetual trend confirmation using DMI crossovers gated by rising ADX on 15m bars.",
        environments=("mainnet", "testnet"),
        params={
            "symbol": "BTCUSDT",
            "interval": "15m",
            "period": 14,
            "adx_threshold": 25,
            "quantity": "0.0001",
            "trade_side": "both",
        },
        research_bars=1800,
        fee_bps=5.0,
        slippage_bps=2.0,
        leverage=1.0,
        notes=(
            "Backtests model directional reversals, not funding or liquidation.",
            "Increase leverage only after validating drawdown tolerance.",
        ),
    ),
    "global_futures_ichimoku_btc_15m": StrategyPreset(
        name="global_futures_ichimoku_btc_15m",
        title="Global Futures Ichimoku BTC 15m",
        strategy_name="ichimoku_trend",
        market=MarketType.FUTURES,
        description="BTC perpetual trend following with cloud breakout confirmation on 15m bars.",
        environments=("mainnet", "testnet"),
        params={
            "symbol": "BTCUSDT",
            "interval": "15m",
            "quantity": "0.0001",
            "trade_side": "both",
        },
        research_bars=1800,
        fee_bps=5.0,
        slippage_bps=2.0,
        leverage=1.0,
        notes=(
            "This preset reacts more slowly than EMA or DMI, but filters noise better.",
        ),
    ),
    "global_futures_atr_btc_15m": StrategyPreset(
        name="global_futures_atr_btc_15m",
        title="Global Futures ATR Breakout BTC 15m",
        strategy_name="atr_breakout",
        market=MarketType.FUTURES,
        description="BTC perpetual breakout trading using ATR-expanded breakout bands on 15m bars.",
        environments=("mainnet", "testnet"),
        params={
            "symbol": "BTCUSDT",
            "interval": "15m",
            "period": 14,
            "breakout_lookback": 20,
            "atr_mult": 0.5,
            "quantity": "0.0001",
            "trade_side": "both",
        },
        research_bars=1800,
        fee_bps=5.0,
        slippage_bps=2.5,
        leverage=1.0,
        notes=(
            "Breakout systems are sensitive to fee and slippage assumptions.",
        ),
    ),
}


def list_presets() -> list[dict[str, Any]]:
    return [
        {
            "name": preset.name,
            "title": preset.title,
            "strategy_name": preset.strategy_name,
            "market": preset.market.value,
            "description": preset.description,
            "environments": list(preset.environments),
            "params": preset.params,
            "research_bars": preset.research_bars,
            "fee_bps": preset.fee_bps,
            "slippage_bps": preset.slippage_bps,
            "leverage": preset.leverage,
            "position_fraction": preset.position_fraction,
            "notes": list(preset.notes),
        }
        for preset in sorted(PRESETS.values(), key=lambda item: item.name)
    ]


def get_preset(name: str) -> StrategyPreset:
    normalized = name.strip().lower()
    aliases = {
        "binance_us_spot_ema_btc_15m": "global_spot_ema_btc_15m",
        "binance_us_spot_bollinger_eth_5m": "global_spot_bollinger_eth_5m",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in PRESETS:
        available = ", ".join(sorted(PRESETS))
        raise ValueError(f"unknown preset {name!r}; available: {available}")
    return PRESETS[normalized]
