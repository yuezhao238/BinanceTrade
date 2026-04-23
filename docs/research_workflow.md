# Research Workflow

This project now separates **research** from **execution**.

Research commands:

- `binance-trade list-presets`
- `binance-trade backtest-builtin-strategy ...`
- `binance-trade backtest-preset ...`

Execution commands remain separate:

- `binance-trade run-builtin-strategy ...`
- `binance-trade run-strategy ...`
- `binance-trade buy-market ...`
- `binance-trade futures-buy-market ...`

## Modeling Assumptions

The backtester uses a deliberately conservative and explicit execution model:

- Signal is computed on the **closed candle**
- Execution happens on the **next candle open**
- Fee and slippage are modeled per side
- Spot is treated as **long-only cash inventory**
- Futures are treated as **directional mark-to-market exposure**
- Funding, liquidation, borrow costs, and partial fills are **not** modeled yet

This is not a substitute for production reconciliation, but it is a much better research baseline than same-bar execution with zero cost.

## Suggested Research Loop

1. Start with a preset.
2. Run `backtest-preset`.
3. Review:
   - `total_return_pct`
   - `max_drawdown_pct`
   - `sharpe`
   - `trade_count`
   - `profit_factor`
4. Stress the same strategy with:
   - more bars
   - higher fees
   - higher slippage
   - lower `position_fraction`
5. Only move to `run-builtin-strategy` after the strategy remains acceptable under worse assumptions.

## Example

```bash
binance-trade list-presets

binance-trade backtest-preset binance_us_spot_ema_btc_15m

binance-trade backtest-builtin-strategy ema_crossover \
  --market spot \
  --bars 2000 \
  --fee-bps 10 \
  --slippage-bps 3 \
  --params-json '{"symbol":"BTCUSDT","interval":"15m","fast_period":12,"slow_period":26,"trade_side":"long"}'
```

## Interpretation

- High return with low trade count can simply mean regime luck.
- Good Sharpe with poor profit factor can still hide ugly tail risk.
- Mean reversion strategies usually degrade faster when slippage assumptions rise.
- Breakout strategies usually degrade faster when both fee and slippage rise.
- If a strategy only works with unrealistically low costs, do not promote it to live execution.
