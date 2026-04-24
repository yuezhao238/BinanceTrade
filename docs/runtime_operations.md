# Runtime Operations

This document covers the production-facing runtime layer that sits between research and live execution.

## What The Daemon Adds

Compared with `run-strategy` and `run-builtin-strategy`, the daemon adds:

- supervised restarts with bounded backoff
- startup and periodic reconciliation when credentials are present
- SQLite-backed runtime sessions and status snapshots
- mirrored JSON heartbeat files under `RUNTIME_DIR`
- CLI health checks that can be used from Docker healthchecks or external process monitors

The daemon is meant for persistent strategies that keep running until the process is stopped. Built-in K-line strategies are research templates and intentionally stop after a signal.

## Runtime Profile Format

Runtime profiles are TOML or JSON files.

Minimal example:

```toml
name = "spot-ema-btcusdt"
market = "spot"
strategy_ref = "examples/strategies/spot_ema_persistent.py:create_strategy"
submission_mode = "inherit"

[params]
symbol = "BTCUSDT"
interval = "1h"
fast_period = 20
slow_period = 50
quote_order_qty = "25"

[daemon]
reconcile_on_start = true
reconcile_interval_seconds = 300
heartbeat_interval_seconds = 30
auto_restart = true
restart_initial_delay_seconds = 5
restart_max_delay_seconds = 60
stop_on_strategy_exit = false
stale_after_seconds = 90
```

## Runtime Stack Format

Stacks are TOML or JSON files that reference multiple runtime profiles.

Example:

```toml
name = "global-spot-core"
profiles = [
  "global_spot_ema_btcusdt.toml",
  "global_spot_ema_ethusdt.toml",
]

[settings]
heartbeat_interval_seconds = 30
stale_after_seconds = 90
stop_on_member_exit = true
stop_on_member_failure = true
```

## Operator Commands

Inspect the profile:

```bash
binance-trade show-runtime-profile examples/runtime/global_spot_ema_btcusdt.toml
```

Check exchange connectivity, filters, and resolved mode:

```bash
binance-trade doctor-runtime-profile examples/runtime/global_spot_ema_btcusdt.toml
```

Run the daemon:

```bash
binance-trade run-daemon examples/runtime/global_spot_ema_btcusdt.toml
```

Inspect current runtime state:

```bash
binance-trade daemon-status
binance-trade daemon-status global-spot-ema-btcusdt
```

Use in container healthchecks:

```bash
binance-trade daemon-health global-spot-ema-btcusdt
```

Inspect the stack:

```bash
binance-trade show-runtime-stack examples/runtime/global_spot_core.toml
```

Check every member profile in the stack:

```bash
binance-trade doctor-runtime-stack examples/runtime/global_spot_core.toml
```

If this reports a `ProxyError` before any Binance JSON response, disable environment-inherited proxies for the bot:

```bash
NETWORK_TRUST_ENV=false
```

Run the stack:

```bash
binance-trade run-daemon-stack examples/runtime/global_spot_core.toml
```

Inspect stack-level health:

```bash
binance-trade daemon-stack-status
binance-trade daemon-stack-status global-spot-core
binance-trade daemon-stack-health global-spot-core
```

Run the local ops dashboard:

```bash
binance-trade run-dashboard
```

Open [http://127.0.0.1:8765](http://127.0.0.1:8765) and keep the daemon running in a separate terminal.

If you only want the dashboard JSON payload for automation or quick inspection:

```bash
binance-trade dashboard-snapshot --no-portfolio
```

## Dashboard Operations

The dashboard is a local control console, not only a monitor. The operations panel can:

- start or stop a runtime stack
- start or stop a single runtime profile
- run `doctor` on a stack or profile
- run Spot `reconcile` for a symbol
- submit manual Spot market buys or sells

For manual orders, keep the submission mode on `DRY_RUN` until you have explicitly validated the flow.  
If you switch a dashboard order form to `LIVE`, it will submit a real order with your configured API key.

## Runtime State

The daemon writes to two places:

- `STATE_DB_PATH`
  - `runtime_sessions` keeps session history
  - `runtime_status` keeps the latest snapshot per service
- `RUNTIME_DIR/<service-name>.json`
  - current heartbeat snapshot for simple process monitors

The stack supervisor adds:

- `runtime_stack_sessions`
- `runtime_stack_status`
- `RUNTIME_DIR/stack-<stack-name>.json`

## Promotion Path

1. Research with `backtest-*`, `benchmark-builtin-strategies`, and `walkforward-*`.
2. Move the chosen idea into a persistent custom strategy.
3. Wrap it in a runtime profile.
4. Validate with `doctor-runtime-profile`.
5. Choose whether to deploy one profile or a stack of profiles.
6. Run under `run-daemon` or `run-daemon-stack` with `DRY_RUN=true`.
7. Observe health with `daemon-*` and the local dashboard.
8. Move to test orders.
9. Move to live with small size.
