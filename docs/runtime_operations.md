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
name = "spot-us-core"
profiles = [
  "spot_ema_btcusdt.toml",
  "spot_ema_ethusdt.toml",
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
binance-trade show-runtime-profile examples/runtime/spot_ema_btcusdt.toml
```

Check exchange connectivity, filters, and resolved mode:

```bash
binance-trade doctor-runtime-profile examples/runtime/spot_ema_btcusdt.toml
```

Run the daemon:

```bash
binance-trade run-daemon examples/runtime/spot_ema_btcusdt.toml
```

Inspect current runtime state:

```bash
binance-trade daemon-status
binance-trade daemon-status spot-ema-btcusdt
```

Use in container healthchecks:

```bash
binance-trade daemon-health spot-ema-btcusdt
```

Inspect the stack:

```bash
binance-trade show-runtime-stack examples/runtime/spot_us_core.toml
```

Check every member profile in the stack:

```bash
binance-trade doctor-runtime-stack examples/runtime/spot_us_core.toml
```

Run the stack:

```bash
binance-trade run-daemon-stack examples/runtime/spot_us_core.toml
```

Inspect stack-level health:

```bash
binance-trade daemon-stack-status
binance-trade daemon-stack-status spot-us-core
binance-trade daemon-stack-health spot-us-core
```

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
7. Move to test orders.
8. Move to live with small size.
