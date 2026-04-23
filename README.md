# Binance Trade

`binance-trade` is a clean Python starter for connecting your own Binance account and trading through:

- Spot REST for account queries and order entry
- USDⓈ-M Futures REST for contract trading, leverage, margin mode, and positions
- WebSocket Streams for market data
- Spot WebSocket API user data subscriptions for private account events
- Futures listenKey user streams for private account and order events
- Local risk gates and SQLite-backed order state
- A pluggable strategy runtime that loads your strategy module from a file path or import path

This project was implemented against Binance official Spot documentation reviewed on April 22, 2026. The design follows the current recommendations:

- REST signed requests require timestamp plus signature and now explicitly document percent-encoding before signing.
- Spot user data can be consumed through the WebSocket API using `userDataStream.subscribe.signature`.
- Market streams and WebSocket API connections are expected to disconnect after 24 hours and should reconnect cleanly.
- USDⓈ-M Futures uses `https://fapi.binance.com` on mainnet, `https://demo-fapi.binance.com` on testnet, and recommends WebSocket streams for timely state while warning that `503` unknown-status responses must be reconciled before retry.
- Futures `exchangeInfo` explicitly says to use `tickSize` and `stepSize` instead of `pricePrecision` and `quantityPrecision`.

Primary references:

- [Spot REST general info](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-api-information)
- [Spot request security](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/request-security)
- [Spot trading endpoints](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/trading-endpoints)
- [Spot filters](https://developers.binance.com/docs/binance-spot-api-docs/filters)
- [Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [Spot user data stream](https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream)
- [Spot WebSocket API user data requests](https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/user-data-stream-requests)
- [Spot testnet general info](https://developers.binance.com/docs/binance-spot-api-docs/testnet/general-info)
- [USDⓈ-M Futures general info](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info)
- [USDⓈ-M Futures new order](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Order)
- [USDⓈ-M Futures exchange info](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information)
- [USDⓈ-M Futures account information v3](https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Account-Information-V3)
- [USDⓈ-M Futures position information v3](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Position-Information-V3)
- [USDⓈ-M Futures user data streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams)
- [USDⓈ-M Futures WebSocket API general info](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info)

## Features

- HMAC, RSA, and Ed25519 request signing
- Testnet and mainnet endpoint switching
- Exchange rule parsing for `PRICE_FILTER`, `LOT_SIZE`, `MARKET_LOT_SIZE`, `MIN_NOTIONAL`, `NOTIONAL`, `MAX_NUM_ORDERS`, and `MAX_POSITION`
- Deterministic client order IDs
- Configurable risk checks:
  - max order notional
  - max open orders per symbol
  - symbol allow-list
  - cooldown between accepted orders
- Futures support:
  - market and limit orders
  - `positionSide`
  - `reduceOnly`
  - leverage changes
  - margin type changes
  - position queries
- Local SQLite order/event journal
- Strategy runtime:
  - load strategy from `module:factory` or `/path/to/file.py:factory`
  - your strategy returns order intents instead of calling exchange plumbing directly
  - supports Spot and Futures with the same runner
  - includes 21 built-in strategies out of the box
- CLI for:
  - health checks
  - account inspection
  - ticker price lookup
  - market and limit orders
  - test orders
  - order lookup and cancel
  - market stream watch
  - user stream watch
  - custom strategy execution
  - built-in strategy catalog and one-command execution
  - futures account/position inspection
  - futures leverage and margin mode changes

## Quick Start

### 1. Create environment

```bash
cp .env.example .env
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Fill credentials

For HMAC keys:

```bash
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
BINANCE_API_KEY_TYPE=HMAC
```

For RSA or Ed25519 keys:

```bash
BINANCE_API_KEY=...
BINANCE_API_KEY_TYPE=ED25519
BINANCE_PRIVATE_KEY_PATH=/absolute/path/to/private-key.pem
BINANCE_PRIVATE_KEY_PASSPHRASE=
```

### 3. Start with testnet

```bash
BINANCE_ENV=testnet
DRY_RUN=true
```

### 4. Run health check

```bash
binance-trade doctor
```

Example output includes:

- selected environment and resolved endpoints
- server clock skew
- symbol filter summary
- account trading flags if credentials are configured

### 5. Run your own strategy module

The starter now includes a real strategy runtime. Your strategy can live in this repo or elsewhere and be loaded by file path.

Example:

```bash
binance-trade run-strategy \
  examples/strategies/spot_mean_reversion.py:create_strategy \
  --market spot \
  --params-json '{"symbol":"BTCUSDT","lookback":20,"threshold_pct":"0.002","quote_order_qty":"25"}'
```

The strategy factory should return an object with:

- `market_streams() -> list[str]`
- `on_start(ctx)`
- `on_market_event(ctx, event)`
- `on_user_event(ctx, event)`

Your strategy should return one of:

- `None`
- a single `OrderRequest`
- a list of `OrderRequest`
- `StopStrategy(...)` to end the run

See [examples/strategies/spot_mean_reversion.py](/Users/zhaoyue/Documents/Works/Playground/BinanceTrade/examples/strategies/spot_mean_reversion.py:1) for the template.

### 6. Use built-in strategies

The repo now includes 21 built-in strategy templates. See the full catalog in [strategy_catalog.md](/Users/zhaoyue/Documents/Works/Playground/BinanceTrade/docs/strategy_catalog.md:1).

List them:

```bash
binance-trade list-strategies
```

Run one on Spot:

```bash
binance-trade run-builtin-strategy sma_crossover \
  --market spot \
  --params-json '{"symbol":"BTCUSDT","interval":"1m","fast_period":20,"slow_period":50,"quote_order_qty":"25"}'
```

Run one on Futures:

```bash
binance-trade run-builtin-strategy dmi_adx_trend \
  --market futures \
  --params-json '{"symbol":"BTCUSDT","interval":"5m","quantity":"0.001","trade_side":"both"}'
```

## CLI Examples

Read market price:

```bash
binance-trade price BTCUSDT
```

Inspect account:

```bash
binance-trade account
```

Dry-run market buy:

```bash
binance-trade buy-market BTCUSDT --quote 25
```

Send Spot test order to Binance test endpoint:

```bash
binance-trade buy-market BTCUSDT --quote 25 --test-order
```

Place live market buy:

```bash
DRY_RUN=false binance-trade buy-market BTCUSDT --quote 25 --live
```

Place limit sell:

```bash
DRY_RUN=false binance-trade sell-limit BTCUSDT --quantity 0.001 --price 90000 --live
```

Watch public market stream:

```bash
binance-trade watch-market BTCUSDT --stream miniTicker
```

Watch private user stream:

```bash
binance-trade watch-user
```

Fetch order status by client order id:

```bash
binance-trade order-status BTCUSDT --client-order-id bt-btcus-...
```

Cancel by client order id:

```bash
binance-trade cancel BTCUSDT --client-order-id bt-btcus-...
```

Read futures market price:

```bash
binance-trade futures-price BTCUSDT
```

Inspect futures account and positions:

```bash
binance-trade futures-account
binance-trade futures-positions
```

Dry-run futures market buy:

```bash
binance-trade futures-buy-market BTCUSDT --quantity 0.0005
```

Send futures test order:

```bash
binance-trade futures-buy-market BTCUSDT --quantity 0.0005 --test-order
```

Place live futures limit sell:

```bash
DRY_RUN=false binance-trade futures-sell-limit BTCUSDT --quantity 0.001 --price 95000 --live
```

Change futures leverage and margin type:

```bash
binance-trade futures-set-leverage BTCUSDT --leverage 5
binance-trade futures-set-margin-type BTCUSDT --margin-type ISOLATED
```

Watch futures private stream:

```bash
binance-trade futures-watch-user
```

Run a custom futures strategy:

```bash
binance-trade run-strategy path/to/your_strategy.py:create_strategy \
  --market futures \
  --params-json '{"symbol":"BTCUSDT","quantity":"0.001"}'
```

Run a built-in futures strategy:

```bash
binance-trade run-builtin-strategy ichimoku_trend \
  --market futures \
  --params-json '{"symbol":"BTCUSDT","interval":"15m","quantity":"0.001","trade_side":"both"}'
```

## How To Fill `.env`

Use plain `KEY=value` lines.

- Booleans: `true` or `false`
- Numbers: plain numeric text like `50`, `5000`, `0.001`
- Symbol lists: comma-separated, for example `BTCUSDT,ETHUSDT`
- Paths: absolute paths are safest

### Minimal testnet example

```bash
BINANCE_ENV=testnet
BINANCE_API_KEY=your_testnet_key
BINANCE_API_SECRET=your_testnet_secret
BINANCE_API_KEY_TYPE=HMAC

DEFAULT_SYMBOL=BTCUSDT
FUTURES_DEFAULT_SYMBOL=BTCUSDT

DRY_RUN=true
LOG_LEVEL=INFO
STATE_DB_PATH=var/state.db

MAX_ORDER_NOTIONAL=50
MAX_OPEN_ORDERS_PER_SYMBOL=5
ORDER_COOLDOWN_SECONDS=5
ALLOWED_SYMBOLS=BTCUSDT,ETHUSDT

FUTURES_MAX_ORDER_NOTIONAL=50
FUTURES_MAX_OPEN_ORDERS_PER_SYMBOL=5
FUTURES_ORDER_COOLDOWN_SECONDS=5
FUTURES_ALLOWED_SYMBOLS=BTCUSDT,ETHUSDT

REQUEST_TIMEOUT_SECONDS=10
RECV_WINDOW_MS=5000
```

### Mainnet HMAC example

```bash
BINANCE_ENV=mainnet
BINANCE_API_KEY=your_live_key
BINANCE_API_SECRET=your_live_secret
BINANCE_API_KEY_TYPE=HMAC
DRY_RUN=true
```

### Mainnet RSA or Ed25519 example

```bash
BINANCE_ENV=mainnet
BINANCE_API_KEY=your_live_key
BINANCE_API_KEY_TYPE=ED25519
BINANCE_PRIVATE_KEY_PATH=/absolute/path/to/private-key.pem
BINANCE_PRIVATE_KEY_PASSPHRASE=
DRY_RUN=true
```

### What each key means

- `BINANCE_ENV`: `testnet` or `mainnet`
- `BINANCE_API_KEY`: your Binance API key
- `BINANCE_API_SECRET`: only for `HMAC`
- `BINANCE_API_KEY_TYPE`: `HMAC`, `RSA`, or `ED25519`
- `BINANCE_PRIVATE_KEY_PATH`: only for `RSA` or `ED25519`
- `DEFAULT_SYMBOL`: default Spot pair for `doctor`
- `FUTURES_DEFAULT_SYMBOL`: default Futures pair for `futures-doctor`
- `DRY_RUN`: when `true`, strategies and order commands stop before live submission
- `MAX_ORDER_NOTIONAL`: local Spot cap in quote currency
- `FUTURES_MAX_ORDER_NOTIONAL`: local Futures cap in quote currency estimation
- `ALLOWED_SYMBOLS` and `FUTURES_ALLOWED_SYMBOLS`: local allow-lists
- `STATE_DB_PATH`: SQLite file for orders and events
- `RECV_WINDOW_MS`: Binance signed request receive window

### Recommended progression

1. Start with `BINANCE_ENV=testnet` and `DRY_RUN=true`.
2. Confirm `binance-trade doctor`, `futures-doctor`, `watch-user`, and `futures-watch-user` all work.
3. Keep `DRY_RUN=true` while testing strategies with `run-builtin-strategy` or `run-strategy`.
4. Switch to `--test-order` where available.
5. Move to mainnet with `DRY_RUN=true`.
6. Only then use `--live` with very small size.

## Safety Notes

- Keep `DRY_RUN=true` until `doctor`, `account`, and `watch-user` all work as expected.
- Do not grant withdrawal permissions to the bot key.
- Prefer a sub-account and fixed IP whitelist before switching to mainnet.
- `buy-market` accepts quote notional. `sell-market` uses base quantity to avoid ambiguous quote sizing.
- Futures market orders use base quantity, not quote notional.
- Futures defaults are intentionally conservative. `FUTURES_MAX_ORDER_NOTIONAL=50` will reject larger demo orders locally before they hit Binance.
- Futures `reduceOnly` orders are allowed through local risk caps so you can still shrink exposure under stress.
- Binance still makes the final filter decision. This project pre-validates the common filters locally to reduce preventable rejects.
- This starter currently focuses on Spot plus USDⓈ-M Futures market/limit execution. More advanced futures order types from Binance docs are not wired into the CLI yet.

## Architecture

```text
binance_trade/
  config.py         runtime settings
  signing.py        HMAC/RSA/Ed25519 auth
  rest.py           Spot REST transport and endpoints
  futures_rest.py   USDⓈ-M Futures REST transport and endpoints
  filters.py        exchangeInfo parsing and validation
  risk.py           local guard rails
  state.py          SQLite order and event journal
  ws_market.py      public market streams
  ws_user.py        private user stream over WebSocket API
  futures_ws_user.py futures listenKey stream handling
  service.py        orchestration for CLI and strategies
  strategy_runtime.py strategy loader and runner
  strategy.py       example strategy implementation
  cli.py            user-facing entrypoint
```

## Docker

Build:

```bash
docker build -t binance-trade .
```

Run:

```bash
docker run --rm --env-file .env -v "$(pwd)/var:/app/var" binance-trade binance-trade doctor
```

## Next Extensions

- persistent reconciliation on startup against open orders, balances, and futures positions
- more futures order types: `STOP_MARKET`, `TAKE_PROFIT_MARKET`, trailing stop, and GTD helpers
- separate trade and user-data API keys
- metrics, alerts, and process supervision
