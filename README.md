# Binance Trade

`binance-trade` is a clean Python starter for connecting your own Spot account to Binance and trading through:

- REST for account queries and order entry
- WebSocket Streams for market data
- WebSocket API user data subscriptions for private account events
- Local risk gates and SQLite-backed order state

This project was implemented against Binance official Spot documentation reviewed on April 22, 2026. The design follows the current recommendations:

- REST signed requests require timestamp plus signature and now explicitly document percent-encoding before signing.
- Spot user data can be consumed through the WebSocket API using `userDataStream.subscribe.signature`.
- Market streams and WebSocket API connections are expected to disconnect after 24 hours and should reconnect cleanly.

Primary references:

- [Spot REST general info](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-api-information)
- [Spot request security](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/request-security)
- [Spot trading endpoints](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/trading-endpoints)
- [Spot filters](https://developers.binance.com/docs/binance-spot-api-docs/filters)
- [Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [Spot user data stream](https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream)
- [Spot WebSocket API user data requests](https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/user-data-stream-requests)
- [Spot testnet general info](https://developers.binance.com/docs/binance-spot-api-docs/testnet/general-info)

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
- Local SQLite order/event journal
- CLI for:
  - health checks
  - account inspection
  - ticker price lookup
  - market and limit orders
  - test orders
  - order lookup and cancel
  - market stream watch
  - user stream watch

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

## Safety Notes

- Keep `DRY_RUN=true` until `doctor`, `account`, and `watch-user` all work as expected.
- Do not grant withdrawal permissions to the bot key.
- Prefer a sub-account and fixed IP whitelist before switching to mainnet.
- `buy-market` accepts quote notional. `sell-market` uses base quantity to avoid ambiguous quote sizing.
- Binance still makes the final filter decision. This project pre-validates the common filters locally to reduce preventable rejects.

## Architecture

```text
binance_trade/
  config.py         runtime settings
  signing.py        HMAC/RSA/Ed25519 auth
  rest.py           Spot REST transport and endpoints
  filters.py        exchangeInfo parsing and validation
  risk.py           local guard rails
  state.py          SQLite order and event journal
  ws_market.py      public market streams
  ws_user.py        private user stream over WebSocket API
  service.py        orchestration for CLI and strategies
  strategy.py       example strategy skeleton
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

- persistent reconciliation on startup against open orders and balances
- position-aware strategies using account updates from the user stream
- separate trade and user-data API keys
- metrics and alerting
