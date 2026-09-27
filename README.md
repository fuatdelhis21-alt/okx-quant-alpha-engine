# okx-quant-alpha-engine

Production-oriented Rust workspace for an OKX quant alpha trading engine.

## Current scope

The repository currently delivers a safe-by-default foundation plus the next scaffolding layer:
- Workspace layout for staged development
- Configuration loading (env + optional TOML)
- Runtime mode safety controls
- Time and drift scaffolding
- Structured logging with secret redaction conventions
- OKX REST/WebSocket client foundations
- Market data subscription planning and OKX frame parsing scaffolding
- Market snapshot caching for tickers and order books
- Risk guardrails for staged order requests
- Paper/simulation execution scaffolding with safe live-trading rejection

No live order execution logic is implemented.

## Workspace structure

- `crates/core`: shared domain and safety types (`RuntimeMode`, `InstrumentId`, priorities, health)
- `crates/config`: app configuration, defaults, env/TOML merge, validation
- `crates/market-data`: ticker/order-book domain types and snapshot helpers
- `crates/time`: wall clock, exchange timestamp, monotonic elapsed timing, offset state scaffolding
- `crates/monitoring`: tracing initialization and secret-safe helpers
- `crates/okx-client`: OKX V5 request/signing foundation and safe operation stubs
- `crates/risk`: order guardrails based on market snapshots and configured limits
- `crates/execution`: paper/simulation order submission flow and resting-order bookkeeping
- `crates/websocket-engine`: bounded-channel engine scaffolding, reconnect/backoff, heartbeat, subscriptions, parsing, and snapshot cache
- `apps/trader`: startup binary wiring config + monitoring + websocket/risk/execution scaffolding

## Safety model

- `LIVE_TRADING` defaults to `false`
- runtime `mode` defaults to `paper`
- `mode` and `LIVE_TRADING` must agree (`live` only when both are enabled)
- API credentials are loaded **only** from environment variables
- live order submission is explicitly stubbed as unsupported
- withdrawals are explicitly unsupported
- demo trading header support is configurable (`OKX_DEMO_TRADING`, default `true`)

## Configuration

Optional config file: set `OKX_CONFIG_FILE=/absolute/path/to/config.toml`.

Environment variables:

- Safety/runtime
  - `LIVE_TRADING` (`false` by default)
  - `MODE` (`paper` default, supports `paper|simulation|live`)
- Endpoints
  - `OKX_REST_ENDPOINT`
  - `OKX_PUBLIC_WS_ENDPOINT`
  - `OKX_PRIVATE_WS_ENDPOINT`
  - `OKX_BUSINESS_WS_ENDPOINT`
- API credentials (env-only)
  - `OKX_API_KEY`
  - `OKX_API_SECRET`
  - `OKX_API_PASSPHRASE`
- Network/reliability
  - `REQUEST_TIMEOUT_MS`
  - `RECONNECT_MAX_ATTEMPTS`
  - `RATE_LIMIT_PER_SECOND`
  - `OKX_DEMO_TRADING`
- Market data
  - `MARKET_DATA_INSTRUMENTS`
  - `SUBSCRIBE_TICKERS`
  - `SUBSCRIBE_ORDER_BOOK`
  - `ORDER_BOOK_DEPTH`
- Risk
  - `RISK_MAX_ORDER_QUANTITY`
  - `RISK_MAX_ORDER_NOTIONAL`
  - `RISK_MAX_PRICE_DEVIATION_BPS`
- Paper execution
  - `PAPER_STARTING_BALANCE_USDT`
  - `PAPER_FILL_SLIPPAGE_BPS`

Defaults are documented in `crates/config/src/lib.rs`.

## Development commands

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
cargo run -p trader
```

## Current limitations

See `docs/phase1.md` for details on what remains intentionally out of scope and how the current scaffolding sets up later phases.
