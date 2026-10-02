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
- Market snapshot caching for tickers and order books with incremental merge/checksum validation
- Risk guardrails for staged order requests
- Paper/simulation execution scaffolding plus normalized live REST order submission and cancellation
- Optional authenticated private WebSocket startup login, subscription, and private event parsing flow
- Persistent in-memory portfolio risk accounting, async OMS service orchestration, and a strategy runtime that owns its own snapshot-driven startup decisions

The repository now includes a bounded startup strategy path that can turn cached snapshots into OMS submit/cancel commands, while live order acknowledgements, cancellations, private stream events, and portfolio risk state continue to flow through shared execution-report types.

## Workspace structure

- `crates/core`: shared domain and safety types (`RuntimeMode`, `InstrumentId`, priorities, health)
- `crates/config`: app configuration, defaults, env/TOML merge, validation
- `crates/market-data`: ticker/order-book domain types and snapshot helpers
- `crates/time`: wall clock, exchange timestamp, monotonic elapsed timing, offset state scaffolding
- `crates/monitoring`: tracing initialization and secret-safe helpers
- `crates/okx-client`: OKX V5 request/signing foundation, websocket login, and live order submit/cancel transport
- `crates/risk`: order guardrails plus persistent portfolio-risk/accounting state
- `crates/execution`: paper/simulation order lifecycle flow, live execution gateway, reconciliation state/history, OMS command runtime, async OMS service loop, direct command execution, and runtime state snapshots for orchestration/debugging
- `crates/strategy`: startup-safe strategy orchestration runtime that maintains strategy snapshot state and converts market updates plus execution reports into bounded OMS actions
- `crates/websocket-engine`: bounded-channel engine scaffolding, reconnect/backoff, heartbeat, subscriptions, parsing, private event ingestion, and snapshot cache
- `apps/trader`: startup binary wiring config + monitoring + websocket/risk/execution/strategy orchestration

## Safety model

- `LIVE_TRADING` defaults to `false`
- runtime `mode` defaults to `paper`
- `mode` and `LIVE_TRADING` must agree (`live` only when both are enabled)
- API credentials are loaded **only** from environment variables
- live trading still stays opt-in and credential-gated
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
  - `CONNECT_PUBLIC_STREAM_ON_STARTUP`
  - `CONNECT_PRIVATE_STREAM_ON_STARTUP`
  - `STARTUP_STREAM_UPDATES_LIMIT`
  - `STARTUP_STREAM_TIMEOUT_MS`
- Risk
  - `RISK_MAX_ORDER_QUANTITY`
  - `RISK_MAX_ORDER_NOTIONAL`
  - `RISK_MAX_PRICE_DEVIATION_BPS`
  - `RISK_MAX_POSITION_NOTIONAL`
  - `RISK_MAX_TOTAL_EXPOSURE_NOTIONAL`
  - `RISK_MAX_DAILY_LOSS`
  - `RISK_MAX_OPEN_ORDERS`
- Paper execution
  - `PAPER_STARTING_BALANCE_USDT`
  - `PAPER_FILL_SLIPPAGE_BPS`
- Startup strategy
  - `ENABLE_STARTUP_STRATEGY`
  - `STRATEGY_ORDER_QUANTITY`
  - `STRATEGY_ENTRY_SPREAD_BPS`
  - `STRATEGY_CANCEL_SPREAD_BPS`
  - `STRATEGY_MAX_SIGNALS_PER_RUN`

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

## Optional startup websocket session

The trader binary keeps external network access disabled by default. To exercise the public OKX WebSocket flow on startup, enable:

- `CONNECT_PUBLIC_STREAM_ON_STARTUP=true`
- optionally tune `STARTUP_STREAM_UPDATES_LIMIT` and `STARTUP_STREAM_TIMEOUT_MS`

For authenticated private WebSocket login plus account/orders/positions/fills startup handling, also enable:

- `CONNECT_PRIVATE_STREAM_ON_STARTUP=true`
- set `OKX_API_KEY`, `OKX_API_SECRET`, and `OKX_API_PASSPHRASE`

To exercise the bounded startup strategy loop on cached public snapshots, also enable:

- `ENABLE_STARTUP_STRATEGY=true`
- optionally tune `STRATEGY_ORDER_QUANTITY`, `STRATEGY_ENTRY_SPREAD_BPS`, `STRATEGY_CANCEL_SPREAD_BPS`, and `STRATEGY_MAX_SIGNALS_PER_RUN`
