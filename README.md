# okx-quant-alpha-engine

Production-oriented Phase 1 foundation for an OKX quant alpha trading engine in Rust.

## Phase 1 goals

This phase delivers a safe-by-default workspace and core infrastructure only:
- Workspace layout for future staged development
- Configuration loading (env + optional TOML)
- Runtime mode safety controls
- Time and drift scaffolding
- Structured logging with secret redaction conventions
- OKX REST/WebSocket client foundations
- WebSocket engine foundations with bounded channels/reconnect/heartbeat scaffolding

No live order execution logic is implemented in Phase 1.

## Workspace structure

- `crates/core`: shared domain and safety types (`RuntimeMode`, `InstrumentId`, priorities, health)
- `crates/config`: app configuration, defaults, env/TOML merge, validation
- `crates/time`: wall clock, exchange timestamp, monotonic elapsed timing, offset state scaffolding
- `crates/monitoring`: tracing initialization and secret-safe helpers
- `crates/okx-client`: OKX V5 request/signing foundation and safe operation stubs
- `crates/websocket-engine`: bounded-channel engine scaffolding, reconnect/backoff, heartbeat and health
- `apps/trader`: startup binary wiring config + monitoring + websocket foundations

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

Defaults are documented in `crates/config/src/lib.rs`.

## Development commands

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
cargo run -p trader
```

## Phase 1 limitations

See `docs/phase1.md` for details on what is intentionally out of scope and how this sets up later phases.
