# Architecture and current limitations

## What the repository currently includes

1. Rust Cargo workspace and crate boundaries for modular expansion.
2. Safe defaults for runtime mode (`paper`) and `LIVE_TRADING=false`.
3. Config loading from env and optional TOML file.
4. Structured tracing setup and secret-redaction conventions.
5. Time abstractions for wall-clock, exchange timestamps, and monotonic elapsed measurements.
6. OKX request/signing foundation with endpoint selection and demo-trading header handling.
7. WebSocket engine scaffolding with bounded channels, reconnect backoff, heartbeat supervision, subscription planning, OKX frame parsing, and market snapshot caching.
8. Risk guardrails for staged order requests using configured quantity, notional, and price-band limits.
9. Paper/simulation execution scaffolding with immediate fills for market orders, resting-limit order bookkeeping, and explicit live-execution rejection.
10. Trader app bootstrap that validates config and starts in non-live mode by default.

## What still explicitly does NOT include

- Real OKX WebSocket I/O integration
- Live order submission implementation
- Withdrawal functionality
- Full-depth market data/orderbook reconciliation
- Portfolio-level risk firewall and reconciliation logic
- Latency guarantees

## Design notes for future phases

- Domain types in `qae-core` are reusable by risk, execution, and reconciliation crates.
- `qae-config` keeps credentials env-only to avoid accidental secret persistence.
- `qae-okx-client` separates signing and request assembly from operation-specific APIs.
- `qae-websocket-engine` centralizes connection lifecycle, subscription planning, parsing, and market snapshot state for future stream handlers.
- `qae-risk` and `qae-execution` keep simulated order flow separate from future authenticated live execution APIs.
