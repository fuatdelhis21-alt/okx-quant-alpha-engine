# Phase 1 architecture and limitations

## What Phase 1 includes

1. Rust Cargo workspace and crate boundaries for modular expansion.
2. Safe defaults for runtime mode (`paper`) and `LIVE_TRADING=false`.
3. Config loading from env and optional TOML file.
4. Structured tracing setup and secret-redaction conventions.
5. Time abstractions for wall-clock, exchange timestamps, and monotonic elapsed measurements.
6. OKX request/signing foundation with endpoint selection and demo-trading header handling.
7. WebSocket engine scaffolding (bounded channels, reconnect backoff, heartbeat supervision, health state).
8. Trader app bootstrap that validates config and starts in non-live mode by default.

## What Phase 1 explicitly does NOT include

- Live order submission implementation
- Withdrawal functionality
- Full market data/orderbook processing
- Risk firewall and full execution/reconciliation logic
- Latency guarantees

## Design notes for future phases

- Domain types in `qae-core` are reusable by risk, execution, and reconciliation crates.
- `qae-config` keeps credentials env-only to avoid accidental secret persistence.
- `qae-okx-client` separates signing and request assembly from operation-specific APIs.
- `qae-websocket-engine` centralizes connection lifecycle and health scaffolding for future stream handlers.
