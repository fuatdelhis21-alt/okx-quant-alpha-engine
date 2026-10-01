# Architecture and current limitations

## What the repository currently includes

1. Rust Cargo workspace and crate boundaries for modular expansion.
2. Safe defaults for runtime mode (`paper`) and `LIVE_TRADING=false`.
3. Config loading from env and optional TOML file.
4. Structured tracing setup and secret-redaction conventions.
5. Time abstractions for wall-clock, exchange timestamps, and monotonic elapsed measurements.
6. OKX request/signing foundation with endpoint selection and demo-trading header handling.
7. WebSocket engine scaffolding with bounded channels, reconnect backoff, heartbeat supervision, subscription planning, OKX frame parsing, incremental order-book merge/checksum handling, market snapshot caching, and short-lived public/private startup runtime paths.
8. Risk guardrails for staged order requests using configured quantity, notional, and price-band limits plus persistent in-memory portfolio accounting from balances, positions, and fills.
9. Execution bridging that uses paper fills in paper/simulation modes and submits normalized live REST order requests and cancellations in live mode, with an in-memory OMS command queue and async OMS service loop.
10. Trader app bootstrap that validates config and starts in non-live mode by default.
11. Startup-gated strategy orchestration runtime that maintains its own cached market snapshots and turns market updates into bounded OMS submit/cancel actions.

## What still explicitly is NOT included

- Withdrawal functionality
- Full-depth market data/orderbook reconciliation
- Persistent OMS/state reconciliation beyond in-memory submission/cancellation history, portfolio accounting, async service orchestration, and startup event collection
- Long-running strategy research, optimization, and persistent state
- Latency guarantees

## Design notes for future phases

- Domain types in `qae-core` are reusable by risk, execution, and reconciliation crates.
- `qae-config` keeps credentials env-only to avoid accidental secret persistence.
- `qae-okx-client` separates signing and request assembly from operation-specific APIs, including WebSocket login and REST order submission payloads.
- `qae-websocket-engine` centralizes connection lifecycle, startup authentication, private/public stream ingestion, parsing, and market snapshot state for future stream handlers.
- `qae-risk`, `qae-execution`, and `qae-strategy` keep simulated order flow separate while still allowing live-mode tasks to submit authenticated orders, cancel them, maintain persistent in-memory portfolio accounting, process queued OMS commands against shared market snapshots, and attach bounded strategy-runtime decisions behind explicit startup config gates.
