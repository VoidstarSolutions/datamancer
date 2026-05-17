# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workspace

Cargo workspace (resolver 3, edition 2024) with two crates:

- **`datamancer-core`** — pure types and trait surface (`Provider`, `LiveHandle`, `TapLog`, `HistoricalCache`, `ReplaySource`) plus the event model (`MarketEvent`, `Trade`, `Bar`, `Quote`, `Control`), `Instrument`, `Price`. No I/O, minimal deps. Provider/storage implementation crates depend only on this — never on the orchestrator.
- **`datamancer`** — the session orchestrator. Re-exports the core surface and adds `Datamancer`, `DatamancerBuilder`, `Session`. Holds provider integrations (`providers/alpaca*`) and storage backends (`storage/surreal`) behind cargo features.

Default features: `provider-alpaca`, `storage-surreal`. Both are optional; pulling in a new provider should be additive and gated behind a feature.

Workspace-wide lints: `clippy::pedantic = deny` (with `priority = -1` so individual lints can be relaxed per call site). Member crates opt in via `[lints] workspace = true`. `#![forbid(unsafe_code)]` in both crates.

## Common commands

```bash
cargo build                              # workspace build, default features
cargo test                               # all unit + integration tests (skips #[ignore])
cargo test -p datamancer-core            # core only
cargo test --test session_integration    # one integration test file
cargo test some_test_name                # by name
cargo clippy --all-targets -- -D warnings
cargo fmt
cargo run --example crypto_ticker        # requires provider-alpaca (default)
```

Integration tests live in `crates/datamancer/tests/`. `alpaca_real.rs` is `#[ignore]`d — it hits real Alpaca and needs credentials; run with `cargo test --test alpaca_real -- --ignored`.

## Architectural invariants

These are load-bearing design rules — violating them breaks downstream consumers in subtle ways. The crate README (`crates/datamancer/README.md`) is the authoritative design doc; read it before changing public API.

- **Source-agnostic output.** All provider-specific concerns stay inside `datamancer`. Once an event leaves the crate it must be indistinguishable across providers.
- **Single ordered stream.** A `Session` exposes exactly one `events()` stream merging all subscriptions. Per-instrument demux is a consumer concern.
- **Three timestamp fields, distinct roles** (on every data event):
  - `source_ts` — provider-reported market time. **Only** field engine logic should reason about. Never assigned by datamancer.
  - `seq: u64` — session-monotonic, assigned by datamancer at receipt. **The sole ordering field.** Live: arrival order. Historical fetch: source-timestamp order. Persistence uses `seq` gaps for drop detection.
  - `rx_ts` — wall-clock at byte receipt. **Observability only.** Engine decision logic must never depend on it (re-introduces wall-clock non-determinism). Collapses to `source_ts` in pure-historical replay.
- **`Control` events ride the data stream.** Connectivity changes, gaps, subscription state — all in-band, not a side channel.
- **No timestamp re-sort.** Events emit in arrival order, not re-sorted by `source_ts`. Consumers needing strict timestamp ordering buffer themselves.
- **Replay drains as fast as the consumer reads.** No wall-clock pacing.
- **`Instrument` stays opaque.** Newtype around a symbol string; structured fields (asset class, exchange, contract spec) only when a real use case demands them.
- **Trait dispatch boundary.** `Provider`/`LiveHandle` are dyn-dispatched at the cold session boundary; per-message decode loops inside each provider stay monomorphic.

## Scope reminders

Datamancer produces events; it is **not** an analysis framework, time-series store, or cross-venue reconciler. Persistence (tap log, historical fetch cache, local replay source) is in scope but deferred — keep the session API free of choices that would preclude transparently teeing live to a tap log or serving historical fetches from cache.
