# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workspace

Cargo workspace (resolver 3, edition 2024) with seven crates:

- **`datamancer-core`** — pure types and trait surface (`Provider`, `LiveHandle`, `TapLog`, `HistoricalCache`, `ReplaySource`, `EventSink`) plus the event model (`MarketEvent`, `Trade`, `Bar`, `Quote`, `Control`), `Instrument`, `Price`, `ProviderCredentials` (tagged per-provider credential shapes), and `health.rs`'s `HealthView` (schema 2: a versioned, per-symbol-only reduction of `SystemSnapshot` for app-facing health rendering, with per-provider `ProviderState` — `Connected`/`Connecting`/`Disconnected`/`Unauthenticated`/`CompanionUnreachable`/`Disabled` — and `daemon.credential_backend`). No I/O, minimal deps. Provider/storage/transport implementation crates depend only on this — never on the orchestrator.
- **`datamancer`** — the session orchestrator. Re-exports the core surface and adds `Datamancer`, `DatamancerBuilder`, `Session`. Holds provider integrations (`providers/alpaca*`) and storage backends (`storage/turso`) behind cargo features. Per-provider settings arrive via `SettingsSource<T>` (`Static` or `Watch`, alongside the credential-source `CredentialsSource` API) — `Watch(None)` parks a compiled-in provider disabled without tearing it down, so a settings hot-apply (daemon `configure-provider`/`remove-provider`) enables/disables it without a restart.
- **`datamancer-transport-iceoryx2`** — optional same-host zero-copy iceoryx2 transport (data + diagnostics planes). Depends on `datamancer-core` only; isolates the heavy iceoryx2 dependency tree behind a hard crate boundary. `datamancer` pulls it in via the `transport-iceoryx2` feature and re-exports it as `datamancer::transport`. `SymbolId`/interning are sink-local, never core.
- **`datamancer-transport-ws`** — optional remote WebSocket client transport (one connection = one client; JSON control + event frames, instrument carried inline, no interning). Depends on `datamancer-core` only. `datamancer` pulls it in via the `transport-ws` feature and re-exports it as `datamancer::transport_ws`; `datamancerd` gates its listener/connection glue behind its own `ws` feature. Both features are **off by default**. Alongside `datamancer-transport-iceoryx2`, the two worked examples for a future unified client-transport trait.
- **`datamancer-credentials`** — credential storage for datamancer providers: one store, two consumers (`datamancerd`'s broker and in-process embedders), OS keychain/secret-service with a locked-down file fallback (backend chosen at runtime, surfaced via `HealthView.daemon.credential_backend`). Synchronous/blocking API by design (async callers wrap in `spawn_blocking`). Depends on `datamancer-core` only — never the orchestrator.
- **`datamancer-client`** — optional consumer-side crate: the control vocabulary extracted from `datamancerd` (`spec`, `codes`, `protocol::{uds,ws}`) plus, behind features `ws`/`iceoryx2`, two implementations of one generic `Client` trait. Depends on `datamancer-core` and the relevant transport crate only — never the orchestrator. `datamancer` pulls it in via the `client-ws`/`client-iceoryx2` features and re-exports it as `datamancer::client`; both features are **off by default**. `datamancerd` re-imports the same vocabulary rather than duplicating it. Feature `app` (implies `iceoryx2`, off by default) adds an app-facing `AppHandle::ensure` facade: find-or-spawn-and-connect a same-host daemon plus typed `HealthView` health, and `set_credentials`/`get_credentials`/`clear_credentials` against the daemon's credential broker, with no new protocol semantics.
- **`datamancerd`** — the standalone server: a thin **binary** (`#![forbid(unsafe_code)]`; on Windows, EXT-1 relaxes to `#![deny(unsafe_code)]` with one scoped allow in `win_control` for the named-pipe control transport) that wraps `datamancer` (with `transport-iceoryx2`) and serves multiple same-host consumer processes. It adds **no** new ordering/transport/event semantics — only composition, process lifecycle, a newline-JSON control surface over a platform-native local endpoint (UDS on Unix, owner-DACL named pipe on Windows — see `crates/datamancerd/README.md`), an optional WS client surface (feature `ws`), and graceful shutdown. Owns the credential broker (`datamancer-credentials`-backed, same-uid gated — Unix peer-cred / Windows pipe owner-DACL, hot-applies to running providers on `set-credentials`); the legacy `ALPACA_*` env-var credential fallback is **deprecated** in favor of it (still read at bootstrap when the store is empty, but warns). Owns a `ConfigHub` (config service, spec cycle 3): every compiled-in provider is a fixed set constructed at boot, but each starts disabled unless its `[provider.*]` section is present — `get-config` (ungated)/`configure-provider`/`remove-provider` (same-uid gated, persist-then-apply) enable/disable providers at runtime with no restart, since `provider.*` is the sole `Hot`-classified config surface (`config_class.rs`); `shutdown` (same-uid gated) runs the full drain. The daemon is the sole runtime writer of its own config. It also owns the health/observability surface (cycle 4): the ungated, daemon-stamped `health` op (ping-adjacent, `HealthView` schema 2) plus a periodic push plane on a dedicated `datamancer/health` iceoryx2 service (`AppHandle::watch_health()`'s infallible `HealthStream`), and `[log]`-configured structured logging (`RUST_LOG` always wins over `level`). Operator contracts (TOML config schema, control protocol, stable JSON error codes) are documented in `crates/datamancerd/README.md`.

Default features: `provider-alpaca`, `storage-turso`. `transport-iceoryx2`, `transport-ws`, `client-ws`, and `client-iceoryx2` are **off by default**. All optional; pulling in a new provider/transport should be additive and gated behind a feature.

Workspace-wide lints: `clippy::pedantic = deny` (with `priority = -1` so individual lints can be relaxed per call site). Member crates opt in via `[lints] workspace = true`. **`#![forbid(unsafe_code)]` in all seven crates**, with EXT-1 exceptions where Win32/FFI is unavoidable: the iceoryx2 transport (its gate confirms `ZeroCopySend` is a safe derive) and — on **Windows only** — `datamancerd` + `datamancer-client`, which relax to `#![deny(unsafe_code)]` with a *single* scoped `#[allow(unsafe_code)]` confined to their `win_control` / `win_pipe` module for the named-pipe control transport (owner-DACL + token/owner SID; #36); non-Windows builds stay `forbid`, every `unsafe` block carries a `// SAFETY:` proof. See those crates' docs.

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
cargo run -p datamancerd -- --config datamancerd.toml   # the standalone server
```

Integration tests live in `crates/datamancer/tests/`. `alpaca_real.rs` is `#[ignore]`d — it hits real Alpaca and needs credentials; run with `cargo test --test alpaca_real -- --ignored`. The daemon end-to-end tests (`crates/datamancerd/tests/daemon_e2e.rs`) are `#[ignore]`d — they spawn the binary and need a live iceoryx2 runtime; run with `cargo test -p datamancerd --test daemon_e2e -- --ignored`.

**Before opening a PR, run the CI gates locally** — the licenses/semver job has repeatedly failed only in CI:

```bash
git fetch origin main
cargo deny check                              # licenses, advisories, sources
.github/scripts/semver-checks.sh origin/main  # semver vs the PR base (needs cargo-semver-checks)
```

## Versioning — release-plz owns it, never bump by hand

All seven crates share `[workspace.package] version` in the root `Cargo.toml`, which keeps `datamancer-client`/`datamancerd` in lockstep for the ping version gate. **release-plz owns that field**: it bumps it in the `chore: release` PR, and merging that PR is what tags `vX.Y.Z`. Do not edit the version in a feature PR — the `release` job tags whatever version lands on `main`, so a hand-bump self-releases on merge and leaves the standing release PR computing against a surprise tag. Full flow and the pre-1.0 bump table (while major is `0`, release-plz demotes every bump: `feat:` → patch, `feat!:` → minor) live in `RELEASING.md`.

Because the version is unchanged in every PR, `semver-checks.sh` does not require a bump. It requires a **declaration**: if `cargo-semver-checks` reports a break, some commit in the PR range must carry a Conventional Commits breaking marker (`type!: subject` or a `BREAKING CHANGE:` footer) — that marker is what release-plz reads to compute the bump, so an undeclared break is the one case that would ship under a too-small version. Additive-only changes pass with a note. If the break was unintentional, make the change additive rather than adding the marker.

`cargo-semver-checks` treats any public-API addition to an exhaustive type as breaking (new enum variant, new pub field on a constructible struct) — so wire-compatible JSON additions still count as breaks. Windows CI builds only the ws-portable subset — path-shape assertions in tests must be cfg'd per-OS (Windows data dirs nest `data\`).

## Architectural invariants

These are load-bearing design rules — violating them breaks downstream consumers in subtle ways. The crate README (`crates/datamancer/README.md`) is the authoritative design doc; read it before changing public API.

- **Source-agnostic output.** All provider-specific concerns stay inside `datamancer`. Once an event leaves the crate it must be indistinguishable across providers.
- **Multiplexed stream, per-symbol determinism (not global merge).** A `ClientSession` is the primary consumer handle: it holds a mutable `(instrument, kind)` subscription set and presents **one multiplexed stream** over it. The ordering key is `(instrument, seq)` — monotonic *within* each instrument (source-stamped), arrival-order *across* instruments. It **interleaves**, it does not merge-sort, and there is no cross-symbol/global order (the never-realized global-merge model is an explicit non-goal). Per-instrument demux is a consumer concern. The single-pair `Session` is the one-subscription case (its live path is a referrer onto the same shared authoritative session that backs `ClientSession`). A second live open for a pair **shares** the authoritative session rather than conflicting.
- **Three timestamp fields, distinct roles** (on every data event):
  - `source_ts` — provider-reported market time. **Only** field engine logic should reason about. Never assigned by datamancer.
  - `seq: u64` — **per-symbol** ordering field, stamped **once at the source**
    of the authoritative per-`(instrument, kind)` session by a single-writer
    controller counter, in canonical delivery order, before any sink (Phase 1:
    `stamp → tee → emit`). Identical across all consumers of that symbol — it is
    a property of the shared stream, not of per-consumer poll timing. **The sole
    ordering field**, per-symbol only (there is no cross-instrument order; the
    multiplex key is `(instrument, seq)` — true fan-out lands in Phase 2). Live:
    arrival order. Historical fetch: source-timestamp order. Controls occupy
    `seq` slots. Holes are **real**: evicted/late-join events are numbered, so a
    resume-buffer overflow is a real `seq` hole, surfaced in-band as
    `Control::Gap` at the evicted span; the delivered stream is contiguous only
    while nothing is lost. The tap log persists this source `seq`
    verbatim (it no longer mints its own), so tap-log replay reproduces the
    delivered stream's `seq`. `Seq::SYNTHETIC = Seq(u64::MAX)` tags out-of-band
    synthetic controls and is exempt from per-symbol monotonicity.
  - `rx_ts` — wall-clock at byte receipt. **Observability only.** Engine decision logic must never depend on it (re-introduces wall-clock non-determinism). Collapses to `source_ts` in pure-historical replay.
- **`Control` events ride the data stream.** Connectivity changes, gaps, subscription state — all in-band, not a side channel.
- **No timestamp re-sort.** Events emit in arrival order, not re-sorted by `source_ts`. Consumers needing strict timestamp ordering buffer themselves.
- **Replay drains as fast as the consumer reads.** No wall-clock pacing.
- **`Instrument` stays opaque.** Newtype around a symbol string; structured fields (asset class, exchange, contract spec) only when a real use case demands them.
- **Trait dispatch boundary.** `Provider`/`LiveHandle` are dyn-dispatched at the cold session boundary; per-message decode loops inside each provider stay monomorphic.

## Scope reminders

Datamancer produces events; it is **not** an analysis framework, time-series store, or cross-venue reconciler. Persistence is wired: historical read-through cache, live tap-log
write-through, and the resume primitive (multi-shot `take_events`,
historical→live backfill seam) are implemented. The tap log persists the
source `seq` verbatim (no longer mints its own; appends are strictly
end-of-log). Remaining deferred: cache volume/eviction. Keep the session API
free of choices that would preclude local replay-source integration.

Transport: the optional `transport-iceoryx2` crate carries a client's
multiplexed stream to a same-host consumer process over iceoryx2, plus a
diagnostics plane carrying the serialized `SystemSnapshot`. The POD data payload
preserves the timestamp triple end-to-end (`rx_ts` stays observability-only,
never reconstructed by the subscriber) and carries a sink-local `SymbolId` in
place of `Instrument`. Connection-scoped controls are diagnostics-plane only;
per-symbol controls ride the data plane. Flush/shutdown ordering is load-bearing:
**tap-log flush before sink flush before service drop** — the sink never drops
samples that `flush` promised, but makes no guarantee a crashed/slow subscriber
consumed them (same-host best-effort; cross-process backpressure deferred).
