# App Facade (Cycle 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `datamancer_client::app::AppHandle` — find-or-spawn the daemon, connect, and expose a typed `HealthView`, per cycle 1 of `docs/superpowers/specs/2026-07-05-app-facing-daemon-design.md`.

**Architecture:** Pure `HealthView` types + `SystemSnapshot` reduction land in `datamancer-core` (shared by the in-process embedder and the wire client — spec decision 9). A new `ping` control op gives the daemon a version handshake. A new `app` module in `datamancer-client` (feature `app`, implying `iceoryx2`) implements discover/spawn/readiness as a state machine over two internal traits (`ControlEndpoint`, `DaemonSpawner`) so the logic unit-tests against fakes and Windows stays additive.

**Tech Stack:** Rust edition 2024, tokio, serde/serde_json, thiserror, `directories` (already deps). No new dependencies.

## Global Constraints

- `clippy::pedantic = deny` workspace-wide; every crate is `#![forbid(unsafe_code)]`. Run `cargo clippy --all-targets -- -D warnings` before every commit.
- `cargo fmt` before every commit.
- `datamancer-core` is pure types + traits — no I/O, no tokio.
- `datamancer-client` depends on `datamancer-core` + transport crates only — never the `datamancer` orchestrator.
- Stable JSON error codes and wire shapes are an operator contract: additions need regression tests; never change existing strings/shapes.
- Per-symbol determinism: never present or imply cross-instrument ordering. `rx_ts`/latency are observability-only and must be documented as such wherever surfaced.
- All new public aggregate types are `#[non_exhaustive]` with constructor fns (forward-compat convention, see `snapshot.rs`).
- Commit messages: `type(scope): summary` per repo history (`feat(datamancerd): …`).

---

### Task 1: `HealthView` types + reduction in `datamancer-core`

**Files:**
- Create: `crates/datamancer-core/src/health.rs`
- Modify: `crates/datamancer-core/src/lib.rs` (module + re-exports)

**Interfaces:**
- Consumes: `SystemSnapshot`, `ProviderSnapshot`, `ConnectionState`, `AuthoritativeSessionSnapshot`, `Instrument`, `EventKind`, `ProviderId`, `Timestamp` (all existing, `crates/datamancer-core/src/snapshot.rs`).
- Produces (later tasks rely on exactly these):
  - `HealthView { schema_version: u32, daemon: DaemonHealth, providers: Vec<ProviderHealth>, streams: Vec<StreamHealth> }`
  - `HealthView::SCHEMA_VERSION: u32` (= 1), `HealthView::DEFAULT_STALE_AFTER_NS: i64` (= 5_000_000_000)
  - `HealthView::from_snapshot(snapshot: &SystemSnapshot, stale_after_ns: i64) -> HealthView`
  - `DaemonHealth { version: Option<String>, captured_at: Timestamp }` (fields pub — the facade assigns `version` after the ping handshake)
  - `ProviderHealth { provider: ProviderId, state: ProviderState, detail: Option<String> }`
  - `ProviderState::{Connected, Connecting, Disconnected, Unauthenticated, CompanionUnreachable}` (`#[non_exhaustive]`)
  - `StreamHealth { instrument: Instrument, kind: EventKind, liveness: Liveness, last_event_source_ts: Option<Timestamp>, gap_count: u64, latency: Option<LatencySummary> }`
  - `Liveness::{Idle, Live, Stale { since: Timestamp }}` (`#[non_exhaustive]`; `Gapped`/`Backfilling` arrive in cycle 4 — additive because non_exhaustive)
  - `LatencySummary { last_ns: i64 }`

Design notes locked in by the spec: per-symbol only (one `StreamHealth` per `(instrument, kind)`, no aggregates); `Unauthenticated`/`CompanionUnreachable` reserved now for IBKR even though nothing produces them yet; staleness is computed against `snapshot.captured_at` using `last_rx_ts` (both wall-clock — the sanctioned observability use; document it).

- [ ] **Step 1: Write the failing tests**

Create `crates/datamancer-core/src/health.rs` containing ONLY the test module for now (types come in step 3). Reference the fixture style of `snapshot.rs`'s tests:

```rust
//! App-facing health reduction of [`SystemSnapshot`] (spec 2026-07-05, cycle 1).
//!
//! Pure types + a pure reduction — assembly stays in `datamancer`, transport
//! in `datamancer-client`. Per-symbol only: no cross-instrument aggregate is
//! ever computed. `Liveness`/`latency` derive from wall-clock fields
//! (`captured_at`, `last_rx_ts`, `latency_ns`) — observability only, never
//! engine logic.

#[cfg(test)]
mod tests {
    use super::{HealthView, Liveness, ProviderState};
    use crate::{
        AssetClass, AuthoritativeSessionSnapshot, CacheSnapshot, ConnectionState, EventKind,
        Instrument, ProviderId, ProviderSnapshot, SystemSnapshot, Timestamp,
    };

    fn provider_snapshot(state: ConnectionState, last_error: Option<&str>) -> ProviderSnapshot {
        ProviderSnapshot::new(
            ProviderId::from_static("alpaca-crypto"),
            state,
            0, 0, 1, 1, 0, 0, 10, 0,
            last_error.map(str::to_string),
        )
    }

    fn stream_snapshot(last_rx_ns: Option<i64>) -> AuthoritativeSessionSnapshot {
        let inst = Instrument::new(
            ProviderId::from_static("alpaca-crypto"),
            AssetClass::Crypto,
            "BTC/USD",
        );
        AuthoritativeSessionSnapshot::new(inst, EventKind::Trade, 1, 2)
            .with_timestamps(last_rx_ns.map(|n| Timestamp(n - 7)), last_rx_ns.map(Timestamp))
    }

    fn snapshot(
        providers: Vec<ProviderSnapshot>,
        sessions: Vec<AuthoritativeSessionSnapshot>,
        captured_at_ns: i64,
    ) -> SystemSnapshot {
        SystemSnapshot::new(
            Timestamp(captured_at_ns),
            providers,
            CacheSnapshot::new(Vec::new(), None),
            sessions,
            Vec::new(),
        )
    }

    #[test]
    fn provider_states_map_from_connection_state() {
        let snap = snapshot(
            vec![
                provider_snapshot(ConnectionState::Connected, None),
                provider_snapshot(ConnectionState::Disconnected, Some("ws closed")),
                provider_snapshot(ConnectionState::Unknown, None),
            ],
            vec![],
            1_000,
        );
        let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
        assert_eq!(view.schema_version, HealthView::SCHEMA_VERSION);
        assert_eq!(view.daemon.version, None); // filled by the caller, not the reduction
        assert_eq!(view.daemon.captured_at, Timestamp(1_000));
        let states: Vec<_> = view.providers.iter().map(|p| p.state).collect();
        assert_eq!(
            states,
            vec![
                ProviderState::Connected,
                ProviderState::Disconnected,
                ProviderState::Connecting,
            ]
        );
        assert_eq!(view.providers[1].detail.as_deref(), Some("ws closed"));
    }

    #[test]
    fn liveness_is_idle_live_or_stale_per_symbol() {
        let now = 100_000_000_000_i64; // 100s in ns
        let snap = snapshot(
            vec![provider_snapshot(ConnectionState::Connected, None)],
            vec![
                stream_snapshot(None),                            // no data yet -> Idle
                stream_snapshot(Some(now - 1_000_000_000)),       // 1s ago -> Live
                stream_snapshot(Some(now - 30_000_000_000)),      // 30s ago -> Stale
            ],
            now,
        );
        let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
        assert_eq!(view.streams.len(), 3); // one entry per (instrument, kind); never aggregated
        assert_eq!(view.streams[0].liveness, Liveness::Idle);
        assert!(view.streams[0].latency.is_none());
        assert_eq!(view.streams[1].liveness, Liveness::Live);
        assert_eq!(view.streams[1].latency.map(|l| l.last_ns), Some(7));
        assert_eq!(
            view.streams[2].liveness,
            Liveness::Stale { since: Timestamp(now - 30_000_000_000) }
        );
        assert_eq!(view.streams[2].gap_count, 2);
    }

    #[test]
    fn ibkr_reserved_states_serde_round_trip() {
        // Nothing produces these in cycle 1; they are reserved for IBKR
        // (spec appendix). Guard the wire names now so shipped apps parse them
        // when cycle 4 starts emitting them.
        for (state, wire) in [
            (ProviderState::Unauthenticated, "\"unauthenticated\""),
            (ProviderState::CompanionUnreachable, "\"companion_unreachable\""),
        ] {
            let json = serde_json::to_string(&state).unwrap();
            assert_eq!(json, wire);
            let back: ProviderState = serde_json::from_str(&json).unwrap();
            assert_eq!(back, state);
        }
    }

    #[test]
    fn health_view_serde_round_trips() {
        let snap = snapshot(
            vec![provider_snapshot(ConnectionState::Connected, None)],
            vec![stream_snapshot(Some(50))],
            1_000,
        );
        let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
        let json = serde_json::to_string(&view).unwrap();
        let back: HealthView = serde_json::from_str(&json).unwrap();
        assert_eq!(view, back);
    }
}
```

Add to `crates/datamancer-core/src/lib.rs` alongside the existing module declarations: `pub mod health;` — and extend the existing `pub use` re-export list with `health::{DaemonHealth, HealthView, LatencySummary, Liveness, ProviderHealth, ProviderState, StreamHealth}` (match the file's existing re-export style).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p datamancer-core health 2>&1 | tail -20`
Expected: COMPILE ERROR — `HealthView` etc. not defined.

- [ ] **Step 3: Write the implementation**

Above the test module in `health.rs`:

```rust
use serde::{Deserialize, Serialize};

use crate::{
    event::{EventKind, Timestamp},
    instrument::{Instrument, ProviderId},
    snapshot::{ConnectionState, SystemSnapshot},
};

/// A typed, versioned, app-renderable reduction of [`SystemSnapshot`]:
/// "is my market data healthy?" for an end-user application. One entry per
/// `(instrument, kind)` in [`streams`](Self::streams) — no cross-symbol
/// aggregate exists or is implied.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthView {
    /// Shape version of this view; bump on breaking changes to the reduction.
    pub schema_version: u32,
    pub daemon: DaemonHealth,
    pub providers: Vec<ProviderHealth>,
    pub streams: Vec<StreamHealth>,
}

/// Daemon-process-level health.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonHealth {
    /// The serving process's version. `None` out of the pure reduction — the
    /// caller (facade after the `ping` handshake, or the embedding library)
    /// assigns it, because the snapshot does not carry it.
    pub version: Option<String>,
    /// Wall-clock at snapshot assembly (observability).
    pub captured_at: Timestamp,
}

/// One provider's connection health.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderHealth {
    pub provider: ProviderId,
    pub state: ProviderState,
    /// Human-renderable detail (e.g. the provider's last error). Free text,
    /// non-contractual.
    pub detail: Option<String>,
}

/// Provider connection state, app-facing.
///
/// `Unauthenticated` and `CompanionUnreachable` are **reserved** (spec
/// appendix: IBKR attaches to a local TWS/IB Gateway that can be down or
/// needing re-auth); nothing produces them in cycle 1, but they exist now so
/// shipped consumers already parse them.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderState {
    Connected,
    /// Not yet observed connected (initial / connecting / reconnecting).
    Connecting,
    Disconnected,
    /// Credentials rejected or an auth session lapsed (reserved).
    Unauthenticated,
    /// A required companion process (e.g. IB Gateway) is unreachable (reserved).
    CompanionUnreachable,
}

/// Per-`(instrument, kind)` stream health. Per-symbol only.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamHealth {
    pub instrument: Instrument,
    pub kind: EventKind,
    pub liveness: Liveness,
    /// Provider-reported market time of the last event.
    pub last_event_source_ts: Option<Timestamp>,
    /// Cumulative per-symbol `Control::Gap` count.
    pub gap_count: u64,
    /// `rx_ts`-derived; observability only, never engine logic.
    pub latency: Option<LatencySummary>,
}

/// Stream liveness, judged on wall-clock receipt (`rx_ts` vs the snapshot's
/// `captured_at`) — observability only.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Liveness {
    /// Subscribed, no data event observed yet.
    Idle,
    Live,
    /// No event within the staleness threshold; `since` is the last receipt.
    Stale { since: Timestamp },
}

/// Latency summary for one stream (cycle 1: last observation only).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencySummary {
    /// Last `rx_ts - source_ts`. Signed: straddles two clocks and may be
    /// negative under clock skew (see `AuthoritativeSessionSnapshot::latency_ns`).
    pub last_ns: i64,
}

impl HealthView {
    /// Shape version of the current reduction.
    pub const SCHEMA_VERSION: u32 = 1;
    /// Default staleness threshold: 5 seconds without a received event.
    pub const DEFAULT_STALE_AFTER_NS: i64 = 5_000_000_000;

    /// Reduce a [`SystemSnapshot`] to the app-facing view. Pure: no clock is
    /// read — staleness compares `last_rx_ts` to the snapshot's own
    /// `captured_at` against `stale_after_ns`.
    #[must_use]
    pub fn from_snapshot(snapshot: &SystemSnapshot, stale_after_ns: i64) -> Self {
        let providers = snapshot
            .providers
            .iter()
            .map(|p| ProviderHealth {
                provider: p.provider.clone(),
                state: match p.connection_state {
                    ConnectionState::Connected => ProviderState::Connected,
                    ConnectionState::Disconnected => ProviderState::Disconnected,
                    ConnectionState::Unknown => ProviderState::Connecting,
                },
                detail: p.last_error.clone(),
            })
            .collect();
        let streams = snapshot
            .authoritative_sessions
            .iter()
            .map(|s| StreamHealth {
                instrument: s.instrument.clone(),
                kind: s.kind,
                liveness: match s.last_rx_ts {
                    None => Liveness::Idle,
                    Some(rx) if snapshot.captured_at.0 - rx.0 > stale_after_ns => {
                        Liveness::Stale { since: rx }
                    }
                    Some(_) => Liveness::Live,
                },
                last_event_source_ts: s.last_source_ts,
                gap_count: s.gap_count,
                latency: s.latency_ns.map(|last_ns| LatencySummary { last_ns }),
            })
            .collect();
        Self {
            schema_version: Self::SCHEMA_VERSION,
            daemon: DaemonHealth {
                version: None,
                captured_at: snapshot.captured_at,
            },
            providers,
            streams,
        }
    }
}
```

Note: if `ConnectionState`/`ProviderHealth` field-struct literals trip pedantic lints or the `EventKind`/`Timestamp` imports differ from the paths shown, follow the existing `snapshot.rs` import style — it is the template for this file.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p datamancer-core health`
Expected: 4 passed.
Then: `cargo clippy --all-targets -- -D warnings && cargo fmt`

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-core/src/health.rs crates/datamancer-core/src/lib.rs
git commit -m "feat(core): HealthView — typed app-facing reduction of SystemSnapshot"
```

---

### Task 2: In-process `Datamancer::health()` accessor

**Files:**
- Modify: `crates/datamancer/src/session.rs` (next to `snapshot_live`, ~line 535)
- Modify: `crates/datamancer/src/lib.rs` (extend the core re-export list, ~line 46)

**Interfaces:**
- Consumes: `HealthView::from_snapshot`, `Datamancer::snapshot_live()` (both exist after Task 1).
- Produces: `Datamancer::health(&self) -> HealthView` — embedder parity for the facade's `health()` (spec decision 9). Sets `daemon.version = Some(env!("CARGO_PKG_VERSION"))` (the library's own version — in-process there is no separate daemon).

- [ ] **Step 1: Write the failing test**

In `crates/datamancer/src/session.rs`'s existing `#[cfg(test)]` module (follow its established builder/setup helpers for constructing a `Datamancer` — reuse whatever minimal constructor its current snapshot tests use; if no snapshot test exists there, put the test in `crates/datamancer/tests/session_integration.rs` following that file's setup):

```rust
#[tokio::test]
async fn health_reduces_live_snapshot_with_library_version() {
    let dm = /* the same minimal Datamancer the existing snapshot tests build */;
    let health = dm.health();
    assert_eq!(health.schema_version, datamancer_core::HealthView::SCHEMA_VERSION);
    assert_eq!(health.daemon.version.as_deref(), Some(env!("CARGO_PKG_VERSION")));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p datamancer health_reduces 2>&1 | tail -5`
Expected: COMPILE ERROR — no method `health`.

- [ ] **Step 3: Implement**

In `session.rs`, directly after `snapshot_live` (~line 537):

```rust
/// The app-facing [`HealthView`] reduction of [`Self::snapshot_live`] —
/// in-process parity with the daemon facade's `health()`. Uses the default
/// staleness threshold; embedders needing another threshold can call
/// [`HealthView::from_snapshot`] on a snapshot themselves.
#[must_use]
pub fn health(&self) -> HealthView {
    let mut view =
        HealthView::from_snapshot(&self.snapshot_live(), HealthView::DEFAULT_STALE_AFTER_NS);
    view.daemon.version = Some(env!("CARGO_PKG_VERSION").to_string());
    view
}
```

Add `HealthView` to `session.rs`'s `datamancer_core` import list, and add `DaemonHealth, HealthView, LatencySummary, Liveness, ProviderHealth, ProviderState, StreamHealth` to the re-export list in `crates/datamancer/src/lib.rs` (~line 46).

- [ ] **Step 4: Run tests**

Run: `cargo test -p datamancer health_reduces && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer/src/session.rs crates/datamancer/src/lib.rs
git commit -m "feat(datamancer): in-process health() accessor mirroring the daemon facade"
```

---

### Task 3: `ping` control op with daemon version

**Files:**
- Modify: `crates/datamancer-client/src/protocol/uds.rs` (`Request::Ping`, `Reply.version`, `Reply::pong`)
- Modify: `crates/datamancerd/src/server.rs` (dispatch arm, ~line 511)
- Modify: `crates/datamancerd/README.md` (control-protocol section, after the `instruments` examples ~line 303)

**Interfaces:**
- Produces: wire op `{"op":"ping"}` → `{"ok":true,"version":"0.1.0"}`; `Request::Ping`; `Reply { version: Option<String>, .. }`; `Reply::pong(version: impl Into<String>) -> Reply`. Task 5's readiness probe and Task 6's version-skew guard depend on exactly this reply shape.
- Requires no client registration (usable pre-`open-client`). While draining it returns the existing `shutting_down` error — correct: a draining daemon is not "ready".

- [ ] **Step 1: Write the failing tests**

In `crates/datamancer-client/src/protocol/uds.rs`'s existing test module:

```rust
#[test]
fn ping_round_trips_and_reply_carries_version() {
    let req: Request = serde_json::from_str(r#"{"op":"ping"}"#).expect("de");
    assert!(matches!(req, Request::Ping));
    assert_eq!(serde_json::to_string(&Request::Ping).unwrap(), r#"{"op":"ping"}"#);

    let reply = serde_json::to_value(Reply::pong("0.1.0")).expect("ser");
    assert_eq!(reply["ok"], serde_json::Value::Bool(true));
    assert_eq!(reply["version"], "0.1.0");
    assert!(reply.get("code").is_none());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-client ping_round_trips 2>&1 | tail -5`
Expected: COMPILE ERROR — no variant `Ping`.

- [ ] **Step 3: Implement the vocabulary**

In `uds.rs`, add to `Request` (after `Instruments`):

```rust
    /// Liveness/version probe. Answerable before `open-client`; used by the
    /// app facade for spawn-readiness and version-skew detection.
    Ping,
```

Add to `Reply` (after `instruments`):

```rust
    /// The daemon's crate version (on `ping`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
```

Update every existing `Reply` constructor literal in this file to include `version: None` (the `..Self::ok()` spreads pick it up automatically — only `Reply::ok()` and `Reply::error` spell out all fields). Add the constructor:

```rust
    /// Success carrying the daemon version (on `ping`).
    #[must_use]
    pub fn pong(version: impl Into<String>) -> Self {
        Self {
            version: Some(version.into()),
            ..Self::ok()
        }
    }
```

- [ ] **Step 4: Wire the daemon dispatch**

In `crates/datamancerd/src/server.rs`, add an arm to `dispatch` (after `Request::ListClients`, ~line 534):

```rust
            Request::Ping => Reply::pong(env!("CARGO_PKG_VERSION")),
```

- [ ] **Step 5: Document**

In `crates/datamancerd/README.md`'s control-protocol example block (after the `instruments` lines), add:

```jsonc
{"op":"ping"}          -> {"ok":true,"version":"0.1.0"}
```

with one sentence after the block: `ping` needs no registered client and reports the daemon's crate version; the app facade uses it for spawn-readiness and version-skew detection.

- [ ] **Step 6: Run tests**

Run: `cargo test -p datamancer-client && cargo test -p datamancerd && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: all pass (daemon e2e stays `#[ignore]`d).

- [ ] **Step 7: Commit**

```bash
git add crates/datamancer-client/src/protocol/uds.rs crates/datamancerd/src/server.rs crates/datamancerd/README.md
git commit -m "feat(control): ping op reporting the daemon version"
```

---

### Task 4: `app` module — config, errors, lifecycle traits, ensure state machine

**Files:**
- Create: `crates/datamancer-client/src/app/mod.rs`
- Create: `crates/datamancer-client/src/app/error.rs`
- Create: `crates/datamancer-client/src/app/lifecycle.rs`
- Modify: `crates/datamancer-client/src/lib.rs` (`#[cfg(feature = "app")] pub mod app;`)
- Modify: `crates/datamancer-client/Cargo.toml` (feature `app`)

**Interfaces:**
- Consumes: `crate::ClientError`, `crate::iceoryx2::Iceoryx2ClientError` (existing).
- Produces (Tasks 5–7 rely on exactly these):
  - `EnsureConfig { daemon_binary: PathBuf, config_path: Option<PathBuf>, control_socket: Option<PathBuf>, client_name: String, ready_timeout: Duration, log_path: Option<PathBuf>, poll_interval: Duration, event_buffer: usize }` with `EnsureConfig::new(daemon_binary: impl Into<PathBuf>, client_name: impl Into<String>) -> Self` (defaults: `ready_timeout` 10 s, `poll_interval` 1 ms, `event_buffer` 8192, the `Option`s `None`)
  - `EnsureError::{NoSocketPath, SpawnFailed { binary, source }, ReadyTimeout { timeout, diagnosis }, VersionSkew { daemon, client }, Connect(ClientError<Iceoryx2ClientError>)}` and `ReadyDiagnosis::{DaemonExited { status: Option<i32>, stderr_tail: String }, Unresponsive}`
  - `pub(crate) trait ControlEndpoint { async fn ping(&self, socket: &Path, timeout: Duration) -> Result<String, PingFailure>; }` where `PingFailure` is an opaque `pub(crate) struct PingFailure(pub String)` (reason for diagnostics; any failure = "not ready")
  - `pub(crate) trait SpawnedDaemon: Send { fn poll_exit(&mut self) -> Option<ExitInfo>; }`, `pub(crate) struct ExitInfo { pub status: Option<i32>, pub stderr_tail: String }`
  - `pub(crate) trait DaemonSpawner { type Proc: SpawnedDaemon; fn spawn(&self, binary: &Path, config: Option<&Path>) -> std::io::Result<Self::Proc>; }`
  - `pub(crate) async fn ensure_daemon<E: ControlEndpoint, S: DaemonSpawner>(endpoint: &E, spawner: &S, cfg: &EnsureConfig, socket: &Path) -> Result<String, EnsureError>` — returns the daemon's version string
  - `pub(crate) fn version_compatible(client: &str, daemon: &str) -> bool` — equal major, and equal minor while major is 0; unparseable ⇒ incompatible

- [ ] **Step 1: Feature + module wiring**

`Cargo.toml` features section: `app = ["iceoryx2"]`. `lib.rs`: add `#[cfg(feature = "app")] pub mod app;` after the `iceoryx2` module line. `app/mod.rs` starts as:

```rust
//! App-facing facade for datamancerd (spec 2026-07-05, cycle 1): find a
//! running daemon or spawn one, connect, and expose typed health.
//!
//! Adds **no** protocol semantics — every capability maps to control-surface
//! ops a hand-rolled client could issue. Spawn is detached and unsupervised:
//! the daemon is a shared host service that outlives the app that spawned it;
//! if it dies, the event stream ends and the app calls
//! [`AppHandle::ensure`] again (reconnect-by-recreate).

mod error;
mod lifecycle;

pub use error::{EnsureError, ReadyDiagnosis};

use std::path::PathBuf;
use std::time::Duration;

/// Parameters for [`AppHandle::ensure`] (`AppHandle` lands with the facade).
#[derive(Debug, Clone)]
pub struct EnsureConfig {
    /// The datamancerd binary to spawn if none is running. Explicit — no
    /// `PATH` search (a bundling app knows its sidecar's location; guessing
    /// invites version skew and PATH hijack).
    pub daemon_binary: PathBuf,
    /// Daemon config file. `None` = the daemon's platform default (which
    /// self-scaffolds on first run).
    pub config_path: Option<PathBuf>,
    /// Control socket. `None` = `crate::default_control_socket()`.
    pub control_socket: Option<PathBuf>,
    /// This client's name for `open-client` (unique per daemon).
    pub client_name: String,
    /// Bound on spawn-to-ready. Default 10 s.
    pub ready_timeout: Duration,
    /// Spawned daemon's stdout/stderr destination. `None` = the platform
    /// default (`crate::paths::default_daemon_log()`, Task 5).
    pub log_path: Option<PathBuf>,
    /// Forwarded to the iceoryx2 client (idle poll sleep).
    pub poll_interval: Duration,
    /// Forwarded to the iceoryx2 client (local event buffer bound).
    pub event_buffer: usize,
}

impl EnsureConfig {
    /// Defaults: 10 s ready timeout, 1 ms poll, 8192-event buffer, platform
    /// socket/config/log paths.
    #[must_use]
    pub fn new(daemon_binary: impl Into<PathBuf>, client_name: impl Into<String>) -> Self {
        Self {
            daemon_binary: daemon_binary.into(),
            config_path: None,
            control_socket: None,
            client_name: client_name.into(),
            ready_timeout: Duration::from_secs(10),
            log_path: None,
            poll_interval: Duration::from_millis(1),
            event_buffer: 8192,
        }
    }
}
```

`app/error.rs`:

```rust
use std::path::PathBuf;
use std::time::Duration;

use crate::ClientError;
use crate::iceoryx2::Iceoryx2ClientError;

/// Why a spawned daemon never became ready (inside
/// [`EnsureError::ReadyTimeout`]).
#[derive(Debug)]
pub enum ReadyDiagnosis {
    /// The spawned process exited before the socket answered — and a
    /// subsequent connect never succeeded either (a lost spawn race whose
    /// winner answers is success, not this).
    DaemonExited {
        status: Option<i32>,
        /// Tail of the daemon log (best effort; empty if unreadable).
        stderr_tail: String,
    },
    /// The process appears alive but the socket never answered a ping.
    Unresponsive,
}

/// Failure to find-or-spawn-and-connect a daemon.
#[derive(Debug, thiserror::Error)]
pub enum EnsureError {
    #[error(
        "no control-socket path: no platform default derivable (no home/runtime dir); \
         set EnsureConfig::control_socket explicitly"
    )]
    NoSocketPath,
    #[error("failed to spawn datamancerd at {binary}: {source}")]
    SpawnFailed {
        binary: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("daemon not ready within {timeout:?}: {diagnosis:?}")]
    ReadyTimeout {
        timeout: Duration,
        diagnosis: ReadyDiagnosis,
    },
    #[error("version skew: daemon {daemon} incompatible with client {client}")]
    VersionSkew { daemon: String, client: String },
    #[error(transparent)]
    Connect(#[from] ClientError<Iceoryx2ClientError>),
}
```

`app/lifecycle.rs` — traits, state machine, version check (implementation in step 3; tests first):

- [ ] **Step 2: Write the failing tests**

Test module at the bottom of `app/lifecycle.rs`. Fakes are scripted: the endpoint yields a programmed sequence of ping outcomes; the spawner records calls and hands back a scripted process.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{EnsureConfig, EnsureError, ReadyDiagnosis};
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Ping outcomes served in order; the last entry repeats forever.
    struct ScriptedEndpoint {
        script: Vec<Result<String, PingFailure>>,
        calls: AtomicUsize,
    }
    impl ScriptedEndpoint {
        fn new(script: Vec<Result<String, PingFailure>>) -> Self {
            Self { script, calls: AtomicUsize::new(0) }
        }
    }
    impl ControlEndpoint for ScriptedEndpoint {
        async fn ping(&self, _: &Path, _: Duration) -> Result<String, PingFailure> {
            let i = self.calls.fetch_add(1, Ordering::SeqCst);
            self.script[i.min(self.script.len() - 1)].clone()
        }
    }

    struct ScriptedProc {
        /// `poll_exit` returns `None` this many times, then `Some(exit)`.
        alive_polls: usize,
        exit: Option<ExitInfo>,
    }
    impl SpawnedDaemon for ScriptedProc {
        fn poll_exit(&mut self) -> Option<ExitInfo> {
            if self.alive_polls > 0 {
                self.alive_polls -= 1;
                return None;
            }
            self.exit.clone()
        }
    }

    struct ScriptedSpawner {
        result: Mutex<Option<std::io::Result<ScriptedProc>>>,
        spawned: AtomicUsize,
    }
    impl ScriptedSpawner {
        fn ok(proc_: ScriptedProc) -> Self {
            Self { result: Mutex::new(Some(Ok(proc_))), spawned: AtomicUsize::new(0) }
        }
        fn fails() -> Self {
            Self {
                result: Mutex::new(Some(Err(std::io::Error::from(
                    std::io::ErrorKind::NotFound,
                )))),
                spawned: AtomicUsize::new(0),
            }
        }
        /// A spawner the test expects never to be called.
        fn unreachable() -> Self {
            Self { result: Mutex::new(None), spawned: AtomicUsize::new(0) }
        }
    }
    impl DaemonSpawner for ScriptedSpawner {
        type Proc = ScriptedProc;
        fn spawn(&self, _: &Path, _: Option<&Path>) -> std::io::Result<ScriptedProc> {
            self.spawned.fetch_add(1, Ordering::SeqCst);
            self.result.lock().unwrap().take().expect("unexpected spawn")
        }
    }

    fn fail() -> Result<String, PingFailure> {
        Err(PingFailure("connection refused".to_string()))
    }
    fn cfg() -> EnsureConfig {
        let mut c = EnsureConfig::new("/bundle/datamancerd", "test-app");
        c.ready_timeout = Duration::from_millis(300);
        c
    }

    #[tokio::test]
    async fn already_running_daemon_is_used_without_spawning() {
        let ep = ScriptedEndpoint::new(vec![Ok("0.1.0".to_string())]);
        let sp = ScriptedSpawner::unreachable();
        let v = ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock")).await.unwrap();
        assert_eq!(v, "0.1.0");
        assert_eq!(sp.spawned.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn spawns_then_waits_for_readiness() {
        let ep = ScriptedEndpoint::new(vec![fail(), fail(), Ok("0.1.0".to_string())]);
        let sp = ScriptedSpawner::ok(ScriptedProc { alive_polls: usize::MAX, exit: None });
        let v = ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock")).await.unwrap();
        assert_eq!(v, "0.1.0");
        assert_eq!(sp.spawned.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn lost_spawn_race_still_succeeds_when_winner_answers() {
        // Our spawn exits immediately (single-instance lock held by the
        // winner), but a later ping answers: SUCCESS per the spec.
        let ep = ScriptedEndpoint::new(vec![fail(), fail(), Ok("0.1.0".to_string())]);
        let sp = ScriptedSpawner::ok(ScriptedProc {
            alive_polls: 0,
            exit: Some(ExitInfo { status: Some(1), stderr_tail: "already running".into() }),
        });
        let v = ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock")).await.unwrap();
        assert_eq!(v, "0.1.0");
    }

    #[tokio::test]
    async fn timeout_with_dead_child_diagnoses_daemon_exited() {
        let ep = ScriptedEndpoint::new(vec![fail()]);
        let sp = ScriptedSpawner::ok(ScriptedProc {
            alive_polls: 0,
            exit: Some(ExitInfo { status: Some(2), stderr_tail: "bad config".into() }),
        });
        match ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock")).await {
            Err(EnsureError::ReadyTimeout {
                diagnosis: ReadyDiagnosis::DaemonExited { status: Some(2), stderr_tail },
                ..
            }) => assert_eq!(stderr_tail, "bad config"),
            other => panic!("expected DaemonExited diagnosis, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn timeout_with_live_child_diagnoses_unresponsive() {
        let ep = ScriptedEndpoint::new(vec![fail()]);
        let sp = ScriptedSpawner::ok(ScriptedProc { alive_polls: usize::MAX, exit: None });
        match ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock")).await {
            Err(EnsureError::ReadyTimeout { diagnosis: ReadyDiagnosis::Unresponsive, .. }) => {}
            other => panic!("expected Unresponsive diagnosis, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn spawn_io_failure_is_spawn_failed() {
        let ep = ScriptedEndpoint::new(vec![fail()]);
        let sp = ScriptedSpawner::fails();
        match ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock")).await {
            Err(EnsureError::SpawnFailed { binary, .. }) => {
                assert_eq!(binary, Path::new("/bundle/datamancerd"));
            }
            other => panic!("expected SpawnFailed, got {other:?}"),
        }
    }

    #[test]
    fn version_compatibility_is_major_and_pre_1_minor() {
        assert!(version_compatible("0.1.0", "0.1.9"));
        assert!(!version_compatible("0.1.0", "0.2.0")); // pre-1.0: minor breaks
        assert!(version_compatible("1.2.0", "1.9.3")); // post-1.0: major only
        assert!(!version_compatible("1.0.0", "2.0.0"));
        assert!(!version_compatible("0.1.0", "garbage"));
    }
}
```

Notes for the implementer: `ExitInfo` needs `#[derive(Debug, Clone)]` and `PingFailure` `#[derive(Debug, Clone)]` for the fakes above. These are async-fn-in-trait traits used generically (no dyn) — fine on edition 2024.

- [ ] **Step 3: Run tests to verify failure**

Run: `cargo test -p datamancer-client --features app 2>&1 | tail -5`
Expected: COMPILE ERROR — `ensure_daemon`, traits not defined.

- [ ] **Step 4: Implement `lifecycle.rs`**

```rust
//! Find-or-spawn-and-await-readiness, as a state machine over two seams so
//! the logic tests against fakes and a Windows port is additive: a
//! [`ControlEndpoint`] (UDS today, named pipe later) and a [`DaemonSpawner`]
//! (detached unix spawn today, CreateProcess later).

use std::path::Path;
use std::time::Duration;

use tokio::time::Instant;

use crate::app::error::{EnsureError, ReadyDiagnosis};
use crate::app::EnsureConfig;

/// Interval between readiness probes while awaiting a spawned daemon.
const READY_POLL: Duration = Duration::from_millis(100);
/// Per-probe bound (connect + ping round-trip).
const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// A failed readiness probe (absent socket, refused, stale socket, no/bad
/// reply). The reason is diagnostic only: every failure means "not ready".
#[derive(Debug, Clone)]
pub(crate) struct PingFailure(pub String);

/// One control-surface probe: ping the socket, return the daemon version.
pub(crate) trait ControlEndpoint {
    async fn ping(&self, socket: &Path, timeout: Duration) -> Result<String, PingFailure>;
}

/// A spawned daemon's exit observation (best effort).
#[derive(Debug, Clone)]
pub(crate) struct ExitInfo {
    pub status: Option<i32>,
    /// Tail of the daemon's log/stderr; empty if unavailable.
    pub stderr_tail: String,
}

/// Handle onto a spawned daemon process, for exit polling only — the spawn
/// is detached and deliberately unsupervised.
pub(crate) trait SpawnedDaemon: Send {
    /// `Some` once the process has exited (idempotent thereafter).
    fn poll_exit(&mut self) -> Option<ExitInfo>;
}

/// Spawns the daemon binary, detached, stdio to a log file.
pub(crate) trait DaemonSpawner {
    type Proc: SpawnedDaemon;
    fn spawn(&self, binary: &Path, config: Option<&Path>) -> std::io::Result<Self::Proc>;
}

/// Find a ready daemon on `socket` or spawn one and await readiness.
/// Returns the daemon's version (from `ping`).
///
/// A spawned process exiting is **not** failure while the deadline holds:
/// losing the single-instance race to another app's daemon that then answers
/// is success. The exit is only reported as the diagnosis if no daemon ever
/// answers.
pub(crate) async fn ensure_daemon<E: ControlEndpoint, S: DaemonSpawner>(
    endpoint: &E,
    spawner: &S,
    cfg: &EnsureConfig,
    socket: &Path,
) -> Result<String, EnsureError> {
    if let Ok(version) = endpoint.ping(socket, PROBE_TIMEOUT).await {
        return Ok(version);
    }
    let mut proc_ = spawner
        .spawn(&cfg.daemon_binary, cfg.config_path.as_deref())
        .map_err(|source| EnsureError::SpawnFailed {
            binary: cfg.daemon_binary.clone(),
            source,
        })?;
    let deadline = Instant::now() + cfg.ready_timeout;
    let mut observed_exit: Option<ExitInfo> = None;
    loop {
        if let Ok(version) = endpoint.ping(socket, PROBE_TIMEOUT).await {
            return Ok(version);
        }
        if observed_exit.is_none() {
            observed_exit = proc_.poll_exit();
        }
        if Instant::now() >= deadline {
            let diagnosis = match observed_exit {
                Some(ExitInfo { status, stderr_tail }) => {
                    ReadyDiagnosis::DaemonExited { status, stderr_tail }
                }
                None => ReadyDiagnosis::Unresponsive,
            };
            return Err(EnsureError::ReadyTimeout {
                timeout: cfg.ready_timeout,
                diagnosis,
            });
        }
        tokio::time::sleep(READY_POLL).await;
    }
}

/// Compatibility floor: equal major version, and equal minor while major
/// is 0 (cargo semver convention). Unparseable versions are incompatible.
pub(crate) fn version_compatible(client: &str, daemon: &str) -> bool {
    fn major_minor(v: &str) -> Option<(u64, u64)> {
        let mut parts = v.split('.');
        Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
    }
    match (major_minor(client), major_minor(daemon)) {
        (Some((cmaj, cmin)), Some((dmaj, dmin))) => {
            cmaj == dmaj && (cmaj != 0 || cmin == dmin)
        }
        _ => false,
    }
}
```

Add `mod` visibility in `app/mod.rs`: `pub(crate) use lifecycle::…` is unnecessary — `lifecycle` is already a private module of `app`; Tasks 5–6 use `super::lifecycle::…` paths. Add `tokio` `time` feature to the client crate's optional tokio dep (`"time"` in the feature list) if not already present.

- [ ] **Step 5: Run tests**

Run: `cargo test -p datamancer-client --features app && cargo clippy --all-targets --features app -- -D warnings && cargo fmt`
Expected: 7 new tests pass. (Note: workspace clippy without the feature must also stay clean: `cargo clippy --all-targets -- -D warnings`.)

- [ ] **Step 6: Commit**

```bash
git add crates/datamancer-client/src/app crates/datamancer-client/src/lib.rs crates/datamancer-client/Cargo.toml
git commit -m "feat(client): app lifecycle — find-or-spawn state machine behind platform seams"
```

---

### Task 5: Real platform implementations (`TokioEndpoint`, `ProcessSpawner`)

**Files:**
- Create: `crates/datamancer-client/src/app/platform.rs`
- Modify: `crates/datamancer-client/src/app/mod.rs` (`mod platform;`)
- Modify: `crates/datamancer-client/src/paths.rs` (`default_daemon_log`)

**Interfaces:**
- Consumes: `ControlEndpoint`, `DaemonSpawner`, `SpawnedDaemon`, `ExitInfo`, `PingFailure` (Task 4); `Request::Ping`/`Reply` (Task 3).
- Produces: `pub(crate) struct TokioEndpoint;` implementing `ControlEndpoint`; `pub(crate) struct ProcessSpawner { log_path: PathBuf }` with `ProcessSpawner::new(log_path: PathBuf)` implementing `DaemonSpawner`; `pub fn default_daemon_log() -> Option<PathBuf>` in `paths.rs` (platform data dir + `datamancerd.log`, same `ProjectDirs` pattern as `default_control_socket`).

- [ ] **Step 1: Write the failing tests**

In `paths.rs`'s test module (mirroring the existing socket-path test):

```rust
#[test]
fn default_daemon_log_lives_in_the_data_dir() {
    let path = default_daemon_log().expect("home dir exists in test env");
    assert!(path.ends_with("datamancer/datamancerd.log") ||
            path.to_string_lossy().ends_with("datamancer/datamancerd.log"));
}
```

In `platform.rs`'s test module (these exercise the real endpoint against an in-process fake UDS server, and the spawner against `/bin/sh` — no daemon needed):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::lifecycle::{ControlEndpoint, DaemonSpawner, SpawnedDaemon};
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

    async fn fake_daemon(reply: &'static str) -> std::path::PathBuf {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("control.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            let _req = lines.next_line().await.unwrap();
            write.write_all(reply.as_bytes()).await.unwrap();
            write.write_all(b"\n").await.unwrap();
        });
        path
    }

    #[tokio::test]
    async fn ping_extracts_version_from_a_live_socket() {
        let path = fake_daemon(r#"{"ok":true,"version":"9.9.9"}"#).await;
        let v = TokioEndpoint.ping(&path, Duration::from_secs(1)).await.unwrap();
        assert_eq!(v, "9.9.9");
    }

    #[tokio::test]
    async fn ping_fails_on_error_reply_and_absent_socket() {
        let path = fake_daemon(r#"{"ok":false,"code":"shutting_down","message":"…"}"#).await;
        assert!(TokioEndpoint.ping(&path, Duration::from_secs(1)).await.is_err());
        let absent = std::path::Path::new("/nonexistent/never.sock");
        assert!(TokioEndpoint.ping(absent, Duration::from_millis(200)).await.is_err());
    }

    #[tokio::test]
    async fn spawner_detaches_logs_and_reports_exit_tail() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("d.log");
        let spawner = ProcessSpawner::new(log.clone());
        // `--config <path>` mirrors the real invocation; sh -c ignores it.
        let mut proc_ = spawner
            .spawn(std::path::Path::new("/bin/sh"), None)
            .map(|p| p) // type inference aid if needed
            .unwrap();
        // /bin/sh with no script exits immediately (status 0) — poll until it does.
        let exit = loop {
            if let Some(e) = proc_.poll_exit() {
                break e;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(exit.status, Some(0));
        assert!(log.exists(), "log file must be created");
    }
}
```

(Adjust the `/bin/sh` test if it proves flaky on CI — the load-bearing asserts are: spawn succeeds, the log file is created, `poll_exit` eventually yields the status.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-client --features app platform 2>&1 | tail -5`
Expected: COMPILE ERROR.

- [ ] **Step 3: Implement**

`paths.rs` addition:

```rust
/// Default destination for a facade-spawned daemon's stdout/stderr:
/// `<data dir>/datamancerd.log` (macOS `~/Library/Application
/// Support/datamancer`, Linux `~/.local/share/datamancer`).
#[must_use]
pub fn default_daemon_log() -> Option<PathBuf> {
    let dirs = ProjectDirs::from("", "", "datamancer")?;
    Some(dirs.data_dir().join("datamancerd.log"))
}
```

`platform.rs`:

```rust
//! Unix implementations of the lifecycle seams: a tokio-UDS
//! [`ControlEndpoint`] and a detached-process [`DaemonSpawner`]. A Windows
//! port replaces this module (named pipe + CreateProcess) without touching
//! the state machine.

use std::fs::OpenOptions;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;

use crate::app::lifecycle::{
    ControlEndpoint, DaemonSpawner, ExitInfo, PingFailure, SpawnedDaemon,
};
use crate::protocol::uds::{Reply, Request};

/// How much of the daemon log to quote in an exit diagnosis.
const LOG_TAIL_BYTES: u64 = 2048;

pub(crate) struct TokioEndpoint;

impl ControlEndpoint for TokioEndpoint {
    async fn ping(&self, socket: &Path, timeout: Duration) -> Result<String, PingFailure> {
        let attempt = async {
            let stream = UnixStream::connect(socket)
                .await
                .map_err(|e| PingFailure(format!("connect: {e}")))?;
            let (read, mut write) = stream.into_split();
            let mut line = serde_json::to_vec(&Request::Ping)
                .map_err(|e| PingFailure(format!("encode: {e}")))?;
            line.push(b'\n');
            write
                .write_all(&line)
                .await
                .map_err(|e| PingFailure(format!("write: {e}")))?;
            let reply_line = BufReader::new(read)
                .lines()
                .next_line()
                .await
                .map_err(|e| PingFailure(format!("read: {e}")))?
                .ok_or_else(|| PingFailure("eof before reply".to_string()))?;
            let reply: Reply = serde_json::from_str(&reply_line)
                .map_err(|e| PingFailure(format!("decode: {e}")))?;
            match (reply.ok, reply.version) {
                (true, Some(version)) => Ok(version),
                (true, None) => Err(PingFailure("ping reply missing version".to_string())),
                (false, _) => Err(PingFailure(format!(
                    "daemon rejected ping: {}",
                    reply.code.unwrap_or_default()
                ))),
            }
        };
        tokio::time::timeout(timeout, attempt)
            .await
            .map_err(|_| PingFailure("probe timed out".to_string()))?
    }
}

/// Spawns the daemon **detached** (its own session via `process_group(0)`),
/// stdio appended to a log file — the daemon is a shared host service that
/// must outlive the spawning app.
pub(crate) struct ProcessSpawner {
    log_path: PathBuf,
}

impl ProcessSpawner {
    pub(crate) fn new(log_path: PathBuf) -> Self {
        Self { log_path }
    }
}

pub(crate) struct UnixDaemonProcess {
    child: Child,
    log_path: PathBuf,
    exited: Option<ExitInfo>,
}

impl DaemonSpawner for ProcessSpawner {
    type Proc = UnixDaemonProcess;

    fn spawn(&self, binary: &Path, config: Option<&Path>) -> std::io::Result<UnixDaemonProcess> {
        if let Some(parent) = self.log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        let mut cmd = Command::new(binary);
        if let Some(config) = config {
            cmd.arg("--config").arg(config);
        }
        cmd.stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            cmd.process_group(0);
        }
        let child = cmd.spawn()?;
        Ok(UnixDaemonProcess {
            child,
            log_path: self.log_path.clone(),
            exited: None,
        })
    }
}

impl SpawnedDaemon for UnixDaemonProcess {
    fn poll_exit(&mut self) -> Option<ExitInfo> {
        if self.exited.is_none() {
            if let Ok(Some(status)) = self.child.try_wait() {
                self.exited = Some(ExitInfo {
                    status: status.code(),
                    stderr_tail: log_tail(&self.log_path),
                });
            }
        }
        self.exited.clone()
    }
}

/// Last [`LOG_TAIL_BYTES`] of the daemon log, best effort (empty on any error).
fn log_tail(path: &Path) -> String {
    let read = || -> std::io::Result<String> {
        let mut f = std::fs::File::open(path)?;
        let len = f.metadata()?.len();
        f.seek(SeekFrom::Start(len.saturating_sub(LOG_TAIL_BYTES)))?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        Ok(buf)
    };
    read().unwrap_or_default().trim().to_string()
}
```

Add `mod platform;` to `app/mod.rs`. Ensure the crate's optional tokio feature list includes `"time"` and `"process"` is NOT needed (spawn is std). Ensure `tempfile` is already a dev-dependency (it is).

- [ ] **Step 4: Run tests**

Run: `cargo test -p datamancer-client --features app && cargo clippy --all-targets --features app -- -D warnings && cargo fmt`
Expected: PASS (4 new tests).

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-client/src/app crates/datamancer-client/src/paths.rs
git commit -m "feat(client): unix control-endpoint and detached daemon spawner"
```

---

### Task 6: `AppHandle` facade

**Files:**
- Modify: `crates/datamancer-client/src/app/mod.rs`

**Interfaces:**
- Consumes: `ensure_daemon`, `version_compatible`, `TokioEndpoint`, `ProcessSpawner` (Tasks 4–5); `Iceoryx2Client`/`Iceoryx2Config` (existing); `HealthView` (Task 1); `default_daemon_log` (Task 5); `default_control_socket` (existing).
- Produces (the app-facing API; Task 7's e2e drives it):
  - `pub type AppEvents = <Iceoryx2Client as Client>::Events;`
  - `AppHandle::ensure(cfg: EnsureConfig) -> Result<(AppHandle, AppEvents), EnsureError>`
  - `AppHandle::daemon_version(&self) -> &str`
  - `AppHandle::health(&mut self) -> Result<HealthView, ClientError<Iceoryx2ClientError>>` (snapshot → reduction → `daemon.version` filled)
  - Delegations: `subscribe`, `unsubscribe`, `instruments`, `snapshot`, `close` — same signatures as the `Client` trait methods.

- [ ] **Step 1: Write the failing tests**

The connect path needs a live daemon (e2e, Task 7); unit-test the two pure seams here — the version gate and the health fill — in `app/mod.rs`'s test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skew_gate_produces_typed_error() {
        match check_version("0.2.0") {
            Err(EnsureError::VersionSkew { daemon, client }) => {
                assert_eq!(daemon, "0.2.0");
                assert_eq!(client, env!("CARGO_PKG_VERSION"));
            }
            other => panic!("expected VersionSkew, got {other:?}"),
        }
        assert!(check_version(env!("CARGO_PKG_VERSION")).is_ok());
    }

    #[test]
    fn health_fill_sets_daemon_version() {
        use datamancer_core::{CacheSnapshot, HealthView, SystemSnapshot, Timestamp};
        let snap = SystemSnapshot::new(
            Timestamp(1),
            vec![],
            CacheSnapshot::new(vec![], None),
            vec![],
            vec![],
        );
        let view = fill_health(&snap, "0.1.0");
        assert_eq!(view.daemon.version.as_deref(), Some("0.1.0"));
        assert_eq!(view.schema_version, HealthView::SCHEMA_VERSION);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-client --features app skew_gate 2>&1 | tail -5`
Expected: COMPILE ERROR — `check_version`/`fill_health` undefined.

- [ ] **Step 3: Implement in `app/mod.rs`**

```rust
use datamancer_core::{HealthView, InstrumentInfo, ProviderId, SystemSnapshot};

use crate::Client as _;
use crate::error::ClientError;
use crate::iceoryx2::{Iceoryx2Client, Iceoryx2ClientError, Iceoryx2Config};
use crate::spec::{SubscriptionSpec, UnsubscribeSpec};

/// The multiplexed event stream (same contract as the underlying
/// [`crate::Client`] impl: `(instrument, seq)`-ordered, loss never silent).
pub type AppEvents = <Iceoryx2Client as crate::Client>::Events;

/// Gate the connection on the daemon's reported version.
fn check_version(daemon: &str) -> Result<(), EnsureError> {
    let client = env!("CARGO_PKG_VERSION");
    if lifecycle::version_compatible(client, daemon) {
        Ok(())
    } else {
        Err(EnsureError::VersionSkew {
            daemon: daemon.to_string(),
            client: client.to_string(),
        })
    }
}

/// Reduce a snapshot and stamp the daemon version onto it.
fn fill_health(snapshot: &SystemSnapshot, daemon_version: &str) -> HealthView {
    let mut view = HealthView::from_snapshot(snapshot, HealthView::DEFAULT_STALE_AFTER_NS);
    view.daemon.version = Some(daemon_version.to_string());
    view
}

/// The app-facing daemon handle: found-or-spawned, connected, versioned.
///
/// Holds the same-host [`Iceoryx2Client`] and adds no protocol semantics —
/// every method maps to control-surface ops.
pub struct AppHandle {
    client: Iceoryx2Client,
    daemon_version: String,
}

impl AppHandle {
    /// Find a running daemon at the (default or configured) control socket,
    /// or spawn `cfg.daemon_binary` detached and await readiness; then
    /// connect. Losing a spawn race to another app's daemon is success.
    ///
    /// # Errors
    ///
    /// [`EnsureError`] — each variant is app-actionable (see its docs).
    pub async fn ensure(cfg: EnsureConfig) -> Result<(Self, AppEvents), EnsureError> {
        let socket = cfg
            .control_socket
            .clone()
            .or_else(crate::default_control_socket)
            .ok_or(EnsureError::NoSocketPath)?;
        let log_path = cfg
            .log_path
            .clone()
            .or_else(crate::paths::default_daemon_log)
            .ok_or(EnsureError::NoSocketPath)?;
        let daemon_version = lifecycle::ensure_daemon(
            &platform::TokioEndpoint,
            &platform::ProcessSpawner::new(log_path),
            &cfg,
            &socket,
        )
        .await?;
        check_version(&daemon_version)?;
        let (client, events) = Iceoryx2Client::connect(Iceoryx2Config {
            control_socket: socket,
            client_name: cfg.client_name.clone(),
            poll_interval: cfg.poll_interval,
            event_buffer: cfg.event_buffer,
        })
        .await?;
        Ok((
            Self {
                client,
                daemon_version,
            },
            events,
        ))
    }

    /// The daemon version reported at connect (`ping`).
    #[must_use]
    pub fn daemon_version(&self) -> &str {
        &self.daemon_version
    }

    /// Typed health for app rendering: the daemon snapshot reduced to
    /// [`HealthView`], with `daemon.version` filled from the handshake.
    ///
    /// # Errors
    ///
    /// Propagates the underlying `snapshot` control/transport failure.
    pub async fn health(
        &mut self,
    ) -> Result<HealthView, ClientError<Iceoryx2ClientError>> {
        let snapshot = self.client.snapshot().await?;
        Ok(fill_health(&snapshot, &self.daemon_version))
    }

    /// See [`crate::Client::subscribe`].
    ///
    /// # Errors
    ///
    /// See [`crate::Client::subscribe`].
    pub async fn subscribe(
        &mut self,
        spec: &SubscriptionSpec,
    ) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client.subscribe(spec).await
    }

    /// See [`crate::Client::unsubscribe`].
    ///
    /// # Errors
    ///
    /// See [`crate::Client::unsubscribe`].
    pub async fn unsubscribe(
        &mut self,
        spec: &UnsubscribeSpec,
    ) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client.unsubscribe(spec).await
    }

    /// See [`crate::Client::instruments`].
    ///
    /// # Errors
    ///
    /// See [`crate::Client::instruments`].
    pub async fn instruments(
        &mut self,
        provider: Option<&ProviderId>,
    ) -> Result<Vec<InstrumentInfo>, ClientError<Iceoryx2ClientError>> {
        self.client.instruments(provider).await
    }

    /// The raw diagnostics snapshot (prefer [`Self::health`] for rendering).
    ///
    /// # Errors
    ///
    /// See [`crate::Client::snapshot`].
    pub async fn snapshot(
        &mut self,
    ) -> Result<SystemSnapshot, ClientError<Iceoryx2ClientError>> {
        self.client.snapshot().await
    }

    /// Graceful close of this client (the daemon keeps running — deliberate
    /// daemon stop is a cycle-3 capability).
    ///
    /// # Errors
    ///
    /// See [`crate::Client::close`].
    pub async fn close(self) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client.close().await
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p datamancer-client --features app && cargo clippy --all-targets --features app -- -D warnings && cargo fmt`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-client/src/app/mod.rs
git commit -m "feat(client): AppHandle facade — ensure, version gate, typed health"
```

---

### Task 7: End-to-end tests (`#[ignore]`d)

**Files:**
- Create: `crates/datamancerd/tests/app_facade_e2e.rs`
- Modify: `crates/datamancerd/Cargo.toml` (dev-dependency line 64: add `"app"` to the `datamancer-client` features list)

**Interfaces:**
- Consumes: `AppHandle`, `EnsureConfig`, `AppEvents` (Task 6); `env!("CARGO_BIN_EXE_datamancerd")` (cargo-provided); the existing `daemon_e2e.rs` conventions (temp config, `#[ignore]`, cleanup).

These run only with a live iceoryx2 runtime and Alpaca paper credentials, like the existing e2e: `cargo test -p datamancerd --test app_facade_e2e -- --ignored --test-threads=1`. Follow `daemon_e2e.rs` for the config-TOML fixture (copy its provider/cache/session sections; override `admin_socket` to a temp path). `--test-threads=1` because the daemon's single-instance lock is host-global.

- [ ] **Step 1: Write the tests**

```rust
//! End-to-end: the app facade against the real binary. `#[ignore]`d — needs
//! a live iceoryx2 runtime; run with
//! `cargo test -p datamancerd --test app_facade_e2e -- --ignored --test-threads=1`.
//! The single-instance lock is per-user host-global: these tests cannot run
//! alongside a real datamancerd or in parallel with daemon_e2e.

use std::path::PathBuf;
use std::time::Duration;

use datamancer_client::app::{AppHandle, EnsureConfig};

/// Minimal daemon config in a tempdir; socket path returned alongside.
/// Mirror daemon_e2e.rs's fixture (same provider + [server] sections) —
/// override admin_socket to the tempdir.
fn write_config(dir: &std::path::Path) -> (PathBuf, PathBuf) {
    let socket = dir.join("control.sock");
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[provider.alpaca_crypto]
account_type = "paper"
venue = "us"

[server]
admin_socket = "{}"
service_prefix = "app-facade-e2e"
shutdown_timeout_secs = 5
"#,
            socket.display()
        ),
    )
    .unwrap();
    (config, socket)
}

fn ensure_cfg(dir: &std::path::Path, name: &str) -> EnsureConfig {
    let (config, socket) = write_config(dir);
    let mut cfg = EnsureConfig::new(env!("CARGO_BIN_EXE_datamancerd"), name);
    cfg.config_path = Some(config);
    cfg.control_socket = Some(socket);
    cfg.log_path = Some(dir.join("daemon.log"));
    cfg.ready_timeout = Duration::from_secs(15);
    cfg
}

/// Kill the daemon we spawned: the facade detaches, so recover the pid from
/// the single-instance lockfile (documented as the holder's pid) and TERM it.
fn stop_daemon() {
    let lock = directories::ProjectDirs::from("", "", "datamancer")
        .unwrap()
        .data_dir()
        .join("datamancerd.lock");
    if let Ok(pid) = std::fs::read_to_string(&lock) {
        let pid = pid.trim().to_string();
        if !pid.is_empty() {
            let _ = std::process::Command::new("kill").arg(&pid).status();
            std::thread::sleep(Duration::from_millis(1500));
        }
    }
}

#[tokio::test]
#[ignore = "needs live iceoryx2 runtime and host-global single-instance lock"]
async fn ensure_spawns_daemon_and_health_reports_version() {
    let dir = tempfile::tempdir().unwrap();
    let (mut handle, _events) = AppHandle::ensure(ensure_cfg(dir.path(), "e2e-a"))
        .await
        .expect("ensure must spawn and connect");
    assert!(!handle.daemon_version().is_empty());
    let health = handle.health().await.expect("health");
    assert_eq!(health.daemon.version.as_deref(), Some(handle.daemon_version()));
    assert!(!health.providers.is_empty(), "configured provider must appear");
    handle.close().await.expect("close");
    stop_daemon();
}

#[tokio::test]
#[ignore = "needs live iceoryx2 runtime and host-global single-instance lock"]
async fn concurrent_ensures_share_one_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let (cfg_a, cfg_b) = (ensure_cfg(dir.path(), "e2e-race-a"), {
        let mut c = ensure_cfg(dir.path(), "e2e-race-b");
        // Same socket + config: both race to spawn; the lock arbitrates.
        c
    });
    let (ra, rb) = tokio::join!(AppHandle::ensure(cfg_a), AppHandle::ensure(cfg_b));
    let (mut a, _ea) = ra.expect("racer A must succeed (spawn or lost-race connect)");
    let (b, _eb) = rb.expect("racer B must succeed (spawn or lost-race connect)");
    assert_eq!(a.daemon_version(), b.daemon_version());
    // Both clients registered on the one daemon.
    let snapshot = a.snapshot().await.expect("snapshot");
    assert!(snapshot.client_sessions.len() >= 2);
    let _ = b.close().await;
    let _ = a.close().await;
    stop_daemon();
}
```

Note: `write_config` is called twice in the race test via `ensure_cfg` — that rewrites the same file with identical contents; harmless. If `daemon_e2e.rs`'s fixture includes sections required by config validation beyond `[provider.*]` (check it first), copy those too. Add `directories` and `tempfile` to `datamancerd`'s dev-dependencies if not present.

- [ ] **Step 2: Verify they compile and are skipped by default**

Run: `cargo test -p datamancerd --test app_facade_e2e`
Expected: `2 ignored`, 0 failed.

- [ ] **Step 3: Run them for real (needs credentials + iceoryx2 runtime)**

Run: `cargo test -p datamancerd --test app_facade_e2e -- --ignored --test-threads=1`
Expected: 2 passed. If the environment lacks credentials, note it in the commit/PR and get a manual run before merge — do not claim they passed.

- [ ] **Step 4: Commit**

```bash
git add crates/datamancerd/tests/app_facade_e2e.rs crates/datamancerd/Cargo.toml
git commit -m "test(datamancerd): app-facade e2e — spawn, health, ensure race"
```

---

### Task 8: Documentation

**Files:**
- Modify: `crates/datamancer-client/README.md` (new "App facade" section)
- Modify: `crates/datamancer-client/CLAUDE.md` (invariants addendum)
- Modify: `CLAUDE.md` (workspace: one sentence on the `app` feature in the `datamancer-client` bullet)

**Interfaces:** none — prose only. Content requirements:

- [ ] **Step 1: `datamancer-client/README.md`** — add a section documenting: feature `app` (implies `iceoryx2`, off by default); an `AppHandle::ensure` example (construct `EnsureConfig::new(bin, name)`, destructure `(handle, events)`, call `health()`); the ensure semantics (connect-or-spawn, detached spawn, lost-race-is-success, spawn-don't-supervise/reconnect-by-recreate, deliberate daemon stop deferred to cycle 3); the version-skew floor (equal major; equal minor pre-1.0); `HealthView` pointers (per-symbol only, latency observability-only, `Unauthenticated`/`CompanionUnreachable` reserved for IBKR); the `ping` op.

- [ ] **Step 2: `datamancer-client/CLAUDE.md`** — append invariants: `app` implies `iceoryx2` and gains no WS lifecycle powers; the facade adds no protocol semantics; platform seams (`ControlEndpoint`/`DaemonSpawner`) are internal traits — Windows lands as a new `platform` impl, never by widening the state machine; `EnsureError` variants and the `ping` reply shape are app-facing contract.

- [ ] **Step 3: Workspace `CLAUDE.md`** — in the `datamancer-client` bullet, mention the `app` feature (app-facing facade: find-or-spawn + typed health; off by default), and add `health.rs`/`HealthView` to the `datamancer-core` description sentence.

- [ ] **Step 4: Verify + commit**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features app -p datamancer-client -- -D warnings && cargo fmt --check`
Expected: clean.

```bash
git add crates/datamancer-client/README.md crates/datamancer-client/CLAUDE.md CLAUDE.md
git commit -m "docs: app facade — README, crate invariants, workspace map"
```

---

## Self-review notes (already applied)

- Spec coverage: ensure/discover/spawn/readiness/race (T4–5), version gate (T4/T6), platform seams (T4–5), `HealthView` in core + embedder parity (T1–2), facade `health()` (T6), typed `EnsureError` (T4), ping (T3), e2e incl. race (T7), docs (T8). Cycle-1 scope cuts stated in spec (no supervision, no push health, no shutdown op) are respected — `close()` doc points at cycle 3 for daemon stop.
- Deviation from spec sketch, recorded: `Liveness` ships `{Idle, Live, Stale}` — `Gapped { spans }` needs gap-span history the live snapshot lacks; `gap_count` carries the cycle-1 signal and the enum is `#[non_exhaustive]` for cycle 4. `LatencySummary` is last-observation-only for the same reason.
- Type consistency: `ensure_daemon` returns `String` (version) consumed by `check_version` (T6); `PingFailure`/`ExitInfo` clone-able for fakes; `Reply.version` produced in T3, consumed in T5.
