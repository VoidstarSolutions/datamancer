# Unified Client Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new `datamancer-client` crate with a generic `Client` trait and two implementations (WebSocket, iceoryx2) so a consumer picks a transport by config/type and everything else is identical — including a new instrument/capability discovery op threaded core → orchestrator → daemon → client.

**Architecture:** The daemon's control vocabulary (`SubscriptionSpec`, stable `codes`, UDS `Request`/`Reply`, WS `WsRequest`/`WsReply`, the `*Cfg` serde enums) moves out of the `datamancerd` binary into `datamancer-client` so client libraries can link it. The trait is generic (associated `Config`/`Error`/`Events` types, RPITIT `impl Future` methods, no boxing); `connect` returns a split `(handle, events)` pair. Discovery derives per-instrument `EventKind` lists from the existing `Provider::list_instruments()` + `supports()` — the `Provider` trait is unchanged.

**Tech Stack:** Rust edition 2024, tokio, tokio-tungstenite 0.29.0 (pinned), iceoryx2 0.9.2 (pinned), serde/serde_json, thiserror, futures, tokio-stream.

**Spec:** `docs/superpowers/specs/2026-07-03-unified-client-transport-design.md`

## Global Constraints

- Every crate: `#![forbid(unsafe_code)]`, `[lints] workspace = true` (clippy pedantic = deny).
- `datamancer-client` base deps: `datamancer-core`, `serde`, `serde_json`, `thiserror`, `futures` only. Feature `ws` adds `datamancer-transport-ws`, `tokio-tungstenite = "0.29.0"`, `tokio`, `tokio-stream`. Feature `iceoryx2` adds `datamancer-transport-iceoryx2`, `iceoryx2 = "0.9.2"`, `tokio`, `tokio-stream`. Both features **off by default**.
- No dependency on the `datamancer` orchestrator from `datamancer-client` (it must stay a peer, like the transport crates).
- Wire formats and stable error-code strings are **unchanged** by the vocabulary move — the relocation must be byte-invisible to existing UDS/WS clients. Existing protocol tests are the guard; they move, they don't change.
- `rx_ts` is never synthesized client-side; the timestamp triple crosses verbatim.
- Run `cargo clippy --all-targets -- -D warnings` and `cargo fmt` before every commit.
- Work happens on branch `design/unified-client-transport`.

---

### Task 1: Core kind enumeration + `InstrumentInfo`

**Files:**
- Modify: `crates/datamancer-core/src/event.rs` (add `BarInterval::ALL`, `EventKind::enumerate()`)
- Modify: `crates/datamancer-core/src/instrument.rs` (add `InstrumentInfo`)
- Modify: `crates/datamancer-core/src/lib.rs` (export `InstrumentInfo`)

**Interfaces:**
- Produces: `BarInterval::ALL: [BarInterval; 6]`; `EventKind::enumerate() -> impl Iterator<Item = EventKind>` (yields exactly 8 kinds: Trade, Quote, Bar×6); `pub struct InstrumentInfo { pub instrument: Instrument, pub kinds: Vec<EventKind> }` (`Debug, Clone, PartialEq, Eq, Serialize, Deserialize`), exported from `datamancer_core`.

- [ ] **Step 1: Write the failing tests**

Append to the `serde_tests` module in `crates/datamancer-core/src/event.rs`:

```rust
#[test]
fn event_kind_enumerate_covers_the_full_kind_space() {
    let kinds: Vec<EventKind> = EventKind::enumerate().collect();
    // Trade + Quote + one Bar per interval.
    assert_eq!(kinds.len(), 2 + BarInterval::ALL.len());
    assert!(kinds.contains(&EventKind::Trade));
    assert!(kinds.contains(&EventKind::Quote));
    for interval in BarInterval::ALL {
        assert!(kinds.contains(&EventKind::Bar(interval)));
    }
    // No duplicates.
    let mut dedup = kinds.clone();
    dedup.sort_unstable();
    dedup.dedup();
    assert_eq!(dedup.len(), kinds.len());
}
```

Add a test module to `crates/datamancer-core/src/instrument.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::{AssetClass, Instrument, InstrumentInfo, ProviderId};
    use crate::{BarInterval, EventKind};

    #[test]
    fn instrument_info_serde_round_trips() {
        let info = InstrumentInfo {
            instrument: Instrument::new(
                ProviderId::from_static("alpaca-crypto"),
                AssetClass::Crypto,
                "BTC/USD",
            ),
            kinds: vec![
                EventKind::Trade,
                EventKind::Quote,
                EventKind::Bar(BarInterval::OneDay),
            ],
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: InstrumentInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info, back);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p datamancer-core event_kind_enumerate instrument_info`
Expected: FAIL to compile — `ALL`, `enumerate`, `InstrumentInfo` not defined.

- [ ] **Step 3: Implement**

In `crates/datamancer-core/src/event.rs`, after the `BarInterval` enum:

```rust
impl BarInterval {
    /// Every interval, in ascending duration order. `BarInterval` is a small
    /// closed enum, so the kind space is finitely enumerable — this powers
    /// [`EventKind::enumerate`] and, through it, per-instrument capability
    /// discovery.
    pub const ALL: [BarInterval; 6] = [
        BarInterval::OneSecond,
        BarInterval::OneMinute,
        BarInterval::FiveMinute,
        BarInterval::FifteenMinute,
        BarInterval::OneHour,
        BarInterval::OneDay,
    ];
}
```

After the `EventKind` enum:

```rust
impl EventKind {
    /// Every subscribable kind: `Trade`, `Quote`, and one `Bar` per
    /// [`BarInterval`]. Used to derive an instrument's capability list by
    /// probing [`crate::Provider::supports`] over the full kind space.
    pub fn enumerate() -> impl Iterator<Item = EventKind> {
        [EventKind::Trade, EventKind::Quote]
            .into_iter()
            .chain(BarInterval::ALL.into_iter().map(EventKind::Bar))
    }
}
```

In `crates/datamancer-core/src/instrument.rs` (add `use crate::EventKind;` to the imports), after `Instrument`:

```rust
/// One catalog row: an instrument a provider can serve, with the event kinds
/// it supports. Produced by capability discovery
/// (`Provider::list_instruments` + `supports` probed over
/// [`EventKind::enumerate`]); carried on the daemon's `instruments` control
/// op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentInfo {
    pub instrument: Instrument,
    /// Kinds this provider serves for this instrument, in
    /// [`EventKind::enumerate`] order.
    pub kinds: Vec<EventKind>,
}
```

In `crates/datamancer-core/src/lib.rs`, add `InstrumentInfo` to the `instrument` re-export list (next to `Instrument`, `AssetClass`, `ProviderId`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p datamancer-core && cargo clippy -p datamancer-core --all-targets -- -D warnings`
Expected: PASS, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-core
git commit -m "feat(core): EventKind enumeration + InstrumentInfo catalog row"
```

---

### Task 2: `Datamancer::instrument_catalog`

**Files:**
- Modify: `crates/datamancer/src/session.rs` (new method on `impl Datamancer`, near `snapshot()` at ~line 518)
- Create: `crates/datamancer/tests/instrument_catalog.rs`

**Interfaces:**
- Consumes: `EventKind::enumerate()`, `InstrumentInfo` (Task 1); existing `Provider::{id, supports, list_instruments}`; `DatamancerInner.providers: Vec<Arc<dyn Provider>>`; `Error::UnknownProvider(String)`.
- Produces: `pub async fn instrument_catalog(&self, provider: Option<&ProviderId>) -> Result<Vec<InstrumentInfo>>` on `Datamancer`.

- [ ] **Step 1: Write the failing test**

Create `crates/datamancer/tests/instrument_catalog.rs`:

```rust
//! `Datamancer::instrument_catalog`: per-instrument kind derivation from
//! `list_instruments` + `supports`, with an optional provider filter.

use async_trait::async_trait;
use datamancer::{
    AssetClass, BarInterval, Datamancer, Error, EventKind, Instrument, InstrumentInfo,
    MarketEvent, ProviderId,
};
use datamancer::traits::{HistoryRequest, LiveHandle, Provider};
use tokio::sync::mpsc;

/// Fake provider whose kind support varies **by instrument** — guards the
/// per-instrument catalog shape (a provider-wide kinds list would collapse
/// this distinction).
struct VaryingFake {
    id: &'static str,
}

#[async_trait]
impl Provider for VaryingFake {
    fn id(&self) -> &str {
        self.id
    }

    fn supports(&self, instrument: &Instrument, kind: EventKind) -> bool {
        match instrument.symbol() {
            // Full-service symbol.
            "BTC/USD" => matches!(
                kind,
                EventKind::Trade | EventKind::Quote | EventKind::Bar(BarInterval::OneDay)
            ),
            // Bars-only symbol.
            "IDX" => matches!(kind, EventKind::Bar(BarInterval::OneDay)),
            _ => false,
        }
    }

    async fn start_live(
        &self,
        _sink: mpsc::Sender<MarketEvent>,
    ) -> datamancer::Result<Box<dyn LiveHandle>> {
        Err(Error::Provider {
            provider: self.id.to_string(),
            message: "not live-capable".to_string(),
        })
    }

    async fn fetch_history(
        &self,
        _request: HistoryRequest,
        _sink: mpsc::Sender<MarketEvent>,
    ) -> datamancer::Result<()> {
        Ok(())
    }

    async fn list_instruments(&self) -> datamancer::Result<Vec<Instrument>> {
        Ok(vec![
            Instrument::new(ProviderId::from_static(self.id), AssetClass::Crypto, "BTC/USD"),
            Instrument::new(ProviderId::from_static(self.id), AssetClass::Crypto, "IDX"),
        ])
    }
}

fn dm() -> Datamancer {
    Datamancer::builder()
        .provider(Box::new(VaryingFake { id: "fake-a" }))
        .provider(Box::new(VaryingFake { id: "fake-b" }))
        .build()
        .expect("build")
}

#[tokio::test]
async fn catalog_derives_kinds_per_instrument() {
    let catalog = dm()
        .instrument_catalog(Some(&ProviderId::from_static("fake-a")))
        .await
        .expect("catalog");
    assert_eq!(
        catalog,
        vec![
            InstrumentInfo {
                instrument: Instrument::new(
                    ProviderId::from_static("fake-a"),
                    AssetClass::Crypto,
                    "BTC/USD"
                ),
                kinds: vec![
                    EventKind::Trade,
                    EventKind::Quote,
                    EventKind::Bar(BarInterval::OneDay),
                ],
            },
            InstrumentInfo {
                instrument: Instrument::new(
                    ProviderId::from_static("fake-a"),
                    AssetClass::Crypto,
                    "IDX"
                ),
                kinds: vec![EventKind::Bar(BarInterval::OneDay)],
            },
        ]
    );
}

#[tokio::test]
async fn catalog_without_filter_fans_over_all_providers() {
    let catalog = dm().instrument_catalog(None).await.expect("catalog");
    // Two providers x two instruments each.
    assert_eq!(catalog.len(), 4);
    assert!(catalog.iter().any(|i| i.instrument.provider().as_str() == "fake-a"));
    assert!(catalog.iter().any(|i| i.instrument.provider().as_str() == "fake-b"));
}

#[tokio::test]
async fn unknown_provider_filter_is_an_error() {
    let err = dm()
        .instrument_catalog(Some(&ProviderId::from_static("nope")))
        .await
        .expect_err("unknown provider");
    assert!(matches!(err, Error::UnknownProvider(p) if p == "nope"));
}
```

Note: if `Datamancer::builder()` / the `traits` module path differ from the imports above, match the pattern used at the top of `crates/datamancer/tests/client_session.rs` — the fake-provider scaffolding there is the precedent (do not invent a new one).

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p datamancer --test instrument_catalog`
Expected: FAIL to compile — `instrument_catalog` not defined.

- [ ] **Step 3: Implement**

In `crates/datamancer/src/session.rs`, inside `impl Datamancer` (after `snapshot_live`), add:

```rust
/// Enumerate the instruments each registered provider can serve, with the
/// event kinds each instrument supports.
///
/// Kinds are derived by probing [`Provider::supports`] over
/// [`EventKind::enumerate`] for every instrument returned by
/// [`Provider::list_instruments`] — the kind space is finite and closed, so
/// no provider-side enumeration surface is needed. Pass `provider` to
/// restrict the catalog (a full equities list is ~10k rows).
///
/// Freshness is pass-through: every call hits the provider's
/// reference-data path live. Requests are startup/operator-time, not
/// hot-path.
///
/// # Errors
///
/// - [`Error::UnknownProvider`] — `provider` names no registered provider.
/// - Any error surfaced by the provider's `list_instruments` call.
pub async fn instrument_catalog(
    &self,
    provider: Option<&datamancer_core::ProviderId>,
) -> Result<Vec<datamancer_core::InstrumentInfo>> {
    let providers: Vec<&Arc<dyn Provider>> = match provider {
        Some(id) => {
            let found = self
                .inner
                .providers
                .iter()
                .find(|p| p.id() == id.as_str())
                .ok_or_else(|| Error::UnknownProvider(id.as_str().to_string()))?;
            vec![found]
        }
        None => self.inner.providers.iter().collect(),
    };
    let mut catalog = Vec::new();
    for p in providers {
        for instrument in p.list_instruments().await? {
            let kinds: Vec<EventKind> = EventKind::enumerate()
                .filter(|kind| p.supports(&instrument, *kind))
                .collect();
            catalog.push(datamancer_core::InstrumentInfo { instrument, kinds });
        }
    }
    Ok(catalog)
}
```

Adjust imports to the file's existing style (it already imports `EventKind`, `Error`, `Arc`, `Provider`; use the already-imported names rather than fully-qualified paths where the file has them). Ensure `datamancer/src/lib.rs` re-exports `InstrumentInfo` (add it wherever core types like `Instrument`/`EventKind` are re-exported).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p datamancer --test instrument_catalog && cargo clippy -p datamancer --all-targets -- -D warnings`
Expected: 3 tests PASS, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer
git commit -m "feat(datamancer): instrument_catalog — per-instrument kind discovery"
```

---

### Task 3: `datamancer-client` crate + vocabulary extraction

**Files:**
- Create: `crates/datamancer-client/Cargo.toml`
- Create: `crates/datamancer-client/src/lib.rs`
- Create: `crates/datamancer-client/src/spec.rs` (Cfg enums + `SubscriptionSpec` + `UnsubscribeSpec`)
- Create: `crates/datamancer-client/src/codes.rs`
- Create: `crates/datamancer-client/src/protocol/mod.rs`, `crates/datamancer-client/src/protocol/uds.rs`, `crates/datamancer-client/src/protocol/ws.rs`
- Modify: `Cargo.toml` (workspace members)
- Modify: `crates/datamancerd/Cargo.toml` (add `datamancer-client` dep, default-features off)
- Modify: `crates/datamancerd/src/config.rs` (Cfg enums become re-imports; `PersistenceCfg::options()` becomes a free fn)
- Modify: `crates/datamancerd/src/control.rs` (types become re-exports; `error_code` stays)
- Modify: `crates/datamancerd/src/ws/protocol.rs` (types become re-exports)
- Modify: `crates/datamancerd/src/server.rs`, `crates/datamancerd/src/ws/conn.rs` (call sites of `persistence.options()`)

**Interfaces:**
- Consumes: `datamancer_core::{AssetClass, EventKind, BarInterval, SystemSnapshot, InstrumentInfo}`.
- Produces (all `pub` from `datamancer_client`): `spec::{AssetClassCfg, EventKindCfg, ScopeCfg, PersistenceCfg, SubscriptionSpec, UnsubscribeSpec}`; `codes` (all existing constants, strings verbatim); `protocol::uds::{Request, Reply}`; `protocol::ws::{WsRequest, WsReply}`. `datamancerd::control` and `datamancerd::ws::protocol` re-export these under their old paths so daemon-internal call sites keep compiling.

Rules for this task: this is a **relocation, not a redesign**. Type definitions, serde attributes, doc comments, and tests move verbatim except where noted. The existing daemon test suite passing unchanged is the acceptance gate.

- [ ] **Step 1: Create the crate skeleton**

`crates/datamancer-client/Cargo.toml`:

```toml
[package]
name = "datamancer-client"
version = "0.1.0"
edition = "2024"
license = "MIT OR Apache-2.0"
description = "Consumer-side client trait and control vocabulary for datamancerd (WebSocket and iceoryx2 transports)"

[features]
# The WebSocket client (network-reachable, single-socket).
ws = [
    "dep:datamancer-transport-ws",
    "dep:tokio-tungstenite",
    "dep:tokio",
    "dep:tokio-stream",
]
# The same-host iceoryx2 client (UDS control + shared-memory data).
iceoryx2 = [
    "dep:datamancer-transport-iceoryx2",
    "dep:iceoryx2",
    "dep:tokio",
    "dep:tokio-stream",
]

[dependencies]
datamancer-core = { path = "../datamancer-core" }
futures = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }

# ws
datamancer-transport-ws = { path = "../datamancer-transport-ws", optional = true }
tokio-tungstenite = { version = "0.29.0", optional = true }

# iceoryx2
datamancer-transport-iceoryx2 = { path = "../datamancer-transport-iceoryx2", optional = true }
iceoryx2 = { version = "0.9.2", optional = true }

# shared by both client impls
tokio = { workspace = true, features = ["io-util"], optional = true }
tokio-stream = { workspace = true, optional = true }

[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt-multi-thread", "time", "io-util"] }
tempfile = "3"

[lints]
workspace = true
```

`crates/datamancer-client/src/lib.rs`:

```rust
//! Consumer-side surface for datamancerd: the control **vocabulary** shared
//! by every transport (subscription specs, stable error codes, request/reply
//! types) and, behind features, concrete clients (`ws`, `iceoryx2`)
//! implementing one generic [`Client`] trait (added in a later task).
//!
//! The vocabulary is the operator-facing contract extracted from the daemon:
//! the JSON shapes and stable code strings here must not change without a
//! breaking-change review — they are regression-guarded by tests.
#![forbid(unsafe_code)]

pub mod codes;
pub mod protocol;
pub mod spec;
```

`crates/datamancer-client/src/protocol/mod.rs`:

```rust
//! Request/reply framings per control surface. One vocabulary
//! ([`crate::spec`], [`crate::codes`]), two framings: newline-JSON over UDS
//! and correlated JSON frames over WS.

pub mod uds;
pub mod ws;
```

Add `"crates/datamancer-client"` to the workspace `members` list in the root `Cargo.toml`.

- [ ] **Step 2: Move the spec enums and subscription types**

Create `crates/datamancer-client/src/spec.rs` by **moving** from `crates/datamancerd/src/config.rs`: `AssetClassCfg`, `EventKindCfg`, `ScopeCfg`, `PersistenceCfg` (definitions + serde attributes + doc comments verbatim), plus their `From` impls into core types (`From<AssetClassCfg> for AssetClass`, `From<EventKindCfg> for EventKind` — both target `datamancer_core` types, so they move). **`PersistenceCfg::options()` does NOT move** (it returns `datamancer::PersistenceOptions`, an orchestrator type — see Step 4).

Then move `SubscriptionSpec` from `crates/datamancerd/src/control.rs` verbatim, and add the new named unsubscribe tuple (fields exactly matching today's inline `Unsubscribe` variant fields, so `#[serde(flatten)]` keeps the wire shape identical):

```rust
/// The `(provider, asset_class, symbol, kind)` tuple an `unsubscribe` names.
/// Flattened into the request frame, so the wire shape is identical to the
/// historical inline fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnsubscribeSpec {
    pub provider: String,
    pub asset_class: AssetClassCfg,
    pub symbol: String,
    pub kind: EventKindCfg,
}
```

- [ ] **Step 3: Move codes and the request/reply types**

`crates/datamancer-client/src/codes.rs`: move the entire `codes` module body from `crates/datamancerd/src/control.rs` verbatim (every constant, every doc comment). `error_code(&datamancer::Error)` **stays** in `datamancerd/src/control.rs` (it matches on the orchestrator error).

`crates/datamancer-client/src/protocol/uds.rs`: move `Request` and `Reply` from `control.rs` verbatim, importing `crate::spec::{AssetClassCfg, EventKindCfg, SubscriptionSpec, UnsubscribeSpec}` and `datamancer_core::SystemSnapshot` (the daemon's `datamancer::SystemSnapshot` is a re-export of the same type). Change `Request::Unsubscribe`'s inline fields to `#[serde(flatten)] spec: UnsubscribeSpec` and update the one daemon match site accordingly. Move the module-doc from `control.rs` too. **Do not move** `Reply::from_library_error` (it needs `datamancer::Error`; it becomes a daemon-side helper — see Step 4). Move the protocol round-trip tests from `control.rs` (they exercise only the moved types; the `error_code` mapping test stays in the daemon).

`crates/datamancer-client/src/protocol/ws.rs`: move `WsRequest`, `WsReply`, and `WsRequest::id()` from `crates/datamancerd/src/ws/protocol.rs` verbatim, same treatment: `Unsubscribe` gets `#[serde(flatten)] spec: UnsubscribeSpec`; `WsReply::from_library_error` stays daemon-side. Move the WS protocol tests (including the UDS/WS `SubscriptionSpec` parity test, which now lives naturally next to both types).

- [ ] **Step 4: Rewire the daemon**

`crates/datamancerd/Cargo.toml`: add

```toml
datamancer-client = { path = "../datamancer-client", default-features = false }
```

`crates/datamancerd/src/config.rs`: delete the moved enums and `From` impls; add

```rust
pub use datamancer_client::spec::{AssetClassCfg, EventKindCfg, PersistenceCfg, ScopeCfg};
```

and replace `PersistenceCfg::options()` with a free function in `config.rs`:

```rust
/// Map a persistence preset to the library [`PersistenceOptions`]. Lives here
/// (not on the moved enum) because the target type is the orchestrator's.
#[must_use]
pub fn persistence_options(cfg: PersistenceCfg) -> PersistenceOptions {
    match cfg {
        PersistenceCfg::None => PersistenceOptions::none(),
        PersistenceCfg::Cached => PersistenceOptions::cached(),
        PersistenceCfg::CachedWithTap => PersistenceOptions::cached().with_tap_log(true),
        PersistenceCfg::ReadOnly => PersistenceOptions::read_only(),
        PersistenceCfg::Refresh => PersistenceOptions::refresh(),
        PersistenceCfg::TapOnly => PersistenceOptions::none().with_tap_log(true),
    }
}
```

Update the call sites: `spec.persistence.options()` → `crate::config::persistence_options(spec.persistence)` in `crates/datamancerd/src/server.rs` and `crates/datamancerd/src/ws/conn.rs`.

`crates/datamancerd/src/control.rs`: shrink to (a) `pub use datamancer_client::codes;`, (b) `pub use datamancer_client::protocol::uds::{Reply, Request};`, (c) `pub use datamancer_client::spec::SubscriptionSpec;`, (d) the retained `error_code` fn and its test, (e) a new daemon-side helper replacing the moved constructor:

```rust
/// An error reply derived from a library error (stable code + display).
#[must_use]
pub fn reply_from_library_error(err: &datamancer::Error) -> Reply {
    Reply::error(error_code(err), err.to_string())
}
```

Update `Reply::from_library_error(...)` call sites in `server.rs` to `reply_from_library_error(...)`, and add the equivalent `ws_reply_from_library_error(id, err)` in `crates/datamancerd/src/ws/protocol.rs` for `WsReply` (updating `ws/conn.rs` call sites). Fix the `Request::Unsubscribe { spec }` match site in `server.rs` and the `WsRequest::Unsubscribe { spec, .. }` site in `ws/conn.rs` (fields now come from `spec.provider`, `spec.asset_class`, etc.).

- [ ] **Step 5: Run the full daemon + client test suites**

Run: `cargo test -p datamancer-client && cargo test -p datamancerd --features ws && cargo clippy --all-targets --features "datamancerd/ws" -- -D warnings 2>/dev/null || cargo clippy -p datamancer-client -p datamancerd --all-targets -- -D warnings`
Expected: all moved protocol tests pass in `datamancer-client`; all remaining daemon tests pass; no warnings. The moved tests passing **unchanged** is the wire-compatibility guard.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/datamancer-client crates/datamancerd
git commit -m "refactor: extract control vocabulary into datamancer-client crate"
```

---

### Task 4: `instruments` op on both control surfaces

**Files:**
- Modify: `crates/datamancer-client/src/protocol/uds.rs` (new `Request` variant + `Reply` field/constructor)
- Modify: `crates/datamancer-client/src/protocol/ws.rs` (new `WsRequest` variant + `WsReply` field/constructor)
- Modify: `crates/datamancerd/src/server.rs` (off-actor dispatch: pass `Datamancer` into the accept loop)
- Modify: `crates/datamancerd/src/ws/conn.rs` (dispatch arm)
- Modify: `crates/datamancerd/README.md` (document the op)

**Interfaces:**
- Consumes: `Datamancer::instrument_catalog` (Task 2), moved protocol types (Task 3).
- Produces: `Request::Instruments { provider: Option<String> }`; `Reply { instruments: Option<Vec<InstrumentInfo>>, .. }` + `Reply::instruments(Vec<InstrumentInfo>) -> Reply`; `WsRequest::Instruments { id: u64, provider: Option<String> }`; `WsReply { instruments: Option<Vec<InstrumentInfo>>, .. }` + `WsReply::instruments(id, Vec<InstrumentInfo>)`. Wire op string: `"instruments"` on both surfaces.

- [ ] **Step 1: Write the failing protocol tests**

In `crates/datamancer-client/src/protocol/uds.rs` tests:

```rust
#[test]
fn instruments_request_parses_with_and_without_filter() {
    let filtered: Request =
        serde_json::from_str(r#"{"op":"instruments","provider":"alpaca-crypto"}"#).unwrap();
    assert!(matches!(filtered, Request::Instruments { provider: Some(p) } if p == "alpaca-crypto"));
    let all: Request = serde_json::from_str(r#"{"op":"instruments"}"#).unwrap();
    assert!(matches!(all, Request::Instruments { provider: None }));
}

#[test]
fn instruments_reply_round_trips() {
    use datamancer_core::{AssetClass, EventKind, Instrument, InstrumentInfo, ProviderId};
    let reply = Reply::instruments(vec![InstrumentInfo {
        instrument: Instrument::new(
            ProviderId::from_static("alpaca-crypto"),
            AssetClass::Crypto,
            "BTC/USD",
        ),
        kinds: vec![EventKind::Trade],
    }]);
    let line = serde_json::to_string(&reply).unwrap();
    let back: Reply = serde_json::from_str(&line).unwrap();
    assert_eq!(reply, back);
    assert!(back.ok);
    assert_eq!(back.instruments.unwrap().len(), 1);
}
```

In `crates/datamancer-client/src/protocol/ws.rs` tests (parity with UDS):

```rust
#[test]
fn ws_instruments_parses_and_carries_id() {
    let req: WsRequest =
        serde_json::from_str(r#"{"id":4,"op":"instruments","provider":"alpaca-crypto"}"#).unwrap();
    assert!(matches!(&req, WsRequest::Instruments { id: 4, provider: Some(p) } if p == "alpaca-crypto"));
    let all: WsRequest = serde_json::from_str(r#"{"id":5,"op":"instruments"}"#).unwrap();
    assert!(matches!(all, WsRequest::Instruments { id: 5, provider: None }));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p datamancer-client instruments`
Expected: FAIL to compile — variants/fields not defined.

- [ ] **Step 3: Add the vocabulary**

`uds.rs` — new `Request` variant (note: `#[serde(default)]` **without** `skip_serializing_if`, so the daemon's `reject_unknown_keys` canonicalization still contains the `provider` key when the caller sent one):

```rust
/// Enumerate available instruments and their supported kinds, optionally
/// restricted to one provider (a full equities catalog is ~10k rows —
/// prefer the filter).
Instruments {
    #[serde(default)]
    provider: Option<String>,
},
```

`Reply` gains a field (after `snapshot`) and a constructor:

```rust
/// The instrument catalog (on `instruments`).
#[serde(default, skip_serializing_if = "Option::is_none")]
pub instruments: Option<Vec<datamancer_core::InstrumentInfo>>,
```

```rust
/// Success carrying the instrument catalog.
#[must_use]
pub fn instruments(catalog: Vec<datamancer_core::InstrumentInfo>) -> Self {
    Self {
        instruments: Some(catalog),
        ..Self::ok()
    }
}
```

(Every existing `Reply` literal constructor gains `instruments: None`.)

`ws.rs` — mirror: `WsRequest::Instruments { id: u64, #[serde(default)] provider: Option<String> }` (add the arm to `WsRequest::id()`), `WsReply.instruments` field, `WsReply::instruments(id, catalog)` constructor.

- [ ] **Step 4: Run protocol tests**

Run: `cargo test -p datamancer-client && cargo test -p datamancerd`
Expected: PASS (daemon compiles because `Reply`/`WsReply` construction goes through the constructors).

- [ ] **Step 5: Wire the daemon dispatch — UDS off-actor**

The UDS dispatcher is a single-actor loop; a catalog request awaits live provider REST and must not stall unrelated control traffic. `Datamancer` is `Clone`, so dispatch it **in the connection task**, before the actor forward. In `crates/datamancerd/src/server.rs`:

1. Thread `dm: Datamancer` into the accept path: `accept_loop(listener, cmd_tx, dm.clone())`, `handle_connection(stream, cmd_tx.clone(), dm.clone())` (adjust both signatures).
2. In `handle_connection`, after `reject_unknown_keys` passes and before the `ServerCommand` forward, intercept:

```rust
let reply = if let Request::Instruments { provider } = &request {
    let filter = provider.clone().map(datamancer::ProviderId::new);
    match dm.instrument_catalog(filter.as_ref()).await {
        Ok(catalog) => Reply::instruments(catalog),
        Err(e) => reply_from_library_error(&e),
    }
} else {
    /* existing oneshot/actor forwarding block, unchanged */
};
```

3. WS side, in `crates/datamancerd/src/ws/conn.rs` `dispatch`, add the arm (per-connection tasks are naturally off-actor):

```rust
WsRequest::Instruments { provider, .. } => {
    let filter = provider.map(ProviderId::new);
    match dm.instrument_catalog(filter.as_ref()).await {
        Ok(catalog) => WsReply::instruments(id, catalog),
        Err(e) => ws_reply_from_library_error(id, &e),
    }
}
```

4. Document the op in `crates/datamancerd/README.md` next to `snapshot`, showing the request/reply JSON from Step 1's tests.

- [ ] **Step 6: Run the daemon suite**

Run: `cargo test -p datamancerd --features ws && cargo clippy -p datamancerd --features ws --all-targets -- -D warnings`
Expected: PASS, no warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/datamancer-client crates/datamancerd
git commit -m "feat(datamancerd): instruments discovery op on UDS and WS control surfaces"
```

---

### Task 5: `ClientError` + the `Client` trait

**Files:**
- Create: `crates/datamancer-client/src/error.rs`
- Create: `crates/datamancer-client/src/client.rs`
- Modify: `crates/datamancer-client/src/lib.rs` (module decls + re-exports)

**Interfaces:**
- Consumes: `datamancer_core::{MarketEvent, SystemSnapshot, InstrumentInfo, ProviderId}`; `spec::{SubscriptionSpec, UnsubscribeSpec}`.
- Produces: `pub enum ClientError<E> { Control { code: String, message: String }, Transport(E) }`; `pub trait Client` exactly as below. Both re-exported at crate root.

- [ ] **Step 1: Write the failing test**

In `crates/datamancer-client/src/client.rs` (test module written first; the trait is compile-checked by implementing it on a trivial fake):

```rust
#[cfg(test)]
mod tests {
    use super::Client;
    use crate::error::ClientError;
    use crate::spec::{SubscriptionSpec, UnsubscribeSpec};
    use datamancer_core::{InstrumentInfo, MarketEvent, ProviderId, SystemSnapshot};
    use futures::stream::{self, Empty};

    #[derive(Debug, thiserror::Error)]
    #[error("never")]
    struct NeverError;

    struct FakeClient;

    impl Client for FakeClient {
        type Config = ();
        type Error = NeverError;
        type Events = Empty<MarketEvent>;

        async fn connect(
            (): Self::Config,
        ) -> Result<(Self, Self::Events), ClientError<Self::Error>> {
            Ok((FakeClient, stream::empty()))
        }
        async fn subscribe(
            &mut self,
            _spec: &SubscriptionSpec,
        ) -> Result<(), ClientError<Self::Error>> {
            Ok(())
        }
        async fn unsubscribe(
            &mut self,
            _spec: &UnsubscribeSpec,
        ) -> Result<(), ClientError<Self::Error>> {
            Err(ClientError::Control {
                code: crate::codes::NOT_SUBSCRIBED.to_string(),
                message: "not subscribed".to_string(),
            })
        }
        async fn snapshot(&mut self) -> Result<SystemSnapshot, ClientError<Self::Error>> {
            Err(ClientError::Transport(NeverError))
        }
        async fn instruments(
            &mut self,
            _provider: Option<&ProviderId>,
        ) -> Result<Vec<InstrumentInfo>, ClientError<Self::Error>> {
            Ok(Vec::new())
        }
        async fn close(self) -> Result<(), ClientError<Self::Error>> {
            Ok(())
        }
    }

    /// The generic consumer shape the trait exists to make possible: code
    /// written once against `C: Client`, transport chosen by type.
    async fn generic_consumer<C: Client>(cfg: C::Config) -> Result<(), ClientError<C::Error>> {
        let (mut client, _events) = C::connect(cfg).await?;
        client.instruments(None).await?;
        client.close().await
    }

    #[tokio::test]
    async fn trait_supports_generic_consumers() {
        generic_consumer::<FakeClient>(()).await.expect("fake ok");
    }

    #[tokio::test]
    async fn control_errors_carry_the_stable_code() {
        let (mut client, _events) = FakeClient::connect(()).await.unwrap();
        match client
            .unsubscribe(&serde_json::from_str::<UnsubscribeSpec>(
                r#"{"provider":"p","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
            )
            .unwrap())
            .await
        {
            Err(ClientError::Control { code, .. }) => {
                assert_eq!(code, crate::codes::NOT_SUBSCRIBED);
            }
            other => panic!("expected Control error, got {other:?}"),
        }
    }
}
```

(dev-dependency note: `tokio` with `macros`/`rt-multi-thread` is already in `[dev-dependencies]` from Task 3.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p datamancer-client client`
Expected: FAIL to compile — `Client`, `ClientError` not defined.

- [ ] **Step 3: Implement**

`crates/datamancer-client/src/error.rs`:

```rust
//! The two-layer client error: control-plane rejections are normalized to the
//! stable [`crate::codes`] vocabulary identically across transports (they are
//! the daemon's contract); only genuine transport failures are the
//! per-implementation `E`.

/// Error from a [`crate::Client`] operation.
#[derive(Debug, thiserror::Error)]
pub enum ClientError<E: std::error::Error> {
    /// The daemon rejected the request. `code` is one of the stable
    /// [`crate::codes`] strings; identical across transports.
    #[error("daemon rejected request ({code}): {message}")]
    Control { code: String, message: String },
    /// The transport itself failed (socket, handshake, shared-memory attach,
    /// codec).
    #[error(transparent)]
    Transport(#[from] E),
}
```

`crates/datamancer-client/src/client.rs`:

```rust
//! The generic client-transport trait: one multiplexed consumer handle,
//! transport chosen at compile time.

use datamancer_core::{InstrumentInfo, MarketEvent, ProviderId, SystemSnapshot};
use futures::Stream;

use crate::error::ClientError;
use crate::spec::{SubscriptionSpec, UnsubscribeSpec};

/// A connected datamancerd client, generic over transport.
///
/// # Contract (upheld by every implementation)
///
/// - **One connection = one client = one multiplexed stream**, ordered by
///   `(instrument, seq)`; per-instrument demux is the consumer's concern.
/// - The timestamp triple (`source_ts`, `seq`, `rx_ts`) crosses verbatim;
///   `rx_ts` is observability-only and never synthesized client-side.
/// - Control rejections surface as [`ClientError::Control`] with the stable
///   [`crate::codes`] strings — identical across transports.
/// - **Loss is never silent.** On iceoryx2, resume-buffer overflow surfaces
///   in-band as `Control::Gap` (a numbered `seq` hole). On WebSocket, a slow
///   consumer is disconnected — the stream ends. A stream that ends after a
///   `SessionClosing` control closed gracefully; one that ends without it
///   lost its connection. Reconnect policy is the consumer's choice.
/// - Connection-scoped provider controls are suppressed from the stream;
///   read connectivity from [`Client::snapshot`].
pub trait Client: Sized + Send {
    /// Per-transport connection parameters (URL/token vs socket-path/name).
    type Config;
    /// Transport-layer failure type. Control rejections are **not** this —
    /// they are [`ClientError::Control`].
    type Error: std::error::Error + Send + 'static;
    /// The multiplexed event stream, yielded in delivery order.
    type Events: Stream<Item = MarketEvent> + Send + Unpin;

    /// Connect and return the split pair: the control handle and the owned
    /// event stream, separate values so a consumer can drain events while
    /// issuing control calls.
    fn connect(
        cfg: Self::Config,
    ) -> impl Future<Output = Result<(Self, Self::Events), ClientError<Self::Error>>> + Send;

    /// Add a subscription to this client's set.
    fn subscribe(
        &mut self,
        spec: &SubscriptionSpec,
    ) -> impl Future<Output = Result<(), ClientError<Self::Error>>> + Send;

    /// Remove a subscription from this client's set.
    fn unsubscribe(
        &mut self,
        spec: &UnsubscribeSpec,
    ) -> impl Future<Output = Result<(), ClientError<Self::Error>>> + Send;

    /// The daemon's current diagnostics snapshot (provider connectivity,
    /// latency, gap counts). This is where connection-scoped provider state
    /// lives — it is deliberately not on the event stream.
    fn snapshot(
        &mut self,
    ) -> impl Future<Output = Result<SystemSnapshot, ClientError<Self::Error>>> + Send;

    /// The instrument catalog: which instruments each provider serves and
    /// which event kinds each supports. Pass `provider` to bound the reply
    /// (a full equities catalog is ~10k rows).
    fn instruments(
        &mut self,
        provider: Option<&ProviderId>,
    ) -> impl Future<Output = Result<Vec<InstrumentInfo>, ClientError<Self::Error>>> + Send;

    /// Graceful close: the daemon emits a terminal `SessionClosing` on the
    /// event stream and tears the client down.
    fn close(self) -> impl Future<Output = Result<(), ClientError<Self::Error>>> + Send;
}
```

`lib.rs` gains:

```rust
mod client;
mod error;

pub use client::Client;
pub use error::ClientError;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p datamancer-client && cargo clippy -p datamancer-client --all-targets -- -D warnings`
Expected: PASS, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-client
git commit -m "feat(datamancer-client): ClientError + generic Client trait"
```

---

### Task 6: `WsClient`

**Files:**
- Create: `crates/datamancer-client/src/ws.rs`
- Modify: `crates/datamancer-client/src/lib.rs` (`#[cfg(feature = "ws")] pub mod ws;`)

**Interfaces:**
- Consumes: `Client`/`ClientError` (Task 5), `protocol::ws::{WsRequest, WsReply}` (Tasks 3–4), `datamancer_transport_ws::{EventFrame, from_wire}`.
- Produces: `ws::{WsClient, WsConfig, WsClientError}`; `WsClient: Client<Config = WsConfig, Error = WsClientError, Events = tokio_stream::wrappers::ReceiverStream<MarketEvent>>`.

- [ ] **Step 1: Write the failing tests**

Tests live in `ws.rs` and run against an in-process tungstenite server on an ephemeral port (no daemon). The fake server accepts one connection, then follows a scripted role: parse each inbound `WsRequest`, send scripted `WsReply`/event frames.

```rust
#[cfg(test)]
mod tests {
    use super::{WsClient, WsConfig};
    use crate::client::Client;
    use crate::error::ClientError;
    use crate::protocol::ws::{WsReply, WsRequest};
    use crate::spec::SubscriptionSpec;
    use datamancer_core::{MarketEvent, Price, Seq, Timestamp};
    use datamancer_transport_ws::{EventFrame, to_wire};
    use futures::{SinkExt as _, StreamExt as _};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message;

    /// Spawn a fake daemon endpoint: accepts one WS connection and hands the
    /// stream to `role`. Returns the `ws://` URL.
    async fn fake_server<F, Fut>(role: F) -> String
    where
        F: FnOnce(
                tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
            ) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            role(ws).await;
        });
        format!("ws://{addr}")
    }

    fn cfg(url: String) -> WsConfig {
        WsConfig {
            url,
            auth_token: None,
            event_buffer: 64,
        }
    }

    fn trade() -> MarketEvent {
        use datamancer_core::{AssetClass, Instrument, ProviderId, Trade};
        MarketEvent::Trade(Trade {
            instrument: Instrument::new(
                ProviderId::from_static("alpaca-crypto"),
                AssetClass::Crypto,
                "BTC/USD",
            ),
            source_ts: Timestamp(111),
            rx_ts: Timestamp(222),
            seq: Seq(7),
            price: Price(123_456),
            size: 99,
        })
    }

    #[tokio::test]
    async fn subscribe_correlates_reply_and_events_flow() {
        let url = fake_server(|mut ws| async move {
            // Expect a subscribe; ack it; then push one event frame.
            let Some(Ok(Message::Text(text))) = ws.next().await else {
                panic!("expected subscribe frame")
            };
            let req: WsRequest = serde_json::from_str(&text).unwrap();
            let WsRequest::Subscribe { id, spec } = req else {
                panic!("expected subscribe")
            };
            assert_eq!(spec.symbol, "BTC/USD");
            // Interleave: event frame BEFORE the reply — correlation must
            // still resolve, and the event must land on the stream.
            let frame = to_wire(&trade()).unwrap();
            ws.send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
                .await
                .unwrap();
            ws.send(Message::Text(
                serde_json::to_string(&WsReply::ok(id)).unwrap().into(),
            ))
            .await
            .unwrap();
            // Hold the socket open until the client is done.
            let _ = ws.next().await;
        })
        .await;

        let (mut client, mut events) = WsClient::connect(cfg(url)).await.expect("connect");
        let spec: SubscriptionSpec = serde_json::from_str(
            r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
        )
        .unwrap();
        client.subscribe(&spec).await.expect("subscribe acked");

        let ev = events.next().await.expect("one event");
        assert_eq!(ev, trade()); // timestamp triple verbatim, price intact
    }

    #[tokio::test]
    async fn error_reply_maps_to_control_error() {
        let url = fake_server(|mut ws| async move {
            let Some(Ok(Message::Text(text))) = ws.next().await else {
                panic!("expected frame")
            };
            let req: WsRequest = serde_json::from_str(&text).unwrap();
            ws.send(Message::Text(
                serde_json::to_string(&WsReply::error(
                    req.id(),
                    crate::codes::DUPLICATE_SUBSCRIPTION,
                    "already subscribed",
                ))
                .unwrap()
                .into(),
            ))
            .await
            .unwrap();
            let _ = ws.next().await;
        })
        .await;

        let (mut client, _events) = WsClient::connect(cfg(url)).await.expect("connect");
        let spec: SubscriptionSpec = serde_json::from_str(
            r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
        )
        .unwrap();
        match client.subscribe(&spec).await {
            Err(ClientError::Control { code, .. }) => {
                assert_eq!(code, crate::codes::DUPLICATE_SUBSCRIPTION);
            }
            other => panic!("expected Control error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bearer_token_is_sent_on_the_handshake() {
        // Raw TCP accept: read the HTTP upgrade request and assert the header
        // before completing the handshake.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_hdr_async(tcp, |req: &_, resp| {
                let auth = req
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                assert_eq!(auth, "Bearer s3cr3t");
                Ok(resp)
            })
            .await
            .unwrap();
            drop(ws);
        });
        let cfg = WsConfig {
            url: format!("ws://{addr}"),
            auth_token: Some("s3cr3t".to_string()),
            event_buffer: 8,
        };
        let _ = WsClient::connect(cfg).await; // may error on immediate drop; header assert is the test
        server.await.unwrap();
    }

    #[tokio::test]
    async fn server_drop_ends_the_event_stream() {
        let url = fake_server(|ws| async move {
            drop(ws); // immediate close
        })
        .await;
        let (_client, mut events) = WsClient::connect(cfg(url)).await.expect("connect");
        assert!(events.next().await.is_none(), "stream ends on connection loss");
    }

    #[tokio::test]
    async fn close_sends_close_client_and_awaits_ack() {
        let url = fake_server(|mut ws| async move {
            let Some(Ok(Message::Text(text))) = ws.next().await else {
                panic!("expected frame")
            };
            let req: WsRequest = serde_json::from_str(&text).unwrap();
            assert!(matches!(req, WsRequest::CloseClient { .. }));
            ws.send(Message::Text(
                serde_json::to_string(&WsReply::ok(req.id())).unwrap().into(),
            ))
            .await
            .unwrap();
        })
        .await;
        let (client, _events) = WsClient::connect(cfg(url)).await.expect("connect");
        client.close().await.expect("close acked");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p datamancer-client --features ws ws::`
Expected: FAIL to compile — `WsClient`, `WsConfig` not defined.

- [ ] **Step 3: Implement `WsClient`**

`crates/datamancer-client/src/ws.rs`:

```rust
//! The WebSocket client: one socket carries control requests, correlated
//! replies, and event frames. A reader task demuxes inbound frames — replies
//! resolve pending requests by correlation `id`; event frames decode through
//! the transport crate's `from_wire` (one wire definition) onto a bounded
//! channel that backs the event stream.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datamancer_core::{InstrumentInfo, MarketEvent, ProviderId, SystemSnapshot};
use datamancer_transport_ws::{EventFrame, from_wire};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt as _, StreamExt as _};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::client::Client;
use crate::error::ClientError;
use crate::protocol::ws::{WsReply, WsRequest};
use crate::spec::{SubscriptionSpec, UnsubscribeSpec};

/// Connection parameters for [`WsClient`].
#[derive(Debug, Clone)]
pub struct WsConfig {
    /// `ws://host:port` (TLS terminates at a reverse proxy; see the daemon's
    /// security posture).
    pub url: String,
    /// Optional shared bearer token, sent as `Authorization: Bearer …` on the
    /// handshake.
    pub auth_token: Option<String>,
    /// Bound on locally buffered, not-yet-consumed events. A consumer that
    /// falls behind past the daemon's own channel is disconnected by the
    /// daemon; this bound is the client-side mirror.
    pub event_buffer: usize,
}

/// Transport-layer failures for [`WsClient`].
#[derive(Debug, thiserror::Error)]
pub enum WsClientError {
    #[error("websocket error: {0}")]
    Socket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("codec error: {0}")]
    Codec(#[from] serde_json::Error),
    #[error("invalid config: {0}")]
    Config(String),
    #[error("connection closed before the reply arrived")]
    ConnectionClosed,
}

type WriteHalf = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<WsReply>>>>;

/// A connected WebSocket client. See [`Client`] for the transport-agnostic
/// contract.
pub struct WsClient {
    write: WriteHalf,
    pending: Pending,
    next_id: u64,
}

/// Inbound frame demux: event frames are internally tagged (`"type"`), replies
/// carry `"id"`/`"ok"` — the untagged union tries in that order.
#[derive(Deserialize)]
#[serde(untagged)]
enum Inbound {
    Event(EventFrame),
    Reply(WsReply),
}

async fn run_reader(
    mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    pending: Pending,
    events: mpsc::Sender<MarketEvent>,
) {
    while let Some(Ok(msg)) = read.next().await {
        let Message::Text(text) = msg else { continue };
        match serde_json::from_str::<Inbound>(&text) {
            Ok(Inbound::Event(frame)) => {
                if events.send(from_wire(&frame)).await.is_err() {
                    break; // consumer dropped the stream
                }
            }
            Ok(Inbound::Reply(reply)) => {
                if let Some(tx) = pending.lock().expect("pending poisoned").remove(&reply.id) {
                    let _ = tx.send(reply);
                }
            }
            // Unknown frame shape: a newer daemon speaking a newer wire.
            // Skipping (rather than erroring) keeps old clients readable.
            Err(_) => {}
        }
    }
    // Socket gone: fail every pending request and end the stream (the events
    // sender drops here, so the consumer's stream yields None).
    pending.lock().expect("pending poisoned").clear();
}

impl WsClient {
    async fn request(&mut self, req: &WsRequest) -> Result<WsReply, ClientError<WsClientError>> {
        let id = req.id();
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .expect("pending poisoned")
            .insert(id, tx);
        let json = serde_json::to_string(req).map_err(WsClientError::from)?;
        self.write
            .send(Message::Text(json.into()))
            .await
            .map_err(WsClientError::from)?;
        let reply = rx
            .await
            .map_err(|_| ClientError::Transport(WsClientError::ConnectionClosed))?;
        if reply.ok {
            Ok(reply)
        } else {
            Err(ClientError::Control {
                code: reply.code.unwrap_or_default(),
                message: reply.message.unwrap_or_default(),
            })
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

impl Client for WsClient {
    type Config = WsConfig;
    type Error = WsClientError;
    type Events = ReceiverStream<MarketEvent>;

    async fn connect(
        cfg: Self::Config,
    ) -> Result<(Self, Self::Events), ClientError<Self::Error>> {
        let mut request = cfg
            .url
            .as_str()
            .into_client_request()
            .map_err(WsClientError::from)?;
        if let Some(token) = &cfg.auth_token {
            let value = format!("Bearer {token}")
                .parse()
                .map_err(|_| WsClientError::Config("auth token is not a valid header value".to_string()))?;
            request.headers_mut().insert("authorization", value);
        }
        let (ws, _resp) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(WsClientError::from)?;
        let (write, read) = ws.split();
        let (ev_tx, ev_rx) = mpsc::channel(cfg.event_buffer.max(1));
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        tokio::spawn(run_reader(read, Arc::clone(&pending), ev_tx));
        Ok((
            WsClient {
                write,
                pending,
                next_id: 1,
            },
            ReceiverStream::new(ev_rx),
        ))
    }

    async fn subscribe(
        &mut self,
        spec: &SubscriptionSpec,
    ) -> Result<(), ClientError<Self::Error>> {
        let req = WsRequest::Subscribe {
            id: self.next_id(),
            spec: spec.clone(),
        };
        self.request(&req).await.map(|_| ())
    }

    async fn unsubscribe(
        &mut self,
        spec: &UnsubscribeSpec,
    ) -> Result<(), ClientError<Self::Error>> {
        let req = WsRequest::Unsubscribe {
            id: self.next_id(),
            spec: spec.clone(),
        };
        self.request(&req).await.map(|_| ())
    }

    async fn snapshot(&mut self) -> Result<SystemSnapshot, ClientError<Self::Error>> {
        let req = WsRequest::Snapshot { id: self.next_id() };
        let reply = self.request(&req).await?;
        reply.snapshot.ok_or_else(|| {
            ClientError::Transport(WsClientError::Config(
                "ok snapshot reply missing snapshot payload".to_string(),
            ))
        })
    }

    async fn instruments(
        &mut self,
        provider: Option<&ProviderId>,
    ) -> Result<Vec<InstrumentInfo>, ClientError<Self::Error>> {
        let req = WsRequest::Instruments {
            id: self.next_id(),
            provider: provider.map(|p| p.as_str().to_string()),
        };
        let reply = self.request(&req).await?;
        Ok(reply.instruments.unwrap_or_default())
    }

    async fn close(mut self) -> Result<(), ClientError<Self::Error>> {
        let req = WsRequest::CloseClient { id: self.next_id() };
        self.request(&req).await.map(|_| ())
    }
}
```

Note: `WsRequest::{Subscribe, Unsubscribe}` field shapes must match Task 3/4's protocol module (`spec` flattened). If the trait-bound checker rejects `impl Future` + `async fn` mixing, use `async fn` directly in the `impl Client for WsClient` block — RPITIT accepts `async fn` as an implementation of an `impl Future`-returning trait method when the `Send` bound is satisfied; if the compiler disagrees on `Send` inference, desugar the impl methods to `fn … -> impl Future … + Send { async move { … } }`.

Add to `lib.rs`:

```rust
#[cfg(feature = "ws")]
pub mod ws;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p datamancer-client --features ws && cargo clippy -p datamancer-client --features ws --all-targets -- -D warnings`
Expected: 5 new tests PASS, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-client
git commit -m "feat(datamancer-client): WsClient — tungstenite Client implementation"
```

---

### Task 7: `Iceoryx2Client`

**Files:**
- Create: `crates/datamancer-client/src/iox2.rs`
- Modify: `crates/datamancer-client/src/lib.rs` (`#[cfg(feature = "iceoryx2")] pub mod iox2;`)

**Interfaces:**
- Consumes: `Client`/`ClientError` (Task 5), `protocol::uds::{Request, Reply}` (Tasks 3–4), `datamancer_transport_iceoryx2::DataSubscriber`, iceoryx2 `Node`/`NodeBuilder` (0.9.2, `ipc_threadsafe`).
- Produces: `iox2::{Iceoryx2Client, Iceoryx2Config, Iceoryx2ClientError}`; `Iceoryx2Client: Client<Config = Iceoryx2Config, Error = Iceoryx2ClientError, Events = tokio_stream::wrappers::ReceiverStream<MarketEvent>>`.

Design notes for the implementer:
- The UDS control protocol is strictly one-request-one-reply per connection, so the control half is a plain `(write_half, buffered_lines)` pair used serially from `&mut self` — no correlation ids.
- The shm poll loop runs on `tokio::task::spawn_blocking` (the `DataSubscriber` poll is sync); it owns the iceoryx2 `Node` (keeping it alive) and forwards events with `blocking_send`. It exits when the events receiver is dropped (send fails) or the stop flag is set by `close()`.
- The pure protocol pieces (open-client handshake, service-name → `client_id` parse, error mapping) are testable against a scripted fake UDS daemon with **no iceoryx2 runtime**; only the full attach needs the gated test (Task 9 covers it end-to-end).

- [ ] **Step 1: Write the failing tests**

In `iox2.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::{parse_client_id, ControlConn, Iceoryx2ClientError};
    use crate::codes;
    use crate::error::ClientError;
    use crate::protocol::uds::{Reply, Request};
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    use tokio::net::UnixListener;

    #[test]
    fn client_id_parses_from_the_service_name() {
        assert_eq!(parse_client_id("datamancer/data/3").unwrap(), 3);
        assert_eq!(parse_client_id("datamancer/data/40").unwrap(), 40);
        assert!(parse_client_id("datamancer/data/").is_err());
        assert!(parse_client_id("nonsense").is_err());
        assert!(parse_client_id("datamancer/data/not-a-number").is_err());
    }

    /// Scripted fake UDS daemon: reads one request line, sends one reply line.
    async fn fake_uds(replies: Vec<Reply>) -> std::path::PathBuf {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("control.sock");
        let listener = UnixListener::bind(&path).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            for reply in replies {
                let _ = lines.next_line().await.unwrap();
                let mut buf = serde_json::to_vec(&reply).unwrap();
                buf.push(b'\n');
                write.write_all(&buf).await.unwrap();
            }
        });
        path
    }

    #[tokio::test]
    async fn control_conn_round_trips_a_request() {
        let path = fake_uds(vec![Reply::service("datamancer/data/7")]).await;
        let mut conn = ControlConn::connect(&path).await.unwrap();
        let reply = conn
            .request(&Request::OpenClient {
                client: "test-client".to_string(),
                subscriptions: vec![],
            })
            .await
            .unwrap();
        assert!(reply.ok);
        assert_eq!(reply.service.as_deref(), Some("datamancer/data/7"));
    }

    #[tokio::test]
    async fn control_error_reply_maps_to_control_error() {
        let path = fake_uds(vec![Reply::error(codes::DUPLICATE_CLIENT, "name in use")]).await;
        let mut conn = ControlConn::connect(&path).await.unwrap();
        let reply = conn
            .request(&Request::OpenClient {
                client: "taken".to_string(),
                subscriptions: vec![],
            })
            .await
            .unwrap();
        match super::check(reply) {
            Err(ClientError::<Iceoryx2ClientError>::Control { code, .. }) => {
                assert_eq!(code, codes::DUPLICATE_CLIENT);
            }
            other => panic!("expected Control error, got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p datamancer-client --features iceoryx2 iox2::`
Expected: FAIL to compile — module contents not defined.

- [ ] **Step 3: Implement `Iceoryx2Client`**

`crates/datamancer-client/src/iox2.rs`:

```rust
//! The same-host iceoryx2 client: bundles the three attaches a consumer
//! previously hand-assembled — the UDS control connection (newline-JSON
//! `open-client`/`subscribe`/…), the shared-memory data + announcement
//! subscriber, and (via the UDS `snapshot` op) diagnostics — behind one
//! [`Client`] handle. The transport crate's `DataSubscriber` and the
//! diagnostics-plane subscriber remain public as lower-level escape hatches.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use datamancer_core::{InstrumentInfo, MarketEvent, ProviderId, SystemSnapshot};
use datamancer_transport_iceoryx2::DataSubscriber;
use iceoryx2::prelude::{NodeBuilder, ipc_threadsafe};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::client::Client;
use crate::error::ClientError;
use crate::protocol::uds::{Reply, Request};
use crate::spec::{SubscriptionSpec, UnsubscribeSpec};

/// Connection parameters for [`Iceoryx2Client`].
#[derive(Debug, Clone)]
pub struct Iceoryx2Config {
    /// Path to datamancerd's UDS control socket.
    pub control_socket: PathBuf,
    /// This client's name for `open-client` (unique per daemon).
    pub client_name: String,
    /// Sleep between empty shm polls. The poll loop drains everything
    /// available each pass, so this bounds added latency only when idle.
    pub poll_interval: Duration,
    /// Bound on locally buffered, not-yet-consumed events.
    pub event_buffer: usize,
}

/// Transport-layer failures for [`Iceoryx2Client`].
#[derive(Debug, thiserror::Error)]
pub enum Iceoryx2ClientError {
    #[error("control socket i/o: {0}")]
    Io(#[from] std::io::Error),
    #[error("control codec: {0}")]
    Codec(#[from] serde_json::Error),
    #[error("control protocol: {0}")]
    Protocol(String),
    #[error("iceoryx2 transport: {0}")]
    Transport(#[from] datamancer_transport_iceoryx2::TransportError),
    #[error("iceoryx2 node: {0}")]
    Node(String),
}

/// Extract the numeric client id from the `open-client` reply's service name
/// (`datamancer/data/{id}`).
fn parse_client_id(service: &str) -> Result<u64, Iceoryx2ClientError> {
    service
        .strip_prefix("datamancer/data/")
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| {
            Iceoryx2ClientError::Protocol(format!("unparseable data-service name: {service}"))
        })
}

/// Map a control [`Reply`] to the two-layer error model.
fn check(reply: Reply) -> Result<Reply, ClientError<Iceoryx2ClientError>> {
    if reply.ok {
        Ok(reply)
    } else {
        Err(ClientError::Control {
            code: reply.code.unwrap_or_default(),
            message: reply.message.unwrap_or_default(),
        })
    }
}

/// The serially-used UDS control connection (strict request→reply per line).
struct ControlConn {
    lines: Lines<BufReader<OwnedReadHalf>>,
    write: OwnedWriteHalf,
}

impl ControlConn {
    async fn connect(path: &Path) -> Result<Self, Iceoryx2ClientError> {
        let stream = UnixStream::connect(path).await?;
        let (read, write) = stream.into_split();
        Ok(Self {
            lines: BufReader::new(read).lines(),
            write,
        })
    }

    async fn request(&mut self, req: &Request) -> Result<Reply, Iceoryx2ClientError> {
        let mut buf = serde_json::to_vec(req)?;
        buf.push(b'\n');
        self.write.write_all(&buf).await?;
        let line = self.lines.next_line().await?.ok_or_else(|| {
            Iceoryx2ClientError::Protocol("control connection closed mid-request".to_string())
        })?;
        Ok(serde_json::from_str(&line)?)
    }
}

/// A connected same-host client. See [`Client`] for the transport-agnostic
/// contract; iceoryx2-specific behavior: loss surfaces **in-band** as
/// `Control::Gap` (the daemon's resume buffer numbers evictions), and the
/// event stream ends when the daemon drops the per-client services.
pub struct Iceoryx2Client {
    control: ControlConn,
    client_name: String,
    stop: Arc<AtomicBool>,
}

impl Client for Iceoryx2Client {
    type Config = Iceoryx2Config;
    type Error = Iceoryx2ClientError;
    type Events = ReceiverStream<MarketEvent>;

    async fn connect(
        cfg: Self::Config,
    ) -> Result<(Self, Self::Events), ClientError<Self::Error>> {
        let mut control = ControlConn::connect(&cfg.control_socket)
            .await
            .map_err(ClientError::Transport)?;
        let reply = control
            .request(&Request::OpenClient {
                client: cfg.client_name.clone(),
                subscriptions: vec![],
            })
            .await
            .map_err(ClientError::Transport)?;
        let reply = check(reply)?;
        let service = reply.service.ok_or_else(|| {
            ClientError::Transport(Iceoryx2ClientError::Protocol(
                "open-client reply missing service name".to_string(),
            ))
        })?;
        let client_id = parse_client_id(&service).map_err(ClientError::Transport)?;

        let (ev_tx, ev_rx) = mpsc::channel(cfg.event_buffer.max(1));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let poll_interval = cfg.poll_interval;
        // The poll loop owns the node (keeping the shm attach alive) and runs
        // on the blocking pool: `DataSubscriber::poll` is sync by design.
        tokio::task::spawn_blocking(move || {
            let node = match NodeBuilder::new().create::<ipc_threadsafe::Service>() {
                Ok(node) => node,
                Err(e) => {
                    tracing_or_eprintln(&format!("iceoryx2 node create failed: {e:?}"));
                    return;
                }
            };
            let mut subscriber = match DataSubscriber::open(&node, client_id) {
                Ok(s) => s,
                Err(e) => {
                    tracing_or_eprintln(&format!("iceoryx2 subscriber open failed: {e}"));
                    return;
                }
            };
            while !stop_flag.load(Ordering::Relaxed) {
                match subscriber.poll() {
                    Ok(events) if events.is_empty() => std::thread::sleep(poll_interval),
                    Ok(events) => {
                        for ev in events {
                            if ev_tx.blocking_send(ev).is_err() {
                                return; // consumer dropped the stream
                            }
                        }
                    }
                    Err(_) => return, // service gone: daemon dropped the client
                }
            }
        });

        Ok((
            Iceoryx2Client {
                control,
                client_name: cfg.client_name,
                stop,
            },
            ReceiverStream::new(ev_rx),
        ))
    }

    async fn subscribe(
        &mut self,
        spec: &SubscriptionSpec,
    ) -> Result<(), ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::Subscribe {
                client: self.client_name.clone(),
                spec: spec.clone(),
            })
            .await
            .map_err(ClientError::Transport)?;
        check(reply).map(|_| ())
    }

    async fn unsubscribe(
        &mut self,
        spec: &UnsubscribeSpec,
    ) -> Result<(), ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::Unsubscribe {
                client: self.client_name.clone(),
                spec: spec.clone(),
            })
            .await
            .map_err(ClientError::Transport)?;
        check(reply).map(|_| ())
    }

    async fn snapshot(&mut self) -> Result<SystemSnapshot, ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::Snapshot)
            .await
            .map_err(ClientError::Transport)?;
        let reply = check(reply)?;
        reply.snapshot.ok_or_else(|| {
            ClientError::Transport(Iceoryx2ClientError::Protocol(
                "ok snapshot reply missing snapshot payload".to_string(),
            ))
        })
    }

    async fn instruments(
        &mut self,
        provider: Option<&ProviderId>,
    ) -> Result<Vec<InstrumentInfo>, ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::Instruments {
                provider: provider.map(|p| p.as_str().to_string()),
            })
            .await
            .map_err(ClientError::Transport)?;
        let reply = check(reply)?;
        Ok(reply.instruments.unwrap_or_default())
    }

    async fn close(mut self) -> Result<(), ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::CloseClient {
                client: self.client_name.clone(),
            })
            .await
            .map_err(ClientError::Transport)?;
        self.stop.store(true, Ordering::Relaxed);
        check(reply).map(|_| ())
    }
}

/// The crate has no tracing dependency; startup failures in the blocking poll
/// task surface on stderr (they also surface to the consumer as an
/// immediately-ended event stream).
fn tracing_or_eprintln(msg: &str) {
    eprintln!("datamancer-client(iceoryx2): {msg}");
}
```

Adjust the `Request::Subscribe`/`Unsubscribe` field shapes to match Task 3's actual definitions (`Subscribe` keeps `client` + flattened `spec`; `Unsubscribe` is `client` + flattened `UnsubscribeSpec`). If `Request::Unsubscribe` after Task 3 has shape `{ client, spec }`, use that; the wire is identical either way.

Add to `lib.rs`:

```rust
#[cfg(feature = "iceoryx2")]
pub mod iox2;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p datamancer-client --features iceoryx2 && cargo clippy -p datamancer-client --all-features --all-targets -- -D warnings`
Expected: PASS (protocol/parse tests; no iceoryx2 runtime needed), no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-client
git commit -m "feat(datamancer-client): Iceoryx2Client — bundled UDS + shm Client implementation"
```

---

### Task 8: `datamancer` re-export features + docs

**Files:**
- Modify: `crates/datamancer/Cargo.toml` (features `client-ws`, `client-iceoryx2`)
- Modify: `crates/datamancer/src/lib.rs` (feature-gated `pub use datamancer_client as client;`)
- Create: `crates/datamancer-client/README.md`
- Create: `crates/datamancer-client/CLAUDE.md`
- Modify: `CLAUDE.md` (root — workspace description: five crates → six)
- Modify: `crates/datamancerd/README.md` (pointer to the client crate, if not already added in Task 4)

- [ ] **Step 1: Add the features**

`crates/datamancer/Cargo.toml`:

```toml
# under [features]
client-ws = ["dep:datamancer-client", "datamancer-client/ws"]
client-iceoryx2 = ["dep:datamancer-client", "datamancer-client/iceoryx2"]

# under [dependencies]
datamancer-client = { path = "../datamancer-client", optional = true, default-features = false }
```

`crates/datamancer/src/lib.rs`, next to the existing transport re-exports:

```rust
#[cfg(any(feature = "client-ws", feature = "client-iceoryx2"))]
pub use datamancer_client as client;
```

- [ ] **Step 2: Verify the feature matrix builds**

Run: `cargo build -p datamancer --features client-ws && cargo build -p datamancer --features client-iceoryx2 && cargo build -p datamancer --features "client-ws client-iceoryx2" && cargo build`
Expected: all four succeed (last one proves both features stay off by default).

- [ ] **Step 3: Write the docs**

`crates/datamancer-client/README.md`: what the crate is (consumer-side trait + vocabulary), the trait contract summary (copy the doc-comment contract from `client.rs`), a connect-and-subscribe example per transport, the stable-codes note, the loss-contract table (iceoryx2 `Gap` in-band vs WS disconnect), and the honest-scoping note carried over from the WS README (worked example, not a hardened endpoint).

`crates/datamancer-client/CLAUDE.md` (keyed to this crate's invariants):

```markdown
# datamancer-client

Consumer-side surface for datamancerd: the shared control vocabulary
(`spec`, `codes`, `protocol::{uds,ws}`) and, behind features `ws` /
`iceoryx2`, the two `Client` trait implementations.

## Invariants / stance

- **`#![forbid(unsafe_code)]`**, `[lints] workspace = true`.
- **Depends on `datamancer-core` + the transport crates only — never the
  `datamancer` orchestrator.** The orchestrator re-exports this crate
  (features `client-ws`/`client-iceoryx2`), not the reverse.
- **The vocabulary is the operator contract.** JSON shapes and stable code
  strings moved here verbatim from `datamancerd`; changing either is a
  breaking change guarded by the moved regression tests. `datamancerd`
  re-imports them — one definition.
- **The trait is generic (assoc types), not dyn.** Transport is a
  compile-time choice. Runtime selection is a consumer-side enum, deferred.
- **`connect` returns a split `(handle, events)` pair** so control calls and
  stream draining never contend.
- **Two-layer errors.** Daemon rejections → `ClientError::Control` with a
  stable code (identical across transports); only transport failures are the
  per-impl `Error` type.
- **Loss contract is documented, not normalized.** iceoryx2: in-band
  `Control::Gap`. WS: stream end. Graceful close is marked by a terminal
  `SessionClosing`. The client never synthesizes events (`rx_ts` included).
- **Pinned versions in lockstep:** tokio-tungstenite 0.29.0 and iceoryx2
  0.9.2 must match the transport crates and `datamancerd`.
```

Root `CLAUDE.md`: update "five crates" to six and add a `datamancer-client` bullet mirroring the transport-crate bullets (depends on core + transport crates; features `ws`/`iceoryx2` off by default; holds the extracted control vocabulary that `datamancerd` re-imports).

- [ ] **Step 4: Full-workspace check**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: green across the workspace with default features.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer crates/datamancer-client CLAUDE.md crates/datamancerd/README.md
git commit -m "feat(datamancer): client re-export features + client crate docs"
```

---

### Task 9: The generic `exercise` e2e — "end user doesn't care" stated executably

**Files:**
- Create: `crates/datamancerd/tests/client_transport_e2e.rs`
- Modify: `crates/datamancerd/Cargo.toml` (dev-dependency on `datamancer-client` with both features)

**Interfaces:**
- Consumes: everything — the daemon binary, both `Client` impls, the `instruments` op.

Notes: this test is gated `#[ignore]` like `daemon_e2e.rs`/`ws_e2e.rs` (needs a live iceoryx2 runtime + the spawned daemon binary). **Reuse the existing harness**: read `crates/datamancerd/tests/ws_e2e.rs` and `daemon_e2e.rs` first and copy their daemon-spawn/config-file/ready-wait helpers verbatim rather than inventing new ones — the WS test already generates a TOML config with `[ws]` enabled on an ephemeral port and waits for readiness; extend that config generation to also produce the UDS socket path the iceoryx2 client needs.

- [ ] **Step 1: Add the dev-dependency**

In `crates/datamancerd/Cargo.toml` `[dev-dependencies]`:

```toml
datamancer-client = { path = "../datamancer-client", features = ["ws", "iceoryx2"] }
```

- [ ] **Step 2: Write the generic exercise**

The heart of the file — one function, written once, run per transport:

```rust
use datamancer_client::{Client, ClientError};
use datamancer_client::spec::{SubscriptionSpec, UnsubscribeSpec};
use datamancer_core::{ControlKind, MarketEvent};
use futures::StreamExt as _;

/// The transport-agnosticism guarantee, stated executably: everything a
/// consumer does — discover, subscribe, receive with the timestamp triple
/// intact, snapshot, unsubscribe, close gracefully — through the trait,
/// with the concrete transport chosen only by the type parameter.
async fn exercise<C: Client>(cfg: C::Config) {
    let (mut client, mut events) = C::connect(cfg).await.expect("connect");

    // Discover: the catalog lists the fake provider's instrument with kinds.
    let catalog = client.instruments(None).await.expect("instruments");
    assert!(!catalog.is_empty(), "catalog must not be empty");
    let info = &catalog[0];
    assert!(!info.kinds.is_empty(), "kinds derived per instrument");

    // Subscribe to a listed instrument+kind (JSON spec matches the daemon's
    // fake-provider config used by the harness).
    let spec: SubscriptionSpec = serde_json::from_str(
        r#"{"provider":"fake","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
    )
    .unwrap();
    client.subscribe(&spec).await.expect("subscribe");

    // An event arrives with the triple verbatim (the harness's fake provider
    // emits deterministic trades; assert seq/source_ts/rx_ts are present and
    // rx_ts is not synthesized equal to source_ts).
    let ev = events.next().await.expect("first event");
    let MarketEvent::Trade(t) = &ev else {
        panic!("expected a trade first, got {ev:?}")
    };
    assert_ne!(t.rx_ts, t.source_ts, "rx_ts must be carried, not synthesized");

    // Connectivity via snapshot, not the stream.
    let snapshot = client.snapshot().await.expect("snapshot");
    assert!(!snapshot.providers.is_empty());

    // Duplicate subscribe surfaces the stable code — identically per transport.
    match client.subscribe(&spec).await {
        Err(ClientError::Control { code, .. }) => {
            assert_eq!(code, datamancer_client::codes::DUPLICATE_SUBSCRIPTION);
        }
        other => panic!("expected duplicate_subscription, got {other:?}"),
    }

    let unspec: UnsubscribeSpec = serde_json::from_str(
        r#"{"provider":"fake","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
    )
    .unwrap();
    client.unsubscribe(&unspec).await.expect("unsubscribe");

    // Graceful close: terminal SessionClosing, then the stream ends.
    client.close().await.expect("close");
    let mut saw_closing = false;
    while let Some(ev) = events.next().await {
        if matches!(ev, MarketEvent::Control(c) if matches!(c.kind, ControlKind::SessionClosing)) {
            saw_closing = true;
        }
    }
    assert!(saw_closing, "graceful close is marked by SessionClosing");
}

#[tokio::test]
#[ignore = "needs a live iceoryx2 runtime and the spawned daemon binary"]
async fn ws_client_passes_the_exercise() {
    let daemon = spawn_daemon().await; // harness helper from ws_e2e.rs
    exercise::<datamancer_client::ws::WsClient>(datamancer_client::ws::WsConfig {
        url: daemon.ws_url(),
        auth_token: daemon.auth_token(),
        event_buffer: 256,
    })
    .await;
}

#[tokio::test]
#[ignore = "needs a live iceoryx2 runtime and the spawned daemon binary"]
async fn iceoryx2_client_passes_the_exercise() {
    let daemon = spawn_daemon().await;
    exercise::<datamancer_client::iox2::Iceoryx2Client>(datamancer_client::iox2::Iceoryx2Config {
        control_socket: daemon.uds_path(),
        client_name: "exercise-iox2".to_string(),
        poll_interval: std::time::Duration::from_millis(5),
        event_buffer: 256,
    })
    .await;
}
```

`spawn_daemon()` and its accessors are the harness: port the config-generation/spawn/ready-wait code from `ws_e2e.rs` into this file (or a shared `tests/common/` module if both files can use it without churn). If the existing harness's fake/real provider setup can't emit a deterministic trade (e.g. `daemon_e2e` currently drives a real provider), adapt the exercise's event assertions to what the harness provides — the load-bearing asserts are: catalog non-empty with kinds, subscribe ok, one event with `rx_ts != source_ts`, snapshot non-empty, `DUPLICATE_SUBSCRIPTION` code, `SessionClosing` then stream end.

- [ ] **Step 3: Compile-check the gated tests**

Run: `cargo test -p datamancerd --features ws --test client_transport_e2e --no-run`
Expected: compiles (tests are `#[ignore]`d; this proves the generic fn type-checks against both impls — the trait's whole point).

- [ ] **Step 4: Run the gated tests (live iceoryx2 runtime required)**

Run: `cargo test -p datamancerd --features ws --test client_transport_e2e -- --ignored`
Expected: both transports PASS the identical exercise. If the environment lacks the iceoryx2 runtime, note it in the commit and flag for a manual run — do not delete or un-gate the tests.

- [ ] **Step 5: Full-workspace final check + commit**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check`

```bash
git add crates/datamancerd
git commit -m "test(datamancerd): generic client-transport exercise over both impls"
```

---

## Task order & dependencies

```
Task 1 (core) → Task 2 (orchestrator catalog) ┐
Task 3 (crate + vocabulary move) ─────────────┼→ Task 4 (instruments op)
                                              └→ Task 5 (trait) → Task 6 (WsClient)
                                                               → Task 7 (Iceoryx2Client)
Task 4,6,7 → Task 8 (re-exports + docs) → Task 9 (generic e2e)
```

Tasks 1–2 and 3 are independent and can be done in either order; everything else is sequential as drawn.
