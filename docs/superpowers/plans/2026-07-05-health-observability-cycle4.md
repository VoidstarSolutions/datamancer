# Health & Observability v1 (Cycle 4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Real health feeding `HealthView` v2 — disabled-provider and `Unauthenticated` states, per-symbol `Gapped`/`Backfilling` liveness, a daemon-side `health` UDS op, a `watch_health()` push stream over a new iceoryx2 health plane, structured (JSON) logging, and the triaged cycle-1/2 residuals.

**Architecture:** Enrich what feeds the existing pure reduction rather than changing its shape ad hoc: `SystemSnapshot` gains the missing facts (`enabled` per provider, recent gap spans + last-gap receipt + backfilling per stream, `Unauthenticated` connection state), the reduction maps them (SCHEMA_VERSION 2), and the daemon serves the reduced view both pull (`health` op, daemon-stamped) and push (a `datamancer/health` iceoryx2 service beside the existing diagnostics plane, same ticker). Auth state rides in-band per the workspace invariant: `ProviderDisconnected` gains a serde-defaulted `cause` field (no new control variant), Alpaca classifies `oxidized_alpaca::Error::StreamingAuth`, and provider accounting derives `Unauthenticated` from the flowing control.

**Tech Stack:** Rust edition 2024, tokio, serde/serde_json/toml, tracing + tracing-subscriber (json feature), iceoryx2 0.9.2 (pinned), existing workspace crates.

## Global Constraints

- `clippy::pedantic = deny` workspace-wide; `#![forbid(unsafe_code)]` in all seven crates.
- Spec (2026-07-05-app-facing-daemon-design.md), verbatim: "**Per-symbol only.** `streams` is keyed `(instrument, kind)`. No global event count, position, or merged sequence."
- Spec, verbatim: "**`Unauthenticated` and `CompanionUnreachable` exist from day one.** Alpaca never emits `CompanionUnreachable`; IBKR will." This cycle Alpaca **produces** `Unauthenticated`; `CompanionUnreachable` stays reserved (golden-tested as a hand-built fixture only).
- Spec, verbatim: "**Versioned:** `HealthView::SCHEMA_VERSION` in the type and (cycle 4) the wire envelope, so skew degrades detectably instead of misrendering." → daemon-side reduction: the `health` op reply and the pushed view carry the **daemon's** `schema_version`.
- Spec, verbatim: "`latency` is the sanctioned `rx_ts` use and is labelled observability-only." `Liveness`/gap-recency judgments stay wall-clock (`rx_ts` vs `captured_at`) — observability only, never engine logic.
- Spec testing bullet, verbatim: "`HealthView` reduction golden tests from snapshot fixtures, including synthetic `Gapped` / `Stale` / `CompanionUnreachable` fixtures Alpaca cannot produce today."
- Cycle-3 revision (memory, verbatim): "provider set is fixed at build time … everything compiled-in starts disabled until enabled" — disabled must be **distinguishable** in `HealthView` (deferred enrichment lands here as `ProviderState::Disabled`).
- The `health` op is **ungated** (read-only, like `snapshot`); it is **not** served on the WS surface (WS consumers reduce client-side via `HealthView::from_snapshot`).
- "Every config field is classified hot or cold in one table … A new field without a classification fails the build" — the new `[log]` section must be classified (Cold) and appear in the `FULL` exhaustiveness fixture.
- No secret material in logs/errors/`Debug` — JSON logging must not change that (the `ws.auth_token` redaction stays).
- Library parity (spec decision 9): everything the daemon surfaces must exist in-process too — `Datamancer::health()` picks up all v2 enrichment automatically via the shared reduction; `Provider::enabled()` is a trait method with a `true` default.
- `datamancer-client` and `datamancerd` bump **in lockstep** (ping version gate; pinned by `daemon_and_client_versions_stay_in_lockstep`). This cycle: both 0.4.0 → **0.5.0**; `datamancer` 0.4.0 → **0.5.0**; `datamancer-core` 0.1.0 → **0.2.0**; `datamancer-transport-iceoryx2` 0.1.0 → **0.2.0**. `HealthView::SCHEMA_VERSION` 1 → **2**.
- iceoryx2 pinned **0.9.2**; new ports on `ipc_threadsafe::Service`; the health plane copies the diagnostics plane's byte-slice + JSON pattern (not the zero-copy hot path).
- tokio watch discipline (cycle-2/3 lessons) unchanged; do not touch the receiver-before-build ordering in providers.
- Windows CI builds only the ws-portable subset; any path-shape assertions cfg'd per-OS.
- Before the PR: `git fetch origin main && cargo deny check && .github/scripts/semver-checks.sh origin/main`.

## File Structure

- `crates/datamancer-core/src/event.rs` — `DisconnectCause` + `ProviderDisconnected.cause` (serde-defaulted).
- `crates/datamancer-core/src/snapshot.rs` — `ConnectionState::Unauthenticated`; `ProviderSnapshot.enabled`; `AuthoritativeSessionSnapshot.{recent_gaps, last_gap_rx_ts, backfilling}` + builders.
- `crates/datamancer-core/src/health.rs` — v2 reduction: `ProviderState::Disabled`, `Liveness::{Gapped, Backfilling}`, `SCHEMA_VERSION = 2`, golden tests, staleness-boundary lock-in.
- `crates/datamancer-core/src/traits/provider.rs` — `Provider::enabled()` default method.
- `crates/datamancer/src/client.rs` — `LiveStats` gap-span ring / last-gap-rx / backfilling flag.
- `crates/datamancer/src/session.rs` — assembler threads new fields; `run_backfill` sets/clears the backfilling flag.
- `crates/datamancer/src/accounting.rs` — `auth_failed` derivation from `DisconnectCause::Unauthenticated`.
- `crates/datamancer/src/providers/alpaca.rs`, `alpaca_crypto.rs` — `enabled()` impls; `StreamingAuth` classification + park-until-rotation.
- `crates/datamancer-transport-iceoryx2/src/diagnostics.rs` — health-plane publisher/subscriber (`datamancer/health`).
- `crates/datamancer-client/src/protocol/uds.rs` — `Request::Health`, `Reply.health`, `Reply::health()`.
- `crates/datamancer-client/src/app/mod.rs` — `health()` switches to the op; `watch_health()`.
- `crates/datamancer-client/src/app/error.rs`, `app/lifecycle.rs` — `ReadyDiagnosis::Unresponsive { last_ping_failure }`.
- `crates/datamancerd/src/server.rs` — `Health` dispatch; ticker publishes both planes.
- `crates/datamancerd/src/config.rs`, `config_class.rs`, `main.rs` — `[log]` section (Cold), JSON/text subscriber init via config peek.
- `crates/datamancerd/src/web/handlers.rs`, `web/state.rs` — `/api/health` envelope over the core `HealthView`.
- `crates/datamancerd/tests/health_observability_e2e.rs` — **new** `#[ignore]`d e2e.
- `crates/datamancerd/README.md`, root `CLAUDE.md` — operator contract + docs.

---

### Task 1: `DisconnectCause` on `ProviderDisconnected` (core event model)

**Files:**
- Modify: `crates/datamancer-core/src/event.rs:191-216` (ControlKind)
- Modify: every workspace construction site of `ControlKind::ProviderDisconnected` (compiler-guided; known sites: `crates/datamancer/src/providers/alpaca.rs:477`, `alpaca_crypto.rs` equivalent, plus any test constructors)

**Interfaces:**
- Produces: `pub enum DisconnectCause { Error (default), Unauthenticated, CompanionUnreachable }` (`#[non_exhaustive]`, `snake_case` wire names) and `ControlKind::ProviderDisconnected { provider: String, reason: String, cause: DisconnectCause }` with `#[serde(default)]` on `cause`. Task 5 sets `cause: DisconnectCause::Unauthenticated`; Task 4's accounting matches on it.

- [ ] **Step 1: Write the failing serde tests** in `event.rs`'s `serde_tests` module:

```rust
#[test]
fn provider_disconnected_cause_defaults_on_old_frames() {
    // A pre-cycle-4 frame has no `cause`; it must parse as Error.
    let old = r#"{"ProviderDisconnected":{"provider":"p","reason":"ws closed"}}"#;
    let kind: ControlKind = serde_json::from_str(old).unwrap();
    assert!(matches!(
        kind,
        ControlKind::ProviderDisconnected { cause: DisconnectCause::Error, .. }
    ));
}

#[test]
fn disconnect_cause_wire_names_are_stable() {
    for (cause, wire) in [
        (DisconnectCause::Error, "\"error\""),
        (DisconnectCause::Unauthenticated, "\"unauthenticated\""),
        (DisconnectCause::CompanionUnreachable, "\"companion_unreachable\""),
    ] {
        let json = serde_json::to_string(&cause).unwrap();
        assert_eq!(json, wire);
        assert_eq!(serde_json::from_str::<DisconnectCause>(&json).unwrap(), cause);
    }
}
```

(Adjust the `old` literal to the actual `ControlKind` serde representation — check an existing round-trip test for whether the enum is externally tagged as shown.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-core provider_disconnected_cause`
Expected: FAIL — `DisconnectCause` not defined.

- [ ] **Step 3: Implement**

In `event.rs`, above `ControlKind`:

```rust
/// Why a provider connection is down — app-renderable classification.
/// `Unauthenticated`: credentials rejected or an auth session lapsed;
/// retrying without new credentials cannot help. `CompanionUnreachable` is
/// reserved (spec appendix: IBKR's local gateway) — nothing emits it yet.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisconnectCause {
    #[default]
    Error,
    Unauthenticated,
    CompanionUnreachable,
}
```

Change the variant:

```rust
    /// Provider connection lost; a reconnect attempt is scheduled or in flight.
    ProviderDisconnected {
        provider: String,
        reason: String,
        /// Classified cause; `Error` on frames that predate the field.
        #[serde(default)]
        cause: DisconnectCause,
    },
```

Re-export `DisconnectCause` from `datamancer-core/src/lib.rs` alongside `ControlKind`, and from `datamancer/src/lib.rs`'s core re-export list. Fix all construction sites with `cause: DisconnectCause::Error` (compiler-guided: `cargo build --workspace --all-features`).

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancer-core && cargo build --workspace --all-features`
Expected: PASS, clean build.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(core): DisconnectCause classification on ProviderDisconnected"
```

---

### Task 2: Snapshot surface enrichment (core)

**Files:**
- Modify: `crates/datamancer-core/src/snapshot.rs` (`ConnectionState` at 55-61, `ProviderSnapshot` at 89-105 + ctor 188-241, `AuthoritativeSessionSnapshot` at 118-137 + ctor 253-298)
- Modify: `crates/datamancer/src/session.rs:600-702` (assembler compiles with defaults — real feeding is Task 4)

**Interfaces:**
- Produces: `ConnectionState::Unauthenticated`; `ProviderSnapshot { pub enabled: bool }` (defaults `true` in `new`, builder `with_enabled(bool)`); `AuthoritativeSessionSnapshot { pub recent_gaps: Vec<GapSpan>, pub last_gap_rx_ts: Option<Timestamp>, pub backfilling: bool }` (default empty/`None`/`false` in `new`, builders `with_gaps(Vec<GapSpan>, Option<Timestamp>)` and `with_backfilling(bool)`). Tasks 3–4 consume all of these.

- [ ] **Step 1: Write the failing test** in `snapshot.rs` tests:

```rust
#[test]
fn snapshot_enrichment_defaults_and_builders() {
    let inst = Instrument::new(ProviderId::from_static("p"), AssetClass::Equity, "AAPL");
    let s = AuthoritativeSessionSnapshot::new(inst.clone(), EventKind::Trade, 1, 0);
    assert!(s.recent_gaps.is_empty());
    assert_eq!(s.last_gap_rx_ts, None);
    assert!(!s.backfilling);
    let span = GapSpan { from_source_ts: Timestamp(1), to_source_ts: Timestamp(2) };
    let s = s
        .with_gaps(vec![span.clone()], Some(Timestamp(3)))
        .with_backfilling(true);
    assert_eq!(s.recent_gaps, vec![span]);
    assert_eq!(s.last_gap_rx_ts, Some(Timestamp(3)));
    assert!(s.backfilling);

    let p = ProviderSnapshot::new(
        ProviderId::from_static("p"), ConnectionState::Unknown,
        0, 0, 0, 0, 0, 0, 0, 0, None,
    );
    assert!(p.enabled); // default: enabled (embedder Static sources)
    assert!(!p.with_enabled(false).enabled);
}
```

Also extend the existing `snapshot_serde_round_trips` struct literal with the new fields (`enabled: false`, `recent_gaps: vec![…]`, `last_gap_rx_ts: Some(Timestamp(9))`, `backfilling: true`) so serde coverage includes them.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-core snapshot_enrichment`
Expected: FAIL — fields/builders not defined.

- [ ] **Step 3: Implement**

`ConnectionState` gains (after `Disconnected`):

```rust
    /// Credentials rejected or an auth session lapsed; reconnect without new
    /// credentials cannot help. Derived from in-band
    /// [`crate::DisconnectCause::Unauthenticated`].
    Unauthenticated,
```

`ProviderSnapshot` gains a field + builder (default `true` inside `new`):

```rust
    /// Whether the provider is enabled (a daemon `Watch(None)` settings
    /// source parks a compiled-in provider disabled; `Static` embedder
    /// sources are always enabled). Disabled is *deliberate* — distinct
    /// from enabled-but-not-yet-connected.
    pub enabled: bool,
```

```rust
    /// Mark the provider deliberately disabled (parked settings watch).
    #[must_use]
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
```

`AuthoritativeSessionSnapshot` gains (with `new` defaults `Vec::new()` / `None` / `false`):

```rust
    /// Most recent per-symbol `Control::Gap` spans (bounded ring; oldest
    /// evicted). Detail behind `gap_count`.
    pub recent_gaps: Vec<GapSpan>,
    /// Wall-clock receipt of the most recent `Control::Gap` (observability).
    pub last_gap_rx_ts: Option<Timestamp>,
    /// Whether a historical→live backfill is currently in progress.
    pub backfilling: bool,
```

```rust
    /// Set the recent gap-span detail.
    #[must_use]
    pub fn with_gaps(mut self, recent_gaps: Vec<GapSpan>, last_gap_rx_ts: Option<Timestamp>) -> Self {
        self.recent_gaps = recent_gaps;
        self.last_gap_rx_ts = last_gap_rx_ts;
        self
    }

    /// Set the backfill-in-progress flag.
    #[must_use]
    pub fn with_backfilling(mut self, backfilling: bool) -> Self {
        self.backfilling = backfilling;
        self
    }
```

`GapSpan` needs `PartialEq` already derived (it is) — import it in the test list. Serde: plain fields (no `skip_serializing_if`), so the diagnostics JSON carries them unconditionally.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancer-core && cargo build --workspace --all-features`
Expected: PASS (assembler still compiles — new fields default via `new`).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(core): snapshot enrichment — enabled bit, gap spans, backfilling, Unauthenticated"
```

---

### Task 3: `HealthView` v2 reduction (core)

**Files:**
- Modify: `crates/datamancer-core/src/health.rs` (whole reduction + tests)

**Interfaces:**
- Consumes: Task 2's snapshot fields.
- Produces: `ProviderState::Disabled`; `Liveness::Gapped { spans: Vec<GapSpan> }` and `Liveness::Backfilling`; `HealthView::SCHEMA_VERSION == 2`. Signature of `HealthView::from_snapshot(&SystemSnapshot, i64) -> HealthView` unchanged — Tasks 6, 7, 10 all call it.

**Reduction semantics (the contract this task locks in):**
- Provider: `enabled == false` → `Disabled` (regardless of connection state); else `Connected→Connected`, `Disconnected→Disconnected`, `Unauthenticated→Unauthenticated`, `Unknown→Connecting`.
- Liveness precedence: `backfilling` → `Backfilling`; else no `last_rx_ts` → `Idle`; else **strictly** `captured_at - rx > stale_after_ns` → `Stale` (exact-threshold age is *not* stale — cycle-1 triage residual, locked in with a boundary test); else a gap received within the window (`captured_at - gap_rx <= stale_after_ns`) → `Gapped { spans: recent_gaps }`; else `Live`.

- [ ] **Step 1: Write the failing tests** (extend `health.rs` tests; update `stream_snapshot` helper to take gap/backfill parameters or add new helpers):

```rust
#[test]
fn disabled_provider_is_distinguishable() {
    let snap = snapshot(
        vec![provider_snapshot(ConnectionState::Unknown, None).with_enabled(false)],
        vec![],
        1_000,
    );
    let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
    assert_eq!(view.providers[0].state, ProviderState::Disabled);
}

#[test]
fn unauthenticated_maps_through() {
    let snap = snapshot(
        vec![provider_snapshot(ConnectionState::Unauthenticated, Some("auth rejected"))],
        vec![],
        1_000,
    );
    let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
    assert_eq!(view.providers[0].state, ProviderState::Unauthenticated);
}

#[test]
fn staleness_boundary_is_strictly_greater() {
    // Exact-threshold age counts Live (cycle-1 triage residual, locked in).
    let now = 100_000_000_000_i64;
    let exactly = now - HealthView::DEFAULT_STALE_AFTER_NS;
    let snap = snapshot(
        vec![provider_snapshot(ConnectionState::Connected, None)],
        vec![stream_snapshot(Some(exactly))],
        now,
    );
    let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
    assert_eq!(view.streams[0].liveness, Liveness::Live);
}

#[test]
fn gapped_when_recent_gap_and_not_stale() {
    let now = 100_000_000_000_i64;
    let span = GapSpan { from_source_ts: Timestamp(1), to_source_ts: Timestamp(2) };
    let s = stream_snapshot(Some(now - 1_000_000_000)) // fresh data
        .with_gaps(vec![span.clone()], Some(Timestamp(now - 2_000_000_000))); // gap 2s ago
    let snap = snapshot(vec![provider_snapshot(ConnectionState::Connected, None)], vec![s], now);
    let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
    assert_eq!(view.streams[0].liveness, Liveness::Gapped { spans: vec![span] });
}

#[test]
fn stale_wins_over_gapped_and_backfilling_wins_over_all() {
    let now = 100_000_000_000_i64;
    let span = GapSpan { from_source_ts: Timestamp(1), to_source_ts: Timestamp(2) };
    // Stale data + old gap ⇒ Stale (gap outside window).
    let stale = stream_snapshot(Some(now - 30_000_000_000))
        .with_gaps(vec![span.clone()], Some(Timestamp(now - 30_000_000_000)));
    // Backfilling ⇒ Backfilling even with stale data.
    let backfilling = stream_snapshot(Some(now - 30_000_000_000)).with_backfilling(true);
    let snap = snapshot(
        vec![provider_snapshot(ConnectionState::Connected, None)],
        vec![stale, backfilling],
        now,
    );
    let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
    assert!(matches!(view.streams[0].liveness, Liveness::Stale { .. }));
    assert_eq!(view.streams[1].liveness, Liveness::Backfilling);
}

#[test]
fn schema_version_is_2() {
    let snap = snapshot(vec![], vec![], 0);
    let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
    assert_eq!(view.schema_version, 2);
}

#[test]
fn v2_wire_golden() {
    // Golden JSON for the new variants, including hand-built
    // CompanionUnreachable (nothing produces it — spec appendix fixture).
    let span = GapSpan { from_source_ts: Timestamp(1), to_source_ts: Timestamp(2) };
    for (liveness, wire) in [
        (Liveness::Backfilling, r#""backfilling""#.to_string()),
        (
            Liveness::Gapped { spans: vec![span] },
            r#"{"gapped":{"spans":[{"from_source_ts":1,"to_source_ts":2}]}}"#.to_string(),
        ),
    ] {
        assert_eq!(serde_json::to_string(&liveness).unwrap(), wire);
    }
    assert_eq!(
        serde_json::to_string(&ProviderState::Disabled).unwrap(),
        r#""disabled""#
    );
    assert_eq!(
        serde_json::to_string(&ProviderState::CompanionUnreachable).unwrap(),
        r#""companion_unreachable""#
    );
}
```

(Verify the `Gapped` golden literal against serde's actual adjacency for struct variants under `rename_all = "snake_case"` — adjust the expected string to what serde produces, then freeze it.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-core health`
Expected: FAIL — `Disabled`/`Gapped`/`Backfilling` not defined.

- [ ] **Step 3: Implement**

`ProviderState` gains (after `CompanionUnreachable`):

```rust
    /// Compiled in but deliberately disabled (parked settings watch). Not an
    /// error: enable via the daemon config service.
    Disabled,
```

`Liveness` gains:

```rust
    /// A `Control::Gap` was received within the staleness window; data is
    /// otherwise flowing. `spans` is the bounded recent-gap detail.
    Gapped { spans: Vec<GapSpan> },
    /// A historical→live backfill is in progress; staleness judgment is
    /// suspended until the seam flushes.
    Backfilling,
```

(`Liveness` loses `Copy`/`Eq` if `GapSpan` forces it — keep `Clone, PartialEq`; chase compile errors.) Bump `SCHEMA_VERSION` to `2`. Reduction body:

```rust
        let providers = snapshot
            .providers
            .iter()
            .map(|p| ProviderHealth {
                provider: p.provider.clone(),
                state: if p.enabled {
                    match p.connection_state {
                        ConnectionState::Connected => ProviderState::Connected,
                        ConnectionState::Disconnected => ProviderState::Disconnected,
                        ConnectionState::Unauthenticated => ProviderState::Unauthenticated,
                        ConnectionState::Unknown => ProviderState::Connecting,
                    }
                } else {
                    ProviderState::Disabled
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
                liveness: if s.backfilling {
                    Liveness::Backfilling
                } else {
                    match s.last_rx_ts {
                        None => Liveness::Idle,
                        Some(rx) if snapshot.captured_at.0 - rx.0 > stale_after_ns => {
                            Liveness::Stale { since: rx }
                        }
                        Some(_) => match s.last_gap_rx_ts {
                            Some(gap_rx)
                                if snapshot.captured_at.0 - gap_rx.0 <= stale_after_ns =>
                            {
                                Liveness::Gapped { spans: s.recent_gaps.clone() }
                            }
                            _ => Liveness::Live,
                        },
                    }
                },
                last_event_source_ts: s.last_source_ts,
                gap_count: s.gap_count,
                latency: s.latency_ns.map(|last_ns| LatencySummary { last_ns }),
            })
            .collect();
```

Update the module doc comment (staleness boundary semantics: "exact-threshold age counts `Live`").

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancer-core && cargo build --workspace --all-features`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(core): HealthView v2 — Disabled, Unauthenticated, Gapped, Backfilling (schema 2)"
```

---

### Task 4: Orchestrator feeds the enrichment (`LiveStats`, assembler, `Provider::enabled`)

**Files:**
- Modify: `crates/datamancer-core/src/traits/provider.rs:31` (trait `Provider`)
- Modify: `crates/datamancer/src/client.rs:228-333` (`LiveStats`)
- Modify: `crates/datamancer/src/session.rs` (assembler 600-702; `run_backfill` ~1720-1800)
- Modify: `crates/datamancer/src/providers/alpaca.rs:263-282`, `alpaca_crypto.rs` equivalent (`enabled()` impls)

**Interfaces:**
- Consumes: Task 2's builders (`with_gaps`, `with_backfilling`, `with_enabled`).
- Produces: `Provider::enabled(&self) -> bool` (default `true`); `LiveStats::{recent_gaps() -> Vec<GapSpan>, last_gap_rx_ts() -> Option<Timestamp>, backfilling() -> bool, set_backfilling(bool)}`. The assembler emits fully-fed snapshots — Tasks 6/7/10 rely on that.

- [ ] **Step 1: Write the failing tests**

`client.rs` tests (module exists near `LiveStats`):

```rust
#[test]
fn live_stats_retain_bounded_recent_gap_spans() {
    let stats = LiveStats::new();
    for i in 0..10_i64 {
        stats.record_event(&MarketEvent::Control(Control {
            source_ts: Timestamp(i),
            rx_ts: Timestamp(1_000 + i),
            seq: Seq(u64::try_from(i).unwrap()),
            kind: ControlKind::Gap {
                provider: "p".to_string(),
                instrument: Instrument::new(ProviderId::from_static("p"), AssetClass::Equity, "X"),
                span: GapSpan { from_source_ts: Timestamp(i), to_source_ts: Timestamp(i + 1) },
            },
        }));
    }
    let spans = stats.recent_gaps();
    assert_eq!(spans.len(), 8); // RECENT_GAPS_CAP — oldest two evicted
    assert_eq!(spans[0].from_source_ts, Timestamp(2));
    assert_eq!(stats.last_gap_rx_ts(), Some(Timestamp(1_009)));
    assert_eq!(stats.gap_count(), 10);
}

#[test]
fn backfilling_flag_sets_and_clears() {
    let stats = LiveStats::new();
    assert!(!stats.backfilling());
    stats.set_backfilling(true);
    assert!(stats.backfilling());
    stats.set_backfilling(false);
    assert!(!stats.backfilling());
}
```

Assembler test (in `session.rs` tests or the existing snapshot integration test file — follow where `assemble_snapshot` coverage lives today): assert a snapshot of a session whose stats carry gaps/backfilling reproduces them, and a provider whose `enabled()` is false yields `ProviderSnapshot.enabled == false`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer live_stats_retain`
Expected: FAIL.

- [ ] **Step 3: Implement**

`traits/provider.rs`, on `trait Provider`:

```rust
    /// Whether this provider is currently enabled. `Watch(None)` settings
    /// sources (daemon-parked) report `false`; the default covers providers
    /// without a runtime settings seam.
    fn enabled(&self) -> bool {
        true
    }
```

Both Alpaca impls (`impl Provider for AlpacaProvider` / `AlpacaCryptoProvider`):

```rust
    fn enabled(&self) -> bool {
        self.cfg.settings.current().is_some()
    }
```

`LiveStats`: add fields + `RECENT_GAPS_CAP`:

```rust
/// Bounded per-symbol recent-gap detail (oldest evicted).
const RECENT_GAPS_CAP: usize = 8;
```

```rust
    /// Recent gap spans (bounded ring; cold mutex, written only on Gap).
    recent_gaps: std::sync::Mutex<std::collections::VecDeque<GapSpan>>,
    has_gap_rx: AtomicBool,
    last_gap_rx_ts: AtomicI64,
    backfilling: AtomicBool,
```

`record_event`'s Control arm becomes:

```rust
            MarketEvent::Control(c) => {
                if let ControlKind::Gap { span, .. } = &c.kind {
                    self.gap_count.fetch_add(1, Ordering::Relaxed);
                    self.last_gap_rx_ts.store(c.rx_ts.0, Ordering::Relaxed);
                    self.has_gap_rx.store(true, Ordering::Relaxed);
                    if let Ok(mut ring) = self.recent_gaps.lock() {
                        if ring.len() == RECENT_GAPS_CAP {
                            ring.pop_front();
                        }
                        ring.push_back(span.clone());
                    }
                }
            }
```

Readers:

```rust
    /// Recent gap spans, oldest first (bounded at `RECENT_GAPS_CAP`).
    pub(crate) fn recent_gaps(&self) -> Vec<GapSpan> {
        self.recent_gaps
            .lock()
            .map(|ring| ring.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Wall-clock receipt of the most recent gap, or `None` before any.
    pub(crate) fn last_gap_rx_ts(&self) -> Option<Timestamp> {
        self.has_gap_rx
            .load(Ordering::Relaxed)
            .then(|| Timestamp(self.last_gap_rx_ts.load(Ordering::Relaxed)))
    }

    /// Whether a historical→live backfill is currently in progress.
    pub(crate) fn backfilling(&self) -> bool {
        self.backfilling.load(Ordering::Relaxed)
    }

    /// Mark backfill in progress (set by `run_backfill`, cleared at the seam
    /// flush and on every backfill exit path).
    pub(crate) fn set_backfilling(&self, active: bool) {
        self.backfilling.store(active, Ordering::Relaxed);
    }
```

`session.rs` assembler (lines ~639-673): extend the per-session build:

```rust
    .with_gaps(stats.recent_gaps(), stats.last_gap_rx_ts())
    .with_backfilling(stats.backfilling())
```

and the provider accounting fold (lines ~604-634): `.with_enabled(provider.enabled())`, looking the provider up in the same registry the accounting map was built from (`session.rs:866-875` builds both — thread the handle or id-lookup accordingly).

`run_backfill` (session.rs ~1720): `self.stats.set_backfilling(true);` on entry; `self.stats.set_backfilling(false);` after `flush_backfill_pending` completes **and on every early-return/error path** (audit each `return`/`?` in the function; if the control flow makes that error-prone, hold a small guard struct whose `Drop` clears the flag).

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancer && cargo test -p datamancer-core`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(datamancer): feed HealthView v2 — gap spans, backfilling flag, Provider::enabled"
```

---

### Task 5: Alpaca produces `Unauthenticated`

**Files:**
- Modify: `crates/datamancer/src/accounting.rs` (fields 29-49, `record_forwarded` 102-131, `record_connection_up` 136-142, `connection_state` 189-200)
- Modify: `crates/datamancer/src/providers/alpaca.rs:474-488` and the equivalent connect-error arm in `alpaca_crypto.rs` (~line 430-460)

**Interfaces:**
- Consumes: Task 1's `DisconnectCause`; Task 2's `ConnectionState::Unauthenticated`.
- Produces: accounting derives `ConnectionState::Unauthenticated`; Alpaca emits `cause: DisconnectCause::Unauthenticated` on `oxidized_alpaca::Error::StreamingAuth` and parks until credential rotation.

- [ ] **Step 1: Write the failing accounting test** (`accounting.rs` tests):

```rust
#[test]
fn unauthenticated_disconnect_derives_state_until_next_connect() {
    let a = ProviderAccounting::default();
    a.record_forwarded(
        &control(ControlKind::ProviderDisconnected {
            provider: "p".to_string(),
            reason: "auth rejected".to_string(),
            cause: DisconnectCause::Unauthenticated,
        }),
        true,
    );
    assert_eq!(a.connection_state(), ConnectionState::Unauthenticated);
    assert_eq!(a.last_error(), Some("auth rejected".to_string()));
    // A successful (re)connect clears the flag.
    a.record_connection_up(false);
    assert_eq!(a.connection_state(), ConnectionState::Connected);
    a.record_connection_down();
    assert_eq!(a.connection_state(), ConnectionState::Disconnected);
}

#[test]
fn plain_disconnect_does_not_mark_unauthenticated() {
    let a = ProviderAccounting::default();
    a.record_forwarded(
        &control(ControlKind::ProviderDisconnected {
            provider: "p".to_string(),
            reason: "ws closed".to_string(),
            cause: DisconnectCause::Error,
        }),
        true,
    );
    assert_eq!(a.connection_state(), ConnectionState::Unknown);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer unauthenticated_disconnect`
Expected: FAIL.

- [ ] **Step 3: Implement accounting**

Field: `auth_failed: std::sync::atomic::AtomicBool` (default `false`). `record_forwarded`'s control match replaces the grouped no-op arm for `ProviderDisconnected`:

```rust
                ControlKind::ProviderDisconnected { reason, cause, .. } => {
                    // Up/down accounting stays with the owning controller
                    // (see comment below); only the *classification* is
                    // folded here, because the controller cannot see it.
                    if *cause == DisconnectCause::Unauthenticated {
                        self.auth_failed.store(true, Ordering::Relaxed);
                        if let Ok(mut slot) = self.last_error.lock() {
                            *slot = Some(reason.clone());
                        }
                    }
                }
```

`record_connection_up` adds `self.auth_failed.store(false, Ordering::Relaxed);`. `connection_state()`:

```rust
        if self.active_connections.load(Ordering::Relaxed) > 0 {
            ConnectionState::Connected
        } else if self.auth_failed.load(Ordering::Relaxed) {
            ConnectionState::Unauthenticated
        } else if self.ever_connected.load(Ordering::Relaxed) {
            ConnectionState::Disconnected
        } else {
            ConnectionState::Unknown
        }
```

- [ ] **Step 4: Implement the Alpaca classification** (same shape in both providers; `alpaca.rs:474` connect-error arm):

```rust
            Err(err) => {
                let unauthenticated = matches!(err, oxidized_alpaca::Error::StreamingAuth);
                emit_control(
                    &sink,
                    ControlKind::ProviderDisconnected {
                        provider: PROVIDER_ID.to_string(),
                        reason: format!("connect failed: {err}"),
                        cause: if unauthenticated {
                            DisconnectCause::Unauthenticated
                        } else {
                            DisconnectCause::Error
                        },
                    },
                )
                .await;
                if unauthenticated && let Some(rx) = cred_rx.as_mut() {
                    // Rejected credentials: retrying cannot help. Park until
                    // a rotation (set-credentials hot-apply) or disable —
                    // mirrors the Missing-credentials park above. Static
                    // sources can't rotate, so they fall through to backoff.
                    if !wait_for_provisioning(rx, &mut cmd_rx, "waiting for new credentials after auth rejection").await {
                        return;
                    }
                    continue 'outer;
                }
                if !sleep_with_jitter(&mut backoff, &cfg.reconnect, &mut cmd_rx).await {
                    return;
                }
                continue 'outer;
            }
```

Verify the concrete error type/path of `connect_result`'s `Err` (the `StreamingStockClient::new*` return) and match accordingly — the classification must be the typed `oxidized_alpaca::Error::StreamingAuth` variant, never a string match. All other `ProviderDisconnected` sites in both providers get `cause: DisconnectCause::Error`.

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p datamancer && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(datamancer): Alpaca auth rejection surfaces as Unauthenticated, parks until rotation"
```

---

### Task 6: UDS `health` op (daemon-side reduction)

**Files:**
- Modify: `crates/datamancer-client/src/protocol/uds.rs` (`Request` 17-86, `Reply` 90-130 + constructors)
- Modify: `crates/datamancerd/src/server.rs` (`dispatch` at 534-601; README table in `crates/datamancerd/README.md`)
- Modify: `crates/datamancer-client/src/app/mod.rs:184-191` (`health()`)

**Interfaces:**
- Consumes: `HealthView::from_snapshot` (Task 3), `Datamancer::snapshot_live()`.
- Produces: `Request::Health` (wire `{"op":"health"}`), `Reply { pub health: Option<HealthView> }`, `Reply::health(view: HealthView) -> Reply`. `AppHandle::health()` keeps its exact signature (`async fn health(&mut self) -> Result<HealthView, ClientError<Iceoryx2ClientError>>`) but now returns the daemon-stamped view. Task 11's e2e asserts `schema_version == 2` and a daemon-stamped `version`.

- [ ] **Step 1: Write the failing round-trip test** (`uds.rs` tests, mirroring `ping_round_trips_and_reply_carries_version`):

```rust
#[test]
fn health_round_trips_and_reply_carries_view() {
    let req: Request = serde_json::from_str(r#"{"op":"health"}"#).unwrap();
    assert!(matches!(req, Request::Health));
    let snap = SystemSnapshot::new(
        Timestamp(1), vec![], CacheSnapshot::new(vec![], None), vec![], vec![],
    );
    let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_STALE_AFTER_NS);
    let line = serde_json::to_string(&Reply::health(view.clone())).unwrap();
    let back: Reply = serde_json::from_str(&line).unwrap();
    assert!(back.ok);
    assert_eq!(back.health, Some(view));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-client health_round_trips`
Expected: FAIL.

- [ ] **Step 3: Implement the vocabulary**

`Request` gains (doc comment: "App-facing health reduction, daemon-stamped; ungated read-only op"):

```rust
    /// The app-facing `HealthView`, reduced and stamped daemon-side.
    Health,
```

`Reply` gains `#[serde(skip_serializing_if = "Option::is_none")] pub health: Option<HealthView>` (follow the existing optional-field pattern) and:

```rust
    /// A successful `health` reply.
    #[must_use]
    pub fn health(view: HealthView) -> Self {
        Self { health: Some(view), ..Self::ok() }
    }
```

(Match the existing constructor style — if `Reply::ok()` + struct-update isn't the local idiom, mirror `Reply::snapshot`'s shape exactly.)

- [ ] **Step 4: Wire the daemon dispatch** (`server.rs`, next to the `Request::Snapshot` arm; the actor holds `self.dm` and `self.credential_backend`):

```rust
            Request::Health => {
                let mut view = HealthView::from_snapshot(
                    &self.dm.snapshot_live(),
                    HealthView::DEFAULT_STALE_AFTER_NS,
                );
                view.daemon.version = Some(env!("CARGO_PKG_VERSION").to_string());
                view.daemon.credential_backend = Some(self.credential_backend.to_string());
                Reply::health(view)
            }
```

`health` is **ungated** (like `snapshot`) — do not add it to the peer-cred branch in `handle_connection`; it dispatches on-actor. Do **not** add a `WsRequest::Health` (WS consumers reduce client-side). Document the op in `crates/datamancerd/README.md`'s op table (request/reply JSON, "ungated, UDS only").

- [ ] **Step 5: Switch `AppHandle::health()`** (app/mod.rs):

```rust
    /// The daemon's app-facing health view, reduced and stamped daemon-side
    /// (version, credential backend, `schema_version`). The `ensure` version
    /// gate makes daemon/client schema skew unrepresentable in practice; the
    /// `schema_version` field is the detectable degradation if it ever isn't.
    pub async fn health(&mut self) -> Result<HealthView, ClientError<Iceoryx2ClientError>> {
        let reply = self.control_request(&Request::Health).await?;
        reply.health.ok_or_else(|| {
            ClientError::Control {
                code: codes::BAD_REQUEST.to_string(),
                message: "health reply missing view".to_string(),
            }
        })
    }
```

(Adapt the request helper and error construction to the exact existing idiom used by `get_config` in the same file.) Delete `fill_health` and move its two unit tests (`health_fill_sets_daemon_version`, `health_fill_sets_backend_alongside_version`) to daemon-side coverage: a `server.rs` unit test asserting the `Health` dispatch stamps `version`/`credential_backend` (follow how existing dispatch arms are unit-tested there; if dispatch isn't unit-testable without a runtime, keep the stamping assertions in the Task 11 e2e and delete the client tests with the function).

- [ ] **Step 6: Run to verify pass**

Run: `cargo test -p datamancer-client && cargo test -p datamancerd && cargo build --workspace --all-features`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(datamancerd): ungated health op — daemon-side HealthView reduction, stamped"
```

---

### Task 7: Health push plane + `watch_health()`

**Files:**
- Modify: `crates/datamancer-transport-iceoryx2/src/diagnostics.rs` (+ re-exports in `lib.rs`)
- Modify: `crates/datamancerd/src/server.rs:277-279, 976-998` (`spawn_diagnostics`)
- Modify: `crates/datamancer-client/src/app/mod.rs` (`watch_health()`)

**Interfaces:**
- Consumes: Task 3's `HealthView` (serde), Task 6's stamping pattern.
- Produces: transport `Iceoryx2HealthPublisher::{new(node), publish(&HealthView)}` / `Iceoryx2HealthSubscriber::{open(node), receive() -> Result<Option<HealthView>>}` on service `"datamancer/health"`; `AppHandle::watch_health(&self) -> Result<HealthStream, ClientError<Iceoryx2ClientError>>` where `pub type HealthStream = tokio_stream::wrappers::ReceiverStream<HealthView>`.

- [ ] **Step 1: Write the failing transport codec test** (`diagnostics.rs` tests):

```rust
#[test]
fn health_view_survives_health_plane_codec() {
    let snap = sample_snapshot(1);
    let view = datamancer_core::HealthView::from_snapshot(
        &snap,
        datamancer_core::HealthView::DEFAULT_STALE_AFTER_NS,
    );
    let bytes = encode_health(&view).unwrap();
    assert!(bytes.len() <= DIAGNOSTICS_PAYLOAD_CAPACITY);
    assert_eq!(decode_health(&bytes).unwrap(), view);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-transport-iceoryx2 health_view_survives`
Expected: FAIL.

- [ ] **Step 3: Implement the health plane** in `diagnostics.rs`. Generalize the codec privately and add typed wrappers:

```rust
fn encode_capped<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, DiagnosticsError> {
    let bytes = serde_json::to_vec(value).map_err(DiagnosticsError::Codec)?;
    if bytes.len() > DIAGNOSTICS_PAYLOAD_CAPACITY {
        return Err(DiagnosticsError::Oversize { len: bytes.len() });
    }
    Ok(bytes)
}
```

Re-implement `encode_snapshot` over it; add `encode_health`/`decode_health` (`HealthView`) with the same doc/error contract. In the `runtime` module add `const HEALTH_SERVICE: &str = "datamancer/health";` and `Iceoryx2HealthPublisher`/`Iceoryx2HealthSubscriber` — byte-for-byte mirrors of the diagnostics pair (`publish_subscribe::<[u8]>`, `history_size(1)`, `initial_max_slice_len(DIAGNOSTICS_PAYLOAD_CAPACITY)`, subscriber drains to latest), parameterized only by service name and codec fns (factor a private generic helper if it stays readable under pedantic clippy; otherwise accept the mirrored code). Re-export both from the crate's `lib.rs` beside the diagnostics pair, and via `datamancer::transport`.

- [ ] **Step 4: Publish from the daemon ticker** (`server.rs`). Extend `spawn_diagnostics`:

```rust
fn spawn_diagnostics(
    dm: datamancer::Datamancer,
    publisher: datamancer::transport::Iceoryx2DiagnosticsPublisher,
    health_publisher: datamancer::transport::Iceoryx2HealthPublisher,
    credential_backend: &'static str,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            match dm.snapshot().await {
                Ok(snapshot) => {
                    if let Err(e) = publisher.publish(&snapshot) {
                        tracing::warn!(error = %e, "diagnostics publish failed");
                    }
                    let mut view = datamancer::HealthView::from_snapshot(
                        &snapshot,
                        datamancer::HealthView::DEFAULT_STALE_AFTER_NS,
                    );
                    view.daemon.version = Some(env!("CARGO_PKG_VERSION").to_string());
                    view.daemon.credential_backend = Some(credential_backend.to_string());
                    if let Err(e) = health_publisher.publish(&view) {
                        tracing::warn!(error = %e, "health publish failed");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "diagnostics snapshot failed"),
            }
        }
    })
}
```

(Adapt to the current function's exact shape — keep its logging and cadence semantics; one snapshot feeds both planes.) At the call site (:277-279) construct `Iceoryx2HealthPublisher::new(&self.node)` and pass `self.credential_backend`.

- [ ] **Step 5: Implement `watch_health()`** (app/mod.rs; same `spawn_blocking` poll-loop pattern as the data plane in `iceoryx2.rs:171-208`):

```rust
/// Push stream of daemon-stamped [`HealthView`]s (the `datamancer/health`
/// plane; the daemon publishes on its diagnostics cadence). The stream ends
/// if the subscription fails; drop the stream to stop the poll task.
pub type HealthStream = tokio_stream::wrappers::ReceiverStream<HealthView>;

impl AppHandle {
    /// Subscribe to pushed health views. Late joiners immediately receive
    /// the most recent view (`history_size(1)`).
    pub fn watch_health(&self) -> HealthStream {
        let (tx, rx) = tokio::sync::mpsc::channel::<HealthView>(4);
        tokio::task::spawn_blocking(move || {
            let Ok(node) = iceoryx2::prelude::NodeBuilder::new()
                .create::<iceoryx2::prelude::ipc_threadsafe::Service>()
            else {
                return; // stream ends; caller observes termination
            };
            let Ok(subscriber) =
                datamancer_transport_iceoryx2::Iceoryx2HealthSubscriber::open(&node)
            else {
                return;
            };
            while !tx.is_closed() {
                match subscriber.receive() {
                    Ok(Some(view)) => {
                        if tx.blocking_send(view).is_err() {
                            return;
                        }
                    }
                    Ok(None) => std::thread::sleep(std::time::Duration::from_millis(100)),
                    Err(_) => return,
                }
            }
        });
        tokio_stream::wrappers::ReceiverStream::new(rx)
    }
}
```

(Match the crate's existing poll-interval configuration — if `Iceoryx2Config::poll_interval` is reachable from `AppHandle`, use it instead of the literal 100ms; reuse the existing iceoryx2/tokio-stream imports and error-handling idioms in that file. If pedantic clippy objects to the silent `else return`s, log via the crate's established mechanism — but never panic.)

- [ ] **Step 6: Run to verify pass**

Run: `cargo test -p datamancer-transport-iceoryx2 && cargo build -p datamancer-client --features app && cargo clippy --all-targets --all-features -- -D warnings`
Expected: PASS (live-runtime behavior covered by Task 11's e2e).

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat: health push plane (datamancer/health) + AppHandle::watch_health"
```

---

### Task 8: Surface the `PingFailure` reason in `ReadyDiagnosis`

**Files:**
- Modify: `crates/datamancer-client/src/app/error.rs:9-21` (`ReadyDiagnosis`)
- Modify: `crates/datamancer-client/src/app/lifecycle.rs:24-26, 71-113` (drop the `dead_code` allow; thread the reason)

**Interfaces:**
- Produces: `ReadyDiagnosis::Unresponsive { last_ping_failure: Option<String> }` (breaking enum change — app-facing contract, sanctioned by the 0.5.0 bump).

- [ ] **Step 1: Write the failing test** (`lifecycle.rs` tests, using the existing `ScriptedEndpoint`/`fail()` fakes — extend `fail()` or add a variant that returns a distinctive reason):

```rust
#[tokio::test]
async fn unresponsive_diagnosis_carries_last_ping_failure() {
    // Endpoint never answers; spawned proc never exits → Unresponsive,
    // and the last probe's reason must surface.
    let endpoint = ScriptedEndpoint::new(vec![Err(PingFailure("connect refused (test)".into()))]);
    let spawner = ScriptedSpawner::never_exits();
    let cfg = ensure_cfg_with_tiny_timeout();
    let err = ensure_daemon(&endpoint, &spawner, &cfg, Path::new("/nope")).await.unwrap_err();
    match err {
        EnsureError::ReadyTimeout {
            diagnosis: ReadyDiagnosis::Unresponsive { last_ping_failure },
            ..
        } => assert_eq!(last_ping_failure.as_deref(), Some("connect refused (test)")),
        other => panic!("expected Unresponsive with reason, got {other:?}"),
    }
}
```

(Reuse/extend the existing fake constructors and the existing timeout-test's `EnsureConfig` — mirror `timeout→Unresponsive`'s setup; names above are illustrative of the *existing* test module's helpers.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-client --features app unresponsive_diagnosis`
Expected: FAIL — variant has no field.

- [ ] **Step 3: Implement**

`error.rs`:

```rust
    /// The daemon process (if we spawned one) never exited, but no probe
    /// succeeded before the deadline. `last_ping_failure` is the final
    /// probe's diagnostic reason (connect refused, stale socket, bad reply…),
    /// `None` only if no probe ran.
    Unresponsive { last_ping_failure: Option<String> },
```

Update its `Display` to include the reason when present. `lifecycle.rs`: remove the `#[allow(dead_code)]` on `PingFailure`; in `ensure_daemon` accumulate the reason:

```rust
    let mut last_failure: Option<PingFailure> = None;
    loop {
        match endpoint.ping(socket, PROBE_TIMEOUT).await {
            Ok(hello) => return Ok(hello),
            Err(f) => last_failure = Some(f),
        }
        ...
                None => ReadyDiagnosis::Unresponsive {
                    last_ping_failure: last_failure.take().map(|f| f.0),
                },
```

(The pre-spawn fast-path probe at line 77 keeps `if let Ok` — its failure is expected and immediately superseded.) Update existing tests matching `ReadyDiagnosis::Unresponsive`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancer-client --features app`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(datamancer-client): surface last ping-failure reason in Unresponsive diagnosis"
```

---

### Task 9: Structured logging (`[log]` section, JSON format)

**Files:**
- Modify: `crates/datamancerd/src/config.rs` (add `LogConfig`; `Config` gains `#[serde(default)] pub log: LogConfig`)
- Modify: `crates/datamancerd/src/config_class.rs:20-35` (classification) and the `FULL` fixture (:108-156)
- Modify: `crates/datamancerd/src/main.rs:54-86`
- Modify: `crates/datamancerd/Cargo.toml` (`tracing-subscriber` gains the `json` feature)
- Modify: `crates/datamancerd/README.md` (config schema table)

**Interfaces:**
- Produces: `[log] level = "info" | any EnvFilter directive, format = "text" | "json"`, both Cold. `RUST_LOG` overrides `level` (env wins — operator escape hatch, documented).

- [ ] **Step 1: Write the failing tests**

`config.rs` tests (follow the existing config-parsing test style):

```rust
#[test]
fn log_section_defaults_and_parses() {
    let config = Config::parse("").expect("empty config is valid");
    assert_eq!(config.log.level, "info");
    assert_eq!(config.log.format, LogFormat::Text);
    let config = Config::parse("[log]\nlevel = \"debug\"\nformat = \"json\"\n").expect("parse");
    assert_eq!(config.log.level, "debug");
    assert_eq!(config.log.format, LogFormat::Json);
}
```

`config_class.rs`: add `[log]` to the `FULL` fixture (`level = "debug"`, `format = "json"`) — the existing `every_config_field_is_classified` test then fails until the table has the entry; also extend `provider_fields_are_hot_everything_else_cold` with `assert_eq!(classify("log.format"), Some(FieldClass::Cold));`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancerd log_section && cargo test -p datamancerd every_config_field_is_classified`
Expected: FAIL.

- [ ] **Step 3: Implement config**

`config.rs`:

```rust
/// Logging configuration. Both fields are cold (subscriber installs once at
/// boot). `RUST_LOG` overrides `level` when set — the operator escape hatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct LogConfig {
    /// `tracing_subscriber::EnvFilter` directive (e.g. `"info"`,
    /// `"datamancerd=debug,info"`).
    pub level: String,
    /// Output format: human `text` (default) or newline-delimited `json`.
    pub format: LogFormat,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self { level: "info".to_string(), format: LogFormat::Text }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LogFormat {
    #[default]
    Text,
    Json,
}
```

(Match the file's existing serde/derive conventions — check whether sibling sections use `deny_unknown_fields`.) `Config` gains `#[serde(default)] pub log: LogConfig`. `config_class.rs` CLASSIFICATION gains `("log.", FieldClass::Cold)`.

- [ ] **Step 4: Implement the boot peek** in `main.rs`. Logging must be configured before `run()` logs anything, but the config file is only resolved after the single-instance lock — so peek read-only (no scaffolding, no lock; a missing/unreadable/invalid file yields defaults):

```rust
/// Best-effort read of just the `[log]` section, before the lock/scaffold
/// path runs. Never fails: any problem falls back to defaults, and the
/// real `Config::load` reports it properly later.
fn peek_log_config(explicit: Option<&std::path::Path>) -> config::LogConfig {
    #[derive(serde::Deserialize, Default)]
    struct Peek {
        #[serde(default)]
        log: config::LogConfig,
    }
    let path = match explicit {
        Some(p) => p.to_path_buf(),
        None => match paths::default_config_path() {
            Ok(p) => p,
            Err(_) => return config::LogConfig::default(),
        },
    };
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| toml::from_str::<Peek>(&s).ok())
        .map(|p| p.log)
        .unwrap_or_default()
}
```

(`paths::default_config_path()` = the non-scaffolding path computation `resolve_config_path` already performs — factor it out of `paths.rs` if it doesn't exist as a standalone fn. The `Peek` struct must **not** deny unknown fields.) `main()` becomes:

```rust
#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();
    let log = peek_log_config(args.config.as_deref());
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log.level.clone()));
    match log.format {
        config::LogFormat::Text => tracing_subscriber::fmt().with_env_filter(filter).init(),
        config::LogFormat::Json => tracing_subscriber::fmt().json().with_env_filter(filter).init(),
    }
    match run(args).await {
        ...
```

(`run()` now takes `args` instead of re-parsing.) `Cargo.toml`: `tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }`. Document `[log]` + the `RUST_LOG`-wins rule in `README.md`'s config table. Add a unit test for `peek_log_config` with a tempfile (valid section, missing file → default, unparseable file → default).

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p datamancerd && cargo run -p datamancerd -- --help`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(datamancerd): [log] config section — level + text/json format, cold-classified"
```

---

### Task 10: Web `/api/health` on the core `HealthView`

**Files:**
- Modify: `crates/datamancerd/src/web/handlers.rs:71-107` (replace the local `HealthView`/`ProviderHealth`)
- Modify: `crates/datamancerd/src/web/state.rs` / `web/mod.rs` (thread `credential_backend` into `WebState`)
- Check/Modify: web UI assets consuming `/api/health` (grep the `web_ui`/assets sources for `api/health` and `connection_state` usage; update field access)

**Interfaces:**
- Consumes: core `HealthView` (Task 3).
- Produces: `GET /api/health` → `{"ready": bool, "health": <core HealthView>}`. `ready` = at least one **enabled** provider and every enabled provider `Connected` (disabled providers no longer block readiness — they used to read `Unknown` and fail the old all-connected rule).

- [ ] **Step 1: Write the failing test** (handlers.rs has testable pure builders — mirror how `SessionsView::from_snapshot` is tested):

```rust
#[test]
fn health_envelope_ignores_disabled_providers_for_readiness() {
    let snap = SystemSnapshot::new(
        Timestamp(1_000),
        vec![
            ProviderSnapshot::new(ProviderId::from_static("on"), ConnectionState::Connected,
                0, 0, 0, 0, 0, 0, 0, 0, None),
            ProviderSnapshot::new(ProviderId::from_static("off"), ConnectionState::Unknown,
                0, 0, 0, 0, 0, 0, 0, 0, None).with_enabled(false),
        ],
        CacheSnapshot::new(vec![], None),
        vec![], vec![],
    );
    let env = HealthEnvelope::from_snapshot(&snap, "keychain");
    assert!(env.ready); // the disabled provider does not block readiness
    assert_eq!(env.health.schema_version, 2);
    assert_eq!(env.health.daemon.credential_backend.as_deref(), Some("keychain"));
    assert_eq!(env.health.providers[1].state, ProviderState::Disabled);
}

#[test]
fn health_envelope_not_ready_when_no_enabled_provider() {
    let snap = SystemSnapshot::new(
        Timestamp(1_000),
        vec![ProviderSnapshot::new(ProviderId::from_static("off"), ConnectionState::Unknown,
            0, 0, 0, 0, 0, 0, 0, 0, None).with_enabled(false)],
        CacheSnapshot::new(vec![], None),
        vec![], vec![],
    );
    assert!(!HealthEnvelope::from_snapshot(&snap, "file").ready);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancerd --features web-ui health_envelope`
Expected: FAIL.

- [ ] **Step 3: Implement**

Replace the local `HealthView`/`ProviderHealth` (handlers.rs:71-102) with:

```rust
/// The `/api/health` envelope: a cheap readiness boolean over the full
/// app-facing [`datamancer::HealthView`] ("one type, one reduction" — the
/// web surface is another consumer of the core reduction, not a fork).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct HealthEnvelope {
    /// `true` once every *enabled* provider reports `Connected` (and at
    /// least one is enabled). Disabled providers are deliberate and do not
    /// block readiness.
    pub ready: bool,
    pub health: datamancer::HealthView,
}

impl HealthEnvelope {
    pub(crate) fn from_snapshot(snap: &SystemSnapshot, credential_backend: &str) -> Self {
        let mut health = datamancer::HealthView::from_snapshot(
            snap,
            datamancer::HealthView::DEFAULT_STALE_AFTER_NS,
        );
        health.daemon.version = Some(env!("CARGO_PKG_VERSION").to_string());
        health.daemon.credential_backend = Some(credential_backend.to_string());
        let enabled: Vec<_> = health
            .providers
            .iter()
            .filter(|p| p.state != ProviderState::Disabled)
            .collect();
        let ready = !enabled.is_empty()
            && enabled.iter().all(|p| p.state == ProviderState::Connected);
        Self { ready, health }
    }
}

/// `GET /api/health` — readiness + the full app-facing health view.
pub(crate) async fn health(State(state): State<WebState>) -> Json<HealthEnvelope> {
    Json(HealthEnvelope::from_snapshot(
        &state.live_snapshot(),
        state.credential_backend(),
    ))
}
```

Thread `credential_backend: &'static str` into `WebState` at construction (the server already holds it — follow how other bootstrap facts reach `WebState`). Grep the web UI frontend sources for `/api/health` consumers and update them to the new shape (`ready` unchanged; per-provider state now at `health.providers[].state` with the `HealthView` wire names).

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancerd --features web-ui && cargo clippy -p datamancerd --all-features --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(datamancerd): /api/health serves the core HealthView (readiness envelope)"
```

---

### Task 11: e2e, lockstep version bumps, docs, CI gates

**Files:**
- Create: `crates/datamancerd/tests/health_observability_e2e.rs`
- Modify: `crates/datamancer-core/Cargo.toml` (0.1.0 → 0.2.0), `crates/datamancer/Cargo.toml` (0.4.0 → 0.5.0), `crates/datamancer-transport-iceoryx2/Cargo.toml` (0.1.0 → 0.2.0), `crates/datamancer-client/Cargo.toml` + `crates/datamancerd/Cargo.toml` (0.4.0 → 0.5.0, lockstep)
- Modify: `crates/datamancerd/README.md` (health op, `datamancer/health` service, `[log]`), root `CLAUDE.md` (one-line updates: health op + push plane + `[log]` in the datamancerd bullet; HealthView v2 in the core bullet), `crates/datamancer-client/CLAUDE.md` (add `health`/`watch_health` to the facade op list)

**Interfaces:**
- Consumes: everything above. The e2e is the cycle's acceptance test.

- [ ] **Step 1: Write the e2e** (`#[ignore]`d, mirroring `config_service_e2e.rs`'s fixtures: `write_config_no_providers`-style TOML with `service_prefix = "health-e2e"`, `publish_interval_ms = 200`, scrubbed `ALPACA_*` env, `DATAMANCER_CREDENTIALS_FILE` pinned to the tempdir, the shared `stop_daemon()` pid-from-lockfile helper):

```rust
//! Cycle-4 health/observability e2e: spawn the real daemon, assert the
//! health op, disabled-provider enrichment, hot enable, and the push plane.
//! Needs a live iceoryx2 runtime:
//! `cargo test -p datamancerd --test health_observability_e2e -- --ignored --test-threads=1`

#[tokio::test]
#[ignore = "needs a live iceoryx2 runtime and spawns the daemon binary"]
async fn health_reflects_disabled_enabled_and_pushes() {
    // 1. Spawn with zero [provider.*] sections.
    // 2. AppHandle::ensure → health():
    //    - schema_version == 2
    //    - daemon.version == daemon crate version, credential_backend set
    //    - every provider ProviderState::Disabled
    // 3. configure-provider alpaca_crypto {account_type: "paper"} → health():
    //    - alpaca_crypto no longer Disabled (Connecting/Connected/…)
    //    - alpaca (untouched) still Disabled
    // 4. watch_health(): a view arrives within 3s (publish cadence 200ms)
    //    and carries the same schema_version and provider states.
    // 5. shutdown_daemon; drop.
}
```

Implement fully with the sibling files' helpers (this file's assertions are the four numbered blocks — no `todo!()`s; copy `ensure_cfg`/`stop_daemon` per the local pattern, don't import across test binaries).

- [ ] **Step 2: Run the fast suites, then the e2e**

Run: `cargo test --workspace --all-features` → PASS.
Run: `cargo test -p datamancerd --test health_observability_e2e -- --ignored --test-threads=1` → PASS (needs the iceoryx2 runtime; also re-run the existing `#[ignore]`d `daemon_e2e`, `app_facade_e2e`, `config_service_e2e` — the `ProviderDisconnected` wire change and health additions must not regress them).

- [ ] **Step 3: Version bumps + lockstep guard**

Apply the five bumps listed above. Run: `cargo test -p datamancerd daemon_and_client_versions_stay_in_lockstep` → PASS. Check for any intra-workspace `version =` requirements on path deps and bump those references too.

- [ ] **Step 4: Docs**

- `crates/datamancerd/README.md`: `health` op request/reply JSON (ungated, UDS-only, daemon-stamped, `schema_version`), the `datamancer/health` iceoryx2 service (cadence = `diagnostics.publish_interval_ms`, 1 MiB cap, `history_size(1)`), `[log]` schema + `RUST_LOG`-wins note, `/api/health` envelope shape.
- Root `CLAUDE.md`: update the `datamancer-core` bullet (HealthView v2: `Disabled`/`Gapped`/`Backfilling`, schema 2) and the `datamancerd` bullet (health op + push plane + structured logging).
- `crates/datamancer-client/CLAUDE.md`: add `health` to the facade's op list; note `watch_health` is a read-only plane subscription, not a new protocol op.

- [ ] **Step 5: CI gates**

```bash
git fetch origin main
cargo deny check
.github/scripts/semver-checks.sh origin/main
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --check
```

Expected: all clean (semver-checks passes *because* of the coordinated bumps).

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "test(datamancerd): health/observability e2e; docs and 0.5.0 lockstep bumps for cycle 4"
```

---

## Self-Review

**Spec coverage:** "Real health model feeding HealthView (companion-process states, per-symbol liveness)" → Tasks 1–5 (Unauthenticated produced; CompanionUnreachable stays reserved + golden-tested per the appendix). "`ping` op with version info" → shipped in cycle 1; its residual (unsurfaced PingFailure) → Task 8. "`watch_health()` push stream" → Task 7. "SCHEMA_VERSION … in the wire envelope" → Tasks 3+6+7 (daemon-stamped views on both pull and push paths). "Structured logging" → Task 9. Deferred cycle-3 disabled-provider enrichment → Tasks 2–4. Golden tests incl. synthetic fixtures → Task 3. Web unification (user-approved scope add) → Task 10. Staleness-boundary residual → Task 3.

**Known adaptation points (deliberate, flagged in-task):** exact `Reply` constructor idiom (Task 6), `control_request` helper name (Task 6), `spawn_diagnostics` current shape (Task 7), poll-interval plumbing in `watch_health` (Task 7), fake-helper names in the lifecycle test (Task 8), `default_config_path` factoring (Task 9), `WebState` construction path (Task 10). Each names the file/line to mirror; none is a design decision.

**Type consistency:** `DisconnectCause` (T1) consumed by T4/T5; `with_enabled`/`with_gaps`/`with_backfilling` (T2) consumed by T3 tests and T4 assembler; `HealthView::from_snapshot(&SystemSnapshot, i64)` signature unchanged and used identically in T6/T7/T10; `Reply::health(HealthView)` (T6) used by the T11 e2e; `Iceoryx2HealthPublisher/Subscriber` names match between T7's transport and daemon/client steps.
