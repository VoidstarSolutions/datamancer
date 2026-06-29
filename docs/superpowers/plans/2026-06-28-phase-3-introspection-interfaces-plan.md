# Phase 3 — Library introspection interfaces

**Fidelity:** high

_Part of the datamancer standalone-server roadmap. See `docs/superpowers/specs/2026-06-28-datamancer-server-roadmap.md`._

---

> **Reconciliation pass — authoritative; supersedes any conflicting text below.** Applied from the [cross-phase consistency report](2026-06-28-server-plan-consistency-report.md). Architect decisions: registry/ids/stats are built in **Phase 2** (Issue 3); the diagnostics snapshot is **split** (Issue 6).
>
> Resolutions affecting this phase:
> - **Read registry/stats from Phase 2 (Issue 3):** Phase 2 owns the client-session registry, `ClientSessionId`, and the per-symbol `LiveStats` on `AuthoritativeSession`. This phase only iterates/reads them. Drop any "attach `LiveStats` to `RegistrySentinel`" instruction — `RegistrySentinel` no longer exists; target `AuthoritativeSession`.
> - **Canonical snapshot names:** this phase produces `SystemSnapshot` and `Datamancer::snapshot() -> Result<SystemSnapshot>` (async, fallible). These are canonical; Phase 6 is corrected to match.
> - **Resume-buffer snapshot placement (Issue 4):** the resume buffer is per-client; put `ResumeBufferSnapshot` and per-instrument `gap_count`/`dropped_events` on `ClientSessionSnapshot`, not `AuthoritativeSessionSnapshot`.
> - **Split the snapshot (Issue 6 — decided: split):** provide a small, bounded **live-state snapshot** (sessions, subscriptions, `LiveStats`, latency, resume occupancy) distinct from the heavier **cache catalog** (`Vec<CacheCatalogEntry>`). Phase 4's fixed-size fast diagnostics service carries the live-state snapshot; the cache catalog goes on a separate slower/chunked service. Document an expected size bound for the live-state variant.

> **Detailed-planning hardening (gotcha pass, 2026-06-28) — authoritative.** Adversarial code-level review against `traits/storage.rs`, `storage/surreal.rs`, `providers/alpaca*.rs`, `event.rs`. Supersedes conflicting body text.
>
> **Locked decision — provider metrics scope:** add `Provider::metrics(&self) -> Option<Arc<dyn ProviderMetrics>>` **defaulted to `None`** (object-safe; existing providers compile unchanged). Ship cold-site accounting + the seam now; **Alpaca's throughput/rate-limit impl is deferred** to a later provider pass (those fields read `None`).
>
> **Layering & serde:** `struct ClientSessionId(pub u64)` and all snapshot types live in `datamancer-core` (`snapshot.rs`), `#[non_exhaustive]`, serde-derived; forward-compat = add optional fields only. Add `Serialize/Deserialize` to `Seq`, `EventKind`, `GapSpan`, `BarInterval`, `Adjustment`, `CacheKey` (confirm/add for `Instrument`); add an `Adjustment` token-parse inverse (`from_token`) for catalog ids. Coordinate the `Seq` serde + `SYNTHETIC` const edits with Phase 1's `event.rs` change (one combined edit, no collision).
>
> **Two accessors (the split):** a **fast, infallible, in-memory live-state accessor** and a **slow, fallible cache-catalog accessor**; `snapshot()` composes both, but the fast path stands alone for Phase 4's fast diagnostics service and Phase 6's ArcSwap (no catalog I/O on the hot read).
>
> **`snapshot()` discipline:** hold the registry mutex only to upgrade `Weak`s, clone `Arc<LiveStats>`, read `strong_count`; **release before any `.await`** (never across `catalog().await`). Sampled per-symbol view (Relaxed atomics), non-transactional / no cross-symbol consistency — documented. Assembly must be panic-free under the lock (poisoning hazard, session.rs:245).
>
> **Cache catalog:** `async fn catalog(&self) -> Result<Vec<CacheCatalogEntry>>` on `HistoricalCache`, default `Ok(vec![])`; SurrealCache scans the `coverage` table; **volume is logical** (`event_count`/segments) — no filesystem byte-walk in Phase 3. Parse ids by splitting on `|` (expect 4 parts); **skip-and-log** malformed. Add `asset_class: Option<AssetClass>` to `CoverageDoc` so `Instrument` reconstructs faithfully (old rows → `None`). Verify the surrealdb 3.0 id deserialization shape during impl; benchmark a 1k-row scan and paginate if needed.
>
> **Stats wiring:** `LiveStats` = per-field atomics on the authoritative session (today's `RegistrySentinel` → `AuthoritativeSession` post-Phase-2); `seq_position` = last-assigned per symbol. `ProviderAccounting` = per-provider `Arc` in `DatamancerInner` (history_fetches, coalesced, live_starts, subscribes, unsubscribes, reconnects, connection_state, last_error), cloned into `SessionInner`; coalesced counted at the single-flight re-tile (backfill never coalesces — documented).
>
> **Live-state bound:** bounded by clients × subscriptions × const (few clients expected); document a concrete cap; Phase 4 sizes the fast service to it and chunks the catalog separately.
>
> **Sequencing:** reads Phase 2's client-session + authoritative registries → land after Phase 2. `LiveStats` can attach early (today's `RegistrySentinel`) so live-state lands even before client-session enumeration.
>
> **Tests:** provider-accounting increments (incl. coalesced; backfill non-coalesce); catalog round-trip vs known ranges + malformed-id skip + `asset_class` present; `SystemSnapshot` serde round-trip; snapshot reflects live sessions; lock-not-held-across-`await` (timeout-bounded under load); 1k-row catalog cost.

## Context & goal

Phase 3 gives the datamancer library a programmatic, **serializable** view of its
own runtime state, with **no transport, no daemon, and no web dependency**. Three
deliverables (roadmap:225-238):

1. **Provider-call accounting** — counters at the provider edge (history-fetch
   count, coalesced fetches, live reconnects, rate-limit hits, message/byte
   throughput, last error, connection state).
2. **Cache-catalog enumeration** — a new `HistoricalCache` method that *lists*
   what is cached (keys + covered ranges + volume estimate), distinct from
   today's `gaps()`/`lookup()` which answer coverage for a single key.
3. **System-state snapshot API** — one consolidated `Serialize + Deserialize`
   snapshot consolidating provider accounting, the cache catalog, and per-symbol
   live state (authoritative + client sessions, subscriptions, refcount,
   per-symbol `seq` position, last `source_ts`/`rx_ts` and `rx_ts − source_ts`
   latency, resume-buffer occupancy, gap counts).

The snapshot is the single artifact consumed three ways: the in-process embedder
reads it directly (`Datamancer::snapshot()`); the Phase-4 diagnostics plane
publishes it over iceoryx2 (roadmap:277-285); the Phase-6 UI renders it
(roadmap:342-354). Therefore the snapshot **types** live in `datamancer-core` (so
both crates and any future transport crate share them and they are serde-capable);
the **assembly logic** lives in `datamancer` (it reaches into providers, cache, and
the session registry). This matches the crate-structure decision at roadmap:382-391.

This phase is off the hot path. Its central risk is not performance but coupling
to structures Phase 1 and Phase 2 introduce (source-stamped per-symbol `seq`,
refcounted authoritative sessions, client sessions). Those couplings are called
out as RE-PLAN CHECKPOINTS below.

**Invariants honored.** Determinism is **per-symbol only** (roadmap:34-36,404-406):
the snapshot reports per-`(instrument, kind)` state and never implies a
cross-instrument order; `captured_at` plus `Relaxed` atomic reads mean fields may
skew across symbols by nanoseconds — which is fine precisely because cross-symbol
consistency is a non-goal. `rx_ts` stays **observability-only** (CLAUDE.md): the
latency field is exactly its sanctioned use and must never feed engine logic. Both
crates keep `#![forbid(unsafe_code)]` — every type added here is plain data with
derives plus atomics, no `unsafe`.

## Prerequisites & assumptions

**Depends on Phase 2** (which depends on Phase 1) per the roadmap table
(roadmap:125). Phase 3 reads live state Phase 1/2 own. Where a structure does not
exist yet, this plan states the assumed shape and marks a checkpoint. Slices A and
B carry **no** Phase-2 dependency and are sequenced first; most of Slice C's
per-symbol stats are buildable against **today's** `RegistrySentinel` + per-pair
`seq_counter` (see below), narrowing the true Phase-2 surface to refcount>1 and
client-session enumeration.

- **RE-PLAN CHECKPOINT P1-SEQ (from Phase 1).** Phase 1 moves `seq` stamping from
  `EventStream::poll_next` (session.rs:682) — which today reads
  `SessionInner.seq_counter` (an `Arc<AtomicU64>` created at session.rs:289,
  field at session.rs:544, cloned into the stream at session.rs:599) — to the
  **source** of each authoritative per-`(instrument, kind)` stream. **Note:**
  today's `seq_counter` is already *per-`(instrument, kind)` Session* and shared
  across stream re-takes, so it is nearly per-symbol; Phase 1 changes *where* it
  is incremented, not its `Arc<AtomicU64>` shape. Phase 3 reports "per-symbol
  `seq` position" with a `Relaxed` load of that counter. **Confirm** the
  field/owner when Phase 1 lands; if Phase 1 keeps the counter per-consumer the
  "identical across clients" field is meaningless and this must be revisited.
  **RESOLVED (as shipped):** `seq_position` is the **last-assigned** source `seq`
  for that symbol — `LiveStats::seq_position()` returns the last stamped `seq`
  seen (`client.rs:255`), not the counter's next value.

- **RE-PLAN CHECKPOINT P2-REG (from Phase 2).** Phase 2 turns the live-session
  registry (`type LiveSessionRegistry = Arc<Mutex<HashMap<(Instrument,
  EventKind), Weak<RegistrySentinel>>>>`, session.rs:191-192; `RegistrySentinel`
  at session.rs:1538) into a registry of **refcounted shared authoritative
  sessions** and introduces a **client session** as the public handle
  (roadmap:188-192). **Assumptions Phase 3 reads (read-only):** (a) an
  authoritative-session registry whose entry exposes a cheaply-cloneable
  `Arc<LiveStats>` reachable from `DatamancerInner`, with a subscriber count
  derivable (today `Weak::strong_count`, which is 0/1 because live conflict is
  prevented; Phase 2 makes it N); and (b) a client-session registry reachable
  from `DatamancerInner` enumerating active client sessions and their
  subscription sets. **If Phase 2 keeps `Weak<RegistrySentinel>` with no attached
  state and no client registry, Phase 3 adds those itself** (see Slice C — the
  `LiveStats` attach is designed to graft onto today's `RegistrySentinel` so the
  per-symbol stats do not block on Phase 2; only refcount>1 and client
  enumeration do).
  - **Layering sub-checkpoint:** `ClientSessionId` cannot live only in
    `datamancer` if `SystemSnapshot` (in `datamancer-core`) must carry it — core
    must not depend on the orchestrator (CLAUDE.md crate split). Resolve by
    either defining `ClientSessionId` in `datamancer-core` (Phase 2 then uses the
    core type) **or** carrying a raw `u64` in `ClientSessionSnapshot`. This plan
    assumes the former and flags it for Phase 2 coordination.

- **RE-PLAN CHECKPOINT P2-RING (from Phase 2).** Resume-buffer granularity
  (per-symbol ring vs per-client-multiplexed ring) is an open Phase-2 question
  (roadmap:210; today the ring backs the per-pair session, `EventRing` at
  session.rs:1659, eviction via `into_parts`/`flush_ring` at session.rs:760-772).
  The snapshot's `resume_buffer` fields must match whichever granularity Phase 2
  chooses. **Assumption:** per-authoritative (per-symbol) `EventRing` occupancy is
  reportable. If Phase 2 moves the ring to the per-client stream (roadmap:197),
  the `ResumeBufferSnapshot` field moves from `AuthoritativeSessionSnapshot` to
  `ClientSessionSnapshot`. Revisit field placement when P2-RING resolves.

- **No Phase-2 dependency for items 1 (provider accounting) and 2 (cache
  catalog).** Both are designable and implementable against today's provider edge
  and `SurrealCache`; only their *inclusion in the consolidated snapshot* waits on
  the snapshot type, which itself only needs the Phase-2 live-state shape for
  Slice C. The plan sequences them first to de-risk.

- **serde is already a non-optional `datamancer-core` dependency** (used by
  `Timestamp`/`Instrument`/`ProviderId`/`AssetClass`/`BarInterval`, e.g.
  event.rs:25/30, instrument.rs:11). Phase 3 adds serde **derives** to the subset
  it newly references: `Seq` (event.rs:18), `EventKind` (event.rs:42),
  `GapSpan` (event.rs:163), and `Adjustment` (adjustment.rs:10) **all currently
  lack serde** — verified. This is additive and does not touch
  `MarketEvent`/`Trade`/`Quote`/`Bar`/`Control` (those are the Phase-4 wire-format
  concern). No new crate dependency is required.

## Step-by-step implementation

Three independently-testable slices, sequenced to put the Phase-2-independent
pieces first.

### Slice A — Provider-call accounting (datamancer + a core hook)

Provider methods are `dyn`-dispatched at cold call sites; the per-frame decode
loop is monomorphic inside each provider crate (provider.rs:8-15). Accounting
therefore splits across three collection points.

**Shared handle.** Add `ProviderAccounting` (a struct of `AtomicU64`/atomic state)
and hold it per provider id in `DatamancerInner` as `HashMap<ProviderId,
Arc<ProviderAccounting>>`, built in `DatamancerBuilder` for each registered
provider (`DatamancerInner.providers`, session.rs:168). **Each per-`(instrument,
kind)` controller runs `forward()`/`emit()` for its own pair only** (session.rs:1389,
1411), so the controller must hold a clone of the `Arc<ProviderAccounting>` for its
provider; thread it through `SessionInner` at construction (alongside `provider`,
session.rs:301). The cold-call increments below run on the `Session`/`Datamancer`
side where the handle is directly reachable.

1. **Call-boundary counters (cold sites in `session.rs`).**
   - `provider.start_live(...)` (session.rs:315) → `live_starts`.
   - `provider.fetch_history(...)` spawn sites (session.rs:808 historical path,
     session.rs:1040 backfill path) → `history_fetches`. **Counts upstream
     provider fetch calls, which are per gap *segment*** (`stream_segments` issues
     one per uncovered tile), not per `session()` call — document this.
   - `live.subscribe(...)` (session.rs:316) → `subscribes`;
     `unsubscribe(...)` (session.rs:1345/1356/1374, all teardown/reconnect paths)
     → `unsubscribes`. These are **call counts, not active-subscription deltas**
     (stock subscribe is full-snapshot and reconnect re-applies the full list);
     document so readers don't read call count as live-subscription count.

2. **Coalesced-fetch counter (single-flight).** `FetchLocks::acquire`
   (fetch_locks.rs:38) returns a bare `OwnedMutexGuard<()>` with **no
   contention/coalesce signal** — so detection must read the *re-tile result*, not
   the guard. In `run_historical_cached` (session.rs:1200-1226): when the initial
   `gaps()` is non-empty (we intended to fetch) but after acquiring the slot the
   re-tiled `regaps.is_empty()` (a concurrent winner already filled the range, so
   `fetch_guard` is left `None` and no upstream fetch is issued, session.rs:1222),
   increment `history_fetch_coalesced`. **Backfill does not use `FetchLocks`**
   (explicit non-wiring, session.rs:1269-1274), so its fetches always count as
   upstream and never coalesce — note this asymmetry.

3. **Stream-derived counters (in `forward()`/`emit()`, read from in-band
   `Control`).** The controller sees every event; derive per provider id by
   matching `ControlKind` (event.rs:135-161):
   - `messages` — increment per **live data** event forwarded to the consumer
     (the data-plane throughput metric). **Scope decision:** count live data only;
     do not count cache-replay/backfill-from-cache events (they are not provider
     traffic). Document the scope.
   - `reconnects` — increment on each `ControlKind::ProviderConnected` after the
     first (the first is the initial connect).
   - `connection_state` — set `Connected`/`Disconnected` from
     `ProviderConnected`/`ProviderDisconnected`.
   - `last_error` — set from `ControlKind::ProviderError { message }`.
   - `gaps_emitted` — increment on each `ControlKind::Gap` (also feeds per-symbol
     gap counts in Slice C).
   These require no provider-crate changes — exactly the in-band model the
   diagnostics driver wants (roadmap:280-282).

4. **In-provider counters (bytes, rate-limit) — additive core hook.** Byte
   throughput and rate-limit-hit counts live inside the provider's monomorphic
   loop / REST pagination (Alpaca `fetch_history_via`, websocket reconnect loop)
   and are invisible at the cold boundary and as `Control` events. Add an
   **optional, default-no-op** hook to the `Provider` trait
   (`datamancer-core/src/traits/provider.rs`, alongside the existing default
   `list_instruments` at provider.rs:78):

   ```rust
   /// Optional accounting sink. Default `None` (the provider reports nothing
   /// beyond what datamancer counts at the cold boundary / from Control events).
   fn metrics(&self) -> Option<Arc<dyn ProviderMetrics>> { None }
   ```

   where `ProviderMetrics: Send + Sync` exposes `record_bytes(u64)` /
   `record_rate_limit()` (Relaxed atomic adders). `datamancer` folds these into
   the per-provider accounting at snapshot time. **Decision:** keep the hook out
   of the hot per-message critical section — Alpaca calls `record_bytes` once per
   HTTP page / per websocket frame batch, not per decoded event. If wiring the
   Alpaca side proves larger than expected, ship Slice A with bytes/rate-limit
   reported as `None` and the hook in place — the snapshot fields are `Option`, so
   this degrades gracefully (Open Question 3).

### Slice B — Cache-catalog enumeration

Add a catalog method to `HistoricalCache`
(`datamancer-core/src/traits/storage.rs`, beside `gaps()` at storage.rs:68) that
lists every cached key with its actual covered segments and a volume estimate:

```rust
/// Enumerate everything this cache currently holds. Each entry describes one
/// stored (provider, symbol, kind, adjustment) key, the source-time segments
/// actually covered, the event count, and a best-effort volume estimate.
/// Distinct from `gaps()`/`lookup()`, which answer one key. Default returns an
/// empty catalog (backends that cannot enumerate opt out).
async fn catalog(&self) -> Result<Vec<CacheCatalogEntry>> { Ok(Vec::new()) }
```

**CacheCatalogEntry — honest about what the cache actually stores.** The coverage
record id is `"{provider}|{symbol}|{table}|{adjustment}"` (`coverage_id`,
surreal.rs:179-187) and **neither the coverage doc nor the row shapes
(`TradeRow`/`QuoteRow`/`BarRow`, surreal.rs:202-247) store `asset_class`** — it is
collapsed away. So a faithful `Instrument` (which requires `asset_class` as part of
its identity, instrument.rs:91-95) **cannot be reconstructed from existing cache
data**. The entry therefore carries the *recoverable* components, not a fabricated
`Instrument`:

```rust
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheCatalogEntry {
    pub provider: ProviderId,
    pub symbol: String,
    /// `Some` only when the backend records it (see CoverageDoc change below);
    /// `None` for rows written before this phase. Consumers needing a full
    /// `Instrument` reconstruct it when this is `Some`.
    pub asset_class: Option<AssetClass>,
    pub kind: EventKind,
    /// The adjustment the rows are *stored* under. Trades/quotes always store
    /// under `Raw` regardless of the requested mode (`effective_adjustment`,
    /// surreal.rs:172-177); only bars segregate by mode. Documented so the UI
    /// does not present `Raw` trade rows as if a mode were requested.
    pub adjustment: Adjustment,
    /// Covered source-time segments [from, to). Reuses the span type.
    pub segments: Vec<GapSpan>,
    pub event_count: u64,
    /// Best-effort logical volume estimate in bytes. None if unknown.
    pub est_bytes: Option<u64>,
}
```

**Recommended write-path change (so future catalogs are complete):** add an
`asset_class: Option<String>` field to `CoverageDoc` (surreal.rs:249-254;
SCHEMALESS table, so additive) and populate it from `key.instrument.asset_class()`
on `store`. Legacy rows deserialize it as `None` → entry `asset_class: None`. This
is the smallest change that makes the catalog able to round-trip a real
`Instrument` going forward without fabricating identity for old rows.

**Surreal `catalog()` impl** (`storage/surreal.rs`). The `coverage` table is the
authoritative "what is cached" record. Implement by selecting all coverage rows
**with their record ids** and reconstructing each key:

- Query `SELECT id, segments, event_count, asset_class FROM coverage`, mirroring
  the repo's `.query(...).take(n)` pattern (`count_events_in`, surreal.rs:514).
  **Verify the surrealdb 3.0 deserialized id shape** (typed `RecordId`/`Thing` vs
  raw string) against existing query patterns before finalizing — Open Question 2.
- Parse the id back into `(provider, symbol, table, adjustment)` by splitting on
  `|`. Add `kind_for(table) -> Option<EventKind>` as the inverse of `table_for`
  (surreal.rs:156-167). **`Adjustment` parsing requires a new inverse** — only
  `as_str` exists (adjustment.rs:29); there is **no `from_str`/`FromStr`**. Add a
  `from_token(&str) -> Option<Adjustment>` (or `FromStr`) keyed to the same tokens
  as `as_str` and unit-test the round-trip. Symbols containing `|` are not
  expected for equities/crypto; **skip-and-log malformed ids, never panic**.
- Map `segments: Vec<(i64, i64)>` → `Vec<GapSpan>`.

**Volume estimate.** No SDK API gives on-disk bytes. Use **estimate B (per-key
logical size):** `event_count × bytes_per_row(kind)`, where `bytes_per_row` is a
fixed per-row-shape constant. Rows are SCHEMALESS, so this is explicitly a
*logical* estimate of the serialized field payload (fixed `i64`/`u64` fields plus
the small `adjustment` token); it ignores index/coverage-doc/MVCC overhead and is
honestly labeled an estimate. It is cheap (no scan) and gives a per-key breakdown
the UI can sum. **Optionally** expose a separate whole-store figure as a single
`total_disk_bytes` on the snapshot's cache section (not per-entry) via a filesystem
walk of the embedded SurrealKV directory — but this **must return `None` for
non-file backends** (e.g. an in-memory `mem://` cache used in tests) and is
clearly marked as overstating live size (un-GC'd VLog/MVCC). **Recommendation:**
ship estimate B per-entry now; defer the FS walk to a follow-up if operators ask
for true footprint (Open Question 1).

**No cache `seq`.** `CacheCoverage.first_seq`/`last_seq` are hardcoded `None`
(surreal.rs:343) and replay hardcodes `Seq(0)` (surreal.rs:618/652/689); the cache
carries no usable `seq`. The catalog therefore **does not** report per-key seq —
correct, since `seq` is a live per-symbol property (Slice C), not a cache property.
State this explicitly so no one wires a meaningless cache seq into the snapshot.

### Slice C — System-state snapshot API

Define the consolidated snapshot types in **`datamancer-core`** (new module
`datamancer-core/src/snapshot.rs`, re-exported from the crate root) so they are
shared and `Serialize + Deserialize`. All structs are `#[non_exhaustive]` so
later phases (4/6) can add fields without a breaking change.

```rust
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemSnapshot {
    pub captured_at: Timestamp,            // wall-clock at assembly (observability)
    pub providers: Vec<ProviderSnapshot>,
    pub cache: CacheSnapshot,
    pub authoritative_sessions: Vec<AuthoritativeSessionSnapshot>,
    pub client_sessions: Vec<ClientSessionSnapshot>,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderSnapshot {
    pub provider: ProviderId,
    pub connection_state: ConnectionState,
    pub history_fetches: u64,
    pub history_fetch_coalesced: u64,
    pub live_starts: u64,
    pub subscribes: u64,
    pub unsubscribes: u64,
    pub reconnects: u64,
    pub rate_limit_hits: Option<u64>,      // None until the provider hook reports
    pub messages: u64,                     // live data events forwarded
    pub bytes: Option<u64>,                // None until the provider hook reports
    pub gaps_emitted: u64,
    pub last_error: Option<String>,
}

/// Derivable purely from in-band Control today. `Unknown` is the initial state
/// before any connection event is observed. (No `Reconnecting` variant: the
/// Control model exposes only Connected/Disconnected, and ProviderDisconnected
/// already documents that a reconnect is scheduled/in-flight, event.rs:139.)
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionState { Unknown, Connected, Disconnected }

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheSnapshot {
    pub entries: Vec<CacheCatalogEntry>,
    pub total_disk_bytes: Option<u64>,     // whole-store FS walk, if computed
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthoritativeSessionSnapshot {
    pub instrument: Instrument,            // live state has the full instrument
    pub kind: EventKind,
    pub subscriber_refcount: u32,          // # client sessions referencing this (P2-REG)
    pub seq_position: Option<Seq>,         // current per-symbol source seq (P1-SEQ)
    pub last_source_ts: Option<Timestamp>,
    pub last_rx_ts: Option<Timestamp>,
    pub latency_ns: Option<i64>,           // last rx_ts - source_ts (observability)
    pub gap_count: u64,                     // per-symbol provider/source Control::Gap count (LiveStats); per-client resume-buffer drops live on ClientSessionSnapshot
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResumeBufferSnapshot {
    pub capacity: usize,
    pub occupancy: usize,
    pub dropped_events: u64,               // cumulative evicted (overflow)
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientSessionSnapshot {
    pub id: ClientSessionId,               // see P2-REG layering sub-checkpoint
    pub subscriptions: Vec<SubscriptionRef>,
    pub resume_buffer: ResumeBufferSnapshot,   // per-client buffer (Phase 2); dropped_events = events this client missed
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientSessionId(pub u64);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionRef { pub instrument: Instrument, pub kind: EventKind }
```

**Live-stats collection (the new state Phase 3 reads).** Add a per-authoritative
`LiveStats` struct of atomics. Crucially, this can graft onto **today's**
structures to avoid blocking Slice C on Phase 2: attach an `Arc<LiveStats>` to
`RegistrySentinel` (session.rs:1538) and give the controller a clone. On each
forwarded live event (`forward()`, session.rs:1411) update `last_source_ts`,
`last_rx_ts` (and thus `latency_ns`); on `Control::Gap` bump `gap_count`. The
`seq_position` is the **last-assigned** source `seq`: `record_event` stores each
stamped event's `seq` into `LiveStats.last_seq`, and `seq_position()` returns it
(`client.rs:255`) — not a read of the next-to-assign counter.
Resume-buffer occupancy reads `EventRing` length; `dropped_events` is a cumulative
counter incremented on the eviction path (`flush_ring`/`into_parts`,
session.rs:756-772, and the detached-ring `push` at session.rs:1402/1431 — wire
the eviction count where the ring drops its oldest). With this attach,
**per-symbol stats (seq, last ts, latency, gaps, ring occupancy) are buildable on
today's registry**; only `subscriber_refcount > 1` and the client-session list
genuinely require Phase 2 (P2-REG).

**Assembly: `Datamancer::snapshot(&self) -> Result<SystemSnapshot>`** (async, on
`Datamancer`, session.rs). Steps:
1. Snapshot each provider's `ProviderAccounting` (Relaxed loads) + fold in
   `Provider::metrics()` counters.
2. Call `historical_cache.catalog()` (Slice B); `None` cache → empty
   `CacheSnapshot`. Returns `Result` because `catalog()` returns `Result`.
3. Iterate the authoritative-session registry: take the registry mutex **only**
   long enough to upgrade each `Weak<RegistrySentinel>`, clone its
   `Arc<LiveStats>`, and read `strong_count` for the refcount, then **release the
   lock before reading atomics or awaiting** — never hold the registry mutex
   across `.await` (mirror the discipline in `session()`, session.rs:247-258).
4. Iterate the client-session registry → `ClientSessionSnapshot` (P2-REG).
5. Stamp `captured_at` from the same wall-clock helper the controller uses
   (`wall_clock_ts`, used at session.rs:1255).

**Consistency contract.** The snapshot is a **sampled** point-in-time view, not a
transactional one: atomics use `Relaxed`, the registry lock is held only to clone
handles. Document that fields may skew by nanoseconds across symbols — acceptable
because the snapshot is diagnostic and determinism is per-symbol (cross-symbol
consistency is a non-goal, roadmap:404-406). This keeps `snapshot()` off any lock
the hot path holds across `.await`, matching the Phase-6 "snapshot via atomics,
never block the executor" guidance.

**Serialization format.** Phase 3 commits only to the types being `Serialize +
Deserialize` (round-trippable via `serde_json` in tests). The concrete wire
encoding is Phase 4's choice (diagnostics plane) and Phase 6's (JSON); Phase 3
stays format-agnostic (Open Question 4 / roadmap:247).

## Public API / type changes

**datamancer-core**
- New module `snapshot.rs` (re-exported from crate root): `SystemSnapshot`,
  `ProviderSnapshot`, `CacheSnapshot`, `AuthoritativeSessionSnapshot`,
  `ResumeBufferSnapshot`, `ClientSessionSnapshot`, `ConnectionState`,
  `ClientSessionId`, `SubscriptionRef` — all `Serialize + Deserialize`,
  `#[non_exhaustive]` on the aggregate structs/enum.
- `traits/storage.rs`: new `HistoricalCache::catalog()` (default empty); new
  `CacheCatalogEntry` type.
- `traits/provider.rs`: new optional `Provider::metrics()` hook + `ProviderMetrics`
  trait (default no-op). Additive — existing providers unaffected.
- serde derives added to `Seq`, `EventKind`, `GapSpan`, `Adjustment` (all four
  currently lack them — verified). New `Adjustment::from_token`/`FromStr` inverse
  of `as_str`. No change to `MarketEvent`/data variants.

**datamancer**
- `Datamancer::snapshot(&self) -> Result<SystemSnapshot>` (async).
- Internal: `ProviderAccounting` (per-id atomics) in `DatamancerInner` as
  `HashMap<ProviderId, Arc<ProviderAccounting>>`, the handle cloned into each
  controller via `SessionInner`; `LiveStats` (per-symbol atomics) attached to
  `RegistrySentinel`; cold-site increments at session.rs:315/316/808/1040/1345/
  1356/1374 and the coalesce increment at session.rs:1222; stream-derived
  increments in `forward()`/`emit()`.
- `SurrealCache`: `catalog()` impl + `kind_for(table)` inverse + `bytes_per_row`
  constants + `asset_class` added to `CoverageDoc` write path; optional FS-walk
  helper returning `None` for non-file backends.
- Alpaca: implement `ProviderMetrics` (bytes/rate-limit) — or ship the hook
  returning `None` first (Open Question 3).

All new types are plain data + atomics; `#![forbid(unsafe_code)]` holds in both
crates.

## Test plan

**Unit (datamancer-core)**
- `snapshot_serde_roundtrip` — build a fully-populated `SystemSnapshot` (every
  sub-type non-empty, `None` and `Some` for each optional field, `ConnectionState`
  non-`Unknown`), `serde_json` serialize→deserialize, assert equality.
  **Regression guard** that the Phase-4 diagnostics plane and Phase-6 UI can carry
  the snapshot.
- `cache_catalog_entry_serde_roundtrip` — same for `CacheCatalogEntry`
  (including `asset_class: None` and `Some`).
- `adjustment_token_roundtrip` — `from_token(a.as_str()) == Some(a)` for every
  `Adjustment`; unknown token → `None`. **Guard** for the new inverse.
- `seq_eventkind_gapspan_serde` — derives compile and round-trip.
- `default_catalog_is_empty` — a fake `HistoricalCache` not overriding `catalog()`
  returns `[]`.
- `default_provider_metrics_is_none` — a fake `Provider` returns `None` from
  `metrics()`.

**Unit / integration (datamancer)**
- `provider_accounting_counts_fetches` — drive N historical fetches (N gap
  segments) through a fake provider; assert `history_fetches == N`. **Regression
  guard** for cold-site instrumentation.
- `provider_accounting_counts_coalesced_fetches` — two byte-identical concurrent
  `Scope::Historical` fetches for one key over a cold cache; assert one upstream
  fetch + one `history_fetch_coalesced` (exercises the session.rs:1222 re-tile).
- `provider_accounting_reconnects_from_control` — feed
  `ProviderConnected`/`ProviderDisconnected`/`ProviderConnected`; assert
  `reconnects == 1`, `connection_state == Connected`.
- `provider_accounting_last_error` — feed `ProviderError`; assert `last_error`.
- `cache_catalog_roundtrips_stored_ranges` (SurrealKV, mirroring existing
  surreal cache tests) — store known trade + bar ranges under two bar
  adjustments; assert `catalog()` returns expected provider/symbol/kind/segments/
  counts, `asset_class == Some(...)` for freshly-written rows, trade entry
  `adjustment == Raw`, and non-zero `est_bytes`. **Guard** for id parse +
  `kind_for` + adjustment inverse + the `CoverageDoc.asset_class` write.
- `cache_catalog_skips_malformed_id` — inject a coverage row with a `|`-broken id;
  assert it is skipped (not panicked) and other rows still returned.
- `cache_catalog_empty_when_nothing_stored` — fresh cache → `[]`.
- `snapshot_live_stats_on_todays_registry` — open a live session, push events;
  assert `last_source_ts`/`latency_ns`/`gap_count`/`seq_position` reflect them
  **without depending on Phase 2** (validates the `RegistrySentinel` attach).
- `snapshot_reflects_authoritative_and_client_sessions` (depends on Phase 2
  fakes) — one authoritative session referenced by two client sessions; assert
  `subscriber_refcount == 2`, subscriptions enumerated, `seq_position` advances.
- `snapshot_does_not_block_under_load` — call `snapshot()` while events flow;
  assert it returns promptly and never deadlocks (guards the no-lock-across-await
  contract).

**Cross-phase regression guard.** The full existing session/stream/cache suite
must still pass (Phase 3 adds reads/atomics, changes no delivery behavior).

## Doc / invariant updates

- `crates/datamancer/README.md` — new "Introspection" section: `Datamancer::
  snapshot()`, the catalog method, accounting semantics (call counts ≠
  subscription deltas; `messages` = live data only; bytes/rate-limit are `Option`;
  volume is a *logical* estimate; the snapshot is sampled, not transactional; the
  catalog reports stored adjustment so trades/quotes always read `Raw`).
- `datamancer-core/src/traits/storage.rs` — doc `catalog()` vs `gaps()`
  (catalog = whole-cache enumeration with actual segments; gaps = one key's
  uncovered fringes/holes) and that the catalog carries no `seq`.
- `datamancer-core/src/traits/provider.rs` — doc the `metrics()` hook and that
  datamancer derives connection/reconnect/gap state from in-band `Control`, while
  the provider reports only bytes/rate-limit.
- `snapshot.rs` module docs — reaffirm `rx_ts`/`latency_ns` is **observability
  only** (CLAUDE.md) and that the snapshot is per-symbol with no cross-symbol
  ordering implied.
- **No invariant rewrites here** — those are concentrated in Phases 1–2
  (roadmap:368-380). Phase 3 only adds surface.

## Open questions

1. **Volume estimate fidelity.** Per-key logical estimate (B) only, or also a
   whole-store FS walk (A)? Recommendation: B per-entry now; A behind a follow-up.
   **Verify** whether SurrealKV 3.0 runs background compaction/GC (affects how
   badly A overstates) and confirm the embedded data directory is discoverable.
2. **surrealdb 3.0 id deserialization shape** for `SELECT id, ... FROM coverage`
   — typed `RecordId`/`Thing` vs raw string. Verify against repo query patterns
   before finalizing the id-parse path.
3. **Alpaca metrics wiring scope.** Implement `ProviderMetrics` (bytes,
   rate-limit) this phase, or land the hook `None` and wire Alpaca later? Snapshot
   fields are `Option`, so deferral degrades gracefully — decide by appetite.
4. **Snapshot history.** Point-in-time only (this plan), or a short ring of recent
   snapshots / counters-with-rates (roadmap:245)? Recommendation: point-in-time
   now; rate computation (deltas) is a UI/Phase-6 concern over successive
   snapshots.
5. **`seq_position` semantics under P1-SEQ** — RESOLVED: **last assigned** (the
   last stamped `seq` seen, `LiveStats::seq_position()`), consistent across the
   in-process reader and the transport.
6. **`ClientSessionId` ownership (P2-REG layering).** Define in `datamancer-core`
   (Phase 2 consumes it) or carry a raw `u64` in `ClientSessionSnapshot`? Resolve
   with Phase 2 to avoid a core→orchestrator dependency.

## Risks

- **Coupling to Phase 1/2 internals (primary).** `seq_position`, refcount, and
  client-session enumeration read structures Phases 1–2 own. Mitigated by
  isolating reads behind the `LiveStats` handle + registry seam, the three RE-PLAN
  CHECKPOINTS, and the `RegistrySentinel` attach that lets per-symbol stats land
  on **today's** registry (narrowing the true Phase-2 surface to refcount>1 and
  client enumeration). Slices A and B carry no Phase-2 dependency and land first.
- **Cache cannot reconstruct full `Instrument` identity.** The coverage id and row
  shapes omit `asset_class` (verified, surreal.rs:179-247). Mitigated by the
  honest `CacheCatalogEntry` shape (`provider`/`symbol`/`asset_class: Option`) plus
  the additive `CoverageDoc.asset_class` write so future rows are complete.
- **New public surface in core crates** (roadmap:252). Snapshot types + two trait
  methods bind Phase 4 (transport) and Phase 6 (UI). Mitigated by serde
  round-trip guards, `Option` fields, and `#[non_exhaustive]` on the snapshot
  aggregates.
- **Lock-across-await regression.** `snapshot()` must never hold the registry
  mutex across `.await` (the `catalog()` call awaits). Guarded by
  `snapshot_does_not_block_under_load` and the clone-then-release pattern
  mirroring session.rs:247-258.
- **id round-trip fragility in the catalog.** Reconstructing key components from
  `provider|symbol|table|adjustment` assumes `|`-free symbols and complete
  `table→EventKind` / `token→Adjustment` inverses. Guarded by
  `cache_catalog_roundtrips_stored_ranges` + `cache_catalog_skips_malformed_id`;
  malformed ids are skip-and-logged, never panic.
- **Accounting accuracy caveats** (documentation, not correctness): stock
  full-snapshot subscribe + full reconnect re-apply mean call counts overstate
  "subscription changes"; backfill fetches bypass `FetchLocks`
  (session.rs:1269-1274) so never coalesce; `history_fetches` counts per gap
  segment, not per `session()` call. All documented rather than "fixed".

## Review notes

Changes made to the draft during adversarial review (file:line claims verified
against the working tree):

- **Corrected a false API claim:** the draft said adjustment parses via an
  existing `as_str`/`from_str` round-trip. `Adjustment` (adjustment.rs:25-38) has
  **only `as_str`** — no `from_str`/`FromStr` and no serde derive. Added an
  explicit `from_token`/`FromStr` inverse + serde derive as required work, with a
  round-trip test.
- **Fixed a catalog correctness gap the draft missed:** the coverage id
  (`provider|symbol|table|adjustment`, surreal.rs:179-187) and the row shapes
  (surreal.rs:202-247) carry **no `asset_class`**, so the draft's
  "`AssetClass` defaulting the cache already relies on" is inaccurate and a
  faithful `Instrument` cannot be rebuilt. Replaced `CacheCatalogEntry.instrument:
  Instrument` with honest recoverable components (`provider`, `symbol`,
  `asset_class: Option<AssetClass>`, `kind`, `adjustment`) and added an additive
  `CoverageDoc.asset_class` write so future rows reconstruct fully.
- **Corrected the coalesce instrumentation point:** `FetchLocks::acquire`
  (fetch_locks.rs:38) returns a bare guard with no contention signal. Coalesce is
  detected at the re-tile in `run_historical_cached` (session.rs:1209-1226) when
  `initial` is non-empty but `regaps` is empty — not from the guard. Rewrote
  Slice A item 2 accordingly. Also corrected the FetchLocks location (it lives in
  `fetch_locks.rs`, not session.rs:1212-1235; 1212 is only the `acquire` call).
- **Made the accounting-handle plumbing explicit:** `forward()`/`emit()`
  (session.rs:1389/1411) run in per-`(instrument, kind)` controllers, so a shared
  `Arc<ProviderAccounting>` (keyed by `ProviderId` in `DatamancerInner`) must be
  threaded into each controller — the draft glossed this.
- **Tightened the dependency story:** noted that `LiveStats` can attach to
  **today's** `RegistrySentinel` + per-pair `seq_counter`, so most per-symbol
  stats are buildable without Phase 2; only `subscriber_refcount > 1` and
  client-session enumeration truly need P2. Added a no-Phase-2 test for this.
- **Added a layering checkpoint (P2-REG sub-item):** `ClientSessionId` in a core
  snapshot type cannot depend on a Phase-2 `datamancer`-side definition; resolve
  by defining it in core or using a raw `u64`.
- **Simplified `ConnectionState`:** dropped the unreachable `Reconnecting` variant
  (the `Control` model exposes only Connected/Disconnected; event.rs:139 already
  folds reconnect-in-flight into Disconnected) and added `Unknown` for the
  pre-connect initial state.
- **Minor corrections:** serde is already a `datamancer-core` dependency (not
  "to add"); `ProviderSnapshot.provider` uses `ProviderId` not `String`; pinned
  `messages` scope to live data; required `total_disk_bytes` to be `None` for
  non-file (mem) backends; added `#[non_exhaustive]` to snapshot aggregates;
  noted `history_fetches` counts per gap segment; added a malformed-id skip test.

**Unresolved concerns (correctly deferred to checkpoints, not guesses):**

- P1-SEQ field name/owner and `seq_position` semantics — RESOLVED: `seq_position`
  is the last-assigned source `seq` (`LiveStats::seq_position()`).
- P2-REG registry value shape (whether Phase 2 attaches state or Phase 3 must) and
  `ClientSessionId` ownership — coordinate with Phase 2.
- P2-RING resume-buffer granularity — determines whether `ResumeBufferSnapshot`
  hangs off the authoritative or client snapshot.
- surrealdb 3.0 `id` deserialization shape (Open Question 2) — verify before
  implementing the id-parse path.
