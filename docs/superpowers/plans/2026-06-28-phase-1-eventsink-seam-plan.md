# Phase 1 — EventSink seam + seq-at-source

**Fidelity:** execution-ready

_Part of the datamancer standalone-server roadmap. See `docs/superpowers/specs/2026-06-28-datamancer-server-roadmap.md`._

---

> **Reconciliation pass — authoritative; supersedes any conflicting text below.** Applied from the [cross-phase consistency report](2026-06-28-server-plan-consistency-report.md). Architect decisions: registry/ids/stats are built in **Phase 2** (Issue 3); the diagnostics snapshot is **split** into bounded live-state vs. cache catalog (Issue 6).
>
> Resolutions affecting this phase:
> - **publish signature (Issue 2):** the canonical in-process method is `async fn publish(&self, MarketEvent) -> PublishOutcome` (owned). Also define `publish_borrowed(&self, &MarketEvent) -> PublishOutcome` for sinks that serialize from a borrow (Phase 4). The borrowed-`Result` shape some drafts assume is rejected.
> - **EventSink seam location (Issue 1):** the tee + resume ring stay core-side, so every sink inherits them. Only the sink *attachment point* moves — from the authoritative controller (this phase) to the **per-client output in Phase 2**.
> - **seq sentinel (Issue 8):** the `event.rs` `Seq` doc rewrite owned by this phase also defines the synthetic-control sentinel constant `Seq::SYNTHETIC` (used by Phase 2) and documents it as exempt from per-symbol monotonicity, so the Phase 1/2/3 `event.rs` edits do not collide.

> **Detailed-planning hardening (gotcha pass, 2026-06-28) — authoritative.** Adversarial code-level review against current `session.rs` / `event.rs` / `storage/surreal_tap_log.rs`. The items below supersede conflicting body text.
>
> **Locked decisions:**
> - **Tap-log `seq` converges in Phase 1.** `seq` is stamped at the source *before* the tap-log tee, so `forward()` becomes **`stamp → tee → emit`** — the stamped event reaches both the tap log and the consumer. The surreal tap log **persists the provided `seq` (store the `u64`) and stops minting its own canonical `seq`.** Tap-log replay then reproduces the source `seq` verbatim, so Phase 2 fan-out inherits no mismatch. (This expands Phase 1 into `storage/surreal_tap_log.rs` + its tests.)
> - **`Seq::SYNTHETIC = Seq(u64::MAX)`** — one `const` in `event.rs`, unreachable by the monotonic counter, documented exempt from per-symbol monotonicity.
> - **`sink.flush()` at shutdown: log-and-swallow** in Phase 1 (in-process flush is a no-op); revisit propagation in Phase 4.
>
> **Single stamping site (Gotchas 1,2,4,12).** The controller assigns `seq` from a plain `Controller.next_seq: u64` (init `0`) at the one point an event is about to be forwarded/buffered — before `tee` and before any ring push. A plain `u64` suffices (single owning controller task; no atomic, no shared counter). **Remove `SessionInner.seq_counter` (session.rs:544, init 289), `EventStream.seq` (676) + its clone (599), and the `stamp_seq` call in `poll_next`** — `poll_next` becomes a pass-through. Consequences, now intended and documented:
> - Control events get their real `seq` at this site; the `Seq(0)` placeholders (session.rs:873/909/932/1518 and tests 1876/1886/1896) are overwritten here, not at delivery.
> - Evicted/undelivered events **are** numbered → an overflow is a real `seq` hole reported as `Control::Gap`. Flip the `EventRing` doc (1657) and `Seq` doc (event.rs:11) from "never a hole" → "a hole, reported as `Gap`".
> - Backfill live arrivals stamp at the seam in reception order (per-symbol determinism, not `source_ts` order) — add a clarifying comment in `run_backfill`.
>
> **Explicitness / guards (Gotchas 3,5,6,8,10,11):** `debug_assert!(ev.seq().is_some())` in `emit_prestamped`; document the `EventRing` FIFO (`pop_front`) invariant behind `dropped_first_seq`; fix the `resume.rs` overflow test comment (survivors keep push-time `seq`; eviction `Gap` occupies `dropped_first_seq`; the hole is real); the `Seq` doc enumerates per-symbol / stamped-at-source / identical-across-a-symbol's-consumers / controls-occupy-slots / holes-are-real / `SYNTHETIC`-exempt.
>
> **Tests:** add (a) two in-process subscribers to one authoritative stream see identical `(seq, source_ts)`; (b) tap-log replay reproduces the source `seq` (convergence guard, round-trip through the surreal tap log); (c) the total-eviction (empty-survivor) assertion. Update the `resume.rs` overflow test to the real-hole invariant. Existing session/stream + tap-log suites are the regression guards.
>
> **Deferred-but-noted:** registry-mutex panic-safety (session.rs:245) is pre-existing and untouched.

## Context & goal

Phase 1 of the standalone-server roadmap. Two coupled changes, both behind the
existing in-process `Stream` API (no new transport, no new public consumer
handle yet):

1. **Introduce the `EventSink` seam** (`datamancer-core`) and make today's
   in-process delivery one implementation of it. The tap-log tee and the resume
   buffer (`EventRing`) stay **core-side** of the sink (owned by the controller),
   so every future sink (iceoryx2 in Phase 4) inherits them.
2. **Move `seq` stamping from `EventStream::poll_next` to the source** — the
   authoritative per-`(instrument, kind)` controller — so `seq` becomes a
   property of the shared stream, assigned once, in the canonical delivery order
   of that authoritative session, before any sink. This lands the per-symbol
   `seq` invariant doc change.

The load-bearing semantic consequence (embraced, not avoided): an event that is
**evicted from the resume buffer or missed on a late join now produces a real
hole in `seq`**, surfaced in-band as `Control::Gap`. Today the delivered stream
is contiguous-by-construction because undelivered events are never numbered;
after Phase 1 a per-symbol `seq` hole is a legitimate, reported loss.

Determinism is **per-symbol only**. There is no fan-out yet (Phase 2) and no
cross-instrument ordering (a permanent non-goal). The authoritative unit is the
existing per-pair controller; the existing one-live-session-per-pair registry
(`RegistrySentinel`, `Session._registry_anchor`, session.rs:515-519) already
enforces the singleton. `seq` is stamped by exactly one task — the controller —
so a single-writer counter is all that is needed at the source.

Primary file: `crates/datamancer/src/session.rs`. Trait + docs:
`crates/datamancer-core/src/{event.rs,traits/}`, `crates/datamancer/README.md`,
root `CLAUDE.md`.

## Prerequisites / assumptions

- **No dependency on earlier phases** (Phase 1 is the root). Both crates keep
  `#![forbid(unsafe_code)]`; nothing here needs `unsafe`.
- **Verified against current code** (commit on `feat/single-flight-cache-fetch`):
  - `seq` is stamped today in `EventStream::poll_next` (session.rs:686-692) from
    `EventStream.seq: Arc<AtomicU64>`, cloned from `SessionInner.seq_counter`
    (session.rs:544, 599). The controller does **not** read the counter today.
  - `Sink::Attached(mpsc::Sender<MarketEvent>)` / `Sink::Detached(EventRing)`
    (session.rs:720-723). Single chokepoints: `emit` (session.rs:1389) and
    `forward` (session.rs:1411, = `tee` then `emit`).
  - `flush_ring` (session.rs:759) is called from exactly two sites — resume
    re-attach (`handle_command`/`Take`, session.rs:1480) and the backfill seam
    (`run_backfill`, session.rs:1319).
  - `EventRing` (session.rs:1659) tracks `dropped: Option<GapSpan>` via
    `note_drop` (session.rs:1688); `into_parts` (session.rs:1713).
  - `MarketEvent::seq(&self) -> Option<Seq>` exists (event.rs:77).
  - `async-trait` and `futures` are already deps of `datamancer-core`
    (Cargo.toml:8-9; used in `traits/storage.rs`). No new dependency.
  - Historical scope has **no** registry participation
    (`_registry_anchor: None`, session.rs:516), so two independent historical
    sessions for the same pair are constructible — the basis for the agreement
    test below.

- **Assumption A — single consumer in Phase 1.** There is still exactly one
  outstanding `EventStream` per session (multi-shot re-take, but not concurrent
  fan-out). True multi-subscriber fan-out arrives in Phase 2.
  - **RE-PLAN CHECKPOINT (two-subscriber test):** The roadmap names a
    "two in-process subscribers to one authoritative instrument see identical
    `(seq, source_ts)`" test. That literal shape needs Phase-2 fan-out
    (refcounted shared authoritative session + multiplex) and **cannot be
    written in Phase 1** — the registry rejects a second live session for the
    same pair, and `take_events` is single-outstanding (`prepare_attach` returns
    `Error::EventsAlreadyTaken` while a stream is open, session.rs:741-745).
    Phase 1 ships the *realizable* agreement guard instead (two independent
    **historical** sessions over byte-identical input produce identical
    `(seq, source_ts)` sequences — see Test plan), which proves `seq` is now a
    deterministic function of source order rather than of per-consumer poll
    timing. Revisit in Phase 2 to add the live fan-out agreement test.

- **Assumption B — `EventSink` trait signature refines the 2026-06-14 sketch.**
  The design doc sketches `async fn publish(&self, ev: &MarketEvent)`. The
  in-process resume-buffer path needs to **recover ownership of a rejected
  event** to divert it into the ring (mpsc `send` already returns the value in
  `SendError`, exploited today at session.rs:1393). A `&` signature would force
  a hot-path clone on every event and cannot hand the event back. Phase 1
  therefore adopts an **owned-with-return** signature (below). Deliberate,
  documented refinement of the sketch.
  - **RE-PLAN CHECKPOINT (sink signature sign-off):** confirm the owned-return
    signature is acceptable before Phase 4 builds the iceoryx2 sink against it;
    iceoryx2 serializes from a borrow, so if a `&`-based path is later preferred
    it can be added as an **additional** method (`publish_borrowed`) without
    breaking the in-process sink.

## Step-by-step implementation

### Step 1 — Add the `EventSink` trait (`datamancer-core`)

New module `crates/datamancer-core/src/traits/sink.rs`:

```rust
use async_trait::async_trait;
use crate::{error::Result, event::MarketEvent};

/// Receives an authoritative per-(instrument,kind) session's events in `seq`
/// order. Implementations own their transport (an in-process channel, an
/// iceoryx2 publisher, ...). `seq` is already stamped at the source before
/// `publish`; the sink must preserve order and never renumber.
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Publish one fully-formed, `seq`-stamped event in delivery order.
    /// Returns `PublishOutcome::Rejected(ev)` (handing the event back) when the
    /// transport cannot accept it — e.g. the in-process consumer dropped its
    /// stream — so the caller can divert it to the resume buffer.
    async fn publish(&self, ev: MarketEvent) -> PublishOutcome;

    /// Flush any transport-side buffering (shutdown ordering). The core-side
    /// resume buffer is NOT the sink's concern — the controller flushes that.
    async fn flush(&self) -> Result<()>;
}

#[derive(Debug)]
pub enum PublishOutcome {
    Delivered,
    /// Transport refused the event; ownership returned for buffering.
    Rejected(MarketEvent),
}
```

- Register `pub mod sink;` in `crates/datamancer-core/src/traits/mod.rs` and
  re-export `EventSink`, `PublishOutcome` there and in
  `crates/datamancer-core/src/lib.rs` (alongside `TapLog`, `HistoricalCache`).
- No new dependency (Step-0 verification confirmed `async-trait` + `futures`).

### Step 2 — In-process sink wrapping the channel (`datamancer`)

In `session.rs`, define a sink type that owns the consumer-facing
`mpsc::Sender<MarketEvent>` (today's `Sink::Attached` payload):

```rust
struct InProcessSink {
    tx: mpsc::Sender<MarketEvent>,
}

#[async_trait]
impl EventSink for InProcessSink {
    async fn publish(&self, ev: MarketEvent) -> PublishOutcome {
        match self.tx.send(ev).await {
            Ok(()) => PublishOutcome::Delivered,
            Err(tokio::sync::mpsc::error::SendError(ev)) => PublishOutcome::Rejected(ev),
        }
    }
    async fn flush(&self) -> Result<()> { Ok(()) } // immediate; no transport buffer
}
```

Change the `Sink::Attached` arm to hold the sink; the resume buffer stays the
core-side `Detached` arm:

```rust
enum Sink {
    Attached(InProcessSink),   // was Attached(mpsc::Sender<MarketEvent>)
    Detached(EventRing),
}
```

This satisfies "in-process delivery is a sink wrapping the channel," and
`Detached(EventRing)` keeps the resume buffer **core-side** (the controller owns
attach/detach/buffering; the sink only delivers).

Touch points for the wrapper type change (all currently match on the old
`Sender` payload):
- `prepare_attach` (session.rs:746-747) builds `Sink::Attached(InProcessSink { tx })`.
- The closed-checks at session.rs:741-742, 880-881, 1390-1391 become
  `s.tx.is_closed()` against the `InProcessSink`.
- The `tx.clone()` in `finish_historical` (session.rs:880) becomes `s.tx.clone()`.

### Step 3 — Move `seq` to the source (the central change)

**Model:** `seq` is assigned by the controller, **once per event, in the
canonical delivery order of this authoritative session's single stream**, before
the event reaches the sink. Because exactly one task (the controller) stamps,
replace the shared `Arc<AtomicU64>` with a plain single-writer counter on the
controller. This structurally encodes "stamped once at the source":

- **Remove** `SessionInner.seq_counter` (session.rs:544) and its init
  (session.rs:289).
- **Remove** `EventStream.seq` (session.rs:676) and the clone into it
  (session.rs:599).
- **Add** `next_seq: u64` to `Controller` (session.rs:725-734), initialized to
  `0` at construction (session.rs:299-307).

Add a controller helper and a delivery primitive (factored so the stamping and
non-stamping paths share the attach/detach/buffer logic — avoids two
near-identical functions, the duplication the original risk section flagged):

```rust
fn stamp(&mut self, ev: MarketEvent) -> MarketEvent {
    let seq = Seq(self.next_seq);
    self.next_seq += 1;
    stamp_seq(ev, seq) // existing fn, session.rs:1740
}

/// Hand a fully-formed event to the sink; on rejection or while detached,
/// buffer into the resume ring. Does NOT stamp — callers stamp (or not) first.
async fn deliver(&mut self, ev: MarketEvent) {
    let ev = match &self.sink {
        Sink::Attached(s) if !s.tx.is_closed() => match s.publish(ev).await {
            PublishOutcome::Delivered => return,
            PublishOutcome::Rejected(ev) => ev,
        },
        _ => ev,
    };
    if matches!(self.sink, Sink::Attached(_)) {
        self.sink = Sink::Detached(EventRing::new(self.ring_capacity));
    }
    if let Sink::Detached(ring) = &mut self.sink { ring.push(ev); }
}

async fn emit(&mut self, ev: MarketEvent) {
    let ev = self.stamp(ev);
    self.deliver(ev).await;
}

/// Replay an already-stamped buffered event without renumbering it.
async fn emit_prestamped(&mut self, ev: MarketEvent) {
    self.deliver(ev).await;
}
```

`emit` stamps **before** the ring push, so evicted events are now numbered →
overflow becomes a real `seq` hole (the intended invariant). For the **detached
resume path this is correct**: while detached there is no other producer into
the outbound order, so push order == canonical delivery order; survivors keep
their push-time `seq` with a hole where evicted events were.

**Post-implementation note:** convergence landed in this phase, so the original
"unchanged `forward`" plan below was superseded. As shipped, `forward`
(session.rs:1834) is `stamp(ev) → tee(&ev) → deliver(ev)` — it stamps the source
`seq` **before** the tee, so the tap log records the source `seq` verbatim and
tap-log replay reproduces the delivered stream's `seq`. (The one remaining
pre-stamp tee is the backfill-seam `buffer_live_arrival` path, tracked
separately.)

### Step 4 — The two ring-flush paths (the resume × backfill split)

`flush_ring` (session.rs:759) is reached from two sites whose rings are in
**different stamping states** after Step 3. Split it. One half is behavior-
identical to today's `flush_ring`; the other is new.

- **Backfill seam** (`run_backfill`, session.rs:1319). The `pending` ring is
  filled by `buffer_live_arrival` (session.rs:1421-1436) via a raw `p.push(ev)`
  — **unstamped**. Keep this raw (do **not** add stamping to
  `buffer_live_arrival`): deferring their `seq` to the seam is what places live
  arrivals *after* all backfill segments in canonical order and preserves
  monotonicity. The flush is therefore **identical to today's `flush_ring`**
  (eviction `Gap` via `emit_gap`, then survivors via `emit` — both stamp).
  Rename `flush_ring` → `flush_backfill_pending` for call-site clarity; body
  unchanged. Because backfill segments direct-emit via `emit` (session.rs:1022,
  1070) and this flush is **sequential after** backfill, the counter advances
  backfill-first then pending — monotonic, no interleave hole.

- **Resume re-attach** (`handle_command`/`Take`, session.rs:1480). The ring was
  filled by `emit` → `deliver` → detached push (Step 3), so its events are
  **already stamped** at push. Add `flush_resume_ring(ring)` that must NOT
  renumber:
  1. If `dropped_first_seq` is `Some(first)` (an eviction occurred), construct
     the eviction `Control::Gap` **with `seq = first`** (the first-evicted slot)
     and span from `dropped`, and deliver it via `emit_prestamped`. **Do not use
     `emit_gap`** here: `emit_gap` stamps a *fresh* (post-survivor) `seq`, which
     would order the gap after the survivors and break monotonicity.
  2. Deliver survivors via `emit_prestamped` (keep their push-time `seq`).
  Delivered order: `Gap(seq = first_evicted), survivors(seq = push-time)` —
  monotonic, with the evicted span as a true `seq` hole.

`handle_command`/`Take` (session.rs:1480) calls `flush_resume_ring`;
`run_backfill` (session.rs:1319) calls `flush_backfill_pending`.

### Step 5 — `EventRing` records first-evicted `seq`

In `EventRing` (session.rs:1659) add `dropped_first_seq: Option<Seq>` (init
`None` in `new`). In `note_drop` (session.rs:1688), set it on the **first**
eviction only:

```rust
self.dropped_first_seq.get_or_insert_with(|| evicted.seq().expect("data/control events are seq-stamped"));
```

FIFO eviction (`pop_front`, session.rs:1676) guarantees the first eviction is
the lowest `seq`, so `get_or_insert` captures the correct hole start.
`into_parts` returns it alongside the existing span:
`(Option<GapSpan>, Option<Seq>, VecDeque<MarketEvent>)`. The backfill `pending`
ring leaves `dropped_first_seq` unread (its events are unstamped at push;
`flush_backfill_pending` does not consult it).

Note: an evicted `Control::Gap` is itself stamped on the resume path, so
`evicted.seq()` is `Some` for every variant the ring can hold. The
`.expect(...)` documents this invariant; if a future metadata-only control with
`seq()==None` is added, that becomes a deliberate re-plan point.

### Step 6 — Enumerate and convert every production site

All sites that relied on `poll_next` numbering now route through `emit`/`forward`
(stamp) or `emit_prestamped` (no stamp). Concrete list (line refs at time of
writing):

| Site | Today | Phase 1 |
|---|---|---|
| Live data (session.rs:1368) | `forward(ev)` | unchanged — `forward`→`emit` stamps |
| Historical (non-cached) data (826) | `forward(ev)` | unchanged |
| Cache replay data (1022) | `emit(ev)` | unchanged — `emit` stamps |
| Gap-fetch data (1070) | `emit(ev)` | unchanged |
| `emit_gap` / `emit_provider_error` (905/928) | `forward` | unchanged |
| `SessionClosing` in `shutdown` (1515) & `finish_historical` (870) | `emit` | unchanged — `emit` stamps; the `Seq(0)` field is a stub overwritten by `stamp` |
| Backfill live arrivals (`buffer_live_arrival`, 1421-1436) | raw `pending.push` | **stays raw/unstamped**; stamped at the seam by `flush_backfill_pending` |
| Resume re-attach (`Take`, 1480) | `flush_ring` | `flush_resume_ring` (emit_prestamped) |
| Backfill seam (1319) | `flush_ring` | `flush_backfill_pending` (= old `flush_ring`, emit) |

All `Control { seq: Seq(0), .. }` construction placeholders (`emit_gap` 909,
`emit_provider_error` 932, `SessionClosing` 873/1518) stay as written — they are
**stub values overwritten by `stamp` in `emit`/`forward`**, exactly as they were
overwritten by `poll_next` before. No behavior change there.

Add `EventSink::flush` at shutdown: in `shutdown` (session.rs:1513-1524), after
the existing tap-log flush, call the attached sink's `flush()` (no-op for
in-process) for forward-compat with buffering transports. Guard on the
`Attached` arm; skip when `Detached`.

## Public API / type changes

- **New (core):** `datamancer_core::EventSink`, `datamancer_core::PublishOutcome`
  (re-exported through `datamancer`). Additive.
- **`Seq` doc** (event.rs:11-17) rewritten (see Doc updates). Type unchanged.
- **No change** to `MarketEvent`, `Trade/Quote/Bar/Control`, `Instrument`,
  `Provider`, `LiveHandle`, `TapLog`, `HistoricalCache`. `Session`/`take_events`
  keep their signatures; `EventStream` keeps its public signature but loses its
  `seq` field internally.
- **Internal only:** `Sink::Attached(InProcessSink)`; new `InProcessSink`;
  `Controller.next_seq`, `Controller::{stamp, deliver, emit_prestamped,
  flush_resume_ring, flush_backfill_pending}`; `EventRing.dropped_first_seq`;
  removal of `SessionInner.seq_counter` and `EventStream.seq`.

## Test plan

**Regression guards (must pass unchanged unless listed):** the full existing
suite — `cargo test` (and `cargo clippy --all-targets -- -D warnings`,
`cargo fmt`). Specifically verify green as-is:
- `session_integration::live_session_assigns_monotonic_seq_and_passes_events_through`
  (session_integration.rs:184, seq `[0,1,2]`) — production order == delivery
  order; under source-stamping the controller stamps in the same order.
- `session_integration::live_stream_retake_resumes_with_contiguous_seq`
  (session_integration.rs:550, expects `[(100,0),(200,1),(300,2),(400,3)]`,
  line 601) — **key resume-path guard.** Events 300/400 arrive while detached:
  `emit` stamps them 2/3 at push; clean re-take has no eviction, so
  `flush_resume_ring` emits no `Gap` and replays survivors prestamped (2,3).
  The test's "no Gap on a clean re-take" panic guard (line 597) pins this.
- Backfill seam tests in `resume.rs`
  (`stitched_session_splices_cache_provider_and_live_in_order` :333,
  `failed_backfill_gaps_to_the_live_edge_and_live_continues` :413,
  `tap_log_captures_only_the_live_tail_of_a_stitched_session` :466) — no `Gap`,
  contiguous seq across the seam; the deferred-stamp-at-seam design (Step 4)
  preserves this.
- `surreal_tap_log.rs` seq tests — the tap log keeps its own store-canonical
  `seq` (store-side hwm); the tee still feeds it stub-`seq` events (Step 3 keeps
  `forward = tee; emit`).

**Regression guard that MUST change (the invariant landing):**
`resume::overflow_reports_one_gap_and_tap_log_captures_everything`
(resume.rs:219; `resume_buffer_events(4)`, ten trades 100..1000, six evicted).
Under source-stamping the ten events are stamped seq 0..9 at push; seq 0..5
(source 100..600) are evicted; survivors keep seq 6..9.
- Keep `assert_eq!(c.seq.0, 0)` (resume.rs:270) — the eviction `Gap` occupies
  the first-evicted slot via `dropped_first_seq`.
- Keep the `Gap` span assertions (resume.rs:273-274: `[100, 601)`).
- Change the survivor assertion (resume.rs:292) from
  `vec![(700,1),(800,2),(900,3),(1000,4)]` to
  `vec![(700,6),(800,7),(900,8),(1000,9)]`.
- Update the comment block (resume.rs:260-262) to describe the hole (seq 1..5)
  as the new invariant. This is the primary regression guard for the semantic
  change.

**New unit tests (`session.rs` `#[cfg(test)]`):**
- `event_sink_in_process_round_trips` — `InProcessSink::publish` returns
  `Delivered` on success and `Rejected(ev)` (same event) after the receiver is
  dropped.
- `resume_ring_records_first_evicted_seq` — push N stamped events past capacity;
  `into_parts` returns `dropped_first_seq == Some(seq of first pushed)` and the
  expected span.
- `flush_resume_ring_places_gap_at_hole_start` (controller-level or focused) —
  delivered order is `Gap(seq=first_evicted)` then survivors with non-contiguous
  push-time seq; the hole equals the evicted span.

**Edge cases to cover:**
- **No eviction on re-take** — already covered by the retake integration test
  (no `Gap`, prestamped survivors).
- **Total eviction** — capacity small enough that every buffered event is
  evicted before re-attach: `flush_resume_ring` emits the `Gap` (at
  `dropped_first_seq`) and **zero** survivors; next live event continues from
  the post-eviction `next_seq`. Add a focused assertion (unit or small
  integration) since no existing test hits the empty-survivor branch.

**New agreement guard (realizable Phase-1 form — see RE-PLAN CHECKPOINT):**
- `historical_seq_is_deterministic_across_independent_sessions` — open two
  independent **historical** sessions over the same instrument+range backed by
  identical input (mock provider or pre-seeded cache; historical scope does not
  touch the live registry, session.rs:516), drain both, assert the
  `(seq, source_ts)` sequences are identical. Proves `seq` is now a function of
  source order, not poll timing — the property the eventual two-subscriber live
  test will rely on.

## Doc / invariant updates

The `seq` invariant is stated in four places; all four must move together or the
baseline contradicts itself.

- **`crates/datamancer-core/src/event.rs:11-17`** (`Seq` doc): replace
  "assigned by datamancer at delivery into the consumer stream" with: stamped
  **once at the source** of each authoritative per-symbol stream, **identical
  across all consumers of that symbol**, **per-symbol** (not global). The
  multiplex ordering key is `(instrument, seq)`. A consumer that misses events
  (resume-buffer eviction, late join) observes a real `seq` **hole**, surfaced
  as `Control::Gap`.
- **`event.rs:55`** (`seq` bullet on `MarketEvent`): "session-monotonic ordering
  field" → "per-symbol, source-stamped ordering field."
- **`crates/datamancer/src/session.rs` docs:** rewrite the `EventStream` doc
  (667-673), drop the removed `SessionInner.seq_counter` doc (541-543), add a
  `Controller.next_seq` doc, and update `emit` (1384-1388), `forward`
  (1406-1410), `EventRing` (1650-1658), and `stamp_seq` (1738-1739) docs to
  describe source-stamping in canonical delivery order and the new
  hole-on-eviction semantics. The `EventRing` doc's "Evicted events are never
  `seq`-stamped, so an overflow is a reported gap — never a `seq` hole" (1657)
  becomes "evicted events were stamped at push, so an overflow is a reported gap
  **and** a `seq` hole."
- **`crates/datamancer/README.md`** (the `seq` bullet and the resume paragraph):
  `seq` is per-symbol, stamped once at source, identical across consumers of
  that symbol; the delivered stream is contiguous *only while nothing is lost* —
  eviction/late-join is a reported `Control::Gap` **and** a `seq` hole. Remove
  "stamped at delivery from a counter shared across re-takes ... never a `seq`
  hole" (now false).
- **Root `CLAUDE.md`** — the `seq: u64` bullet under "Three timestamp fields":
  replace "stamped by datamancer at delivery ... `EventStream` stamps on poll
  ... contiguous by construction ... never a hole in `seq`" with the per-symbol
  source-stamped wording, and state that holes are now real and surfaced as
  `Control::Gap`. Keep the per-symbol-only / no-global-order point. Leave the
  broader "single ordered stream" / fan-out wording for **Phase 2** (note it
  there).

## Open questions

- **Sink signature** — owned-with-return (`PublishOutcome`) vs the doc's
  `&MarketEvent`. Recommended owned-return (Assumption B); needs sign-off before
  Phase 4. Optional future `publish_borrowed(&self, &MarketEvent)` for
  serializing transports can be added additively.
- **`EventSink::flush` at shutdown** — wired in for forward-compat though
  in-process is a no-op; confirm this is the desired shape.
- **Tap-log `seq` convergence** — ~~deferred~~ **LANDED in this phase.** `forward`
  now stamps before the tee (`stamp → tee → deliver`), so the tap log persists
  the source `seq` verbatim and no longer mints its own. (The original plan
  deferred this; it was pulled into Phase 1 since the change was a one-liner.)
- **Per-stream resume buffer granularity** — Phase 1 keeps one ring per
  authoritative session. Per-symbol-vs-per-multiplexed-stream buffering is a
  Phase 2 decision; nothing here forecloses it.

## Risks

- **Cross-cutting risk #1 — seq-at-source × resume/backfill.** Mitigated by the
  explicit two-flush split (Step 4): resume = stamp-at-push +
  `flush_resume_ring` (prestamped, gap at `dropped_first_seq`); backfill =
  stamp-at-seam + `flush_backfill_pending` (= old `flush_ring`). Crossing them
  reintroduces non-monotonic `seq` at the seam or erases the overflow hole. The
  backfill no-`Gap`/contiguous-seam tests, the retake test, and the updated
  overflow test together pin all three behaviors.
- **Gap placement subtlety** — using `emit_gap` (which stamps fresh) inside
  `flush_resume_ring` would order the gap after the survivors. Step 4 mandates a
  prestamped gap at `dropped_first_seq`; the `flush_resume_ring` unit test guards
  this.
- **Missed production site** — an emit path left on a non-stamping route ships
  `Seq(0)`. Mitigation: the Step 6 table is exhaustive against the current
  controller; the monotonic-seq integration test collapses to 0 on a stub leak.
- **Wrapper-type fan-out** — `Sink::Attached(InProcessSink)` changes every match
  arm that read the bare `Sender`; Step 2 enumerates them (741, 746-747, 880,
  1390). The compiler catches any miss.
- **Hot-path clone** — avoided by owned-return; the happy path moves the event
  into the channel exactly as today.
- **Low overall** — steady-path behavior is preserved; the existing suite plus
  the three new/updated tests are the guard.

## Review notes

Changes made to the draft:
- **Verified all file:line claims against the code.** They were accurate
  (`emit` 1389, `forward` 1411, `flush_ring` 759 + its two callers 1319/1480,
  `EventRing` 1659, `note_drop` 1688, `into_parts` 1713, `stamp_seq` 1740,
  `Sink` enum 720, `seq_counter` 544/289/599, `EventStream` 674/682,
  `MarketEvent::seq` event.rs:77). Corrected the `Seq` doc range to 11-17 and a
  couple of nearby line refs.
- **Counter design sharpened.** The draft kept `seq_counter` as a shared
  `Arc<AtomicU64>` in `SessionInner` and merely stopped cloning it. Since only
  the single controller task stamps after this change, I moved it to a plain
  `Controller.next_seq: u64` and removed the atomic and the `EventStream.seq`
  field entirely — structurally encoding "one writer at the source" and dropping
  a needless atomic. Listed the exact construction sites to update.
- **Factored `deliver`** out of `emit`/`emit_prestamped` so the two paths share
  one attach/detach/buffer body, directly addressing the draft's own
  "two near-identical functions" risk.
- **Made the gap-placement subtlety load-bearing and explicit:**
  `flush_resume_ring` must build the eviction `Gap` with `seq =
  dropped_first_seq` and use `emit_prestamped`, NOT `emit_gap` (which stamps a
  fresh post-survivor seq and would break monotonicity). The draft implied this
  but did not call out that `emit_gap` is unusable here.
- **Noted `flush_backfill_pending` == today's `flush_ring`** (behavior
  unchanged, rename only) — lowers churn and clarifies that only the resume path
  is genuinely new logic.
- **Confirmed the retake test (session_integration:550) stays green** and
  explained why (detached arrivals stamped at push, no eviction, prestamped
  replay), since it is the main resume-path regression guard alongside the
  overflow test.
- **Added edge-case coverage:** total-eviction (empty-survivor) branch and a
  `dropped_first_seq` unit test — neither was exercised by the existing suite.
- **Verified the agreement test is realizable:** historical scope has no
  registry participation (session.rs:516), so two independent historical
  sessions for the same pair are constructible.
- Kept both RE-PLAN CHECKPOINTs (two-subscriber live test deferred to Phase 2;
  sink-signature sign-off before Phase 4).

Unresolved concerns (flagged, not blocking Phase 1):
- ~~Tap-log/source `seq` convergence remains deferred.~~ **Resolved in this
  phase:** `forward` is `stamp → tee → deliver`, so the tap log persists the
  source `seq` verbatim.
- `EventSink::flush` at shutdown is wired for forward-compat only (no-op
  in-process); confirm this is the desired shape vs deferring entirely to
  Phase 4.
