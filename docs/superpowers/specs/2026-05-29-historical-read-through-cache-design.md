# Historical Read-Through Cache — Design

**Date:** 2026-05-29
**Status:** Approved design, pre-implementation
**Crate:** `datamancer` (orchestrator); no changes to `datamancer-core` trait surface.

## Context

`SurrealCache` already implements `HistoricalCache` + `ReplaySource` (store,
coverage tracking, `gaps()`, source_ts-ordered replay) and is tested in
`tests/surreal_cache.rs`. The `DatamancerBuilder` already accepts
`.historical_cache(...)`. What's missing is the wiring: today
`Controller::run_historical` always calls `provider.fetch_history(full_range)`
and forwards the result; it never consults the cache. The `persist=true` flag
is accepted but does nothing on the historical path.

This is the first of three specs decomposed from "set up SurrealDB
persistence":

- **Spec A (this doc): Historical read-through cache** — serve cached ranges
  from disk, fetch only the gaps, splice into one ordered stream, record
  coverage honestly.
- **Spec B: `SurrealTapLog` + live write-through** — append-only live event log
  and teeing live events to it.
- **Spec C: Resume primitive** — live re-take after stream drop and the
  historical→live backfill seam.

## Problem

A consumer requests one year of history; later they request ten years that
overlap it. The cache must:

1. **Not re-fetch what it already has.** Hit the provider only for the
   uncovered subranges.
2. **Deliver one ordered stream.** The session contract is a single
   `source_ts`-ordered, monotonic-`seq` stream — cached and freshly-fetched
   events spliced seamlessly.
3. **Record coverage honestly.** Never mark a range "covered" that wasn't
   actually fetched, so an interrupted fetch re-surfaces as a gap next time.

(Storage volume — ten years of tick data on disk — is explicitly deferred. See
Non-goals.)

## Key insight: gaps tile the range

`HistoricalCache::gaps(key)` returns the uncovered subranges of `[from, to]`,
ordered by `from_source_ts` and disjoint. The covered subranges are exactly the
complement within `[from, to]`. Together they **tile** the requested range into
an alternating, time-ordered sequence of segments:

```text
[from ............................................. to]
 |--covered--|------gap------|--covered--|----gap----|
   replay        fetch+store     replay     fetch+store
   from cache    from provider   from cache  from provider
```

Because the segments are disjoint and already sorted by `source_ts`, emitting
them left-to-right yields a globally `source_ts`-ordered stream **by
construction** — no merge-sort. This is why problems 1 and 2 collapse into one
mechanism. Problem 3 is handled by claiming coverage only for gap segments that
fetch successfully.

## Architecture

### Control interface: `PersistenceOptions`

The `persist: bool` argument on `Datamancer::session` overloads several
independent axes. It is replaced by an options block so the full space is
expressible without ambiguity. For Spec A only the two cache axes exist; Spec B
adds `write_tap_log` and Spec C adds resume, additively (the struct is
`#[non_exhaustive]` with builder setters so new axes never break call sites).

```rust
/// How a session interacts with the configured persistence layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct PersistenceOptions {
    /// Historical: serve covered subranges from cache, fetch only the gaps.
    pub read_cache: bool,
    /// Historical: write fetched gap data back to the cache.
    pub write_cache: bool,
}

impl PersistenceOptions {
    pub const fn none()      -> Self;  // (F,F) — also Default
    pub const fn cached()    -> Self;  // (T,T)
    pub const fn read_only() -> Self;  // (T,F)
    pub const fn refresh()   -> Self;  // (F,T)
    #[must_use] pub fn read_cache(self, on: bool) -> Self;
    #[must_use] pub fn write_cache(self, on: bool) -> Self;
}
```

The two flags compose into the full historical option space:

| `read_cache` | `write_cache` | Mode | Behavior |
|---|---|---|---|
| F | F | **ephemeral** | always hit provider, store nothing (old `persist=false`) |
| T | T | **cached** | read-through: serve covered ranges, fetch & store only gaps |
| T | F | **read-only** | serve cache + fetch gaps for this run, don't persist them |
| F | T | **refresh** | ignore cached coverage, re-fetch the range, overwrite |

### API changes

- `Datamancer::session(instrument, kind, scope, persist: bool)` →
  `session(instrument, kind, scope, options: PersistenceOptions)`.
- `Session::set_persisting(bool)` → `set_persistence(PersistenceOptions)`;
  `is_persisting()` → `persistence() -> PersistenceOptions`. The stored state
  becomes `PersistenceOptions` (it is `Copy`; a `Mutex<PersistenceOptions>` or
  packed atomic replaces the current `AtomicBool`).
- `PersistenceRequired` becomes axis-aware: returned when a requested axis has
  no backing layer — for Spec A, `(read_cache || write_cache)` with no
  `historical_cache` configured.
- Existing `session_integration.rs` call sites migrate `false →
  PersistenceOptions::none()` and `true → PersistenceOptions::cached()`.

### Trigger

The read-through path engages in `run_historical` **iff** `options.read_cache`
is set AND a `historical_cache` is configured. The plan (which segments are
covered vs gap) is fixed at fetch start from a single `gaps()` snapshot; a
mid-fetch `set_persistence` does not re-plan the in-flight fetch (it only
affects subsequent sessions/writes). When `read_cache` is false the whole range
is treated as one gap (no cache reads); gap data is stored only when
`options.write_cache` is set. This single loop subsumes all four modes above —
including today's behavior, which is `(F,F)`.

To bound blast radius, the legacy stream-straight-from-provider path is kept
intact for the `(F,F)` / no-cache case, and the new `run_historical_cached`
loop runs when `(read_cache || write_cache)` and a cache is present.

### The tiling helper (pure, unit-testable)

A free function in `session.rs`:

```rust
enum Segment {
    Covered { from: Timestamp, to: Timestamp },
    Gap     { from: Timestamp, to: Timestamp },
}

/// Tile [from, to) into ordered, disjoint Covered/Gap segments given the
/// cache's reported gaps (themselves ordered + disjoint within [from, to)).
fn tile(from: Timestamp, to: Timestamp, gaps: &[GapSpan]) -> Vec<Segment>;
```

Algorithm: walk a cursor from `from`; for each gap, emit `Covered(cursor,
gap.from)` if non-empty, then `Gap(gap.from, gap.to)`, advance cursor to
`gap.to`; finally emit `Covered(cursor, to)` if non-empty. Degenerate cases:
empty cache → one `Gap(from, to)`; fully covered → one `Covered(from, to)`.

### The read-through fetch loop

New method `Controller::run_historical_cached`, selected at the top of
`run_historical` when the trigger fires. Pseudocode:

```text
key  = CacheKey { instrument, kind, from, to }
// read_cache decides whether we consult coverage at all.
gaps = if options.read_cache { cache.gaps(&key).await.unwrap_or(whole_range) }
       else                  { vec![whole_range] }   // refresh / no-read: all gap
segments = tile(from, to, &gaps)         // all-gap when read_cache is off

for seg in segments:
    select on cmd_rx in parallel (handle SetPersistence / Close; Close aborts):
    match seg {
        Covered(r) => {
            // Serve from disk. ReplaySource yields source_ts-ordered events.
            let src = cache.as_replay_source(key.with_range(r));
            let mut s = src.open(ReplayRequest {
                instruments: vec![instrument], kinds: vec![kind],
                from: r.from, to: r.to,
            }).await?;
            while let Some(ev) = s.next().await { self.forward(ev).await; }
        }
        Gap(r) => {
            // Fetch from provider into a private channel; forward AND buffer.
            let (tx, mut rx) = mpsc::channel(default_buffer());
            let fetch = spawn(provider.fetch_history(HistoryRequest{..r}, tx));
            let mut batch = Vec::new();
            while let Some(ev) = rx.recv().await {
                batch.push(ev.clone());
                self.forward(ev).await;
            }
            match fetch.await {
                Ok(Ok(())) => {
                    if options.write_cache {                          // read-only mode skips this
                        cache.store(&key.with_range(r), &batch).await?;  // claims [r.from, r.to)
                    }
                }
                _ => {                      // fetch errored / panicked
                    // Confirmed contiguous prefix. +1 so the last received event
                    // (half-open scan) stays covered; r.from if nothing arrived.
                    let confirmed_to = batch.last().map(|e| max_source_ts(e) + 1)
                                            .unwrap_or(r.from);
                    if options.write_cache {     // honest coverage: claim only the prefix
                        cache.store(&key.with_range(r.from..confirmed_to), &batch).await?;
                    }
                    // Tell the consumer the rest of this segment is missing.
                    self.emit_gap_control(instrument, r.with_from(confirmed_to)).await;
                    break;                  // abort remaining segments; re-request resumes
                }
            }
        }
    }

// then the existing post-fetch handshake: SessionClosing + drain/auto-close.
```

Notes:

- **`forward()` is reused unchanged.** It assigns the monotonic session `seq`.
  Because segments are emitted in `source_ts` order, `seq` rises with
  `source_ts` — exactly the documented historical-fetch seq invariant.
- **Cache events arrive with `seq = Seq(0)` and a stored `rx_ts`;** `forward()`
  reassigns `seq`. `rx_ts` is observability-only and the splice never depends
  on it, so mixed provenance (provider wall-clock on gap events, stored value
  on cached events) is acceptable; no special handling.
- **Command handling preserved.** The loop must still service `cmd_rx`
  (`SetPersistence`, `Close`) and the no-consumer fast path / `SessionClosing`
  handshake exactly as the current `run_historical` does. `Close` mid-fetch
  aborts the in-flight segment and shuts down.
- **Store granularity:** one `store` call per gap segment, holding that
  segment's events in memory. Fine for now; chunked/streaming store for very
  large gaps is a noted follow-up (volume is deferred).

### Coverage-truth refinement in `SurrealCache::store`

Today `store` extends the claimed coverage to the event span:
`from = key.from.min(min_ts); to = key.to.max(max_ts + 1)`. For honest
coverage, `store` must claim **exactly** the `CacheKey`'s `[from, to)` (the
range the caller asserts was fetched), not widen it to whatever timestamps
happened to arrive. Change `store` to mark `[key.from, key.to)` covered
verbatim. The caller (`run_historical_cached`) is now responsible for passing a
key range that reflects only what was actually, successfully fetched.

This is the only change to `surreal.rs`. Existing `surreal_cache.rs` assertions
(`coverage.to.0 >= 400`, the gap tests) remain satisfied because their store
calls already use key ranges that bound their events; verify during
implementation.

### Failure semantics

On a gap-segment fetch error or panic:

1. Store only the confirmed contiguous prefix `[gap.from, last_received_ts + 1)`
   (or an empty range if no events arrived), so the unfetched remainder stays a
   gap. The `+1` keeps the last received event inside the half-open coverage.
2. Emit an in-band `ControlKind::Gap` for the unfetched remainder so the
   consumer knows the stream is incomplete (Control events ride the data
   stream — existing invariant).
3. Abort the remaining segments and run the normal shutdown handshake. A later
   re-request will see the remaining gap via `gaps()` and re-fetch it.

(Continuing past a failed segment to later segments is a possible future
enhancement; aborting keeps post-failure ordering trivial to reason about.)

## Components & boundaries

- `tile()` — pure function, no I/O. Owns the segment math. Unit-tested in
  isolation.
- `Controller::run_historical_cached()` — orchestrates the segment loop over
  the `dyn HistoricalCache` + `dyn Provider`. Provider/cache-agnostic: works
  with any `HistoricalCache`, not just Surreal.
- `SurrealCache::store()` — coverage claim tightened to exactly the key range.
- `PersistenceOptions` + the `session` / `set_persistence` / `persistence`
  signature changes — the control interface. Lives in `datamancer` (`session.rs`
  or a small `persistence.rs`), re-exported from the crate root.
- No `datamancer-core` changes. No `TapLog` work (Spec B). No `take_events`
  changes (Spec C).

## Testing

Integration tests in a new `tests/historical_cache.rs` (feature-gated
`storage-surreal`), using an extended `FakeProvider` that (a) **records the
ranges it was asked to fetch** and (b) can be configured to **error mid-fetch**,
plus `SurrealCache::Memory`:

1. **Cold fetch** — empty cache, request `[a, d)`: provider asked for `[a, d)`
   once; consumer receives all events in `source_ts` order with monotonic
   `seq`; `lookup` reports coverage afterward.
2. **Fully cached** — pre-store `[a, d)`, re-request `[a, d)`: provider
   `fetch_history` is **not** called; all events served from cache, in order.
3. **Partial overlap (the conundrum)** — pre-store `[b, c)` inside `[a, d)`;
   request `[a, d)`: provider asked **only** for `[a, b)` and `[c, d)` (assert
   recorded ranges); consumer receives the full merged `[a, d)` stream in
   `source_ts` order; coverage merges to `[a, d)`.
4. **Coverage truth on failure** — request a gap the provider errors partway
   through: coverage claims only the confirmed prefix; a `ControlKind::Gap` is
   emitted for the remainder; a second request re-fetches the still-missing
   span.
5. **Ordering/seq invariant** — spliced stream across a covered+gap boundary is
   strictly `source_ts`-ordered with contiguous `seq`.
6. **Mode matrix** — `read_only` (T,F): serves cache + fetches gaps but the
   gaps are **not** persisted (re-request still reports them as gaps).
   `refresh` (F,T): provider asked for the **whole** range despite existing
   coverage, and the result is stored.

Unit tests for `tile()`: empty gaps, full gap, leading/trailing/middle gaps,
adjacent gaps, single-point boundaries.

Regression: the existing `session_integration.rs` suite migrates its
`persist: bool` call sites to `PersistenceOptions` (`false →
PersistenceOptions::none()`); the `(F,F)` / no-cache path stays behaviorally
unchanged.

## Example & docs

- `examples/cached_history.rs`: build a `Datamancer` with
  `SurrealCache::embedded(tempdir)`, run a historical session with
  `PersistenceOptions::cached()` over a bar range, drain it; then run the
  **same** request again and observe it served from cache (no provider
  round-trip / near-instant).
  Modeled on `crypto_ticker.rs`; uses Alpaca historical bars if reachable
  without credentials, otherwise documents the construction shape.
- A "Persistence — historical cache" section in `crates/datamancer/README.md`
  describing read-through, gap-fill, coverage semantics, and the deferred
  volume concern.

## Non-goals (this spec)

- **Storage volume / eviction / granularity.** No bounds on cache size; a giant
  fresh fetch can fill the disk. Deferred until it bites.
- **`TapLog` / live write-through.** Spec B.
- **Resume primitive / `take_events` multi-shot / live backfill seam.** Spec C.
- **Provider history growth.** Settled past ranges are treated as stable; no
  automatic re-validation or invalidation. Manual invalidation can be added
  later if a real use case demands it.

## Risks & mitigations

- **Restructuring `run_historical` regresses the lifecycle** (no-consumer fast
  path, command handling, `SessionClosing` handshake). Mitigation: branch into
  a separate `run_historical_cached` and leave the existing path byte-for-byte
  intact for the `(F,F)` / no-cache case; reuse the same post-fetch handshake
  code.
- **Coverage over/under-claim.** Mitigation: `store` claims exactly the key
  range; the caller computes that range from actual fetch success. Test 4
  guards the failure case.
- **Provider re-fetched despite cache.** Mitigation: test 2 and 3 assert the
  exact ranges the provider was asked for.
