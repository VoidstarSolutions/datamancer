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

```
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

### Trigger

The read-through path engages in `run_historical` **iff** `persist == true` AND
a `historical_cache` is configured on the `Datamancer`. Otherwise the existing
stream-straight-from-provider path runs unchanged. (`persist=true` with only a
`TapLog` and no cache → stream-through; `TapLog` is a live-only sink and is not
consulted here.) The choice is made once at fetch start; a mid-fetch
`set_persisting` toggle does not re-plan the in-flight fetch (it only affects
the `persisting` flag for future sessions/writes).

Rationale: `persist=true` means "use the cache" (read **and** write). Keeping
the existing non-persist path untouched avoids disturbing the many passing
`session_integration` tests that run with `persist=false`.

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

```
key  = CacheKey { instrument, kind, from, to }
gaps = cache.gaps(&key).await?           // empty on error → treat whole range as gap
segments = tile(from, to, &gaps)

for seg in segments:
    select on cmd_rx in parallel (handle SetPersisting / Close; Close aborts):
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
                    cache.store(&key.with_range(r), &batch).await?;  // claims [r.from, r.to)
                }
                _ => {                      // fetch errored / panicked
                    // Honest coverage: claim only the confirmed contiguous prefix.
                    // +1 so the last received event (half-open scan) stays covered;
                    // if nothing arrived, claim an empty range (whole gap re-surfaces).
                    let confirmed_to = batch.last().map(|e| max_source_ts(e) + 1)
                                            .unwrap_or(r.from);
                    cache.store(&key.with_range(r.from..confirmed_to), &batch).await?;
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
  (`SetPersisting`, `Close`) and the no-consumer fast path / `SessionClosing`
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

Unit tests for `tile()`: empty gaps, full gap, leading/trailing/middle gaps,
adjacent gaps, single-point boundaries.

Regression: the existing `session_integration.rs` suite (all `persist=false`)
must continue to pass unchanged — the non-persist path is untouched.

## Example & docs

- `examples/cached_history.rs`: build a `Datamancer` with
  `SurrealCache::embedded(tempdir)`, run a historical session with
  `persist=true` over a bar range, drain it; then run the **same** request again
  and observe it served from cache (no provider round-trip / near-instant).
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
  intact for `persist=false`; reuse the same post-fetch handshake code.
- **Coverage over/under-claim.** Mitigation: `store` claims exactly the key
  range; the caller computes that range from actual fetch success. Test 4
  guards the failure case.
- **Provider re-fetched despite cache.** Mitigation: test 2 and 3 assert the
  exact ranges the provider was asked for.
