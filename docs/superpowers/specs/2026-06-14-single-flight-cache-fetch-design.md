# Single-Flight Cache Fetch (Per-`CacheKey` Queueing) — Design

**Date:** 2026-06-14
**Status:** Approved design, pre-implementation
**Crate:** `datamancer` (orchestrator read-through path + a new in-process
fetch registry). No `datamancer-core` trait changes; no `HistoricalCache`
trait changes.

## Context

The historical read-through cache already deduplicates *sequential* requests:
a `cached()` session records each completed fetch's `[from, to)` range as
covered, so a later overlapping request fetches only the gaps
(`storage/surreal.rs`; verified by
`tests/historical_cache.rs::partial_overlap_fetches_only_the_gaps_and_merges_in_order`).

The hole is **concurrent** requests. Coverage is recorded only when a fetch
*completes*. There is no in-flight coordination today — multiple historical
sessions for the same pair are explicitly stateless concurrent reads
(`session.rs`). So if N sessions request the same uncovered range before any of
them finishes, **all N hit the provider** for the same data.

The motivating workload: a **block-size parameter sweep** spawns hundreds of
sessions on startup, each requesting the same multi-year window against a cold
cache. Without coordination that is hundreds of identical provider fetches of
(e.g.) ten years of history.

## Goal

Ensure **at most one outstanding provider fetch per `CacheKey`** within a
single process. Concurrent requests for the same key wait for the in-flight
fetch, then serve what it cached — collapsing the sweep's hundreds of fetches
to one per key.

## Non-goals

- **Cross-process coalescing.** In-process only — one `Datamancer` instance. A
  sweep is one process spawning many sessions, which this fully covers.
  Cross-process sharing is the parked consumer-transport/server design
  (`2026-06-14-consumer-transport-seam-design.md`), explicitly out of scope.
- **Range-precise coalescing.** We serialize at `CacheKey` granularity, not by
  `[from, to)` range. At most one fetch per key, full stop. Interval-level
  overlap sharing is unnecessary for the sweep (identical ranges) and is not
  built.
- **Changing replay/serve concurrency.** Only the gap-*fetch* serializes.
  Reading already-covered ranges from disk stays fully concurrent.
- **Cache-volume observability.** Wanted, but scoped to a separate small spec
  (covered-ranges + event-count stats API). Not in this one.
- **`HistoricalCache` trait or `seq` changes.** None.

## Mechanism

`Datamancer` (the `Arc`-shared orchestrator reachable from every session's
read-through path) gains an **in-process fetch registry**: a map from
`CacheKey` to a shared completion handle. The registry mediates entry into the
gap-fetch portion of `run_historical_cached`.

When a `cached()` (read-cache) session is ready to fetch gaps for a key:

- **Winner** — the first session to claim the key in the registry. It proceeds
  exactly as today: tile the range, fetch each gap from the provider, forward
  events to *its* consumer, store to cache, mark coverage. On completion —
  **success or failure** — it signals the completion handle and removes the key
  from the registry. **The winner's hot path is unchanged.**

- **Waiter** — the key is already claimed. The session does **not** fetch. It
  blocks on the completion handle until the winner releases the key.

### Waiters re-tile; they do not reuse the winner's result

When a waiter wakes, it makes **no assumption** about what the winner covered.
It re-runs `cache.gaps(key)` against the now-current coverage and re-tiles its
own requested `[from, to)`:

- Covered subranges replay from cache (concurrent, unlocked).
- Any still-uncovered remainder is a fresh gap — the session re-enters the
  single-flight for that key (now likely becoming the winner for the
  remainder).

This is what makes the design correct under the **existing partial-fetch
semantics** with no special-casing:

- Winner stored a prefix and emitted `Control::Gap` for the rest → the waiter
  sees the remainder as a fresh gap and fetches it.
- Winner failed entirely → coverage is unchanged, the waiter re-tiles to the
  same gap and becomes the next winner.
- Winner fully covered the range → the waiter re-tiles to all-covered and
  serves entirely from cache.

Coverage — not the winner's success — is the source of truth, so the waiter
path is uniform.

### Progress / termination

Each single-flight round makes progress or surfaces an honest gap:

- A successful fetch (even an empty one) records coverage, shrinking the
  remaining gap.
- A failed fetch records nothing and emits `Control::Gap`; the waiter that
  becomes the next winner retries the remainder. Repeated provider failure
  terminates the same way it does today — the gap is reported, not retried
  forever.

No new unbounded-retry loop is introduced.

## Where it lives

- The registry is owned by the shared `Datamancer` inner (the same `Arc`-shared
  state that already holds the provider and cache handles), so every session
  consults the same instance. It is a process-local concern; it is **not** part
  of the `HistoricalCache` trait (the cache stays a stateless store, usable by
  any backend).
- The single-flight wraps the gap-fetch entry/exit in `run_historical_cached`
  in `session.rs`. Cache-replay and provider-forward internals are otherwise
  untouched.

### Completion-handle primitive (implementation note)

The handle must be free of lost-wakeups: a waiter that arrives *after* the
winner has already signalled must still observe completion rather than block
forever. The registry insert/lookup must be atomic against the winner's
removal. `tokio::sync::watch` (retains the latest state) or a `Shared` future
satisfies both; a bare `Notify` does not (a signal sent before a waiter
registers is lost). The exact primitive is an implementation choice — TDD on
the late-waiter case is expected to drive it out. Whatever is chosen, dropping
a winner mid-fetch (task cancelled / session closed) must still release the key
and wake waiters, never strand them.

## Testing

- **The headline test:** N concurrent `cached()` sessions request the same cold
  `(key, range)`. Assert the provider's `fetch_history` is invoked **once** and
  all N consumers receive the full range in order. (Use a counting/mock
  provider.)
- **Partial-fetch recovery:** winner fetches only a prefix (provider fails
  midway, per existing semantics); a waiter fetches the remainder and the
  combined coverage is complete.
- **Winner failure:** winner's fetch fails entirely; a waiter becomes the next
  winner and the data is fetched exactly once on retry.
- **No false serialization:** two concurrent sessions on *different* `CacheKey`s
  fetch concurrently (the lock is per key, not global).
- **Late waiter / no lost-wakeup:** a session that begins waiting after the
  winner has already completed observes completion and serves from cache
  without hanging.
- **Cancellation safety:** a winner dropped mid-fetch releases the key; a
  waiter recovers rather than deadlocking.
- **Regression:** existing `historical_cache.rs` sequential-dedup tests still
  pass unchanged.

## Out of scope, recorded for the sibling spec

Cache-volume observability — a small stats surface (per-`CacheKey` covered
ranges and event counts; a total) so the caller can *see* what is cached and
confirm dedup is working. Its own design→plan cycle.
