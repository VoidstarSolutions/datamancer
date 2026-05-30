# SurrealDB Tap Log + Live Write-Through — Design

**Date:** 2026-05-30
**Status:** Approved design, pre-implementation
**Crate:** `datamancer` (orchestrator) + `datamancer-core` doc fix only (the
`seq` invariant text). No `TapLog` trait changes.

## Context

`Spec A` (historical read-through cache) has landed on `main`. This is the
second of three specs decomposed from "set up SurrealDB persistence":

- **Spec A (done): Historical read-through cache** — serve cached ranges from
  disk, fetch only the gaps, splice into one ordered stream.
- **Spec B (this doc): `SurrealTapLog` + live write-through** — an append-only
  log of the live event stream, teed off the session as events arrive, and
  replayable in exact arrival order.
- **Spec C: Resume primitive** — live re-take after stream drop and the
  historical→live backfill seam (this is where tap-log `seq` rebasing on
  splice actually gets exercised).

The `TapLog` trait already exists in `datamancer-core`:

```rust
pub trait TapLog: Send + Sync {
    async fn append(&self, ev: &MarketEvent) -> Result<()>;
    async fn flush(&self) -> Result<()>;
    fn as_replay_source(&self) -> Box<dyn ReplaySource>;
}
```

`DatamancerBuilder::tap_log(Box<dyn TapLog>)` and `DatamancerInner.tap_log:
Option<Arc<dyn TapLog>>` already exist. `Controller::shutdown` already calls
`tap_log.flush()`. `forward()` has the tee point marked with a TODO. What's
missing: the `SurrealTapLog` implementation, the `forward()` tee, and the
`write_tap_log` persistence axis.

## The ordering problem (why the tap log is not keyed like the cache)

The cache keys rows by `source_ts` because historical fetch is *defined* as
`source_ts`-ordered. The tap log captures the **live** stream, whose defining
invariant is **arrival order** (`seq`). Arrival order ≠ `source_ts` order:
quotes and trades interleave, and providers report market timestamps slightly
out of order. If the tap log keyed by `source_ts` like the cache, replay would
silently re-sort and lose bit-for-bit fidelity with what the session emitted.

So the tap log preserves arrival order, and the ordering key is `seq`.

### `seq` is a pure total-order key — it has no drop-detection role

The CLAUDE.md invariant currently reads "persistence uses `seq` gaps for drop
detection." **This is wrong and will be corrected.** `seq` is assigned by
datamancer, at receipt, to events it actually received — it is contiguous by
construction and datamancer never skips a number:

- A provider dropping a message on the wire is invisible at the `seq` layer —
  datamancer never received it, never numbered it. There is no missing `6`
  between `5` and `7`; the next received event simply gets `6`.
- Historical fetch numbers in `source_ts` order, still contiguous.
- Splice/rebase produces contiguous-or-offset `seq`, by construction.
- The only way to manufacture a hole is an internal lossy write path — a thing
  to *not build*, not a thing to detect.

Real drop detection is **provider-native**: an exchange feed with its own
sequence numbers jumps, datamancer notices, and emits a `Control::Gap`. That
gap *occupies* a `seq` slot — it is content, not absence. Missing-market-data
gaps are a `source_ts`/coverage concept (the cache's `GapSpan` machinery).
Neither is ever a hole in datamancer's `seq`.

**Consequence:** `seq` is purely a total-order key. The store owns it, may
re-densify it freely on rebase, and replay is `ORDER BY seq`.

## Storage model

### Per-`(instrument, kind)` shard tables

One table per unique `(instrument, kind)` pair. `EventKind` is
`Trade | Quote | Bar(BarInterval)`, so resolution is already folded into
`kind` — `(instrument, EventKind)` is the full shard key.

Rationale: **compression.** Homogeneous rows — a constant symbol, delta-
friendly monotonic timestamps, same-magnitude prices — compress far better than
a single table interleaving, e.g., AAPL at ~$200 with BTC at ~$60k.

Each shard is internally `seq`-sorted (arrival order = `seq` order). Replay
across shards is a streaming **k-way merge by `seq`** — deterministic, and
O(number of shards) memory, not O(rows), precisely because `seq` is a clean
global total order.

### Row shape (`SurrealValue`, mirroring the cache's row style)

- `id` — a write-time **ULID**. Stable, order-independent identity that
  survives a `seq` rebase.
- `seq: u64` — **indexed.** The store-canonical order key, assigned by the tap
  log on write (`high_water_mark + 1…`), *not* the session-local `seq` the live
  consumer saw. A rebase is `UPDATE … SET seq = seq + offset` — a field update,
  never a record-id migration.
- `kind` discriminator + instrument fields.
- `source_ts`, `rx_ts` — preserved as-is. `source_ts` drives windowed replay;
  `rx_ts` keeps the live observability data that pure-historical replay lacks.
- payload columns per kind (price/size; OHLCV; bid/ask…).

### `streams` registry table

Symbols are not always valid table-name tokens (`BTC/USD`, `.`, …), and replay
must enumerate the shards matching a request without guessing names. The
registry maps `(instrument, kind)` → an opaque, safe shard table name (a
sequential id like `tap_0001`, or a hash); the raw instrument/kind live in the
registry row.

- **Write path** consults it (cached in memory) to resolve or create a shard.
- **Replay** queries it to enumerate the shards matching the request's
  `instruments × kinds`.

### `meta` table

A single row holding the global `seq` high-water mark. One cheap read on `open`
(no scan across hundreds of shards); bumped as the writer assigns. On a future
rebase, the affected shards' `seq` fields are updated and the high-water mark
adjusted.

### Storage location

Namespace `datamancer`, database `taplog` (separate from the cache's `cache`
db). Config mirrors `SurrealCacheConfig`: `Memory` / `Embedded { path }` /
`Remote`, with an `embedded(path)` constructor.

### Scale caveat

This is one table per instrument-kind *ever* tapped into a store. Fine for
hundreds-to-low-thousands of instruments; worth a flag only when tapping a very
large universe (tens of thousands) into one store.

## Write-through path

### The `write_tap_log` axis

`PersistenceOptions` gains a third field `write_tap_log: bool`, orthogonal to
the two cache axes (cache = historical; tap = live capture). The struct is
`#[non_exhaustive]`, so a `with_tap_log(bool)` modifier is added:
`PersistenceOptions::none().with_tap_log(true)` for pure live capture, or
stacked onto a cached preset. Existing presets stay cache-only.

### The tee in `forward()`

`forward()` is shared by the live and historical paths. Gate the tee on three
conditions, all required:

1. `persistence.write_tap_log` is set, **and**
2. `scope == Live`, **and**
3. the event is data (`Trade | Quote | Bar`), never `Control`.

When all hold: call `tap_log.append(&stamped)` (which takes `&MarketEvent` and
clones internally to enqueue), then send `stamped` on to `events_tx` as today.
One clone per tapped event, inside `append` — the same cost the cache tee
already pays.

### `append` is a cheap enqueue, not a disk write

`SurrealTapLog` owns an **unbounded** channel and a background writer task
spawned at `open`. `append` pushes onto the channel and returns immediately —
`forward()` never awaits disk, so the live stream cannot stall on persistence.
(Unbounded means no live stall and no silent drop; unbounded memory growth
under a sustained disk-can't-keep-up burst is an accepted risk, deferred.)

The channel carries a command enum:

```rust
enum WriteCmd {
    Event(MarketEvent),
    Flush(oneshot::Sender<Result<()>>),
}
```

The writer drains in order. For each `Event` it assigns the store-canonical
`seq` (`++high_water_mark`, ignoring the event's session-local `seq`),
resolves/creates the shard via the registry, and inserts a row with a fresh
ULID id.

### Error handling — best-effort, never fatal

A tap write failing must not break the live session (live invariant). The
writer logs errors via `tracing` and keeps draining; the most recent error is
surfaced when `flush()` is called, so shutdown can report it.

### Flush & shutdown

`flush()` sends a `Flush(oneshot)` barrier; the writer processes everything
queued ahead of it, commits, and replies — giving ordered, durable flush.
`Controller::shutdown` already calls `flush()`; afterward the tap log's sender
drops, and the writer task drains and exits.

## Replay

`SurrealTapLog::as_replay_source(&self)` takes no key (the whole log); the
`ReplayRequest` does all the filtering, keeping the tap log's replay surface
identical to the cache's from a consumer's point of view.

`open(ReplayRequest { instruments, kinds, from, to })`:

1. Query the `streams` registry for every shard whose `(instrument, kind)` is
   in `instruments × kinds`.
2. Open a `seq`-ordered cursor per matching shard:
   `SELECT * FROM <shard> WHERE source_ts >= from AND source_ts < to ORDER BY seq`.
3. **K-way merge by `seq`** across those cursors in Rust — repeatedly emit the
   min-`seq` head. Because `seq` is globally assigned from the one high-water
   mark, this reconstructs the exact global arrival order. Memory O(shards).
4. Emit as a `'static` `BoxStream<MarketEvent>` owning its cursors.

**Edges:** empty store or no matching shards → empty stream. Events in a matched
shard but outside `[from, to)` are dropped by the `WHERE`. Emitted events carry
the stored canonical `seq`; when replay feeds a live session, `forward()`
re-stamps `seq` at receipt as it does for cache replay today — so standalone
replay sees canonical `seq`, session-fed replay gets re-stamped. Same contract
as the cache.

## Testing

Mirror the `surreal_cache.rs` style: in-memory engine for the fast suite, plus
one on-disk embedded round-trip.

### Storage-level (`tests/surreal_tap_log.rs`, in-memory)

- **Round-trip:** `append` events → `flush` → `open` → assert order and values
  preserved.
- **Fidelity (the load-bearing test):** append events whose **arrival order
  differs from `source_ts` order** (e.g., a quote at `source_ts` 300 appended
  before a trade at `source_ts` 200). Replay must emit in **arrival/`seq`
  order**, not re-sorted by `source_ts`.
- **Sharding:** events across multiple `(instrument, kind)` pairs land in
  separate shards (assert via the registry); replay k-way-merges them back into
  one globally `seq`-ordered stream.
- **Awkward symbols:** an instrument like `BTC/USD` resolves to a safe shard
  name and round-trips.
- **Multi-session append / high-water mark:** append, drop, reopen, append
  again — canonical `seq` continues from the high-water mark; replay orders the
  combined set correctly.
- **`source_ts` windowing:** replay with `[from, to)` narrower than the data
  filters correctly.
- **Embedded round-trip:** on-disk `SurrealKV` to a tempdir, reopen (the
  poll-until-lock-clears pattern the cache test uses), assert data survives.

### Session-level (extend session integration tests)

- Live scope + `write_tap_log(true)` → events land in the log.
- Historical scope, or `write_tap_log(false)` → nothing captured (gating).
- `Control` events are **not** captured (data-only).

### Unit

- `PersistenceOptions::none().with_tap_log(true)` sets the axis; presets stay
  cache-only.

## Non-goals (deferred)

- **Unbounded memory bound on the writer channel.** Accepted risk; revisit only
  if it surfaces.
- **Storage volume / retention / compaction.**
- **`seq` rebase on splice.** The schema (order-independent ULID id + indexed
  `seq` field + `meta` high-water mark) is *designed to permit* it cheaply, but
  the rebase operation itself is Spec C.
- **Tapping `Control` events.** Tap log is data-only. If replay needs to
  re-surface gaps, that is derived from coverage metadata in Spec C.
- **Cross-store reconciliation, analysis, time-series querying.**
