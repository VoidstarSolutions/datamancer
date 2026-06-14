# Scoped request: split/dividend-adjusted historical bars in datamancer

## Goal
Historical bars fed to Citadel must be corporate-action–adjusted (default **`All`** =
split + dividend + spin-off), so P&F charts stop fabricating phantom reversals at split
boundaries. Wire the full adjustment enum through so the mode is flippable later, but
default to `All`.

## Where the work is
- **`oxidized-alpaca`: no changes.** The capability already exists — `Adjustment` enum
  (`src/restful/market_data/stock/mod.rs:50`) and the builder method
  `StockBarsRequest::adjustment(Adjustment)` (`bars.rs:80`, also `.adjustments(iter)` at
  `:89`). Reference only.
- **`citadel`: no changes required.** `main.rs:184` builds via `Datamancer::builder()`
  with `AlpacaProviderConfig::default()`; if the builder defaults to `All`, Citadel gets
  adjusted data for free. (A user-facing toggle can come later.)
- **All work is in the `datamancer` repo** (`datamancer-core` + `datamancer` crates).

## Core design constraint (do not violate)
The adjustment mode must be a **single source of truth** that the session stamps into
**both** `HistoryRequest` (so the provider fetches adjusted) **and** `CacheKey` (so the
cache stores/serves under a mode-segregated key). Do **not** put the mode independently on
the provider instance and the cache instance — they could disagree and write adjusted data
under a raw key, silently corrupting the cache. Provider reads `request.adjustment`; cache
reads `key.adjustment`; both descend from one `DatamancerInner.adjustment`.

## Changes

### 1. `datamancer-core` — new type + thread through request/key
- Add `enum Adjustment { Raw, Split, Dividend, SpinOff, All }`, `#[derive(..., Default)]`
  with `#[default] All`. Place in a small module; re-export from `lib.rs:28-31`.
- `HistoryRequest` (`traits/provider.rs:106`): add `pub adjustment: Adjustment`.
- `CacheKey` (`traits/storage.rs:111`): add `pub adjustment: Adjustment`.

### 2. `datamancer` — carry the mode and stamp it
- `DatamancerInner` (`session.rs:165`) + `DatamancerBuilder` (`session.rs:373`): add
  `adjustment` field, a builder method `.adjustment(Adjustment)`, default `All` in the
  builder/`build()`.
- `SessionInner` (`session.rs:498`): add `adjustment`; set from `self.inner.adjustment`
  when constructing in `session()` (`session.rs:271`).
- Stamp `adjustment: self.inner.adjustment` (or session's copy) into **every** literal:
  - `HistoryRequest` at `session.rs:764` and `session.rs:952`.
  - `CacheKey` at `session.rs:898, 1012, 1034, 1078, 1141`.

### 3. `datamancer` provider — apply at fetch
- `providers/alpaca.rs` `fetch_history_via`, the `EventKind::Bar` path (~`:727`): map
  `datamancer_core::Adjustment` → `oxidized_alpaca …::Adjustment` and chain
  `.adjustment(mode)` onto the `stock_bars(...)` builder. Add the mapping fn.
- **Only** the historical `stock_bars` REST path. The live-stream/trade paths stay raw —
  you can't adjust a real-time tick, and intraday splits don't occur.

### 4. `datamancer` cache — segregate by mode (the part with a trap)
Reads `SELECT … WHERE provider AND symbol AND source_ts BETWEEN …` — they filter by
symbol/time, **not** by `row_id`. So folding the mode into `row_id` alone is insufficient:
a fresh `All`-mode read would still return orphaned raw rows in the same table. Segregate
at the read level. Recommended (avoids dynamic table names that would break the up-front
`DEFINE TABLE` block at `surreal.rs:131-141`):
- Add an `adjustment` discriminant column to `BarRow` (`surreal.rs:206`); set trade/quote
  rows to `raw`.
- Include `adjustment` in `coverage_id` (`:159`) and `row_id` (`:353`).
- Add `AND adjustment = $adj` to every bar `SELECT`/`DELETE`/`count`: store DELETE `:330`,
  count `:477`, reads `~:538-616`.
- Old raw-keyed rows become **orphaned** (harmless; clean cut, no migration). Optional: a
  one-shot cleanup/log of orphan count.

### 5. Tests
- Update `CacheKey` literals in helpers: `tests/historical_cache.rs:139`,
  `tests/resume.rs:297`, `tests/surreal_cache.rs:52`.
- Add a **segregation test**: same `(symbol, range)` stored under two modes does not
  collide on read or in coverage.
- Add a provider test asserting the `.adjustment(...)` is applied to the bars request (or
  that `Raw` vs `All` map correctly).

## Verify (datamancer is its own workspace)
Run in the datamancer repo root: `cargo fmt --all`,
`cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`. Confirm
whether the surreal cache tests use an embedded backend (kv-mem/rocksdb) or need a running
SurrealDB before relying on them.

## Decisions already locked
- Approach: **request adjusted bars from Alpaca + segregate the cache by mode** (not local
  split-event tracking).
- Default mode: **`All`** (split + dividend + spin-off), enum wired through so it's
  flippable.

## Line numbers
All `file:line` references are from the state of the repo as of 2026-06-14 and are
navigational hints, not guarantees — confirm before editing.
