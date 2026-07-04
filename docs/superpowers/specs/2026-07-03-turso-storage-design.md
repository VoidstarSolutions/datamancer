# Turso Storage Backend — Design

**Date:** 2026-07-03
**Status:** Approved direction (user decision); plan to follow after SP1 (CI) completes.
**Status:** implemented — cutover landed in Task 9 of the corresponding plan; `storage-turso` is the default, `storage-surreal` is deleted.

## Goal

Replace the SurrealDB storage backend (`crates/datamancer/src/storage/surreal.rs`)
with a Turso-based backend, removing the workspace's only BUSL-1.1 dependency
tree (and both no-fix advisories rooted in it) ahead of the open-source flip.

## Decision and rationale

**Engine: Turso (`turso` crate — the Rust ground-up SQLite rewrite, MIT),
from day one.** Considered and rejected:

- `rusqlite`/SQLite: safest engine, but reintroduces a C dependency and
  cross-language build friction the project explicitly wants to avoid.
- `redb`/pure-Rust KV: no SQL; hand-rolled key encoding and range logic for
  what is naturally relational data.
- Keep SurrealDB: heavy dependency tree (~200 crates), BUSL-1.1
  source-available license, two advisories with no upstream fix.

Turso is in beta (as of 2026-07). Accepted because: the storage layer is a
read-through cache + tap log whose data is recoverable from providers; the
usage pattern is deliberately simple; and real deployment is still some way
off. Risk is contained by the two constraints below.

## Binding constraints

1. **SQLite-compatible subset only.** Schema and queries restricted to what
   both Turso and stock SQLite execute identically: `CREATE TABLE`,
   `CREATE INDEX`, `INSERT`, half-open-range `SELECT`/`DELETE`,
   transactions. No engine-specific SQL. This keeps the on-disk format and
   dialect portable — if Turso beta bites, `rusqlite` is a contained
   fallback for the same file, not a migration.
2. **Crash-durability parity tests on the tap log before default.** The
   flush contract is load-bearing (tap-log flush before sink flush before
   service drop — CLAUDE.md). The port's test suite must include
   kill-during-append tests demonstrating that a completed `flush` survives
   process death, before `storage-turso` becomes a default feature.

## Shape of the port

- New `crates/datamancer/src/storage/turso.rs` behind feature
  `storage-turso`; implements the same `HistoricalCache` + `TapLog` traits.
- Semantics ported 1:1 from `surreal.rs` (974 lines, well-tested): per-kind
  tables (`trades`, `quotes`, `bars_1s` … `bars_1d`), the `coverage` table
  with intersection/`gaps` logic, store-claims-whole-range behavior for
  empty fetches, adjustment-mode scoping of bar rows, source-`seq` persisted
  verbatim, strictly end-of-log tap appends.
- Real composite index `(provider, symbol, adjustment, source_ts)` — the
  index the surreal module doc wished for.
- Async: `turso` is async-native; no `spawn_blocking` shim.
- Existing surreal backend tests become the parity suite: port them against
  the new backend first (TDD), then add the crash-durability tests.
- Cutover: `storage-turso` replaces `storage-surreal` in default features;
  the surreal module and feature are **deleted** in the same change (not
  kept as an alternative — one backend, per YAGNI), along with the
  transitional `deny.toml` exceptions and advisory ignores (marked in that
  file) and the surreal-specific config keys in `datamancerd`.

## Out of scope

- Cache volume/eviction (still deferred, as before).
- Turso's MVCC/`BEGIN CONCURRENT`, vector search, or any beta-only feature.
- Data migration from existing surreal caches (pre-deployment; caches are
  rebuildable from providers).

## Sequencing

After SP1 (CI pipeline) of the open-sourcing program; before SP5
(flip-public), so the public repo never ships a BUSL dependency tree.
Ordering with SP2–SP4 is flexible; earlier is better because it deletes the
`deny.toml` transitional block that SP1's gate documents.
