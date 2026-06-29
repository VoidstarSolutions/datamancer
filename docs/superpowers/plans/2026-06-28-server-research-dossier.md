# Datamancer Standalone-Server Roadmap — Research Dossier

_Auto-generated shared ground truth for the per-phase implementation plans (see the roadmap spec). Descriptive, not a plan._

# Datamancer Standalone-Server Roadmap — Research Brief (Shared Ground Truth)

This brief consolidates the codebase and external-dependency research for the standalone-server roadmap. It is descriptive ground truth, not a plan. All paths are relative to `/Users/zacharyheylmun/dev/voidstar/datamancer/`. Primary source file for the orchestrator is `crates/datamancer/src/session.rs` unless noted.

Roadmap invariants this brief is written against (do not violate downstream):
- Determinism is **per-symbol only**; cross-instrument/global ordering is a non-goal.
- One multiplexed stream per client over one connection; the client session is the primary consumer handle; single-instrument = one subscription.
- `seq` is per-symbol, stamped at source, identical across clients of a symbol; multiplex ordering key is `(instrument, seq)`.
- The authoritative per-`(instrument, kind)` session is the deterministic unit — a shared, refcounted singleton (existing one-live-session-per-pair registry).
- Same-host only (network deferred); Alpaca-only; transport carries a data plane **and** a diagnostics plane.
- Both crates keep `#![forbid(unsafe_code)]`.

---

## (a) Current Architecture & Key Extension Points

### Session / Controller / Registry

- **`Datamancer`** (session.rs:163) is an `Arc`-shareable orchestrator holding providers (168), optional tap log + cache (169–170), the live-session registry (179: `Arc<Mutex<HashMap<(Instrument, EventKind), Weak<RegistrySentinel>>>`), resume-buffer capacity (180), and the corporate-action adjustment mode (185, stamped into every `HistoryRequest` and `CacheKey`).
- **`Session`** (513) is a single-owner (non-`Clone`) handle. It holds `_registry_anchor: Option<Arc<RegistrySentinel>>` (519) and `_drop_guard: Option<oneshot::Sender<()>>` (523, the live lifecycle anchor). Public API: `take_events()`, `set_persistence()`, `close()`.
- **`Controller`** (725) runs the lifecycle in a spawned tokio task. Entry points `run_historical()` (773) and `run_live()` (1326). `forward()` (1411) tees to tap log then emits; `emit()` (1389) delivers-or-buffers without tee.
- **`Scope`** (63): `Historical { from, to }` | `Live { backfill_from: Option<Timestamp> }`. Backfill stitches history `[from, B)` (B = wall-clock at session start) with buffered live arrivals spliced at the seam.
- **`SessionCommand`** (699): `SetPersistence`, `Take`, `Close` — each carries a `oneshot` reply.
- **Open path** (`Datamancer::session`, 219): fail-fast on `UnsupportedEventKind`, `LiveSessionConflict` (249), or missing persistence (226–230). Live sessions probe-and-reserve the registry slot atomically (241–259); occupied + `strong_count() > 0` ⇒ reject. Live upstream subscribe happens eagerly before the `Session` returns (315–316).
- **`RegistrySentinel`** (1538): Session holds `Arc`, registry holds `Weak`; `Drop` (1543) clears the entry only if no successor registered (`strong_count() == 0`, 1553) — enables zero-downtime switchover.
- **Teardown**: Live — session drop → `drop_guard` fires → controller sees `drop_rx` close (1370) → unsubscribe (1374–1375) → `shutdown()` (1377). Historical — `finish_historical` (860) checks `stream_taken` (863–864); never taken ⇒ shutdown; taken ⇒ emit `SessionClosing` (870–875) and drain.

**Extension points / relevance to roadmap.** The authoritative per-pair singleton the roadmap needs already exists as the registry + `RegistrySentinel` refcount mechanism; refcounting client sessions onto a shared authoritative session is an extension of `strong_count`-based sharing, not a new concept. `SessionCommand` (699) and `Scope` (63) are the natural additive seams for new client-session commands and modes. `Datamancer.route` (351) is the capability-based provider-pinning seam.

**Gotchas.** Registry mutex `unwrap` assumes no panics inside the critical section (245). `stream_taken` uses Acquire/Release pairing (864 read / 585 write). For live, the `drop_guard` — not stream-taken — is the lifecycle anchor.

### Seq Stamping (load-bearing change for the roadmap)

- **Today seq is stamped at delivery, per-consumer**, in `EventStream::poll_next` (682–695): `Seq(self.seq.fetch_add(1, Relaxed))` then `stamp_seq(ev, seq)`. Counter lives in `SessionInner.seq_counter: Arc<AtomicU64>` (526–549), init 0 (289), cloned into each `EventStream` on `take_events()` (599) so seq is **contiguous across re-takes by construction**.
- `stamp_seq` (1740–1748) overwrites the `seq` field on Trade/Quote/Bar/Control.
- **Events never delivered are never numbered** — ring evictions (1650–1658) and undelivered channel residue (560–563) produce no seq and no hole.
- Control events are created with placeholder `Seq(0)` (emit_gap 905–920; SessionClosing 873; ProviderError 932; shutdown 1518) and overwritten at delivery.
- Providers never assign seq (provider.rs:40–46): they surface ordered/decoded events; "Datamancer assigns final seq downstream."

**Roadmap implication (the core refactor).** The roadmap requires seq stamped **at source**, per-symbol, identical across clients. This moves stamping out of `poll_next` and into the authoritative per-`(instrument, kind)` session, before fan-out. Consequences:
1. The "contiguous-by-construction / no holes" property is **deliberately given up** downstream: a client missing events (buffer overflow, late join) observes a real hole in `seq`, surfaced as `Control::Gap`.
2. Multi-take contiguity that is currently free (shared counter) must instead come from the single shared upstream counter.
3. Tap-log already assigns its own canonical seq (see below); per the roadmap these must converge to one shared per-symbol order.

### Resume Buffer / EventRing / Detached Sink

- **`EventRing`** (1659–1716): bounded FIFO with loss accounting. Capacity via `DatamancerBuilder::resume_buffer_events()`, **default 65,536** (180, 457–464). Overflow evicts oldest and extends a `dropped: Option<GapSpan>` span (1674–1681). Evicted data events extend `[ts, ts+1)`; evicted `Control::Gap` preserve their embedded span (1683–1698); other controls are skipped. Evicted events are never seq-stamped.
- **`Sink`** (717–723): `Attached(mpsc::Sender)` (backpressured delivery into the live `EventStream`) | `Detached(EventRing)` (buffer while no consumer). Default attach channel buffer = **1024** (709–711) — distinct from the 65,536 ring.
- **`prepare_attach`** (740–753): rejects double-take (`EventsAlreadyTaken`), creates fresh channel, swaps in `Attached`, extracts the prior ring.
- **`flush_ring`** (759–768): on re-attach, emit one `Control::Gap` for the dropped span, then replay buffered events in arrival order.
- **`emit` detach-on-drop** (1389–1404): on `SendError` (consumer gone) swap to `Detached(EventRing)` and park events.

**Backfill seam machinery.** `run_backfill` (1248–1321) runs the same tile/segment streaming as historical (1298) with a **separate** `pending: EventRing` (1300) for live arrivals during backfill; `BackfillSide` (1580–1592) threads `provider_rx`, `pending`, `drop_rx`, and `edge` (B). `buffer_live_arrival` (1416–1436) **tees to tap log then buffers** (durability survives later eviction). Seam flush (1317–1320): one Gap for evictions + buffered events; a healthy seam emits no synthetic control. Conservative edge coverage: a segment touching `edge` claims only `confirmed_prefix_end()` (1085–1094); failed gap-fetch gaps through to `edge` (1136–1139). `tile()` (1624–1648) partitions `[from,to)` into alternating Covered (cache replay) / Gap (provider fetch) segments; gap-fetch failure claims only the confirmed prefix (1113–1149).

**Roadmap implications.** The tee and resume buffer sit **core-side of the consumer sink** (forward at 1411–1414; detach at 1389–1404) — every future sink (in-process, iceoryx2) inherits them. Open design question: with one multiplexed stream per client over many symbols, buffering granularity (per-symbol ring vs per-multiplexed-stream ring) is undecided. Backfill seam (`buffer_live_arrival` special case) must survive any EventSink refactor.

### Cache (HistoricalCache / SurrealKV)

- **Trait** (datamancer-core/src/traits/storage.rs:47–96): `lookup` (52) → `Option<CacheCoverage>`; `store` (56) claims coverage for the exact key range (even if empty); `gaps` (68, default reports only fringes — backends override for internal holes); `as_replay_source` (95).
- **`CacheKey`** (111–122): `instrument` (carries provider), `kind`, `from`, `to`, `adjustment`. **`CacheCoverage`** (125–133): `from`, `to`, `event_count`, `first_seq`/`last_seq` — **always `None`** in Surreal.
- **Surreal impl** (crates/datamancer/src/storage/surreal.rs): coverage stored as one `CoverageDoc` per key (142) with sorted non-overlapping `segments: Vec<(i64,i64)>`; `merge_in` (257) collapses overlaps; `gaps_within` (297) enumerates all internal + fringe holes; `gaps()` override (467–479) reports all holes. `coverage_id` (179–187) keys on `provider|symbol|table|adjustment`. `effective_adjustment` (172–177): **bars respect adjustment; trades/quotes forced to `Raw`**. Row IDs use 20-digit zero-padded nanos (394) for lexicographic = source-time order. `lookup` returns `None` both on no-doc and zero-overlap (331–346).

**Roadmap implications (cache enumeration — "what's cached?").** No trait API to enumerate keys/catalog today; you must read the `coverage` table directly (it is the authoritative "what ranges are cached" record). A new method like `enumerate_keys() -> Vec<(CacheKey, CacheCoverage)>` would make it explicit. `first_seq`/`last_seq` being hardcoded `None` (343) and replay hardcoding `Seq(0)` (618 etc.) means the cache layer carries **no usable seq today** — relevant if deterministic per-symbol seq must survive a cache round-trip. No indices defined (48–49).

### Provider Edge (Alpaca)

- **Trait** (datamancer-core/src/traits/provider.rs): dynamic dispatch at the cold boundary only (start/subscribe/unsubscribe/fetch_history, 10–15). `start_live` (47) → `Box<dyn LiveHandle>`; `fetch_history` (52–56) owns pagination + rate limits; `subscribe`/`unsubscribe` (95/98) operate one pair at a time; `list_instruments` (78–80) default empty.
- **Stock** (providers/alpaca.rs): `start_live` (175–184) spawns **one websocket per call — no connection multiplex**. `LiveCommand` enum (226–230); `AlpacaLiveHandle.active` mirror (239). Subscribe sends **full snapshot** each call (364), rolls back on error (385–386). Reconnect (299–471): re-applies full subscription list (340), emits `ProviderConnected`/`ProviderDisconnected`. `fetch_history` (685–774): Trade ✓, **Quote NOT implemented** (720–728 returns error), Bar ✓ with adjustment.
- **Crypto** (providers/alpaca_crypto.rs): `start_live` (189–196) **multiplexes via a lazily-spawned shared hub** (`ensure_hub`, 156/163); `routes: HashMap<(Instrument, EventKind), mpsc::Sender>` (326). `fetch_history` returns error (202–206).
- **`ReconnectPolicy`** (session.rs:1779–1793): 500ms initial, 30s max, full jitter.

**Roadmap implications.** Connection multiplexing is **not enforced by core** — stock = N websockets, crypto = one shared hub (Alpaca allows one connection per credential pair). No provider-call accounting / metrics exist anywhere; hook points are the call sites at session.rs:315 (start_live), 808/1040 (fetch_history), 316/1345/1356/1374 (sub/unsub), and inside the per-provider tasks. Stock subscribe is full-snapshot (non-delta) and reconnect re-applies the full list — accounting must distinguish provider-level from connection-level events. `FetchLocks` dedups overlapping historical fetches (1212–1235) but only on the cold-sweep `Scope::Historical` path (not backfill).

### Event & Control Model (the serialization blocker)

- **`MarketEvent`** (datamancer-core/src/event.rs:60) and Trade (87) / Quote (97) / Bar (109) / Control (128) / ControlKind (135) / GapSpan (163) / Seq (18) / Price (price.rs:12) all derive only `Debug, Clone, PartialEq` — **NO serde**.
- Serde-enabled: `Timestamp` (25, transparent i64), `BarInterval` (30), `Instrument` (instrument.rs:90), `ProviderId` (19), `AssetClass` (66).
- `MarketEvent` is `#[non_exhaustive]` (59); **`ControlKind` is NOT** — adding control variants is a breaking change.

**Control scoping (drives multiplex routing).** Per-symbol (instrument-qualified): `SubscriptionChanged` (144, carries instrument+kind), `Gap` (152, carries instrument+span). Connection/session-scoped (broadcast to all clients): `ProviderConnected` (138), `ProviderDisconnected` (140), `ProviderError` (158), `SessionClosing` (160).

**Roadmap implications.** Shipping events over any transport requires serde (or a custom encoder) on the data + Control types and on `Seq`/`Price` — this is the single largest event-model gap. The per-symbol vs connection-scoped split maps directly onto the roadmap's data plane (per-`(instrument, seq)`) vs diagnostics plane (broadcast). The timestamp triple (source_ts = market time / decisions; rx_ts = observability only; seq = sole ordering key) must be preserved end-to-end (see CLAUDE.md invariants).

### Tap Log

- **Trait** (datamancer-core/src/traits/storage.rs:31–43): `append`, `flush`, `as_replay_source`.
- **Tee placement is core-side, before the consumer sink**: `forward()` (session.rs:1411–1414) tees then emits; `tee()` (1438–1462) gates: data-events only (Control skipped, 1443–1448), live-scope only (backfill/historical route to cache), skip when `write_tap_log` off.
- **`SurrealTapLog`** (storage/surreal_tap_log.rs): `append` (315–321) enqueues to an **unbounded** channel (299), non-blocking, swallows send errors. `flush` (323–332) barrier-syncs via oneshot, returns the most recent write error once then clears. Background writer (345–519) assigns its **own canonical `seq` (hwm)** per event on the hot path (392–394); incoming session events carry stub `seq=0`. Writes are best-effort (logged, never propagated into the live session). Replay (534–652) fetches each per-pair shard, **materializes the whole window into one vec, sorts by seq, then streams** (650).

**Roadmap implications.** The roadmap's Phase-1 `EventSink` trait (datamancer-core; `publish` in seq order, `flush` for shutdown) keeps the tap-log tee and resume buffer core-side so every sink inherits them (per docs/superpowers/specs/2026-06-28-datamancer-server-roadmap.md:145–152 and the transport-seam design 2026-06-14). The tap log's per-file canonical `seq` and the roadmap's source-stamped per-symbol `seq` must become the **same value** — converging these is the key cross-layer coordination. Control events, backfill data, and overflow gaps are **not** in the tap log (data-only), so tap-log replay alone cannot reconstruct diagnostics. `set_persistence` (1498–1510) toggles `write_tap_log` at runtime — the sink design must support mid-stream toggling.

---

## (b) External Dependency Facts (uncertainty flagged)

### iceoryx2 (same-host data plane)

- **Version churn — pin and verify at implementation time.** Latest published ≈ **0.9.2** (some snippets cite 0.8.1). Apache-2.0 OR MIT, pure-Rust core.
- Model: `Node` → named *service* → ports. `ipc::Service` = cross-process shared memory (the server case); `local::Service` = single-process. True zero-copy, lock-free; no broker daemon strictly required for pub-sub.
- Pub-sub API (confirmed): `node.service_builder(&name).publish_subscribe::<T>().open_or_create()?`; publisher `loan_uninit()`/`write_payload()`/`send()` or `send_copy()`; subscriber `receive()` returns `Option` (non-blocking). Multiple publishers/subscribers are first-class. Blocking/event-driven wakeups need a **separate event/notifier (WaitSet) service** composed with pub-sub — **verify wiring**.
- Late-joiner / buffering config on the builder (confirmed signatures): `history_size(N)` (retained samples for late joiners), `subscriber_max_buffer_size`, `max_subscribers`, `max_publishers`, `enable_safe_overflow(bool)` (true = lossy newest-wins; false = backpressure), `subscriber_max_borrowed_samples`. **All fixed at service creation — pre-allocated shared memory, cannot grow dynamically.**
- **History caveat:** pub-sub `history_size` requires the **publisher process alive** to deliver history; it does not survive publisher exit. Persistence-after-writer-exit is the separate **blackboard** pattern, not pub-sub.
- Request-response exists (`request_response::<Req,Resp>()`, client/server builders, streaming `PendingResponse`) — surface is newer, **verify method names against the pinned version**. Candidate for a control/diagnostics request channel.
- **Payload = the main constraint (and the `forbid(unsafe_code)` gate).** Payload must be `#[repr(C)]`, self-contained, **no heap/pointers/`String`/`Vec`/`Box`**. Use `#[derive(ZeroCopySend)]` (generates a *safe* impl); variable-length data → fixed-capacity containers from `iceoryx2-bb-container` (`FixedSizeByteString`, `FixedSizeVec`) or slice payloads `publish_subscribe::<[T]>()`. A **hand-written `impl ZeroCopySend` is `unsafe`** and would violate `forbid(unsafe_code)`. **Verify-during-implementation gate:** write one POD payload early and confirm it compiles under `#![forbid(unsafe_code)]` using only the derive. This is where the POD/interning layer for `MarketEvent` lives — entirely sink-side, core `MarketEvent` untouched.
- Discovery: hierarchical names (`"My/Funk/Name".try_into()?`); `open_or_create()` checks type/config compatibility; service-discovery + global config exist — **exact discovery call surface uncertain, verify**.

### axum (diagnostics / introspection plane)

- **Pin to axum 0.8** (current **0.8.6**; 0.9 in-dev/breaking). Builds on hyper 1.x, tokio 1.44+; `axum::serve` re-exported (no direct hyper).
- Minimal deps: `axum = "0.8"`, `tokio` (`rt-multi-thread`/`macros`/`net`), `tower-http = "0.6"` (`fs` for `ServeDir`/`ServeFile`), `serde`. `Json` is in default features. **Verify the exact 0.8.6 default-feature set** (whether `json`/`query` are default — has shifted across 0.8.x).
- **Route path syntax changed in 0.8**: captures are `/{id}` (braces), wildcard `/{*rest}` — old `/:id` panics/won't match.
- JSON: return `Json<T: Serialize>`; shared state via `.with_state(...)` + `State<T>` (`Clone+Send+Sync+'static`, wrap in `Arc`). **Prefer atomics / `arc-swap` snapshots over a contended lock** held across `.await`.
- Static UI: `nest_service("/assets", ServeDir::new(dir))`; SPA fallback `ServeDir::new("ui").not_found_service(ServeFile::new("ui/index.html"))`. Embedding assets in the binary is not tower-http — needs `rust-embed` or `tower-serve-static` (**verify axum-0.8 fit**).
- `/metrics`: low-dep path = `metrics-exporter-prometheus` `PrometheusBuilder::new().install_recorder()?` + `get(move || ready(handle.render()))`. `install_recorder` is **process-global, one-shot — fails if called twice** (test-harness hazard). Alternative `axum-prometheus` is heavier and **historically lags axum majors — verify 0.8 support**.
- Tokio integration: **embed in the existing runtime via `tokio::spawn` — a second runtime is an anti-pattern.** Bind `127.0.0.1` or UDS for same-host-only. `with_graceful_shutdown(future)` wired to the orchestrator's shutdown. **Do not block the executor in handlers** (snapshot via atomics/`spawn_blocking`) — the main embedding risk in a live data process. Read-only hardening = register only `get` routes (no axum-level switch).

### Cache enumeration (SurrealKV)

- **`INFO FOR DB`** returns schema only (tables map = name → DEFINE statement, plus users/params/functions/etc.) — **never counts or byte sizes**. `INFO FOR TABLE <t>` = fields/indexes/events. **Verify the deserialized shape in surrealdb 3.0** (`Value` vs typed map); mirror the repo's existing `.query(...).take(n)`.
- Counts: `SELECT count() FROM <t> GROUP ALL` (plain `count()` yields per-record 1s). Full scan without a `DEFINE INDEX ... COUNT` (repo defines none). For the domain catalog, **read the `coverage` table directly** — it is already the authoritative "what symbols/ranges/adjustments are cached" record; prefer it over re-deriving from data tables.
- **No query/SDK API for on-disk byte size.** `ESTIMATE_COUNT()` is unimplemented (issue #4164) — do not rely on it.
- Disk estimation:
  - **A (recommended):** filesystem walk of the embedded SurrealKV directory (LSM: WAL + SSTables + VLog + manifest). True footprint but whole-store (not per-table) and **overstates live size** (MVCC version history, un-GC'd VLog, pre-compaction redundancy). `Memory` config has no path.
  - **B:** `count × estimated_bytes_per_row` from the fixed schema — per-table logical breakdown, approximate (ignores compression/keys/overhead).
  - **C (uncertain):** SurrealKV checkpoint internally reports SSTable count/total bytes but is **not surfaced through the surrealdb Rust SDK** — treat as unavailable.
- Verify: `INFO FOR DB` deserialized type in 3.0; whether a COUNT index is worth adding; whether SurrealKV 3.0 runs background compaction/GC automatically (affects how much walk A overstates).

---

## (c) Most Important Risks & Cross-Cutting Concerns

1. **Seq semantics inversion is the central, load-bearing change.** Moving seq from per-consumer delivery-time (poll_next, 690–691) to per-symbol source-time at the authoritative session changes an invariant that the whole codebase currently leans on ("delivered stream contiguous by construction; drops are never seq holes"). Post-change, holes in `seq` become real and must be surfaced as `Control::Gap`. Ring-overflow accounting (1650–1698), the backfill seam, and multi-take contiguity all currently depend on non-stamping of undelivered events; each must be re-derived under source-stamping. The tap-log's existing canonical seq (surreal_tap_log.rs:392) and the new per-symbol seq must converge to one value.

2. **No serde on the event model.** Trade/Quote/Bar/Control/ControlKind/GapSpan/Seq/Price have no serde (event.rs). Every transport (iceoryx2 POD payloads, axum JSON) is blocked until this is addressed — either serde derives or a sink-side encoder. `ControlKind` is **not** `#[non_exhaustive]`, so any wire-format design must account for control-variant additions being breaking.

3. **Determinism is per-symbol only — the multiplex must not imply global order.** The authoritative unit is the per-`(instrument, kind)` session; the multiplex key is `(instrument, seq)`. Buffering granularity for the multiplexed client stream (per-symbol ring vs single shared ring) is an open design question with direct correctness impact on per-symbol gap accounting — a shared ring would conflate losses across symbols and muddy the per-symbol `Gap`.

4. **Refcounted shared authoritative session vs. the current single-owner model.** Today `Session` is non-`Clone`, anchored by `drop_guard`, with a one-live-session-per-pair registry enforced by `RegistrySentinel` strong-count. Client sessions sharing one authoritative session require refcounting the authoritative handle and reworking teardown (last-client-drop vs. drop_guard) and the registry's switchover semantics (1543–1553).

5. **Two planes over one transport.** Per-symbol data (`SubscriptionChanged`, `Gap`) routes per-instrument; connection/session-scoped controls (`ProviderConnected/Disconnected`, `ProviderError`, `SessionClosing`) broadcast. iceoryx2 maps cleanly to per-symbol pub-sub services + a broadcast/diagnostics service or request-response channel; the split is already legible from `ControlKind` payloads (event.rs:135–160).

6. **iceoryx2 `forbid(unsafe_code)` gate.** Safe only if every payload uses `#[derive(ZeroCopySend)]` + fixed-size containers; any hand-written `unsafe impl ZeroCopySend` breaks the crate invariant. Validate with one payload type before committing to the approach. iceoryx2 resource limits are fixed at service creation (size max_subscribers/buffers/history up front), and pub-sub history needs the publisher alive — late-joiner snapshot strategy must account for this (or use the symbol-table/announcement service noted in the roadmap).

7. **axum embedding risks in a live process.** Single shared tokio runtime (`tokio::spawn`, not a second runtime); never block the executor in handlers (snapshot via atomics/`arc-swap`); `install_recorder` is process-global one-shot (test hazard); pin axum 0.8 and verify default features + route syntax. Bind loopback/UDS for same-host-only.

8. **No provider-call accounting exists.** Any diagnostics plane reporting fetch/subscribe/reconnect activity needs instrumentation added at the cold-boundary call sites (session.rs:315/808/1040/316/1345/1356/1374) and inside provider tasks. Stock full-snapshot subscribe + full re-apply on reconnect means call counts ≠ subscription deltas. `FetchLocks` dedup runs only on `Scope::Historical`, not backfill — contention accounting must observe pre-lock.

9. **Cache lacks both an enumeration API and on-disk size introspection.** The catalog must read the `coverage` table directly (authoritative) and estimate disk via filesystem walk (whole-store, overstates live size). `first_seq`/`last_seq` are unpopulated and replay hardcodes `Seq(0)` — the cache carries no usable seq, relevant if deterministic per-symbol order must survive cache replay/backfill.

10. **Tap-log durability is best-effort and unbounded.** Append uses an unbounded channel (surreal_tap_log.rs:299) with no backpressure; write failures are logged, never propagated unless a caller hits `flush()` (which `close()` does, session.rs:1523). A long-lived live session that never closes can silently drop tap writes on disk error and accumulate unbounded in-flight events. Tap-log replay materializes the entire window in memory and sorts by seq (650).

---

Key files for plan authors: `crates/datamancer/src/session.rs` (orchestrator/controller/ring/backfill/seq), `crates/datamancer-core/src/event.rs` (event model + serde gap), `crates/datamancer-core/src/traits/{provider.rs,storage.rs}` (trait seams), `crates/datamancer/src/providers/{alpaca.rs,alpaca_crypto.rs}` (provider edge), `crates/datamancer/src/storage/{surreal.rs,surreal_tap_log.rs}` (cache + tap log), `crates/datamancer/README.md` (authoritative design doc), `docs/superpowers/specs/2026-06-28-datamancer-server-roadmap.md` and the 2026-06-14 transport-seam design (EventSink shape).
