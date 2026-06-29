# Phase 6 — Introspection web UI

**Fidelity:** design-level (firms up after 3 + 5)

_Part of the datamancer standalone-server roadmap. See `docs/superpowers/specs/2026-06-28-datamancer-server-roadmap.md`._

---

> **Reconciliation pass — authoritative; supersedes any conflicting text below.** Applied from the [cross-phase consistency report](2026-06-28-server-plan-consistency-report.md). Architect decisions: registry/ids/stats built in **Phase 2** (Issue 3); diagnostics snapshot **split** (Issue 6).
>
> Resolutions affecting this phase:
> - **Snapshot naming (Issue 5):** use `SystemSnapshot` and `Datamancer::snapshot() -> Result<SystemSnapshot>` (async, fallible) throughout — not `IntrospectionSnapshot`/`introspect()`. The ArcSwap-refresh task already plans off-thread acquisition, so async+fallible is mechanical; narrow CHECKPOINT A to the unit-identity-key question.
> - **Reads the live-state snapshot (Issue 6):** the UI reads Phase 3's bounded live-state snapshot in-process; the heavier cache catalog comes from its separate accessor/cadence.
> - **Per-symbol `seq` display tolerates the sentinel (Issue 8):** treat `Seq::SYNTHETIC` synthetic-control entries as exempt from per-symbol monotonicity in any gap/ordering display.

> **Fidelity: design-level.** Concrete endpoint shapes and field names firm up
> once Phase 3 fixes the snapshot type and Phase 5 fixes the daemon
> runtime/lifecycle. Every dependency on those phases is a **RE-PLAN
> CHECKPOINT** below. Re-author the executable, task-by-task version of this
> plan with `writing-plans` once Phases 3 and 5 have merged.

## Context & goal

`datamancerd` (Phase 5) is the long-lived server process that hosts the
authoritative per-`(instrument, kind)` sessions, fans them out to client
sessions, publishes the Phase-4 diagnostics plane, and assembles the Phase-3
introspection snapshot (provider accounting, cache catalog, live system state).
Phase 6 adds an **HTTP server, embedded in the daemon's existing tokio
runtime**, that renders that snapshot **read-only** for a single same-host
operator:

- JSON endpoints over the Phase-3 snapshot API (the machine surface).
- A small web UI over those endpoints (the human surface; built with the
  `frontend-design` skill, per roadmap mandate).
- Optionally a `/metrics` Prometheus endpoint derived from the same data.

This phase adds **no** new domain state and **no** new ordering/transport
semantics. It is a pure consumer of the Phase-3 snapshot — the *same* snapshot
the Phase-4 diagnostics plane carries to client processes; the web UI is just
the in-process operator reader of it. The hard determinism constraints
(per-symbol `seq`, `(instrument, seq)` multiplex ordering key, authoritative
per-`(instrument, kind)` singleton, same-host/Alpaca-only,
`#![forbid(unsafe_code)]`) are upheld trivially because this phase never touches
the data plane — it reads a structured snapshot and serializes it. Auth is
deferred (loopback bind, single operator).

## Hard-constraint conformance (explicit)

- **Per-symbol determinism, no cross-symbol ordering.** Phase 6 reads, never
  produces, ordering. The UI must present every ordered quantity (`seq`,
  coverage, latency) **scoped to its authoritative unit** and must never imply a
  global cross-symbol order. See the UI determinism rules in step 6.
- **`(instrument, seq)` is the only ordering key; `seq` is per-symbol.** The UI
  labels `seq` columns as per-symbol and never sums/sorts events across symbols
  into one global sequence.
- **Authoritative per-`(instrument, kind)` singleton, refcounted.** The
  snapshot surfaces the refcount/subscriber count per authoritative unit; the
  UI shows it as a shared singleton, not per-client.
- **One multiplexed stream per client.** The UI presents the client session as
  the primary handle with its subscription set (the Phase-2 model), not a flat
  list of streams.
- **Same-host only, Alpaca-only.** Loopback bind, no network exposure, no
  multi-provider assumptions in the UI.
- **`#![forbid(unsafe_code)]`** stays at the `datamancerd` crate root; none of
  the added deps (axum/tower-http/serde/metrics) require unsafe in *our* code
  (the forbid governs this crate's source, not its dependencies).

## Prerequisites / RE-PLAN CHECKPOINTS

This phase is downstream of Phases 3 and 5. The following are **assumptions**,
each gated by a checkpoint that must be reconciled before execution. (Verified
against current `main` at plan time: `seq` is stamped at *delivery* in
`EventStream::poll_next` — not yet at source; the live-session registry exists
as `DatamancerInner::live_sessions`; `HistoricalCache` has only
`lookup`/`store`/`gaps`/`as_replay_source` — **no catalog-enumeration method
yet**; the event model has **no serde**; there is **no `datamancerd` crate
yet**. All of these are produced by Phases 1, 3, and 5 — Phase 6 depends on
their outputs, none of which exist on `main` today.)

- **RE-PLAN CHECKPOINT A — Phase-3 snapshot type.** Assumed: Phase 3 lands a
  single consolidated, owned, `Serialize`-deriving snapshot type in
  `datamancer-core` (working name `IntrospectionSnapshot`) containing provider
  accounting, the cache catalog, and live system state (authoritative
  per-`(instrument, kind)` sessions, client sessions + subscriptions,
  per-unit subscriber/refcount, per-symbol `seq` position, per-instrument last
  `source_ts`/`rx_ts` + latency, resume-buffer occupancy, gap counts). Assumed
  accessor on the orchestrator: `Datamancer::introspect() -> IntrospectionSnapshot`
  (or an `Arc<ArcSwap<IntrospectionSnapshot>>`-style cheap-clone handle). The
  exact type name, module path, sub-struct names, the **identity used to key
  authoritative units** (instrument-only vs `(instrument, kind)` vs
  `(instrument, kind, adjustment)` — note `CacheKey` already carries
  `adjustment`), and the accessor signature are **Phase-3 decisions**. Until
  Phase 3 lands, treat every snapshot field reference here as illustrative.
  **Revisit when Phase 3 merges: reconcile field names, the unit-identity key,
  and the accessor shape, and confirm the snapshot is cheap to obtain off-thread
  (atomics / arc-swap, not a lock held across `.await`).**

- **RE-PLAN CHECKPOINT B — Phase-5 daemon host & lifecycle.** Assumed: Phase 5
  produces a `datamancerd` binary crate with a single shared tokio runtime, a
  long-lived `Arc<Datamancer>` (or equivalent server context), and a graceful
  shutdown signal (broadcast / `oneshot` / `CancellationToken`). The web server
  must attach to **that** runtime via `tokio::spawn` and to **that** shutdown
  signal via `axum::serve(...).with_graceful_shutdown(...)`. The exact context
  type, shutdown primitive, and config plumbing are **Phase-5 decisions**.
  **Revisit when Phase 5 merges: wire to the real context handle and shutdown
  future, and add the HTTP bind address / enable flag to the Phase-5 config
  format.**

- **RE-PLAN CHECKPOINT C — serde on the snapshot only; share with Phase 4.**
  The event-model serde gap (no serde on `MarketEvent`/`Control`/`Seq`/`Price`,
  confirmed on `main`) does **not** block this phase, because Phase 6 serializes
  only the Phase-3 *snapshot*, never raw `MarketEvent`s. Assumed: the snapshot
  type derives `Serialize` and all constituent types (embedded `Instrument`,
  timestamps, `seq`-position scalars) serialize as plain values. If Phase 3 left
  any snapshot field carrying a non-serde type, that is a Phase-3 bug to fix
  there, not here. **Two confirmations at merge:** (1) the whole snapshot
  round-trips through `serde_json` (reuse Phase 3's own round-trip test); (2) if
  Phase 4 already defines a serialized snapshot wire form for the diagnostics
  plane, Phase 6 reuses **that same `Serialize` impl** — do not introduce a
  divergent JSON shape for the same data.

- **RE-PLAN CHECKPOINT D — control-surface unification (roadmap open
  question).** Phase 5 ships a `subscribe`/`unsubscribe` control surface. This
  phase keeps the web UI **read-only** (`get`-only). Whether the UI later drives
  that control surface (trigger fetch / sub-unsub from the browser) is deferred.
  **Revisit if/when a mutation surface is wanted; adding guarded
  `post`/`delete` routes then is additive.**

- **Assumption — dependency choices.** axum `0.8.x`, `tower-http 0.6` (`fs`
  feature for static assets, `trace` for request logging), `serde`/`serde_json`
  (already workspace deps). Metrics path, if built: `metrics` +
  `metrics-exporter-prometheus`. These are same-host server deps and belong in
  the `datamancerd` crate, **not** in `datamancer` or `datamancer-core` (keeps
  the library transport-free per the architecture invariant). **At
  implementation time:** pin to the verified latest `0.8.x` patch
  (`cargo update -p axum --precise <ver>` after checking), and verify axum's
  default-feature set (whether `json`/`query`/`tokio` are default has shifted
  across the `0.8.x` line — do not assume) and the route-param syntax (axum 0.8
  uses brace syntax `/{param}`, **not** the `0.7` `/:param`).

- **Assumption — read-only by construction.** Hardening = register **only `get`
  routes**; no `post`/`put`/`delete`/`patch` handlers exist, so there is no
  axum-level mutation path to flip. (Checkpoint D covers later unification.)

- **Assumption — single-origin, no CORS.** The UI and the JSON API are served
  from the **same** loopback origin, so no CORS layer is needed. Do **not** add
  a permissive CORS layer (`Any` origin) — it would widen the same-host
  read-only surface to any local browser origin. If a separate dev origin is
  ever used, scope CORS to exactly that origin, never `Any`.

## Step-by-step implementation

All work is in the **`datamancerd` binary crate** (created in Phase 5). Nothing
in `datamancer` or `datamancer-core` changes in this phase — if it does, that is
a signal a Phase-3 snapshot field is missing and the work belongs in Phase 3
(flag it back, do not add it here).

### 1. Crate wiring

- Add dependencies to `crates/datamancerd/Cargo.toml`: `axum = "0.8"`,
  `tower-http = { version = "0.6", features = ["fs", "trace"] }`, `serde_json`
  (workspace), and (if metrics) `metrics` + `metrics-exporter-prometheus`.
  `tokio` and `serde` are already present from Phase 5.
- Gate the whole web server behind a cargo feature `web-ui` (default-on in the
  daemon, off-able for a headless build), plus a runtime config enable flag
  (RE-PLAN CHECKPOINT B). Keep `#![forbid(unsafe_code)]` at the crate root.

### 2. HTTP module layout

Create `crates/datamancerd/src/web/` with:

- `web/mod.rs` — `pub fn router(state: WebState) -> axum::Router` and
  `pub async fn serve(state: WebState, addr: SocketAddr, shutdown: impl Future)`.
- `web/state.rs` — `WebState`, a cheap-`Clone` (`Arc`-wrapped) handle carrying
  whatever obtains a fresh Phase-3 snapshot (RE-PLAN CHECKPOINT A: likely
  `Arc<Datamancer>` or `Arc<ArcSwap<IntrospectionSnapshot>>`).
- `web/handlers.rs` — the `get` handlers.
- `web/dto.rs` — only if the raw Phase-3 snapshot needs reshaping for the wire.
  Prefer serializing the snapshot (or its sub-structs) directly; add view-model
  structs only if a field needs hiding/flattening for the UI.
- `ui/` (static assets) — see step 6.

### 3. State handle: snapshot acquisition off the hot path

- `WebState` exposes `fn snapshot(&self) -> IntrospectionSnapshot` (or
  `Arc<...>`). It must **not** hold a lock across `.await` and must not block the
  shared executor that also drives the live data plane.
- **Default plan: daemon-side periodic refresh into an `ArcSwap`.** The daemon
  refreshes the snapshot on a controlled cadence; handlers do a lock-free
  `state.snapshot.load_full()`. This keeps HTTP handlers always non-blocking and
  bounds the cost of the snapshot walk — critically the **cache-catalog
  enumeration**, whose on-disk volume estimate may require a filesystem walk of
  the SurrealKV store (potentially blocking; MVCC/un-GC'd VLog can overstate
  live size — see open question 5). Refresh cadence is independent of the
  browser poll interval; pick a refresh interval (e.g. 1 s) decoupled from how
  often the SPA polls.
- **Fallback:** if Phase 3 instead offers only an on-demand
  `Datamancer::introspect()` that walks live structures cheaply and
  non-blockingly, the handler may call it directly. Any potentially-blocking
  component (the disk walk) must still go behind `tokio::task::spawn_blocking`
  or be moved to the daemon's refresh timer. **RE-PLAN CHECKPOINT A governs the
  final choice.**

### 4. JSON endpoints (the machine surface, `get` only)

Endpoint set (axum 0.8 brace syntax for any params):

- `GET /api/snapshot` — the entire `IntrospectionSnapshot` as JSON. The single
  source of truth; the UI can be built against this alone. All others are
  conveniences/filters over the same data.
- `GET /api/cache` — cache catalog: enumerated keys (instrument, kind,
  adjustment), covered ranges, on-disk volume estimate. Backed by the Phase-3
  catalog-enumeration method (the new `HistoricalCache` catalog API Phase 3
  adds — current `gaps()` answers coverage for *one* key only; do not re-derive
  the catalog here).
- `GET /api/providers` — provider accounting: history-fetch count, live
  reconnects, rate-limit hits, message/byte throughput, last error, connection
  state per provider.
- `GET /api/sessions` — live state: authoritative per-`(instrument, kind)`
  sessions with subscriber/refcount; client sessions and their subscription
  sets; per-symbol `seq` position; per-instrument last `source_ts`/`rx_ts` and
  `rx_ts − source_ts` latency; resume-buffer occupancy; gap counts.
- `GET /api/health` — liveness/readiness (process up, provider connection-state
  rollup). Cheap; suitable for frequent polling.

Each handler returns `axum::Json<T: Serialize>` built from the snapshot.
`/api/cache`, `/api/providers`, `/api/sessions` are projections of
`/api/snapshot`'s sub-structs (return the sub-struct directly — no copy logic
beyond a field access). Keep handlers thin: acquire snapshot → project → `Json`.

### 5. Optional `/metrics` (Prometheus)

- Behind a sub-feature `metrics` (off by default until a scraper is actually
  deployed).
- Use `metrics-exporter-prometheus`: build a `PrometheusHandle` once at daemon
  startup and add `GET /metrics` returning `handle.render()`. Translate the
  snapshot's numeric fields (fetch counts, reconnects, rate-limit hits,
  throughput, per-symbol gap counts, latency, resume-buffer occupancy,
  subscriber counts) into gauges/counters — either updated on the daemon refresh
  cadence or recomputed on scrape. Label per-symbol metrics with the
  instrument (and kind/adjustment as appropriate) so they stay per-symbol —
  never a single conflated global counter.
- **Hazard:** `install_recorder()` is process-global and one-shot — it
  panics/errs if called twice. Install it exactly once at daemon startup (not
  per-request, not in two tests in the same process). For tests, isolate to a
  single `#[test]` or render without a global install. Document this inline in
  the metrics module.

### 6. The web UI (static assets, served by `tower-http`)

- **RE-PLAN CHECKPOINT — UI tech (roadmap open question): server-rendered vs
  SPA.** Default recommendation: a **lightweight static SPA** (plain HTML +
  vanilla JS or a tiny framework, no build pipeline required) that polls the
  JSON endpoints on an interval and renders tables/cards. Rationale: the data is
  read-only and low-rate; a static bundle keeps the daemon dependency-light and
  avoids a templating engine in a data process. Revisit if the operator wants
  live push (would argue for SSE/WS — out of scope here).
- Serve assets with `ServeDir` + a SPA fallback
  (`ServeDir::new(ui_dir).not_found_service(ServeFile::new(index_html))`).
  Default: assets **on-disk**, rooted at a path from the Phase-5 config, falling
  back to a compiled-in default dir. If a single-file binary is wanted later,
  embed with `rust-embed`/`tower-serve-static` (verify axum-0.8 fit then).
- **Use the `frontend-design` skill when building the UI** (roadmap mandate).
- **UI determinism rules (load-bearing — uphold the per-symbol non-goal in how
  data is displayed):**
  - Make per-symbol / per-authoritative-unit framing **primary**. Surfaces:
    cache catalog + coverage; provider call counts / rate-limit usage /
    throughput / connection health; live client sessions + subscriptions;
    per-symbol latency, `seq` position, and gap counts; resume-buffer occupancy.
  - **Never present a single global cross-symbol "event count", "stream
    position", or merged event ordering.** Any count is per-symbol (or an
    explicitly-labeled sum-of-independent-counters, not an ordering).
  - Label `seq` columns explicitly as **per-symbol** and never sort multiple
    symbols' events into one combined sequence.
  - Show the authoritative unit as a **shared singleton** with its
    subscriber/refcount; show client sessions as the primary consumer handle
    with their subscription sets. Do not collapse `kind` (and `adjustment`,
    where the snapshot distinguishes them) — two kinds for one instrument are
    distinct authoritative units.

### 7. Serve + lifecycle integration

- `serve(state, addr, shutdown)` builds the router, binds a
  `tokio::net::TcpListener` on **loopback** (`127.0.0.1:<port>` and/or
  `[::1]:<port>`) for same-host-only (auth deferred), and runs
  `axum::serve(listener, router).with_graceful_shutdown(shutdown)`.
- Phase 5 spawns this via `tokio::spawn(serve(...))` on the **shared** runtime
  (never a second runtime) and passes its shutdown future so the HTTP server
  drains on daemon shutdown alongside sinks and tap log.
- Add `tower_http::trace::TraceLayer` for request logging via the existing
  `tracing` setup. **RE-PLAN CHECKPOINT B governs the exact spawn site and
  shutdown primitive.**

## Public API / type changes

- **`datamancer-core`:** none. (If a snapshot type needs serde or a field, that
  is a Phase-3 change, not Phase 6.)
- **`datamancer`:** none expected. (If the daemon cannot obtain a non-blocking
  snapshot, the fix — e.g. an `ArcSwap`-published accessor — is Phase-3 scope;
  flag it back rather than adding it here.)
- **`datamancerd` (new in Phase 5, extended here):**
  - new feature `web-ui` (and optional `metrics`);
  - `pub mod web` with `WebState`, `router()`, `serve()`;
  - config additions: HTTP enable flag, bind address/port, UI asset dir,
    metrics toggle (added to the Phase-5 config format — RE-PLAN CHECKPOINT B).
- **Wire/JSON contract:** the JSON shape **is** the Phase-3 snapshot's
  `Serialize` output (shared with the Phase-4 diagnostics plane — CHECKPOINT C).
  Treat it as a versioned read-only contract for the UI; introduce `web/dto.rs`
  view-models only if the UI needs a shape decoupled from internal snapshot
  churn — otherwise serialize the snapshot directly.

## Test plan

Tests live in the `datamancerd` crate. Named regression guards:

- **`web_router_get_only`** (unit) — assert the router exposes only `get`
  methods: every registered route returns `405 Method Not Allowed` for
  `POST`/`PUT`/`DELETE`/`PATCH`. Regression guard for the read-only invariant
  (CHECKPOINT D).
- **`web_no_permissive_cors`** (unit) — assert no `Any`-origin CORS layer is
  registered (guards the single-origin security posture). Lightweight: assert
  responses carry no `access-control-allow-origin: *`.
- **`web_snapshot_endpoint_serializes`** (integration) — build `WebState` over a
  synthetic/known snapshot (or a real `Datamancer` with seeded cache + a fake
  provider), `GET /api/snapshot`, assert `200`, `content-type:
  application/json`, and that the body deserializes back into
  `IntrospectionSnapshot` (round-trip). Primary guard tying the UI contract to
  the Phase-3 type. (CHECKPOINT A/C: reuse Phase-3's serialize round-trip
  fixture.)
- **`web_section_endpoints_match_snapshot`** (integration) — assert
  `/api/cache`, `/api/providers`, `/api/sessions` each equal the corresponding
  sub-object of `/api/snapshot` (projections do not drift).
- **`web_cache_catalog_reflects_stored_ranges`** (integration) — seed the
  SurrealKV cache with known ranges (reuse existing cache test fixtures from
  `tests/surreal_cache.rs` / `tests/historical_cache.rs`), assert `/api/cache`
  lists those keys (instrument, kind, adjustment) + ranges. Guards the
  catalog-enumeration plumbing end-to-end through HTTP.
- **`web_seq_is_per_symbol_in_payload`** (integration) — with two distinct
  symbols seeded, assert the `/api/sessions` (or snapshot) payload exposes `seq`
  positions **keyed per symbol** and exposes no global/merged sequence field.
  Guards the per-symbol determinism non-goal at the contract level (a contract
  guard complementing the visual UI rule).
- **`web_handler_does_not_block_runtime`** (integration) — with the
  daemon-refresh `ArcSwap` design, assert a handler completes without invoking
  the blocking snapshot/disk walk on the request path (e.g. instrument the
  on-demand accessor to panic if called from a handler, asserting handlers only
  read the `ArcSwap`). Guards the executor-blocking risk.
- **`web_graceful_shutdown_drains`** (integration) — drive `serve()` with a
  trigger-able shutdown future; assert in-flight requests complete and the task
  resolves. Mirrors the Phase-5 shutdown-ordering test.
- **`metrics_endpoint_renders`** (integration, feature `metrics`, possibly
  `#[ignore]` due to the process-global recorder hazard) — `GET /metrics`
  returns Prometheus text exposition with expected (per-symbol-labeled) metric
  names. Document the one-shot-`install_recorder` constraint inline.
- **Manual / smoke** — run `datamancerd` with `web-ui`, open the UI in a
  browser, confirm panels render and refresh; confirm no panel implies a global
  cross-symbol order. Use the `verify` skill and the `frontend-design` workflow
  during UI construction.

Existing `datamancer` / `datamancer-core` suites must remain green (this phase
adds no core code; if they break, something leaked out of the daemon crate).

## Doc / invariant updates

- **No core invariant changes.** The `seq`/determinism/ordering invariants in
  `CLAUDE.md`, `crates/datamancer/README.md`, and `event.rs` are settled by
  Phases 1–2; Phase 6 must not restate or alter them.
- **`datamancerd` README / docs:** document the web surface — endpoint list,
  JSON contract = Phase-3 snapshot shape (shared with the Phase-4 diagnostics
  plane), loopback-only bind, single-origin/no-CORS, read-only, auth-deferred,
  the `web-ui`/`metrics` features, and the config keys. State explicitly that
  the UI presents per-symbol framing and implies **no** cross-symbol ordering.
- **Roadmap doc** (`docs/superpowers/specs/2026-06-28-datamancer-server-roadmap.md`):
  mark Phase 6 status implemented when done; record resolved open questions (UI
  tech choice; read-only vs control-surface unification; assets on-disk vs
  embedded; whether `/metrics` was built).
- Note the same-host/loopback + auth-deferred posture prominently as a security
  boundary (single operator, no network exposure, no permissive CORS).

## Open questions

1. **(Roadmap) UI tech: server-rendered vs SPA.** Defaults to a static polling
   SPA; revisit if live-push or heavier interactivity is wanted.
2. **(Roadmap, CHECKPOINT D) Read-only now vs unifying with the Phase-5 control
   surface** (trigger fetch / sub-unsub from the UI). Deferred; `get`-only now
   makes adding a guarded mutation surface later additive.
3. **(Roadmap) Auth deferred** — loopback/same-host single-operator. Revisit if
   network exposure is ever wanted (out of scope).
4. **Snapshot acquisition cadence** — per-request vs daemon-refreshed `ArcSwap`.
   Defaults to daemon-refresh so handlers are lock-free and the cache-disk-walk
   is rate-limited. Final call depends on CHECKPOINT A.
5. **On-disk cache size source** — a filesystem walk overstates live size
   (SurrealKV MVCC / un-GC'd VLog). Decide whether the UI labels it "approximate
   (includes version history)" or uses a count×row-size estimate. Really a
   Phase-3 catalog decision surfaced in the UI; reconcile at CHECKPOINT A.
6. **Assets on-disk (`ServeDir`) vs embedded (`rust-embed`).** Defaults to
   on-disk; embedded if a single-file binary is required (verify axum-0.8 fit).
7. **`/metrics` worth building now?** Optional; the snapshot JSON already serves
   operators. Gate behind a feature; decide on whether a scraper is deployed.
8. **Authoritative-unit identity in the snapshot** — instrument-only vs
   `(instrument, kind)` vs including `adjustment`. The UI must key on whatever
   Phase 3 chooses without collapsing distinct units (CHECKPOINT A).

## Risks

- **Dependency on unlanded phases (highest).** The snapshot type (Phase 3) and
  the daemon host/lifecycle (Phase 5) are assumed. If Phase 3 ships a snapshot
  that is expensive to obtain or carries non-serde fields, or Phase 5's
  runtime/shutdown wiring differs, this plan needs the marked re-plans.
  Mitigant: CHECKPOINTS A/B/C/D localize the exposure; the endpoint/UI design is
  independent of those details.
- **Blocking the executor in a handler.** A naive per-request snapshot/disk walk
  could stall the shared runtime that also drives the live data plane. Mitigant:
  daemon-refresh `ArcSwap` + `spawn_blocking` for any disk walk; the
  `web_handler_does_not_block_runtime` guard.
- **UI implying global ordering.** Presenting a conflated cross-symbol stream
  would contradict the per-symbol determinism non-goal. Mitigant: the UI
  determinism rules (step 6), the `web_seq_is_per_symbol_in_payload` contract
  guard, and a `frontend-design` review.
- **Process-global one-shot `install_recorder`.** Test flakiness / double-install
  panics if `/metrics` is built. Mitigant: install once at startup; isolate the
  metrics test (possibly `#[ignore]`).
- **axum 0.8 version churn** — route syntax (`/{id}`), default-feature drift.
  Mitigant: pin `0.8.x`, verify default features and route syntax at
  implementation time.
- **Read-only drift / CORS widening** — a future contributor adding a mutating
  route or a permissive CORS layer. Mitigant: `web_router_get_only` and
  `web_no_permissive_cors` guards.
- **Same-host security posture** — binding beyond loopback would expose an
  unauthenticated surface. Mitigant: hard-code loopback default, document the
  boundary, keep auth-deferred explicit.

(Overall risk: **moderate**, isolated from core ordering — the phase is a
read-only consumer of an existing snapshot. The real risk concentration is the
two upstream-phase assumptions, not the HTTP/UI code itself.)

## Review notes

Changes made to the draft during adversarial review (verified against `main`:
`seq` stamped at delivery in `EventStream::poll_next`, live-session registry
present, no `HistoricalCache` catalog method, no event-model serde, no
`datamancerd` crate — all consistent with the draft's dependency framing):

- **Added an explicit "Hard-constraint conformance" section** mapping each
  roadmap hard constraint to how this phase upholds it, so reviewers do not have
  to infer it.
- **Sharpened the determinism framing throughout** from loose "per-symbol" to
  the authoritative unit being per-`(instrument, kind)` (and possibly
  `adjustment`), since the deterministic unit and the `(instrument, seq)`
  ordering key are distinct concepts. Added open question 8 and a checkpoint note
  flagging the unit-identity decision to Phase 3.
- **Added a contract-level determinism guard** `web_seq_is_per_symbol_in_payload`
  (the draft only guarded ordering visually in the UI, which is untestable in CI;
  this adds a payload-shape assertion).
- **Added single-origin / no-CORS as an explicit security assumption**, a
  `web_no_permissive_cors` guard, and a doc note — a realistic drift vector the
  draft omitted.
- **Promoted the control-surface unification to RE-PLAN CHECKPOINT D** (it was
  only an open question), since it is a roadmap-level decision the next planner
  must consciously reconcile.
- **Noted the snapshot serialization is shared with the Phase-4 diagnostics
  plane** (folded into CHECKPOINT C) — reuse the same `Serialize`, do not fork
  the JSON shape.
- **Decoupled daemon refresh cadence from browser poll cadence** explicitly in
  step 3 (the draft conflated them).
- **Softened the axum patch-version claim** ("current 0.8.6") to "verify latest
  0.8.x at implementation time" — I cannot verify a specific patch from here, and
  the draft itself already hedged.
- **Anchored fixture references to real files** (`tests/surreal_cache.rs`,
  `tests/historical_cache.rs`) verified present.
- Kept the plan at design-level altitude: did not over-specify Phase-3 field
  names or Phase-5 lifecycle primitives (left as checkpoints), and did not
  expand speculative later-phase (network/auth) detail.

Unresolved concerns (cannot be closed until upstream phases land):
- Whether Phase 3's snapshot is cheap-clone/non-blocking or requires a disk walk
  (drives the `ArcSwap` vs on-demand decision) — CHECKPOINT A.
- The exact authoritative-unit identity key — CHECKPOINT A / open question 8.
- Whether Phase 4 defines the canonical serialized snapshot shape this UI must
  reuse — CHECKPOINT C.
- Phase 5's shutdown primitive and config format — CHECKPOINT B.
