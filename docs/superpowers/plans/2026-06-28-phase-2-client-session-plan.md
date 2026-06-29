# Phase 2 — Client session (multiplex + subscription management)

**Fidelity:** high (some specifics pending Phase 1 EventSink shape)

_Part of the datamancer standalone-server roadmap. See `docs/superpowers/specs/2026-06-28-datamancer-server-roadmap.md`._

---

> **Reconciliation pass — authoritative; supersedes any conflicting text below.** Applied from the [cross-phase consistency report](2026-06-28-server-plan-consistency-report.md). Architect decisions: registry/ids/stats are built in **Phase 2 (this phase)** (Issue 3); the diagnostics snapshot is **split** (Issue 6).
>
> Resolutions affecting this phase:
> - **EventSink seam migrates here (Issue 1):** generalize the per-client output `Sink::Attached` to hold `Box<dyn EventSink>` (or make the per-client controller generic over `EventSink`) so Phase 4/5 attach `Iceoryx2DataSink` at the per-client output. Add the checkpoint: *a `ClientSession` is constructed with an `EventSink`.* Tee + resume ring stay core-side; only the attachment point moves from authoritative (Phase 1) to per-client (here).
> - **publish signature (Issue 2):** use `publish(MarketEvent) -> PublishOutcome` (owned) for in-process and `publish_borrowed(&MarketEvent)` for borrow-serializing sinks. No borrowed-`Result`.
> - **Build registry/ids/stats here (Issue 3 — decided: Phase 2):** add a client-session registry to `DatamancerInner`; define `ClientSessionId` in `datamancer-core`; attach a per-symbol `Arc<LiveStats>` to `AuthoritativeSession` (the type absorbing the former `RegistrySentinel`). Phase 3 only reads these.
> - **Backfill scope is creation-time (Issue 7):** backfill is fixed when the authoritative session is created; later referrers (a live `ClientSession::subscribe`, the daemon anchor) attach to the existing scope without re-specifying it. A pure-live subscribe may attach to a backfill-created authoritative session (it joins the live tail).
> - **seq sentinel (Issue 8):** use Phase 1's `Seq::SYNTHETIC` constant for client-local synthetic controls; do not invent a separate value.

> **Detailed-planning hardening (gotcha pass, 2026-06-28) — authoritative.** Adversarial code-level review against current `session.rs` (registry/`RegistrySentinel` ~1538, `run_live` ~1326, `Sink`/`emit`/`forward`) and the Phase 1 hardening. Supersedes conflicting body text.
>
> **Load-bearing invariant.** The authoritative controller holds a **`Weak`** (never a strong `Arc`) to its `AuthoritativeSession`; teardown triggers when the subscriber fan-out map empties (all `SubscriberGuard`s dropped). A strong `Arc` here deadlocks refcounted teardown. Add a `debug_assert` tying fan-out size to `strong_count`.
>
> **Locked decisions:**
> - **Fan-out isolation:** each subscriber gets its own **bounded channel**; the authoritative task **`try_send`s**. A subscriber that fills (slow/wedged) or closes its channel is **removed from the fan-out and gets a `Control::Gap`** for the missed span — never stalls co-subscribers or the provider. (Consistent with the resume-buffer "missed a numbered span → Gap" model.)
> - **Partial-failure signal:** on a per-symbol substream teardown/failure, emit a per-symbol **`SubscriptionChanged{active:false}`** and **suppress** that substream's `SessionClosing`; the client emits one `SessionClosing` only on `ClientSession::close`.
>
> **Locked implementation choices:**
> - **Interleave:** `StreamMap` (O(1) runtime add/remove) — not `FuturesUnordered`.
> - **Tee once at the source.** The client multiplex controller delivers via `emit` only — never `forward`/`tee` (no double-tee, no re-stamp). Guard comment in the loop.
> - **`ClientSessionId`:** `AtomicU64` `fetch_add` (in `datamancer-core`), process-scoped, not persisted. **Client-session registry:** lazy `Weak` cleanup at lookup.
> - **`LiveStats`:** per-field atomics (lock-free reads for Phase 3); no composite-consistency guarantee (document).
> - **Per-client `EventRing`:** `dropped: HashMap<Instrument, GapSpan>`; `note_drop` processes only data events + per-symbol controls (`Gap` embeds its instrument); connection-scoped controls never reach the per-client ring (coalesced upstream) — add a skip-guard + `debug_assert`. Flush emits one `Gap` per affected instrument in first-evicted-`seq` order (overlapping spans are correct).
> - **`next_seq`:** `saturating_add` so it can never wrap into `Seq(u64::MAX)`.
> - **`SubscriptionChanged` cache:** one per symbol on `AuthoritativeSession` (bounded by live symbols), replayed to each new subscriber. Replay carries the real (older) `seq` → a real, possibly large per-symbol `seq` jump on late join; document as **not a loss**.
> - **Connection-scoped Control dedup:** provider-string approximation retained (P2-E), documented as a Phase-2 limitation (multiple authoritative sessions can share a provider id); the per-symbol `SubscriptionChanged` signal above carries symbol-level visibility. Real connection identity deferred to Phase 3. **Sink-level handling:** the in-process multiplex emits connection-scoped controls in-band; a transport sink may treat them differently — the iceoryx2 sink (Phase 4) suppresses them from the POD wire and surfaces provider state via the diagnostics plane (a documented divergence, in-process in-band vs remote via diagnostics).
>
> **Phase-1 dependency guards (tests up front, fail loud):** `poll_next` is pass-through (no re-stamp); `EventSink`/`InProcessSink` exist; overflow yields a real `Control::Gap`; backfill stamps at the seam (not at push).
>
> **Tests to add/confirm:** interleave arrival-order (assert **no** cross-symbol `seq` order); runtime add/remove mid-stream; refcounted teardown (last-referrer tears down; survives while one remains; teardown-window reopen spawns a fresh authoritative session); `slow_client_does_not_stall_co_subscriber` (timeout-bounded — the isolation guard); `per_client_overflow_reports_one_gap_per_affected_instrument`; per-symbol `SubscriptionChanged{active:false}` on substream teardown; connection-scoped control appears once; backfill creation-time scope (reject `backfill_from: Some` on `ClientSession::subscribe`; a late live referrer joins the existing scope, live tail only).
>
> **Accepted pre-existing:** registry-mutex panic-safety (session.rs:245) — the new `authoritative` helper must be panic-free while holding the lock.

## Context & goal

Today a consumer opens a `Session` scoped to exactly one `(instrument, kind)` pair (`crates/datamancer/src/session.rs:219`, the `Datamancer::session` entry point) and drains a single `EventStream`. The live-session registry (`session.rs:191`) enforces *at most one* live session per pair via `RegistrySentinel` strong-count (`session.rs:1538`, `Drop` at `:1543`), rejecting a second live opener with `Error::LiveSessionConflict` (`session.rs:249`).

Phase 2 introduces the **client session** as the primary consumer handle: it holds a mutable set of `(instrument, kind)` subscriptions and presents **one multiplexed stream** combining them. It must:

- **Interleave** (not merge-sort) the subscribed authoritative per-symbol streams into one output. Ordering key is `(instrument, seq)`: monotonic *within* each instrument (source-stamped, from Phase 1), arrival-order across instruments. There is no cross-symbol order to compute — this is what makes the interleave cheap and is the explicit non-goal the roadmap names.
- Support runtime `subscribe` / `unsubscribe`.
- **Refcount-share** the authoritative per-`(instrument, kind)` session (the deterministic singleton). The existing one-live-session-per-pair registry becomes the singleton holder; conflict becomes sharing; the last referrer leaving tears the authoritative session down.
- Scope `Control` correctly: per-symbol controls (`Gap`, `SubscriptionChanged`) ride that symbol's substream; connection/session-scoped controls (`ProviderConnected`/`Disconnected`, `ProviderError`) appear in the multiplexed stream **once**, not duplicated per symbol.
- Back the **per-client multiplexed stream** with the resume buffer (moved out of the authoritative session, per-client, per-instrument gap accounting).

In-process only. No transport. Determinism is **per-symbol only**; cross-instrument/global ordering is an explicit non-goal (roadmap "Determinism scope", "Non-goals").

## Prerequisites / assumptions

Phase 2 depends on Phase 1 (`EventSink` seam + `seq`-at-source). The following are stated explicitly because Phase 1 has not landed; each is a **RE-PLAN CHECKPOINT** to revisit when Phase 1 is concrete. Treat every checkpoint as blocking: if Phase 1 lands a different shape, re-open the named steps before writing code.

- **RE-PLAN CHECKPOINT P1-A — `seq` is stamped at the authoritative source.** This plan assumes Phase 1 moved `seq` stamping out of `EventStream::poll_next` (`session.rs:682-695`; the `stamp_seq` helper at `session.rs:1740`; the per-session `seq_counter` at `session.rs:544`) into the authoritative per-`(instrument, kind)` session, *before* the event reaches any sink. Two consequences the multiplex relies on:
  1. When two client sessions subscribe to the same symbol they observe **identical `(seq, source_ts)`**, and the client multiplex must **not** re-stamp `seq`.
  2. **The output stream type must not re-stamp.** Today `EventStream::poll_next` itself stamps from `SessionInner.seq_counter`. Phase 1 is expected to neuter that (poll becomes a pass-through). Phase 2 **must verify** the stream type it hands back from `ClientSession::take_events` does no stamping; if Phase 1 leaves `EventStream` stamping in place, Phase 2 must introduce a non-stamping output stream type for the client path instead of reusing `EventStream`. If Phase 1 keeps any per-consumer stamping, the multiplex correctness argument breaks and Step 3 must change.
- **RE-PLAN CHECKPOINT P1-B — `EventSink` trait shape & fan-out granularity.** The transport-seam design sketches `EventSink::publish(&self, ev: &MarketEvent)` / `flush(&self)` (`docs/superpowers/specs/2026-06-14-consumer-transport-seam-design.md`, lines ~104-114). This plan assumes: (1) the in-process delivery is an `EventSink` impl the authoritative session publishes into; (2) the tap-log tee (`tee`/`forward`, `session.rs:1411-1462`) and the resume buffer (`EventRing`, `session.rs:1659`) sit **core-side of the sink boundary**, so fan-out and per-client buffering plug in around the sink rather than inside a single sink. Phase 2 turns the single in-process sink into a **fan-out** delivering to N per-client receivers. The *fan-out send strategy* (await-per-subscriber vs `try_send`-with-per-subscriber-isolation) is governed by the Phase-1 sink shape and is called out as a design decision in Step 1.4 + Open Question 5. If Phase 1 instead bakes the resume buffer *inside* the in-process sink, the buffering-granularity work in Step 4 moves.
- **RE-PLAN CHECKPOINT P1-C — overflow→`Gap` semantics post-source-stamping.** Phase 1's invariant change is that a client missing events now observes a *real hole* in that symbol's `seq`, surfaced as `Control::Gap`. The per-instrument ring accounting in Step 4 assumes that change is in place; the current `EventRing.dropped` accounting (`session.rs:1662`, `note_drop` at `:1688`, `into_parts` at `:1713`) tracks a single source-`ts` span per ring and predates it.

Non-Phase-1 assumptions (verified against current code):

- Both crates keep `#![forbid(unsafe_code)]` (`crates/datamancer/src/lib.rs:25`, and the core crate). Nothing in Phase 2 needs `unsafe`.
- Alpaca-only; same-host only. No transport.
- **Each authoritative live session owns its own provider connection.** `provider.start_live(provider_tx)` (`session.rs:315`; Alpaca impl `providers/alpaca.rs:175-184`) spawns a fresh streaming task / websocket per call. So two authoritative sessions for two symbols are two independent connections, each emitting its own `ProviderConnected`/`Disconnected`. This is *why* Step 5's connection-scoped coalescing is a dedup concern in-process, and why the provider-string approximation (P2-E) is acceptable for Phase 2.
- The existing direct `Session` semantics (historical single-shot, live multi-shot, backfill stitching) remain reachable; Phase 2 reframes `Session` internally as a *single-subscription referrer* onto the shared authoritative session (Step 0/2), preserving its public surface except for the conflict→sharing behavior change.

## Step-by-step implementation

### Step 0 — Decide the type split (resolves roadmap open question)

Introduce a **new public `ClientSession`** type; retain `Session` as the single-pair handle but re-implement it internally as a one-subscription referrer onto a shared **authoritative session**. Rationale: minimizes churn (existing `Session` API and its tests stay as the regression guard), and makes refcount-sharing the single code path both handles use. The authoritative session becomes an internal type; the registry holds *it*, not a bare sentinel.

This resolves roadmap Phase-2 open question "Whether today's `Session` evolves into the client session or a new type is introduced" in favor of: **new `ClientSession`, `Session` retained, shared authoritative/registry plumbing underneath.**

### Step 1 — Extract the authoritative session as a refcounted, fan-out unit

Today `Controller` + `SessionInner` (`session.rs:725`, `:526`) *are* the per-pair authoritative machinery, but with a single consumer `Sink` (`session.rs:720`) and a `seq_counter` cloned into one `EventStream`.

1. Introduce `struct AuthoritativeSession` (internal; new `client.rs` module recommended to keep `session.rs` from growing past its current ~2059 lines, or co-located in `session.rs` if cross-references make a split noisy): wraps the controller `cmd_tx`, the per-symbol source `seq` counter (now stamping at source per P1-A), `instrument`, `kind`, the provider id (needed for synthetic controls, Step 3), and a **subscriber fan-out registry**. Held behind `Arc`; refcounted by subscriber guards.
2. Replace the controller's single `Sink` (`Attached`/`Detached`, `session.rs:720`) with a **fan-out**: `HashMap<SubscriberId, mpsc::Sender<MarketEvent>>`, with `SessionCommand::AddSubscriber`/`RemoveSubscriber` reaching the controller's command loop. `emit` (`session.rs:1389`) changes from "send to one channel or buffer in a ring" to "deliver to every registered subscriber sender." **The resume buffer is removed from the authoritative session** (moved per-client, Step 3/4): backpressure to a present-but-slow client is now the per-client controller's job, not the authoritative one's.
   - **Tee placement unchanged and load-bearing:** `tee` (`session.rs:1442`) still runs exactly once at the authoritative source, *before* fan-out, so every subscriber and the tap log see the same teed event once. The per-client controllers must **never** tee (Step 3.3). Confirm tee stays core-side of fan-out (P1-B).
3. **`SubscriberId`**: a `u64` from an `AtomicU64` on `AuthoritativeSession`. `AddSubscriber` returns a fresh `mpsc::Receiver<MarketEvent>` to the caller; the controller keeps the sender.
4. **Fan-out send strategy (design decision, see Open Q5 / P1-B).** A slow or wedged client controller must not stall *other* clients sharing the same symbol, nor the provider. The authoritative loop iterates subscriber senders; a naive serial `send().await` head-of-line-blocks all subscribers behind the slowest. The per-client `EventRing` lives in the *client* controller (Step 3), so the authoritative→client `mpsc` is expected to be drained promptly by that controller's select loop regardless of downstream consumer attach state. Recommended Phase-2 stance: keep the authoritative→client channel bounded and rely on the client controller draining it promptly into its own ring; document that a client controller that genuinely wedges will exert bounded backpressure on the provider (acceptable in-process, small client count). If Phase 1's `EventSink` already defines fan-out semantics, defer to it. **Add the slow-client isolation test (Step "Test plan").**
5. **`seq` at source (P1-A):** the controller stamps `seq` from the per-symbol source counter before fan-out, so every subscriber receiver carries the *same* `(seq, source_ts)`.

### Step 2 — Registry: conflict → refcounted sharing

1. Change `LiveSessionRegistry` (`session.rs:191`) value type from `Weak<RegistrySentinel>` to `Weak<AuthoritativeSession>` (or a `Weak<AuthoritativeInner>`). The authoritative session *is* the sentinel: holding an `Arc` keeps the slot; the `RegistrySentinel::drop` logic (`session.rs:1543-1557` — clear only if `strong_count() == 0`, preserving zero-downtime successor switchover) folds into the authoritative session's `Drop`.
2. **Open path (`Datamancer::session`, `session.rs:239-263`):** when a live pair is requested and the registry slot is **occupied with `strong_count() > 0`**, instead of returning `Error::LiveSessionConflict` (`session.rs:249`), **`upgrade()` the existing authoritative session and add a subscriber** — share. Create-and-insert a new authoritative session only when the slot is empty or stale (`upgrade()` is `None` / `strong_count()==0`). The lock is still held across probe-and-(insert-or-upgrade) so two concurrent openers can't both create. Note the existing comment at `session.rs:256-259`: the lock must be dropped before the `start_live().await`; preserve that — for the *share* path there is no `await` (just `upgrade` + `AddSubscriber` send), for the *create* path keep the existing drop-before-await discipline.
3. Add a private helper `Datamancer::authoritative(&self, instrument, kind, options) -> Result<(Arc<AuthoritativeSession>, SubscriberId, mpsc::Receiver<MarketEvent>)>` encapsulating route + probe-and-(create|share) + AddSubscriber. Both `Session::open` and `ClientSession::subscribe` call it. (Return shape pending the AddSubscriber handshake; adjust to whatever the controller command returns.)
4. **Lifecycle anchor inversion.** Today the live lifecycle anchor is the single `Session`'s `_drop_guard` (`session.rs:523`; controller watches `drop_rx` at `session.rs:1370-1379`). Replace with **subscriber refcount**:
   - Each subscription holds a `SubscriberGuard` (RAII), owning its `SubscriberId` and an `Arc<AuthoritativeSession>` (the Arc is what keeps the registry slot non-stale).
   - **The controller must NOT hold a strong `Arc<AuthoritativeSession>`** (it would pin its own refcount and the slot could never clear). It holds a `Weak`, or nothing, and treats **fan-out-map emptiness as the teardown trigger**.
   - On guard drop → send `RemoveSubscriber`. When the controller observes the fan-out map empty, it runs the existing upstream teardown verbatim — `LiveHandle::unsubscribe` + `close` (`session.rs:1373-1377`) then `shutdown()` — and exits. Separately, the guard's `Arc` drop drives `strong_count → 0`, which clears the registry slot via the folded `Drop` logic.
   - **Ordering note (delicate, Risk #1):** guard drop both queues `RemoveSubscriber` (async) and drops the `Arc` (sync). The registry slot may clear slightly before the controller finishes upstream teardown. That is benign for correctness — a concurrent new opener that arrives in that window sees `strong_count()==0`/`upgrade()==None` and creates a *fresh* authoritative session (new provider connection), exactly as a post-teardown reopen does today. Keep the existing unsubscribe+shutdown sequence byte-for-byte; only change *what* triggers it.
   - **RE-PLAN CHECKPOINT P2-D (Phase 5 interaction):** Phase 5 (`datamancerd`) wants authoritative sessions to keep running *regardless of client presence*. That is satisfied additively by the daemon holding its own `SubscriberGuard`-equivalent referrer; Phase 2 ships "last referrer tears down." Do not special-case it now — keep the refcount the single source of truth so a daemon-held referrer composes.

### Step 3 — Client session: multiplex + per-client resume buffer

Create `struct ClientSession` (public) + a `ClientController` task (in `client.rs`).

1. **`ClientSession` public handle** holds a `cmd_tx: mpsc::Sender<ClientCommand>` to the controller task and an `Arc<Datamancer>`-equivalent (the orchestrator handle is `Clone`, `session.rs:162`). Not `Clone` (single-owner handle), mirroring `Session`.
2. **`ClientController` task** owns:
   - `subscriptions: HashMap<(Instrument, EventKind), SubscriptionEntry>` where `SubscriptionEntry { guard: SubscriberGuard, rx: mpsc::Receiver<MarketEvent>, provider: String }`.
   - a **per-client** client-facing `Sink` (reuse the existing `Sink` enum `Attached`/`Detached`, `session.rs:720`) backed by a **per-client `EventRing`** with per-instrument accounting (Step 4).
   - the connection-scoped control coalescer (Step 5).
3. **Interleave (core loop):** `tokio::select!` over (a) a `StreamMap`/`FuturesUnordered`-style poll across all subscription `rx`s keyed by `(instrument, kind)` (so a removed subscription's future drops cleanly on unsubscribe), (b) the `ClientCommand` channel, (c) the client drop/close signal. For each event drained from any substream, route it through the per-client sink using **emit-only** logic (`emit`, `session.rs:1389`: attached → send with backpressure, flipping to `Detached`+ring on consumer-gone; detached → push to ring). **Never `forward`/`tee`** — the tap-log tee already happened once at the authoritative source (Step 1.2). **No `seq` stamping, no sorting** — arrival order across substreams; `seq` already source-stamped (P1-A). The detach-on-consumer-gone flip (currently in `emit`, `session.rs:1398`) now lives in the *client* controller.
4. **`ClientCommand`** (mirrors `SessionCommand`, `session.rs:699`): `Subscribe { instrument, kind, reply }`, `Unsubscribe { instrument, kind, reply }`, `Take(reply)`, `Close(reply)`. Each carries a `oneshot`.
   - `Subscribe`:
     - Reject a duplicate `(instrument, kind)` already in this client's set (return a clear error; do not double-subscribe).
     - Reject non-live scope / non-`None` backfill for Phase 2 (P2-F) with a clear error.
     - Call `Datamancer::authoritative(...)` → get the `Arc<AuthoritativeSession>` + `SubscriberId` + `rx`; store the `SubscriptionEntry`; add the substream to the interleave set.
     - **Subscription-state visibility for the new subscriber.** When the authoritative session already existed (shared), the upstream provider does **not** re-emit `SubscriptionChanged` (it was already subscribed — Alpaca subscribe path at `providers/alpaca.rs:244`, control emitted at `:374`). Two options; **recommended: the authoritative session caches its last per-symbol `SubscriptionChanged{active:true}` (a real, source-stamped event with a real `seq`) and replays it to each new subscriber on `AddSubscriber`.** This keeps the new subscriber's view inside the source-stamped, identical-`(seq, source_ts)` space and sidesteps the synthetic-seq problem below. If newly created, the real provider-driven `SubscriptionChanged` arrives via the substream naturally (do not also replay a cache).
   - `Unsubscribe`: drop the `SubscriptionEntry` (its `SubscriberGuard` drop → `RemoveSubscriber` → maybe authoritative teardown). Remove the substream from the interleave set. Emit a **client-local** `SubscriptionChanged{active:false}` into this client's stream — this one is genuinely client-local (the authoritative session stays up for other clients), so it is *synthetic*. See the synthetic-seq decision below.
   - `Take`: reuse `prepare_attach` (`session.rs:740`) semantics against the per-client sink — refuse double-take with `Error::EventsAlreadyTaken`, swap a fresh channel, flush the prior per-client ring via `flush_ring`-equivalent (`session.rs:759`, adapted for per-instrument gaps, Step 4).
   - `Close`: emit a single `Control::SessionClosing` into the client stream, drop all subscription entries (tears down each authoritative refcount), shut the task down.
   - **Synthetic-control seq decision (new — the draft missed this).** A client-local synthetic control (`SubscriptionChanged{active:false}` on unsubscribe; a client-local `SessionClosing`) is *not* part of any symbol's source-stamped seq space and is seen by only this client, so it cannot carry an identical-across-clients source seq. Define and document a **sentinel `seq` for client-local synthetic controls** (e.g. `Seq(u64::MAX)` or a documented `Seq::SYNTHETIC`) and assert in tests that consumers treating per-symbol `seq` as a within-instrument order key tolerate it (these controls carry an `instrument` only for `SubscriptionChanged`; consumers must not fold them into per-symbol monotonicity checks). `ControlKind` is unchanged — no new variant — only the seq value convention is new. (This is why the *subscribe* case prefers cached-replay of the real event over synthesis.)
5. **`ClientSession` public methods:** `subscribe`, `unsubscribe`, `take_events`, `close`, plus read-only `subscriptions() -> Vec<(Instrument, EventKind)>` (useful for Phase 3 introspection). `Datamancer::client_session(&self) -> ClientSession` constructor spawns the controller with an empty subscription set.

### Step 4 — Per-client resume buffer with per-instrument gap accounting

Roadmap open question (per-symbol vs per-multiplexed-stream buffering) resolves to **one per-client ring**, but accounting must stay **per-instrument** so overflow does not conflate losses across symbols.

1. Change `EventRing` (`session.rs:1659`): replace `dropped: Option<GapSpan>` (`session.rs:1662`) with `dropped: HashMap<Instrument, GapSpan>`.
2. `note_drop` (`session.rs:1688`): key the extended span by the evicted event's `Instrument` (data events via their `instrument` accessor; an evicted `Control::Gap` via its embedded `instrument`, `event.rs:152-156`). Non-instrument-bearing controls (connection-scoped) are still skipped — they are coalesced upstream (Step 5) and carry no market-data span.
3. `flush_ring` (`session.rs:759`) / `into_parts` (`session.rs:1713`): emit **one `Control::Gap` per affected instrument** (each carrying its own span + provider + instrument) before replaying buffered events in arrival order. Note: `emit_gap` (`session.rs:762`) currently takes a single span and synthesizes one gap; it (or a new helper) needs the instrument + provider to build a per-instrument `ControlKind::Gap`. Provider id comes from the `SubscriptionEntry`. This preserves per-symbol `seq`-hole semantics through the per-client buffer (P1-C).
4. The per-client ring capacity comes from the existing builder knob `resume_buffer_events` (`DatamancerInner` field at `session.rs:180`; builder method at `session.rs:461`; default `DEFAULT_RESUME_BUFFER_EVENTS = 65_536` at `session.rs:715`).

### Step 5 — Connection-scoped Control: appears once

Per-symbol controls (`Gap`, `SubscriptionChanged`) carry an instrument (`event.rs:144-156`) and ride their substream into the multiplex unchanged. Connection/session-scoped controls (`ProviderConnected`/`Disconnected`, `event.rs:138-140`; `ProviderError`, `:158`) are emitted by *each* authoritative substream (each has its own provider connection — see assumptions), so a client with N substreams on one provider would see N copies.

1. In `ClientController`, maintain `seen_provider_state: HashMap<String /*provider*/, ConnState>`. Forward a connection-scoped control into the client stream **only when it changes the recorded state** for that provider (e.g. Connected→Disconnected). This yields "each distinct transition once per provider" — the legible Phase-2 meaning of "rides the multiplexed stream once." `ProviderError` is forwarded once per distinct message-or-transition (decide dedup granularity at implementation; recommended: forward each `ProviderError` but de-dup exact repeats within an unchanged connection state).
2. `SessionClosing` arriving from a substream (because a sibling authoritative session tore down) is **suppressed** at the client multiplex; the client emits its *own* single `SessionClosing` on `ClientSession::close` (Step 3.4).
3. **RE-PLAN CHECKPOINT P2-E:** true connection *identity* (which symbols share one provider websocket; stock = N sockets vs crypto = one hub) is not modeled in core today, and in Phase 2 each authoritative session has its own connection regardless. The provider-string keying above is the Phase-2 approximation. Revisit when Phase 3 adds provider-call/connection accounting so "connection-scoped" can key on a real connection id rather than the provider string.

### Step 6 — Wire-up, exports, examples

- Export `ClientSession` from `crates/datamancer/src/lib.rs:39-41` alongside `Session`.
- Add one `client_session` example demonstrating `client_session()` + `subscribe` over two instruments (leave existing `Session` examples as-is, e.g. `examples/crypto_ticker.rs`, to keep them as regression demos).

## Public API / type changes

- **New public `ClientSession`** (`datamancer`):
  - `subscribe(&self, Instrument, EventKind, Scope, PersistenceOptions) -> Result<()>`
  - `unsubscribe(&self, Instrument, EventKind) -> Result<()>`
  - `take_events(&self) -> Result<EventStream>` (or a non-stamping client stream type — see P1-A.2)
  - `subscriptions(&self) -> Vec<(Instrument, EventKind)>`
  - `close(self) -> Result<()>`
  - **RE-PLAN CHECKPOINT P2-F (backfill under sharing):** a shared authoritative session has *one* scope; two clients cannot give it different `backfill_from`. For Phase 2, **client subscriptions are pure-live** (`Scope::Live { backfill_from: None }`); `Scope::Historical` and non-`None` backfill are **rejected on `ClientSession::subscribe`** with a clear error (different backfill ranges would break the identical-`(seq, source_ts)` guarantee). Direct `Session` retains full backfill/historical on the single-owner path. Recommendation: keep `Scope`/`PersistenceOptions` in the signature (forward-compatible) but reject the unsupported variants now; revisit when per-client historical-join is designed.
- **New `Datamancer::client_session(&self) -> ClientSession`** constructor.
- **`Error::LiveSessionConflict` (`session.rs:249`) is no longer returned for shared live opens.** Keep the variant (it may apply to a future exclusive mode) but it stops firing on a second live referrer. Document the behavior change. (Consider a new error variant for the Phase-2 rejections above, e.g. `Error::UnsupportedClientScope` or reuse an existing config-style error — decide at implementation.)
- **Internal** (no public surface): `AuthoritativeSession`, `SubscriberId`, `SubscriberGuard`, `ClientController`, `ClientCommand`, `SessionCommand::{AddSubscriber, RemoveSubscriber}`, registry value type change (`session.rs:191`), `EventRing.dropped` → per-instrument map (`session.rs:1662`), fan-out replacing the single `Sink` inside the authoritative controller, last-`SubscriptionChanged` cache on the authoritative session.
- `MarketEvent`/`ControlKind`/event model: **unchanged** (`event.rs`). No serde here (Phase 4). Phase 2 synthesizes only existing `SubscriptionChanged`/`Gap`/`SessionClosing` variants. The *only* new convention is the sentinel `seq` value for client-local synthetic controls (Step 3.4) — a value convention, not a type change.

## Test plan

New integration file `crates/datamancer/tests/client_session.rs` (reuse the fake-provider harness from `tests/session_integration.rs:26-160` and `tests/resume.rs:24-215`). Regression guards:

- `multiplex_interleaves_two_instruments_in_arrival_order` — subscribe to two symbols on one client; assert all events appear, each symbol's substream is internally `seq`-monotonic, and **no cross-symbol order is asserted** (arrival order only). Core interleave guard.
- `runtime_subscribe_adds_instrument_midstream` — start with one symbol, `subscribe` a second mid-drain; assert the second's events begin appearing and a `SubscriptionChanged{active:true}` is observed for it (real-or-cached-replay).
- `runtime_unsubscribe_removes_instrument_midstream` — `unsubscribe` mid-drain; assert no further events for that symbol and a client-local `SubscriptionChanged{active:false}`.
- `two_clients_sharing_one_authoritative_see_identical_seq_source_ts` — two `ClientSession`s subscribe to the same pair; assert identical `(seq, source_ts)` per data event (carries the Phase-1 guarantee through the multiplex; directly exercises P1-A).
- `client_output_stream_does_not_restamp_seq` — assert the seq a client observes equals the source-stamped seq (guards P1-A.2 — no double-stamping in the client output stream).
- `slow_client_does_not_stall_co_subscriber` — two clients share one symbol; one client stops draining (detaches / wedges its consumer); assert the other client keeps receiving promptly (guards the fan-out isolation decision, Step 1.4). Bound the assertion with a timeout.
- `last_referrer_drop_tears_down_authoritative_session` — two referrers; drop both; assert upstream `unsubscribe` ran and the registry slot cleared (probe via a fresh open creating a *new* authoritative session, e.g. observing a fresh `ProviderConnected`). Refcount-teardown guard.
- `authoritative_survives_while_one_referrer_remains` — drop one of two referrers; assert the symbol keeps flowing to the survivor and upstream stayed subscribed.
- `per_client_overflow_reports_one_gap_per_affected_instrument` — detach the client stream, overflow the per-client ring with events from two symbols, reattach; assert exactly one `Control::Gap` *per affected instrument* with correct spans (per-instrument accounting guard; analogue of `resume.rs:219 overflow_reports_one_gap_and_tap_log_captures_everything`).
- `connection_scoped_control_appears_once_in_multiplex` — two substreams both emit `ProviderConnected`; assert the client sees it once per provider/transition.
- `session_closing_emitted_once_on_client_close` — `close()` emits exactly one `SessionClosing`; substream closings are suppressed.
- `unsubscribe_then_resubscribe_synthesizes_or_replays_subscription_changed` — covers the control path for shared/rejoined subscriptions.
- `client_subscribe_rejects_historical_and_backfill` — `Scope::Historical` and `Live{backfill_from: Some(_)}` rejected with a clear error (P2-F).
- `client_subscribe_rejects_duplicate_pair` — subscribing the same `(instrument, kind)` twice on one client errors (Step 3.4).

Unit tests (`#[cfg(test)]` in `session.rs`/`client.rs`): `EventRing` per-instrument `note_drop`/`flush` (two instruments evicted → two spans, with an evicted `Control::Gap` folding its embedded span into the right instrument); the connection-scoped coalescer state machine.

Regression suite (must pass unchanged except where noted): `tests/session_integration.rs`, `tests/resume.rs`, `tests/historical_cache.rs`. **Updates required** — the three `live_session_conflict_*` tests (`session_integration.rs:346, 388, 423`): conflict becomes sharing. Convert `live_session_conflict_rejects_second_live_session_for_same_pair` into `second_live_open_shares_authoritative_session` (assert both handles receive the same events with identical `seq`); keep `..._clears_when_first_is_dropped` / `..._clears_when_first_is_closed` but re-point them at refcount teardown (slot clears only when the *last* referrer leaves).

## Doc / invariant updates

- **`CLAUDE.md` (root)** — rewrite the **"Single ordered stream"** invariant: a `Session` no longer "exposes exactly one events() stream merging all subscriptions." Replace with: the **client session** presents one multiplexed stream over a mutable subscription set, **deterministic per symbol with no cross-symbol ordering**, ordering key `(instrument, seq)`, interleave (not merge-sort). Explicitly contrast with the never-realized global-merge model. Coordinate with Phase 1's `seq` edits to the same file.
- **`crates/datamancer/README.md`** — update the "emits one ordered stream… demux downstream", "single output stream multiplexes everything… globally ordered", and related passages. Replace the global-merge model with the client-session multiplex model (per-symbol determinism, `(instrument, seq)` key, refcounted shared authoritative sessions, client session as primary handle). Drop any "persistence sinks use seq gaps to detect drops" framing that assumes a single global stream (cross-reference the Phase-1 per-symbol `seq` edit).
- **`crates/datamancer/CLAUDE.md`** — the "merged stream must be totally ordered and reproducible" line becomes **per-symbol determinism** (within-instrument total order; cross-instrument arrival-order only).
- **`crates/datamancer/src/lib.rs:5`** module doc ("produces a single ordered event stream") and **`crates/datamancer/src/session.rs:1-42`** module docs — align with per-symbol `seq` and the client/authoritative split. Coordinate with Phase 1's edits to the same spots (`event.rs` `seq` doc).

## Open questions

1. **`subscribe` signature scope** — keep `Scope`/`PersistenceOptions` and reject unsupported variants (recommended, P2-F), or drop them for Phase 2? Recommendation: keep + reject.
2. **Interleave fairness** — `StreamMap` vs `FuturesUnordered` vs round-robin: any is correct (no cross-symbol order guarantee), but a busy symbol could starve others. Probably fine for Phase 2; note if fairness becomes a concern.
3. **Connection identity** (P2-E) — provider-string coalescing is an approximation until Phase 3 connection accounting.
4. **Does `Session` stay, or become a thin `ClientSession`-with-one-subscription wrapper?** Recommended: keep `Session` as-is on the single-owner path (less churn, preserves backfill); share only the authoritative/registry plumbing (Step 0).
5. **Fan-out send strategy** (Step 1.4, P1-B) — await-per-subscriber vs `try_send`-with-isolation. Governed by the Phase-1 `EventSink` shape; the `slow_client_does_not_stall_co_subscriber` test is the guard regardless of choice.
6. **Synthetic-control `seq` sentinel value** (Step 3.4) — exact value and whether it warrants a named `Seq` constant in core; coordinate with the Phase-1 `seq` doc edit.

## Risks

- **Refcount teardown vs the old `drop_guard` anchor** (highest-risk item). The drop_guard→subscriber-refcount inversion touches the most subtle lifecycle code (`session.rs:1370-1379`, registry switchover `:1543-1557`). The controller-holds-`Weak`-not-`Arc` rule (Step 2.4) is load-bearing: holding the `Arc` would deadlock teardown. Mitigation: keep the existing unsubscribe+shutdown sequence verbatim; only change *what* triggers it (fan-out map empty). Tests `last_referrer_drop_tears_down_authoritative_session` + `authoritative_survives_while_one_referrer_remains` are the guards.
- **Conflict→sharing is a behavior change** altering existing tests and any embedder relying on `LiveSessionConflict`. Mitigation: explicit doc + test conversion; the variant is retained.
- **Per-client buffering granularity** — a single per-client ring is correct only with per-instrument accounting (Step 4); otherwise overflow conflates symbols' gaps. The per-instrument-gap test guards it.
- **Synthetic-control seq** — injecting client-local controls into a source-stamped seq space is a correctness wrinkle the draft missed; resolved by the sentinel convention (Step 3.4) + preferring cached-replay for the subscribe case. Guard with the resubscribe test and a consumer-tolerance assertion.
- **Fan-out backpressure / co-subscriber isolation** — a slow client controller could backpressure the authoritative `mpsc` and (since the authoritative session no longer buffers) the provider. Mitigation: the client controller drains its subscriber `rx` promptly into its own ring under detach; the detach-on-consumer-gone flip moves to the client controller. Guard: `slow_client_does_not_stall_co_subscriber`.
- **Phase-1 coupling** — checkpoints P1-A/B/C gate correctness of Steps 1, 3, 4. If Phase 1 lands a different `EventSink`/`seq` placement (especially leaving `EventStream` stamping in place, P1-A.2), revise before coding.
- **Risk level: moderate** — lifecycle, refcounting, and Control routing, as the roadmap notes. No merge-sort, so well short of the rejected unified-`seq` rewrite.

## Review notes

Changes made to the draft (adversarial review against the roadmap + current code; all cited file:line claims in the draft were checked and found accurate):

- **New correctness gap surfaced — synthetic-control `seq`.** The draft synthesized `SubscriptionChanged`/`SessionClosing` into the multiplex without addressing that these client-local controls have no source-stamped, identical-across-clients `seq` (which P1-A makes the rule). Added a sentinel-seq convention (Step 3.4), and changed the *subscribe* case to prefer the authoritative session **caching + replaying the real `SubscriptionChanged`** (a genuine source-stamped event) over synthesis, leaving only the genuinely client-local unsubscribe/close controls synthetic.
- **New correctness gap — output-stream re-stamping.** `EventStream::poll_next` currently stamps `seq` itself (`session.rs:682-695`). Promoted this to P1-A.2 with an explicit Phase-2 verification step and a dedicated test (`client_output_stream_does_not_restamp_seq`); the draft only said "multiplex must not re-stamp" without noting the existing stream type does.
- **New correctness gap — controller must hold `Weak`, not `Arc`.** The draft's refcount inversion would deadlock if the controller pinned its own `Arc<AuthoritativeSession>`. Made the Weak rule and the fan-out-map-emptiness trigger explicit (Step 2.4), and documented the benign guard-drop ordering window.
- **Sharpened fan-out isolation.** The draft mentioned backpressure only in Risks; promoted it to a design decision (Step 1.4, Open Q5) tied to P1-B, with a `slow_client_does_not_stall_co_subscriber` test, since head-of-line blocking across co-subscribers would silently violate per-client independence.
- **Corrected tee semantics in the client controller.** The draft said route "exactly as forward/emit do today"; `forward` tees to the tap log. Specified emit-only in the client controller (tee happens once at the authoritative source) to avoid double-logging.
- **Completeness additions:** explicit rejection of `Scope::Historical`/backfill and duplicate-pair on `ClientSession::subscribe` (+ tests); provider-id plumbing for per-instrument `Gap` synthesis (Step 4.3, since `emit_gap` currently lacks instrument/provider); confirmation that each authoritative session owns its own provider connection (assumptions) which underpins Step 5; `lib.rs:5` module-doc added to the doc-update list.
- **Tightened file:line references** to the actual entry points (`Datamancer::session` at 219, `seq_counter` at 544, builder knob at 461 vs inner field at 180, default at 715, drop_rx loop at 1370-1379, Alpaca subscribe at 244 / control at 374).
- **Altitude:** kept the high-tier detail, left Phase-1-dependent specifics as checkpoints, did not over-specify Phase 4/5 transport mechanics.

Unresolved concerns (carry into implementation): (1) exact `EventSink`/fan-out shape and the send strategy await the Phase-1 plan (P1-B/Q5); (2) the synthetic-seq sentinel value should be coordinated with the Phase-1 `seq` doc rewrite so the two edits to `event.rs`/README don't conflict; (3) whether `ProviderError` dedup granularity (Step 5.1) needs per-message vs per-transition is left to implementation judgment.
