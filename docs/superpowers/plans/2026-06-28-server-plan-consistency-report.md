# Datamancer Standalone-Server Roadmap — Cross-Phase Consistency Report

_Auto-generated cross-phase coherence pass over the six plans._

Based on the roadmap and all six plans, here is the consistency report.

---

# Cross-Phase Consistency Report — datamancer standalone-server roadmap

## Executive summary

The six plans are individually strong and unusually checkpoint-disciplined: nearly every downstream assumption is tagged as a RE-PLAN CHECKPOINT, and the per-symbol-determinism constraint and the data-plane/diagnostics-plane split are respected consistently throughout (Phases 3–6 treat the Phase-3 `SystemSnapshot` as the single diagnostics artifact, read in-process by Phase 6 and over iceoryx2 by Phase 4/5 — no divergent shapes).

However, the checkpoints are mostly one-directional: downstream phases flag what they need, but the **depended-on phases (1 and 2) do not acknowledge or produce several of those needs**. The three highest-impact issues are all seam/ownership gaps at the Phase-1→2→4 boundary:

1. **The `EventSink` trait has no defined attachment point after Phase 2.** Phase 1 puts the sink at the authoritative controller; Phase 2 dismantles that (raw `mpsc` fan-out) and routes per-client output through the *concrete* internal `Sink` enum, not the `EventSink` trait. Phase 4/5 need to attach the iceoryx2 sink at the client output, but no phase generalizes the enum to `dyn EventSink`.
2. **`EventSink::publish` signature contradiction.** Phase 1 decides owned-return (`publish(MarketEvent) -> PublishOutcome`); Phases 2, 4, and 5 all write against the borrowed `publish(&MarketEvent) -> Result`. Phase 5's fallback pump (`sink.publish(&ev).await`) will not compile against Phase 1's decision.
3. **Phase 2 produces neither a client-session registry/id nor the per-symbol `LiveStats`** that Phase 3's snapshot assembly iterates, and Phase 2 removes `RegistrySentinel` (the structure Phase 3 plans to attach `LiveStats` to).

Plus naming drift (Phase 6 uses `IntrospectionSnapshot`/`introspect()` vs Phase 3's actual `SystemSnapshot`/`snapshot()`) and a snapshot-shape mismatch (resume buffer is per-client after Phase 2, but Phase 3 hangs `ResumeBufferSnapshot` off the authoritative snapshot).

None of these are fatal; all are resolvable by tightening Phase 1/2's produced surface and aligning Phase 3/6 to the now-concrete Phase 2/3 decisions.

## Cross-phase dependency & checkpoint table

| Edge | Downstream assumes | Upstream actually produces | Status |
|---|---|---|---|
| 1→2 (P1-A seq-at-source) | seq stamped at authoritative source, no re-stamp | Phase 1 does exactly this; removes `EventStream.seq` | ✅ consistent |
| 1→2 (P1-B EventSink shape + tee/ring core-side) | sink trait; tee+ring core-side | Phase 1: tee+ring core-side ✅; **but sink at authoritative level, Phase 2 relocates to client** | ⚠️ seam relocation unstated |
| 1→2/4/5 (publish signature) | `publish(&MarketEvent) -> Result` (borrowed) | Phase 1: `publish(MarketEvent) -> PublishOutcome` (owned) | ❌ contradiction |
| 1→2 (P1-C overflow→Gap) | per-client seq hole as `Control::Gap` | Phase 1 lands this invariant | ✅ consistent |
| 1→2 two-subscriber live test | deferred to Phase 2 | Phase 2 includes `two_clients_sharing..._identical_seq` | ✅ acknowledged both ways |
| 2→3 (P2-REG registry + client registry) | authoritative registry exposes `LiveStats`+refcount; **client-session registry + `ClientSessionId`** | Phase 2: `Weak<AuthoritativeSession>` registry, **no client registry, no `ClientSessionId`, no `LiveStats`**; removes `RegistrySentinel` | ❌ not produced (Phase 3 must add) |
| 2→3 (P2-RING ring granularity) | ResumeBufferSnapshot placement | Phase 2 decides **per-client** ring | ⚠️ Phase 3 struct still per-authoritative |
| 2→4 (P2-1 client type + sink wiring site) | `ClientSession` constructed-with-sink | Phase 2: `ClientSession` ✅ name; **only `take_events()->EventStream`, concrete `Sink` enum, no `dyn EventSink` seam** | ❌ wiring point missing |
| 2→4 (P2-3 control routing) | per-symbol vs connection-scoped split | Phase 2 Step 5 defines exactly this | ✅ consistent |
| 2→5 (P2-D anchor regardless of clients) | daemon holds its own referrer | Phase 2 P2-D explicitly composes (refcount = single source) | ✅ acknowledged both ways |
| 3→4/5/6 (snapshot type) | `SystemSnapshot` serde | Phase 3 produces `SystemSnapshot`, `snapshot()->Result<_>` async | ✅ (4,5) / ❌ name drift (6) |
| 3→4 (P3-1 format + max size) | bounded serialized payload for fixed shm | Phase 3 snapshot is **unbounded** (Vec catalog/sessions); format open | ⚠️ size bound unresolved |
| 3→5 (P3 accessor) | `snapshot()` sync | Phase 3: **async + fallible** | ⚠️ flagged by P3, minor |
| 4→5 (P4 sink/Node/feature) | per-client sink ctor, diagnostics publisher, Node ownership | Phase 4 designs all; Node ownership left open | ✅ checkpointed |
| 5→6 (B daemon host/shutdown) | shared runtime, `Arc<Datamancer>`, shutdown signal, config | Phase 5 produces these | ✅ consistent |
| 3→6 (A snapshot accessor) | cheap non-blocking `introspect()`/ArcSwap | Phase 3: I/O-bearing async `snapshot()` | ⚠️ name + cost; Phase 6 owns ArcSwap |

## Concrete issues to resolve

**Issue 1 — `EventSink` attachment point lost after Phase 2 (highest).**
Phase 1 wires `EventSink` (as `Sink::Attached(InProcessSink)`) at the *authoritative* controller. Phase 2 Step 1.2 replaces the authoritative sink with a raw `HashMap<SubscriberId, mpsc::Sender<MarketEvent>>` fan-out, and Step 3.2 routes per-client output through the *concrete* `Sink::Attached(InProcessSink)` enum. Phase 4 (P2-1) and Phase 5 (Step 4) need to attach `Iceoryx2DataSink` at the per-client output, but the enum holds a concrete `InProcessSink`, not `Box<dyn EventSink>`.
*Fix:* Add a Phase-2 step generalizing the per-client `Sink::Attached` to hold `Box<dyn EventSink>` (or make `ClientController` generic over `EventSink`), and add an explicit Phase-2 RE-PLAN CHECKPOINT acknowledging Phase 4/5's "construct client session with an `EventSink`" need. State in Phase 2 that the EventSink seam *migrates from authoritative (Phase 1) to per-client output (Phase 2)* — currently this migration is silent, and Phase 1's "every future sink inherits tee+ring" is written assuming the sink stays authoritative-side.

**Issue 2 — `publish` signature contradiction.**
Phase 1 deliberately chose owned-return `async fn publish(&self, MarketEvent) -> PublishOutcome` (no `Result`). Phases 2 (P1-B), 4 (P1-1), and 5 (Step 4 pump: `sink.publish(&ev).await?`) assume borrowed `publish(&MarketEvent) -> Result`. Phase 4 explicitly needs to *serialize from a borrow*.
*Fix:* Adopt Phase 1's resolution everywhere: keep owned-return as the in-process path and add the `publish_borrowed(&self, &MarketEvent)` method Phase 1 already anticipated; point Phase 4 at `publish_borrowed`. Update Phase 5's fallback pump to the chosen signature and replace its `?` error handling with `PublishOutcome` matching. This is the explicit subject of Phase 1's "sink-signature sign-off before Phase 4" checkpoint — close it by editing the downstream plans, which currently still reference the rejected borrowed-only shape.

**Issue 3 — Phase 2 does not produce the client-session registry, `ClientSessionId`, or `LiveStats` that Phase 3 assembles.**
Phase 3 Slice C step 4 "iterate the client-session registry" and emits `ClientSessionSnapshot { id: ClientSessionId, ... }`; Phase 3 also plans to attach `Arc<LiveStats>` to `RegistrySentinel`. But Phase 2 (a) creates `ClientSession` handles with no registry in `DatamancerInner` and no id concept (`SubscriberId` is per-authoritative-subscriber, not a client id), and (b) **removes `RegistrySentinel`**, folding it into `AuthoritativeSession`.
*Fix:* Either (preferred) add to Phase 2: a client-session registry in `DatamancerInner`, a `ClientSessionId` (defined in `datamancer-core` per Phase 3's P2-REG layering sub-checkpoint, resolving Phase 3 OQ6), and a per-authoritative `LiveStats` handle on `AuthoritativeSession`; or explicitly assign that work to Phase 3 and correct Phase 3's "attach to `RegistrySentinel`" instruction to target `AuthoritativeSession` (the `RegistrySentinel`-attach narrative is dead once Phase 2 lands first, which the dependency order requires).

**Issue 4 — Resume-buffer snapshot placement mismatch.**
Phase 2 decides the resume buffer is **per-client** (Step 4). Phase 3 puts `resume_buffer: ResumeBufferSnapshot` on `AuthoritativeSessionSnapshot` and leaves `ClientSessionSnapshot` with only `{id, subscriptions}`.
*Fix:* Move `ResumeBufferSnapshot` (and per-instrument `gap_count`/`dropped_events`) onto `ClientSessionSnapshot`. This is exactly what Phase 3's P2-RING checkpoint anticipated; resolve it now that Phase 2 has decided.

**Issue 5 — Snapshot naming drift in Phase 6.**
Phase 6 references `IntrospectionSnapshot` and `Datamancer::introspect()`; Phase 3 actually produces `SystemSnapshot` and `Datamancer::snapshot() -> Result<SystemSnapshot>` (async). Phase 3 is high-fidelity/concrete, so Phase 6's names are simply wrong, not merely TBD.
*Fix:* Rename throughout Phase 6 to `SystemSnapshot`/`snapshot()`; note it is async+fallible (affects Phase 6's ArcSwap-refresh task, which already plans off-thread acquisition, so the fix is mechanical). CHECKPOINT A can then be narrowed to the unit-identity-key question.

**Issue 6 — Snapshot is unbounded but the diagnostics plane needs a bounded payload.**
Phase 3's `SystemSnapshot` carries `Vec<CacheCatalogEntry>` (one per cached key) and `Vec`s of sessions — potentially large. Phase 4's diagnostics service uses a fixed-capacity iceoryx2 payload (P3-1) and flags chunking only as an open question. A large cache catalog can exceed any fixed cap.
*Fix:* Decide ownership of bounding. Recommended: Phase 3 documents a size expectation / offers a "live-state-only" snapshot variant separate from the heavier cache catalog (Phase 5 already wants to split cache-catalog onto a slower cadence for I/O reasons — reuse that split for size). Phase 4 then sizes the fixed payload to the live-state portion and publishes the catalog on a separate, chunked or larger-cap service. Surface this as a coordinated Phase-3/Phase-4 decision rather than two independent open questions.

**Issue 7 — Phase 5 lifecycle-anchor-with-backfill vs Phase 2 single-scope sharing (minor).**
Phase 5 opens startup anchors via the direct `Session` path with `Scope::Live{backfill_from: Some}` and expects clients to *share* that authoritative session; Phase 2 (P2-F) states a shared authoritative session has one scope and rejects differing backfill, and restricts `ClientSession::subscribe` to pure-live. An anchor created with backfill plus a later live client referrer is precisely the mismatched-scope case Phase 2 warns about.
*Fix:* Phase 2 should specify that backfill is a creation-time property of the authoritative session and later referrers (including a live `ClientSession::subscribe` and the daemon anchor) attach to the existing scope without re-specifying it; add this to Phase 2's P2-F treatment and Phase 5's anchor description so the compose-via-refcount story (P2-D) is consistent for the backfill case.

**Issue 8 — Synthetic-control `seq` sentinel coordination (minor).**
Phase 2 invents a sentinel `seq` (`Seq::SYNTHETIC`/`u64::MAX`) for client-local synthetic controls. Phase 1 owns the `Seq` doc rewrite and Phase 3 adds serde to `Seq`; neither mentions the sentinel.
*Fix:* Coordinate the sentinel value/constant into Phase 1's `event.rs` `Seq` doc edit (so the three `event.rs` edits across Phases 1–3 don't collide) and confirm Phase 4's POD payload (which carries `seq: u64`) and Phase 6's "per-symbol seq" UI rule tolerate the sentinel without folding it into monotonicity checks. (Phase 2 already lists this as its OQ6; just bind it to the Phase-1 edit.)

## Items that are consistent (no action)

- Per-symbol determinism / `(instrument, seq)` interleave (not merge-sort): respected in all six plans, including Phase 4 POD payload, Phase 6 UI rules, and Phase 5 one-sink-per-client.
- `ClientSession` as the public handle, `Session` retained: Phase 2 decides; Phases 3–6 align.
- `SymbolId` sink-local, per-service, `CONNECTION` sentinel: Phase 4 only; no leakage into core or other phases.
- Data-plane vs diagnostics-plane split: consistent across Phases 3 (content), 4 (two services), 5 (per-client data + one process-wide diagnostics), 6 (in-process snapshot reader).
- `#![forbid(unsafe_code)]` handling: Phase 4 quarantines iceoryx2 in a new crate; Phases 1–3, 5, 6 keep the forbid.
- Phase 2↔Phase 1 two-subscriber agreement test handoff, and Phase 2↔Phase 5 anchor-composes-via-refcount (P2-D): acknowledged in both directions.

## Resolution status (reconciliation pass, 2026-06-28)

All eight issues resolved via an authoritative **Reconciliation pass** block at the
top of each affected plan (those blocks supersede conflicting body text). Two
ownership questions were decided by the architect:

- **Issue 3 → Phase 2** builds the client-session registry, `ClientSessionId`
  (in `datamancer-core`), and per-symbol `LiveStats` on `AuthoritativeSession`;
  Phase 3 only reads them.
- **Issue 6 → split snapshot:** a bounded live-state snapshot on the fast
  diagnostics service, the heavier cache catalog on a separate slower/chunked
  service.

| Issue | Resolution | Lands in |
|---|---|---|
| 1 EventSink attachment | seam migrates to per-client `Box<dyn EventSink>` output | Phase 2 (noted in 1) |
| 2 publish signature | owned `publish(MarketEvent)->PublishOutcome` + `publish_borrowed(&MarketEvent)` | Phase 1 (used by 2,4,5) |
| 3 registry/id/LiveStats | built in Phase 2; read in Phase 3 | Phase 2 |
| 4 resume snapshot placement | on `ClientSessionSnapshot` | Phase 3 |
| 5 Phase 6 naming | `SystemSnapshot`/`snapshot()` | Phase 6 |
| 6 snapshot bounding | split live-state vs cache catalog | Phases 3 + 4 |
| 7 backfill scope | creation-time; referrers attach to existing scope | Phases 2 + 5 |
| 8 seq sentinel | `Seq::SYNTHETIC` defined in Phase 1's `event.rs` edit | Phase 1 (used by 2,4,6) |

## Final consistency sweep (post-hardening, 2026-06-28)

After all six phases received their "Detailed-planning hardening" blocks, a second
sweep re-checked the cross-phase couplings the hardening touched. **12 couplings
verified consistent** (publish/`publish_borrowed` signatures; EventSink seam
migration P1→P2→P4; `Seq::SYNTHETIC = Seq(u64::MAX)`; tap-log seq convergence;
`ClientSessionId` in core; `LiveStats` build-vs-read; resume-buffer granularity;
connection-control divergence; backpressure chain P4→P2; one-Node-per-process;
the split-snapshot delivery chain P3→P4→P5→P6 incl. the in-process web-UI path;
naming/types). **4 residual issues found and fixed:**

1. *(major)* Phase 5 fallback-pump snippet used borrowed-`Result` `sink.publish(&ev)` — corrected to owned `publish(ev) -> PublishOutcome` matching (Phase 1's locked signature).
2. *(major)* Phase 3 body struct still hung `resume_buffer` on `AuthoritativeSessionSnapshot` — moved to `ClientSessionSnapshot` (per-client, per the reconciliation); authoritative `gap_count` clarified as per-symbol provider/source gaps.
3. *(minor)* Phase 2 now notes that a transport sink may handle connection-scoped controls differently (the Phase-4 diagnostics-only divergence).
4. *(minor)* Phase 5 now states the control-protocol client-name ↔ ephemeral `ClientSessionId` mapping.

No rearchitecture required. The hardened plan set is internally consistent.
