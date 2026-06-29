# CodeRabbit PR #8 — verified triage

Review date: 2026-06-29. Branch: `feat/server-implementation`. 42 findings
(33 Major, 9 Minor/Trivial; no Critical). Each was independently verified
against the **current** code by a dedicated reviewer, not taken on faith.

**Outcome:** 37 VALID, 2 PARTIAL, 3 REJECT.

Effort key: **S** ≈ local edit, **M** ≈ multi-site / signature change, **L** ≈
new API or control-flow rework.

---

## REJECT — do not apply (3)

| ID | File | Why rejected |
| --- | --- | --- |
| F05 | datamancer/README.md:237 | Says "move `SessionClosing` to the diagnostics plane." The README is **correct**: `payload.rs:218-227` deliberately routes `SessionClosing` on the **data plane** to `SymbolId::CONNECTION`; only `ProviderConnected/Disconnected/ProviderError` are suppressed. Applying the fix would contradict the locked design. |
| F07 | phase-4 plan | Misattributed. `grep` shows **no** `IntrospectionSnapshot`/`introspect()` in the phase-4 plan; the real offender is the phase-6 plan → covered by **F13**. |
| F08 | phase-4 plan | Misattributed. **No** "SPA" text in the phase-4 plan; the real offender is the phase-6 plan → covered by **F11**. |

---

## PARTIAL — valid but narrower than stated (2)

| ID | File | Note | Effort |
| --- | --- | --- | --- |
| F06 | datamancer/README.md:44 | "reproduces the consumer's original experience exactly" reads as the full multiplex; the bullet is already per-symbol-framed. Reword to "reproduces that symbol's substream exactly." | S |
| F11 | phase-6 plan:271-278,402 | Locked decision (HTMX, no SPA) is right; stale SPA *recommendation* text still lives in the body. Reclassify as rejected alternative. | S |

---

## VALID (37), grouped into fix themes

### Theme 1 — `seq` sentinel (root cause spans plan + type-doc + code)
The prior `add28f6` rationale is wrong: `saturating_add` caps **at** `u64::MAX`,
which **is** `Seq::SYNTHETIC`. One code fix (clamp counter to `u64::MAX - 1` or
hard-error at the boundary) + doc corrections resolves all three.
- **F36** `session.rs:1777` — `stamp()` can emit the reserved synthetic seq. **S**
- **F35** `event.rs:40` — doc claims `u64::MAX` unreachable; stamping path can reach it. **S**
- **F14** phase-2 plan:32 — "saturate so it can never wrap into the sentinel" is logically wrong. **S** (doc)

### Theme 2 — session lifecycle (datamancer)
- **F01** `session.rs:1668` — orphaned backfills keep fetching after the last referrer leaves. **L**
- **F18** `session.rs:314` — `scope()/persistence()` read per-handle caches, not shared authoritative state. **M**
- **F19** `session.rs:557` — `subscriber_refcount` uses `strong_count()` (counts `Arc`s, ≈2× per referrer). **M**

### Theme 3 — accounting & fan-out (datamancer)
- **F02** `accounting.rs:110` — provider reconnect/disconnect state derived from one shared `seen_connect` flag; cross-symbol contamination. **M**
- **F03** `client.rs:454` — `TrySendError::Full` drops a stamped event with no per-instrument `Gap`. **M**

### Theme 4 — storage (datamancer)
- **F15** `surreal.rs:519` — `catalog().event_count` drifts upward on re-store (delete not subtracted). **M**
- **F16** `surreal_tap_log.rs:386` — backfill live arrivals tee'd to the tap log **before** stamping → persisted with `seq=0`. **M**
- **F17** `surreal_tap_log.rs:386` — source `seq` not unique across shard lifetimes (`next_seq` resets to 0 per controller) → replay order ambiguous. **M**

### Theme 5 — iceoryx2 transport
- **F26** `subscriber.rs:152` — malformed announcements silently discarded (`if let Ok`). **S**
- **F29** `sink.rs:124` — symbol marked announced **before** the send succeeds. **S**
- **F27** `payload.rs:201` — unknown `MarketEvent` variants acked as `Delivered`. **M**
- **F28** `sink.rs:75` — single-shot announcements strand late joiners once history (512) rolls over. **M**

### Theme 6 — daemon lifecycle (datamancerd)
- **F21** `server.rs:563` — EOF teardown armed before `OpenClient` succeeds → a duplicate-name open tears down the live client. **S**
- **F24** `server.rs:332` — unconditional `remove_file` on the admin socket (no socket-type/liveness check). **S**
- **F25** `config.rs:453` — `backfill_from` parser accepts impossible times (`02-31`, `99:00`). **S**
- **F20** `server.rs:281` — web `bind()` runs inside the spawned task; startup failures detached from bootstrap. **M**
- **F22** `server.rs:221` — shutdown stops consuming `cmd_rx` before stopping its producers → a request in that window hangs forever. **M**
- **F23** `shutdown.rs:80` — flush-then-close drops terminal close events (`SessionClosing`) emitted by `close()`. **M**
- **F42** `control.rs:85` — control requests accept unknown JSON keys (no `deny_unknown_fields`; `flatten` complicates the `Subscribe` arm). **M**

### Theme 7 — web / metrics (datamancerd)
- **F30** `web/metrics.rs:72` — session labels use bare `symbol()`, collapsing two providers' `AAPL` into one series. **S**
- **F32** `web/mod.rs:101` — loopback-only bind is doc-only; `serve` doesn't reject a non-loopback `addr`. **S**
- **F38** `web/metrics.rs:35` — `install()` not idempotent under concurrent calls. **S**
- **F39** `web/ui.rs:105` — session tables render bare `symbol`, losing provider identity (data is already present). **S**
- **F37** `web/mod.rs:278` — graceful-shutdown test connects+drops a socket; never issues a real in-flight request. **M**
- **F31** `web/refresh.rs:86` — the fast live loop calls full `dm.snapshot()`, so the slow catalog walk still stalls the live/SSE path. **L** (needs a live-only snapshot API on `datamancer`)

### Theme 8 — docs & plans
- **F34** `CLAUDE.md:16` — "all three crates" should be "all four." **S**
- **F40** `examples/client_session.rs:28` — use the `datamancer::providers::AccountType` re-export. **S**
- **F41** `traits/provider.rs:84` — doc must require `metrics()` to return a stable shared sink, not a fresh one per call. **S**
- **F04** `datamancerd/README.md:184` — shutdown order lists tap-log flush **last**; contract is tap-log flush **first**. **S**
- **F10** phase-5 plan:26 — control `subscribe` described as "preferences, return actual scope"; should mirror Phase 2's reject-on-backfill (matches shipped `client.rs`). **S**
- **F09** phase-1 plan:263 — stale "tap-log seq convergence deferred" passages contradict the shipped converged contract. **M**
- **F12** phase-3 plan:29 — `seq_position` has three incompatible definitions; pin one (`Controller.next_seq`). **M**
- **F13** phase-6 plan:100 — rename remaining `IntrospectionSnapshot`/`introspect()` to `SystemSnapshot`/`snapshot()`. **M**

### Theme 9 — test determinism
- **F33** `tests/introspection.rs:274` — coalesced-fetch assertion is scheduler-dependent; needs a barrier or a relaxed bound. **M**

---

## Suggested sequencing

Docs-only themes (8) and the sentinel doc bits are zero-risk and can land first.
Code themes ordered by blast radius: 1 (sentinel) → 4 (storage, shares the
`next_seq` root cause) → 3 → 2 → 5 → 6 → 7 → 9. F31 (L) may fold into the
already-tracked "live-only snapshot split" deferral rather than this pass.
