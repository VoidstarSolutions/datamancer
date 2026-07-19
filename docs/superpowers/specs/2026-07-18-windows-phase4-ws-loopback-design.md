# Native Windows Phase 4 — WS-loopback data transport (Design)

> Elaborates **Phase 4** of `docs/superpowers/specs/2026-07-15-native-windows-support-design.md` (§5). Read that spec first — this doc assumes its §2.5 transport decision and its guiding constraints.
>
> **Status: decisions locked (2026-07-19).** Architecture: **hybrid** — Windows `AppHandle` keeps admin ops (credentials/config/shutdown/ping/health) on the Phase-3 owner-DACL named pipe; WS carries only the data plane + a new health-push stream. Health transport: **WS server-push** (add a health op to the WS surface following #40's `capabilities` op-add template). The `feat/instrument-capabilities` (#40) and stock-quotes (#41) work has merged and this branch has rebased onto the resulting `0.7.0` main, so the WS-vocabulary structures Phase 4 extends are the current in-branch code. See §7 for the locked rationale.

## 1. Goal

Give the Windows **same-host** consumer path a working data/diagnostics/health transport, since iceoryx2's shared-memory transport is not viable on Windows (spike settled in parent spec §2.5: iceoryx2 0.9.2 compiles but cannot create a node at runtime — its PAL reads `/etc/passwd`). Per §2.5 the decision is **option C1: reuse the existing WS transport over loopback**, `#[cfg(windows)]`-selected. iceoryx2 stays the Linux/macOS zero-copy transport, **byte-for-byte unchanged**. Swapping the transport on Windows loses only *zero-copy* (a performance property), never a capability.

C2 (a Windows-native shared-memory transport) is an explicit **non-goal** for this phase — a later optimization only if Windows perf demands zero-copy.

## 2. Current state (what already exists — reuse, do not rebuild)

- **WS transport crate** `datamancer-transport-ws` — wire format (`EventFrame`, `WS_SUBPROTOCOL = "datamancer.v2"`), `WsDataSink`, `run_writer`. Client-side only; `#![forbid(unsafe_code)]`; Windows CI already builds it. Instrument is carried **inline** on every frame (no `SymbolId` interning, unlike iceoryx2).
- **Daemon WS surface** `datamancerd/src/ws/` (feature `ws`, off by default, **no `cfg(windows)` gate**): `serve` binds a `TcpListener` (default **loopback** `127.0.0.1:9001`), per-connection handshake with optional bearer token + subprotocol enforcement, bridges inbound control frames to `dm.client_session()` and pumps its `EventStream` through `WsDataSink`. `dispatch` handles `Subscribe`/`Unsubscribe`/`Snapshot`/`CloseClient`/`Instruments` today.
- **`WsClient`** `datamancer-client/src/ws.rs` — full `Client` impl over one TCP/WS socket, correlated replies + inline event frames, reader task demuxes by `id`. Portable, unit-tested.
- **Windows control plane (Phase 3, already on this branch):** named-pipe `ControlConn` (cfg-split in `iceoryx2.rs`), `app/platform_windows.rs` (named-pipe `ControlEndpoint` + detached `DaemonSpawner`), `win_control.rs`, `datamancer-winsec`, `win_pipe.rs`. Owner-DACL same-user auth.

## 3. The gap

The WS surface today is a **data + pull-snapshot + instruments** surface. Two things are missing for a Windows same-host `AppHandle`:

1. **No health-push carrier over WS.** The daemon's health push plane is an iceoryx2-only service (`datamancer/health`), consumed by `AppHandle::watch_health()` via `NodeBuilder`/`Iceoryx2HealthSubscriber`. There is no `WsRequest::Health`/pushed health frame. **On Windows `watch_health()`'s `NodeBuilder::create()` fails at runtime and the stream silently ends.** This is the single biggest data-plane gap.
2. **`AppHandle` is hard-wired to `Iceoryx2Client`.** Its inner `client: Iceoryx2Client`, the `AppEvents` type alias, and every admin method's `ClientError<Iceoryx2ClientError>` error type bind the app facade to iceoryx2 (whose shm data plane fails on Windows). The *control plane* is already seamed and Windows-ported (the `platform`/`platform_windows` swap); only the *connected data client* is iceoryx2-bound.

Diagnostics: the WS `Snapshot` op already covers the diagnostics *pull* capability; there is no pushed diagnostics stream over WS, and none is required (the pull covers it).

## 4. Recommended architecture — **hybrid** (open decision, see §7)

On Windows, `AppHandle` composes **two purpose-built connections**:

- **Admin ops over the Phase-3 named-pipe control connection** — `ping`/`health`/`set|get|clear-credentials`/`get-config`/`configure|remove-provider`/`shutdown`. The named pipe enforces **owner-DACL same-user** auth (Phase 3), so credential/config/shutdown ops keep their same-user guarantee.
- **Data plane over `WsClient`** (loopback WS) — `subscribe`/`unsubscribe`/`snapshot`/`instruments`/`capabilities`/`close` + the event stream.
- **`watch_health()`** consumes a **new WS health-push stream** (§5 item 1).

Unix/macOS: unchanged — one `Iceoryx2Client` carrying both control (pipe/UDS) and shm data.

**Why hybrid, not WS-only:** putting credential/config/shutdown ops on a loopback **TCP** socket (bearer-token only) would drop the named pipe's same-user guarantee — a security regression. Zach's own `feat/instrument-capabilities` keeps WS a **data/catalog surface** (it adds `capabilities` to WS alongside `instruments`, but adds **no** admin ops), so hybrid follows the established direction. Cost: a Windows `AppHandle` holds two connections.

## 5. Worklist (each item `#[cfg(windows)]`-additive; unix/iceoryx2 untouched; `forbid(unsafe)` holds)

The `WatchHealth` **subscribe** follows Zach's `capabilities` op-add template (`WsRequest` variant → `id()` arm → `WsReply::ok(id)` ack → dispatch → round-trip tests). The **pushed** `HealthView` diverges from that request/reply template — it is server-initiated, so it rides a **dedicated pushed frame** on a **separate client channel**, not the reply-by-`id` demux (§7 decision 4).

| # | Item | Touches (anchors on the rebased branch) |
|---|---|---|
| 1 | **Standalone pipe control client** — factor the named-pipe `Request`/`Reply` control logic (today buried in `Iceoryx2Client`'s cfg-split `ControlConn` — `iceoryx2.rs:104-152` — whose `connect` also does the iceoryx2 shm attach that fails on Windows) into a small reusable `PipeControlClient` for admin ops (`ping`/`health`/credentials/config/`shutdown`), built on `win_pipe::connect_verified`. Unix untouched. | `datamancer-client/src/` (new module) |
| 2 | **Health-push over WS** — `WsRequest::WatchHealth { id }` (after `ws.rs:48`) + `id()` arm; a **distinct** `WsHealthPush { view: HealthView }` message in `protocol/ws.rs` added to `WsClient`'s `Inbound` demux — **NOT an `EventFrame`** (respects transport-ws's "go through `to_wire`/`from_wire`" invariant) — routed to a separate health channel; daemon `dispatch` arm (`ws/conn.rs:316`) spawns a push task (mirrors `spawn_pump:325`) serializing the stamped view on `self.diag_interval`, reusing `server.rs:stamped_health_view:53`. **`transport-ws` untouched.** | `protocol/ws.rs`, `client/ws.rs`, `datamancerd/ws/conn.rs` (+ `server.rs` plumbing) |
| 3 | **Windows `AppHandle` (hybrid)** — `cfg`-split so Windows holds **two** channels: `PipeControlClient` (admin) + `WsClient` (data). `cfg`-select `AppEvents` (`mod.rs:53`), the `client` field (`:116`), the connect in `ensure` (`:147-153`), and the admin/data-method error types | `datamancer-client/src/app/mod.rs` |
| 4 | **`watch_health()` Windows arm** — replace the iceoryx2 `NodeBuilder`/`Iceoryx2HealthSubscriber` body (`mod.rs:401-428`) with a `#[cfg(windows)]` arm consuming item 2's WS health channel; unix arm unchanged | `datamancer-client/src/app/mod.rs` |
| 5 | **Spawn/config wiring** — Windows daemon spawned `--features ws` with scaffolded `[ws].enabled=true` on loopback (`config.rs:363-385`, default off); `EnsureConfig` gains a WS data endpoint (`mod.rs:70-91`) the Windows `ensure` branch feeds to `WsClient`; thread `credential_backend` into the WS layer for `stamped_health_view` | `client/app/*`, `datamancerd` config/spawn |
| 6 | **CI (⚠ pending Zach's blessing — §7 decision 5)** — a native-Windows job that spawns the daemon `--features ws` + runs a WS-loopback `app` smoke test (sidesteps the iceoryx2 blocker; **reverses** the `ci.yml:84-87` support boundary) | `.github/workflows/ci.yml` |

**Dependency order:** 1 (pipe control client) and 2 (health-over-WS) are independent and can land first/in parallel; 3 depends on 1+2; 4 depends on 2+3; 5 alongside 3; 6 last (and gated).

## 6. Alignment with Zach's conventions

- **Concrete `cfg`-split, not a trait abstraction.** `instrument-capabilities` keeps two compile-time `Client` impls with no `dyn`, no unified seam. The "unified client-transport trait" is explicitly *deferred* (parent spec §4.1; `transport-ws/CLAUDE.md`: extract it "from the intersection of these two, not designed in the abstract"). Mirror the existing splits — `spawn_control` (two functions), `ControlConn` (two struct defs), `platform`/`platform_windows`.
- **Never widen a shared state machine to special-case Windows** (`app/lifecycle.rs` stays transport-neutral).
- **macOS/Linux build byte-for-byte unaffected** — the parent spec's litmus test.
- **Vocabulary is the operator contract** — additions to `WsRequest`/`WsReply` and `ClientError` are breaking-change-reviewed; prefer additive request variants and `cfg`-split type aliases over changing existing signatures.
- **`forbid(unsafe)`** holds in `datamancer-transport-ws` and `datamancer-client` — WS is pure-safe; Phase 4 introduces no `unsafe` (no EXT-1 here).

## 7. Decisions (all locked 2026-07-19)

1. **Hybrid vs WS-only `AppHandle` on Windows** (§4) — **LOCKED: hybrid** (2026-07-19). Admin ops stay on the owner-DACL pipe (same-user secure); WS carries data + health only. WS-only was rejected: it would put credential/config/shutdown ops on a bearer-token TCP loopback, dropping the pipe's same-user guarantee, and diverges from Zach's WS-as-data-surface pattern.
2. **Health transport mechanism** — **LOCKED: WS server-push** (2026-07-19). Add a health op to the WS surface following #40's `capabilities` op-add template; the daemon pushes the stamped `HealthView` on its diagnostics cadence, matching `watch_health()`'s existing push semantics. Polling the pipe's `Request::Health` was rejected (client-driven cadence changes the semantics, adds control-connection traffic).
3. **Admin ops on Windows need a control client that doesn't exist yet** — **LOCKED: extract a standalone `PipeControlClient`** (2026-07-19). `Iceoryx2Client::connect` fails on Windows (iceoryx2 shm attach), so it can't back the hybrid's admin plane; the pipe `Request`/`Reply` logic buried in its `ControlConn` (`iceoryx2.rs:104-152`) is factored into a small reusable client on `win_pipe::connect_verified`. Rejected: bending `Iceoryx2Client` into a control-only shape (muddies the data-plane crate's contract). This is **worklist item 1** and is real added scope beyond the initial estimate.
4. **Pushed-health wire shape** — **LOCKED: a dedicated push message in the client protocol vocab (`protocol/ws.rs`), added to `WsClient`'s inbound demux; `transport-ws` untouched** (revised 2026-07-19 after reading `transport-ws/CLAUDE.md`). `WatchHealth` is a request/reply *subscribe* (follows #40's template), but the ongoing `HealthView` pushes are server-initiated with no client `id` to echo, so they cannot use the reply-by-`id` oneshot demux. **The frame is NOT an `EventFrame`** — `transport-ws/CLAUDE.md` mandates all `EventFrame`s go through `to_wire`/`from_wire`, and a health push is not a `MarketEvent`, so it cannot. Instead a distinct message (e.g. `WsHealthPush { view: HealthView }`) is added to the client's `Inbound` untagged union, with a JSON shape disjoint from `EventFrame` (`"type"` tag) and `WsReply` (`"id"`+`"ok"`) so demux stays unambiguous; `run_reader` routes it to a dedicated health channel surfaced as `HealthStream`. The daemon serializes this message onto the same single-writer channel. `EventFrame`/`to_wire`/`from_wire` and the whole `transport-ws` crate stay byte-for-byte unchanged. Rejected: `EventFrame::Health` (violates the transport-ws go-through-`to_wire` invariant); overloading `WsReply.health` with a reserved sticky `id` (fights the pending-oneshot demux).
5. **CI support-boundary reversal** — **PENDING ZACH'S BLESSING** (2026-07-19). `ci.yml:84-87` documents (per the open-sourcing spec) that Windows gets only the ws-portable subset, not the daemon/`app`. Worklist item 6 reverses that. Per Chris's call, the Phase 4 *code* (items 1-5) proceeds, but the CI/policy edit (item 6) is **not merged without Zach's sign-off** as the open-sourcing-spec owner. Flagged to Zach 2026-07-19.

## 8. Dependency on `feat/instrument-capabilities` (coordination)

**Resolved (2026-07-19): #40 merged, this branch rebased onto it.** `feat/instrument-capabilities` (#40, `0.7.0`) added `WsRequest::Capabilities`, a `WsReply.capabilities` field (threaded `None` through **every** constructor), `Client::capabilities()`, both client impls, and the `ws/conn.rs` dispatch arm. Phase 4 now follows that **exact** op-add template for its health op, so it lands *additively* beside `capabilities` (one more variant / field / `None` / dispatch arm / test) rather than colliding. The detailed task-plan is authored against this rebased `0.7.0` code. Note: #42 (`feat!: split supports into Live/History`) is approved but not merged; it touches none of the WS-vocabulary or `server.rs` files, so it will not affect Phase 4 — when it merges, our catch-up is a mechanical `0.8.0` version bump (plus the hand-maintained winsec dep literal).

## 9. Non-goals

- C2 Windows-native shared-memory transport (later, only if perf demands zero-copy).
- A unified/`dyn` client-transport trait (deferred; extract from the two concrete impls later).
- Admin/credential ops on the WS surface (they stay on the same-user named pipe).
- Any change to the Linux/macOS iceoryx2 path.

## 10. Next step

Once (1) the §7 decisions are confirmed and (2) `feat/instrument-capabilities` has merged to main and this branch has rebased onto it, author the detailed TDD task-plan under `docs/superpowers/plans/` in the established format (see `2026-07-18-live-latest-seed.md`) against the real code, then implement task-by-task.
