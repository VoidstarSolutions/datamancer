# Native Windows Phase 4 — WS-loopback data transport — Implementation Plan

> **For agentic workers:** implement task-by-task, TDD (failing test first). Steps use checkbox (`- [ ]`) syntax. Design + locked decisions: `docs/superpowers/specs/2026-07-18-windows-phase4-ws-loopback-design.md`. Parent spec: `docs/superpowers/specs/2026-07-15-native-windows-support-design.md` §5.

**Goal:** Give the Windows same-host consumer path a working data/diagnostics/health transport over **WS-loopback** (iceoryx2 shm is not viable on Windows). **Hybrid**: Windows `AppHandle` uses a named-pipe `PipeControlClient` for admin ops + `WsClient` for the data plane + a new WS health-push stream. Linux/macOS (iceoryx2) is **byte-for-byte unchanged**.

## Global constraints

- Every change is `#[cfg(windows)]`-additive; the macOS/Linux build is byte-for-byte unaffected (parent-spec litmus test). iceoryx2 paths untouched.
- `#![forbid(unsafe_code)]` holds in `datamancer-transport-ws` and `datamancer-client` — WS is pure-safe; Phase 4 introduces no `unsafe`.
- Toolchain pinned **1.96.1**; Windows builds need `LIBCLANG_PATH=C:/Program Files/LLVM/bin` (iceoryx2 bindgen). Gate `cargo` per crate on **Windows** — `cfg(windows)` code is invisible to the Linux jobs (this has bitten Phase 3 repeatedly).
- **Versioning (post-#43):** feature PRs **never** touch `[workspace.package] version` — release-plz owns it. A breaking public-API addition (e.g. a new `WsRequest` variant, which cargo-semver-checks flags as `enum_variant_added`) must be **declared** with a Conventional-Commits marker — a `type(scope)!:` subject (e.g. `feat(windows)!:`) or a `BREAKING CHANGE:` footer — so the new `semver-checks.sh` marker gate passes and release-plz sizes the bump. The `[target.'cfg(windows)']` winsec dep version *requirement* still needs hand-alignment to the current workspace version on each bump (a recurring artifact flagged for Zach).
- Follow Zach's op-add template (`WsRequest::Capabilities` in `protocol/ws.rs`) for the request/reply parts; the **pushed** health frame diverges (§ design decision 4).
- Per-crate gates after each task: `cargo clippy -p <crate> [--features …] --all-targets -- -D warnings`, `cargo test -p <crate> …`, `cargo fmt --check`.
- **CI item (worklist 6) is gated on Zach's blessing** — do not merge the `ci.yml` boundary change without it.

---

## Task 1 — Standalone `PipeControlClient` (Windows admin plane)

**Rationale:** `Iceoryx2Client::connect` fails on Windows (iceoryx2 shm attach), so it can't back the hybrid's admin ops. Factor the named-pipe `Request`/`Reply` control logic (buried in `iceoryx2.rs`'s cfg-split `ControlConn`, lines 104-152) into a small reusable client on `win_pipe::connect_verified`.

**Files:**
- New: `crates/datamancer-client/src/pipe_control.rs` (`#[cfg(all(windows, feature = "iceoryx2"))]`)
- Modify: `crates/datamancer-client/src/lib.rs` (module decl, mirroring the `win_pipe` gate at ~line 28)

**Interfaces produced:**
- `pub(crate) struct PipeControlClient` wrapping the connected pipe halves (`ReadHalf/WriteHalf<NamedPipeClient>` + `Lines` reader), mirroring `ControlConn`'s Windows arm.
- `PipeControlClient::connect(path: &Path) -> io::Result<Self>` — `win_pipe::connect_verified(path)` then split (see `iceoryx2.rs:127-141`).
- `async fn request(&mut self, req: &protocol::uds::Request) -> Result<protocol::uds::Reply, ClientError<…>>` — serialize + `\n`, read one reply line, deserialize (mirrors `iceoryx2.rs::ControlConn::request` at ~143-152).

- [ ] **Step 1 — failing test.** In `pipe_control.rs` `#[cfg(test)] mod tests`, stand up a `tokio` named-pipe **server** (via `tokio::net::windows::named_pipe::ServerOptions`), spawn a task that reads one line and replies `{"ok":true}\n`, then assert `PipeControlClient::connect(name).request(&Request::Ping)` returns a reply with `ok == true`. (Same-process, so `connect_verified`'s owner-SID self-check passes — the pipe owner is this token. On an elevated CI runner the default tokio pipe owner is Administrators, which would fail the owner check; create the pipe with an owner-stamped SD or gate the test `#[ignore]` with a note — see Phase 3 `win_control` test `owner_dacl_pipe_round_trips_same_user` for the stamping pattern.)
- [ ] **Step 2 — run, expect fail** (`cargo test -p datamancer-client --features app pipe_control` on Windows) — `PipeControlClient` unresolved.
- [ ] **Step 3 — implement** `PipeControlClient` by lifting the `#[cfg(windows)]` `ControlConn` shape from `iceoryx2.rs:110-152` into the new module (fields, `connect`, `request`). Do **not** remove `ControlConn` from `iceoryx2.rs` yet (unix + the iceoryx2 client still use it) — this is an additive extraction. Keep it `pub(crate)`.
- [ ] **Step 4 — run, expect pass.**
- [ ] **Step 5 — clippy + fmt** (`-p datamancer-client --features app`, on Windows).
- [ ] **Step 6 — commit** `feat(windows): standalone PipeControlClient for the hybrid admin plane`.

---

## Task 2 — Health push over WS (`WatchHealth` op + pushed `Health` frame)

**Rationale:** `watch_health()` reads the iceoryx2 health plane, which fails on Windows. Add a WS subscribe op + a server-pushed health frame on a dedicated client channel (design decision 4).

### 2a — WS control vocabulary (`protocol/ws.rs`)
**Interfaces:** `WsRequest::WatchHealth { id: u64 }`; no new `WsReply` payload field is required (the ack is `WsReply::ok(id)`; the pushed health rides the frame in 2b).

- [ ] **Step 1 — failing test.** Add `ws_watch_health_parses_and_carries_id` (mirror `ws_capabilities_parses_and_carries_id` at `ws.rs:218-230`): parse `{"id":7,"op":"watch-health"}` → `WsRequest::WatchHealth { id: 7 }`, assert `req.id() == 7`.
- [ ] **Step 2 — run, expect fail.**
- [ ] **Step 3 — implement:** add the `WatchHealth { id }` variant after `ws.rs:48` (shape like `Snapshot`/`CloseClient` — `id`-only), and add `| Self::WatchHealth { id }` to the `id()` or-pattern (`:56-61`).
- [ ] **Step 4 — run, expect pass;** Step 5 clippy/fmt; **Step 6 commit** `feat(windows): WsRequest::WatchHealth subscribe op`.

### 2b — Pushed `Health` frame (`transport-ws/wire.rs`)
**Interfaces:** a health frame carrying `datamancer_core::HealthView`, produced/consumed **outside** `to_wire`/`from_wire` (it is not a `MarketEvent`).

- [ ] **Step 1 — failing test.** In `wire.rs` tests, assert a `Health` frame round-trips: `serde_json::to_string(&EventFrame::Health { view }) → from_str` yields an equal frame, and its `"type"` tag is `"health"`. Use a minimal `HealthView` fixture (schema-2 default).
- [ ] **Step 2 — run, expect fail.**
- [ ] **Step 3 — implement:** add `Health { view: datamancer_core::HealthView }` to `EventFrame` (after `:87`), import `HealthView` at `wire.rs:12-15`. **Do not** add it to `to_wire` (it never derives from a `MarketEvent` — leave the `_ => None` at `:133`) or `from_wire` (the client intercepts it before `from_wire`; add an explicit `EventFrame::Health { .. } => …` arm in `from_wire` at `:180` that returns a dedicated error/`None` so a stray health frame can never masquerade as a `MarketEvent`). Document the exception at the enum.
- [ ] **Step 4 — pass;** Step 5 clippy/fmt (`-p datamancer-transport-ws`); **Step 6 commit** `feat(windows): pushed Health event frame carrying HealthView`.

### 2c — `WsClient` health channel (`client/ws.rs`)
**Interfaces:** a second inbound sink `mpsc::Sender<HealthView>`; a `WsClient::watch_health()` (or connect-returned `HealthStream`) surfacing it.

- [ ] **Step 1 — failing test.** Drive `run_reader` with a serialized `EventFrame::Health` and assert the `HealthView` arrives on the health channel and **not** on the market-event channel.
- [ ] **Step 2 — fail;** **Step 3 — implement:** thread a `health_tx: mpsc::Sender<HealthView>` into `run_reader` (alongside `events` at `ws.rs:102`); in the `Inbound::Event` arm (`:113-119`) **intercept** `EventFrame::Health { view }` → `health_tx` **before** calling `from_wire`; everything else → `from_wire` as today. Surface the receiver (return it from `connect`, or an accessor). A `watch_health()` method sends `WsRequest::WatchHealth` via `request()` then returns the health `ReceiverStream`.
- [ ] **Step 4 — pass;** Step 5 clippy/fmt; **Step 6 commit** `feat(windows): WsClient health-push channel`.

### 2d — Daemon health-push task (`datamancerd/ws/conn.rs` + `server.rs`)
**Interfaces:** a `WatchHealth` dispatch arm that spawns a periodic push of `stamped_health_view` onto the connection's single-writer `tx`.

- [ ] **Step 1 — failing e2e-ish test.** In `datamancerd/tests/ws_e2e.rs` (`#![cfg(feature="ws")]`) add (behind the existing `#[ignore]`/live gate as appropriate): connect a WS client, send `watch-health`, assert ≥1 `Health` frame arrives within a diagnostics interval carrying a stamped `HealthView` (version populated).
- [ ] **Step 2 — fail;** **Step 3 — implement:** add a `WatchHealth { id }` arm to `dispatch` (`ws/conn.rs:316`) that returns `WsReply::ok(id)` and spawns a push task (mirror `spawn_pump:325`) which, on `self.diag_interval`, calls `dm.snapshot()` → `stamped_health_view(snapshot, credential_backend)` (reuse `server.rs:53`) and enqueues an `EventFrame::Health` JSON line onto the writer `tx` (the channel cloned into the sink at `conn.rs:55`). **Plumb `credential_backend`** (`&'static str`, `server.rs:1311`) into `handle_connection`/`dispatch` (new param — thread from `start_ws`/`serve` at `server.rs:475-494`). Tie the task's lifetime to the connection (abort on drop / EOF, like the pump).
- [ ] **Step 4 — pass** (`cargo test -p datamancerd --features ws ws_e2e -- --ignored` where live); Step 5 clippy (`-p datamancerd --features ws`) + fmt; **Step 6 commit** `feat(windows): daemon pushes HealthView over WS on watch-health`.

---

## Tasks 3–6 — outline (detailed against the landed foundation)

> These depend on Tasks 1–2 being implemented; their exact code is finalized once the foundation lands (TDD — no speculative code over unimplemented pieces). Structure + interfaces below.

- [ ] **Task 3 — Windows `AppHandle` (hybrid).** `#[cfg(windows)]`-split so the handle holds `PipeControlClient` (admin) + `WsClient` (data). Sites (from the map): `AppEvents` alias (`mod.rs:53`), the `client` field (`:116`), imports (`:41-49`), the connect in `ensure` (`:147-153`; keep `ensure_daemon`/pipe `ping` at `:139-146` for both), admin-method error types (`:177,195,217,243,261,275,297,317`) and data-method delegations (`:329-390`). Admin methods route to `PipeControlClient::request`; data methods to `WsClient`. Tests: an `#[ignore]` same-host smoke (spawn daemon `--features ws`, `AppHandle::ensure`, `subscribe`, admin `ping`/`health`). Commit per coherent slice.
- [ ] **Task 4 — `watch_health()` Windows arm.** Replace the iceoryx2 body (`mod.rs:401-428`) with a `#[cfg(windows)]` arm returning the `WsClient` health `HealthStream` from Task 2c; unix arm unchanged. Update `datamancer-client/CLAUDE.md` (the "watch_health never touches the control connection" / "app implies iceoryx2" stances change **on Windows only**). Test: the Windows arm yields a `HealthView` from a live daemon.
- [ ] **Task 5 — Spawn/config wiring.** `EnsureConfig` (`mod.rs:70-91`) gains a WS data endpoint (default `127.0.0.1:9001`, mirroring `config.rs:405-411`); the Windows `ensure` branch feeds it to `WsConfig`. The Windows daemon spawn (`platform_windows.rs::ProcessSpawner`) passes `--features ws`? — **N.B.** features are compile-time, so the *shipped Windows daemon binary must be built with `ws`*; the scaffolded config must set `[ws].enabled=true` on loopback (`config.rs:363-385`, default off). Decide enablement site (scaffold default on Windows vs app-written config). Tests: config round-trip; ensure-config default.
- [ ] **Task 6 — CI (⚠ pending Zach's blessing).** A native-Windows job that builds the daemon `--features ws` and runs the WS-loopback `app` smoke test (sidesteps iceoryx2; **reverses** `ci.yml:84-87`). **Do not merge without Zach's sign-off.** Update the boundary comment to reflect the lifted scope.

---

## Self-review checklist (run before PR)

- [ ] macOS/Linux unaffected: `git diff` shows only `#[cfg(windows)]`-additive changes to shared files; iceoryx2 paths untouched; unix `watch_health`/`AppHandle` byte-identical.
- [ ] `forbid(unsafe_code)` intact in `datamancer-transport-ws` + `datamancer-client`.
- [ ] Windows gates green: `cargo clippy -p datamancerd --features ws`, `-p datamancer-client --features app`, `-p datamancer-transport-ws`; `cargo test` for the new tests; `cargo fmt --check`.
- [ ] `to_wire`/`from_wire` still "one frame = one `MarketEvent`" — the `Health` frame never round-trips through them as a `MarketEvent`.
- [ ] Breaking changes **declared** via a `type(scope)!:` / `BREAKING CHANGE:` marker (NOT a manual version bump — release-plz owns it, #43); winsec dep requirement aligned to the workspace version; `cargo deny check`; `.github/scripts/semver-checks.sh origin/main` (the marker gate).
- [ ] Docs updated: `datamancerd/README.md` (WS health op), `datamancer-client/CLAUDE.md` (Windows hybrid stance).
- [ ] CI boundary change (Task 6) NOT merged without Zach's blessing.
