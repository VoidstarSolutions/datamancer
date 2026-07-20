# Windows WS-only daemon boot — Design

> **Companion to** `docs/superpowers/specs/2026-07-18-windows-phase4-ws-loopback-design.md` (Phase 4). Phase 4's client half (hybrid `AppHandle`, Tasks 3–4) has landed; this note covers the **daemon half** the Phase-4 plan under-scoped: making `datamancerd` actually *boot and run* on Windows. It is the precondition for Phase-4 Task 6 (native-Windows daemon CI) and directly implements Zach's blessing wording — *"removing the limitations preventing the daemon running on Windows."*

## 1. Problem

`datamancerd` **compiles** on Windows (CI's `native clippy + control-transport tests` job proves it) but has **never booted** there. `Server::bootstrap` unconditionally creates the one process-wide iceoryx2 node:

```rust
// server.rs:235
let node = NodeBuilder::new()
    .create::<ipc_threadsafe::Service>()
    .map_err(|e| DaemonError::Transport(format!("node create: {e:?}")))?;
```

Per the parent spec §2.5, **iceoryx2 node creation fails at runtime on Windows** (iceoryx2 0.9.2's PAL reads `/etc/passwd`; `InternalError`). So `bootstrap` returns `Err` and the daemon exits before serving anything. Two more sites depend on that node — the iceoryx2 diagnostics + health publishers (`run`, `server.rs:302-304`) and the per-client `Iceoryx2DataSink` (`open_client`, `server.rs:770`).

## 2. Key insight — the WS surface is already node-independent

The WS listener path does **not** touch `self.node`:

- `ws/conn.rs` bridges each connection to `dm.client_session()` and pumps its `EventStream` through `WsDataSink::new(tx)` (`ws/conn.rs:59-60`) — a channel sink, no iceoryx2.
- Health over WS is `spawn_health_push` (`ws/conn.rs:378`, Phase-4 Task 2), a per-connection ticker calling `dm.snapshot()` + `stamped_health_view` — no node.
- Diagnostics over WS is the pull `snapshot` op (`dispatch`), `dm.snapshot()` — no node.

So on Windows the daemon can serve its **entire** data/diagnostics/health surface over WS-loopback with **no iceoryx2 node at all**. The node and its three consumers are the *only* thing blocking boot, and every one of them is an iceoryx2-transport concern that Windows replaces with WS.

`self.node` is used at exactly three sites:

| Site | What | Windows replacement |
|---|---|---|
| `run`, `:302` | `Iceoryx2DiagnosticsPublisher` | WS `snapshot` pull |
| `run`, `:304` | `Iceoryx2HealthPublisher` (+ `spawn_diagnostics` ticker) | WS `spawn_health_push` |
| `open_client`, `:770` | per-client `Iceoryx2DataSink` (the iceoryx2 same-host client flow) | WS clients use `WsDataSink`; the pipe's `open-client` is not a Windows data path |

## 3. Design — `#[cfg(not(windows))]`-gate the iceoryx2 node stack

Consistent with the codebase's standing convention (*concrete `cfg`-split, not runtime branches or a trait abstraction* — parent spec §6, and the existing `#[cfg(unix)]/#[cfg(windows)]` splits throughout `server.rs`), the node genuinely **does not exist** on Windows. So the field itself is cfg-split, not made `Option<Node>`:

```rust
#[cfg(not(windows))]
node: Node,
```

**Decision 1 — cfg-split field, not `Option<Node>`.** `Option<Node>` would compile the iceoryx2 node type into the Windows build (it can't run) and scatter `.expect()`/`match None` at every use. A cfg-split field makes "no iceoryx2 on Windows" a **type-level** invariant and keeps each use site honest. Rejected: `Option<Node>` (muddies the Windows build with an unconstructible field and runtime-None branches).

Cascading cfg-gates (all `#[cfg(not(windows))]`, unix/macOS byte-for-byte unchanged):

- **`bootstrap`** — the `NodeBuilder::create()` block and the `node` field init.
- **`run`** — the two publishers, the `spawn_diagnostics` ticker, and its `diagnostics.abort()` in the `drain()` call (`:389`). On Windows none of these exist; WS carries health/diagnostics.
- **imports** — `NodeBuilder`, `ipc_threadsafe`, `Node`, `Iceoryx2DiagnosticsPublisher`, `Iceoryx2HealthPublisher`, `Iceoryx2DataSink` gated so Windows sees no unused-import warning under `-D warnings`.
- **`open_client`** — see Decision 2.

**Decision 2 — `open-client` over the control plane is rejected on Windows.** `open_client` creates an iceoryx2 data sink; it *is* the iceoryx2 same-host client flow. On Windows the control plane (owner-DACL named pipe) is the **admin** surface only — data rides WS (the hybrid `AppHandle` never sends `open-client` over the pipe). So on Windows `open_client` returns a clear, stable-coded rejection rather than trying to build a sink from a node that doesn't exist:

```rust
#[cfg(windows)]
async fn open_client(&mut self, _client: String, _subscriptions: Vec<SubscriptionSpec>) -> Reply {
    Reply::error(
        codes::UNSUPPORTED_ON_WINDOWS,
        "open-client (iceoryx2 data plane) is unavailable on Windows; use the WS data surface",
    )
}
```

`subscribe`/`unsubscribe`/`close` over the pipe then naturally return `unknown_client` (no client was ever opened). A new stable code `UNSUPPORTED_ON_WINDOWS` is added to `datamancer-client`'s `codes` (the shared vocabulary); it is additive.

**Decision 3 — warn (do NOT `compile_error!`) when a Windows daemon lacks a data plane.** With the node stack gone on Windows, a Windows daemon built **without** `--features ws` has **no data plane** — admin (pipe) only. That is fine for the existing `native clippy + control-transport tests` CI job (which builds `datamancerd --all-targets` **without** `ws` precisely to lint the `win_control` Win32 code), so `ws` must **not** be a hard compile requirement — a `#[cfg(all(windows, not(feature = "ws")))] compile_error!` would break that job. Instead, `run()` emits a one-line boot-time `tracing::warn!` on Windows when no WS data plane is serving (built without `ws`, or `[ws].enabled = false`): *"no data plane on Windows (built without `ws` / `[ws]` disabled); only the admin control plane is served."* The shipped Windows binary and the Task-6 boot/smoke job build `--features ws`; the warning catches a misconfigured deployment without breaking the control-transport lint build.

> **Adversarial-review note (2026-07-20):** the original draft made `ws` mandatory via `compile_error!`; verifying against `ci.yml` showed the `native clippy + control-transport` job builds `datamancerd --all-targets` **without** `ws`, which that hard error would break. Downgraded to a runtime warn. Also verified: the scaffold's Windows `[ws]` section validates **without** the `ws` feature (`scaffold_template_parses_and_validates` passes no-ws on Windows), and the orchestrator (`datamancer` crate) creates **no** iceoryx2 node — `build_runtime` is node-free — so cfg-gating the daemon's node + its three consumers is sufficient to boot WS-only.

## 4. What stays unchanged

- **Unix/macOS**: byte-for-byte. Every change is `#[cfg(not(windows))]`/`#[cfg(windows)]`-additive; the iceoryx2 node, publishers, and `open_client` sink path are the unix build verbatim.
- **`forbid`/`deny(unsafe_code)`**: unaffected — this is pure control-flow cfg-gating, no new FFI.
- **The WS surface**: unchanged. `start_ws`, `ws/conn.rs`, `WsDataSink`, `spawn_health_push` already work node-free; Windows simply runs them as the *only* surface instead of alongside iceoryx2.
- **Versioning**: `UNSUPPORTED_ON_WINDOWS` is an additive `codes` const; the daemon-facing changes are cfg-gated and mostly invisible to the Linux semver job. Any verdict is covered by the in-range `feat(windows)!:` marker (PR #37).

## 5. Test & CI (unblocks Phase-4 Task 6)

- **Unit**: a Windows-gated test that `Server::bootstrap` succeeds (no node-create error) and a WS client can `snapshot`/`subscribe` against the booted daemon on loopback.
- **`ws_e2e.rs`**: portable except `graceful_shutdown_closes_live_connection`, which uses `libc::kill(SIGTERM)` (`:215`). Gate that one test `#[cfg(unix)]` (SIGTERM is a Unix concept; the Windows graceful-drain equivalent — `CTRL_SHUTDOWN` / the `shutdown` control op — is separate). The other five ws_e2e tests use portable `child.kill()` and compile on Windows.
- **Task 6 CI**: a native-Windows job that builds `datamancerd --features ws`, boots it, and runs a WS-loopback `app` smoke test (`AppHandle::ensure` → `subscribe` → admin `ping`/`health`). Reverses the `ci.yml:84-87` support-boundary comment (Zach-blessed). Lands as its own reviewed slice.

## 6. Non-goals

- A Windows-native shared-memory data transport (C2 — later, only if zero-copy is required).
- Removing iceoryx2 from the Windows *build* graph (it stays a compiled dependency behind the `transport-iceoryx2` feature; only its *runtime* node is skipped).
- Any change to the Linux/macOS iceoryx2 data/diagnostics/health planes.
- iceoryx2 `open-client` support on Windows (explicitly rejected — WS is the Windows data surface).

## 7. Risk

Low–moderate. The change is mechanical cfg-gating of three well-identified node consumers, all of which have a proven node-free WS replacement already running in the same binary. The main risk is an *unused-symbol* cascade on Windows under `-D warnings` (imports, `max_clients`/`service_prefix` if they become Windows-dead) — caught immediately by the local Windows clippy gate. Booting the daemon on Windows for the first time may surface a second, currently-masked runtime dependency on the node (none found in the audit — the WS path is fully node-free), which the boot smoke test would catch.
