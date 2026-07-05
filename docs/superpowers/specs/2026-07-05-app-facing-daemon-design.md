# App-Facing Daemon: lifecycle, credentials, config, and health

**Date:** 2026-07-05
**Status:** Approved design; cycle 1 ready for implementation planning

## Motivation

Customer-facing tools — first among them the trade-execution application — need
a running datamancer backend without their users ever knowing `datamancerd`
exists. That requires four capabilities the daemon does not have today:

1. **Silent, reliable daemon management** — an app must find a running daemon
   or start one, without a human editing TOML or launching processes.
2. **A credential story fit for end users** — Alpaca keys currently arrive via
   environment variables, which a launchd/systemd-spawned daemon never sees and
   which cannot be changed after boot.
3. **A programmatic configuration surface** — today only the loopback web UI
   can rewrite config, and everything applies on restart.
4. **App-consumable observability** — the app must render "is my market data
   healthy?" natively; the `SystemSnapshot` exists but has no typed, versioned,
   app-facing reduction.

A second data provider — IBKR via the TWS API (`rust-ibapi` against a local
TWS/IB Gateway) — is planned. It is **not** built in this effort, but its
shape constrains several designs here; those constraints are recorded in the
appendix so we do not design ourselves into a corner.

## Decisions (settled during design)

| # | Question | Decision |
|---|----------|----------|
| 1 | Who owns daemon lifecycle? | **Both**: OS supervision (launchd/systemd) and app-spawn must work. Apps connect if a daemon is running, spawn if not; the existing single-instance lock arbitrates races. |
| 2 | Hot reload vs restart-to-apply? | **Selective hot reload**: providers and credentials apply live; storage paths, sockets, and listeners stay apply-on-restart. |
| 3 | Where do credentials live? | **The daemon is the single owner**, storing them via an OS-keychain-backed store (locked-down file fallback for headless hosts). Apps provision and retrieve credentials over the UDS control surface. Credentials leave `config.toml` entirely. |
| 4 | Observability priority? | **App-facing health API first** (typed, versioned, in `datamancer-client`), developer-facing structured logging/diagnostics second, ops-facing metrics later. |
| 5 | Config authority? | **Control surface is primary.** The daemon validates, applies (live where hot-classified), and atomically persists TOML as the sole hot-path writer. Operator hand-edits are still honored at boot. |
| 6 | IBKR scope? | **Constraints only** (appendix). Integration path: TWS API via `rust-ibapi` against a user-run IB Gateway/TWS. |
| 7 | Platforms? | macOS + Linux now. Platform-specific bits go behind small internal traits so a **Windows port is additive** (named pipes, Credential Manager, CreateProcess), not a redesign. |
| 8 | Consumer stack? | Tauri app with a Rust core. `datamancer-client`'s Rust crate API is the deliverable; no FFI/bindings work. |
| 9 | Embedding? | **In-process embedding stays first-class** until the daemon path is effectively done — as **capability parity, not API freeze**. An internal app ships today embedding the `datamancer` library directly; breaking library API changes are acceptable (the embedder absorbs updates), but every new capability here (credential store, health view) must be surfaced through the library API too, never daemon-only. |

## Delivery: four cycles, consumer-driven order

Each cycle is its own plan → implementation → review. A daemon capability
lands only when a facade method needs it, and the trade-execution app can
integrate from cycle 1's first release.

1. **Cycle 1 — `datamancer-client` app facade.** `AppHandle`: ensure-running
   (discover/spawn/readiness), typed health derived from today's
   `SystemSnapshot`, platform trait seams.
2. **Cycle 2 — credential broker.** `CredentialBackend` store,
   `set-/get-/clear-credentials` ops, peer-cred gate, hot provider
   (re)connect on credential change, env-var deprecation.
3. **Cycle 3 — config service.** `get-config`/`configure-provider`/
   `remove-provider`/`shutdown` ops, hot/cold field classification shared
   with the web UI, daemon-persisted TOML.
4. **Cycle 4 — health & observability v1.** Real health model feeding
   `HealthView` (companion-process states, per-symbol liveness), `ping` op
   with version info, `watch_health()` push stream, structured logging.

## Architecture: the `AppHandle` facade

A new `app` module in `datamancer-client` behind a cargo feature `app`
(implying `iceoryx2` — the app path is same-host UDS control + iceoryx2 data;
the WS transport remains the remote path and gains **no** lifecycle powers).
It layers on top of the existing `Client` trait without modifying it.

```
AppHandle::ensure(EnsureConfig) -> (AppHandle, Events)
   ├─ discover: probe default_control_socket() by connecting
   │     ├─ accepts → connect (existing UDS control + iceoryx2 Client path)
   │     └─ refused/absent → spawn datamancerd, await readiness (bounded)
   ├─ subscribe / unsubscribe / instruments / events   (delegates to Client)
   ├─ health() -> HealthView          (cycle 1: SystemSnapshot reduction)
   ├─ set_credentials / get_credentials / clear_credentials     (cycle 2)
   └─ get_config / configure_provider / shutdown_daemon         (cycle 3)
```

Structural rules:

- **Platform seams are traits from day one** (internal, not public API):
  - `ControlEndpoint` — UDS today; named pipe later.
  - `DaemonSpawner` — detached spawn + readiness today; Windows equivalents later.
  - `CredentialBackend` (daemon-side, cycle 2) — macOS Keychain / Linux
    secret-service / 0600-file fallback.
- **The facade adds no protocol semantics.** Every `AppHandle` method maps to
  control-surface ops. Anything the facade can do, a hand-rolled UDS client
  can do. The daemon stays the single authority.
- **Spawn, don't supervise.** `ensure()` starts a daemon but does not babysit
  it. If the daemon dies, the app's stream ends without `SessionClosing` (the
  existing loss contract) and the app calls `ensure()` again —
  reconnect-by-recreate. Always-on deployments use OS supervision.

## Cycle 1 detail: discovery, spawn, readiness

**Discovery is connect, not stat.** A socket file may be stale after a crash;
the probe is a real connect attempt with a short timeout. Refused or absent
means not running.

**`EnsureConfig`:**

- `daemon_binary: PathBuf` — required; no `PATH` search. The Tauri app knows
  where its bundled sidecar `datamancerd` sits. Guessing invites version skew
  and PATH hijack.
- `config_path: Option<PathBuf>` — `None` uses the daemon's platform default
  (which self-scaffolds on first run — already built).
- `ready_timeout: Duration` — default ~10 s.

**Spawn is detached**: own session/process group, stdio to the daemon's log
file rather than the app's pipes. The daemon deliberately outlives the app
that spawned it — it is a shared host service, and app-quit must not tear
down another app's market data. Deliberate stop is the cycle-3 `shutdown` op.

**Readiness**: poll-connect the socket until it accepts and answers a trivial
request (`list-clients` today; the cycle-4 `ping` adds version info), bounded
by `ready_timeout`. On expiry the error carries a diagnosis, not just
"timeout": *daemon exited (status + stderr tail)* vs. *process alive, socket
unresponsive*.

**Race handling.** Two apps ensuring simultaneously both spawn; the
single-instance lock makes one spawned daemon exit "already running". The
facade treats *lost race + subsequent successful connect* as success and
keeps retrying discovery for the remainder of `ready_timeout` before
reporting failure. No new locking — the daemon's existing lock is the arbiter.

**Version skew.** On connect the facade compares the daemon's reported
version against its own compatibility floor (bundled-sidecar deployments mean
app N+1 can meet daemon N spawned by another app). Cycle-1 policy: surface
`VersionSkew { daemon, client }` as a typed error and let the app decide.
Auto-upgrade orchestration is explicitly deferred.

## The health model: `HealthView`

A typed, versioned reduction the app renders directly. Cycle 1 computes it
client-side from `SystemSnapshot`; cycle 4 enriches what feeds it. The shape
is the contract and is designed for IBKR now.

**Placement (decision 9):** the `HealthView` types and the
`SystemSnapshot → HealthView` reduction are pure (no I/O), so they live in
`datamancer-core` next to `SystemSnapshot`. `datamancer-client` exposes them
over the wire (`AppHandle::health()`); the `datamancer` library exposes the
same reduction in-process (a `health()` accessor on the embedder's handle) —
one type, one reduction, both consumption modes.

```rust
pub struct HealthView {
    pub daemon: DaemonHealth,          // version, uptime, degraded-subsystem flags,
                                       // active credential backend (cycle 2+)
    pub providers: Vec<ProviderHealth>,
    pub streams: Vec<StreamHealth>,    // per (instrument, kind) — never aggregated
}

pub struct ProviderHealth {
    pub provider: ProviderId,
    pub state: ProviderState,          // Connected | Connecting | Disconnected
                                       // | Unauthenticated | CompanionUnreachable
    pub detail: Option<String>,        // human-renderable, non-contractual
}

pub struct StreamHealth {
    pub instrument: Instrument,
    pub kind: EventKind,
    pub liveness: Liveness,            // Live | Stale { since } | Gapped { spans }
                                       // | Backfilling
    pub last_event_source_ts: Option<Timestamp>,
    pub gap_count: u64,
    pub latency: Option<LatencySummary>, // rx_ts-derived; observability-only
}
```

- **Per-symbol only.** `streams` is keyed `(instrument, kind)`. No global
  event count, position, or merged sequence — the UI must not imply
  cross-symbol order (mirrors the workspace invariant).
- **`Unauthenticated` and `CompanionUnreachable` exist from day one.** Alpaca
  never emits `CompanionUnreachable`; IBKR will. Both enums are additionally
  `#[non_exhaustive]`.
- **Pull now, push later.** Cycle 1: `health()` on demand, app polls at its
  render cadence. Cycle 4: `watch_health()` push stream of the same type.
- **Versioned:** `HealthView::SCHEMA_VERSION` in the type and (cycle 4) the
  wire envelope, so skew degrades detectably instead of misrendering.
- `latency` is the sanctioned `rx_ts` use and is labelled observability-only.

## Control-protocol additions (cycles 2–3)

Same newline-JSON vocabulary; each op mirrored as an `AppHandle` method.
**None are served on the WS surface** — same-host trust only — and every one
is gated by a new **peer-credential (same-uid) check** on the UDS connection
(`SO_PEERCRED` / `getpeereid`).

### Cycle 2 — credentials

```jsonc
{"op":"set-credentials","provider":"alpaca","credentials":{"type":"api_key_pair","key_id":"…","secret":"…"}}
{"op":"get-credentials","provider":"alpaca"}    // -> stored shape, or code credentials_missing
{"op":"clear-credentials","provider":"alpaca"}
```

- `credentials` is a **tagged per-provider shape**, not a universal pair.
  Alpaca: `api_key_pair`. IBKR later: `{"type":"gateway","host":…,"port":…,
  "client_id":…}` — "credentials" containing no secret.
- Stored via `CredentialBackend`; backend chosen at runtime (keychain →
  secret-service → 0600 file), and the active backend is visible in
  `HealthView.daemon` so a surprising fallback is never silent.
- `set-credentials` on a configured provider **hot-applies**: the daemon
  reconnects that provider with the new credentials; in-flight sessions ride
  the existing in-band `Control` connectivity events.
- `get-credentials` exists because the trade-execution app uses the same keys
  for its own trading connections: the daemon is the one copy, apps read it —
  nothing to keep in sync. This is the agreed alternative to sharing keychain
  items across differently-signed binaries (fragile ACLs on macOS, none on
  Linux).
- **The credential store is a shared component, not daemon internals**
  (decision 9). It lives in a new small crate — working name
  `datamancer-credentials` — depending on `datamancer-core` only (it has
  platform I/O: keychain / secret-service / 0600 file, so it cannot live in
  core). Both consumers use the same store:
  - `datamancerd` wraps it with the control-surface ops above (the broker).
  - The `Datamancer` **builder** gains a credential-source API (explicit
    credentials, the shared store, or env vars), so an embedder reads or
    writes the same keychain entries in-process — one store regardless of
    embedding vs. daemon.
- With library parity in place, env-var loading is deprecated **everywhere**
  (warning first, removed once the store is proven) rather than daemon-only.
- The `[ws].auth_token` secret can migrate to the same store later, retiring
  the redaction dance in `GET/PUT /api/config`.

**Status:** implemented on `feature/credential-broker`, with two recorded
deviations from this section: (1) the credential-source API lands on each
provider's config (`CredentialsSource::{Env,Static,Watch}`) rather than on
the `Datamancer` builder itself — the builder consumes already-constructed
providers, so this is the honest surface for the same one-store-two-consumers
goal; (2) `clear-credentials` removes the stored entry but does not un-apply
credentials already hot-applied to a running provider (no un-auth primitive
exists) — a running stream keeps its last-applied credentials until restart.

### Cycle 3 — config

```jsonc
{"op":"get-config"}                             // full Config + applied-vs-disk divergence
{"op":"configure-provider","provider":"alpaca","settings":{ /* non-secret */ }}
{"op":"remove-provider","provider":"alpaca"}
{"op":"shutdown"}                               // graceful, deliberate stop
```

- Flow per mutating op: validate → apply live if hot-classified → atomically
  persist TOML → reply `{"applied":"live"}` or `{"applied":"restart_required"}`.
- **Every config field is classified hot or cold in one table**, shared by
  the web UI (which becomes another client of the same classification —
  generalizing today's boolean `restart_required`) and the control surface.
  A new field without a classification fails the build.
- Concurrent writers serialize through the daemon's single control actor:
  last-write-wins per op, never a torn file. The daemon is the sole hot-path
  writer; operator hand-edits are read at boot only.

## Error handling

Extends the existing two-layer model (`ClientError::Control` with stable
codes vs. transport errors) with typed lifecycle errors:

- `EnsureError::SpawnFailed { source, stderr_tail }`
- `EnsureError::ReadyTimeout { diagnosis }` — daemon-exited-with-status vs.
  alive-but-unresponsive
- `EnsureError::VersionSkew { daemon, client }`
- `EnsureError::NoSocketPath` — headless host with no derivable default;
  configure explicitly on both sides.

New stable control codes (regression-guarded like the existing table):
`credentials_missing`, `credential_backend_unavailable`, `permission_denied`
(peer-cred rejection), `restart_required` (valid op, cold-classified field),
`unknown_config_field`.

Credential-op error messages never echo secret material.

## Testing

- **Cycle 1:** facade unit tests against fake `ControlEndpoint` /
  `DaemonSpawner` (spawn race, stale socket, ready-timeout diagnoses — all
  simulable without a daemon). `#[ignore]`d e2e in the `daemon_e2e.rs`
  pattern: spawn → ensure → subscribe → health → shutdown; plus a
  two-`ensure()` race e2e proving lock arbitration.
- **Cycle 2:** one `CredentialBackend` contract test run against every
  backend (file backend in CI; keychain/secret-service `#[ignore]`d for dev
  machines). Hot-apply e2e: set credentials → provider reconnects → `Control`
  events observed. Peer-cred rejection test.
- **Cycle 3:** classification-table exhaustiveness test (new config field
  without a hot/cold class fails the build). TOML round-trip preserving
  operator boot-edits. Concurrent-writer serialization test.
- **Cycle 4:** `HealthView` reduction golden tests from snapshot fixtures,
  including synthetic `Gapped` / `Stale` / `CompanionUnreachable` fixtures
  Alpaca cannot produce today.

## Appendix: IBKR constraints (recorded, not built)

Planned integration: **TWS API via `rust-ibapi`** against a user-run IB
Gateway/TWS instance (the trade-execution app likely manages that gateway for
order routing; datamancer is told its host:port and never touches IBKR auth).

What this design must not preclude — and how it doesn't:

1. **Non-key-pair credentials.** IBKR's "credentials" are a gateway
   host:port + client id, no secret. → Tagged per-provider credential shapes
   (cycle 2).
2. **Companion-process health.** A provider can be "configured but its local
   gateway is unreachable or unauthenticated (daily re-auth/2FA lapsed)". →
   `ProviderState::{CompanionUnreachable, Unauthenticated}` reserved in the
   enum from cycle 1.
3. **Attach-style connect.** Provider config must tolerate "connect" meaning
   "attach to a local process", including it disappearing mid-session —
   surfaced through the same in-band `Control` connectivity events.
4. **Structured instrument identity.** IBKR contracts are structured
   (conId / exchange / currency), unlike opaque symbols. Constraint recorded:
   symbol→contract resolution stays **inside the provider** (source-agnostic
   invariant), and `Instrument` stays opaque until a real cross-provider
   collision forces the issue. Likely eventual shape: a provider-qualified
   instrument namespace — documented here, not committed to.
