# Phase 5 — datamancerd binary

**Fidelity:** design-level (firms up after 4)

_Part of the datamancer standalone-server roadmap. See `docs/superpowers/specs/2026-06-28-datamancer-server-roadmap.md`._

---

> **Reconciliation pass — authoritative; supersedes any conflicting text below.** Applied from the [cross-phase consistency report](2026-06-28-server-plan-consistency-report.md). Architect decisions: registry/ids/stats built in **Phase 2** (Issue 3); diagnostics snapshot **split** (Issue 6).
>
> Resolutions affecting this phase:
> - **Fallback pump signature (Issue 2):** the in-process pump uses `publish(MarketEvent) -> PublishOutcome` (owned) and matches on `PublishOutcome` rather than `?`-propagating a `Result`; use `publish_borrowed` only where serializing from a borrow.
> - **Anchor attaches to existing scope (Issue 7):** the daemon's lifecycle anchor and any client `subscribe` attach to the authoritative session's creation-time scope without re-specifying backfill (compose-via-refcount). Backfill is set when the authoritative session is first created.
> - **Diagnostics publishing (Issue 6):** publish Phase 3's bounded live-state snapshot on the fast diagnostics service and the cache catalog on the separate slower/chunked service.

> **Detailed-planning hardening (gotcha pass, 2026-06-28) — authoritative.** Adversarial design-level review vs the Datamancer/Session API + Phases 1-4. (Cross-phase "not coded yet" items are sequencing, not gotchas.) Supersedes conflicting body text.
>
> **Locked decision — control surface = Unix domain socket + newline-JSON.** Same-host, filesystem-permission access control, no extra transport dep, CLI-scriptable. **One long-lived control connection per client**; explicit graceful **`close`** op + connection-**EOF as emergency teardown** (tears down that client's `ClientSession` + iceoryx2 service, decrements authoritative refcounts). Subscribe/unsubscribe map to `ClientSession` mutators via an `mpsc` to a single-threaded server task (no lock held across `.await`). Library errors (`LiveSessionConflict`/`UnsupportedEventKind`/`PersistenceRequired`) map to **stable JSON error codes** (constants table, regression-guarded).
>
> **Config (TOML):** provider creds (secret refs via env/file), cache/DB path, tap log, adjustment, `resume_buffer_events`, `[[startup_session]]` (+ `always_on: bool`, default `false`), iceoryx2 caps, diagnostics cadences, control-socket path, `[web_ui]` (enabled, localhost bind addr/port, `assets_dir`, `live_state_cadence_ms`=1000, `cache_catalog_cadence_ms`=30000), `[server] shutdown_timeout_secs`=30.
>
> **iceoryx2 Node:** **one per process** (datamancerd creates at startup); per-client sinks own their *service* on it (refines Phase 4's "sink owns Node+service").
>
> **Startup anchors:** `always_on=true` holds the authoritative session for the process lifetime regardless of clients; `false` (default) is refcount-driven (pre-created for warmth, torn down at last client).
>
> **Backfill sharing:** an authoritative session created with `backfill_from` keeps history available while anchored; later clients of that symbol attach to the **live tail only**. Control `subscribe` carries scope/backfill as *preferences*; on conflict with an existing authoritative scope the reply returns the **actual** scope rather than erroring.
>
> **Service-cap overrun:** **reject the subscribe** (`service cap N exceeded`); dynamic recreation deferred.
>
> **Shutdown ordering:** SIGTERM/SIGINT → stop accepting control requests → stop authoritative provider subscriptions → flush per-client sinks + tap log (serialized, single shutdown holder) → drain → exit; whole drain bounded by `shutdown_timeout_secs`. **Last**-client-drop (not first) flushes a shared authoritative tap log (Phase 2 lifecycle). `EventSink::flush` log-and-swallow (Phase 1).
>
> **Runtime/supervision:** one tokio multi-thread runtime hosts authoritative tasks, per-client controllers, the control listener, the diagnostics publisher, and (Phase 6) the web server. An authoritative-task panic surfaces to subscribers as per-symbol `SubscriptionChanged{active:false}` (Phase 2); the daemon logs, does not abort.
>
> **Tests:** config parse/validate; client connect→subscribe→receive→disconnect cleanup (EOF + graceful `close`); dead-client teardown decrements refcounts; `always_on` anchor survives zero clients; refcount anchor tears down at last client; graceful shutdown drains within timeout; control request/response round-trip + stable error codes. Integration tests gated on Phase 1/2 landing.

## Fidelity

**Design-level.** This phase sits on Phases 1-4 and firms up once they land.
Every dependency on an earlier phase is an explicit RE-PLAN CHECKPOINT, not a
silent assumption. Do not code to the assumed shapes — re-confirm against the
real APIs at execution time and push any missing library surface *down* into the
phase that owns it.

## Hard constraints honored (do not violate)

- **Determinism is per-symbol only.** The binary computes **no**
  cross-instrument/global order. The multiplex ordering key is `(instrument,
  seq)`; events of different instruments have no defined relative order. Every
  design choice below preserves this — in particular, one sink **per client**
  (never shared) so per-client resume/gap accounting stays isolated.
- **One multiplexed stream per client over one connection.** "One connection"
  maps to one named iceoryx2 data-plane service per client. The client session
  is the primary consumer handle; a single-instrument client is just a session
  with one subscription.
- **`seq` is per-symbol, stamped at source, identical across clients of a
  symbol.** Two clients of the same instrument observe byte-identical `(seq,
  source_ts)` because they share the one authoritative per-`(instrument, kind)`
  session.
- **The authoritative per-`(instrument, kind)` session is the deterministic
  unit** — a shared singleton, refcounted across client sessions.
- **Same-host only** (network deferred), **Alpaca-only**. The consumer transport
  carries a data plane *and* a diagnostics plane.
- **`#![forbid(unsafe_code)]`** in the binary; any unsafe-adjacent iceoryx2
  interop is sealed inside the Phase-4 transport crate.

## Context & goal

Phase 5 is the server product: a thin `datamancerd` binary crate that wraps the
library and serves multiple consumer processes same-host. It introduces **no**
new ordering, transport, or event-model semantics. Its job is composition +
process lifecycle + a control surface:

1. Build a `Datamancer` from a config file (provider creds, cache/DB client, tap
   log, resume-buffer size, adjustment).
2. Accept client connections; per client create a Phase-2 client session wired
   to a per-client Phase-4 iceoryx2 data-plane service, and publish the Phase-3
   snapshot on the Phase-4 diagnostics plane.
3. Hold authoritative sessions alive as the cross-process lifecycle anchor so
   they keep running and recording across client presence.
4. Expose a control surface for runtime `subscribe`/`unsubscribe` mutating a
   client session's subscription set.
5. Graceful shutdown: stop accepting, `flush()` sinks + tap log, drain.

Authz is **deferred** (same-host, single-operator). Out of scope (Phase 6): the
HTTP/web introspection UI. The diagnostics *plane* (the iceoryx2 transport
carrying the snapshot) is Phase 4; Phase 5 only *drives* it.

## Current-code baseline (verified against the tree)

These are the real shapes Phase 5 builds on. Where the roadmap requires a
*change* to them, that change is owned by the earlier phase and called out as a
RE-PLAN gate — not absorbed into the binary.

- `Datamancer` is `Clone` (Arc inner, `session.rs:162`). No `Arc<Datamancer>`
  wrapper needed.
- The **only** session entry today is
  `Datamancer::session(instrument, kind, scope, options) -> Result<Session>`
  (`session.rs:219`). There is no client-session / multiplexed-stream type yet —
  that is Phase 2.
- **The live registry currently *rejects* a second live session for a pair**
  with `Error::LiveSessionConflict` (`session.rs:239-249`); it does **not** share
  or refcount. The roadmap's "shared refcounted singleton" is an *inversion* of
  this behavior — see RE-PLAN P2 (load-bearing: until it lands, two clients on
  the same live symbol cannot coexist, so the headline agreement test cannot
  pass).
- `Session::take_events(&self) -> Result<EventStream>` is **async and fallible**
  and multi-shot (resume primitive) (`session.rs:573`) — not the sync signature
  to assume in a pump loop.
- Builder surface verified: `provider`/`provider_arc`, `tap_log`/`tap_log_arc`,
  `historical_cache`/`historical_cache_arc`, `pin`, `resume_buffer_events`,
  `adjustment`, `build` (`session.rs:397-481`).
- `Scope::Historical { from, to }` | `Scope::Live { backfill_from: Option<_> }`
  (`session.rs:63`). `PersistenceOptions` presets: `none`/`cached`/`read_only`/
  `refresh` + `with_tap_log(bool)`; `#[non_exhaustive]` (`session.rs:90-155`).
- Provider creds are loaded **from the environment by `oxidized_alpaca`, keyed on
  `account_type` (Paper/Live)**. `AlpacaProviderConfig` /
  `AlpacaCryptoProviderConfig` expose `account_type` (+ crypto `venue:
  AlpacaCryptoVenue::{Us, EuKraken}` and `reconnect: ReconnectPolicy`) — **no**
  explicit key/secret fields (`alpaca_crypto.rs:105-123`, `alpaca.rs:106-119`).
- `TapLog::flush()` exists (`traits/storage.rs:38`). `HistoricalCache` has **no**
  `flush` — do not assume a cache flush in the shutdown path.
- `SurrealCache::open(SurrealCacheConfig)` / `SurrealTapLog::open(...)`, both with
  `embedded(path)` and `Memory` variants (`storage/surreal*.rs`).
- `Instrument::new(ProviderId::new(provider), AssetClass, symbol)` — the config
  provider/asset_class/symbol triple maps straight onto this.
- `seq` today is **session-monotonic, stamped at delivery** (CLAUDE.md). The
  binary relies on the Phase-1 redefinition (**per-symbol, stamped at source**).
  This is a semantic change owned by Phase 1 — see RE-PLAN P1.

## RE-PLAN CHECKPOINTS

Re-confirm each against the real API before writing code.

- **P1 — `seq` redefinition + `EventSink` shape.** (a) Confirm Phase 1 made `seq`
  per-symbol/at-source (the binary's multiplex key `(instrument, seq)` depends on
  it). (b) Assumed `trait EventSink: Send + Sync` with async `publish(&MarketEvent)`
  + `flush()` in `datamancer-core`, wired via a builder method analogous to
  `historical_cache`. Confirm the method name, per-`Datamancer` vs per-client,
  and that the tap-log tee + resume buffer stay **core-side of the sink** so a
  single `flush` drains both. Verify which `flush` covers which.
- **P2 — client session API + registry inversion.** (a) Confirm the live
  registry changed from *reject-second-live* (`LiveSessionConflict`, current) to
  *shared-refcounted-singleton*; if it has **not**, Phase 5 cannot serve two
  clients of one live symbol and must block on Phase 2. (b) Assumed public
  `ClientSession` (name TBD; today's `Session` may evolve or a new type is added)
  with: construction from a `Datamancer` yielding a refcounting handle;
  `subscribe(instrument, kind, scope, options)` / `unsubscribe(instrument, kind)`
  live mutators; and either construction *with* an `EventSink` (preferred) or a
  `take_events()` the binary pumps. Confirm whether `take_events` stays async/
  fallible/multi-shot.
- **P3 — snapshot API.** Assumed `Datamancer::snapshot() -> SystemSnapshot`
  (serializable, point-in-time, in-memory). Confirm the name, whether it is
  `async`, whether the cache-catalog portion does I/O (it likely needs a
  coverage-table read — **not** free; see Step 6), and that it reflects client
  sessions + their subscriptions + per-symbol refcount created by this binary.
- **P4 — iceoryx2 transport (sink + diagnostics + crate placement).** Assumed
  behind `transport-iceoryx2`: a per-client data-plane sink constructor bound to
  a named service; a diagnostics publisher taking a serialized `SystemSnapshot`;
  and the symbol-table announcement service with sink-local `SymbolId` interning
  (the binary should not manage `SymbolId`). Confirm: (a) lives in `datamancer`
  behind the feature vs a separate `datamancer-transport-iceoryx2` crate (the
  `forbid(unsafe_code)` boundary — `datamancerd` depends on whichever); (b) the
  iceoryx2 `Node` ownership model (per-process vs per-service) and whether the
  binary owns the `Node`; (c) the pinned iceoryx2 version; (d) backpressure /
  overflow semantics across the shm boundary (history depth vs resume buffer) —
  the binary inherits Phase-4's choice, it does not reconcile.
- **P5 — control-surface transport.** Roadmap candidates: iceoryx2
  request-response vs a local admin socket (UDS). This plan **recommends UDS +
  newline-JSON** for Phase 5 (rationale in Step 5); revisit iceoryx2
  request-response once Phase 4 confirms its RR surface is stable on the pinned
  version.

Additional assumptions:

- **Runtime.** Single shared tokio multi-threaded runtime (`#[tokio::main]`); no
  second runtime. The binary owns no ordering logic.
- **Provider creds.** Selected by `account_type` from config; the actual
  key/secret come from env via `oxidized_alpaca`. **There is no current ctor that
  accepts explicit creds** — if the config must carry explicit key/secret, that
  is a provider-crate change (RE-PLAN against the provider, not Phase 5). The
  config schema below therefore selects `account_type` only and documents the env
  vars.
- **Client discovery.** "One connection per client" = one named iceoryx2
  data-plane service per client, created on an explicit `open-client` control
  command (resolved below in Step 3) rather than iceoryx2 auto-discovery.

## Step-by-step implementation

### Step 0 — New crate skeleton

Add `crates/datamancerd` to workspace `members` in the root `Cargo.toml`.
`crates/datamancerd/Cargo.toml`:

- `edition = "2024"`, `[lints] workspace = true`; `#![forbid(unsafe_code)]` at
  the top of `main.rs`.
- deps: `datamancer` (path; default features `provider-alpaca`,
  `storage-surreal`, plus `transport-iceoryx2`), `tokio` (workspace; **`signal`
  is not in the workspace tokio features** — add it here, or add to the workspace
  feature list), `serde` + `serde_json` (normal deps — note `serde` is only a
  *dev*/optional dep of `datamancer` today, so declare them directly), `toml`
  (config, see Step 1), `thiserror`, `tracing` + `tracing-subscriber`, `clap`
  (`--config`).
- If Phase 4 placed the transport in `datamancer-transport-iceoryx2`, depend on
  that crate directly too.

`src/main.rs` thin: parse args, load config, build, run, await shutdown. Logic
in submodules: `config.rs`, `server.rs` (supervisor), `client.rs` (per-client
lifecycle), `control.rs` (control surface), `diagnostics.rs` (snapshot cadence +
publish), `shutdown.rs`.

### Step 1 — Config loading (`config.rs`)

`serde`-`Deserialize` `Config`, format **TOML** (human-editable, Cargo-aligned).
RE-PLAN: roadmap lists config format as open; TOML recommended, confirm no
YAML/JSON preference.

Config shape (grounded in the real ctors above):

```toml
# provider creds: account_type selects which env credential pair oxidized_alpaca
# loads (paper -> ALPACA_PAPER_API_KEY_ID/SECRET, live -> ALPACA_API_KEY_ID/...).
# There is NO explicit key/secret field today; selecting "live" requires the
# live env creds to be present. (RE-PLAN against the provider crate if explicit
# creds in config become a requirement.)
[provider.alpaca]
account_type = "paper"            # paper | live

[provider.alpaca_crypto]
account_type = "paper"
venue = "us"                       # us | eu_kraken   (AlpacaCryptoVenue)

[cache]
backend = "surreal-embedded"       # surreal-embedded | surreal-memory | surreal-remote
path = "/var/lib/datamancerd/cache"   # embedded path
# (remote URL/ns/db only if/when SurrealCacheConfig grows a remote variant)

[tap_log]
backend = "surreal-embedded"
path = "/var/lib/datamancerd/taplog"

[session]
resume_buffer_events = 65536
adjustment = "all"                 # maps to Adjustment

[server]
admin_socket = "/run/datamancerd/admin.sock"   # control surface (UDS)
service_prefix = "datamancerd"                  # iceoryx2 service/node name prefix

[diagnostics]
publish_interval_ms = 1000         # live-state cadence on the diagnostics plane
# cache_catalog_interval_ms = 30000  # slower cadence for the I/O-heavy portion

# optional: instruments brought up at boot (authoritative sessions held
# regardless of client presence — the lifecycle-anchor set)
[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"
symbol = "BTC/USD"
kind = "trade"
scope = "live"                     # live | live_backfill
backfill_from = "2026-06-01T00:00:00Z"   # required iff scope = live_backfill
persistence = "cached_with_tap"           # preset name -> PersistenceOptions
```

`config.rs` responsibilities:

- `Config::load(path) -> Result<Config>` (read file + `toml::from_str`).
- `Config::into_datamancer(self) -> Result<Datamancer>`: construct providers from
  the `[provider.*]` sections (`AlpacaProvider::new(AlpacaProviderConfig { .. })`,
  `AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig { .. })` — `account_type`
  from config, venue/reconnect for crypto), open the cache
  (`SurrealCache::open(SurrealCacheConfig::embedded(path))` / `Memory`) and tap
  log, then
  `Datamancer::builder().provider_arc(..).historical_cache_arc(..).tap_log_arc(..).resume_buffer_events(..).adjustment(..).build()`.
- Map `persistence` preset names to `PersistenceOptions` (e.g.
  `cached_with_tap` -> `PersistenceOptions::cached().with_tap_log(true)`).
- Map `scope`/`backfill_from` to `Scope::Live { backfill_from }`; reject
  `live_backfill` without `backfill_from`.
- Validate: at least one provider; `[cache]` present if any startup session uses
  cache (`uses_cache`); `[tap_log]` present if any uses `write_tap_log`. Fail
  fast with `Error::Config`-style messages (mirrors the library's own
  `PersistenceRequired` fail-fast).

### Step 2 — Supervisor / lifecycle anchor (`server.rs`)

`Server` owns: the built `Datamancer` (clone-shareable); the iceoryx2 `Node`
(per RE-PLAN P4); the **authoritative-session anchors** for `startup_session`
entries; the connected-client registry (`HashMap<ClientId, ClientHandle>`); the
control listener; and the diagnostics ticker.

Anchors: open the startup sessions at boot and **hold them for the process
lifetime**. Once Phase 2 makes authoritative sessions shared refcounted
singletons (RE-PLAN P2), the anchor is one more referrer: a client subscribing to
`BTC/USD` shares the *same* authoritative stream (identical `(seq, source_ts)`)
and client churn never tears it down. Without an anchor, an authoritative session
is created on first client subscribe and torn down on last unsubscribe (pure
refcount). **NOTE (current-code gate):** with today's reject-second-live
registry, opening a startup anchor for a live pair and then having a client
subscribe to the same pair would fail with `LiveSessionConflict`. The anchor
model is only correct after the P2 inversion lands.

`Server::run()`: (1) assemble anchors; (2) start diagnostics task (Step 6); (3)
start control listener (Step 5); (4) `select!` on shutdown (Step 7).

### Step 3 — Per-client lifecycle (`client.rs`)

A client connects by issuing an `open-client` control command (Step 5) naming
itself. On open the server:

1. allocates a `ClientId` and the per-client data-plane service name
   (`{service_prefix}/client/{id}/data`);
2. creates the **per-client iceoryx2 data-plane sink** (RE-PLAN P4) — strictly
   one sink per client (constraint: no shared sinks);
3. creates a Phase-2 **client session** wired so its multiplexed output flows
   into that sink (RE-PLAN P2 — preferred: construct *with* the sink; fallback:
   the Step-4 pump);
4. registers `ClientHandle { client_session, sink, pump_task, service_name }`
   keyed by `ClientId`.

Initial subscriptions come from the `open-client` command (may list instruments)
or start empty and fill via `subscribe`.

**Teardown** (`close-client`, or detected disconnect — Step 5): drop the client
session (releasing its refcounts; last referrer tears the authoritative session
down unless a startup anchor holds it), `flush()` then drop the sink, abort/join
the pump task. RE-PLAN P2: confirm dropping the client session is the
unsubscribe-all path and refcount release is prompt enough that the next
diagnostics snapshot reflects it.

**Control scoping (carried, not re-derived).** Per-symbol `Control` (`Gap`,
`SubscriptionChanged`) ride each symbol's substream; connection-scoped `Control`
(`ProviderConnected/Disconnected`, `ProviderError`, `SessionClosing`) ride the
multiplexed stream once. This is Phase-2 behavior; the binary just lets the
multiplexed stream flow to the sink.

### Step 4 — Wiring the data plane (sink binding)

Preferred (RE-PLAN P2): the client session is constructed *with* an `EventSink`
and itself calls `sink.publish` in `(instrument, seq)` arrival order, inheriting
the core-side resume buffer + tap-log tee. The binary owns no event loop.

Fallback pump (if the client session only exposes `take_events()`):

```text
let mut stream = client_session.take_events().await?;   // async + fallible today
let sink = client_data_sink;                            // one sink, this client
let pump = tokio::spawn(async move {
    while let Some(ev) = stream.next().await {
        if let Err(e) = sink.publish(&ev).await { warn!(?e, "sink publish"); break; }
    }
    let _ = sink.flush().await;
});
```

A single sequential pump preserves the multiplexed stream's existing order
(per-symbol monotone `seq`; no cross-symbol order is created or required).
Backpressure across the iceoryx2 boundary is a Phase-4 concern (RE-PLAN P4); the
binary inherits it and does not reconcile here.

### Step 5 — Control surface (`control.rs`)

**Recommendation: UDS admin protocol** (newline-delimited JSON). Rationale: (a)
same-host and access-controlled by filesystem perms (a free perimeter while authz
is deferred); (b) decoupled from the uncertain pinned-iceoryx2 RR surface; (c)
easy to drive from a CLI/operator and later the Phase-6 web UI. RE-PLAN P5:
revisit iceoryx2 RR once Phase 4 standardizes it.

Protocol (one JSON object per line):

- `{"op":"open-client","client":"exec-1","subscriptions":[{...}]}` ->
  `{"ok":true,"service":"datamancerd/client/exec-1/data"}`
- `{"op":"subscribe","client":"exec-1","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade","scope":"live","persistence":"cached_with_tap"}`
- `{"op":"unsubscribe","client":"exec-1","symbol":"BTC/USD","kind":"trade"}`
- `{"op":"close-client","client":"exec-1"}`
- `{"op":"list-clients"}` / `{"op":"snapshot"}` (returns the Phase-3 snapshot as
  JSON — same data the diagnostics plane carries; single assembly path).

Each command maps to a `ServerCommand` enum sent over an `mpsc` channel to the
server task (no lock held across `.await`; the server task owns the registry
single-threadedly). `subscribe`/`unsubscribe` call the client-session mutators
(RE-PLAN P2). Replies carry success/error; map library `Error` variants
(`LiveSessionConflict`, `UnsupportedEventKind`, `PersistenceRequired`) to stable
JSON error codes (an operator-facing contract — regression-guarded in tests).

Disconnect detection: a crashed client closes its admin connection; the listener
treats EOF on a connection that issued `open-client` as an implicit
`close-client` (releasing authoritative refcounts). RE-PLAN: confirm the admin
connection is long-lived per client (enabling this); otherwise add an explicit
heartbeat or rely on explicit `close-client`.

### Step 6 — Diagnostics plane driver (`diagnostics.rs`)

A `tokio::spawn`ed ticker on `diagnostics.publish_interval_ms`:

1. assemble the snapshot (RE-PLAN P3). **The cache-catalog portion does I/O**
   (coverage-table read) and must not run on the hot tick: split — live state
   every interval, cache catalog on a slow cadence (or via `spawn_blocking`),
   merge. Confirm whether `snapshot()` is `async` and whether it self-splits.
2. hand the snapshot to the Phase-4 diagnostics publisher (RE-PLAN P4).

The diagnostics plane is one process-wide service (all clients read it), distinct
from per-client data-plane services. RE-PLAN P4 delivery mode: this plan assumes
**periodic publish**; if Phase 4 chose request-response, the ticker becomes a
request handler. The snapshot also feeds `{"op":"snapshot"}` and (Phase 6) the
web UI — single assembly path.

### Step 7 — Graceful shutdown (`shutdown.rs`)

`tokio::select!` in `Server::run` on `tokio::signal::ctrl_c()` and SIGTERM
(`signal::unix`). On signal, ordered drain:

1. **Stop accepting**: close the control listener; reject new
   `open-client`/`subscribe`.
2. **Stop the diagnostics ticker** (optionally one final "shutting down"
   snapshot if the type supports it).
3. **Drain clients**: per `ClientHandle`, in order — signal the client session to
   close (emits `SessionClosing` down each substream; Phase-2/core behavior),
   `flush()` the per-client sink so buffered events reach subscribers, await the
   pump task, drop the handle.
4. **Drop startup anchors**: releases the last refcounts; authoritative sessions
   tear down (unsubscribe upstream, provider `shutdown`).
5. **Flush the tap log**: ensure `TapLog::flush()` completes. NOTE: there is **no
   `HistoricalCache::flush`** today — do not assume one. RE-PLAN P1: confirm
   where the authoritative `flush` for sinks + tap log is invoked — does dropping
   anchors flush the tap log (the current `Session::close` path calls tap-log
   `flush`), or does the binary need an explicit `Datamancer`-level flush? If the
   latter doesn't exist, that is a Phase-1/2 library gap, not binary code.
6. Exit 0.

Wrap the whole drain in `tokio::time::timeout` so a disk-stalled tap-log
`flush()` (best-effort unbounded channel) cannot hang shutdown forever (log +
force-exit on timeout).

## Public API / type changes

`datamancerd` is a **binary** — **no new public library API**. The only
public-ish artifacts: the **config schema** (TOML) and the **admin-socket
protocol** (JSON ops), both documented operator contracts.

Library gaps found during execution belong in the earlier phase that owns them,
recorded as RE-PLAN feedback, **not** patched into the binary. Anticipated gaps:
(a) registry inversion to shared refcounted live sessions (Phase 2 — currently
`LiveSessionConflict`); (b) client-session-with-sink construction (Phase 2); (c)
a process-level `flush`/drain covering all sinks + tap log (Phase 1/2); (d)
iceoryx2 `Node` lifecycle ownership exposed to the binary (Phase 4); (e) explicit
provider creds in config if required (provider crate).

## Test plan

Binary crate: integration tests in `crates/datamancerd/tests/` + unit tests on
pure submodules. Most behavior needs the iceoryx2 runtime and/or live Alpaca, so
split sharply into pure-unit vs `#[ignore]`d end-to-end.

Unit (no runtime, no network — regression guards for the binary's own logic):

- `config_parses_minimal_toml` — minimal config round-trips.
- `config_into_datamancer_builds` — config with a fake/echo provider (or
  Alpaca construct-only path) yields a `Datamancer`; assert
  resume-buffer/adjustment/cache/tap wiring matches config.
- `config_rejects_cache_session_without_cache` — startup session needing cache,
  no `[cache]` -> config error.
- `config_rejects_live_backfill_without_from` — `scope = live_backfill` without
  `backfill_from` -> config error.
- `persistence_preset_maps` — each preset name -> the right
  `PersistenceOptions`.
- `control_protocol_roundtrip` — serde each admin op + reply.
- `control_command_maps_to_server_command` — parsed op -> right
  `ServerCommand`; unknown op -> structured error reply.
- `subscribe_error_maps_to_json_code` — `LiveSessionConflict` /
  `UnsupportedEventKind` / `PersistenceRequired` map to stable JSON codes
  (operator-contract guard).
- `shutdown_drain_order` — with mock client handles + a recording fake sink,
  assert order stop-accept -> diagnostics-stop -> per-client flush ->
  anchor-drop -> tap-log-flush, and that `flush()` is called on every sink before
  drop (mirrors the existing tap-log flush test).

Integration (`#[ignore]`d in CI; need iceoryx2 runtime and/or Alpaca creds):

- `two_clients_same_symbol_agree` — two clients subscribe to one instrument over
  their two per-client services; assert identical `(seq, source_ts)` per event
  across both (the headline per-symbol-agreement guard). **Gated on RE-PLAN P2**
  (shared refcounted live sessions); cannot pass against the current
  reject-second-live registry.
- `runtime_subscribe_unsubscribe` — subscribe mid-run, receive, unsubscribe, stop
  receiving; snapshot reflects the change.
- `authoritative_survives_client_churn` — a startup-anchored symbol keeps
  recording (tap log grows) while clients connect/disconnect; refcount never
  hits zero.
- `last_referrer_teardown` — non-anchored symbol: last unsubscribe tears down the
  authoritative session.
- `diagnostics_plane_roundtrip` — a subscriber reads the diagnostics service and
  reconstructs a snapshot reflecting current clients/subscriptions.
- `graceful_shutdown_flushes` — SIGTERM during live flow; assert sinks + tap log
  flushed and process exits 0 within the timeout.
- `symbol_table_resolution_through_daemon` — a late-joining client resolves all
  `SymbolId -> Instrument` it sees (Phase-4 behavior, end-to-end).

RE-PLAN (hermetic CI): the integration harness needs a deterministic provider for
CI without Alpaca. Recommend a `[provider.replay]` config variant backed by a
`ReplaySource`/tap-log replay so the headline tests run hermetically — confirm at
execution whether the config can name a test/replay provider.

## Doc / invariant updates

- `crates/datamancerd/README.md` (new) — operator-facing: config schema, admin
  protocol, connection model (one iceoryx2 service per client), lifecycle-anchor
  semantics, shutdown behavior; state loudly that the UDS perimeter is filesystem
  perms only (not network-safe) and that the daemon introduces **no cross-symbol
  ordering**.
- Root `CLAUDE.md` "Workspace" — add `datamancerd` as a third crate (thin binary;
  depends on `datamancer`; `#![forbid(unsafe_code)]`).
- Root `CLAUDE.md` "Common commands" — add
  `cargo run -p datamancerd -- --config datamancerd.toml` and the `#[ignore]`d
  daemon integration invocation.
- `crates/datamancer/README.md` — short "Standalone server" pointer: the library
  stays primary; `datamancerd` is the thin wrapper (embedders still use the
  in-process sink for zero hops).
- No **invariant** changes here — per-symbol determinism, `(instrument, seq)`
  ordering, and `seq`-at-source were settled in Phases 1-2. Phase 5 only consumes
  them.

## Open questions

1. Config format — TOML recommended; confirm no YAML/JSON preference. (RE-PLAN.)
2. Control-surface transport — UDS+JSON vs iceoryx2 RR (RE-PLAN P5).
3. Connection/client-identity model — explicit `open-client` vs iceoryx2
   auto-discovery.
4. Disconnect detection — implicit `close-client` on admin EOF vs heartbeat
   (depends on whether the admin connection is long-lived per client).
5. Diagnostics delivery mode — periodic publish (assumed) vs RR (RE-PLAN P4); and
   the live-state-vs-cache-catalog cadence split (Step 6).
6. Where the authoritative `flush` lives — does dropping anchors flush tap log +
   sinks, or is an explicit `Datamancer::flush` needed? (RE-PLAN P1; note: no
   cache flush exists today.)
7. Client session <-> sink binding — construct-with-sink vs binary-owned pump
   (RE-PLAN P2).
8. Hermetic CI provider — a `[provider.replay]` config path.
9. Explicit provider creds in config — current ctors only take `account_type`
   (env-resolved). Needed? (RE-PLAN against provider crate.)

## Risks

- **Dependency stack-up + the registry inversion (highest).** Phase 5 sits on
  1-4; the single most load-bearing gate is the Phase-2 change from
  reject-second-live (`LiveSessionConflict`, current code) to shared refcounted
  live sessions. Until it lands, two clients of one live symbol cannot coexist
  and the headline agreement test cannot pass. Mitigated by the explicit RE-PLAN
  gates and by keeping the binary thin.
- **Shutdown drain hangs.** A best-effort unbounded tap-log channel means a
  disk-stalled `flush()` can hang; mitigate with the Step-7 timeout +
  force-exit-with-log.
- **iceoryx2 Node/service lifecycle in a long-lived process.** Services are
  fixed-size at creation; per-client create/teardown churn must not leak shm on
  client crash — verify a crashed client's service is reclaimable (Phase-4
  property).
- **Blocking the executor in the diagnostics ticker.** The cache-catalog portion
  needs I/O; cadence-split + `spawn_blocking` (Step 6).
- **Authz deferred.** UDS filesystem perms are the only perimeter; document
  loudly that this is not a network-safe surface.
- **`forbid(unsafe_code)` vs iceoryx2.** All unsafe-adjacent interop stays sealed
  in the Phase-4 transport crate; the binary must see only `EventSink` + a
  snapshot publisher, never a type forcing unsafe conversions.
- **Per-client gap isolation.** Each client gets its own copy of a shared
  symbol's events into its own service; one client's overflow `Gap` must not
  pollute another. The binary must keep strictly one sink per client and never
  share a sink — undermining this would break per-client determinism accounting.

## Review notes

Changes made to the draft:

- **Added a verified current-code baseline section.** The draft asserted shapes
  (e.g. provider creds, `take_events` signature) that disagree with the tree.
  Corrected: `Datamancer::session` is the only entry today; the live registry
  *rejects* a second live session (`LiveSessionConflict`, `session.rs:239-249`)
  rather than sharing — this is an *inversion* Phase 2 must perform, now flagged
  as the highest-risk gate and as a precondition on the headline
  `two_clients_same_symbol_agree` test (the draft listed that test as if it could
  pass unconditionally).
- **Corrected the provider-cred config.** Current ctors take only
  `account_type` (env-resolved by `oxidized_alpaca`); there are **no** explicit
  key/secret fields. Removed the draft's `api_key_id`/`api_secret_key` override
  fields from the schema and added open question #9 + a RE-PLAN against the
  provider crate. Fixed crypto `venue` to the real enum (`us`/`eu_kraken`).
- **Fixed the fallback pump** to match the real async/fallible/multi-shot
  `take_events()` signature.
- **Corrected the shutdown flush.** `HistoricalCache` has no `flush`; only
  `TapLog::flush` exists. Removed "any cache flush" and noted the absence.
- **Called out the `seq` semantic change** (current: session-monotonic at
  delivery; required: per-symbol at source) as a Phase-1-owned dependency folded
  into RE-PLAN P1 — the draft assumed the target semantics silently.
- **Promoted the hard constraints to an explicit honored-constraints section**
  and reinforced per-symbol-only / no-cross-symbol-ordering and one-sink-per-
  client throughout.
- **Tightened dep notes**: `serde`/`serde_json` are not normal deps of
  `datamancer` (serde optional, serde_json dev-only) so the binary must declare
  them; confirmed `signal` is absent from the workspace tokio features.
- Minor: noted `Datamancer` is `Clone` (no `Arc` wrapper), added
  `config_rejects_live_backfill_without_from` and `persistence_preset_maps` unit
  tests.

Unresolved concerns (correctly deferred to RE-PLAN, not resolvable now): the
exact Phase-1 `EventSink`/`flush` shape; whether Phase 2 yields construct-with-
sink or a pump; whether `snapshot()` is async and self-splits the I/O-heavy
catalog; iceoryx2 `Node`/service lifecycle and backpressure semantics; and the
control-surface transport (UDS recommended pending Phase-4 RR stability). The
plan stays at design altitude on these deliberately given the "firms up after 4"
fidelity.
