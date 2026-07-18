# datamancerd

The standalone **datamancer server**: a thin binary that wraps the `datamancer`
library and serves multiple consumer processes on the **same host**. It adds no
new ordering, transport, or event-model semantics — its job is composition,
process lifecycle, and a control surface.

Embedders who want zero hops should keep using the library in-process. Reach for
`datamancerd` when several processes on one machine need to share authoritative
sessions (and their recording) and read a single multiplexed stream each.

> **Security:** the control surface is same-host only, guarded by OS access
> control — a Unix-domain socket with a same-uid peer-cred gate on Unix, or an
> **owner-only-DACL named pipe** on Windows (only the daemon's own user can
> open it). There is no network transport. This is **not** a network-safe
> surface; run it same-host, single-operator. See
> [Control transport](#control-transport-platform).

> **Ordering:** determinism is **per symbol** only. The daemon computes **no**
> cross-instrument or global order. Two clients of the same instrument observe
> byte-identical `(seq, source_ts)` because they share the one authoritative
> per-`(instrument, kind)` session.

**Writing a consumer?** Don't hand-roll the control protocol below —
`datamancer-client` (`crates/datamancer-client`) is the vocabulary this
daemon speaks (`spec`, `codes`, request/reply framings) plus, behind features
`ws`/`iceoryx2`, ready-made `Client` implementations for both surfaces. See
that crate's README for the trait contract, connect-and-subscribe examples,
and the loss contract per transport.

## Running

```bash
cargo run -p datamancerd -- --config datamancerd.toml
# end-to-end tests need a live iceoryx2 runtime and are #[ignore]d:
cargo test -p datamancerd --test daemon_e2e -- --ignored
```

Provider credentials are **not** in the config. They live in the daemon-owned
credential store, provisioned over the control socket with `set-credentials`
(see [Credentials](#credentials)). The `account_type` selects paper vs. live
endpoints — and, for the deprecated env-var fallback only, which environment
pair is read at startup
(`paper` → `ALPACA_PAPER_API_KEY_ID`/`ALPACA_PAPER_API_SECRET_KEY`,
`live` → `ALPACA_LIVE_API_KEY_ID`/`ALPACA_LIVE_API_SECRET_KEY`).

### Single instance

Only one `datamancerd` runs per user on a host — **regardless of which config it
is given**. At startup, before loading config or opening any storage, the daemon
takes an exclusive advisory lock (`flock`) on a fixed, config-independent
lockfile:

- macOS: `~/Library/Application Support/datamancer/datamancerd.lock`
- Linux: `~/.local/share/datamancer/datamancerd.lock` (`$XDG_DATA_HOME` respected)

A second launch while one is running fails fast and exits non-zero with, e.g.:

```
another datamancerd is already running (pid 4321); single-instance lock held at \
<data dir>/datamancerd.lock
```

The lock is held for the whole process and released by the kernel on exit —
clean or not — so a crash leaves at most a harmless leftover lockfile that the
next start re-locks. The file's contents (the holder's PID) are diagnostic only;
the lock itself is authoritative. This is a **per-host, per-user** guarantee: it
does not coordinate across users or hosts.

### Config file location

`--config <path>` is **optional**. When given, that path is used verbatim: a
missing explicit path is an error (it is never scaffolded — you asked for a
specific file). When omitted, the daemon resolves the platform-native default:

- macOS: `~/Library/Application Support/datamancer/config.toml`
- Linux: `~/.config/datamancer/config.toml` (`$XDG_CONFIG_HOME` respected)

On first run at the **default path only**, if no file exists there, the daemon
creates the parent directory and atomically writes a commented starter config
(paper Alpaca provider, web UI enabled on `127.0.0.1:8080`, the control socket
left at its published default). Subsequent runs load the existing file
unchanged. Config-file writes (scaffolding and UI saves) are atomic: the new
contents land in a sibling `<path>.tmp` file that is fsynced then renamed over
the target, so a reader never observes a torn file.

## Config (TOML)

```toml
[provider.alpaca]
account_type = "paper"            # paper | live

[provider.alpaca_crypto]
account_type = "paper"
venue = "us"                      # us | us_kraken | eu_kraken

[cache]
backend = "embedded"              # embedded | memory
path = "/var/lib/datamancerd/cache.db"   # optional; default: <data dir>/cache.db

[tap_log]
backend = "embedded"
path = "/var/lib/datamancerd/taplog.db"  # optional; default: <data dir>/taplog.db

[session]
resume_buffer_events = 65536
adjustment = "all"                # raw | split | dividend | spin_off | all

[server]
# admin_socket defaults to the datamancer-owned well-known path
# ($XDG_RUNTIME_DIR/datamancer/control.sock on Linux,
# ~/Library/Application Support/datamancer/control.sock on macOS); set
# explicitly only to override. On a host with no home/runtime dir, the
# daemon falls back to /run/datamancer/control.sock, but a client's
# `default_control_socket()` cannot discover that path — configure it
# explicitly on both sides.
# admin_socket = "/run/datamancer/control.sock"
service_prefix = "datamancerd"
shutdown_timeout_secs = 30

[diagnostics]
publish_interval_ms = 1000
cache_catalog_interval_ms = 30000

[iceoryx2]
max_clients = 64                  # per-client data-plane service cap

[web_ui]                          # optional; requires the `web-ui` feature (default on)
enabled = false                   # off unless explicitly enabled
bind = "127.0.0.1"                # loopback only; a non-loopback bind is rejected
port = 8080
assets_dir = "/usr/share/datamancerd/ui"   # optional static assets (missing dir → warn)
live_state_cadence_ms = 1000      # fast cadence: live-state swap + SSE
cache_catalog_cadence_ms = 30000  # slow cadence: cache-catalog swap (/api/cache)

# Boot-time authoritative sessions held as lifecycle anchors.
[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"            # equity | crypto
symbol = "BTC/USD"
kind = "trade"                    # trade | quote | bar1s | bar1m | bar5m | bar15m | bar1h | bar1d
scope = "live"                    # live | live_backfill
backfill_from = "2026-06-01T00:00:00Z"   # required iff scope = live_backfill
persistence = "cached_with_tap"   # none | cached | cached_with_tap | read_only | refresh | tap_only
always_on = true                  # hold for the process lifetime regardless of clients

[log]
level = "info"                    # any tracing_subscriber::EnvFilter directive, e.g. "datamancerd=debug,info"
format = "text"                   # text | json
```

Compiled-in providers start disabled: the daemon boots with zero providers
configured, and providers are enabled at runtime via the config service's
`configure-provider` op (or by uncommenting a `[provider.*]` section and
restarting). Validation fails fast on the remaining cross-section invariants:
a startup session using a cache preset requires `[cache]`; one writing the
tap log requires `[tap_log]`; `scope = live_backfill` requires a parseable
`backfill_from`.

`[log]` configures the tracing subscriber installed at the very start of
`main` — before the single-instance lock or config-file resolution/scaffolding
run — via a best-effort, read-only peek of just the `[log]` section (a missing
file, an unreadable path, or unparseable TOML all silently fall back to the
defaults above; the real `Config::load` reports those problems properly once
the normal load path runs). **`RUST_LOG` always wins**: when set, it overrides
`level` entirely (the operator escape hatch for a one-off debug session
without touching the config file). `format = "json"` emits newline-delimited
JSON records instead of the default human-readable text.

For `embedded`, `path` is optional and defaults to the platform-native
data directory (`<data dir>` above): macOS
`~/Library/Application Support/datamancer`, Linux
`~/.local/share/datamancer` (`$XDG_DATA_HOME` respected), with `cache.db` and
`taplog.db` database files created on first use. Set `path` explicitly for a
system location like `/var/lib/datamancerd`, or on a headless host with no home
directory (where no default can be derived).

## Connection model

One **long-lived control connection per client**. The client names itself with
`open-client`; the daemon creates **one** iceoryx2 data-plane service for it
(`{service_prefix}/data/{id}`) and a per-client multiplexing session whose
output is pumped into that service. There is strictly **one sink per client**
(never shared), so per-client resume/gap accounting stays isolated.

A graceful `close-client` or a connection **EOF** tears the client down,
flushing its sink and releasing its authoritative refcounts.

### Lifecycle anchors

`always_on = true` startup sessions are held for the process lifetime regardless
of client presence, so the authoritative stream keeps running and recording
across client churn. Other startup sessions are refcount-driven: with the shared
authoritative registry they come up on first client subscribe and tear down at
the last referrer.

## Web introspection surface (feature `web-ui`)

An optional HTTP server, embedded in the daemon's shared tokio runtime, that
renders the introspection `SystemSnapshot` for a single same-host operator and
exposes one settings surface for the config file. It is otherwise a pure
consumer of the snapshot — the **same** snapshot the diagnostics plane carries
to client processes — and adds no new ordering, transport, or domain state.
Enable it with `[web_ui] enabled = true`.

> **Reach it at the literal bind address, not `localhost`.** The server binds a
> single address family (`bind = "127.0.0.1"` → IPv4 `127.0.0.1:8080`). On a
> dual-stack host `localhost` resolves to both `127.0.0.1` and `::1`, and a
> browser preferring IPv6 (Happy Eyeballs) can silently land on an unrelated
> service listening on `::1:<port>` — the bind still succeeds, so nothing looks
> wrong. Open `http://127.0.0.1:8080` explicitly, or change `port` if it
> collides. The startup log prints the exact URL to use.

> **Security boundary:** **loopback bind only** (a non-loopback `bind` is
> rejected at startup), **single-origin** (no CORS layer is added — never a
> permissive `Any` origin), `nosniff` + `Content-Security-Policy` response
> headers, and **one mutating route** — `PUT /api/config`, guarded by
> content-type (JSON only, 415 otherwise) and an Origin/Host same-origin check
> (403 otherwise). Every other route is `GET`-only. Auth is **deferred**:
> single operator, no network exposure. This mirrors the control-socket posture
> — **not** a network-safe surface.

The JSON contract **is** the `SystemSnapshot` `Serialize` output (shared with
the diagnostics plane); the section endpoints are pure projections of it. Two
independent refresh tasks publish into two `ArcSwap`s on independent cadences —
a fast live-state swap (`live_state_cadence_ms`) and a slow cache-catalog swap
(`cache_catalog_cadence_ms`) — both warmed before the listener binds, so a
handler never serves an empty snapshot and never invokes the on-demand
(potentially blocking) snapshot accessor.

| Route | Body |
| --- | --- |
| `GET /` | server-rendered operator page (button-less; live via SSE) |
| `GET /config` | server-rendered settings page (reads/writes the config file) |
| `GET /api/snapshot` | the entire live-state `SystemSnapshot` |
| `GET /api/cache` | cache catalog (slow swap): keys + ranges + est. bytes |
| `GET /api/providers` | provider accounting |
| `GET /api/sessions` | authoritative + client sessions (per-symbol) |
| `GET /api/health` | `{"ready": bool, "health": <core HealthView>}` — readiness (at least one enabled provider, all enabled providers `Connected`; disabled providers never block readiness) plus the full app-facing `HealthView` |
| `GET /api/stream` | SSE of the live-state envelope, one event per refresh |
| `GET /api/config` | the on-disk config file + restart-required flag + path |
| `PUT /api/config` | validate and atomically rewrite the config file |
| `GET /metrics` | Prometheus exposition (only with feature `metrics`) |

### Config settings surface (`GET`/`PUT /api/config`)

`GET /api/config` returns the config **as currently on disk** (so external
hand-edits show up, not just UI-driven ones), plus bookkeeping:

```jsonc
{"config": { /* full Config, same schema as the TOML file */ },
 "restart_required": false,
 "path": "/home/op/.config/datamancer/config.toml"}
```

**Secrets are redacted.** `[ws].auth_token` is never sent to a client: the
`GET`/`PUT` response replaces it with the placeholder `"<redacted>"` (like a
masked password field). On `PUT`, a body that submits the placeholder verbatim
**keeps the stored token unchanged** — the real value is restored from disk
before the write, so a UI round-trip (GET → edit → PUT) never clobbers the
secret and never flags a spurious restart. Submitting a different value rotates
the token; the literal placeholder is never persisted as a token.

`PUT /api/config` takes a full `Config` JSON body, validates it (the same
`Config::validate` the daemon runs at boot), and — on success — atomically
rewrites the file and returns the same envelope shape with the recomputed
`restart_required`. On failure nothing is written and the file is untouched.
Errors are `{"code": "...", "message": "..."}` with stable codes: `config`
(read/parse/validate/serialize failures, `422`/`500`) and `bad_request`
(missing/invalid JSON content-type → `415`, malformed JSON → `400`,
cross-origin write → `403`).

**`restart_required` semantics:** the daemon's runtime is immutable after boot
(apply-on-restart, no hot reload) — `Server::bootstrap` is handed an owned
`Config` clone and never re-reads the file. `restart_required` is **parsed**
config inequality between the on-disk file and the boot-time config, not a
byte diff: writing back exactly the boot config (even through a save that
drops comments) clears the flag. The same flag streams live over
`GET /api/stream`'s SSE envelope:

```jsonc
{"snapshot": { /* SystemSnapshot */ }, "restart_required": true}
```

so the `/config` settings page (and any other client watching the stream) can
show a restart banner without polling. The flag is only recomputed by
`GET`/`PUT /api/config`, not by the SSE stream itself: external hand-edits are
reflected after the next settings-page load (`GET /api/config`); the stream
does not re-stat the file itself.

**UI saves drop TOML comments.** The `/config` page's save button `PUT`s the
full validated config back; `Config::save` re-serializes it, so hand-written
comments in the file are lost on the first UI-driven save (values survive,
prose doesn't).

**Per-symbol presentation (load-bearing):** every ordered quantity (`seq`,
coverage, latency, gaps) is shown **per-`(instrument, kind)`**. The UI implies
**no** cross-symbol order: there is no global event count, stream position, or
merged sequence; `seq` is labelled per-symbol and `latency_ns` is observability
only (the sanctioned `rx_ts` use).

### Metrics (feature `metrics`)

Off by default until a scraper is deployed; usable independently of `web-ui`
(the recorder installs without the UI; the `/metrics` route registers only when
`web-ui` is also enabled). Per-symbol series are labelled per `(instrument,
kind)`, so cardinality is bounded by the number of actively-subscribed units.
The Prometheus recorder is **process-global and one-shot** — installed exactly
once at startup.

## Control transport (platform)

The control surface speaks the same newline-JSON vocabulary over a
platform-native, same-host endpoint. The transport differs; the protocol does
not.

- **Unix — Unix-domain socket.** Bound at `admin_socket` (default
  `$XDG_RUNTIME_DIR/datamancer/control.sock`). The socket is world-openable, so
  the privileged ops (credential ops, `configure-provider`/`remove-provider`,
  `shutdown`) are gated per-request on the connection's kernel-reported peer uid
  (`SO_PEERCRED`) matching the daemon's own effective uid.

- **Windows — owner-only-DACL named pipe.** Bound at `admin_socket` (default
  `\\.\pipe\datamancer\<user>\control`). Access control is the **pipe object's
  DACL**, not the name: the daemon builds it granting access to exactly one
  principal — its own process-token user SID (SDDL `D:P(A;;GA;;;<sid>)`;
  protected, no inherited `Everyone` ACE, no `SYSTEM` ACE). A *different* user
  therefore cannot open the pipe at all — the OS enforces same-user **at
  connect time** — so every accepted connection is already privileged (there is
  no per-op peer-cred check; the DACL is the gate). The first instance is
  created with `FILE_FLAG_FIRST_PIPE_INSTANCE`, so if another process squatted
  the control name first, the daemon **fails to start** rather than serve an
  unsecured surface (fail-closed). Any failure to resolve the SID or build the
  DACL likewise aborts startup.

  - **Client-side identity check (defense in depth).** Before sending anything
    privileged, `datamancer-client` verifies the connected pipe's **owner SID
    equals its own token SID**, so a bug that weakened the server DACL cannot
    silently leak credentials to a foreign or same-user-squatted endpoint.
  - **Integrity levels.** The DACL keys on the user SID, not the integrity
    level; Windows' mandatory "no-write-up" policy still applies. Run the daemon
    and its clients at the **same integrity level** (both elevated or both not):
    a medium-IL client may be denied write access to an elevated daemon's pipe.
  - **Scope.** The named-pipe *control* surface is native-Windows (#29). The
    same-host *data* plane (iceoryx2) does not yet run on Windows; a portable
    WS-loopback data path is a later phase.

## Control protocol (newline-JSON)

One JSON object per line; one reply line per request.

```jsonc
{"op":"open-client","client":"exec-1","subscriptions":[ /* SubscriptionSpec… */ ]}
  -> {"ok":true,"service":"datamancerd/data/0"}
{"op":"subscribe","client":"exec-1","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade","scope":"live","persistence":"cached_with_tap"}
{"op":"unsubscribe","client":"exec-1","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}
{"op":"close-client","client":"exec-1"}
{"op":"list-clients"}  -> {"ok":true,"clients":["exec-1"]}
{"op":"snapshot"}      -> {"ok":true,"snapshot":{ /* SystemSnapshot */ }}
{"op":"instruments","provider":"alpaca-crypto"}
  -> {"ok":true,"instruments":[{"instrument":{ /* Instrument */ },"live_kinds":["trade"],"history_kinds":[]}]}
{"op":"instruments"}  -> {"ok":true,"instruments":[ /* full catalog across all providers */ ]}
{"op":"capabilities","provider":"alpaca","symbols":["AAPL","MSFT"]}
  -> {"ok":true,"capabilities":[{"instrument":{ /* Instrument */ },"capabilities":{ /* InstrumentCapabilities, optional */ }}]}
{"op":"ping"}          -> {"ok":true,"version":"0.8.0","credential_backend":"keychain"}
{"op":"set-credentials","provider":"alpaca-crypto","credentials":{"type":"api_key_pair","key_id":"AK…","secret":"…"}}
  -> {"ok":true}
{"op":"get-credentials","provider":"alpaca-crypto"}
  -> {"ok":true,"credentials":{"type":"api_key_pair","key_id":"AK…","secret":"…"}}
{"op":"clear-credentials","provider":"alpaca-crypto"}
  -> {"ok":true}
{"op":"get-config"}
  -> {"ok":true,"config":{ /* full Config, same schema as the TOML file */ },"restart_required":false}
{"op":"configure-provider","provider":"alpaca","settings":{"account_type":"live"}}
  -> {"ok":true,"applied":"live"}
{"op":"remove-provider","provider":"alpaca"}
  -> {"ok":true,"applied":"live"}
{"op":"shutdown"}
  -> {"ok":true}
{"op":"health"}
  -> {"ok":true,"health":{ /* HealthView, schema_version 2, daemon-stamped */ }}
```

`instruments` enumerates the discoverable catalog and, per entry, the
`EventKind`s that instrument supports; `provider` is optional and restricts
the catalog to one provider (a full equities catalog is ~10k rows — prefer
the filter when you know the provider). Because it awaits a live provider
REST call, it is dispatched off the single-actor control loop (in the
per-connection task) so it cannot stall unrelated `open-client`/`subscribe`/
etc. traffic on other connections.

`capabilities` looks up on-demand, per-instrument order/fractional
capabilities (`InstrumentCapabilities`: things like fractionable, min order
size, supported order types/time-in-force) for a provider's symbols; `provider`
is required, `symbols` names the instruments to enrich. Like `instruments`, it
is an **ungated** read op and, because it awaits a live provider REST call, is
dispatched off the single-actor control loop so it cannot stall unrelated
traffic on other connections. The reply is a list of `InstrumentEntry`
(instrument + optional capabilities) — an absent/`None` field anywhere in a
capabilities block means **unknown**, never "unsupported" or "false"; a
provider that doesn't populate a field simply omits it rather than asserting
`false`. The lookup is keyed by provider + symbol, not by asset class: the
daemon builds each lookup with a placeholder `asset_class`, but a provider that
resolves the symbol **corrects it** on the returned `instrument`, so the echoed
`asset_class` is authoritative whenever capabilities come back populated (Alpaca
returns the real class — `crypto` for a crypto pair, `equity` for an equity). A
provider with no reference-data surface returns nothing to correct, so its rows
echo the placeholder class unchanged; treat the class as authoritative only
alongside populated capabilities.

`ping` needs no registered client and reports the daemon's crate version plus
the active credential-store backend; the app facade uses it for
spawn-readiness and version-skew detection.

`health` returns the app-facing `HealthView` (the versioned, per-symbol-only
reduction of `SystemSnapshot` — see `datamancer-core`'s `health.rs`), reduced
and stamped **daemon-side**: `daemon.version` and `daemon.credential_backend`
are filled from the daemon's own crate version and active credential backend,
not the client's. It is **ungated** (like `snapshot`) and **UDS-only** — there
is no WS equivalent; WS consumers reduce their own `snapshot` reply
client-side. `AppHandle::health()` sends this op rather than reducing
`snapshot` itself, so the `HealthView.schema_version` field is the daemon's,
not a client-side guess.

### `datamancer/health` push plane (iceoryx2)

Alongside the pull-style `health` op, the daemon periodically **publishes**
the same daemon-stamped `HealthView` on a dedicated iceoryx2 service,
`datamancer/health` — a single byte-slice pub-sub service, sibling to (and
sharing its wire cap with) `datamancer/diagnostics`. Cadence is
`[diagnostics] publish_interval_ms` (default 1000ms; the health/observability
e2e uses 200ms). Like the diagnostics service, it is capped at 1 MiB per
sample and built with `history_size(1)`, so a late-joining subscriber
receives the most recent view immediately rather than waiting a full cadence
tick. The service name is **host-global**, not scoped by `service_prefix` —
the same one-`datamancerd`-per-host constraint noted above for the
diagnostics service applies here too. `AppHandle::watch_health()` is the
supported subscriber: a same-host shared-memory subscription independent of
the control connection, returning an infallible `HealthStream`
(`tokio_stream::wrappers::ReceiverStream<HealthView>`) that never errors —
a setup failure ends the stream immediately instead.

`get-config`/`configure-provider`/`remove-provider`/`shutdown` are the
config-service ops (spec cycle 3); see the dedicated section below for
gating, classification, and semantics.

Errors reply `{"ok":false,"code":"…","message":"…"}` with **stable codes**
(`live_session_conflict`, `unsupported_event_kind`, `persistence_required`,
`unsupported_client_scope`, `duplicate_subscription`, `not_subscribed`,
`unknown_provider`, `unknown_client`, `duplicate_client`,
`service_cap_exceeded`, `bad_request`, `shutting_down`,
`credentials_missing`, `credential_backend_unavailable`, `permission_denied`,
`unknown_config_field`, …). These are an operator contract and are
regression-guarded. (`restart_required` is not an error code — it is the
boolean `get-config`/`get`/`PUT /api/config` field and the string value a
mutating config op's `applied` field carries when a cold-classified change
needs a restart to take effect.)

## Credentials

The daemon is the **one holder** of provider credentials: a single
daemon-owned credential store, provisioned and read over the control socket.
Nothing credential-shaped lives in the config file.

- **Store + backend selection.** The store opens at bootstrap: the OS
  keychain where it initializes, else a permissions-locked (`0600`) file at
  `<data dir>/credentials.json`. The choice is never silent — `ping` reports
  the active backend as `credential_backend` (`"keychain"`,
  `"secret-service"`, `"credential-manager"`, `"file"`). Setting the `DATAMANCER_CREDENTIALS_FILE`
  env var forces the file backend at that path — a testing/ops escape hatch
  (see `datamancer-credentials/README.md`), not a supported config surface.
- **Local-control-only, same-user gated.** The three credential ops exist
  **only** on the local control surface (UDS/named pipe) — never on the WS
  surface (its frame vocabulary simply has no such ops). Same-user enforcement
  is platform-native: on Unix each credential op checks the connection's
  kernel-reported peer uid (`SO_PEERCRED`/`getpeereid`) against the daemon's own
  effective uid, and a mismatch — or an unreadable peer — gets
  `permission_denied`; on Windows the pipe's owner-only DACL already restricts
  connect to the daemon's user, so the op runs already-authorized (see
  [Control transport](#control-transport-platform)). Other control ops are
  unaffected by the gate.
- **Hot-apply.** `set-credentials` persists to the store, then applies live:
  the running provider reconnects its stream with the new credentials and
  rebuilds its REST clients on next use. No restart, no resubscribe — the
  session and its `seq` stream carry across the rotation (consumers see the
  usual in-band reconnect controls).
- **Clear does not un-apply.** `clear-credentials` empties the store only. A
  running provider keeps its last applied credentials until the daemon
  restarts — there is deliberately no live revocation half-state.
- **Why `get-credentials` exists.** Consuming apps (e.g. a trading process)
  reuse the same keys for their own provider connections; the daemon's store
  is the single copy they read instead of keeping a second one. It returns
  the stored value, so it is exactly what `set-credentials` persisted.
- **Env vars are deprecated.** When the store has no entry for a configured
  provider at startup, the daemon falls back to the provider's
  `ALPACA_{PAPER,LIVE}_API_*` pair and logs a deprecation warning (naming the
  provider, never values). Provision via `set-credentials`; the fallback will
  be removed once the broker is proven.
- **No credentials at start.** With an empty store and no env pair, the
  provider starts **parked**: it emits no connectivity control, REST-backed
  ops (`instruments`, historical fetch) fail provider-unavailable, and live
  subscribes fail until `set-credentials` arrives — at which point it
  connects without a restart. The bootstrap log states clearly which
  providers started unprovisioned.
- **A store must open, or the daemon won't boot.** On a host with no
  reachable keychain/secret-service **and** no derivable home directory
  (e.g. a minimal service unit without `HOME`), `open_default` has nowhere
  to put the file fallback and bootstrap fails fast with a
  `credential store: …` error — even if `ALPACA_*` env vars are set. Give
  the service a `HOME` (or a secret-service) to restore the pre-0.3 env-only
  behavior.

## Config service

Four control-socket ops (spec cycle 3) let an operator or the app facade
read and mutate the daemon's runtime config without hand-editing the file or
restarting — the daemon is the **sole runtime writer** of its own config. A
hand edit made while the daemon is up is not just deferred to the next
restart: it is silently **at risk of being overwritten**. The hub persists
candidates derived from its own in-memory state, not from the file, so the
next mutating op (`configure-provider`/`remove-provider`/the web UI's `PUT`)
writes the hub's view of the config back to disk and clobbers the hand edit
(last-write-wins, by design). Only a restart picks up a hand edit safely.

> **Provider id vs. section key.** The ops' `provider` field takes the
> provider *id* (`alpaca`, `alpaca-crypto`), while the config JSON/TOML
> section keys are the corresponding *TOML section names*
> (`provider.alpaca`, `provider.alpaca_crypto`) — note the hyphen/underscore
> mismatch for the crypto provider.

```jsonc
{"op":"get-config"}
  -> {"ok":true,"config":{ /* full Config, same schema as the TOML file */ },"restart_required":false}
{"op":"configure-provider","provider":"alpaca","settings":{"account_type":"live"}}
  -> {"ok":true,"applied":"live"}
{"op":"remove-provider","provider":"alpaca"}
  -> {"ok":true,"applied":"live"}
{"op":"shutdown"}
  -> {"ok":true}
```

- **Gating.** `get-config` is ungated — any local connection can read it,
  same posture as `snapshot`/`list-clients`. Credentials never live in the
  config; the one secret-shaped field, `[ws].auth_token`, is redacted
  (replaced with a fixed placeholder) in every `get-config` reply, so
  ungating it does not leak the token. `configure-provider`,
  `remove-provider`, and `shutdown` are **mutating** and gated exactly like
  the credential ops: the connection's kernel-reported peer uid must match
  the daemon's own effective uid (`permission_denied` otherwise). None of
  the four ops exist on the WS client surface — its frame vocabulary has no
  config-service equivalent.
- **Hot/cold classification.** Whether a change applies live or needs a
  restart is determined by a per-dotted-path classification table in
  `crates/datamancerd/src/config_class.rs` (`CLASSIFICATION`, matched by
  longest prefix) — that module is the single source of truth for "is field
  X hot or cold." Currently **all** `provider.*` fields (including a
  section's presence/absence) are `Hot`; every other top-level section
  (`cache`, `tap_log`, `session`, `server`, `diagnostics`, `iceoryx2`,
  `web_ui`, `ws`, `startup_session`) is `Cold`. A build-time exhaustiveness
  test fails if a new config field is added without a classification entry.
- **`configure-provider`/`remove-provider` always apply live today.** Because
  `provider.*` is Hot, both ops persist-then-apply in one step and their
  `applied` field is currently always `"live"` — `"restart_required"` is
  reserved for a future Cold-classified mutating op (or the web UI's
  full-config `PUT`, which is where `restart_required` is actually observed
  today, via divergence against Cold sections).
- **Disabled-until-configured posture.** Every compiled-in provider is
  constructed at boot (parked, not literally absent) but starts **disabled**
  unless its `[provider.*]` section is present; a scaffolded/empty config
  enables nothing, so a fresh boot with zero provider sections is a valid,
  well-defined state (`get-config` returns a fixed-key `provider` table with
  both provider ids present and `null` values, e.g.
  `"provider":{"alpaca":null,"alpaca_crypto":null}`).
  `configure-provider` enables a provider at runtime; uncommenting the
  section and restarting does the same thing at boot.
- **`remove-provider` leaves credentials stored.** It only removes the
  config section (disabling the provider); the credential store is a
  separate lifecycle (see Credentials above) — re-enabling the same
  provider later reuses whatever credentials are already on file.
- **`shutdown` runs the full drain.** It is not a bare process exit: the
  same ordered drain described under Shutdown below runs (tap-log flush
  before sink flush before service drop), gated same-uid like the other
  mutating ops. The `{"ok":true}` reply is sent via a oneshot before the
  drain starts and is delivered best-effort — the connection task writes it
  out while the drain runs concurrently, so a slow/interrupted connection
  may not see it even though the daemon still shuts down cleanly.

## WebSocket client surface (feature `ws`)

An optional remote client transport: a TCP WebSocket listener where **one
connection is one client** (no `open-client` request — connecting implicitly
opens it). It reuses the UDS control vocabulary and `codes` table but is a
genuinely separate, network-reachable, mutating surface from the loopback
read-only web UI — treat its security posture independently. Gate it behind
the `ws` cargo feature (off by default) and `[ws] enabled = true` in config.
It is one of two worked client-transport examples alongside the iceoryx2 data
plane; see `crates/datamancer-transport-ws/README.md` for the wire format.

Clients must offer the event-frame wire version as the WebSocket subprotocol
(`Sec-WebSocket-Protocol: datamancer.v2`); the handshake is rejected with 400
otherwise, and the daemon echoes the token on acceptance. This is what keeps a
client built against a different wire version from silently misreading the
raw fixed-point size/volume fields (see the transport README for the version
history and the 64-bit-integer parsing requirement).

```toml
[ws]                               # optional; requires the `ws` feature (off by default)
enabled = false                    # off unless explicitly enabled
bind = "127.0.0.1"                 # loopback default; can be bound off-loopback
port = 9001
auth_token = "change-me"           # optional shared bearer token; omit to disable auth
channel_depth = 1024               # bounded per-connection outbound channel
max_connections = 64               # hard cap on concurrent clients; accepts past this close immediately
keepalive_secs = 30                # reserved; see caveat below
```

> **Security:** this surface is mutating (subscribe/unsubscribe/close-client)
> and, unlike the web UI, may be bound off-loopback. `auth_token`, when set,
> is checked as a bearer token at the WS handshake
> (`Authorization: Bearer <token>`); a missing or wrong token gets an HTTP 401
> before the WS upgrade completes. Running with no `auth_token` logs a
> warning, louder when `bind` is not loopback (unauthenticated remote
> access). TLS is **out of scope**: terminate it at a reverse proxy if the
> deployment needs it. This is a worked example of a remote client surface,
> not yet a hardened public endpoint.

> **Operational note:** run at most **one** `datamancerd` per host. Two
> daemons on the same host collide on the host-global diagnostics iceoryx2
> single-publisher service regardless of whether `ws` is enabled — this is a
> pre-existing daemon-wide constraint, not specific to the WS surface.

`keepalive_secs` is **reserved**: the daemon does not currently send
server-initiated pings. `tokio-tungstenite` auto-pongs inbound client pings,
so a client that pings on its own schedule gets working keepalive today;
server-initiated keepalive is not yet implemented.

Control frames are JSON, tag field `op`, kebab-case, each carrying a
correlation `id` that the reply echoes (there is no `client` field — the
connection identifies the client):

```jsonc
{"id":1,"op":"subscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade","scope":"live","persistence":"cached_with_tap"}
  -> {"id":1,"ok":true}
{"id":2,"op":"snapshot"}
  -> {"id":2,"ok":true,"snapshot":{ /* SystemSnapshot */ }}
{"id":3,"op":"unsubscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}
{"id":4,"op":"close-client"}
  -> {"id":4,"ok":true}
{"id":5,"op":"instruments","provider":"alpaca-crypto"}
  -> {"id":5,"ok":true,"instruments":[{"instrument":{ /* Instrument */ },"live_kinds":["trade"],"history_kinds":[]}]}
{"id":6,"op":"capabilities","provider":"alpaca","symbols":["AAPL","MSFT"]}
  -> {"id":6,"ok":true,"capabilities":[{"instrument":{ /* Instrument */ },"capabilities":{ /* InstrumentCapabilities, optional */ }}]}
```

Like on the UDS surface, `instruments` (optional `provider` filter) and
`capabilities` (required `provider` + `symbols`) are dispatched per-connection
rather than through any shared actor, so they never block other connections
while they await a live provider REST call.

### `instruments` rows: `live_kinds` and `history_kinds`

Each catalog row reports the kinds a provider streams and the kinds it can
backfill **separately**. The two genuinely differ and neither contains the
other: Alpaca equities stream only minute and daily bars while backfilling five
intervals, and a provider may stream a kind it cannot backfill at all.

There is no combined `kinds` field. Compute the union client-side if you want
it — a single list is what let a consumer read "quotes are supported" and
conclude it could backfill quotes, which is the ambiguity this split removes.

**Breaking in 0.8.0.** Rows before 0.8.0 carried one `kinds` array. Neither new
field has a default, so a pre-0.8.0 payload now fails to parse rather than
silently deserializing as "supports nothing." `datamancer-client` and
`datamancerd` bump in lockstep and the `ping` version gate rejects the skew.

Errors reuse the **same stable `codes` table** as the UDS control surface
(`{"id":5,"ok":false,"code":"unsupported_event_kind","message":"…"}`). Event
frames (JSON, tag field `type`: `trade`/`quote`/`bar`/`gap`/
`subscription_changed`/`session_closing`) and control replies share the one
outbound socket, ordered by a single per-connection writer task — a client
distinguishes them by the presence of `type` vs. `id`/`ok`. Connection-scoped
controls (provider connect/disconnect/error) are **not** carried on the event
stream; read connectivity via the `snapshot` reply instead.

Backpressure is bounded and lossy-on-overrun **by disconnection**: if a remote
consumer falls behind and the connection's outbound channel
(`channel_depth`) fills, the connection is torn down rather than silently
dropping frames on a live connection.

Graceful daemon shutdown tears down live WS connections before the UDS-client
drain: each connection's `session.close()` emits a terminal
`session_closing` frame, the pump drains it under a bound, then the socket
gets a clean WS Close frame — honoring the same tap-log-flush-before-drop
ordering as every other consumer.

## Shutdown

SIGTERM/SIGINT triggers a bounded, serialized drain: drain the web server (if
enabled) → **stop accepting new WS connections and tear down in-flight ones**
(feature `ws`) → stop accepting control requests → stop the diagnostics ticker
→ per client close the session and drain its pump (so a terminal
`SessionClosing` reaches the sink instead of being severed) → drop the startup
anchors → **flush the tap log** (the durable record) → per client flush the
sink → drop the clients/sinks. The web server (and WS surface) are drained
first so they stop reporting on / mutating a data plane about to be torn down;
the tap log flushes **before** the best-effort per-client sink flushes (the
load-bearing tap-log-before-sink-flush contract) so a stall in those
best-effort steps cannot lose durable writes. The whole drain is bounded by
`server.shutdown_timeout_secs` so a disk-stalled tap-log flush cannot hang
shutdown forever (it is logged and the process force-exits).
