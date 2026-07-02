# datamancerd

The standalone **datamancer server**: a thin binary that wraps the `datamancer`
library and serves multiple consumer processes on the **same host**. It adds no
new ordering, transport, or event-model semantics — its job is composition,
process lifecycle, and a control surface.

Embedders who want zero hops should keep using the library in-process. Reach for
`datamancerd` when several processes on one machine need to share authoritative
sessions (and their recording) and read a single multiplexed stream each.

> **Security:** the control surface is a Unix-domain socket guarded by
> **filesystem permissions only**. There is no authentication and no network
> transport. This is **not** a network-safe surface; run it same-host,
> single-operator.

> **Ordering:** determinism is **per symbol** only. The daemon computes **no**
> cross-instrument or global order. Two clients of the same instrument observe
> byte-identical `(seq, source_ts)` because they share the one authoritative
> per-`(instrument, kind)` session.

## Running

```bash
cargo run -p datamancerd -- --config datamancerd.toml
# end-to-end tests need a live iceoryx2 runtime and are #[ignore]d:
cargo test -p datamancerd --test daemon_e2e -- --ignored
```

Provider credentials are **not** in the config. The `account_type` selects which
environment credential pair `oxidized_alpaca` loads
(`paper` → `ALPACA_PAPER_API_KEY_ID`/`ALPACA_PAPER_API_SECRET_KEY`,
`live` → `ALPACA_LIVE_API_KEY_ID`/`ALPACA_LIVE_API_SECRET_KEY`).

### Config file location

`--config <path>` is **optional**. When given, that path is used verbatim: a
missing explicit path is an error (it is never scaffolded — you asked for a
specific file). When omitted, the daemon resolves the platform-native default:

- macOS: `~/Library/Application Support/datamancerd/config.toml`
- Linux: `~/.config/datamancerd/config.toml` (`$XDG_CONFIG_HOME` respected)

On first run at the **default path only**, if no file exists there, the daemon
creates the parent directory and atomically writes a commented starter config
(paper Alpaca provider, web UI enabled on `127.0.0.1:8080`, a user-writable
admin socket next to the config file). Subsequent runs load the existing file
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
backend = "surreal-embedded"      # surreal-embedded | surreal-memory
path = "/var/lib/datamancerd/cache"   # optional; default: <data dir>/cache

[tap_log]
backend = "surreal-embedded"
path = "/var/lib/datamancerd/taplog"  # optional; default: <data dir>/taplog

[session]
resume_buffer_events = 65536
adjustment = "all"                # raw | split | dividend | spin_off | all

[server]
admin_socket = "/run/datamancerd/admin.sock"
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
```

Validation fails fast: at least one provider must be configured; a startup
session using a cache preset requires `[cache]`; one writing the tap log
requires `[tap_log]`; `scope = live_backfill` requires a parseable
`backfill_from`.

For `surreal-embedded`, `path` is optional and defaults to the platform-native
data directory (`<data dir>` above): macOS
`~/Library/Application Support/datamancerd`, Linux
`~/.local/share/datamancerd` (`$XDG_DATA_HOME` respected), with `cache` and
`taplog` subdirectories created on first use. Set `path` explicitly for a
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
| `GET /api/health` | process-up + per-provider connection rollup |
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
 "path": "/home/op/.config/datamancerd/config.toml"}
```

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
```

Errors reply `{"ok":false,"code":"…","message":"…"}` with **stable codes**
(`live_session_conflict`, `unsupported_event_kind`, `persistence_required`,
`unsupported_client_scope`, `duplicate_subscription`, `not_subscribed`,
`unknown_provider`, `unknown_client`, `duplicate_client`,
`service_cap_exceeded`, `bad_request`, `shutting_down`, …). These are an
operator contract and are regression-guarded.

## Shutdown

SIGTERM/SIGINT triggers a bounded, serialized drain: drain the web server (if
enabled) → stop accepting control requests → stop the diagnostics ticker → per
client close the session and drain its pump (so a terminal `SessionClosing`
reaches the sink instead of being severed) → drop the startup anchors → **flush
the tap log** (the durable record) → per client flush the sink → drop the
clients/sinks. The web server is drained first so it stops reporting on a data
plane about to be torn down; the tap log flushes **before** the best-effort
per-client sink flushes (the load-bearing tap-log-before-sink-flush contract) so
a stall in those best-effort steps cannot lose durable writes. The whole drain
is bounded by `server.shutdown_timeout_secs` so a disk-stalled tap-log flush
cannot hang shutdown forever (it is logged and the process force-exits).
