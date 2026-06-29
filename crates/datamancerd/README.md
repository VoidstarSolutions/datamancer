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
(`paper` → `ALPACA_PAPER_API_KEY_ID`/`SECRET`, `live` → `ALPACA_API_KEY_ID`/…).

## Config (TOML)

```toml
[provider.alpaca]
account_type = "paper"            # paper | live

[provider.alpaca_crypto]
account_type = "paper"
venue = "us"                      # us | us_kraken | eu_kraken

[cache]
backend = "surreal-embedded"      # surreal-embedded | surreal-memory
path = "/var/lib/datamancerd/cache"

[tap_log]
backend = "surreal-embedded"
path = "/var/lib/datamancerd/taplog"

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

[web_ui]                          # optional; served in Phase 6
enabled = false
bind = "127.0.0.1"
port = 8080
assets_dir = "/usr/share/datamancerd/ui"
live_state_cadence_ms = 1000
cache_catalog_cadence_ms = 30000

# Boot-time authoritative sessions held as lifecycle anchors.
[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"            # equity | crypto
symbol = "BTC/USD"
kind = "trade"                    # trade | quote | bar_1s | bar_1m | bar_5m | bar_15m | bar_1h | bar_1d
scope = "live"                    # live | live_backfill
backfill_from = "2026-06-01T00:00:00Z"   # required iff scope = live_backfill
persistence = "cached_with_tap"   # none | cached | cached_with_tap | read_only | refresh | tap_only
always_on = true                  # hold for the process lifetime regardless of clients
```

Validation fails fast: at least one provider must be configured; a startup
session using a cache preset requires `[cache]`; one writing the tap log
requires `[tap_log]`; `scope = live_backfill` requires a parseable
`backfill_from`.

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

SIGTERM/SIGINT triggers a bounded, serialized drain: stop accepting control
requests → stop the diagnostics ticker → per client flush the sink then close
the session → drop the startup anchors → flush the tap log. The whole drain is
bounded by `server.shutdown_timeout_secs` so a disk-stalled tap-log flush cannot
hang shutdown forever (it is logged and the process force-exits).
