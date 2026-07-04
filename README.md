# Datamancer

A unified subscription and replay layer for financial market data — usable two ways:

- **As a library** (`datamancer`), compiled into your process. Talk to a provider, get a
  normalized, multiplexed stream of typed `MarketEvent`s, with historical read-through
  caching, a live tap log, and resume built in.
- **As a standalone server** (`datamancerd`), a same-host daemon that holds authoritative
  sessions alive and fans each client's multiplexed stream out to a separate consumer
  process over a zero-copy [iceoryx2](https://iceoryx.io/) transport.

The library is primary; the server is a thin wrapper that adds composition, process
lifecycle, and a control surface — **no** new ordering, transport, or event semantics.

> **Status:** early-stage. The public API is co-evolving with its first consumers; expect
> breaking changes until it stabilizes. A license has not yet been selected.

## Core design rule: per-symbol determinism

Ordering is **per symbol only**. Each instrument's substream is a source-stamped,
within-instrument total order — the key is `(instrument, seq)`. Across instruments the
multiplexed stream *interleaves* in arrival order; it does **not** compute a global,
cross-symbol order, and a globally merge-sorted stream is an explicit non-goal. Two
consumers of the same instrument observe byte-identical `(seq, source_ts)` because they
share one authoritative per-`(instrument, kind)` session.

Every data event carries three distinct timestamp/identity fields:

| Field | Role |
| --- | --- |
| `source_ts` | provider-reported market time — the **only** field engine logic should reason about |
| `seq: u64` | per-symbol ordering, stamped **once at the source**; the sole ordering field, identical across consumers |
| `rx_ts` | wall-clock at byte receipt — **observability only**, never feeds engine logic |

## Workspace layout

Cargo workspace (resolver 3, edition 2024), `#![forbid(unsafe_code)]` in every crate.

| Crate | What it is |
| --- | --- |
| [`datamancer-core`](crates/datamancer-core) | Pure types + trait surface (`Provider`, `LiveHandle`, `HistoricalCache`, `EventSink`, …) and the event model. No I/O. |
| [`datamancer`](crates/datamancer/README.md) | The session orchestrator. Re-exports core, adds `Datamancer`, provider integrations, and storage backends behind features. |
| [`datamancer-transport-iceoryx2`](crates/datamancer-transport-iceoryx2) | Optional same-host zero-copy iceoryx2 transport (data + diagnostics planes). |
| [`datamancerd`](crates/datamancerd/README.md) | The standalone server binary: TOML config, Unix-socket control surface, optional web UI. |

**Read the crate READMEs for the authoritative design docs** — [`crates/datamancer/README.md`](crates/datamancer/README.md)
(library design, event model, persistence, transports) and
[`crates/datamancerd/README.md`](crates/datamancerd/README.md) (operator contracts: config
schema, control protocol, error codes).

## Build, test, lint

```bash
cargo build                                              # workspace, default features
cargo test                                               # all unit + integration tests (skips #[ignore])
cargo clippy --all-targets -- -D warnings
cargo fmt
```

Default features are `provider-alpaca` + `storage-turso`; `transport-iceoryx2` is **off
by default**. Ignored tests need live resources:

```bash
cargo test --test alpaca_real -- --ignored               # hits real Alpaca; needs credentials
cargo test -p datamancer-transport-iceoryx2 -- --ignored # needs a live iceoryx2 runtime
cargo test -p datamancerd --test daemon_e2e -- --ignored # spawns the binary + iceoryx2 runtime
```

## Using the library

```rust
use datamancer::{Datamancer, PersistenceOptions};

let dm = Datamancer::builder()
    .provider_arc(provider)
    .historical_cache(Box::new(TursoCache::open(TursoCacheConfig::embedded("./cache.db")).await?))
    .build()?;

let mut session = dm
    .session(instrument, kind, scope, PersistenceOptions::cached())
    .await?;

while let Some(event) = session.events().next().await {
    // … one multiplexed, per-symbol-deterministic stream of MarketEvent
}
```

Runnable, credential-free demos live in [`crates/datamancer/examples`](crates/datamancer/examples):

```bash
cargo run --example crypto_ticker     # live crypto trades (needs Alpaca creds in env)
cargo run --example cached_history    # historical read-through cache
cargo run --example client_session    # multiplexed client session over several symbols
cargo run --example resume            # drop and re-take a live stream
cargo run --example tap_replay        # replay from a tap log
```

## Running the server

```bash
cargo run -p datamancerd -- --config datamancerd.toml
```

A minimal config (full schema in [`crates/datamancerd/README.md`](crates/datamancerd/README.md)):

```toml
[provider.alpaca_crypto]
account_type = "paper"            # paper | live
venue = "us"

# Repo-local paths so the sample runs without root; use system paths
# (/var/lib/datamancerd, /run/datamancerd) in a real deployment.
[cache]
backend = "embedded"
path = "./.datamancerd/cache.db"

[tap_log]
backend = "embedded"
path = "./.datamancerd/taplog.db"

[server]
admin_socket = "./.datamancerd/admin.sock"
service_prefix = "datamancerd"
shutdown_timeout_secs = 30

[web_ui]                           # optional read-only introspection UI (feature web-ui, default on)
enabled = false
bind = "127.0.0.1"                 # loopback only; a non-loopback bind is rejected
port = 8080

[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"
symbol = "BTC/USD"
kind = "trade"
scope = "live"
persistence = "cached_with_tap"
always_on = true
```

### Credentials

Provider credentials are **not** in the config — `account_type` selects which environment
credential pair is loaded:

| `account_type` | Env vars |
| --- | --- |
| `paper` | `ALPACA_PAPER_API_KEY_ID`, `ALPACA_PAPER_API_SECRET_KEY` |
| `live` | `ALPACA_LIVE_API_KEY_ID`, `ALPACA_LIVE_API_SECRET_KEY` |

### Clients

Clients connect over the admin Unix socket and speak newline-delimited JSON
(`open-client` → `subscribe`/`unsubscribe` → `close-client`); the daemon creates one
iceoryx2 data-plane service per client and pumps that client's multiplexed stream into it.
See the [control protocol](crates/datamancerd/README.md#control-protocol-newline-json) for
the full op set and the stable error codes.

> **Security:** both the control socket and the optional web UI are **same-host,
> single-operator** surfaces — filesystem-permission-guarded socket, loopback-only UI, no
> authentication, no network transport. Do not expose them to a network.

## Features at a glance

| Crate | Feature | Default | Purpose |
| --- | --- | :-: | --- |
| `datamancer` | `provider-alpaca` | ✅ | Alpaca provider integration |
| `datamancer` | `storage-turso` | ✅ | Turso (embedded SQLite-compatible) cache + tap-log backend |
| `datamancer` | `transport-iceoryx2` | — | Same-host zero-copy transport |
| `datamancerd` | `web-ui` | ✅ | Embedded read-only introspection UI + JSON API |
| `datamancerd` | `metrics` | — | Prometheus `/metrics` endpoint |

## What Datamancer does *not* do

It produces events; it is not an analysis framework, a general-purpose time-series store,
or a cross-venue reconciler. There is no semantic enrichment, no source-timestamp
re-sorting, no wall-clock-paced replay, and no cross-symbol/global ordering. Consumers that
need any of those build them on top.
