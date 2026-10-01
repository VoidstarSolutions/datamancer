# Datamancer

A unified subscription and replay layer for financial market data — usable two ways:

- **As a library** (`datamancer`), compiled into your process. Talk to a provider, get a
  normalized, multiplexed stream of typed `MarketEvent`s, with historical read-through
  caching, a live tap log, and resume built in.
- **As a standalone server** (`datamancerd`), a same-host daemon that holds authoritative
  sessions alive and fans each client's multiplexed stream out to a separate consumer
  process — over zero-copy [iceoryx2](https://iceoryx.io/) shared memory on Unix, or
  WebSocket-over-loopback on Windows.

The library is primary; the server is a thin wrapper that adds composition, process
lifecycle, and a control surface — **no** new ordering, transport, or event semantics.

> **Status:** early-stage (`0.9.x`). The public API is co-evolving with its first
> consumers; expect breaking changes until it stabilizes. Pre-1.0, a `feat!:` bump is a
> *minor* release, so **every minor is potentially breaking**. A license has not yet been
> selected — see [Licensing](#licensing).

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

Loss is never silent: an evicted or missed span is surfaced in-band as `Control::Gap`, so
a delivered stream is contiguous only while nothing was lost.

## Workspace layout

Cargo workspace (resolver 3, edition 2024), eight crates.

| Crate | What it is |
| --- | --- |
| [`datamancer-core`](crates/datamancer-core) | Pure types + trait surface (`Provider`, `LiveHandle`, `HistoricalCache`, `EventSink`, …), the event model, `ProviderCredentials`, and the app-facing `HealthView`. No I/O. |
| [`datamancer`](crates/datamancer/README.md) | The session orchestrator. Re-exports core, adds `Datamancer`, provider integrations, and storage backends behind features. |
| [`datamancer-transport-iceoryx2`](crates/datamancer-transport-iceoryx2) | Optional same-host zero-copy iceoryx2 transport (data + diagnostics planes). |
| [`datamancer-transport-ws`](crates/datamancer-transport-ws/README.md) | Optional remote WebSocket client transport (one connection = one client; JSON frames, no interning). |
| [`datamancer-client`](crates/datamancer-client/README.md) | Consumer-side crate: the control vocabulary (`spec`, `codes`, `protocol`) plus two implementations of one generic `Client` trait, and the `app` facade (`AppHandle::ensure`). |
| [`datamancer-credentials`](crates/datamancer-credentials/README.md) | Credential storage — OS keychain / secret-service with a locked-down file fallback. Synchronous by design. |
| [`datamancer-winsec`](crates/datamancer-winsec) | Shared Windows security primitives: token/handle identity and process integrity level. The workspace's single audited `unsafe` surface for these. |
| [`datamancerd`](crates/datamancerd/README.md) | The standalone server binary: TOML config, local control surface, credential broker, config service, health plane, optional web UI and WS client surface. |

**Read the crate READMEs for the authoritative design docs** — [`crates/datamancer/README.md`](crates/datamancer/README.md)
(library design, event model, persistence, transports) and
[`crates/datamancerd/README.md`](crates/datamancerd/README.md) (operator contracts: config
schema, control protocol, error codes).

### `unsafe` policy

`#![forbid(unsafe_code)]` is the default in every crate. Three audited exceptions exist,
each scoped to a single module with a `// SAFETY:` proof on every block:

| Crate | Policy | Why |
| --- | --- | --- |
| `datamancer-winsec` | `deny` on Windows, one scoped `allow` in `ffi` | Win32 token/handle identity and integrity reads |
| `datamancer-transport-iceoryx2` | scoped allow | confirms `ZeroCopySend` is a safe derive |
| `datamancerd` | `deny` on **Windows only**, one scoped allow in `win_control` | named-pipe creation + SDDL |

Every non-Windows build of every crate stays `forbid`. `datamancer-client` is `forbid` on
**every** platform — its Windows pipe checks delegate all FFI to `datamancer-winsec`.

## Build, test, lint

```bash
cargo build                                              # workspace, default features
cargo test                                               # all unit + integration tests (skips #[ignore])
cargo clippy --all-targets -- -D warnings
cargo fmt
```

Ignored tests need live resources:

```bash
cargo test --test alpaca_real -- --ignored               # hits real Alpaca; needs credentials
cargo test -p datamancer-transport-iceoryx2 -- --ignored # needs a live iceoryx2 runtime
cargo test -p datamancerd --test daemon_e2e -- --ignored # spawns the binary + iceoryx2 runtime
```

Before opening a PR, run the CI gates that only fail in CI:

```bash
git fetch origin main
cargo deny check                                         # licenses, advisories, sources
.github/scripts/semver-checks.sh origin/main             # needs cargo-semver-checks
```

Branch shape is **semi-linear**: rebase onto `main`, never merge `main` in, and queue one
PR at a time. Versioning is owned by release-plz — never bump `[workspace.package] version`
by hand. See [`RELEASING.md`](RELEASING.md).

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

Runnable demos live in [`crates/datamancer/examples`](crates/datamancer/examples):

```bash
cargo run --example crypto_ticker     # live crypto trades (needs Alpaca credentials)
cargo run --example cached_history    # historical read-through cache
cargo run --example client_session    # multiplexed client session over several symbols
cargo run --example resume            # drop and re-take a live stream
cargo run --example tap_replay        # replay from a tap log
```

## Running the server

```bash
cargo run -p datamancerd -- --config datamancerd.toml
```

`--config` is optional; omitted, the daemon resolves a platform-native default path and
scaffolds a commented starter config on first run. A minimal config (full schema in
[`crates/datamancerd/README.md`](crates/datamancerd/README.md)):

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

Every compiled-in provider is constructed at boot but starts **disabled** unless its
`[provider.*]` section is present. Providers are enabled and disabled at runtime through
the config service (`configure-provider` / `remove-provider`) with no restart — the daemon
is the sole runtime writer of its own config.

### Credentials

Provider credentials are **not** in the config file. The daemon owns a credential broker
backed by the OS keychain (or a locked-down file fallback); provision through the control
surface:

```jsonc
{"op":"set-credentials","provider":"alpaca-crypto",
 "credentials":{"type":"api_key_pair","key_id":"AK…","secret":"…"}}
```

Credentials hot-apply to running providers. `ProviderCredentials` is tagged per provider
*shape*, not a universal key/secret pair — `api_key_pair` today, with a secret-free
`gateway` shape (host / port / client id) reserved for companion-process providers.

> **Deprecated:** the `ALPACA_PAPER_*` / `ALPACA_LIVE_*` environment fallback is still read
> at bootstrap when the store is empty, but it warns. Use the broker
> ([#54](https://github.com/VoidstarSolutions/datamancer/issues/54)).

### Clients

Clients speak newline-delimited JSON over a platform-native local endpoint — a Unix socket
on macOS/Linux, an owner-only-DACL named pipe on Windows. The op set covers streaming
(`open-client` → `subscribe`/`unsubscribe` → `close-client`), bounded historical queries
(`open-query` / `cancel-query` / `list-queries`), discovery (`instruments`,
`capabilities`), credentials, config, and `health`. Privileged ops are same-uid gated
(peer-cred on Unix, pipe owner SID plus an integrity floor on Windows).

Rather than hand-rolling the wire protocol, depend on
[`datamancer-client`](crates/datamancer-client/README.md) — its `app` feature's
`AppHandle::ensure` does find-or-spawn-and-connect with a version gate, typed `HealthView`
health, and credential provisioning.

See the [control protocol](crates/datamancerd/README.md#control-protocol-newline-json) for
the full op set and the stable error codes. **Match errors on the stable `codes` strings,
never on message text.**

> **Security:** the control surface and the optional web UI are **same-host,
> single-operator** surfaces — permission-guarded socket or owner-only pipe, loopback-only
> UI. The optional WS surface has an optional bearer token but no TLS. Datamancer is not
> yet a hardened public endpoint; do not expose it to a network.

### Platform differences

| | macOS / Linux | Windows |
| --- | --- | --- |
| Control surface | Unix domain socket (peer-cred gated) | named pipe, owner-only DACL + integrity floor |
| Data plane | iceoryx2 shared memory | WebSocket over loopback (no iceoryx2 node) |
| Historical queries | ✅ | ❌ answers `unsupported_on_windows` |

Windows builds need LLVM and the MSVC toolchain — see
[`docs/windows-native-build.md`](docs/windows-native-build.md).

## Features at a glance

| Crate | Feature | Default | Purpose |
| --- | --- | :-: | --- |
| `datamancer` | `provider-alpaca` | ✅ | Alpaca provider integration (equities + crypto) |
| `datamancer` | `storage-turso` | ✅ | Turso (embedded SQLite-compatible) cache + tap-log backend |
| `datamancer` | `transport-iceoryx2` | — | Same-host zero-copy transport |
| `datamancer` | `transport-ws` | — | Remote WebSocket transport |
| `datamancer` | `client-ws` / `client-iceoryx2` | — | Re-export `datamancer-client` as `datamancer::client` |
| `datamancer-client` | `ws` / `iceoryx2` | — | The two `Client` implementations |
| `datamancer-client` | `app` | — | `AppHandle::ensure` find-or-spawn facade (implies both) |
| `datamancerd` | `web-ui` | ✅ | Embedded read-only introspection UI + JSON API |
| `datamancerd` | `ws` | — | Remote WebSocket client surface |
| `datamancerd` | `metrics` | — | Prometheus `/metrics` endpoint |

Pulling in a new provider or transport should always be additive and feature-gated.

## Providers

| Provider | Live | History | Status |
| --- | :-: | :-: | --- |
| Alpaca equities | ✅ | ✅ | shipped |
| Alpaca crypto | ✅ | ✅ | shipped |
| IBKR (TWS / IB Gateway) | — | — | **reserved, not built** — credential and health shapes exist in core; no provider implementation |

## What Datamancer does *not* do

It produces events; it is not an analysis framework, a general-purpose time-series store,
or a cross-venue reconciler. There is no semantic enrichment, no source-timestamp
re-sorting, no wall-clock-paced replay, and no cross-symbol/global ordering. Consumers that
need any of those build them on top.

## Licensing

**No license has been selected yet** and no `LICENSE` file exists, so the source is
currently "all rights reserved" by default. The recorded intent is a `MIT OR Apache-2.0`
dual license (see
[`docs/superpowers/specs/2026-07-03-open-sourcing-design.md`](docs/superpowers/specs/2026-07-03-open-sourcing-design.md)),
but that decision is **unexecuted**. Downstream projects should not describe a stack built
on Datamancer as open-source-based until the file lands.
