# WebSocket Client Interface — Design

**Date:** 2026-07-02
**Status:** Approved (brainstorming complete; ready for implementation plan)
**Crate(s):** new `datamancer-transport-ws`; `datamancer` (re-export feature); `datamancerd` (listener + glue)

## Motivation

Today a datamancerd consumer must attach **twice**: a Unix-domain-socket control
connection (newline-JSON `open-client`/`subscribe`/…) *and* a same-host iceoryx2
shared-memory attach to read its per-client `DataPayload` stream. That makes
clients heavy — same-host only, effectively Rust-linked against iceoryx2.

This adds a **second, deliberately different** client transport: a single
bidirectional **WebSocket** connection that is network-reachable (cross-host)
and language-agnostic. It is a worked example, not a replacement. The explicit
downstream goal is to **lift a common client-transport trait** once two real
implementations (iceoryx2 and WebSocket) exist; this design keeps the WS side a
structural peer of the iceoryx2 crate so that later extraction is symmetric. We
do **not** build the abstraction now.

### Non-goals

- No new ordering/transport/event semantics. The WS interface composes existing
  `ClientSession` machinery; it adds only a wire format and a connection shape.
- No premature trait unification (deferred until this second example lands).
- No TLS in v1 (terminate at a reverse proxy). No multi-tenant auth beyond a
  single shared bearer token.

## Connection model

**One WebSocket connection = one client.** On connect, the daemon does the
equivalent of `open-client` implicitly and opens a `dm.client_session()`. The
socket carries everything:

- **Inbound (client → daemon): control.** JSON text frames reusing datamancerd's
  existing `Request` vocabulary — `subscribe` / `unsubscribe` / `snapshot` /
  `close-client`. `open-client` is implicit on connect and is never sent; the
  per-request `client` field is dropped (the connection identifies the client).
  One control vocabulary across UDS and WS.
- **Reply (daemon → client): control acks.** The existing `Reply` JSON, with
  `service` omitted (the stream is this socket) and a small **correlation `id`**
  echoed from the request so a client can match replies to requests on the
  shared socket. Errors use the same stable `codes` table shared with UDS.
- **Outbound (daemon → client): events.** A new tagged JSON event frame (see
  Wire protocol), the client's multiplexed `EventStream` serialized frame by
  frame.

Each WS connection is **self-contained** — its own `ClientSession` — so it does
not go through the daemon's single-actor `ServerCommand` registry. Control is
dispatched directly against the connection's session.

## Crate layout & dependencies

New crate **`datamancer-transport-ws`**, sibling to
`datamancer-transport-iceoryx2`:

- `#![forbid(unsafe_code)]`, `[lints] workspace = true` (matches all crates).
- Depends on **`datamancer-core` only** (event types + `EventSink` /
  `PublishOutcome`), plus `tokio-tungstenite`, `tokio`, `futures`, `serde`,
  `serde_json`, `thiserror`. **No `axum`, no orchestrator dependency** — it stays
  a peer of the iceoryx2 crate so the eventual trait-lift is symmetric.
- `datamancer` re-exports it behind a new **`transport-ws`** feature as
  `datamancer::transport_ws` (paralleling `transport-iceoryx2` →
  `datamancer::transport`), **off by default**.
- `datamancerd` pulls it in behind a **`ws`** feature and owns the listener plus
  the per-connection glue (the only part that touches `ClientSession`).

Module surface in the crate:

| Module | Role | iceoryx2 analog |
|---|---|---|
| `wire.rs` | JSON event frame types + `to_wire`/`from_wire` | `payload.rs` (`to_pod`/`from_pod`) |
| `sink.rs` | `WsDataSink: EventSink` | `sink.rs` (`Iceoryx2DataSink`) |
| `writer.rs` | single-writer task draining a channel to the socket write half | (n/a — iceoryx2 publishes directly) |
| `error.rs` | `WsTransportError` | `error.rs` |

## Wire protocol

All frames are JSON **text** frames over the one WS connection.

**Inbound control** — `Request` verbatim (with `client` omitted, `id` added):

```json
{"id":7,"op":"subscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}
```

**Reply**:

```json
{"id":7,"ok":true}
{"id":8,"ok":false,"code":"unsupported_event_kind","message":"…"}
```

**Outbound event frame** — new tagged type in `wire.rs`, instrument carried
**inline** on every frame (no `SymbolId` interning / announcement race; JSON is
self-describing):

```json
{"type":"trade",
 "instrument":{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD"},
 "seq":42,"source_ts":1719900000000,"rx_ts":1719900000005,
 "price":6543210,"size":100}
```

Variants (top-level `type` tags): `trade` / `quote` / `bar` / `gap` /
`subscription_changed` / `session_closing`. The per-symbol control kinds are
**flattened** to their own top-level tags — there is no nested `control`
wrapper on the wire.

**Timestamp triple preserved end-to-end** (`source_ts`, `seq`, `rx_ts`); `rx_ts`
stays observability-only and is never synthesized on decode. `Seq::SYNTHETIC`
serializes verbatim.

**Control routing mirrors iceoryx2's rule.** Connection-scoped controls
(`ProviderConnected`/`ProviderDisconnected`/`ProviderError`) are **suppressed**
from the event stream (`to_wire` returns `None`) — a client reads connectivity
from the `snapshot` reply. Per-symbol `gap` / `subscription_changed` ride the
event stream with their real instrument inline; `session_closing` is a bare
terminal frame.

`to_wire(&MarketEvent) -> Option<EventFrame>` / `from_wire(&EventFrame) ->
MarketEvent` mirror `to_pod`/`from_pod`, with round-trip unit tests per variant.

## Connection lifecycle (datamancerd `ws/` module)

Per accepted connection, mirroring the UDS + iceoryx2 pairing in `server.rs`,
over one socket:

1. **Accept + handshake** — dedicated TCP listener → `accept_hdr_async` (a
   header-aware callback runs the bearer-token check against the request
   headers at handshake, before the upgrade completes; see Security) → split
   into `(write, read)` halves.
2. **Spawn the writer task** (from the crate) owning `write`, draining a bounded
   `mpsc<Message>`.
3. **Build `WsDataSink`** over the channel `Sender`; hand a second `Sender` clone
   to the control loop so replies and event frames funnel through the one writer
   (single-writer ⇒ no interleaved-write corruption, deterministic frame order).
4. **Open the client** — `dm.client_session()` → `take_events()` →
   `spawn_pump(stream, sink)`, the **same** pump shape the iceoryx2 path uses
   (`stream.next()` → `sink.publish()`).
5. **Control loop** — read text frames from `read`, parse `Request`, dispatch
   against this connection's `ClientSession`
   (`subscribe`/`unsubscribe`/`snapshot`/`close`), send `Reply` (echoing `id`)
   into the writer channel.
6. **Teardown** — on WS close / read EOF / control error / `close-client`: flush
   the sink, `session.close()` (emits terminal `session_closing`), let the pump
   drain under a bounded timeout, then drop. This is the existing `DrainClient`
   shape applied per-connection.

Daemon graceful shutdown signals all live WS connections to run teardown; the
load-bearing ordering holds everywhere: **tap-log flush → sink flush → drop**.

## Backpressure & slow consumers

The **bounded `mpsc`** is the backpressure primitive — the meaningful difference
from iceoryx2's same-host best-effort:

- `WsDataSink::publish` does `try_send`. Success → `Delivered`. **Full** (remote
  consumer too slow) → `Rejected(ev)`, handing the event back; the pump logs and
  stops, and the connection is torn down. A slow remote client is dropped, never
  allowed to wedge the daemon or grow memory unboundedly.
- **Documented policy: remote WS delivery is lossy-on-overrun by disconnection**,
  not by silent drop. (A future refinement could emit a `Gap`-style marker before
  dropping; v1 disconnects — simplest correct behavior.)
- Channel depth is configurable. (`keepalive_secs` is **reserved** for a future
  server-initiated `Ping`/`Pong` liveness probe; v1 does not send server pings —
  dead-but-not-closed detection relies on the client and TCP.)

## Security posture & config

This surface is **mutating and network-reachable** — the opposite of the loopback
read-only web UI — so it gets its own posture, not a mount on the existing axum
server.

New `[ws]` config block:

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `false` | off unless explicitly enabled |
| `bind` | `127.0.0.1` | bind address |
| `port` | (required when enabled) | listen port |
| `auth_token` | unset | optional shared bearer token |
| `channel_depth` | e.g. `1024` | per-connection outbound queue depth |
| `keepalive_secs` | e.g. `30` | reserved; future server-initiated ping interval |
| `max_connections` | e.g. `64` | hard cap on concurrent client connections |

- **Auth (v1): optional shared bearer token**, checked at the WS handshake via
  the `Authorization: Bearer …` header; a missing or wrong token is rejected
  with HTTP 401 before the upgrade completes. If unset, the daemon logs a
  **prominent warning**, and louder when bound off-loopback (same spirit as the
  web UI's non-loopback warning).
- **TLS is out of scope for v1** — terminate at a reverse proxy; documented as
  such.
- Honest scoping: a worked example to exercise the interface shape, not yet a
  hardened public endpoint. `crates/datamancer-transport-ws/README.md` states this
  plainly, alongside the stable control `codes` it shares with UDS.

## Testing

Mirrors the iceoryx2 crate's split — fast unit tests guard the wire format;
runtime tests are gated.

- **Crate unit tests (normal CI):** `to_wire`/`from_wire` round-trips per
  `MarketEvent` variant (each bar interval, gap, subscription-changed,
  session-closing); `WsDataSink` `publish → Delivered` and `full → Rejected(ev)`
  via a tiny-capacity channel; connection-scoped controls suppressed from the
  event stream; `Seq::SYNTHETIC` survives round-trip; `rx_ts` carried, not
  synthesized.
- **`datamancerd` integration (gated `#[ignore]`, like `daemon_e2e.rs`):** WS
  listener on an ephemeral port; a `tokio-tungstenite` client connects,
  `subscribe`s, asserts reply `ok` + `id` echo; a fake provider event drives
  through and the event frame arrives with the timestamp triple intact;
  `snapshot` returns a `SystemSnapshot`; slow-consumer overrun disconnects;
  graceful shutdown emits `session_closing`; missing/incorrect bearer token is
  rejected at handshake.
- **Control-vocabulary parity test:** the same `Request` JSON parses for both UDS
  and WS, guarding the "one control vocabulary" claim.

## Deferred / future

- Lift a common client-transport trait from the iceoryx2 and WS implementations.
- `Gap`-marker-before-drop refinement for slow consumers.
- TLS-native listener / stronger auth.
- Periodic diagnostics push over the WS (today: pull via `snapshot`).
