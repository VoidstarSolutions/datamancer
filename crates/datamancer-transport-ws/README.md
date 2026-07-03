# datamancer-transport-ws

A remote WebSocket client transport for datamancer. One bidirectional WS
connection is one client: inbound JSON control frames drive that client's
`ClientSession`, and its multiplexed `EventStream` is serialized outbound as
JSON event frames. This crate owns the wire format, the channel-backed
`WsDataSink`, and the single-writer socket task; `datamancerd` (feature `ws`)
owns the listener and the per-connection glue that touches the orchestrator
(`crates/datamancerd/src/ws/`).

This is one of two worked examples of a same-host/remote client transport —
the other is `datamancer-transport-iceoryx2` (same-host, zero-copy). Compare
the two before adding a third; a unified client-transport trait is a natural
future extraction once the shape of "second transport" is no longer
theoretical.

## Connection model

- **One connection = one client.** There is no separate `open-client` request:
  connecting the socket implicitly opens the client, mirroring the UDS control
  surface's `open-client` but without a name to negotiate.
- **Single writer per connection.** Outbound event frames (from the sink) and
  outbound control replies both enqueue JSON strings onto one bounded channel;
  one writer task (`run_writer`) drains it to the socket. This guarantees
  frames never interleave mid-write and that reply/event ordering on the wire
  is deterministic (whatever order they were enqueued in).

## Inbound control: `WsRequest`

Reuses the UDS control vocabulary — the same `SubscriptionSpec` body and the
same stable `codes` error table (`crates/datamancerd/src/control.rs`) — but
drops the per-request `client` field (the connection already identifies the
client) and adds a correlation `id` that every request carries and every
reply echoes, because event frames and replies interleave on the shared
socket.

Requests are tagged JSON, `op` field, kebab-case:

```jsonc
{"id":1,"op":"subscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade","scope":"live","persistence":"cached_with_tap"}
{"id":2,"op":"unsubscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}
{"id":3,"op":"snapshot"}
{"id":4,"op":"close-client"}
```

Replies echo the request `id`:

```jsonc
{"id":1,"ok":true}
{"id":3,"ok":true,"snapshot":{ /* SystemSnapshot */ }}
{"id":5,"ok":false,"code":"unsupported_event_kind","message":"…"}
```

`close-client` with `ok:true` triggers a graceful close of this connection's
session (terminal `session_closing` frame, then a clean WS Close). There is no
`service` field on any WS reply — unlike the UDS `open-client` reply, there is
no separate per-client iceoryx2 service to name; the event stream *is* this
socket.

## Outbound events: `EventFrame`

Unlike the iceoryx2 POD, the `Instrument` is carried **inline** on every
frame — JSON is self-describing, so there is no `SymbolId` interning or
announcement-ordering race to manage. Prices cross the wire as raw `i64`
(core `Price` does not derive `Serialize`). The timestamp triple
(`source_ts`/`seq`/`rx_ts`) is preserved end-to-end; `rx_ts` is never
synthesized on decode, and `Seq::SYNTHETIC` (`u64::MAX`) round-trips verbatim.

Tagged JSON, `type` field, snake_case: `trade`, `quote`, `bar`, `gap`,
`subscription_changed`, `session_closing`.

Control routing matches the iceoryx2 transport's rule: connection-scoped
controls (`ProviderConnected`/`ProviderDisconnected`/`ProviderError`) are
**suppressed** from the event stream (`to_wire` returns `None`) — a client
reads connectivity via the `snapshot` reply instead. Per-symbol `Gap` and
`SubscriptionChanged`, and the terminal `SessionClosing`, are carried on the
stream.

## Backpressure

Each connection's outbound channel is bounded (`[ws].channel_depth`,
default 1024). If a remote consumer falls behind and the channel fills, the
sink's `publish` returns `PublishOutcome::Rejected` and the connection is
**torn down** — delivery is lossy-on-overrun *by disconnection*, never by
silently dropping frames on a live connection. A reconnecting client gets a
fresh session with no attempt at replay-from-overrun in this crate.

## Security posture

This surface is **mutating and network-reachable** — a different posture from
the loopback, read-only web UI. It is guarded by an optional shared bearer
token (`[ws].auth_token`) checked at the WS handshake: a missing or wrong
`Authorization: Bearer …` header on a configured deployment gets an HTTP 401
before the WS upgrade completes. Running without a token logs a warning
(louder when bound off-loopback). TLS is explicitly **out of scope** for this
crate/surface — terminate TLS at a reverse proxy if the deployment needs it.

Treat this transport as a worked example of a remote client surface, not yet a
hardened public endpoint.

## Tests

Unit tests cover wire round-trips per frame variant (including the suppressed
connection-scoped controls and `Seq::SYNTHETIC`), the sink's
deliver/reject-on-full-channel behavior, and the writer's string-to-`Message`
framing. They run under normal CI (no live socket required).
