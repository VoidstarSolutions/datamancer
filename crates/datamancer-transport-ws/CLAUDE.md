# datamancer-transport-ws

Remote WebSocket client transport for datamancer. One WS connection is one
client: inbound JSON control frames drive a `ClientSession`; its multiplexed
`EventStream` is serialized outbound as JSON `EventFrame`s. This crate owns
the wire format (`wire.rs`), the channel-backed `EventSink` (`sink.rs`), and
the single-writer socket task (`writer.rs`). `datamancerd` (feature `ws`)
owns the listener and the per-connection orchestrator glue
(`crates/datamancerd/src/ws/`) — it is the only consumer today.

## Invariants / stance

- **`#![forbid(unsafe_code)]`.** No exception here; this crate has no
  low-level payload layout constraint that would motivate one (contrast
  `datamancer-transport-iceoryx2`, which documents its own narrow EXT-1
  carve-out).
- **Depends on `datamancer-core` only**, plus `tokio-tungstenite` as this
  crate's transport library. `datamancer-core` has no I/O and no
  orchestrator dependency; this crate does not depend on `datamancer` (the
  orchestrator pulls this crate in, not the reverse).
- **`tokio-tungstenite` is pinned to `0.29.0`.** `datamancerd`'s `Cargo.toml`
  pins the same version for its `ws` feature (`accept_hdr_async`, the
  handshake types); keep the two in lockstep — this crate's public
  `run_writer` takes a `Sink<Message>` from that exact tungstenite version.
- **Wire format is transport-internal.** `EventFrame` is `pub` so external
  tooling can decode it directly if needed, but the JSON shape may change
  between versions. `to_wire`/`from_wire` are the supported conversion path
  between `MarketEvent` and the wire type — always go through them rather
  than constructing/matching `EventFrame` by hand.
- **Prices cross as raw `i64`.** Core `Price` does not derive `Serialize`;
  `to_wire`/`from_wire` do the `Price(i64)` unwrap/wrap explicitly. Do not add
  a `Serialize` impl to `Price` in core to route around this — the newtype
  staying opaque outside I/O boundaries is deliberate.
- **Instrument is carried inline, not interned.** JSON is self-describing, so
  unlike the iceoryx2 POD payload there is no `SymbolId`/announcement plane
  and no announcement-race to hold-buffer against.
- **Timestamp triple preserved verbatim.** `source_ts`, `seq`, and `rx_ts` all
  cross unchanged; `rx_ts` stays observability-only and is never synthesized
  by `from_wire`. `Seq::SYNTHETIC` (`u64::MAX`) round-trips as-is — it is not
  special-cased or filtered here.
- **Control routing.** Connection-scoped controls
  (`ProviderConnected`/`ProviderDisconnected`/`ProviderError`) are
  **suppressed** on the event stream (`to_wire` returns `None`), matching the
  iceoryx2 transport's rule — a remote client reads connectivity via the WS
  control surface's `snapshot` reply instead. Per-symbol `Gap` and
  `SubscriptionChanged`, and the terminal `SessionClosing`, are carried.
  `WsDataSink::publish` distinguishes "legitimately suppressed control" (acks
  `Delivered`, puts nothing on the wire) from "unencodable data variant"
  (acks `Rejected`, hands the event back) by checking `matches!(ev,
  MarketEvent::Control(_))` when `to_wire` returns `None` — do not collapse
  that branch, it is what keeps a future non-`Control` `MarketEvent` variant
  (the enum is `#[non_exhaustive]`) from being silently swallowed.
- **Single-writer task ordering.** `WsDataSink::publish` and control replies
  both enqueue onto the *same* per-connection string channel; exactly one
  task (`run_writer`) drains it to the socket. Never add a second writer onto
  the same connection — two writers racing on one `WebSocketStream` half can
  interleave partial frames.
- **Lossy-on-overrun, by disconnection.** The sink's outbound channel is
  bounded; `try_send` on a full channel returns `PublishOutcome::Rejected`
  rather than blocking or silently dropping. The caller (`datamancerd`'s pump)
  treats a reject as "stop the pump, tear the connection down." There is no
  drop-oldest/drop-newest buffering inside this crate — overrun is always
  visible as a disconnect, never a silent gap on a live connection.
- **`run_writer` is transport-generic for testability.** It is generic over
  `Sink<Message> + Unpin` rather than hard-coded to a `WebSocketStream` half,
  so the writer's framing/ordering behavior is unit-testable with a plain
  `futures::channel::mpsc` sink.

## Relationship to `datamancer-transport-iceoryx2`

These two crates are the **two worked examples** of a same-host/remote client
transport bolted onto the same `EventSink`/`ClientSession` seam — one
zero-copy and same-host-only, one JSON-over-TCP and network-reachable. Their
divergences (interned `SymbolId` vs. inline `Instrument`, POD vs. JSON,
diagnostics-plane snapshot vs. `snapshot` reply) are real design points, not
accidents; a future unified client-transport trait should be extracted from
the intersection of these two, not designed in the abstract ahead of a third
concrete transport.

## Tests

Unit tests in each module (`wire.rs`, `sink.rs`, `writer.rs`) cover wire
round-trips per frame variant (including suppressed connection-scoped
controls and `Seq::SYNTHETIC`), sink deliver/reject-on-full-channel behavior,
and writer framing/shutdown. All run under normal CI — no live socket or
network required.
