# Unified Client Transport — Design

**Date:** 2026-07-03
**Status:** Approved (brainstorming complete; ready for implementation plan)
**Crates:** new `datamancer-client`; `datamancerd` (vocabulary extraction);
`datamancer` (re-export features). Transport crates unchanged in role.
**Companion:** `2026-07-03-client-transport-surface-review.md` — the
layer-by-layer surface comparison this design was decided from.

## Motivation

The iceoryx2 and WebSocket transports were built as the two worked examples
from which a unified client-transport trait would be extracted (per the WS
design spec's explicit deferral). The surface review found:

1. The two transports already agree on every consumer-visible semantic —
   the trait contract exists and needs lifting, not inventing.
2. Nobody can write a transport-agnostic consumer today because neither
   transport ships a complete client: iceoryx2 ships half (data-plane
   `DataSubscriber`; the UDS control connection is hand-rolled), WS ships none.
3. The shared control vocabulary (`SubscriptionSpec`, stable `codes`,
   request/reply types) is trapped in the `datamancerd` **binary** crate, which
   no library can link against.

Goal: a consumer picks a transport by config/type and everything else — the
subscribe vocabulary, error codes, snapshot, the one multiplexed
`MarketEvent` stream, the loss and close semantics — is identical.

### Non-goals

- **No server-side changes beyond the vocabulary extraction.** `EventSink`
  already unifies the sinks; the listener/connection glue differs irreducibly
  per transport and stays where it is. A server-side seam waits for a third
  transport, exactly as this trait waited for the second.
- **No new ordering/transport/event semantics.** The trait exposes the
  existing contract; it adds nothing to it.
- **No dyn-dispatch/runtime-selection layer.** The trait is generic
  (associated types, zero-cost). A consumer wanting runtime transport
  selection writes its own enum over the two concrete clients.
- **No normalization of loss across transports** beyond the documented
  contract (see Loss & close semantics).

## Crate layout

New workspace member **`datamancer-client`** (`#![forbid(unsafe_code)]`,
`[lints] workspace = true`):

| Feature | Deps added | Provides |
|---|---|---|
| (base) | `datamancer-core`, `serde`, `serde_json`, `thiserror`, `futures` | the `Client` trait, `ClientError`, the control **vocabulary** |
| `ws` | `datamancer-transport-ws`, `tokio-tungstenite` (pinned in lockstep, 0.29.0), `tokio` | `WsClient` |
| `iceoryx2` | `datamancer-transport-iceoryx2`, `tokio` | `Iceoryx2Client` |

Both features off by default, matching the transport-feature convention.

Dependency direction (no cycles, core stays pure):

```
datamancer-core
   ↑                ↑
transport crates → datamancer-client ← datamancerd (default-features off)
   ↑                                      ↑
datamancer  (re-exports client behind matching features)
```

- `datamancer` gains features `client-ws` / `client-iceoryx2` re-exporting
  `datamancer-client` as `datamancer::client`, paralleling
  `transport-iceoryx2` → `datamancer::transport`.
- `datamancerd` depends on `datamancer-client` with default features off —
  vocabulary only, no client impls, no new transitive weight.

### Vocabulary extraction

Moves from `datamancerd` into `datamancer-client` (types unchanged, wire
bytes unchanged — this is a relocation, not a redesign):

- `SubscriptionSpec` and a new `UnsubscribeSpec` (the
  provider/asset-class/symbol/kind tuple `unsubscribe` takes today, named).
- The stable `codes` table and `error_code` mapping. (`error_code` maps
  `datamancer::Error` — it stays in `datamancerd`, which is the only place
  that has the library error in hand; the **code strings** move.)
- UDS `Request`/`Reply`; WS `WsRequest`/`WsReply`.
- The serde config enums the spec leans on: `AssetClassCfg`, `EventKindCfg`,
  `ScopeCfg`, `PersistenceCfg` move out of `datamancerd::config`;
  `datamancerd` re-imports them.

`datamancerd::control` and `datamancerd::ws::protocol` shrink to re-exports.
Existing protocol round-trip, unknown-key rejection, code-stability, and
UDS/WS parity tests move with the types (or stay as daemon-side guards where
they assert daemon behavior).

## The trait

```rust
pub trait Client: Sized + Send {
    /// Per-transport connection parameters (URL/token vs socket-path/name).
    type Config;
    /// Transport-layer failures only; control rejections are `ClientError::Control`.
    type Error: std::error::Error + Send + 'static;
    /// The one multiplexed event stream, keyed `(instrument, seq)`.
    type Events: Stream<Item = MarketEvent> + Send + Unpin;

    fn connect(cfg: Self::Config)
        -> impl Future<Output = Result<(Self, Self::Events), ClientError<Self::Error>>> + Send;
    fn subscribe(&mut self, spec: &SubscriptionSpec)
        -> impl Future<Output = Result<(), ClientError<Self::Error>>> + Send;
    fn unsubscribe(&mut self, spec: &UnsubscribeSpec)
        -> impl Future<Output = Result<(), ClientError<Self::Error>>> + Send;
    fn snapshot(&mut self)
        -> impl Future<Output = Result<SystemSnapshot, ClientError<Self::Error>>> + Send;
    fn close(self)
        -> impl Future<Output = Result<(), ClientError<Self::Error>>> + Send;
}
```

Decisions embedded here (each user-approved):

- **Generic, not dyn.** Associated types, `impl Future` methods, no boxing.
  Transport is a compile-time choice; consumers needing runtime selection
  wrap the two concrete types themselves.
- **`connect` returns `(Self, Self::Events)` — a split pair.** The control
  handle and the owned event stream are separate values, so a consumer can
  drain events while issuing control calls without borrow conflicts. Both
  impls produce this naturally (iceoryx2: control and data are separate
  attaches already; WS: the reader task must demux replies from event frames
  anyway, so events land on a channel).
- **Two-layer errors.**

  ```rust
  pub enum ClientError<E> {
      /// Daemon rejected the request: a stable code from `codes` + message.
      Control { code: String, message: String },
      /// The transport itself failed (socket, handshake, shm attach, codec).
      Transport(E),
  }
  ```

  Control-plane rejections are byte-identical across transports (they *are*
  the daemon's contract); only genuine transport failures differ per impl.
- **The trait starts at "connected."** All connect-time configuration
  (URL, bearer token, channel depth vs. UDS socket path, client name) lives
  in per-transport `Config` structs.

### Loss & close semantics (the documented contract)

`Self::Events` yields plain `MarketEvent`. The trait documents, and both
impls uphold:

- **Loss is never silent.** On iceoryx2, resume-buffer overflow surfaces
  in-band as `Control::Gap` at the evicted span (numbered `seq` hole). On WS,
  a slow consumer is disconnected — the stream ends.
- **Graceful close is marked.** A stream that ends after `SessionClosing`
  closed gracefully; a stream that ends without one lost its connection.
- No client-side synthesis: the client never invents events the wire did not
  carry (consistent with `rx_ts` never being reconstructed).

## The two implementations

### `WsClient` (new)

- `connect`: `tokio-tungstenite` connect with optional
  `Authorization: Bearer` header; split socket; spawn a **reader task** that
  demuxes inbound text frames: `WsReply` (matched to pending requests by
  correlation `id` via a oneshot map) vs `EventFrame` (decoded with the
  existing `from_wire` — one wire definition, already round-trip-tested) onto
  a bounded channel that backs `Self::Events`.
- Control methods: allocate `id` from a counter, send `WsRequest`, await the
  correlated `WsReply`; `ok:false` → `ClientError::Control`.
- `snapshot`: the `snapshot` op's reply carries `SystemSnapshot`.
- `close`: send `close-client`, await the ack, then drain the stream until
  `SessionClosing`/EOF and let the socket close.
- Config: `{ url, auth_token: Option<String>, event_buffer: usize, .. }`.

### `Iceoryx2Client` (composition of existing pieces)

Bundles today's three hand-assembled attaches behind one handle:

- `connect`: open the UDS control socket (tokio `UnixStream`, newline-JSON);
  send `open-client` with the configured client name (optionally seeding
  subscriptions); parse the `client_id` from the reply's service name
  (`datamancer/data/{id}`); create/hold the iceoryx2 `Node`; open
  `DataSubscriber`; spawn an async adapter that drives the subscriber's sync
  `poll()` (yield/interval between empty polls) onto a channel backing
  `Self::Events`.
- Control methods: newline-JSON request/reply on the held UDS connection
  (one in-flight request at a time — the UDS protocol is strictly
  request-reply per connection, so no correlation id is needed).
- `snapshot`: the UDS `snapshot` op (request/reply, matching WS), **not** the
  diagnostics plane — the plane stays available as a lower-level extra for
  push-style consumers, as does the sync `DataSubscriber` for consumers who
  want the poll loop themselves.
- `close`: `close-client` over UDS, drain until `SessionClosing`/service drop.
- Config: `{ control_socket: PathBuf, client_name: String, poll_interval, .. }`.

Neither impl adds transport semantics: they compose the wire codecs, control
protocols, and attach sequences that exist today.

## `datamancerd` changes

Mechanical relocation only. `control.rs` / `ws/protocol.rs` re-import the
moved types; `config.rs` re-imports the moved enums; behavior, wire bytes,
stable codes, and the operator contract in `crates/datamancerd/README.md` are
unchanged (README gains a pointer to the client crate).

## Testing

- **Vocabulary:** moved round-trip + code-stability tests; a daemon-side
  parity test asserts the documented JSON lines still parse verbatim
  post-extraction.
- **`WsClient` unit tests (normal CI):** in-process tungstenite server on an
  ephemeral port — reply/`id` correlation (including out-of-order replies
  interleaved with event frames), event decode with timestamp triple intact,
  bearer-token header attach, `snapshot` payload, close handshake, stream
  ends on server drop.
- **`Iceoryx2Client`:** protocol/parse logic (service-name → `client_id`,
  newline framing) unit-tested; the live bundled attach is a gated
  `#[ignore]` test alongside the existing iceoryx2 runtime tests.
- **The payoff test — one generic exercise:**

  ```rust
  async fn exercise<C: Client>(cfg: C::Config) { /* subscribe → trade arrives
      with (source_ts, seq, rx_ts) verbatim → snapshot → close → SessionClosing */ }
  ```

  run against **both** impls in the gated daemon e2e suite. This test is the
  "end user doesn't care which transport" guarantee, stated executably.

## Deferred

- Runtime transport selection helper (enum over the two clients) — write it
  when a consumer actually needs it.
- Server-side listener/glue seam — waits for a third transport.
- Reconnect/resubscribe policy inside the clients (today: connection loss is
  surfaced; recovery is the consumer's choice).
- WS `Gap`-before-drop refinement and diagnostics push (unchanged from the WS
  spec's deferrals).
