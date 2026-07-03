# Client Transport Surface Review — iceoryx2 vs WebSocket

**Date:** 2026-07-03
**Status:** Review complete; feeds the trait-boundary decision for the unified
client-transport surface.
**Scope:** Everything a consumer touches between "I want datamancerd's stream"
and "I hold `MarketEvent`s" — the two transport crates *and* the `datamancerd`
glue, layer by layer.

## Why this document

The WS design spec (2026-07-02) deliberately deferred trait unification until
two real transports existed: "a future unified client-transport trait should be
extracted from the intersection of these two, not designed in the abstract."
Both now exist. This review lays the two surfaces side by side to find that
intersection, name the genuine divergences, and locate where the asymmetry
actually lives — so the trait boundary is decided from evidence.

## Layer-by-layer comparison

A consumer's path crosses five layers. Per layer, what each transport ships:

### 1. Wire codec (logical `MarketEvent` ↔ wire form)

| | iceoryx2 (`payload.rs`) | WS (`wire.rs`) |
|---|---|---|
| Wire type | `#[repr(C)]` `Copy` `DataPayload` | JSON-tagged `EventFrame` |
| Instrument | interned `SymbolId` + announcement plane | inline on every frame |
| Encode | `to_pod(&ev, &mut SymbolTable) -> Result<Option<DataPayload>>` | `to_wire(&ev) -> Option<EventFrame>` |
| Decode | `from_pod(&payload, &SymbolResolver) -> Result<MarketEvent, FromPodError>` — fallible (`Unresolved`, `BadDiscriminant`) | `from_wire(&frame) -> MarketEvent` — infallible post-serde |
| Decode ordering hazard | announcement race → `HoldBuffer` required | none (self-describing) |

**Shared contract (identical by design, test-guarded on both sides):**
`to_*` returns `None` for connection-scoped controls (intentional suppression;
connectivity is read out-of-band) *and* for unknown future non-`Control`
variants (must surface as `Rejected`, never silently ack). Timestamp triple
(`source_ts`, `seq`, `rx_ts`) crosses verbatim; `rx_ts` is never synthesized on
decode; `Seq::SYNTHETIC` round-trips as-is. Per-symbol `Gap` /
`SubscriptionChanged` and terminal `SessionClosing` ride the stream.

### 2. Server-side sink

**Already unified** — both implement core's `EventSink`
(`publish`/`publish_borrowed`/`flush` → `PublishOutcome`), fed by the same
`spawn_pump(stream, sink)` shape in `datamancerd`. Differences are internal
policy, correctly hidden behind the trait:

| | `Iceoryx2DataSink` | `WsDataSink` |
|---|---|---|
| Overrun policy | blocking (`RetryUntilDelivered`, no safe overflow); loss accounted core-side as `Control::Gap` | `try_send` → `Rejected(ev)` → pump stops → connection torn down |
| State | interning table, announce-once + periodic full-table republish, `flush` = republish | stateless serializer over a bounded `mpsc<String>` |
| Extra machinery | announcement service | single-writer task (`run_writer`) shared with control replies |

### 3. Server connection/listener glue (`datamancerd`)

| | UDS + iceoryx2 (`server.rs`) | WS (`ws/listener.rs`, `ws/conn.rs`) |
|---|---|---|
| Accept | `UnixListener` accept loop | TCP listener + `accept_hdr_async` (bearer-token check at handshake) |
| Client registry | single-actor `ServerCommand` registry; explicit `open-client` by name; `DUPLICATE_CLIENT` possible | none — each connection is self-contained; `open-client` implicit on connect |
| Session wiring | `open_client` → `Iceoryx2DataSink::new(node, id)` → `client_session()` → `take_events()` → pump | same shape inline per connection with `WsDataSink` |
| Teardown | `DrainClient`: tap-log flush → sink flush → drop | same ordering per connection; plus slow-consumer cancel and daemon-shutdown broadcast |

Not unified, but structurally parallel on purpose (`ws/conn.rs` explicitly
"mirrors the UDS-control + iceoryx2-sink pairing in `server.rs`").

### 4. Control surface

One vocabulary, two framings — both defined **inside `datamancerd`**, not in
either transport crate:

| | UDS (`control.rs`) | WS (`ws/protocol.rs`) |
|---|---|---|
| Request type | `Request` — `open-client` / `subscribe` / `unsubscribe` / `close-client` / `list-clients` / `snapshot`; per-request `client: String` | `WsRequest` — `subscribe` / `unsubscribe` / `snapshot` / `close-client`; correlation `id: u64`; no `client` field |
| Reply type | `Reply { ok, service?, clients?, snapshot?, code?, message? }` | `WsReply { id, ok, code?, message?, snapshot? }` |
| Shared pieces | `SubscriptionSpec`, stable `codes` table, `error_code` mapping — literally the same items, imported by the WS module | |
| Why they differ | one shared socket needs `id` correlation; connection ≠ client on UDS | |

`SubscriptionSpec` itself leans on `datamancerd::config` serde types
(`AssetClassCfg`, `EventKindCfg`, `ScopeCfg`, `PersistenceCfg`).

### 5. Client side — where the story breaks down

| | iceoryx2 consumer | WS consumer |
|---|---|---|
| Attaches | **three**: UDS control socket (hand-rolled newline-JSON), `DataSubscriber::open(node, client_id)` for data+announcements, `Iceoryx2DiagnosticsSubscriber::open(node)` for connectivity | **one** socket carries control, events, and snapshot |
| Shipped client code | `DataSubscriber` (sync `poll() -> Vec<MarketEvent>`, `HoldBuffer` inside) + diagnostics subscriber; **no control client** | **none** — the crate ships only server-side pieces (`WsDataSink`, `wire`, `run_writer`); a client hand-rolls tokio-tungstenite + `from_wire` |
| Control types importable? | no — `Request`/`Reply` live in the `datamancerd` **binary** crate | no — `WsRequest`/`WsReply` likewise |
| Client identity | explicit name via `open-client`; must parse the reply's `service` name (`datamancer/data/{id}`) to learn its `client_id` | implicit — the connection is the client |
| Consumption model | synchronous poll, batch `Vec` | async frames (would be a `Stream`) |
| Connectivity | poll diagnostics plane for latest `SystemSnapshot` | request/reply `snapshot` |
| Loss visibility | in-band `Control::Gap` (numbered hole) | disconnect (torn connection) |
| Connect config | iceoryx2 `Node` + `client_id` | URL + optional bearer token + `channel_depth` |

**Findings:**

1. **The intersection is wide and deliberate.** Both transports already agree
   on every semantic a consumer cares about: one attach = one client = one
   multiplexed stream keyed `(instrument, seq)`; the subscribe/unsubscribe/
   snapshot/close vocabulary with the same `SubscriptionSpec` and stable error
   `codes`; connection-scoped controls suppressed with connectivity via
   `SystemSnapshot`; the timestamp triple verbatim; `SessionClosing` terminal;
   loss always *visible*, never silent. This intersection **is** the trait
   contract — it needs lifting, not inventing.

2. **The asymmetry is mostly missing client code, not mismatched design.** The
   iceoryx2 side ships half a client (data plane only); the WS side ships none.
   Nobody can write a transport-agnostic consumer today because there is no
   complete client for *either* transport.

3. **The control vocabulary is trapped in the binary crate.** `Request`/
   `Reply`, `WsRequest`/`WsReply`, `SubscriptionSpec`, and the `codes` table
   live in `datamancerd`, which no library can depend on. Any client library —
   trait or no trait — forces extracting these into a linkable crate. The
   `*Cfg` serde enums that `SubscriptionSpec` uses come along.

4. **Genuine divergences the trait must expose, not hide:**
   - **Loss surface:** iceoryx2 loss is an in-band `Gap` on a live attach; WS
     loss is a disconnect. Both are "visible loss," but a consumer's recovery
     differs (resubscribe vs reconnect). The contract can honestly promise
     "loss is never silent" and no more.
   - **Consumption model:** sync poll vs async stream. One must be adapted to
     the other; async-stream is the natural common surface (the iceoryx2 poll
     wraps trivially; the reverse does not).
   - **Connect configuration:** node/client-name vs URL/token. Inherently
     per-transport; belongs in per-implementation config structs, with the
     trait starting at "connected."
   - **UDS-only ops:** `open-client` (explicit, named) and `list-clients` are
     registry concepts with no WS analog. `open-client` folds into `connect`;
     `list-clients` is an operator op, not a consumer op — leave it on UDS.

5. **Per-transport quirks that stay hidden below the trait:** the
   `SymbolId`/announcement race and `HoldBuffer` (iceoryx2 decode detail); the
   correlation `id` (WS framing detail); the single-writer channel; interning.
   None leak into the shared surface — confirming the sink-side rule
   ("SymbolId is sink-local, never core") extends cleanly to the client side.

## Where the trait boundary should sit

The evidence points at a **client-side trait over an async multiplexed
consumer handle**, roughly:

```text
connect(per-transport config) → handle
handle.subscribe(spec) / unsubscribe(spec)   — Result with the stable code vocabulary
handle.snapshot() → SystemSnapshot           — connectivity/diagnostics, transport-agnostic
handle.events() → impl Stream<Item = MarketEvent>  — the one multiplexed stream
handle.close()                               — graceful, terminal SessionClosing
```

with the iceoryx2 implementation *bundling* its three attaches (UDS control +
data/announcement shm + diagnostics) behind one handle, and the WS
implementation being the currently-missing tungstenite client.

Prerequisite regardless of trait shape: **extract the shared control
vocabulary** (`SubscriptionSpec`, `codes`, request/reply types, the `*Cfg`
serde enums) out of `datamancerd` into a linkable home, so both the daemon and
client implementations speak it from one definition.

The server side needs **no new trait**: `EventSink` already unifies the sinks,
and the listener glue is per-transport by nature (socket types, handshakes,
registries differ irreducibly). Structural parallelism between `server.rs` and
`ws/conn.rs` is maintained by convention and tests today; a third transport
would be the evidence for lifting a server-side seam, exactly as the second
transport was the evidence for this one.

Open decisions for the design phase (not settled here):

- **Where the trait and implementations live** — a new `datamancer-client`
  crate vs. traits in `datamancer-core` with impls in each transport crate.
- **Whether the trait is dyn-compatible** (boxed stream, dyn dispatch at the
  cold boundary — consistent with the `Provider`/`LiveHandle` precedent) or
  generic.
- **How loss is expressed at the trait level** (documented contract only, vs. a
  normalized "stream ended: reason" item).
- **Blocking/poll access for non-async consumers** — does the iceoryx2 sync
  poll survive as a transport-specific escape hatch?
