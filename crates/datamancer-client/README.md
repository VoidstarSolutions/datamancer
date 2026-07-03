# datamancer-client

Consumer-side surface for [`datamancerd`](../datamancerd): the control
**vocabulary** shared by every transport (subscription specs, stable error
codes, request/reply framings) plus, behind features `ws` and `iceoryx2`, two
concrete implementations of one generic [`Client`] trait — one connection,
one multiplexed event stream, transport chosen at compile time.

This crate depends on `datamancer-core` and the relevant transport crate
only; it never depends on the `datamancer` orchestrator. `datamancer`
re-exports it (as `datamancer::client`) behind the `client-ws` /
`client-iceoryx2` features, mirroring how it re-exports the transport crates
themselves.

## The `Client` trait contract

Every implementation upholds the same contract (from the doc comment on
[`Client`] in `src/client.rs`):

- **One connection = one client = one multiplexed stream**, ordered by
  `(instrument, seq)`; per-instrument demux is the consumer's concern.
- The timestamp triple (`source_ts`, `seq`, `rx_ts`) crosses verbatim;
  `rx_ts` is observability-only and never synthesized client-side.
- Control rejections surface as `ClientError::Control` with the stable
  `codes` strings — identical across transports.
- **Loss is never silent.** On iceoryx2, resume-buffer overflow surfaces
  in-band as `Control::Gap` (a numbered `seq` hole). On WebSocket, a slow
  consumer is disconnected — the stream ends. A stream that ends after a
  `SessionClosing` control closed gracefully; one that ends without it lost
  its connection. Reconnect policy is the consumer's choice.
- Connection-scoped provider controls are suppressed from the stream; read
  connectivity from `Client::snapshot`.

`connect` returns a split `(handle, events)` pair, so a consumer can issue
control calls (`subscribe`, `unsubscribe`, `snapshot`, `instruments`,
`close`) while draining the event stream on another task without contention.

## Stable codes

Control rejections carry one of the strings in `codes` (`duplicate_subscription`,
`not_subscribed`, `unknown_provider`, `session_closed`, `duplicate_client`,
`unsupported_event_kind`, `shutting_down`, `internal`, …) — identical across
both transports and regression-guarded by tests. Treat these as an operator
contract: match on the string, not on transport-specific error text.

## Loss contract

| Transport | Overrun / backpressure                                          | Graceful close                                                                 |
|-----------|------------------------------------------------------------------|---------------------------------------------------------------------------------|
| iceoryx2  | In-band `Control::Gap` — the daemon's resume buffer numbers the evicted `seq` span; the stream stays open. | Terminal `SessionClosing`, **but** see the caveat below — it can race with stream teardown. |
| WS        | The daemon disconnects a slow consumer; the event stream simply ends (no in-band marker for this case). | Terminal `SessionClosing` frame, then a clean WS close.                          |

**iceoryx2 close race (known, narrow):** on `Iceoryx2Client::close`, the
daemon emits the terminal `SessionClosing` sample before dropping the
per-client service, but this client's background poll loop can observe the
service disappear (an `Err` from the shared-memory poll, which ends the
event stream) before it drains that final sample. The caller that invoked
`close` already knows the shutdown was intentional; it is stream-readers on
a *different* task that should not assume they will always observe the
`SessionClosing` marker on this transport. The WS transport does not have
this race (single-writer socket: the close frame is always the last thing
written before the socket closes).

## Example: connect and subscribe

### iceoryx2 (same host)

```rust,no_run
use datamancer_client::{Client, spec::{AssetClassCfg, EventKindCfg, SubscriptionSpec}};
use datamancer_client::iceoryx2::{Iceoryx2Client, Iceoryx2Config};
use futures::StreamExt as _;
use std::time::Duration;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = Iceoryx2Config {
    control_socket: "/tmp/datamancerd/control.sock".into(),
    client_name: "my-consumer".to_string(),
    poll_interval: Duration::from_millis(5),
    event_buffer: 1024,
};
let (mut client, mut events) = Iceoryx2Client::connect(cfg).await?;

client
    .subscribe(&SubscriptionSpec {
        provider: "alpaca-crypto".to_string(),
        asset_class: AssetClassCfg::Crypto,
        symbol: "BTC/USD".to_string(),
        kind: EventKindCfg::Trade,
        scope: Default::default(),
        persistence: Default::default(),
    })
    .await?;

while let Some(event) = events.next().await {
    println!("{event:?}");
}
# Ok(())
# }
```

### WebSocket (remote)

```rust,no_run
use datamancer_client::{Client, spec::{AssetClassCfg, EventKindCfg, SubscriptionSpec}};
use datamancer_client::ws::{WsClient, WsConfig};
use futures::StreamExt as _;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = WsConfig {
    url: "ws://127.0.0.1:8765".to_string(),
    auth_token: Some("s3cr3t".to_string()),
    event_buffer: 1024,
};
let (mut client, mut events) = WsClient::connect(cfg).await?;

client
    .subscribe(&SubscriptionSpec {
        provider: "alpaca-crypto".to_string(),
        asset_class: AssetClassCfg::Crypto,
        symbol: "BTC/USD".to_string(),
        kind: EventKindCfg::Trade,
        scope: Default::default(),
        persistence: Default::default(),
    })
    .await?;

while let Some(event) = events.next().await {
    println!("{event:?}");
}
# Ok(())
# }
```

Both examples are the same shape by design: code written against `C: Client`
is transport-agnostic; only `C::Config` (`Iceoryx2Config` vs `WsConfig`) and
the `connect` call differ.

When starting from discovery instead of a hand-written spec, use
`SubscriptionSpec::new` to close the loop — it converts the core vocabulary
`Client::instruments` returns into the wire vocabulary `subscribe` takes:

```rust,ignore
for info in client.instruments(None).await? {
    for kind in info.kinds {
        client
            .subscribe(&SubscriptionSpec::new(&info.instrument, kind)?)
            .await?;
    }
}
```

## Honest scoping

Both client implementations are worked examples of a consumer-side transport,
not yet hardened public endpoints — mirroring the posture of the transport
crates they wrap:

- The **WebSocket** client is a remote, network-reachable surface. It sends
  an optional bearer token (`WsConfig::auth_token`) on the handshake, but TLS
  is out of scope — terminate it at a reverse proxy if the deployment needs
  it. Running against a daemon without a token configured is a deployment
  choice, not one this crate second-guesses.
- The **iceoryx2** client is same-host only (UDS control socket + shared
  memory data plane) and assumes a trusted local process boundary; it does
  no authentication of its own.

Compare the two before adding a third transport — a unified,
runtime-selectable client is a natural future extraction once the shape of
"second transport" is no longer theoretical.

[`Client`]: ./src/client.rs
