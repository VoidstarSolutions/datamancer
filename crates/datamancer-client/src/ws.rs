//! The WebSocket client: one socket carries control requests, correlated
//! replies, and event frames. A reader task demuxes inbound frames — replies
//! resolve pending requests by correlation `id`; event frames decode through
//! the transport crate's `from_wire` (one wire definition) onto a bounded
//! channel that backs the event stream.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datamancer_core::{InstrumentInfo, MarketEvent, ProviderId, SystemSnapshot};
use datamancer_transport_ws::{EventFrame, from_wire};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt as _, StreamExt as _};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::client::Client;
use crate::error::ClientError;
use crate::protocol::ws::{WsReply, WsRequest};
use crate::spec::{SubscriptionSpec, UnsubscribeSpec};

/// Connection parameters for [`WsClient`].
#[derive(Debug, Clone)]
pub struct WsConfig {
    /// `ws://host:port` (TLS terminates at a reverse proxy; see the daemon's
    /// security posture).
    pub url: String,
    /// Optional shared bearer token, sent as `Authorization: Bearer …` on the
    /// handshake.
    pub auth_token: Option<String>,
    /// Bound on locally buffered, not-yet-consumed events. A consumer that
    /// falls behind past the daemon's own channel is disconnected by the
    /// daemon; this bound is the client-side mirror.
    pub event_buffer: usize,
}

/// Transport-layer failures for [`WsClient`].
#[derive(Debug, thiserror::Error)]
pub enum WsClientError {
    #[error("websocket error: {0}")]
    Socket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("codec error: {0}")]
    Codec(#[from] serde_json::Error),
    #[error("invalid config: {0}")]
    Config(String),
    #[error("connection closed before the reply arrived")]
    ConnectionClosed,
}

type WriteHalf = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<WsReply>>>>;

/// A connected WebSocket client. See [`Client`] for the transport-agnostic
/// contract.
pub struct WsClient {
    write: WriteHalf,
    pending: Pending,
    next_id: u64,
}

/// Inbound frame demux: event frames are internally tagged (`"type"`), replies
/// carry `"id"`/`"ok"` — the untagged union tries in that order.
#[derive(Deserialize)]
#[serde(untagged)]
enum Inbound {
    Event(EventFrame),
    Reply(WsReply),
}

async fn run_reader(
    mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    pending: Pending,
    events: mpsc::Sender<MarketEvent>,
) {
    while let Some(Ok(msg)) = read.next().await {
        let Message::Text(text) = msg else { continue };
        match serde_json::from_str::<Inbound>(&text) {
            Ok(Inbound::Event(frame)) => {
                if events.send(from_wire(&frame)).await.is_err() {
                    break; // consumer dropped the stream
                }
            }
            Ok(Inbound::Reply(reply)) => {
                if let Some(tx) = pending.lock().expect("pending poisoned").remove(&reply.id) {
                    let _ = tx.send(reply);
                }
            }
            // Unknown frame shape: a newer daemon speaking a newer wire.
            // Skipping (rather than erroring) keeps old clients readable.
            Err(_) => {}
        }
    }
    // Socket gone: fail every pending request and end the stream (the events
    // sender drops here, so the consumer's stream yields None).
    pending.lock().expect("pending poisoned").clear();
}

impl WsClient {
    async fn request(&mut self, req: &WsRequest) -> Result<WsReply, ClientError<WsClientError>> {
        let id = req.id();
        // Serialize before registering: a codec failure must not leave a
        // stale entry in `pending`.
        let json = serde_json::to_string(req).map_err(WsClientError::from)?;
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .expect("pending poisoned")
            .insert(id, tx);
        if let Err(e) = self.write.send(Message::Text(json.into())).await {
            // The request never reached the wire, so no reply will ever
            // resolve this entry — and the reader task only clears the map
            // when the *read* half dies. Remove it here or a half-open
            // socket (write direction dead, read alive) leaks one sender
            // per failed request.
            self.pending.lock().expect("pending poisoned").remove(&id);
            return Err(ClientError::Transport(WsClientError::from(e)));
        }
        let reply = rx
            .await
            .map_err(|_| ClientError::Transport(WsClientError::ConnectionClosed))?;
        if reply.ok {
            Ok(reply)
        } else {
            Err(ClientError::Control {
                code: reply.code.unwrap_or_default(),
                message: reply.message.unwrap_or_default(),
            })
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

impl Client for WsClient {
    type Config = WsConfig;
    type Error = WsClientError;
    type Events = ReceiverStream<MarketEvent>;

    async fn connect(cfg: Self::Config) -> Result<(Self, Self::Events), ClientError<Self::Error>> {
        let mut request = cfg
            .url
            .as_str()
            .into_client_request()
            .map_err(WsClientError::from)?;
        if let Some(token) = &cfg.auth_token {
            let value = format!("Bearer {token}").parse().map_err(|_| {
                WsClientError::Config("auth token is not a valid header value".to_string())
            })?;
            request.headers_mut().insert("authorization", value);
        }
        let (ws, _resp) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(WsClientError::from)?;
        let (write, read) = ws.split();
        let (ev_tx, ev_rx) = mpsc::channel(cfg.event_buffer.max(1));
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        tokio::spawn(run_reader(read, Arc::clone(&pending), ev_tx));
        Ok((
            WsClient {
                write,
                pending,
                next_id: 1,
            },
            ReceiverStream::new(ev_rx),
        ))
    }

    async fn subscribe(&mut self, spec: &SubscriptionSpec) -> Result<(), ClientError<Self::Error>> {
        let req = WsRequest::Subscribe {
            id: self.next_id(),
            spec: spec.clone(),
        };
        self.request(&req).await.map(|_| ())
    }

    async fn unsubscribe(
        &mut self,
        spec: &UnsubscribeSpec,
    ) -> Result<(), ClientError<Self::Error>> {
        let req = WsRequest::Unsubscribe {
            id: self.next_id(),
            spec: spec.clone(),
        };
        self.request(&req).await.map(|_| ())
    }

    async fn snapshot(&mut self) -> Result<SystemSnapshot, ClientError<Self::Error>> {
        let req = WsRequest::Snapshot { id: self.next_id() };
        let reply = self.request(&req).await?;
        reply.snapshot.ok_or_else(|| {
            ClientError::Transport(WsClientError::Config(
                "ok snapshot reply missing snapshot payload".to_string(),
            ))
        })
    }

    async fn instruments(
        &mut self,
        provider: Option<&ProviderId>,
    ) -> Result<Vec<InstrumentInfo>, ClientError<Self::Error>> {
        let req = WsRequest::Instruments {
            id: self.next_id(),
            provider: provider.map(|p| p.as_str().to_string()),
        };
        let reply = self.request(&req).await?;
        Ok(reply.instruments.unwrap_or_default())
    }

    async fn close(mut self) -> Result<(), ClientError<Self::Error>> {
        let req = WsRequest::CloseClient { id: self.next_id() };
        self.request(&req).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::{WsClient, WsConfig};
    use crate::client::Client;
    use crate::error::ClientError;
    use crate::protocol::ws::{WsReply, WsRequest};
    use crate::spec::SubscriptionSpec;
    use datamancer_core::{MarketEvent, Price, Seq, Timestamp};
    use datamancer_transport_ws::to_wire;
    use futures::{SinkExt as _, StreamExt as _};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message;

    /// Spawn a fake daemon endpoint: accepts one WS connection and hands the
    /// stream to `role`. Returns the `ws://` URL.
    async fn fake_server<F, Fut>(role: F) -> String
    where
        F: FnOnce(tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            role(ws).await;
        });
        format!("ws://{addr}")
    }

    fn cfg(url: String) -> WsConfig {
        WsConfig {
            url,
            auth_token: None,
            event_buffer: 64,
        }
    }

    fn trade() -> MarketEvent {
        use datamancer_core::{AssetClass, Instrument, ProviderId, Trade};
        MarketEvent::Trade(Trade {
            instrument: Instrument::new(
                ProviderId::from_static("alpaca-crypto"),
                AssetClass::Crypto,
                "BTC/USD",
            ),
            source_ts: Timestamp(111),
            rx_ts: Timestamp(222),
            seq: Seq(7),
            price: Price(123_456),
            size: 99,
        })
    }

    #[tokio::test]
    async fn subscribe_correlates_reply_and_events_flow() {
        let url = fake_server(|mut ws| async move {
            // Expect a subscribe; ack it; then push one event frame.
            let Some(Ok(Message::Text(text))) = ws.next().await else {
                panic!("expected subscribe frame")
            };
            let req: WsRequest = serde_json::from_str(&text).unwrap();
            let WsRequest::Subscribe { id, spec } = req else {
                panic!("expected subscribe")
            };
            assert_eq!(spec.symbol, "BTC/USD");
            // Interleave: event frame BEFORE the reply — correlation must
            // still resolve, and the event must land on the stream.
            let frame = to_wire(&trade()).unwrap();
            ws.send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
                .await
                .unwrap();
            ws.send(Message::Text(
                serde_json::to_string(&WsReply::ok(id)).unwrap().into(),
            ))
            .await
            .unwrap();
            // Hold the socket open until the client is done.
            let _ = ws.next().await;
        })
        .await;

        let (mut client, mut events) = WsClient::connect(cfg(url)).await.expect("connect");
        let spec: SubscriptionSpec = serde_json::from_str(
            r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
        )
        .unwrap();
        client.subscribe(&spec).await.expect("subscribe acked");

        let ev = events.next().await.expect("one event");
        assert_eq!(ev, trade()); // timestamp triple verbatim, price intact
    }

    #[tokio::test]
    async fn error_reply_maps_to_control_error() {
        let url = fake_server(|mut ws| async move {
            let Some(Ok(Message::Text(text))) = ws.next().await else {
                panic!("expected frame")
            };
            let req: WsRequest = serde_json::from_str(&text).unwrap();
            ws.send(Message::Text(
                serde_json::to_string(&WsReply::error(
                    req.id(),
                    crate::codes::DUPLICATE_SUBSCRIPTION,
                    "already subscribed",
                ))
                .unwrap()
                .into(),
            ))
            .await
            .unwrap();
            let _ = ws.next().await;
        })
        .await;

        let (mut client, _events) = WsClient::connect(cfg(url)).await.expect("connect");
        let spec: SubscriptionSpec = serde_json::from_str(
            r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
        )
        .unwrap();
        match client.subscribe(&spec).await {
            Err(ClientError::Control { code, .. }) => {
                assert_eq!(code, crate::codes::DUPLICATE_SUBSCRIPTION);
            }
            other => panic!("expected Control error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bearer_token_is_sent_on_the_handshake() {
        // Raw TCP accept: read the HTTP upgrade request and assert the header
        // before completing the handshake.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            // `Callback::on_request`'s `Err` is the tungstenite `Response`
            // type itself (large); this is test-only handshake plumbing, not
            // a Result this crate's code returns.
            #[allow(clippy::result_large_err)]
            let ws = tokio_tungstenite::accept_hdr_async(
                tcp,
                |req: &tokio_tungstenite::tungstenite::handshake::server::Request, resp| {
                    let auth = req
                        .headers()
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    assert_eq!(auth, "Bearer s3cr3t");
                    Ok(resp)
                },
            )
            .await
            .unwrap();
            drop(ws);
        });
        let cfg = WsConfig {
            url: format!("ws://{addr}"),
            auth_token: Some("s3cr3t".to_string()),
            event_buffer: 8,
        };
        let _ = WsClient::connect(cfg).await; // may error on immediate drop; header assert is the test
        server.await.unwrap();
    }

    #[tokio::test]
    async fn server_drop_ends_the_event_stream() {
        let url = fake_server(|ws| async move {
            drop(ws); // immediate close
        })
        .await;
        let (_client, mut events) = WsClient::connect(cfg(url)).await.expect("connect");
        assert!(
            events.next().await.is_none(),
            "stream ends on connection loss"
        );
    }

    #[tokio::test]
    async fn close_sends_close_client_and_awaits_ack() {
        let url = fake_server(|mut ws| async move {
            let Some(Ok(Message::Text(text))) = ws.next().await else {
                panic!("expected frame")
            };
            let req: WsRequest = serde_json::from_str(&text).unwrap();
            assert!(matches!(req, WsRequest::CloseClient { .. }));
            ws.send(Message::Text(
                serde_json::to_string(&WsReply::ok(req.id()))
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();
        })
        .await;
        let (client, _events) = WsClient::connect(cfg(url)).await.expect("connect");
        client.close().await.expect("close acked");
    }

    #[tokio::test]
    async fn failed_send_does_not_leak_a_pending_entry() {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};

        let url = fake_server(|mut ws| async move {
            let _ = ws.next().await;
        })
        .await;
        // Hand-build a client whose write half is already closed and that has
        // no reader task: the send fails deterministically, and nothing else
        // can clean the pending map behind the request's back. This is the
        // half-open shape (write direction dead, read direction alive) where
        // a leaked entry would otherwise persist indefinitely.
        let (ws, _resp) = tokio_tungstenite::connect_async(url.as_str())
            .await
            .unwrap();
        let (mut write, read) = ws.split();
        write.close().await.unwrap();
        drop(read);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let mut client = WsClient {
            write,
            pending: Arc::clone(&pending),
            next_id: 1,
        };
        let spec: SubscriptionSpec = serde_json::from_str(
            r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
        )
        .unwrap();
        match client.subscribe(&spec).await {
            Err(ClientError::Transport(_)) => {}
            other => panic!("expected transport error, got {other:?}"),
        }
        assert!(
            pending.lock().unwrap().is_empty(),
            "failed send must remove its pending entry"
        );
    }
}
