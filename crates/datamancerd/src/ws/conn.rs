//! One accepted WebSocket connection = one client. Mirrors the UDS-control +
//! iceoryx2-sink pairing in `server.rs`, but over a single socket: inbound
//! control frames drive this connection's `ClientSession`; its multiplexed
//! `EventStream` is pumped to the socket via the crate's `WsDataSink` +
//! single-writer task. Replies and event frames funnel through the one writer.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use datamancer::traits::{EventSink, PublishOutcome};
use datamancer::transport_ws::{WsDataSink, run_writer};
use datamancer::{ClientSession, Datamancer, Instrument, ProviderId, Scope};
use futures::StreamExt as _;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{
    ErrorResponse, Request as HsRequest, Response as HsResponse,
};

use crate::control::codes;
use crate::ws::{WsReply, WsRequest};

/// Accept the WS handshake (enforcing the bearer token if configured), then run
/// the bridge until the socket closes or the client session ends.
pub async fn handle_connection(
    tcp: TcpStream,
    peer: SocketAddr,
    dm: Datamancer,
    auth_token: Option<Arc<String>>,
    channel_depth: usize,
) {
    let ws = match accept_with_auth(tcp, auth_token).await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::warn!(%peer, error = %e, "ws handshake rejected");
            return;
        }
    };
    tracing::info!(%peer, "ws client connected");

    let (write, mut read) = ws.split();

    // Single writer: both event frames (via the sink) and control replies enqueue
    // strings on this channel; `run_writer` drains it to the socket.
    let (tx, rx) = mpsc::channel::<String>(channel_depth);
    let writer = tokio::spawn(run_writer(rx, write));

    // Open this connection's client and start pumping its stream into the sink.
    let session = dm.client_session();
    let sink: Arc<dyn EventSink> = Arc::new(WsDataSink::new(tx.clone()));
    let stream = match session.take_events().await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::warn!(%peer, error = %e, "take_events failed");
            let _ = tx.try_send(
                serde_json::to_string(&WsReply::from_library_error(0, &e)).unwrap_or_default(),
            );
            return;
        }
    };
    let pump = spawn_pump(stream, sink);

    // Control loop: read frames, dispatch against the session, reply on `tx`.
    let mut closed_by_client = false;
    while let Some(msg) = read.next().await {
        let text = match msg {
            Ok(Message::Text(t)) => t.to_string(),
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_)) => {
                continue;
            }
        };
        let reply = dispatch(&session, &dm, &text).await;
        let close_after = matches!(
            serde_json::from_str::<WsRequest>(&text),
            Ok(WsRequest::CloseClient { .. })
        ) && reply.ok;
        if let Ok(line) = serde_json::to_string(&reply)
            && tx.send(line).await.is_err()
        {
            break;
        }
        if close_after {
            closed_by_client = true;
            break;
        }
    }

    // Teardown: close the session (emits terminal `session_closing` on the
    // stream), let the pump drain under a bound, then drop the writer.
    let _ = session.close().await;
    if tokio::time::timeout(Duration::from_secs(2), pump).await.is_err() {
        tracing::warn!(%peer, "ws pump did not drain in time");
    }
    // Dropping `tx` lets `run_writer` finish once the channel empties.
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;
    tracing::info!(%peer, closed_by_client, "ws client disconnected");
}

/// Perform the tungstenite server handshake, rejecting the upgrade with 401 if a
/// configured bearer token is missing or wrong.
// The `ErrorResponse` (an `http::Response`) is the callback's imposed return
// type from `accept_hdr_async`; its size is not ours to shrink.
#[allow(clippy::result_large_err)]
async fn accept_with_auth(
    tcp: TcpStream,
    auth_token: Option<Arc<String>>,
) -> Result<WebSocketStream<TcpStream>, tokio_tungstenite::tungstenite::Error> {
    tokio_tungstenite::accept_hdr_async(tcp, move |req: &HsRequest, resp: HsResponse| {
        if let Some(expected) = auth_token.as_ref() {
            let presented = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "));
            if presented != Some(expected.as_str()) {
                let mut err = ErrorResponse::new(Some("missing or invalid bearer token".into()));
                *err.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
                return Err(err);
            }
        }
        Ok(resp)
    })
    .await
}

/// Dispatch one parsed control frame against the connection's session.
async fn dispatch(session: &ClientSession, dm: &Datamancer, line: &str) -> WsReply {
    let req = match serde_json::from_str::<WsRequest>(line) {
        Ok(req) => req,
        Err(e) => return WsReply::error(0, codes::BAD_REQUEST, format!("invalid request: {e}")),
    };
    let id = req.id();
    match req {
        WsRequest::Subscribe { spec, .. } => {
            let instrument = Instrument::new(
                ProviderId::new(spec.provider.clone()),
                spec.asset_class.into(),
                spec.symbol.clone(),
            );
            match session
                .subscribe(
                    instrument,
                    spec.kind.into(),
                    Scope::Live {
                        backfill_from: None,
                    },
                    spec.persistence.options(),
                )
                .await
            {
                Ok(()) => WsReply::ok(id),
                Err(e) => WsReply::from_library_error(id, &e),
            }
        }
        WsRequest::Unsubscribe {
            provider,
            asset_class,
            symbol,
            kind,
            ..
        } => {
            let instrument = Instrument::new(ProviderId::new(provider), asset_class.into(), symbol);
            match session.unsubscribe(instrument, kind.into()).await {
                Ok(()) => WsReply::ok(id),
                Err(e) => WsReply::from_library_error(id, &e),
            }
        }
        WsRequest::Snapshot { .. } => match dm.snapshot().await {
            Ok(snapshot) => WsReply::snapshot(id, snapshot),
            Err(e) => WsReply::from_library_error(id, &e),
        },
        WsRequest::CloseClient { .. } => WsReply::ok(id),
    }
}

/// Pump the client's multiplexed stream into the WS sink, in arrival order.
/// Stops when the stream ends or the sink rejects (slow-consumer overrun).
fn spawn_pump(
    mut stream: datamancer::EventStream,
    sink: Arc<dyn EventSink>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(ev) = stream.next().await {
            match sink.publish(ev).await {
                PublishOutcome::Delivered => {}
                PublishOutcome::Rejected(_) => {
                    tracing::warn!("ws sink rejected event (slow consumer); stopping pump");
                    break;
                }
            }
        }
        let _ = sink.flush().await;
    })
}
