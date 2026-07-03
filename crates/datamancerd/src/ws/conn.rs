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
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
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
            // `id:0` is the out-of-band sentinel: this failure precedes any
            // client request, so there is no request id to echo. We skip
            // `session.close()` because the stream was never taken — there is no
            // pump/terminal frame to drain, and the session drops cleanly.
            tracing::warn!(%peer, error = %e, "take_events failed");
            let _ = tx.try_send(
                serde_json::to_string(&WsReply::from_library_error(0, &e)).unwrap_or_default(),
            );
            return;
        }
    };
    // Per-connection cancel: the pump signals this on slow-consumer overrun so the
    // read loop breaks into the shared teardown path (tearing the socket down)
    // rather than leaving the connection silently stalled. `notify_one` stores a
    // permit, so a `notified()` that races after the signal still completes.
    let cancel = Arc::new(tokio::sync::Notify::new());
    let pump = spawn_pump(stream, sink, Arc::clone(&cancel));

    // Control loop: read frames, dispatch against the session, reply on `tx`.
    // Also select on the daemon-wide shutdown signal so graceful drain triggers
    // this connection's teardown instead of leaving it blocked on the read.
    let mut closed_by_client = false;
    let mut drained = false;
    loop {
        let msg = tokio::select! {
            // Any resolution of `recv` — Ok(()), Lagged, or Closed — means "shut
            // down now": break to the shared teardown path below.
            _ = shutdown.recv() => {
                drained = true;
                break;
            }
            // The pump hit slow-consumer overrun (bounded outbound channel full):
            // tear the connection down via the shared teardown path instead of
            // leaving the client silently stalled.
            () = cancel.notified() => {
                tracing::warn!(%peer, "ws slow consumer overran outbound channel; tearing down");
                break;
            }
            msg = read.next() => match msg {
                Some(msg) => msg,
                None => break,
            },
        };
        let text = match msg {
            Ok(Message::Text(t)) => t.to_string(),
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_)) => {
                continue;
            }
        };
        // `dispatch` also reports whether the frame was a `close-client`, so the
        // read loop need not re-parse it to compute `close_after`.
        let (reply, was_close_client) = dispatch(&session, &dm, &text).await;
        let close_after = was_close_client && reply.ok;
        if let Ok(line) = serde_json::to_string(&reply) {
            // Enqueue the reply, but stay responsive to teardown while doing so.
            // A stalled consumer can fill the shared outbound channel; a bare
            // `tx.send(line).await` would then park here — deaf to both the
            // pump's overrun `cancel` and the daemon `shutdown` — until TCP
            // eventually errors. Racing the send against those signals preserves
            // the prompt-teardown guarantee for a client that keeps sending
            // control frames while never draining its reads.
            tokio::select! {
                res = tx.send(line) => {
                    if res.is_err() {
                        break;
                    }
                }
                _ = shutdown.recv() => {
                    drained = true;
                    break;
                }
                () = cancel.notified() => {
                    tracing::warn!(%peer, "ws slow consumer overran while sending reply; tearing down");
                    break;
                }
            }
        }
        if close_after {
            closed_by_client = true;
            break;
        }
    }

    // Teardown (shared by every exit: client Close/EOF/error, close-client, and
    // daemon shutdown): close the session (emits terminal `session_closing` on
    // the stream), let the pump drain under a bound, then drop the writer so
    // `run_writer` emits the clean WS Close frame once the channel empties.
    let _ = session.close().await;
    if tokio::time::timeout(Duration::from_secs(2), pump).await.is_err() {
        tracing::warn!(%peer, "ws pump did not drain in time");
    }
    // Dropping `tx` lets `run_writer` finish once the channel empties.
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;
    tracing::info!(%peer, closed_by_client, drained, "ws client disconnected");
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
            // RFC 7235 auth schemes are case-insensitive, so match "Bearer"
            // ignoring case; the token value itself is compared exactly.
            let presented = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.split_once(' '))
                .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("Bearer"))
                .map(|(_, token)| token);
            // Compare the secret in constant time: a short-circuiting `!=` leaks
            // the length of the matching prefix as a timing side-channel on a
            // network-facing auth boundary. A missing/mis-scheme header (no
            // secret involved) rejects immediately.
            let authorized = presented.is_some_and(|t| ct_eq(t.as_bytes(), expected.as_bytes()));
            if !authorized {
                let mut err = ErrorResponse::new(Some("missing or invalid bearer token".into()));
                *err.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
                return Err(err);
            }
        }
        Ok(resp)
    })
    .await
}

/// Constant-time byte-slice equality. Runs in time proportional to the input
/// length regardless of where (or whether) the bytes first differ, so a
/// bearer-token comparison leaks no timing signal about the secret's contents.
/// Length is not itself secret here (an attacker controls their own presented
/// token), so an early length check is acceptable.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Dispatch one parsed control frame against the connection's session. Returns
/// the reply plus whether the frame was a `close-client` (so the caller need not
/// re-parse the line to decide on a graceful close).
async fn dispatch(session: &ClientSession, dm: &Datamancer, line: &str) -> (WsReply, bool) {
    let req = match serde_json::from_str::<WsRequest>(line) {
        Ok(req) => req,
        Err(e) => {
            return (
                WsReply::error(0, codes::BAD_REQUEST, format!("invalid request: {e}")),
                false,
            );
        }
    };
    let id = req.id();
    let is_close_client = matches!(req, WsRequest::CloseClient { .. });
    let reply = match req {
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
    };
    (reply, is_close_client)
}

/// Pump the client's multiplexed stream into the WS sink, in arrival order.
/// Stops when the stream ends or the sink rejects (slow-consumer overrun). On
/// overrun it signals `cancel` so the read loop runs the shared teardown path
/// (lossy-on-overrun by disconnection, never silent drop).
fn spawn_pump(
    mut stream: datamancer::EventStream,
    sink: Arc<dyn EventSink>,
    cancel: Arc<tokio::sync::Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(ev) = stream.next().await {
            match sink.publish(ev).await {
                PublishOutcome::Delivered => {}
                PublishOutcome::Rejected(_) => {
                    tracing::warn!("ws sink rejected event (slow consumer); stopping pump");
                    cancel.notify_one();
                    break;
                }
            }
        }
        let _ = sink.flush().await;
    })
}

#[cfg(test)]
mod tests {
    use super::ct_eq;

    #[test]
    fn ct_eq_matches_only_identical_bytes() {
        assert!(ct_eq(b"s3cr3t-token", b"s3cr3t-token"));
        assert!(ct_eq(b"", b""));
        // Differing content of equal length.
        assert!(!ct_eq(b"s3cr3t-token", b"s3cr3t-toke_"));
        // Differing lengths (prefix match) must not compare equal.
        assert!(!ct_eq(b"s3cr3t", b"s3cr3t-token"));
        assert!(!ct_eq(b"s3cr3t-token", b"s3cr3t"));
        // A wrong token that shares no prefix.
        assert!(!ct_eq(b"correct-horse", b"battery-staple"));
    }
}
