//! The same-host iceoryx2 client: bundles the three attaches a consumer
//! previously hand-assembled — the UDS control connection (newline-JSON
//! `open-client`/`subscribe`/…), the shared-memory data + announcement
//! subscriber, and (via the UDS `snapshot` op) diagnostics — behind one
//! [`Client`] handle. The transport crate's `DataSubscriber` and the
//! diagnostics-plane subscriber remain public as lower-level escape hatches.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use datamancer_core::{
    ControlKind, InstrumentEntry, InstrumentInfo, MarketEvent, ProviderId, SystemSnapshot,
};
use datamancer_transport_iceoryx2::DataSubscriber;
use futures::Stream;
// `::` prefix: this module is itself named `iceoryx2`, so the extern crate is
// named explicitly (bare paths here happen to resolve to the crate today, but
// only because this module contains no item named `iceoryx2`).
use ::iceoryx2::prelude::{NodeBuilder, ipc_threadsafe};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
#[cfg(windows)]
use tokio::io::{ReadHalf, WriteHalf};
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(unix)]
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
#[cfg(windows)]
use tokio::net::windows::named_pipe::NamedPipeClient;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::client::Client;
use crate::error::ClientError;
use crate::protocol::uds::{Reply, Request};
use crate::spec::{QueryId, QuerySpec, SubscriptionSpec, UnsubscribeSpec};

/// Connection parameters for [`Iceoryx2Client`].
#[derive(Debug, Clone)]
pub struct Iceoryx2Config {
    /// Path to datamancerd's UDS control socket.
    pub control_socket: PathBuf,
    /// This client's name for `open-client` (unique per daemon).
    pub client_name: String,
    /// Sleep between empty shm polls. The poll loop drains everything
    /// available each pass, so this bounds added latency only when idle.
    pub poll_interval: Duration,
    /// Bound on locally buffered, not-yet-consumed events.
    pub event_buffer: usize,
}

/// Transport-layer failures for [`Iceoryx2Client`].
#[derive(Debug, thiserror::Error)]
pub enum Iceoryx2ClientError {
    /// Control socket I/O failure.
    #[error("control socket i/o: {0}")]
    Io(#[from] std::io::Error),
    /// Control-frame codec failure.
    #[error("control codec: {0}")]
    Codec(#[from] serde_json::Error),
    /// The control protocol was violated (unexpected shape/EOF).
    #[error("control protocol: {0}")]
    Protocol(String),
    /// The iceoryx2 transport crate failed.
    #[error("iceoryx2 transport: {0}")]
    Transport(#[from] datamancer_transport_iceoryx2::TransportError),
    /// Creating the iceoryx2 `Node` (the shared-memory attach) failed. This is
    /// distinct from `Transport`: it originates in the `iceoryx2` crate
    /// itself (`NodeCreationFailure`), one layer below the service/port
    /// errors `datamancer-transport-iceoryx2` funnels into `Transport`.
    #[error("iceoryx2 node creation failed: {0}")]
    Node(String),
}

/// Extract the numeric client id from the `open-client` reply's service name.
/// The id sits in the trailing `.../data/{id}` segments regardless of the
/// daemon's configured service prefix (`service_prefix` in the daemon's
/// config is not fixed to `"datamancer"`, so this must not hardcode it).
fn parse_client_id(service: &str) -> Result<u64, Iceoryx2ClientError> {
    let mut segments = service.rsplit('/');
    let id = segments.next();
    let marker = segments.next();
    match (marker, id) {
        (Some("data"), Some(id)) => id.parse().ok(),
        _ => None,
    }
    .ok_or_else(|| {
        Iceoryx2ClientError::Protocol(format!("unparseable data-service name: {service}"))
    })
}

/// Map a control [`Reply`] to the two-layer error model.
fn check(reply: Reply) -> Result<Reply, ClientError<Iceoryx2ClientError>> {
    if reply.ok {
        Ok(reply)
    } else {
        Err(ClientError::Control {
            code: reply.code.unwrap_or_default(),
            message: reply.message.unwrap_or_default(),
        })
    }
}

/// The serially-used control connection (strict request→reply per line). UDS
/// on Unix; a named pipe on Windows — the newline-JSON framing is identical.
#[cfg(unix)]
struct ControlConn {
    lines: Lines<BufReader<OwnedReadHalf>>,
    write: OwnedWriteHalf,
}

#[cfg(windows)]
struct ControlConn {
    lines: Lines<BufReader<ReadHalf<NamedPipeClient>>>,
    write: WriteHalf<NamedPipeClient>,
}

impl ControlConn {
    #[cfg(unix)]
    async fn connect(path: &Path) -> Result<Self, Iceoryx2ClientError> {
        let stream = UnixStream::connect(path).await?;
        let (read, write) = stream.into_split();
        Ok(Self {
            lines: BufReader::new(read).lines(),
            write,
        })
    }

    #[cfg(windows)]
    async fn connect(path: &Path) -> Result<Self, Iceoryx2ClientError> {
        // The control-socket `Path` carries the pipe name on Windows
        // (`\\.\pipe\datamancer\<user>\control`; see `crate::paths`).
        // `connect_verified` retries `ERROR_PIPE_BUSY` and — critically —
        // rejects the pipe unless its owner SID is this user's (review B1), so
        // credentials never flow to a foreign-owner endpoint (a pipe owned by
        // a different user).
        let stream = crate::win_pipe::connect_verified(path).await?;
        let (read, write) = tokio::io::split(stream);
        Ok(Self {
            lines: BufReader::new(read).lines(),
            write,
        })
    }

    async fn request(&mut self, req: &Request) -> Result<Reply, Iceoryx2ClientError> {
        let mut buf = serde_json::to_vec(req)?;
        buf.push(b'\n');
        self.write.write_all(&buf).await?;
        let line = self.lines.next_line().await?.ok_or_else(|| {
            Iceoryx2ClientError::Protocol("control connection closed mid-request".to_string())
        })?;
        Ok(serde_json::from_str(&line)?)
    }
}

/// A command for the control-connection task.
enum ControlCmd {
    /// A round trip whose reply the caller awaits.
    Request {
        /// The request to send.
        request: Request,
        /// Where to deliver the reply (or transport error) once it arrives.
        reply: tokio::sync::oneshot::Sender<Result<Reply, Iceoryx2ClientError>>,
    },
    /// Fire-and-forget. Exists so `Drop` — which cannot await — can still send
    /// a `cancel-query`. Used by `QueryStream::drop`.
    Fire(Request),
}

/// A cloneable handle to the task that owns the control connection.
///
/// The connection is serially used (strict request→reply per line), so exactly
/// one task owns it and callers queue commands. The channel is **unbounded**:
/// that makes [`ControlHandle::fire`] synchronous and infallible while the task
/// lives, which is what makes drop-cancellation reliable. Volume is bounded in
/// practice by in-flight queries plus concurrent user calls.
#[derive(Clone)]
struct ControlHandle {
    tx: tokio::sync::mpsc::UnboundedSender<ControlCmd>,
}

impl ControlHandle {
    /// Spawn the task that owns `conn` and returns a handle to it.
    fn spawn(mut conn: ControlConn) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ControlCmd>();
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    ControlCmd::Request { request, reply } => {
                        let result = conn.request(&request).await;
                        let failed = result.is_err();
                        let _ = reply.send(result);
                        if failed {
                            // The connection is unusable; every later caller
                            // gets `Protocol` from the closed-channel arm.
                            break;
                        }
                    }
                    ControlCmd::Fire(request) => {
                        if conn.request(&request).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Self { tx }
    }

    /// Send `request` to the control task and await its reply.
    async fn request(&self, request: &Request) -> Result<Reply, Iceoryx2ClientError> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(ControlCmd::Request {
                request: request.clone(),
                reply: reply_tx,
            })
            .map_err(|_| Iceoryx2ClientError::Protocol("control connection closed".to_string()))?;
        reply_rx.await.map_err(|_| {
            Iceoryx2ClientError::Protocol("control connection closed mid-request".to_string())
        })?
    }

    /// Enqueue a request whose reply is discarded. Synchronous and infallible
    /// while the control task lives — callable from `Drop`. Used by
    /// `QueryStream::drop`.
    fn fire(&self, request: Request) {
        let _ = self.tx.send(ControlCmd::Fire(request));
    }
}

/// Attach to the data service for `service_id` and stream its events.
///
/// The attach happens on the blocking task (the `Node` must live on the thread
/// that polls it) and its result is signalled back over a oneshot, so an attach
/// failure surfaces as a `ClientError::Transport` instead of a silently-ended
/// stream. Used for both the client's live stream and each query's stream — a
/// query channel is byte-identical to a client channel, only short-lived.
///
/// The poll loop's `Err(_) => return` arm ends the stream on any service
/// failure, which carries two different meanings depending on the caller: for
/// a client's live stream it means the daemon dropped the client's services
/// (a lost connection); for a query it is the **normal** end, reached when the
/// daemon reaps the finished query's service once it has delivered every
/// event.
async fn spawn_subscriber(
    service_id: u64,
    poll_interval: Duration,
    event_buffer: usize,
    stop: Arc<AtomicBool>,
) -> Result<ReceiverStream<MarketEvent>, Iceoryx2ClientError> {
    let (ev_tx, ev_rx) = mpsc::channel(event_buffer.max(1));
    let (attach_tx, attach_rx) = tokio::sync::oneshot::channel::<Result<(), Iceoryx2ClientError>>();
    tokio::task::spawn_blocking(move || {
        let node = match NodeBuilder::new().create::<ipc_threadsafe::Service>() {
            Ok(node) => node,
            Err(e) => {
                let _ = attach_tx.send(Err(Iceoryx2ClientError::Node(e.to_string())));
                return;
            }
        };
        let mut subscriber = match DataSubscriber::open(&node, service_id) {
            Ok(s) => s,
            Err(e) => {
                let _ = attach_tx.send(Err(Iceoryx2ClientError::from(e)));
                return;
            }
        };
        // Attach succeeded: tell the caller it can return `Ok`, then fall
        // through into the poll loop on this same thread/Node.
        if attach_tx.send(Ok(())).is_err() {
            // The caller gave up waiting (e.g. it was cancelled) — nothing
            // else can observe this stream, so there is no point in
            // polling. `subscriber`/`node` drop here, releasing the attach.
            return;
        }
        while !stop.load(Ordering::Relaxed) {
            match subscriber.poll() {
                Ok(events) if events.is_empty() => std::thread::sleep(poll_interval),
                Ok(events) => {
                    for ev in events {
                        if ev_tx.blocking_send(ev).is_err() {
                            return; // consumer dropped the stream
                        }
                    }
                }
                // Service gone: for a client's live stream the daemon dropped
                // the client's services; for a query this is the normal end,
                // reached when the daemon reaps the finished query's service.
                Err(_) => return,
            }
        }
    });

    match attach_rx.await {
        Ok(Ok(())) => Ok(ReceiverStream::new(ev_rx)),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(Iceoryx2ClientError::Node(
            "shm-attach task ended without reporting a result".to_string(),
        )),
    }
}

/// A bounded historical event stream.
///
/// **Dropping this stream cancels the query** — an abandoned backtest must not
/// keep the daemon fetching. Cancellation is a fire-and-forget `cancel-query`
/// on the control task's unbounded channel, which is why `Drop` can send it
/// without awaiting. A stream that already observed `SessionClosing` sends
/// nothing: the daemon has already reaped that query, and asking it to cancel
/// an id it no longer has would only produce `unknown_query` and log noise.
pub struct QueryStream {
    inner: ReceiverStream<MarketEvent>,
    id: QueryId,
    control: ControlHandle,
    stop: Arc<AtomicBool>,
    finished: bool,
}

impl Stream for QueryStream {
    type Item = MarketEvent;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let polled = std::pin::Pin::new(&mut self.inner).poll_next(cx);
        match &polled {
            std::task::Poll::Ready(None) => self.finished = true,
            std::task::Poll::Ready(Some(MarketEvent::Control(control)))
                if matches!(control.kind, ControlKind::SessionClosing) =>
            {
                self.finished = true;
            }
            _ => {}
        }
        polled
    }
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if !self.finished {
            self.control.fire(Request::CancelQuery { query: self.id });
        }
    }
}

/// A connected same-host client. See [`Client`] for the transport-agnostic
/// contract; iceoryx2-specific behavior: loss surfaces **in-band** as
/// `Control::Gap` (the daemon's resume buffer numbers evictions), and the
/// event stream ends when the daemon drops the per-client services.
pub struct Iceoryx2Client {
    control: ControlHandle,
    client_name: String,
    stop: Arc<AtomicBool>,
    /// Sleep between empty shm polls; carried past `connect` so a later
    /// [`Client::query`] can spawn its own subscriber with the same cadence.
    poll_interval: Duration,
    /// Bound on locally buffered, not-yet-consumed events for a subscriber
    /// spawned after `connect` (i.e. a query's channel).
    event_buffer: usize,
    /// Set by [`Client::close`] so [`Drop`] does not fire a second
    /// `close-client` (which would only answer `unknown_client`).
    closed: bool,
}

/// Dropping a client tears its daemon-side session down.
///
/// The control connection lives in a task behind a cloneable handle, and a
/// [`QueryStream`] holds one of those clones without borrowing the client — so
/// `let (id, s) = client.query(&spec).await?; drop(client);` leaves the socket
/// open for as long as the query stream is held, and EOF alone would no longer
/// tell the daemon to tear the client down. This makes the teardown explicit
/// rather than a side effect of the socket closing: fire-and-forget
/// `close-client` on the unbounded channel (synchronous, so it is callable
/// from `Drop`), plus the stop flag for the local poll task.
impl Drop for Iceoryx2Client {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        self.stop.store(true, Ordering::Relaxed);
        self.control.fire(Request::CloseClient {
            client: self.client_name.clone(),
        });
    }
}

impl Client for Iceoryx2Client {
    type Config = Iceoryx2Config;
    type Error = Iceoryx2ClientError;
    type Events = ReceiverStream<MarketEvent>;

    async fn connect(cfg: Self::Config) -> Result<(Self, Self::Events), ClientError<Self::Error>> {
        let mut control = ControlConn::connect(&cfg.control_socket)
            .await
            .map_err(ClientError::Transport)?;
        let reply = control
            .request(&Request::OpenClient {
                client: cfg.client_name.clone(),
                subscriptions: vec![],
            })
            .await
            .map_err(ClientError::Transport)?;
        let reply = check(reply)?;
        let service = reply.service.ok_or_else(|| {
            ClientError::Transport(Iceoryx2ClientError::Protocol(
                "open-client reply missing service name".to_string(),
            ))
        })?;
        let client_id = parse_client_id(&service).map_err(ClientError::Transport)?;

        let stop = Arc::new(AtomicBool::new(false));
        // Shm attach (node create + subscriber open) must surface as a
        // `connect` failure, not an eprintln plus a silently-ended stream —
        // the spec's error contract treats it as a `ClientError::Transport`.
        let events = spawn_subscriber(
            client_id,
            cfg.poll_interval,
            cfg.event_buffer,
            Arc::clone(&stop),
        )
        .await
        .map_err(ClientError::Transport)?;

        Ok((
            Iceoryx2Client {
                control: ControlHandle::spawn(control),
                client_name: cfg.client_name,
                stop,
                poll_interval: cfg.poll_interval,
                event_buffer: cfg.event_buffer,
                closed: false,
            },
            events,
        ))
    }

    async fn subscribe(&mut self, spec: &SubscriptionSpec) -> Result<(), ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::Subscribe {
                client: self.client_name.clone(),
                spec: spec.clone(),
            })
            .await
            .map_err(ClientError::Transport)?;
        check(reply).map(|_| ())
    }

    async fn unsubscribe(
        &mut self,
        spec: &UnsubscribeSpec,
    ) -> Result<(), ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::Unsubscribe {
                client: self.client_name.clone(),
                spec: spec.clone(),
            })
            .await
            .map_err(ClientError::Transport)?;
        check(reply).map(|_| ())
    }

    async fn snapshot(&mut self) -> Result<SystemSnapshot, ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::Snapshot)
            .await
            .map_err(ClientError::Transport)?;
        let reply = check(reply)?;
        reply.snapshot.ok_or_else(|| {
            ClientError::Transport(Iceoryx2ClientError::Protocol(
                "ok snapshot reply missing snapshot payload".to_string(),
            ))
        })
    }

    async fn instruments(
        &mut self,
        provider: Option<&ProviderId>,
    ) -> Result<Vec<InstrumentInfo>, ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::Instruments {
                provider: provider.map(|p| p.as_str().to_string()),
            })
            .await
            .map_err(ClientError::Transport)?;
        let reply = check(reply)?;
        reply.instruments.ok_or_else(|| {
            ClientError::Transport(Iceoryx2ClientError::Protocol(
                "ok instruments reply missing instruments payload".to_string(),
            ))
        })
    }

    async fn capabilities(
        &mut self,
        provider: &ProviderId,
        symbols: &[String],
    ) -> Result<Vec<InstrumentEntry>, ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::Capabilities {
                provider: provider.as_str().to_string(),
                symbols: symbols.to_vec(),
            })
            .await
            .map_err(ClientError::Transport)?;
        let reply = check(reply)?;
        reply.capabilities.ok_or_else(|| {
            ClientError::Transport(Iceoryx2ClientError::Protocol(
                "ok capabilities reply missing capabilities payload".to_string(),
            ))
        })
    }

    /// Graceful close. **Known race:** the daemon emits a terminal
    /// `SessionClosing` on the data plane before tearing the service down,
    /// but this client's poll loop can observe the service go away (an
    /// `Err` from `subscriber.poll()`, which ends the event stream) before it
    /// drains that final sample. The closer already knows the close was
    /// intentional — it is the one that called `close` — so this is narrow
    /// and pre-existing; stream-readers on the iceoryx2 transport should not
    /// rely on always observing the `SessionClosing` marker (unlike the WS
    /// transport, which is single-writer and does not have this race).
    async fn close(mut self) -> Result<(), ClientError<Self::Error>> {
        // `close` consumes the client, so this is the caller's last chance to
        // signal the poll task. Set the stop flag unconditionally *before* the
        // round-trip: a transport failure below must not leave the
        // spawn_blocking loop (and its Node/DataSubscriber) running forever.
        self.stop.store(true, Ordering::Relaxed);
        // The explicit close owns the teardown from here; `Drop` (which runs
        // when this call returns, on both the ok and error paths) must not
        // repeat the `close-client`.
        self.closed = true;
        let reply = self
            .control
            .request(&Request::CloseClient {
                client: self.client_name.clone(),
            })
            .await
            .map_err(ClientError::Transport)?;
        check(reply).map(|_| ())
    }

    type Query = QueryStream;

    /// Open a bounded historical query and attach to its data service.
    ///
    /// # Attach race
    ///
    /// The daemon starts pumping the query as soon as it replies, so a query
    /// that completes almost immediately (empty range, fully cached) can drain
    /// — and have the daemon reap its service — before this call attaches.
    /// That surfaces as `ClientError::Transport` for what was a legitimately
    /// empty result, indistinguishable from a real attach failure. This race
    /// is documented and accepted, not mitigated — the daemon does not hold
    /// the service open to cover it. Callers that must tell the two apart
    /// should treat a transport error on a very short/empty range as
    /// inconclusive and retry the query.
    async fn query(
        &mut self,
        spec: &QuerySpec,
    ) -> Result<(QueryId, Self::Query), ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::OpenQuery { spec: spec.clone() })
            .await
            .map_err(ClientError::Transport)?;
        let reply = check(reply)?;
        let id = reply.query.ok_or_else(|| {
            ClientError::Transport(Iceoryx2ClientError::Protocol(
                "open-query reply missing query id".to_string(),
            ))
        })?;
        let stop = Arc::new(AtomicBool::new(false));
        // The query is already open daemon-side. If attaching fails we must
        // cancel it here, or it holds one of the daemon's few query slots (and
        // keeps paging the provider) until it finishes on its own.
        let events =
            match spawn_subscriber(id, self.poll_interval, self.event_buffer, Arc::clone(&stop))
                .await
            {
                Ok(events) => events,
                Err(e) => {
                    self.control.fire(Request::CancelQuery { query: id });
                    return Err(ClientError::Transport(e));
                }
            };
        Ok((
            id,
            QueryStream {
                inner: events,
                id,
                control: self.control.clone(),
                stop,
                finished: false,
            },
        ))
    }

    async fn cancel_query(&mut self, query: QueryId) -> Result<(), ClientError<Self::Error>> {
        let reply = self
            .control
            .request(&Request::CancelQuery { query })
            .await
            .map_err(ClientError::Transport)?;
        check(reply).map(|_| ())
    }
}

impl Iceoryx2Client {
    /// Raw control round-trip, `pub(crate)` for the `app` facade's credential
    /// methods (see `crate::app::AppHandle`). Deliberately **not** part of
    /// the `Client` trait: credential ops are same-host/UDS-only and must
    /// not appear on the transport-generic trait the WS client also
    /// implements.
    ///
    /// `#[cfg(not(windows))]`: the Windows hybrid `AppHandle` routes admin ops
    /// through `PipeControlClient` (the owner-DACL pipe), not this client, so
    /// this method has no Windows consumer.
    #[cfg(not(windows))]
    pub(crate) async fn control_request(
        &mut self,
        req: &Request,
    ) -> Result<Reply, ClientError<Iceoryx2ClientError>> {
        let reply = self
            .control
            .request(req)
            .await
            .map_err(ClientError::Transport)?;
        check(reply)
    }
}

#[cfg(test)]
mod tests {
    use super::parse_client_id;
    #[cfg(unix)]
    use super::{ControlConn, ControlHandle, Iceoryx2ClientError, QueryStream};
    #[cfg(unix)]
    use crate::codes;
    #[cfg(unix)]
    use crate::error::ClientError;
    #[cfg(unix)]
    use crate::protocol::uds::{Reply, Request};
    #[cfg(unix)]
    use datamancer_core::{Control, ControlKind, MarketEvent, Seq, Timestamp};
    #[cfg(unix)]
    use std::sync::Arc;
    #[cfg(unix)]
    use std::sync::Mutex;
    #[cfg(unix)]
    use std::sync::atomic::AtomicBool;
    #[cfg(unix)]
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    #[cfg(unix)]
    use tokio::net::UnixListener;
    #[cfg(unix)]
    use tokio_stream::wrappers::ReceiverStream;

    #[test]
    fn client_id_parses_from_the_service_name() {
        assert_eq!(parse_client_id("datamancer/data/3").unwrap(), 3);
        assert_eq!(parse_client_id("datamancer/data/40").unwrap(), 40);
        assert!(parse_client_id("datamancer/data/").is_err());
        assert!(parse_client_id("nonsense").is_err());
        assert!(parse_client_id("datamancer/data/not-a-number").is_err());
    }

    /// The id is extracted from the trailing `/data/{id}` segments
    /// regardless of the daemon's configured `service_prefix` — it need not
    /// be literally `"datamancer"`.
    #[test]
    fn client_id_parses_regardless_of_daemon_service_prefix() {
        assert_eq!(parse_client_id("datamancerd/data/40").unwrap(), 40);
        assert_eq!(parse_client_id("custom-prefix/data/7").unwrap(), 7);
        assert!(parse_client_id("data/").is_err());
        assert!(parse_client_id("prefix/data/not-a-number").is_err());
        assert!(parse_client_id("prefix/notdata/3").is_err());
        assert!(parse_client_id("3").is_err());
    }

    /// Scripted fake UDS daemon: reads one request line, sends one reply line.
    /// UDS-only; the Windows named-pipe control path is exercised by the
    /// Phase 3 runtime tests.
    #[cfg(unix)]
    fn fake_uds(replies: Vec<Reply>) -> std::path::PathBuf {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("control.sock");
        let listener = UnixListener::bind(&path).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            for reply in replies {
                let _ = lines.next_line().await.unwrap();
                let mut buf = serde_json::to_vec(&reply).unwrap();
                buf.push(b'\n');
                write.write_all(&buf).await.unwrap();
            }
        });
        path
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn control_conn_round_trips_a_request() {
        let path = fake_uds(vec![Reply::service("datamancer/data/7")]);
        let mut conn = ControlConn::connect(&path).await.unwrap();
        let reply = conn
            .request(&Request::OpenClient {
                client: "test-client".to_string(),
                subscriptions: vec![],
            })
            .await
            .unwrap();
        assert!(reply.ok);
        assert_eq!(reply.service.as_deref(), Some("datamancer/data/7"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn control_error_reply_maps_to_control_error() {
        let path = fake_uds(vec![Reply::error(codes::DUPLICATE_CLIENT, "name in use")]);
        let mut conn = ControlConn::connect(&path).await.unwrap();
        let reply = conn
            .request(&Request::OpenClient {
                client: "taken".to_string(),
                subscriptions: vec![],
            })
            .await
            .unwrap();
        match super::check(reply) {
            Err(ClientError::<Iceoryx2ClientError>::Control { code, .. }) => {
                assert_eq!(code, codes::DUPLICATE_CLIENT);
            }
            other => panic!("expected Control error, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn close_sets_the_stop_flag_even_when_the_transport_fails() {
        use super::Iceoryx2Client;
        use crate::client::Client as _;
        use std::sync::atomic::Ordering;

        // A fake daemon that accepts and immediately hangs up: the
        // close-client round-trip fails at the transport layer (connection
        // closed before any reply line arrives).
        let path = fake_uds(vec![]);
        let control = ControlConn::connect(&path).await.unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let client = Iceoryx2Client {
            control: ControlHandle::spawn(control),
            client_name: "doomed".to_string(),
            stop: Arc::clone(&stop),
            poll_interval: std::time::Duration::from_millis(10),
            event_buffer: 16,
            closed: false,
        };
        match client.close().await {
            Err(ClientError::Transport(_)) => {}
            other => panic!("expected transport error, got {other:?}"),
        }
        assert!(
            stop.load(Ordering::Relaxed),
            "close() must signal the poll task even when the request fails — \
             it consumes the client, so this is the last chance"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn control_handle_round_trips_and_fires_without_awaiting() {
        let path = fake_uds(vec![Reply::ok(), Reply::ok()]);
        let conn = ControlConn::connect(&path).await.unwrap();
        let handle = ControlHandle::spawn(conn);

        let reply = handle.request(&Request::ListQueries).await.unwrap();
        assert!(reply.ok);

        // `fire` is synchronous: it must not need an await to enqueue.
        handle.fire(Request::CancelQuery { query: 1 });
    }

    /// Scripted fake UDS daemon that additionally records every line it
    /// received into a shared buffer, and keeps reading past the queued
    /// `replies` so a fire-and-forget request with no queued reply is still
    /// recorded.
    #[cfg(unix)]
    fn fake_uds_recording(replies: Vec<Reply>) -> (std::path::PathBuf, Arc<Mutex<Vec<String>>>) {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("control.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            let mut replies = replies.into_iter();
            while let Ok(Some(line)) = lines.next_line().await {
                recorder.lock().unwrap().push(line);
                if let Some(reply) = replies.next() {
                    let mut buf = serde_json::to_vec(&reply).unwrap();
                    buf.push(b'\n');
                    if write.write_all(&buf).await.is_err() {
                        return;
                    }
                }
            }
        });
        (path, seen)
    }

    #[cfg(unix)]
    fn query_stream_for_tests(
        control: ControlHandle,
        finished: bool,
    ) -> (QueryStream, tokio::sync::mpsc::Sender<MarketEvent>) {
        let (tx, rx) = tokio::sync::mpsc::channel::<MarketEvent>(4);
        let stream = QueryStream {
            inner: ReceiverStream::new(rx),
            id: 42,
            control,
            stop: Arc::new(AtomicBool::new(false)),
            finished,
        };
        (stream, tx)
    }

    /// A terminal `MarketEvent::Control(SessionClosing)`, built with the same
    /// field shape as the crate's other `SessionClosing` construction sites
    /// (see e.g. `datamancer-transport-iceoryx2/src/payload.rs`).
    #[cfg(unix)]
    fn session_closing_event() -> MarketEvent {
        MarketEvent::Control(Control {
            source_ts: Timestamp(1),
            rx_ts: Timestamp(2),
            seq: Seq::SYNTHETIC,
            kind: ControlKind::SessionClosing,
        })
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_an_unfinished_query_stream_cancels_it() {
        let (path, seen) = fake_uds_recording(vec![]);
        let control = ControlHandle::spawn(ControlConn::connect(&path).await.unwrap());
        let (stream, _tx) = query_stream_for_tests(control, false);

        drop(stream);

        // `fire` enqueues synchronously; let the control task drain and write.
        for _ in 0..50 {
            if !seen.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let lines = seen.lock().unwrap().clone();
        let sent: Request = serde_json::from_str(lines.first().expect("a cancel was sent"))
            .expect("parse recorded request");
        assert_eq!(sent, Request::CancelQuery { query: 42 });
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_finished_query_stream_sends_nothing_on_drop() {
        let (path, seen) = fake_uds_recording(vec![]);
        let control = ControlHandle::spawn(ControlConn::connect(&path).await.unwrap());
        let (stream, _tx) = query_stream_for_tests(control, true);

        drop(stream);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            seen.lock().unwrap().is_empty(),
            "a completed query was already reaped; cancelling it would only log noise"
        );
    }

    /// The `finished` flag is what suppresses the cancel, and it is set by
    /// polling a terminal `SessionClosing` through the stream — so exercise
    /// that path rather than setting the flag by hand.
    #[cfg(unix)]
    #[tokio::test]
    async fn polling_session_closing_marks_the_stream_finished() {
        use futures::StreamExt as _;

        let (path, seen) = fake_uds_recording(vec![]);
        let control = ControlHandle::spawn(ControlConn::connect(&path).await.unwrap());
        let (mut stream, tx) = query_stream_for_tests(control, false);

        tx.send(session_closing_event()).await.expect("send");
        let event = stream.next().await.expect("the terminal control");
        assert!(matches!(event, MarketEvent::Control(_)));

        drop(stream);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            seen.lock().unwrap().is_empty(),
            "observing SessionClosing must suppress the drop-cancel"
        );
    }

    #[cfg(unix)]
    fn client_for_tests(control: ControlHandle, stop: Arc<AtomicBool>) -> super::Iceoryx2Client {
        super::Iceoryx2Client {
            control,
            client_name: "dropped".to_string(),
            stop,
            poll_interval: std::time::Duration::from_millis(10),
            event_buffer: 16,
            closed: false,
        }
    }

    /// The control connection lives in a task now, so dropping the client no
    /// longer closes the socket while a `QueryStream` holds a handle clone —
    /// the teardown has to be sent explicitly.
    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_a_client_without_close_tears_the_session_down() {
        use std::sync::atomic::Ordering;

        let (path, seen) = fake_uds_recording(vec![]);
        let control = ControlHandle::spawn(ControlConn::connect(&path).await.unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        // A live query stream keeps a clone of the control handle alive past
        // the client's drop — the case that made the implicit EOF teardown
        // stop working.
        let (_query, _tx) = query_stream_for_tests(control.clone(), true);

        drop(client_for_tests(control, Arc::clone(&stop)));

        for _ in 0..50 {
            if !seen.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let lines = seen.lock().unwrap().clone();
        let sent: Request = serde_json::from_str(lines.first().expect("a close was sent"))
            .expect("parse recorded request");
        assert_eq!(
            sent,
            Request::CloseClient {
                client: "dropped".to_string()
            }
        );
        assert!(
            stop.load(Ordering::Relaxed),
            "the drop must also stop the local poll task"
        );
    }

    /// `close()` owns the teardown; the `Drop` that runs when it returns must
    /// not send a second `close-client`.
    #[cfg(unix)]
    #[tokio::test]
    async fn close_does_not_also_fire_a_drop_close() {
        use crate::client::Client as _;

        let (path, seen) = fake_uds_recording(vec![Reply::ok()]);
        let control = ControlHandle::spawn(ControlConn::connect(&path).await.unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let client = client_for_tests(control, stop);

        client.close().await.expect("close");

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let lines = seen.lock().unwrap().clone();
        assert_eq!(
            lines.len(),
            1,
            "expected exactly one close-client: {lines:?}"
        );
    }
}
