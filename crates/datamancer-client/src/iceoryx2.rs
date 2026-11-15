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
    ControlKind, InstrumentEntry, InstrumentInfo, MarketEvent, ProviderId, Subscribed,
    SystemSnapshot,
};
use datamancer_transport_iceoryx2::{DataServiceName, DataSubscriber, Polled, TransportError};
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

/// The failure of a query's attach: a query whose service is gone, or is
/// being removed by the daemon, was [dropped](ClientError::QueryDropped).
fn attach_error(query: QueryId, error: Iceoryx2ClientError) -> ClientError<Iceoryx2ClientError> {
    match error {
        Iceoryx2ClientError::Transport(
            TransportError::ServiceNotFound(_) | TransportError::StaleService(_),
        ) => ClientError::QueryDropped { query },
        other => ClientError::Transport(other),
    }
}

/// How a service's event stream ended on its own.
#[derive(Debug, Clone, PartialEq)]
enum StreamEnd {
    /// The daemon dropped the service after everything it sent was received.
    Closed,
    /// Receiving from the service failed.
    Failed(TransportError),
}

/// What a service's subscriber thread forwards: an event, or last how the
/// stream ended.
type Forwarded = Result<MarketEvent, StreamEnd>;

/// Forward what `poll` returns into `items` through `wrap`, sleeping
/// `poll_interval` after an empty poll, until `stop` is set, the consumer
/// drops its receiver, or the service's stream ends. How it ended is forwarded
/// last; whatever `wrap` maps to `None` is not forwarded.
fn forward_events<T>(
    mut poll: impl FnMut() -> Result<Polled, TransportError>,
    items: &mpsc::Sender<T>,
    wrap: fn(Forwarded) -> Option<T>,
    stop: &AtomicBool,
    poll_interval: Duration,
) {
    let end = loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        match poll() {
            Ok(Polled::Events(polled)) if polled.is_empty() => {
                // With no events flowing `blocking_send`'s error can never
                // fire, so an idle pass checks for a consumer that vanished
                // without `close()` running (#64's wedge).
                if items.is_closed() {
                    return;
                }
                std::thread::sleep(poll_interval);
            }
            Ok(Polled::Events(polled)) => {
                for item in polled.into_iter().filter_map(|ev| wrap(Ok(ev))) {
                    if items.blocking_send(item).is_err() {
                        return;
                    }
                }
            }
            Ok(Polled::Closed) => break StreamEnd::Closed,
            Err(e) => break StreamEnd::Failed(e),
        }
    };
    if let Some(item) = wrap(Err(end)) {
        let _ = items.blocking_send(item);
    }
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

/// Whether a failed round trip leaves the control connection unusable. A
/// request or reply line that does not decode leaves the line framing intact.
fn breaks_the_connection(error: &Iceoryx2ClientError) -> bool {
    !matches!(error, Iceoryx2ClientError::Codec(_))
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
                        let failed = result.as_ref().is_err_and(breaks_the_connection);
                        let _ = reply.send(result);
                        if failed {
                            // The connection is unusable; every later caller
                            // gets `Protocol` from the closed-channel arm.
                            break;
                        }
                    }
                    ControlCmd::Fire(request) => {
                        if conn
                            .request(&request)
                            .await
                            .is_err_and(|e| breaks_the_connection(&e))
                        {
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

/// Attach to the data service `service` and stream what `wrap` makes of its
/// events and of how its stream ended.
///
/// The attach happens on the blocking task (the `Node` must live on the thread
/// that polls it) and its result is signalled back over a oneshot, so an attach
/// failure surfaces as a `ClientError::Transport` instead of a silently-ended
/// stream. Used for both the client's live stream and each query's stream — a
/// query channel is byte-identical to a client channel, only short-lived.
async fn spawn_subscriber<T: Send + 'static>(
    service: DataServiceName,
    poll_interval: Duration,
    event_buffer: usize,
    stop: Arc<AtomicBool>,
    wrap: fn(Forwarded) -> Option<T>,
) -> Result<ReceiverStream<T>, Iceoryx2ClientError> {
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
        let mut subscriber = match DataSubscriber::open(&node, &service) {
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
        forward_events(|| subscriber.poll(), &ev_tx, wrap, &stop, poll_interval);
    });

    match attach_rx.await {
        Ok(Ok(())) => Ok(ReceiverStream::new(ev_rx)),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(Iceoryx2ClientError::Node(
            "shm-attach task ended without reporting a result".to_string(),
        )),
    }
}

/// What the daemon is doing with a [`QueryStream`]'s query, as far as the
/// stream has seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryState {
    /// Nothing says the daemon has finished with it.
    InFlight,
    /// The stream yielded its terminal `SessionClosing`.
    Delivered,
    /// The daemon dropped its service without one.
    Dropped,
}

/// A bounded historical event stream.
///
/// It yields the query's events and ends after its terminal
/// [`ControlKind::SessionClosing`]. If the query does not get that far, the
/// last item is the error that ended it: [`ClientError::QueryDropped`], or
/// [`ClientError::Transport`] when receiving failed.
///
/// **Dropping this stream cancels the query** — an abandoned backtest must not
/// keep the daemon fetching. Cancellation is a fire-and-forget `cancel-query`
/// on the control task's unbounded channel, which is why `Drop` can send it
/// without awaiting. A stream that already yielded
/// [`ControlKind::SessionClosing`] or [`ClientError::QueryDropped`] sends
/// nothing: the daemon has already
/// reaped that query, and asking it to cancel an id it no longer has would
/// only produce `unknown_query` and log noise.
pub struct QueryStream {
    inner: ReceiverStream<Forwarded>,
    id: QueryId,
    control: ControlHandle,
    stop: Arc<AtomicBool>,
    state: QueryState,
}

impl Stream for QueryStream {
    type Item = Result<MarketEvent, ClientError<Iceoryx2ClientError>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let forwarded = std::task::ready!(std::pin::Pin::new(&mut self.inner).poll_next(cx));
        std::task::Poll::Ready(match forwarded {
            Some(Ok(ev)) => {
                if matches!(&ev, MarketEvent::Control(c) if c.kind == ControlKind::SessionClosing) {
                    self.state = QueryState::Delivered;
                }
                Some(Ok(ev))
            }
            Some(Err(_)) if self.state == QueryState::Delivered => None,
            Some(Err(StreamEnd::Closed)) => {
                self.state = QueryState::Dropped;
                Some(Err(ClientError::QueryDropped { query: self.id }))
            }
            Some(Err(StreamEnd::Failed(e))) => Some(Err(ClientError::Transport(e.into()))),
            None => None,
        })
    }
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if self.state == QueryState::InFlight {
            self.control.fire(Request::CancelQuery { query: self.id });
        }
    }
}

/// A connected same-host client. See [`Client`] for the transport-agnostic
/// contract; iceoryx2-specific behavior: loss surfaces **in-band** as
/// `Control::Gap` (the daemon's resume buffer numbers evictions), and the
/// event stream ends when the daemon drops the per-client services. A killed
/// daemon drops nothing: its stream may stay silent instead of ending.
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
        let service: DataServiceName = service
            .parse()
            .map_err(|e| ClientError::Transport(Iceoryx2ClientError::Transport(e)))?;

        let stop = Arc::new(AtomicBool::new(false));
        // Shm attach (node create + subscriber open) must surface as a
        // `connect` failure, not an eprintln plus a silently-ended stream —
        // the spec's error contract treats it as a `ClientError::Transport`.
        let events = spawn_subscriber(
            service,
            cfg.poll_interval,
            cfg.event_buffer,
            Arc::clone(&stop),
            Result::ok,
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

    async fn subscribe(
        &mut self,
        spec: &SubscriptionSpec,
    ) -> Result<Subscribed, ClientError<Self::Error>> {
        let reply = match self
            .control
            .request(&Request::Subscribe {
                client: self.client_name.clone(),
                spec: spec.clone(),
            })
            .await
        {
            Ok(reply) => reply,
            // The daemon answered, but the reply cannot be read, so it may
            // hold the subscription.
            Err(e @ Iceoryx2ClientError::Codec(_)) => {
                let _ = self.unsubscribe(&spec.clone().into()).await;
                return Err(ClientError::Transport(e));
            }
            Err(e) => return Err(ClientError::Transport(e)),
        };
        let subscribed = check(reply)?.subscribed();
        if let Err(e) = spec.check_subscribed(&subscribed) {
            let _ = self.unsubscribe(&spec.clone().into()).await;
            return Err(e);
        }
        Ok(subscribed)
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

    /// Graceful close. **Known race:** this stops the local poll loop before
    /// asking the daemon to tear the client down, so the event stream can end
    /// before it delivers the daemon's terminal `SessionClosing`. The closer
    /// already knows the close was intentional, as it is the one that called
    /// `close`; stream-readers on the iceoryx2 transport should not rely on
    /// always observing the `SessionClosing` marker (unlike the WS transport,
    /// which is single-writer and does not have this race).
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
    /// The daemon publishes nothing until this call attaches, and holds the
    /// query for a bounded grace period. Attaching within it delivers the
    /// whole query. Otherwise the daemon has given up on the query and nothing
    /// is delivered: this call fails, or, when the attach coincides with the
    /// daemon giving up, the stream's only item is the same error. If the
    /// daemon is killed, the stream may stay silent instead of ending, so a
    /// consumer that must notice bounds its wait for the next item.
    ///
    /// # Errors
    ///
    /// As [`Client::query`]: [`ClientError::QueryDropped`] is the daemon
    /// having given up on the query before this call attached.
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
        // The query is already open daemon-side: every failure from here on
        // must cancel it, or it holds one of the daemon's few query slots
        // (and keeps paging the provider) until it finishes on its own.
        let Some(service) = reply.service else {
            self.control.fire(Request::CancelQuery { query: id });
            return Err(ClientError::Transport(Iceoryx2ClientError::Protocol(
                "open-query reply missing service name".to_string(),
            )));
        };
        let service = match service.parse::<DataServiceName>() {
            Ok(service) => service,
            Err(e) => {
                self.control.fire(Request::CancelQuery { query: id });
                return Err(ClientError::Transport(Iceoryx2ClientError::Transport(e)));
            }
        };
        let stop = Arc::new(AtomicBool::new(false));
        let events = match spawn_subscriber(
            service,
            self.poll_interval,
            self.event_buffer,
            Arc::clone(&stop),
            Some,
        )
        .await
        {
            Ok(events) => events,
            Err(e) => {
                self.control.fire(Request::CancelQuery { query: id });
                return Err(attach_error(id, e));
            }
        };
        Ok((
            id,
            QueryStream {
                inner: events,
                id,
                control: self.control.clone(),
                stop,
                state: QueryState::InFlight,
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
    #[cfg(unix)]
    use super::Iceoryx2ClientError;
    #[cfg(unix)]
    use super::{
        ControlConn, ControlHandle, Forwarded, QueryState, QueryStream, StreamEnd, attach_error,
        forward_events,
    };
    #[cfg(unix)]
    use crate::codes;
    #[cfg(unix)]
    use crate::error::ClientError;
    #[cfg(unix)]
    use crate::protocol::uds::{Reply, Request};
    #[cfg(unix)]
    use datamancer_core::{Control, ControlKind, MarketEvent, Seq, Timestamp};
    #[cfg(unix)]
    use datamancer_transport_iceoryx2::{Polled, TransportError};
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
        let path = fake_uds(vec![Reply::service("datamancerd/v3/data/7")]);
        let mut conn = ControlConn::connect(&path).await.unwrap();
        let reply = conn
            .request(&Request::OpenClient {
                client: "test-client".to_string(),
                subscriptions: vec![],
            })
            .await
            .unwrap();
        assert!(reply.ok);
        assert_eq!(reply.service.as_deref(), Some("datamancerd/v3/data/7"));
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

    /// A refused subscribe is the refusal, not a created session.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_subscribe_error_reply_is_not_read_as_created() {
        use super::Iceoryx2Client;
        use crate::client::Client as _;
        use crate::spec::SubscriptionSpec;

        let path = fake_uds(vec![Reply::error(codes::DUPLICATE_CLIENT, "name in use")]);
        let mut client = Iceoryx2Client {
            control: ControlHandle::spawn(ControlConn::connect(&path).await.unwrap()),
            client_name: "c".to_string(),
            stop: Arc::new(AtomicBool::new(false)),
            poll_interval: std::time::Duration::from_millis(10),
            event_buffer: 16,
            closed: false,
        };
        let spec: SubscriptionSpec = serde_json::from_str(
            r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
        )
        .unwrap();
        match client.subscribe(&spec).await {
            Err(ClientError::Control { code, .. }) => assert_eq!(code, codes::DUPLICATE_CLIENT),
            other => panic!("expected the refusal, got {other:?}"),
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
    async fn a_reply_with_an_unreadable_scope_unsubscribes_first() {
        use super::Iceoryx2Client;
        use crate::client::Client as _;
        use crate::spec::SubscriptionSpec;

        let subscribed = serde_json::to_string(&Reply::from_subscribed(
            datamancer_core::Subscribed::created(datamancer_core::LiveScope::live()),
        ))
        .unwrap()
        .replace(r#""scope":"live""#, r#""scope":"live_forever""#);
        let (path, seen) = fake_uds_lines(vec![
            subscribed,
            serde_json::to_string(&Reply::ok()).unwrap(),
        ]);
        let mut client = Iceoryx2Client {
            control: ControlHandle::spawn(ControlConn::connect(&path).await.unwrap()),
            client_name: "c".to_string(),
            stop: Arc::new(AtomicBool::new(false)),
            poll_interval: std::time::Duration::from_millis(10),
            event_buffer: 16,
            closed: false,
        };
        let spec: SubscriptionSpec = serde_json::from_str(
            r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
        )
        .unwrap();
        match client.subscribe(&spec).await {
            Err(ClientError::Transport(Iceoryx2ClientError::Codec(_))) => {}
            other => panic!("expected an unreadable reply, got {other:?}"),
        }
        let lines = seen.lock().unwrap().clone();
        let sent: Vec<Request> = lines
            .iter()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(
            matches!(sent.as_slice(), [Request::Subscribe { .. }, Request::Unsubscribe { spec, .. }] if spec.symbol == "BTC/USD"),
            "the client unsubscribes first: {sent:?}"
        );
    }

    /// Like [`fake_uds_recording`], with each reply a raw line.
    #[cfg(unix)]
    fn fake_uds_lines(replies: Vec<String>) -> (std::path::PathBuf, Arc<Mutex<Vec<String>>>) {
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
                if let Some(reply) = replies.next()
                    && write
                        .write_all(format!("{reply}\n").as_bytes())
                        .await
                        .is_err()
                {
                    return;
                }
            }
        });
        (path, seen)
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
        state: QueryState,
    ) -> (QueryStream, tokio::sync::mpsc::Sender<Forwarded>) {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let stream = QueryStream {
            inner: ReceiverStream::new(rx),
            id: 42,
            control,
            stop: Arc::new(AtomicBool::new(false)),
            state,
        };
        (stream, tx)
    }

    /// Everything a query stream yields when its subscriber thread forwards
    /// `forwarded` and ends, the stream itself, and the lines its fake daemon
    /// has received.
    #[cfg(unix)]
    async fn query_stream_fed(
        forwarded: Vec<Forwarded>,
    ) -> (
        Vec<Result<MarketEvent, ClientError<Iceoryx2ClientError>>>,
        QueryStream,
        Arc<Mutex<Vec<String>>>,
    ) {
        use futures::StreamExt as _;
        let (path, seen) = fake_uds_recording(vec![]);
        let control = ControlHandle::spawn(ControlConn::connect(&path).await.unwrap());
        let (mut stream, tx) = query_stream_for_tests(control, QueryState::InFlight);
        for item in forwarded {
            tx.send(item).await.unwrap();
        }
        drop(tx);
        let mut yielded = Vec::new();
        while let Some(item) = stream.next().await {
            yielded.push(item);
        }
        (yielded, stream, seen)
    }

    /// The first request the fake daemon receives within half a second.
    #[cfg(unix)]
    async fn first_request(seen: &Mutex<Vec<String>>) -> Option<Request> {
        for _ in 0..50 {
            if let Some(line) = seen.lock().unwrap().first() {
                return Some(serde_json::from_str(line).expect("parse recorded request"));
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        None
    }

    /// Forward what `polls` yields, in order, through `wrap` into a channel;
    /// returns what the channel received.
    #[cfg(unix)]
    fn forward<T>(
        polls: Vec<Result<Polled, TransportError>>,
        wrap: fn(Forwarded) -> Option<T>,
    ) -> Vec<T> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let mut polls = polls.into_iter();
        forward_events(
            || polls.next().expect("the forwarding ends within the script"),
            &tx,
            wrap,
            &AtomicBool::new(false),
            std::time::Duration::ZERO,
        );
        drop(tx);
        let mut received = Vec::new();
        while let Ok(item) = rx.try_recv() {
            received.push(item);
        }
        received
    }

    #[cfg(unix)]
    #[test]
    fn a_closed_service_is_forwarded_after_its_events() {
        let received = forward(
            vec![
                Ok(Polled::Events(vec![session_closing_event()])),
                Ok(Polled::Events(Vec::new())),
                Ok(Polled::Closed),
            ],
            Some,
        );
        assert_eq!(
            received,
            vec![Ok(session_closing_event()), Err(StreamEnd::Closed)]
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_receive_failure_is_forwarded_as_a_failure() {
        let failure = TransportError::Receive("receive failed".to_string());
        let received = forward(vec![Err(failure.clone())], Some);
        assert_eq!(received, vec![Err(StreamEnd::Failed(failure))]);
    }

    #[cfg(unix)]
    #[test]
    fn the_live_stream_carries_events_only() {
        let received = forward(
            vec![
                Ok(Polled::Events(vec![session_closing_event()])),
                Ok(Polled::Closed),
            ],
            Result::ok,
        );
        assert_eq!(received, vec![session_closing_event()]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_query_closed_after_session_closing_ends_without_an_error() {
        let (yielded, stream, seen) =
            query_stream_fed(vec![Ok(session_closing_event()), Err(StreamEnd::Closed)]).await;
        assert!(matches!(yielded.as_slice(), [Ok(MarketEvent::Control(_))]));

        drop(stream);
        assert_eq!(first_request(&seen).await, None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_query_closed_without_session_closing_ends_with_query_dropped() {
        let (yielded, stream, seen) = query_stream_fed(vec![Err(StreamEnd::Closed)]).await;
        assert!(matches!(
            yielded.as_slice(),
            [Err(ClientError::QueryDropped { query: 42 })]
        ));

        drop(stream);
        assert_eq!(first_request(&seen).await, None, "the daemon dropped it");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_query_whose_receive_fails_ends_with_the_failure_and_is_cancelled_on_drop() {
        let failure = TransportError::Receive("receive failed".to_string());
        let (yielded, stream, seen) =
            query_stream_fed(vec![Err(StreamEnd::Failed(failure.clone()))]).await;
        assert!(matches!(
            yielded.as_slice(),
            [Err(ClientError::Transport(Iceoryx2ClientError::Transport(e)))] if *e == failure
        ));

        drop(stream);
        assert_eq!(
            first_request(&seen).await,
            Some(Request::CancelQuery { query: 42 })
        );
    }

    #[cfg(unix)]
    #[test]
    fn attaching_to_a_reaped_query_reports_it_dropped() {
        let reaped = Iceoryx2ClientError::Transport(TransportError::ServiceNotFound(
            "p/v3/data/7".to_string(),
        ));
        assert!(matches!(
            attach_error(7, reaped),
            ClientError::QueryDropped { query: 7 }
        ));
        let being_removed =
            Iceoryx2ClientError::Transport(TransportError::StaleService("p/v3/data/7".to_string()));
        assert!(matches!(
            attach_error(7, being_removed),
            ClientError::QueryDropped { query: 7 }
        ));
        let other = Iceoryx2ClientError::Node("no shared memory".to_string());
        assert!(matches!(
            attach_error(7, other),
            ClientError::Transport(Iceoryx2ClientError::Node(_))
        ));
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
    async fn dropping_a_query_stream_still_in_flight_cancels_it() {
        let (path, seen) = fake_uds_recording(vec![]);
        let control = ControlHandle::spawn(ControlConn::connect(&path).await.unwrap());
        let (stream, _tx) = query_stream_for_tests(control, QueryState::InFlight);

        drop(stream);

        assert_eq!(
            first_request(&seen).await,
            Some(Request::CancelQuery { query: 42 })
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_query_stream_that_yielded_session_closing_sends_nothing_on_drop() {
        use futures::StreamExt as _;

        let (path, seen) = fake_uds_recording(vec![]);
        let control = ControlHandle::spawn(ControlConn::connect(&path).await.unwrap());
        let (mut stream, tx) = query_stream_for_tests(control, QueryState::InFlight);

        tx.send(Ok(session_closing_event())).await.expect("send");
        let event = stream.next().await.expect("the terminal control");
        assert!(matches!(event, Ok(MarketEvent::Control(_))));

        drop(stream);
        assert_eq!(
            first_request(&seen).await,
            None,
            "a delivered query was already reaped; cancelling it would only log noise"
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
        let (_query, _tx) = query_stream_for_tests(control.clone(), QueryState::Delivered);

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
