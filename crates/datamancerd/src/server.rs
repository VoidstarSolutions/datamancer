//! The daemon supervisor: process lifecycle, the per-client registry, the
//! control listener, the diagnostics ticker, and graceful shutdown.
//!
//! A single async **actor task** (`run`) owns the client registry and the
//! iceoryx2 [`Node`]; the control listener and per-connection readers send it
//! [`ServerCommand`]s over an `mpsc`, so no lock is ever held across an
//! `.await`. One iceoryx2 node per process; per-client sinks own their service
//! on it. Startup-session anchors hold authoritative sessions alive across
//! client presence (`always_on=true` for the whole process lifetime).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datamancer::transport::{Iceoryx2DataSink, Iceoryx2DiagnosticsPublisher};
use datamancer::{
    ClientSession, Datamancer, EventKind, Instrument, ProviderId, Scope, Session, TapLog,
    traits::{EventSink, PublishOutcome},
};
use futures::StreamExt as _;
use iceoryx2::prelude::{NodeBuilder, ipc_threadsafe};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::control::{Reply, Request, SubscriptionSpec, codes, reply_from_library_error};
use crate::error::{DaemonError, Result};
use crate::shutdown::{DrainClient, DrainRecorder, drain};

type Node = iceoryx2::prelude::Node<ipc_threadsafe::Service>;

/// One connected client: its multiplexing session, its per-client data-plane
/// sink, the pump task feeding the sink, and its service name.
struct ClientEntry {
    name: String,
    session: Option<ClientSession>,
    sink: Arc<dyn EventSink>,
    pump: JoinHandle<()>,
    service: String,
}

#[async_trait]
impl DrainClient for ClientEntry {
    fn name(&self) -> &str {
        &self.name
    }

    async fn flush_sink(&self) {
        if let Err(e) = self.sink.flush().await {
            tracing::warn!(client = %self.name, error = %e, "sink flush failed during drain");
        }
    }

    async fn close(&mut self) {
        if let Some(session) = self.session.take() {
            // Closing emits a terminal `SessionClosing` into the client stream.
            let _ = session.close().await;
        }
        // Let the pump deliver the remaining stream (incl. `SessionClosing`) into
        // the sink before the later flush, rather than severing it immediately.
        // The session close ends the stream, so the pump finishes on its own;
        // bound the wait so a wedged pump cannot hang shutdown, aborting only if
        // it overruns.
        let mut pump = std::mem::replace(&mut self.pump, tokio::spawn(async {}));
        if tokio::time::timeout(Duration::from_secs(2), &mut pump)
            .await
            .is_err()
        {
            pump.abort();
        }
    }
}

/// A command handed to the server actor by a control connection.
enum ServerCommand {
    /// A parsed control request plus the channel to reply on.
    Request {
        request: Request,
        reply: oneshot::Sender<Reply>,
    },
    /// A control connection for `client` hit EOF (emergency teardown).
    Disconnect { client: String },
}

/// Live handles to the embedded web server: its `serve` task, a one-shot
/// shutdown trigger, and the two snapshot-refresh tasks.
#[cfg(feature = "web-ui")]
struct WebHandles {
    serve: JoinHandle<std::io::Result<()>>,
    shutdown: oneshot::Sender<()>,
    refreshers: crate::web::refresh::Refreshers,
}

#[cfg(feature = "web-ui")]
impl WebHandles {
    /// Trigger graceful shutdown: signal the serve task to drain, await it under
    /// a short bound, then abort the refresh tasks.
    async fn shutdown(self) {
        let _ = self.shutdown.send(());
        if tokio::time::timeout(Duration::from_secs(5), self.serve)
            .await
            .is_err()
        {
            tracing::warn!("web server did not drain within timeout");
        }
        self.refreshers.abort();
    }
}

/// The daemon supervisor.
pub struct Server {
    dm: Datamancer,
    node: Node,
    tap_log: Option<Arc<dyn TapLog>>,
    /// Startup-session anchors, held for the process lifetime.
    anchors: Vec<Session>,
    clients: HashMap<String, ClientEntry>,
    next_client_id: u64,
    service_prefix: String,
    max_clients: usize,
    admin_socket: PathBuf,
    shutdown_timeout: Duration,
    diag_interval: Duration,
    /// Optional web-UI settings (Phase 6); `None` (or `enabled=false`) disables
    /// the embedded HTTP introspection surface.
    #[cfg(feature = "web-ui")]
    web: Option<crate::config::WebUiConfig>,
    /// Optional WS client-surface settings; `None` (or `enabled=false`) disables
    /// the remote WebSocket listener.
    #[cfg(feature = "ws")]
    ws: Option<crate::config::WsConfig>,
    /// The web layer's handle to the on-disk config file (Phase 6 config API).
    #[cfg(feature = "web-ui")]
    config_state: crate::web::ConfigState,
    /// `true` once a shutdown signal has been observed; rejects new requests.
    draining: bool,
}

impl Server {
    /// Build the daemon from a validated [`Config`]: assemble the
    /// [`Datamancer`], create the one process-wide iceoryx2 node, and open the
    /// `[[startup_session]]` anchors.
    ///
    /// # Errors
    ///
    /// Propagates config/library/transport errors.
    pub async fn bootstrap(config: Config, config_path: std::path::PathBuf) -> Result<Self> {
        let admin_socket = config.server.admin_socket.clone();
        let service_prefix = config.server.service_prefix.clone();
        let max_clients = config.iceoryx2.max_clients;
        let shutdown_timeout = Duration::from_secs(config.server.shutdown_timeout_secs);
        let diag_interval = Duration::from_millis(config.diagnostics.publish_interval_ms);
        let startup_sessions = config.startup_session.clone();

        #[cfg(feature = "web-ui")]
        let web = config.web_ui.clone();
        #[cfg(feature = "ws")]
        let ws = config.ws.clone();
        #[cfg(feature = "web-ui")]
        let config_state = crate::web::ConfigState::new(config_path, config.clone());
        #[cfg(not(feature = "web-ui"))]
        let _ = config_path;
        tracing::debug!(
            live_state_ms = config.diagnostics.publish_interval_ms,
            cache_catalog_ms = config.diagnostics.cache_catalog_interval_ms,
            "diagnostics cadence (cache-catalog split deferred; single cadence in use)"
        );

        let built = config.build_runtime().await?;
        let dm = built.datamancer;
        let tap_log = built.tap_log;

        let node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .map_err(|e| DaemonError::Transport(format!("node create: {e:?}")))?;

        // `always_on=true` anchors are held for the process lifetime regardless
        // of client presence. `always_on=false` startup sessions are
        // refcount-driven warmth: with the shared authoritative registry they
        // are created on first client subscribe, so there is nothing to hold at
        // boot (a held-but-clientless session would defeat the refcount model).
        let mut anchors = Vec::new();
        for s in &startup_sessions {
            if !s.always_on {
                tracing::debug!(
                    symbol = %s.symbol,
                    "startup_session is refcount-driven (always_on=false); deferring to first client"
                );
                continue;
            }
            let scope = s.resolve_scope()?;
            let session = dm
                .session(
                    s.instrument(),
                    s.kind.into(),
                    scope,
                    crate::config::persistence_options(s.persistence),
                )
                .await?;
            tracing::info!(symbol = %s.symbol, "anchored always_on startup session");
            anchors.push(session);
        }

        Ok(Self {
            dm,
            node,
            tap_log,
            anchors,
            clients: HashMap::new(),
            next_client_id: 0,
            service_prefix,
            max_clients,
            admin_socket,
            shutdown_timeout,
            diag_interval,
            #[cfg(feature = "web-ui")]
            web,
            #[cfg(feature = "ws")]
            ws,
            #[cfg(feature = "web-ui")]
            config_state,
            draining: false,
        })
    }

    /// Run the daemon until a shutdown signal, then drain gracefully.
    ///
    /// # Errors
    ///
    /// Propagates control-socket bind errors.
    pub async fn run(mut self) -> Result<()> {
        let listener = self.bind_socket()?;
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<ServerCommand>(256);

        let accept = tokio::spawn(accept_loop(listener, cmd_tx.clone(), self.dm.clone()));

        let publisher = Iceoryx2DiagnosticsPublisher::new(&self.node)
            .map_err(|e| DaemonError::Transport(format!("diagnostics publisher: {e:?}")))?;
        let diagnostics = spawn_diagnostics(self.dm.clone(), publisher, self.diag_interval);

        #[cfg(feature = "web-ui")]
        let mut web_handles = self.start_web().await?;

        #[cfg(feature = "ws")]
        let (ws_task, ws_shutdown) = self.start_ws();

        tracing::info!(socket = %self.admin_socket.display(), "datamancerd listening");

        let mut sigterm = unix_terminate()?;
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("SIGINT received; shutting down");
                    break;
                }
                _ = sigterm.recv() => {
                    tracing::info!("SIGTERM received; shutting down");
                    break;
                }
                maybe = cmd_rx.recv() => {
                    match maybe {
                        Some(cmd) => self.handle(cmd).await,
                        None => break,
                    }
                }
            }
        }

        self.draining = true;

        // Drain the embedded web server first: stop serving introspection HTTP
        // before tearing down the data plane it reports on.
        #[cfg(feature = "web-ui")]
        if let Some(handles) = web_handles.take() {
            handles.shutdown().await;
        }

        // Stop accepting new WS clients and let in-flight connections tear down.
        #[cfg(feature = "ws")]
        {
            if let Some(trigger) = ws_shutdown {
                let _ = trigger.send(());
            }
            if tokio::time::timeout(Duration::from_secs(5), ws_task)
                .await
                .is_err()
            {
                tracing::warn!("ws surface did not drain within timeout");
            }
        }

        let recorder = DrainRecorder::default();
        let clients: Vec<Box<dyn DrainClient>> = self
            .clients
            .drain()
            .map(|(_, entry)| Box::new(entry) as Box<dyn DrainClient>)
            .collect();
        let drain_fut = drain(
            &recorder,
            move || accept.abort(),
            move || diagnostics.abort(),
            clients,
            std::mem::take(&mut self.anchors),
            self.tap_log.clone(),
        );
        tokio::pin!(drain_fut);

        // Drop the supervisor's own `cmd_tx` clone so `cmd_rx` closes once the
        // accept loop and connection tasks release theirs.
        drop(cmd_tx);

        // Keep servicing `cmd_rx` while the drain runs: the accept loop is
        // aborted at the start of `drain`, but connection tasks already spawned
        // may still send a request. Reply `shutting_down` promptly so a producer
        // never blocks forever on its reply channel (which would otherwise leave
        // a wedged connection task and could stall a clean exit).
        let drained = tokio::time::timeout(self.shutdown_timeout, async {
            let mut producers_open = true;
            loop {
                tokio::select! {
                    () = &mut drain_fut => break,
                    maybe = cmd_rx.recv(), if producers_open => match maybe {
                        Some(ServerCommand::Request { reply, .. }) => {
                            let _ = reply.send(Reply::error(
                                codes::SHUTTING_DOWN,
                                "daemon is shutting down",
                            ));
                        }
                        Some(ServerCommand::Disconnect { .. }) => {}
                        None => producers_open = false,
                    },
                }
            }
        })
        .await;
        if drained.is_err() {
            tracing::error!("shutdown drain exceeded timeout; forcing exit");
        }
        tracing::debug!(phases = ?recorder.entries(), "drain phases");
        let _ = std::fs::remove_file(&self.admin_socket);
        tracing::info!("datamancerd shutdown complete");
        Ok(())
    }

    /// Start the embedded web UI if it is configured and enabled: warm both
    /// snapshot swaps, spawn the two refresh tasks, install the optional metrics
    /// recorder, and spawn the loopback `serve` task. Returns `None` when the UI
    /// is disabled.
    ///
    /// # Errors
    ///
    /// Propagates a snapshot-warm failure or a bad bind address.
    #[cfg(feature = "web-ui")]
    async fn start_web(&self) -> Result<Option<WebHandles>> {
        use std::net::SocketAddr;

        let Some(web) = self.web.as_ref().filter(|w| w.enabled) else {
            return Ok(None);
        };

        let addr: SocketAddr = format!("{}:{}", web.bind, web.port)
            .parse()
            .map_err(|e| DaemonError::ConfigInvalid(format!("web bind address: {e}")))?;

        // Bind synchronously here (loopback enforced inside `bind`), so a bind
        // failure surfaces to the caller instead of being swallowed in the
        // detached serve task.
        let listener = crate::web::bind(addr).await?;

        #[cfg(feature = "metrics")]
        if let Err(e) = crate::web::metrics::install() {
            tracing::warn!(error = %e, "metrics recorder install failed; /metrics will 503");
        }

        // Warm both swaps before serving so a handler never serves an empty
        // snapshot.
        let mut refreshers = crate::web::refresh::Refreshers::warm(&self.dm).await?;
        refreshers.spawn(
            self.dm.clone(),
            web.live_state_cadence_ms,
            web.cache_catalog_cadence_ms,
        );

        let state = crate::web::AppState {
            snapshots: refreshers.state.clone(),
            config: self.config_state.clone(),
        };
        let assets_dir = web.assets_dir.clone();
        let (shutdown, shutdown_rx) = oneshot::channel::<()>();
        let serve = tokio::spawn(async move {
            crate::web::serve(state, listener, assets_dir, async move {
                let _ = shutdown_rx.await;
            })
            .await
        });

        Ok(Some(WebHandles {
            serve,
            shutdown,
            refreshers,
        }))
    }

    /// Start the WS client surface if enabled. Returns the serve task and its
    /// shutdown trigger (both `None`-equivalent when disabled: a no-op task and a
    /// dropped sender).
    #[cfg(feature = "ws")]
    fn start_ws(
        &self,
    ) -> (
        tokio::task::JoinHandle<std::io::Result<()>>,
        Option<oneshot::Sender<()>>,
    ) {
        let Some(cfg) = self.ws.as_ref().filter(|w| w.enabled).cloned() else {
            return (tokio::spawn(async { Ok(()) }), None);
        };
        let (shutdown, shutdown_rx) = oneshot::channel::<()>();
        let dm = self.dm.clone();
        let task = tokio::spawn(async move {
            crate::ws::serve(dm, cfg, async move {
                let _ = shutdown_rx.await;
            })
            .await
        });
        (task, Some(shutdown))
    }

    fn bind_socket(&self) -> Result<UnixListener> {
        if let Some(parent) = self.admin_socket.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        self.clear_stale_socket()?;
        Ok(UnixListener::bind(&self.admin_socket)?)
    }

    /// Remove a *stale* admin socket left by an unclean prior exit.
    ///
    /// The global single-instance lock (acquired before `run`) rules out another
    /// live `datamancerd`, so a socket at this path with no listener is
    /// necessarily stale and safe to remove. Refuses to delete a path that
    /// exists and is *not* a socket, so a misconfiguration cannot clobber an
    /// arbitrary file. Also refuses to steal a socket with a *live* listener:
    /// the lock excludes another daemon but not an unrelated program, so a live
    /// listener here means `admin_socket` is misconfigured onto a foreign
    /// service rather than left stale.
    fn clear_stale_socket(&self) -> Result<()> {
        use std::os::unix::fs::FileTypeExt;
        let meta = match std::fs::symlink_metadata(&self.admin_socket) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if !meta.file_type().is_socket() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "admin_socket path {} exists and is not a socket; refusing to remove it",
                    self.admin_socket.display()
                ),
            )
            .into());
        }
        // The lock rules out another datamancerd, but not an unrelated program.
        // A live listener here is therefore a foreign service, not a stale
        // socket — do not steal it.
        if std::os::unix::net::UnixStream::connect(&self.admin_socket).is_ok() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!(
                    "admin_socket {} is already in use by another process",
                    self.admin_socket.display()
                ),
            )
            .into());
        }
        // A socket with no listener is stale (unclean prior exit); safe to remove.
        std::fs::remove_file(&self.admin_socket)?;
        Ok(())
    }

    async fn handle(&mut self, cmd: ServerCommand) {
        match cmd {
            ServerCommand::Request { request, reply } => {
                let response = self.dispatch(request).await;
                let _ = reply.send(response);
            }
            ServerCommand::Disconnect { client } => {
                self.teardown_client(&client).await;
            }
        }
    }

    async fn dispatch(&mut self, request: Request) -> Reply {
        if self.draining {
            return Reply::error(codes::SHUTTING_DOWN, "daemon is shutting down");
        }
        match request {
            Request::OpenClient {
                client,
                subscriptions,
            } => self.open_client(client, subscriptions).await,
            Request::Subscribe { client, spec } => self.subscribe(&client, spec).await,
            Request::Unsubscribe { client, spec } => {
                let instrument = Instrument::new(
                    ProviderId::new(spec.provider),
                    spec.asset_class.into(),
                    spec.symbol,
                );
                self.unsubscribe(&client, instrument, spec.kind.into())
                    .await
            }
            Request::CloseClient { client } => {
                if self.clients.contains_key(&client) {
                    self.teardown_client(&client).await;
                    Reply::ok()
                } else {
                    Reply::error(codes::UNKNOWN_CLIENT, format!("no such client {client:?}"))
                }
            }
            Request::ListClients => Reply::clients(self.clients.keys().cloned().collect()),
            Request::Snapshot => match self.dm.snapshot().await {
                Ok(snapshot) => Reply::snapshot(snapshot),
                Err(e) => reply_from_library_error(&e),
            },
            // Dispatched off-actor in `handle_connection` (a catalog request
            // awaits live provider REST and must not stall the actor). This
            // arm only exists for match exhaustiveness / defense in depth —
            // it should be unreachable in normal operation.
            Request::Instruments { provider } => {
                let filter = provider.map(ProviderId::new);
                match self.dm.instrument_catalog(filter.as_ref()).await {
                    Ok(catalog) => Reply::instruments(catalog),
                    Err(e) => reply_from_library_error(&e),
                }
            }
        }
    }

    async fn open_client(&mut self, client: String, subscriptions: Vec<SubscriptionSpec>) -> Reply {
        if self.clients.contains_key(&client) {
            return Reply::error(
                codes::DUPLICATE_CLIENT,
                format!("client {client:?} already open"),
            );
        }
        if self.clients.len() >= self.max_clients {
            return Reply::error(
                codes::SERVICE_CAP_EXCEEDED,
                format!("service cap {} exceeded", self.max_clients),
            );
        }

        let id = self.next_client_id;
        let sink = match Iceoryx2DataSink::new(&self.node, id) {
            Ok(sink) => Arc::new(sink) as Arc<dyn EventSink>,
            Err(e) => {
                return Reply::error(codes::INTERNAL, format!("data sink: {e:?}"));
            }
        };
        let service = format!("{}/data/{id}", self.service_prefix);

        let session = self.dm.client_session();
        // Apply seed subscriptions before taking the stream.
        for spec in subscriptions {
            let instrument = spec_instrument(&spec);
            if let Err(e) = session
                .subscribe(
                    instrument,
                    spec.kind.into(),
                    Scope::Live {
                        backfill_from: None,
                    },
                    crate::config::persistence_options(spec.persistence),
                )
                .await
            {
                return reply_from_library_error(&e);
            }
        }

        let stream = match session.take_events().await {
            Ok(stream) => stream,
            Err(e) => return reply_from_library_error(&e),
        };
        let pump = spawn_pump(stream, sink.clone());

        self.next_client_id += 1;
        self.clients.insert(
            client.clone(),
            ClientEntry {
                name: client,
                session: Some(session),
                sink,
                pump,
                service: service.clone(),
            },
        );
        Reply::service(service)
    }

    async fn subscribe(&mut self, client: &str, spec: SubscriptionSpec) -> Reply {
        let Some(entry) = self.clients.get(client) else {
            return Reply::error(codes::UNKNOWN_CLIENT, format!("no such client {client:?}"));
        };
        let Some(session) = entry.session.as_ref() else {
            return Reply::error(codes::SESSION_CLOSED, "client session is closing");
        };
        let instrument = spec_instrument(&spec);
        match session
            .subscribe(
                instrument,
                spec.kind.into(),
                Scope::Live {
                    backfill_from: None,
                },
                crate::config::persistence_options(spec.persistence),
            )
            .await
        {
            Ok(()) => Reply::ok(),
            Err(e) => reply_from_library_error(&e),
        }
    }

    async fn unsubscribe(
        &mut self,
        client: &str,
        instrument: Instrument,
        kind: EventKind,
    ) -> Reply {
        let Some(entry) = self.clients.get(client) else {
            return Reply::error(codes::UNKNOWN_CLIENT, format!("no such client {client:?}"));
        };
        let Some(session) = entry.session.as_ref() else {
            return Reply::error(codes::SESSION_CLOSED, "client session is closing");
        };
        match session.unsubscribe(instrument, kind).await {
            Ok(()) => Reply::ok(),
            Err(e) => reply_from_library_error(&e),
        }
    }

    async fn teardown_client(&mut self, client: &str) {
        if let Some(mut entry) = self.clients.remove(client) {
            tracing::info!(client, service = %entry.service, "tearing down client");
            entry.flush_sink().await;
            entry.close().await;
        }
    }
}

/// The instrument for a subscription spec.
fn spec_instrument(spec: &SubscriptionSpec) -> Instrument {
    Instrument::new(
        ProviderId::new(spec.provider.clone()),
        spec.asset_class.into(),
        spec.symbol.clone(),
    )
}

/// Reject a control request that carries unknown top-level JSON keys. `serde`'s
/// `deny_unknown_fields` cannot cover `Request` directly (it is an
/// internally-tagged enum, and `Subscribe` `#[serde(flatten)]`s its spec — both
/// disable the attribute), so compare the input object's keys against the keys
/// the parsed request round-trips to. Any input key absent from the canonical
/// form was silently ignored and is reported as an error.
fn reject_unknown_keys(line: &str, request: &Request) -> std::result::Result<(), String> {
    let input: serde_json::Value =
        serde_json::from_str(line).map_err(|e| format!("invalid request: {e}"))?;
    let canonical = serde_json::to_value(request).map_err(|e| format!("invalid request: {e}"))?;
    if let (Some(input), Some(canonical)) = (input.as_object(), canonical.as_object()) {
        for key in input.keys() {
            if !canonical.contains_key(key) {
                return Err(format!("unknown field `{key}`"));
            }
        }
    }
    Ok(())
}

/// Pump a client's multiplexed stream into its per-client sink, in arrival
/// order (`(instrument, seq)`; no cross-symbol order is created). A single
/// sequential pump preserves the stream's existing order.
fn spawn_pump(mut stream: datamancer::EventStream, sink: Arc<dyn EventSink>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(ev) = stream.next().await {
            match sink.publish(ev).await {
                PublishOutcome::Delivered => {}
                PublishOutcome::Rejected(_) => {
                    tracing::warn!("sink rejected event; stopping pump");
                    break;
                }
            }
        }
        if let Err(e) = sink.flush().await {
            tracing::warn!(error = %e, "final sink flush failed");
        }
    })
}

/// Accept control connections and spawn a reader per connection.
async fn accept_loop(listener: UnixListener, cmd_tx: mpsc::Sender<ServerCommand>, dm: Datamancer) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(handle_connection(stream, cmd_tx.clone(), dm.clone()));
            }
            Err(e) => {
                tracing::warn!(error = %e, "control accept failed");
            }
        }
    }
}

/// One long-lived control connection. Reads newline-delimited JSON requests,
/// forwards each to the server actor, writes the reply line. On EOF, if this
/// connection had opened a client, signals an emergency teardown.
///
/// `Request::Instruments` is dispatched here, off-actor, rather than forwarded
/// to the actor: it awaits a live provider REST call and must not stall
/// unrelated control traffic on the single-actor loop.
async fn handle_connection(
    stream: UnixStream,
    cmd_tx: mpsc::Sender<ServerCommand>,
    dm: Datamancer,
) {
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    let mut opened_client: Option<String> = None;

    loop {
        let Ok(Some(line)) = lines.next_line().await else {
            break;
        };
        if line.trim().is_empty() {
            continue;
        }
        let reply = match serde_json::from_str::<Request>(&line) {
            Ok(request) => {
                if let Err(detail) = reject_unknown_keys(&line, &request) {
                    Reply::error(codes::BAD_REQUEST, detail)
                } else if let Request::Instruments { provider } = &request {
                    let filter = provider.clone().map(ProviderId::new);
                    match dm.instrument_catalog(filter.as_ref()).await {
                        Ok(catalog) => Reply::instruments(catalog),
                        Err(e) => reply_from_library_error(&e),
                    }
                } else {
                    let open_client_name = match &request {
                        Request::OpenClient { client, .. } => Some(client.clone()),
                        _ => None,
                    };
                    let (tx, rx) = oneshot::channel();
                    if cmd_tx
                        .send(ServerCommand::Request { request, reply: tx })
                        .await
                        .is_err()
                    {
                        break;
                    }
                    match rx.await {
                        Ok(reply) => {
                            // Arm EOF teardown only after a *successful* open, so a
                            // rejected open (e.g. duplicate-name) never causes the
                            // EOF path to tear down the existing client of that
                            // name.
                            if reply.ok
                                && let Some(name) = open_client_name
                            {
                                opened_client = Some(name);
                            }
                            reply
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(e) => Reply::error(codes::BAD_REQUEST, format!("invalid request: {e}")),
        };
        let Ok(mut buf) = serde_json::to_vec(&reply) else {
            continue;
        };
        buf.push(b'\n');
        if write.write_all(&buf).await.is_err() {
            break;
        }
    }

    // Emergency teardown on EOF for a client this connection opened.
    if let Some(client) = opened_client {
        let _ = cmd_tx.send(ServerCommand::Disconnect { client }).await;
    }
}

/// Spawn the diagnostics ticker: assemble the snapshot on cadence and publish
/// it on the diagnostics plane. `snapshot()` is async (the cache catalog does
/// I/O); awaiting it here keeps the ticker off the actor's critical path.
fn spawn_diagnostics(
    dm: Datamancer,
    publisher: Iceoryx2DiagnosticsPublisher,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            match dm.snapshot().await {
                Ok(snapshot) => {
                    if let Err(e) = publisher.publish(&snapshot) {
                        tracing::warn!(error = %e, "diagnostics publish failed");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "diagnostics snapshot failed");
                }
            }
        }
    })
}

/// A SIGTERM stream (Unix). Wrapped so `run` can `select!` on it.
fn unix_terminate() -> Result<tokio::signal::unix::Signal> {
    Ok(tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate(),
    )?)
}
