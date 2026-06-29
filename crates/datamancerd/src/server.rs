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
use crate::control::{Reply, Request, SubscriptionSpec, codes};
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
            let _ = session.close().await;
        }
        self.pump.abort();
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
    pub async fn bootstrap(config: Config) -> Result<Self> {
        let admin_socket = config.server.admin_socket.clone();
        let service_prefix = config.server.service_prefix.clone();
        let max_clients = config.iceoryx2.max_clients;
        let shutdown_timeout = Duration::from_secs(config.server.shutdown_timeout_secs);
        let diag_interval = Duration::from_millis(config.diagnostics.publish_interval_ms);
        let startup_sessions = config.startup_session.clone();

        #[cfg(feature = "web-ui")]
        let web = config.web_ui.clone();
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
                    s.persistence.options(),
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

        let accept = tokio::spawn(accept_loop(listener, cmd_tx.clone()));

        let publisher = Iceoryx2DiagnosticsPublisher::new(&self.node)
            .map_err(|e| DaemonError::Transport(format!("diagnostics publisher: {e:?}")))?;
        let diagnostics = spawn_diagnostics(self.dm.clone(), publisher, self.diag_interval);

        #[cfg(feature = "web-ui")]
        let mut web_handles = self.start_web().await?;

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
        if tokio::time::timeout(self.shutdown_timeout, drain_fut)
            .await
            .is_err()
        {
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
        if !addr.ip().is_loopback() {
            return Err(DaemonError::ConfigInvalid(format!(
                "web bind {addr} is not a loopback address; the web UI is same-host only"
            )));
        }

        #[cfg(feature = "metrics")]
        if let Err(e) = crate::web::metrics::install() {
            tracing::warn!(error = %e, "metrics recorder install failed; /metrics will 503");
        }

        // Warm both swaps before binding so a handler never serves an empty
        // snapshot.
        let mut refreshers = crate::web::refresh::Refreshers::warm(&self.dm).await?;
        refreshers.spawn(
            self.dm.clone(),
            web.live_state_cadence_ms,
            web.cache_catalog_cadence_ms,
        );

        let state = refreshers.state.clone();
        let assets_dir = web.assets_dir.clone();
        let (shutdown, shutdown_rx) = oneshot::channel::<()>();
        let serve = tokio::spawn(async move {
            crate::web::serve(state, addr, assets_dir, async move {
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

    fn bind_socket(&self) -> Result<UnixListener> {
        if let Some(parent) = self.admin_socket.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        // Remove a stale socket from a prior unclean exit.
        let _ = std::fs::remove_file(&self.admin_socket);
        Ok(UnixListener::bind(&self.admin_socket)?)
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
            Request::Unsubscribe {
                client,
                provider,
                asset_class,
                symbol,
                kind,
            } => {
                let instrument =
                    Instrument::new(ProviderId::new(provider), asset_class.into(), symbol);
                self.unsubscribe(&client, instrument, kind.into()).await
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
                Err(e) => Reply::from_library_error(&e),
            },
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
                    spec.persistence.options(),
                )
                .await
            {
                return Reply::from_library_error(&e);
            }
        }

        let stream = match session.take_events().await {
            Ok(stream) => stream,
            Err(e) => return Reply::from_library_error(&e),
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
                spec.persistence.options(),
            )
            .await
        {
            Ok(()) => Reply::ok(),
            Err(e) => Reply::from_library_error(&e),
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
            Err(e) => Reply::from_library_error(&e),
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
async fn accept_loop(listener: UnixListener, cmd_tx: mpsc::Sender<ServerCommand>) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(handle_connection(stream, cmd_tx.clone()));
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
async fn handle_connection(stream: UnixStream, cmd_tx: mpsc::Sender<ServerCommand>) {
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
                if let Request::OpenClient { client, .. } = &request {
                    opened_client = Some(client.clone());
                }
                let (tx, rx) = oneshot::channel();
                if cmd_tx
                    .send(ServerCommand::Request { request, reply: tx })
                    .await
                    .is_err()
                {
                    break;
                }
                match rx.await {
                    Ok(reply) => reply,
                    Err(_) => break,
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
