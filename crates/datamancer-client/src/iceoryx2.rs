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

use datamancer_core::{InstrumentEntry, InstrumentInfo, MarketEvent, ProviderId, SystemSnapshot};
use datamancer_transport_iceoryx2::DataSubscriber;
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
use crate::spec::{SubscriptionSpec, UnsubscribeSpec};

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

/// A connected same-host client. See [`Client`] for the transport-agnostic
/// contract; iceoryx2-specific behavior: loss surfaces **in-band** as
/// `Control::Gap` (the daemon's resume buffer numbers evictions), and the
/// event stream ends when the daemon drops the per-client services.
pub struct Iceoryx2Client {
    control: ControlConn,
    client_name: String,
    stop: Arc<AtomicBool>,
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

        let (ev_tx, ev_rx) = mpsc::channel(cfg.event_buffer.max(1));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let poll_interval = cfg.poll_interval;
        // Shm attach (node create + subscriber open) must surface as a
        // `connect` failure, not an eprintln plus a silently-ended stream —
        // the spec's error contract treats it as a `ClientError::Transport`.
        // The attach itself has to happen on the blocking task (the `Node`
        // has to live on the same thread that then polls it), so the result
        // is signaled back over this oneshot; `connect` awaits it before
        // returning, and the blocking task falls through into the poll loop
        // on success without needing a second handoff.
        let (attach_tx, attach_rx) =
            tokio::sync::oneshot::channel::<Result<(), Iceoryx2ClientError>>();
        tokio::task::spawn_blocking(move || {
            let node = match NodeBuilder::new().create::<ipc_threadsafe::Service>() {
                Ok(node) => node,
                Err(e) => {
                    let _ = attach_tx.send(Err(Iceoryx2ClientError::Node(e.to_string())));
                    return;
                }
            };
            let mut subscriber = match DataSubscriber::open(&node, client_id) {
                Ok(s) => s,
                Err(e) => {
                    let _ = attach_tx.send(Err(Iceoryx2ClientError::from(e)));
                    return;
                }
            };
            // Attach succeeded: tell `connect` it can return `Ok`, then fall
            // through into the poll loop on this same thread/Node.
            if attach_tx.send(Ok(())).is_err() {
                // `connect` gave up waiting (e.g. it was cancelled) — nothing
                // else can observe this stream, so there is no point in
                // polling. `subscriber`/`node` drop here, releasing the
                // attach.
                return;
            }
            while !stop_flag.load(Ordering::Relaxed) {
                match subscriber.poll() {
                    Ok(events) if events.is_empty() => std::thread::sleep(poll_interval),
                    Ok(events) => {
                        for ev in events {
                            if ev_tx.blocking_send(ev).is_err() {
                                return; // consumer dropped the stream
                            }
                        }
                    }
                    Err(_) => return, // service gone: daemon dropped the client
                }
            }
        });

        match attach_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(ClientError::Transport(e)),
            Err(_) => {
                // The blocking task ended (e.g. panicked) without reporting
                // either outcome.
                return Err(ClientError::Transport(Iceoryx2ClientError::Node(
                    "shm-attach task ended without reporting a result".to_string(),
                )));
            }
        }

        Ok((
            Iceoryx2Client {
                control,
                client_name: cfg.client_name,
                stop,
            },
            ReceiverStream::new(ev_rx),
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
        let reply = self
            .control
            .request(&Request::CloseClient {
                client: self.client_name.clone(),
            })
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
    use super::{ControlConn, Iceoryx2ClientError};
    #[cfg(unix)]
    use crate::codes;
    #[cfg(unix)]
    use crate::error::ClientError;
    #[cfg(unix)]
    use crate::protocol::uds::{Reply, Request};
    #[cfg(unix)]
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    #[cfg(unix)]
    use tokio::net::UnixListener;

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
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        // A fake daemon that accepts and immediately hangs up: the
        // close-client round-trip fails at the transport layer (connection
        // closed before any reply line arrives).
        let path = fake_uds(vec![]);
        let control = ControlConn::connect(&path).await.unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let client = Iceoryx2Client {
            control,
            client_name: "doomed".to_string(),
            stop: Arc::clone(&stop),
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
}
