//! The WS listener: bind, accept, spawn one `handle_connection` per socket.
//! Its own bind/posture, separate from the loopback read-only web UI, because
//! this surface is mutating and network-reachable.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use datamancer::Datamancer;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, broadcast};
use tokio::task::JoinSet;

use crate::config::WsConfig;
use crate::ws::conn::handle_connection;

/// Serve the WS client surface until `shutdown` resolves. New accepts stop once
/// shutdown fires; in-flight connections are then signalled to tear down (each
/// runs its `session.close()` → tap-log flush → clean WS Close), and their tasks
/// are drained under a bound shorter than the supervisor's own drain timeout.
///
/// # Errors
///
/// Propagates the bind error.
pub async fn serve(
    dm: Datamancer,
    cfg: WsConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let addr: SocketAddr = format!("{}:{}", cfg.bind, cfg.port).parse().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("ws bind address: {e}"),
        )
    })?;
    let listener = TcpListener::bind(addr).await?;

    if cfg.auth_token.is_none() {
        if addr.ip().is_loopback() {
            tracing::warn!(%addr, "ws surface has no auth_token (loopback only; set [ws].auth_token before exposing)");
        } else {
            tracing::warn!(%addr, "ws surface bound OFF-LOOPBACK with NO auth_token — unauthenticated remote access; set [ws].auth_token");
        }
    }
    tracing::info!(%addr, "datamancerd ws client surface listening");

    let auth_token = cfg.auth_token.map(Arc::new);
    let channel_depth = cfg.channel_depth;

    // Live connection tasks, plus a broadcast signal each one selects on so a
    // daemon-wide shutdown triggers their teardown (rather than leaving them
    // blocked on the socket read). Capacity 1: a single fan-out `()`.
    let mut conns: JoinSet<()> = JoinSet::new();
    let (conn_shutdown_tx, _) = broadcast::channel::<()>(1);

    // Hard cap on concurrent connections. Each accept takes one owned permit,
    // moved into its task and released on drop when the connection ends — so an
    // abusive/runaway client cannot exhaust memory, FDs, or session state by
    // opening connections without bound. Accepts past the cap close immediately.
    let conn_limit = Arc::new(Semaphore::new(cfg.max_connections));

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((tcp, peer)) => {
                    let Ok(permit) = Arc::clone(&conn_limit).try_acquire_owned() else {
                        tracing::warn!(
                            %peer,
                            max = cfg.max_connections,
                            "ws connection cap reached; rejecting new connection",
                        );
                        drop(tcp);
                        continue;
                    };
                    let dm = dm.clone();
                    let auth_token = auth_token.clone();
                    let conn_shutdown = conn_shutdown_tx.subscribe();
                    conns.spawn(async move {
                        // Held for the connection's lifetime; releases on drop.
                        let _permit = permit;
                        handle_connection(
                            tcp,
                            peer,
                            dm,
                            auth_token,
                            channel_depth,
                            conn_shutdown,
                        )
                        .await;
                    });
                }
                Err(e) => tracing::warn!(error = %e, "ws accept failed"),
            },
        }
    }

    // New accepts have stopped (the accept loop broke). Signal every live
    // connection to tear down, then drain their tasks under a bound shorter than
    // the supervisor's 5s `ws_task` await so this returns before that fires.
    let _ = conn_shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        while conns.join_next().await.is_some() {}
    })
    .await;
    Ok(())
}
