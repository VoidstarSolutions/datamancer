//! The WS listener: bind, accept, spawn one `handle_connection` per socket.
//! Its own bind/posture, separate from the loopback read-only web UI, because
//! this surface is mutating and network-reachable.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use datamancer::Datamancer;
use tokio::net::TcpListener;

use crate::config::WsConfig;
use crate::ws::conn::handle_connection;

/// Serve the WS client surface until `shutdown` resolves. New accepts stop once
/// shutdown fires; in-flight connections are dropped by their own teardown.
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
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((tcp, peer)) => {
                    let dm = dm.clone();
                    let auth_token = auth_token.clone();
                    tokio::spawn(handle_connection(tcp, peer, dm, auth_token, channel_depth));
                }
                Err(e) => tracing::warn!(error = %e, "ws accept failed"),
            },
        }
    }
    Ok(())
}
