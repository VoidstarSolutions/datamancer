//! Standalone named-pipe control client for the Windows **hybrid** admin plane.
//!
//! Speaks the daemon's newline-JSON `Request`/`Reply` control vocabulary over
//! the owner-verified control pipe — the same framing as the iceoryx2 client's
//! `ControlConn`, factored out here so the app facade can drive admin ops
//! (`ping`/`health`/credentials/config/`shutdown`) on Windows **without** the
//! iceoryx2 shared-memory data plane, which does not run on Windows (native-
//! Windows design spec §2.5). The data plane is carried separately by `WsClient`
//! (Phase 4 hybrid); this client is the control half.
//!
//! Serial, strict request→reply per line — one in-flight request at a time,
//! matching the daemon's `serve_connection` contract.

use std::path::Path;

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines, ReadHalf, WriteHalf};
use tokio::net::windows::named_pipe::NamedPipeClient;

use crate::protocol::uds::{Reply, Request};

/// Failure talking to the daemon over the control pipe. Surfaced as the
/// `AdminError` transport type by the Windows hybrid [`crate::app::AppHandle`]
/// (`ClientError::Transport(PipeControlError)`); daemon rejections are mapped
/// to `ClientError::Control` at the facade, not represented here.
#[derive(Debug, thiserror::Error)]
pub enum PipeControlError {
    #[error("control pipe I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("control JSON: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("{0}")]
    Protocol(String),
}

/// A serially-used named-pipe control connection. Mirrors
/// `iceoryx2::ControlConn`'s Windows arm but carries no iceoryx2 data plane.
pub(crate) struct PipeControlClient {
    lines: Lines<BufReader<ReadHalf<NamedPipeClient>>>,
    write: WriteHalf<NamedPipeClient>,
}

impl PipeControlClient {
    /// Connect to the daemon's control pipe and prepare for request/reply. The
    /// pipe's owner SID + this process's integrity are verified by
    /// [`crate::win_pipe::connect_verified`] before anything is sent, so
    /// credentials never flow to a foreign-owner endpoint.
    pub(crate) async fn connect(path: &Path) -> Result<Self, PipeControlError> {
        let stream = crate::win_pipe::connect_verified(path).await?;
        let (read, write) = tokio::io::split(stream);
        Ok(Self {
            lines: BufReader::new(read).lines(),
            write,
        })
    }

    /// Send one `Request`, read exactly one `Reply` line. Serial.
    pub(crate) async fn request(&mut self, req: &Request) -> Result<Reply, PipeControlError> {
        let mut buf = serde_json::to_vec(req)?;
        buf.push(b'\n');
        self.write.write_all(&buf).await?;
        let line = self.lines.next_line().await?.ok_or_else(|| {
            PipeControlError::Protocol("control connection closed mid-request".to_string())
        })?;
        Ok(serde_json::from_str(&line)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};

    /// Exercises the request/reply **framing** over a real named pipe. The
    /// client is built from raw split halves, deliberately bypassing the
    /// owner-SID check in `connect()` (`connect_verified`): that check requires
    /// the pipe to be owner-stamped as the token user, which fails on an
    /// elevated CI runner (the default pipe owner is Administrators). The
    /// owner-DACL/integrity path is covered by `datamancerd`'s `win_control`
    /// tests; here we prove the newline-JSON round-trip environment-independently.
    #[tokio::test]
    async fn request_round_trips_over_a_pipe() {
        let name = r"\\.\pipe\datamancer-test-pipectl-request";
        let server = ServerOptions::new()
            .first_pipe_instance(true)
            .create(name)
            .expect("create server pipe");
        let server_task = tokio::spawn(async move {
            server.connect().await.expect("accept");
            let (read, mut write) = tokio::io::split(server);
            let mut lines = BufReader::new(read).lines();
            if let Ok(Some(_req)) = lines.next_line().await {
                write.write_all(b"{\"ok\":true}\n").await.expect("reply");
            }
        });

        let client = ClientOptions::new().open(name).expect("client connect");
        let (read, write) = tokio::io::split(client);
        let mut ctl = PipeControlClient {
            lines: BufReader::new(read).lines(),
            write,
        };
        let reply = ctl.request(&Request::Ping).await.expect("request");
        assert!(reply.ok, "expected an ok reply");
        server_task.await.expect("server task");
    }

    /// The full `connect()` path (owner-SID + integrity self-check via
    /// `connect_verified`). `#[ignore]`d: on an elevated CI runner the default
    /// pipe owner is Administrators, so the owner check fails even for a
    /// same-process pipe. Run locally as a normal (Medium-integrity) user:
    ///   `cargo test -p datamancer-client --features app pipe_control -- --ignored`
    #[tokio::test]
    #[ignore = "owner-SID/integrity check fails on elevated CI; run locally as a normal user"]
    async fn connect_verifies_owner_and_round_trips() {
        let name = r"\\.\pipe\datamancer-test-pipectl-connect";
        let server = ServerOptions::new()
            .first_pipe_instance(true)
            .create(name)
            .expect("create server pipe");
        let server_task = tokio::spawn(async move {
            server.connect().await.expect("accept");
            let (read, mut write) = tokio::io::split(server);
            let mut lines = BufReader::new(read).lines();
            if let Ok(Some(_req)) = lines.next_line().await {
                write.write_all(b"{\"ok\":true}\n").await.expect("reply");
            }
        });

        let mut ctl = PipeControlClient::connect(Path::new(name))
            .await
            .expect("connect");
        let reply = ctl.request(&Request::Ping).await.expect("request");
        assert!(reply.ok, "expected an ok reply");
        server_task.await.expect("server task");
    }
}
