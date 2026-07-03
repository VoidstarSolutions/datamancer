//! The two-layer client error: control-plane rejections are normalized to the
//! stable [`crate::codes`] vocabulary identically across transports (they are
//! the daemon's contract); only genuine transport failures are the
//! per-implementation `E`.

/// Error from a [`crate::Client`] operation.
#[derive(Debug, thiserror::Error)]
pub enum ClientError<E: std::error::Error> {
    /// The daemon rejected the request. `code` is one of the stable
    /// [`crate::codes`] strings; identical across transports.
    #[error("daemon rejected request ({code}): {message}")]
    Control { code: String, message: String },
    /// The transport itself failed (socket, handshake, shared-memory attach,
    /// codec).
    #[error(transparent)]
    Transport(#[from] E),
}
