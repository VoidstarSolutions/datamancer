//! The two-layer client error: control-plane rejections are normalized to the
//! stable [`crate::codes`] vocabulary identically across transports (they are
//! the daemon's contract); only genuine transport failures are the
//! per-implementation `E`.

use datamancer_core::LiveScope;

/// Error from a [`crate::Client`] operation.
///
/// `#[non_exhaustive]`: future variants (e.g. a client-side timeout) must
/// not be a breaking change for downstream `match` arms.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError<E: std::error::Error> {
    /// The daemon rejected the request. `code` is one of the stable
    /// [`crate::codes`] strings; identical across transports.
    #[error("daemon rejected request ({code}): {message}")]
    Control { code: String, message: String },
    /// The transport itself failed (socket, handshake, shared-memory attach,
    /// codec).
    #[error(transparent)]
    Transport(#[from] E),
    /// The daemon created the session with a scope other than the one asked
    /// for. The client has asked the daemon to drop the subscription; events
    /// the session sent before that, or all of them if that request failed,
    /// still reach the event stream.
    #[error("daemon created the session as {got:?}, not as the {asked:?} asked for")]
    ScopeNotHonored {
        /// The scope the subscribe asked for.
        asked: LiveScope,
        /// The scope the daemon created.
        got: LiveScope,
    },
}
