//! Transport-crate error type. Mirrors the iceoryx2 crate: funnel wire/socket
//! failures into one stringly-typed error that converts into the core
//! `datamancer_core::Error` via `std::io::Error` so no core change is needed.

/// An error originating in the WebSocket transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsTransportError {
    /// Serializing an event frame to JSON failed.
    Encode(String),
    /// The outbound channel is closed (writer task gone / connection dropped).
    Closed,
    /// A `MarketEvent` variant this transport build cannot encode reached the
    /// sink (core gained a data variant newer than this transport).
    Unsupported(String),
}

impl std::fmt::Display for WsTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(m) => write!(f, "ws frame encode error: {m}"),
            Self::Closed => f.write_str("ws outbound channel closed"),
            Self::Unsupported(m) => write!(f, "unsupported event for ws transport: {m}"),
        }
    }
}

impl std::error::Error for WsTransportError {}

impl From<WsTransportError> for datamancer_core::Error {
    fn from(e: WsTransportError) -> Self {
        datamancer_core::Error::Io(std::io::Error::other(e.to_string()))
    }
}

/// Convenience alias for transport-crate results.
pub type Result<T, E = WsTransportError> = std::result::Result<T, E>;
