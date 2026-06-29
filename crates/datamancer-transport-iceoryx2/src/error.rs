//! Transport-crate error type.
//!
//! iceoryx2's builder/port errors are many small enums; this crate funnels them
//! into one stringly-typed [`TransportError`] at the crate boundary. The
//! [`EventSink`](datamancer_core::EventSink) `flush` contract returns the core
//! [`datamancer_core::Error`]; [`TransportError`] converts into it via
//! [`std::io::Error`] so no `datamancer-core` change is needed.

/// An error originating in the iceoryx2 transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// A service name was not a valid iceoryx2 `ServiceName`.
    BadServiceName(String),
    /// Creating or opening an iceoryx2 service/port failed.
    Service(String),
    /// Loaning or sending a sample failed.
    Send(String),
    /// An instrument could not be interned for transport (tuple too long).
    Interning(String),
    /// A `MarketEvent` variant this transport build cannot encode reached the
    /// sink (the core event model gained a data variant newer than this
    /// transport). Surfaced rather than silently acknowledged as delivered.
    Unsupported(String),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadServiceName(m) => write!(f, "invalid iceoryx2 service name: {m}"),
            Self::Service(m) => write!(f, "iceoryx2 service error: {m}"),
            Self::Send(m) => write!(f, "iceoryx2 send error: {m}"),
            Self::Interning(m) => write!(f, "symbol interning error: {m}"),
            Self::Unsupported(m) => write!(f, "unsupported event for transport: {m}"),
        }
    }
}

impl std::error::Error for TransportError {}

impl From<TransportError> for datamancer_core::Error {
    fn from(e: TransportError) -> Self {
        datamancer_core::Error::Io(std::io::Error::other(e.to_string()))
    }
}

/// Convenience alias for transport-crate results.
pub type Result<T, E = TransportError> = std::result::Result<T, E>;
