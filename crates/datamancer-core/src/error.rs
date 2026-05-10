//! Error types for datamancer.

use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Error {
    /// A provider id referenced in configuration was not registered with this
    /// datamancer instance.
    #[error("unknown provider: {0}")]
    UnknownProvider(String),

    /// The requested event kind is not supported by any registered provider
    /// capable of serving the requested instrument.
    #[error("no registered provider supports event kind {kind:?} for {instrument}")]
    UnsupportedEventKind {
        kind: crate::EventKind,
        instrument: crate::Instrument,
    },

    /// A live session for `(instrument, kind)` is already active. Datamancer
    /// enforces at most one concurrent live session per pair; close the
    /// existing session before opening another.
    #[error("a live session for {kind:?} on {instrument} is already active")]
    LiveSessionConflict {
        instrument: crate::Instrument,
        kind: crate::EventKind,
    },

    /// The requested session configuration requires a persistence layer
    /// (HistoricalCache and/or TapLog) but none is configured on the
    /// Datamancer instance.
    #[error("this configuration requires persistence but none is configured")]
    PersistenceRequired,

    /// The session has already been closed.
    #[error("session closed")]
    SessionClosed,

    /// The session's event stream has already been taken and is still live.
    /// After the previous EventStream is dropped, take_events can be called
    /// again.
    #[error("event stream is currently held by another consumer")]
    EventsAlreadyTaken,

    /// A provider-level error surfaced through the unified error path. The
    /// embedded message is provider-defined; structured provider errors should
    /// arrive as in-band [`ControlKind::ProviderError`](crate::ControlKind)
    /// entries instead.
    #[error("provider {provider}: {message}")]
    Provider { provider: String, message: String },

    /// Storage-layer error.
    #[error("storage: {0}")]
    Storage(String),

    /// Configuration error detected at session construction.
    #[error("config: {0}")]
    Config(String),

    /// Generic catch-all for I/O.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
