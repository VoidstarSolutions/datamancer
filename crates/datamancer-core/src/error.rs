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
    #[error(
        "no registered provider supports event kind {kind:?} for {instrument} on the {surface:?} surface"
    )]
    UnsupportedEventKind {
        kind: crate::EventKind,
        instrument: crate::Instrument,
        /// Which surface was missing. A backfilling live session requires both
        /// [`crate::Surface::Live`] and [`crate::Surface::History`] from one
        /// provider, so this names the half that failed rather than leaving the
        /// caller to guess.
        surface: crate::Surface,
    },

    /// The requested session configuration requires a persistence layer
    /// (`HistoricalCache` and/or `TapLog`) but none is configured on the
    /// `Datamancer` instance.
    #[error("this configuration requires persistence but none is configured")]
    PersistenceRequired,

    /// The session has already been closed.
    #[error("session closed")]
    SessionClosed,

    /// A client session already holds a subscription for `(instrument, kind)`.
    /// Subscribe once per pair; demux is a consumer concern.
    #[error("client already subscribed to {kind:?} on {instrument}")]
    DuplicateSubscription {
        instrument: crate::Instrument,
        kind: crate::EventKind,
    },

    /// A client session was asked to unsubscribe from a pair it does not hold.
    #[error("client is not subscribed to {kind:?} on {instrument}")]
    NotSubscribed {
        instrument: crate::Instrument,
        kind: crate::EventKind,
    },

    /// A live backfill was asked to start at or past the live edge. Nothing
    /// lies in `[from, edge)`, so it is refused rather than opened as a
    /// backfill that never runs. Ask for pure-live, or for a start in the past.
    #[error("backfill_from {from:?} is not before the live edge {edge:?}; nothing to backfill")]
    BackfillFromNotInPast {
        /// The requested backfill start.
        from: crate::Timestamp,
        /// The live edge the request was checked against.
        edge: crate::Timestamp,
    },

    /// The session's event stream has already been taken and is still live.
    /// After the previous `EventStream` is dropped, `take_events` can be called
    /// again.
    #[error("event stream is currently held by another consumer")]
    EventsAlreadyTaken,

    /// A provider-level error surfaced through the unified error path. The
    /// embedded message is provider-defined; structured provider errors should
    /// arrive as in-band [`ControlKind::ProviderError`](crate::ControlKind)
    /// entries instead.
    #[error("provider {provider}: {message}")]
    Provider { provider: String, message: String },

    /// The provider cannot serve because it has no usable credentials: none
    /// are provisioned, or the ones it has were rejected. Retrying without new
    /// credentials cannot help; the fix is provisioning (for a daemon,
    /// `set-credentials`). Surfaced as [`crate::ProviderState::Unauthenticated`]
    /// when it fails a live open, so a provider that never connected still
    /// reports why.
    #[error("provider {provider}: unauthenticated: {message}")]
    Unauthenticated {
        /// The provider id.
        provider: String,
        /// Provider-defined detail: which credentials are missing or why
        /// they were rejected.
        message: String,
    },

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
