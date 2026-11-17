//! Transport-crate error type.
//!
//! iceoryx2's builder/port errors are many small enums; this crate funnels them
//! into one stringly-typed [`TransportError`] at the crate boundary. The
//! [`EventSink`](datamancer_core::EventSink) `flush` contract returns the core
//! [`datamancer_core::Error`]; [`TransportError`] converts into it via
//! [`std::io::Error`] so no `datamancer-core` change is needed.

use iceoryx2::service::builder::publish_subscribe::{
    PublishSubscribeOpenError, PublishSubscribeOpenOrCreateError,
};

/// An error originating in the iceoryx2 transport.
///
/// `#[non_exhaustive]`: a consumer matches with a wildcard arm, so a variant
/// added later is an additive change rather than a break.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportError {
    /// A service name was not a valid iceoryx2 `ServiceName`.
    BadServiceName(String),
    /// Creating or opening an iceoryx2 service/port failed.
    Service(String),
    /// The named service does not exist on the host: nothing has created it,
    /// or its owner has already removed it.
    ServiceNotFound(String),
    /// The named service exists on the host but is marked for destruction.
    /// Either its owner is removing it, which passes on its own, or a previous
    /// owner was killed while removing it: iceoryx2's dead-node cleanup never
    /// visits such a service, and opening it fails until the stale resources
    /// are removed (see the daemon README, "Recovering from a crashed
    /// predecessor").
    StaleService(String),
    /// Loaning or sending a sample failed.
    Send(String),
    /// Receiving a sample failed.
    Receive(String),
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
            Self::ServiceNotFound(name) => write!(f, "iceoryx2 service {name} does not exist"),
            Self::StaleService(name) => write!(
                f,
                "iceoryx2 service {name} is marked for destruction: its owner is removing \
                 it, or was killed while doing so and its stale resources must be removed"
            ),
            Self::Send(m) => write!(f, "iceoryx2 send error: {m}"),
            Self::Receive(m) => write!(f, "iceoryx2 receive error: {m}"),
            Self::Interning(m) => write!(f, "symbol interning error: {m}"),
            Self::Unsupported(m) => write!(f, "unsupported event for transport: {m}"),
        }
    }
}

impl std::error::Error for TransportError {}

/// Classify a publish-subscribe `open` failure on service `name`:
/// `DoesNotExist` is [`TransportError::ServiceNotFound`],
/// `IsMarkedForDestruction` is [`TransportError::StaleService`]; everything
/// else keeps its debug text as [`TransportError::Service`].
pub(crate) fn open_error(name: &str, e: PublishSubscribeOpenError) -> TransportError {
    match e {
        PublishSubscribeOpenError::DoesNotExist => {
            TransportError::ServiceNotFound(name.to_string())
        }
        PublishSubscribeOpenError::IsMarkedForDestruction => {
            TransportError::StaleService(name.to_string())
        }
        other => TransportError::Service(format!("{other:?}")),
    }
}

/// Classify a publish-subscribe `open_or_create` failure on service `name`:
/// an open failure as [`open_error`] does; anything else, `SystemInFlux`
/// included (a transient race, not a stale service), keeps its debug text as
/// [`TransportError::Service`].
pub(crate) fn open_or_create_error(
    name: &str,
    e: PublishSubscribeOpenOrCreateError,
) -> TransportError {
    match e {
        PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(e) => open_error(name, e),
        other => TransportError::Service(format!("{other:?}")),
    }
}

impl From<TransportError> for datamancer_core::Error {
    fn from(e: TransportError) -> Self {
        datamancer_core::Error::Io(std::io::Error::other(e.to_string()))
    }
}

/// Convenience alias for transport-crate results.
pub type Result<T, E = TransportError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::{
        PublishSubscribeOpenError, PublishSubscribeOpenOrCreateError, TransportError,
        open_or_create_error,
    };

    #[test]
    fn a_service_marked_for_destruction_classifies_as_stale() {
        let stale = TransportError::StaleService("p/v3/data/0".to_string());
        assert_eq!(
            open_or_create_error(
                "p/v3/data/0",
                PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(
                    PublishSubscribeOpenError::IsMarkedForDestruction
                )
            ),
            stale
        );
        assert!(stale.to_string().contains("p/v3/data/0"));
    }

    /// `SystemInFlux` is a transient race (open/create ping-pong), not a
    /// stale service; it must not send an operator to the stale-service
    /// recovery.
    #[test]
    fn system_in_flux_is_not_classified_as_stale() {
        let e = open_or_create_error(
            "p/v3/data/0",
            PublishSubscribeOpenOrCreateError::SystemInFlux,
        );
        assert!(matches!(&e, TransportError::Service(m) if m.contains("SystemInFlux")));
    }

    #[test]
    fn a_service_that_does_not_exist_classifies_as_not_found() {
        assert_eq!(
            open_or_create_error(
                "p/v3/data/0",
                PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(
                    PublishSubscribeOpenError::DoesNotExist
                )
            ),
            TransportError::ServiceNotFound("p/v3/data/0".to_string())
        );
    }

    #[test]
    fn a_receive_failure_does_not_read_as_a_send_failure() {
        let failure = TransportError::Receive("ExceedsMaxBorrows".to_string());
        assert_eq!(
            failure.to_string(),
            "iceoryx2 receive error: ExceedsMaxBorrows"
        );
    }

    #[test]
    fn other_open_failures_keep_the_debug_text() {
        let e = open_or_create_error(
            "p/v3/data/0",
            PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(
                PublishSubscribeOpenError::IncompatibleTypes,
            ),
        );
        assert!(matches!(&e, TransportError::Service(m) if m.contains("IncompatibleTypes")));
    }
}
