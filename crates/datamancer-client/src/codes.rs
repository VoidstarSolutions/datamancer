//! Stable JSON error codes returned in `Reply::code`/`WsReply::code`. These are
//! an operator-facing contract; changing a string is a breaking change and is
//! regression-guarded in tests.

/// A live session for the pair is already active and cannot be shared as
/// requested.
pub const LIVE_SESSION_CONFLICT: &str = "live_session_conflict";
/// No registered provider supports the requested `(instrument, kind)`.
pub const UNSUPPORTED_EVENT_KIND: &str = "unsupported_event_kind";
/// The requested persistence preset requires a backend that is not
/// configured.
pub const PERSISTENCE_REQUIRED: &str = "persistence_required";
/// A client subscription requested an unsupported (non pure-live) scope.
pub const UNSUPPORTED_CLIENT_SCOPE: &str = "unsupported_client_scope";
/// The client already holds a subscription for this pair.
pub const DUPLICATE_SUBSCRIPTION: &str = "duplicate_subscription";
/// The client is not subscribed to this pair.
pub const NOT_SUBSCRIBED: &str = "not_subscribed";
/// A referenced provider id is not registered.
pub const UNKNOWN_PROVIDER: &str = "unknown_provider";
/// The underlying session has shut down.
pub const SESSION_CLOSED: &str = "session_closed";
/// The event stream is already held.
pub const EVENTS_ALREADY_TAKEN: &str = "events_already_taken";
/// A storage-layer error.
pub const STORAGE: &str = "storage";
/// A library configuration error at session construction.
pub const CONFIG: &str = "config";
/// An I/O or provider-level library error.
pub const PROVIDER: &str = "provider";
/// The named client is not connected/registered.
pub const UNKNOWN_CLIENT: &str = "unknown_client";
/// A client tried to `open-client` a name already in use.
pub const DUPLICATE_CLIENT: &str = "duplicate_client";
/// The iceoryx2 service cap would be exceeded by this subscribe.
pub const SERVICE_CAP_EXCEEDED: &str = "service_cap_exceeded";
/// The request was malformed or named an unsupported op.
pub const BAD_REQUEST: &str = "bad_request";
/// The daemon is shutting down and is no longer accepting requests.
pub const SHUTTING_DOWN: &str = "shutting_down";
/// An unexpected internal error.
pub const INTERNAL: &str = "internal";
/// No credentials are stored for the named provider.
pub const CREDENTIALS_MISSING: &str = "credentials_missing";
/// The credential-store backend failed or is unavailable.
pub const CREDENTIAL_BACKEND_UNAVAILABLE: &str = "credential_backend_unavailable";
/// The connection's peer credentials failed the same-uid check.
pub const PERMISSION_DENIED: &str = "permission_denied";
/// The op was persisted but a cold-classified field needs a daemon restart
/// to take effect.
pub const RESTART_REQUIRED: &str = "restart_required";
/// A configure-provider payload carried a field the provider's config
/// section does not define.
pub const UNKNOWN_CONFIG_FIELD: &str = "unknown_config_field";
