//! Daemon-level error type.
//!
//! `datamancerd` wraps the `datamancer` library; this error covers the binary's
//! own concerns (config loading/validation, control-socket I/O, transport
//! setup) and carries through library [`datamancer::Error`] values where they
//! surface. Control-surface replies map library errors to *stable JSON error
//! codes* separately (see [`crate::control`]).

use std::path::PathBuf;

/// Errors raised by the daemon outside the per-request control path.
#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    /// A config file could not be read.
    #[error("failed to read config file {path}: {source}")]
    ConfigRead {
        path: PathBuf,
        source: std::io::Error,
    },

    /// A config file could not be parsed as TOML.
    #[error("failed to parse config: {0}")]
    ConfigParse(#[from] toml::de::Error),

    /// A config file parsed but failed validation.
    #[error("invalid config: {0}")]
    ConfigInvalid(String),

    /// A library operation failed while building or running the daemon.
    #[error(transparent)]
    Library(#[from] datamancer::Error),

    /// An iceoryx2 transport-plane setup error (node/service/publisher).
    #[error("transport: {0}")]
    Transport(String),

    /// An I/O error on the control socket or elsewhere.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Daemon result alias.
pub type Result<T> = std::result::Result<T, DaemonError>;
