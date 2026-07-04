//! Well-known, datamancer-owned filesystem conventions shared by every
//! consumer and by `datamancerd` itself.
//!
//! The control socket is a client<->daemon rendezvous, so its default location
//! is owned and published here rather than configured out of band. A daemon
//! that binds the default and a client that resolves it meet with no shared
//! configuration: discovery is by convention, not by guessing.

use std::path::PathBuf;

use directories::ProjectDirs;

/// The well-known per-platform default path for datamancerd's Unix control
/// socket.
///
/// - **Linux:** `$XDG_RUNTIME_DIR/datamancer/control.sock` (falls back to the
///   data dir when no runtime dir is set).
/// - **macOS:** `~/Library/Application Support/datamancer/control.sock`
///   (there is no runtime dir on macOS, so the data dir is used).
///
/// Returns `None` when no home/runtime directory can be resolved for the
/// current user.
#[must_use]
pub fn default_control_socket() -> Option<PathBuf> {
    let dirs = ProjectDirs::from("", "", "datamancer")?;
    let base = dirs.runtime_dir().unwrap_or_else(|| dirs.data_dir());
    Some(base.join("control.sock"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_control_socket_matches_documented_location() {
        let path = default_control_socket().expect("home/runtime dir exists in test env");
        let s = path.to_string_lossy();
        #[cfg(target_os = "macos")]
        assert!(
            s.ends_with("Library/Application Support/datamancer/control.sock"),
            "documented macOS path drifted: {s}"
        );
        #[cfg(target_os = "linux")]
        assert!(
            s.ends_with("datamancer/control.sock"),
            "documented Linux path drifted: {s}"
        );
    }
}
