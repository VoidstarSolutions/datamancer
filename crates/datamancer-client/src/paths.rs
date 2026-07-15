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

/// Default destination for a facade-spawned daemon's stdout/stderr:
/// `<data dir>/datamancerd.log` (macOS `~/Library/Application
/// Support/datamancer`, Linux `~/.local/share/datamancer`).
#[must_use]
pub fn default_daemon_log() -> Option<PathBuf> {
    let dirs = ProjectDirs::from("", "", "datamancer")?;
    Some(dirs.data_dir().join("datamancerd.log"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_daemon_log_lives_in_the_data_dir() {
        let path = default_daemon_log().expect("home dir exists in test env");
        // Everywhere: the well-known file name.
        assert_eq!(
            path.file_name().and_then(|n| n.to_str()),
            Some("datamancerd.log")
        );
        // Unix data dirs end in `datamancer/`; Windows nests a `data/` subdir
        // (`…\datamancer\data\`), so pin the documented layout per-OS like the
        // control-socket test above.
        let s = path.to_string_lossy();
        #[cfg(target_os = "macos")]
        assert!(
            s.ends_with("Library/Application Support/datamancer/datamancerd.log"),
            "documented macOS path drifted: {s}"
        );
        #[cfg(target_os = "linux")]
        assert!(
            s.ends_with("datamancer/datamancerd.log"),
            "documented Linux path drifted: {s}"
        );
        #[cfg(windows)]
        assert!(
            s.replace('\\', "/")
                .ends_with("datamancer/data/datamancerd.log"),
            "documented Windows path drifted: {s}"
        );
    }

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
        #[cfg(windows)]
        assert!(
            s.replace('\\', "/")
                .ends_with("datamancer/data/control.sock"),
            "documented Windows path drifted: {s}"
        );
    }
}
