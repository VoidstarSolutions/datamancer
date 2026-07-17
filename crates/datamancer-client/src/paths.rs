//! Well-known, datamancer-owned filesystem conventions shared by every
//! consumer and by `datamancerd` itself.
//!
//! The control socket is a client<->daemon rendezvous, so its default location
//! is owned and published here rather than configured out of band. A daemon
//! that binds the default and a client that resolves it meet with no shared
//! configuration: discovery is by convention, not by guessing.

use std::path::PathBuf;

use directories::ProjectDirs;

/// The well-known per-platform default for datamancerd's control endpoint.
///
/// - **Linux:** `$XDG_RUNTIME_DIR/datamancer/control.sock` (falls back to the
///   data dir when no runtime dir is set).
/// - **macOS:** `~/Library/Application Support/datamancer/control.sock`
///   (there is no runtime dir on macOS, so the data dir is used).
/// - **Windows:** a named pipe `\\.\pipe\datamancer\<user>\control` — the pipe
///   namespace is machine-global, so it is disambiguated per user. This is
///   disambiguation, not access control: the Phase 3 server adds an owner-SID
///   ACL plus client-side server-identity verification (#29).
///
/// Returns `None` when the endpoint can't be resolved — no home/runtime dir on
/// non-Windows, or no resolvable user on Windows.
#[must_use]
pub fn default_control_socket() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        // Fail closed (like the non-Windows arm) rather than invent a shared
        // name if the user can't be resolved. USERNAME is disambiguation, not
        // access control, and is not globally unique; a SID is the robust key
        // (Phase 3, #29).
        let user = std::env::var("USERNAME").ok().filter(|u| !u.is_empty())?;
        Some(PathBuf::from(format!(
            r"\\.\pipe\datamancer\{user}\control"
        )))
    }
    #[cfg(not(windows))]
    {
        let dirs = ProjectDirs::from("", "", "datamancer")?;
        let base = dirs.runtime_dir().unwrap_or_else(|| dirs.data_dir());
        Some(base.join("control.sock"))
    }
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
            s.starts_with(r"\\.\pipe\datamancer\") && s.ends_with(r"\control"),
            "documented Windows pipe name drifted: {s}"
        );
    }
}
