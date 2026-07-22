//! Filesystem plumbing for the daemon's config file: the platform-native
//! default location, first-run scaffolding, and atomic writes.
//!
//! Writes are atomic within a filesystem: the new contents land in a sibling
//! temp file which is then renamed over the target, so a reader (including a
//! concurrently-restarting daemon) never observes a torn config.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use crate::error::{DaemonError, Result as DaemonResult};

/// Atomically replace `path` with `contents`: write `<path>.tmp` in the same
/// directory, fsync, then rename over `path`.
///
/// # Errors
///
/// Propagates I/O errors from create/write/sync/rename. On failure the target
/// file is untouched (the temp file is best-effort removed).
pub fn atomic_write(path: &Path, contents: &str) -> std::io::Result<()> {
    let mut tmp_name = path.as_os_str().to_os_string();
    tmp_name.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp_name);
    let result = (|| {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(contents.as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// The platform-native default config file:
/// `<config_dir>/config.toml` for the `datamancer` project
/// (macOS: `~/Library/Application Support/datamancer/config.toml`;
/// Linux: `$XDG_CONFIG_HOME/datamancer/config.toml`).
#[must_use]
pub fn default_config_path() -> Option<PathBuf> {
    directories::ProjectDirs::from("", "", "datamancer")
        .map(|dirs| dirs.config_dir().join("config.toml"))
}

/// The platform-native default **data** directory for embedded storage
/// (the cache and tap log): `<data_dir>/datamancer`
/// (macOS: `~/Library/Application Support/datamancer`;
/// Linux: `~/.local/share/datamancer`, `$XDG_DATA_HOME` respected).
///
/// This is the data-dir analog of [`default_config_path`]'s config dir — a
/// growing embedded database belongs in the data dir, not the config dir (they
/// coincide on macOS but not on Linux).
#[must_use]
pub fn default_data_dir() -> Option<PathBuf> {
    directories::ProjectDirs::from("", "", "datamancer").map(|dirs| dirs.data_dir().to_path_buf())
}

/// The commented first-run scaffold. The control socket default now lives in
/// the runtime/data dir and needs no root (see [`crate::config`]'s
/// `default_admin_socket`, which resolves
/// `datamancer_client::default_control_socket()`).
#[must_use]
pub fn default_config_toml() -> String {
    // The same-host data plane on Windows is WS-loopback (iceoryx2 shared
    // memory is not viable on Windows — see the native-Windows spec §2.5), so
    // the scaffold enables `[ws]` on loopback by default: it is the *only*
    // data transport there, so a fresh Windows daemon must serve it out of the
    // box (the shipped Windows binary is built `--features ws`). On unix the
    // data plane is iceoryx2 and `[ws]` stays off (omitted here), so the
    // scaffold is unchanged on unix.
    #[cfg(windows)]
    const PLATFORM_DATA_PLANE: &str = r#"
# Windows same-host data plane: WS over loopback (iceoryx2 shared memory is not
# viable on Windows). Enabled by default because it is the only data transport
# on Windows; the app's AppHandle dials ws://127.0.0.1:9001 by convention.
[ws]
enabled = true
bind = "127.0.0.1"
port = 9001
"#;
    #[cfg(not(windows))]
    const PLATFORM_DATA_PLANE: &str = "";

    let base = r#"# datamancerd configuration.
#
# Generated on first run; edit by hand or through the web UI settings page
# (http://127.0.0.1:8080/config). UI saves rewrite this file and drop comments.
# Changes take effect on daemon restart.
#
# Credentials are NEVER read from this file: the Alpaca providers resolve
# API keys from the environment (see crates/datamancerd/README.md) or the
# daemon's credential store via `set-credentials`.

# Compiled-in providers start disabled: the daemon boots with zero providers
# configured and providers are enabled at runtime via `configure-provider`
# (the config-service control op) or by uncommenting a section below and
# restarting. `paper` selects the paper-trading credential pair from the
# environment; `live` the live pair.
# [provider.alpaca]
# account_type = "paper"

# Uncomment for the crypto provider (venues: "us", "us_kraken", "eu_kraken").
# [provider.alpaca_crypto]
# account_type = "paper"
# venue = "us"

# Historical read-through cache; required by cache-using persistence presets.
# `path` is optional for embedded: it defaults to the platform data dir
# (macOS ~/Library/Application Support/datamancer/cache.db, Linux
# ~/.local/share/datamancer/cache.db) and is created on first use.
# [cache]
# backend = "embedded"
# path = "/path/to/cache.db"

# Live tap-log write-through; required by tap-writing persistence presets.
# `path` is optional for embedded (defaults to <data dir>/taplog.db).
# [tap_log]
# backend = "embedded"
# path = "/path/to/taplog.db"

[server]
# UDS control socket (same-host operator surface; filesystem permissions are
# the access control). Defaults to the datamancer-owned well-known path
# ($XDG_RUNTIME_DIR/datamancer/control.sock on Linux,
# ~/Library/Application Support/datamancer/control.sock on macOS) that
# consumers resolve by convention. Uncomment only to override.
# admin_socket = "/path/to/control.sock"

# Read-mostly operator UI + JSON API + the config settings page. Loopback only.
# `bind` picks ONE address family. Reach the UI at the literal http://<bind>:<port>
# (e.g. http://127.0.0.1:8080), not http://localhost — on a dual-stack host
# `localhost` also resolves to IPv6 ::1, and a browser preferring that family can
# land on an unrelated service sharing the port. If :8080 collides with another
# app, change `port` here.
[web_ui]
enabled = true
bind = "127.0.0.1"
port = 8080

# Boot-time session anchors. None by default: a fresh daemon connects to
# nothing until a client subscribes or an anchor is added.
# [[startup_session]]
# provider = "alpaca-crypto"
# asset_class = "crypto"
# symbol = "BTC/USD"
# kind = "trade"
# always_on = true

# Structured logging. The subscriber installs from a best-effort peek of this
# section before the rest of config is even resolved, so RUST_LOG always
# overrides `level` if set. Uncomment only to override the defaults below.
# [log]
# level = "info"
# format = "text"
"#;
    format!("{base}{PLATFORM_DATA_PLANE}")
}

/// Resolve which config file the daemon should load.
///
/// - `Some(path)`: used verbatim; a missing file is the caller's error to
///   surface (no scaffolding of explicit paths).
/// - `None`: the platform default path; when the file does not exist, its
///   directory is created and a commented default is scaffolded.
///
/// # Errors
///
/// [`DaemonError::ConfigInvalid`] when no home directory exists to derive the
/// default path; I/O errors from scaffolding.
pub fn resolve_config_path(explicit: Option<PathBuf>) -> DaemonResult<PathBuf> {
    if let Some(path) = explicit {
        Ok(path)
    } else {
        let default = default_config_path().ok_or_else(|| {
            DaemonError::ConfigInvalid(
                "no home directory found to derive the default config path; pass --config"
                    .to_string(),
            )
        })?;
        resolve_in(None, default)
    }
}

/// Testable core of [`resolve_config_path`]: `default` is injected so tests
/// never touch the real home directory.
fn resolve_in(explicit: Option<PathBuf>, default: PathBuf) -> DaemonResult<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if !default.exists() {
        let dir = default
            .parent()
            .ok_or_else(|| {
                DaemonError::ConfigInvalid(format!(
                    "default config path {} has no parent directory",
                    default.display()
                ))
            })?
            .to_path_buf();
        std::fs::create_dir_all(&dir)?;
        atomic_write(&default, &default_config_toml())?;
        tracing::info!(path = %default.display(), "no config found; wrote default config");
    }
    Ok(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn atomic_write_creates_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.toml");
        let content = "test content\n";
        atomic_write(&path, content).expect("atomic_write");
        let read = fs::read_to_string(&path).expect("read");
        assert_eq!(read, content);
    }

    #[test]
    fn atomic_write_replaces_existing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.toml");
        fs::write(&path, "old content").expect("write old");
        atomic_write(&path, "new content").expect("atomic_write");
        let read = fs::read_to_string(&path).expect("read");
        assert_eq!(read, "new content");
    }

    #[test]
    fn atomic_write_cleans_up_temp_on_failure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("subdir").join("test.toml");
        // Ensure parent doesn't exist to cause write failure.
        let result = atomic_write(&path, "content");
        assert!(result.is_err(), "write should fail due to missing parent");
        // Verify no .tmp file was left behind (best-effort cleanup).
        let entries: Vec<_> = fs::read_dir(dir.path())
            .expect("read_dir")
            .collect::<Result<Vec<_>, _>>()
            .expect("entries");
        for entry in entries {
            let name = entry.file_name();
            assert!(
                !name.to_string_lossy().ends_with(".tmp"),
                "temp file left behind: {name:?}"
            );
        }
    }

    #[test]
    fn atomic_write_appends_tmp_extension() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.yml");
        atomic_write(&path, "a = 1\n").expect("atomic_write");
        // Verify the target file exists.
        assert!(path.exists(), "target file should exist");
        // Verify no .toml.tmp was created (old buggy behavior).
        let toml_tmp = dir.path().join("config.toml.tmp");
        assert!(
            !toml_tmp.exists(),
            "old-style temp file should not exist: {toml_tmp:?}"
        );
        // Verify no other .tmp files are left.
        let entries: Vec<_> = fs::read_dir(dir.path())
            .expect("read_dir")
            .collect::<Result<Vec<_>, _>>()
            .expect("entries");
        for entry in entries {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            assert!(
                !name_str.ends_with(".tmp"),
                "temp file left behind: {name:?}"
            );
        }
    }

    #[test]
    fn atomic_write_replaces_contents() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        atomic_write(&path, "a = 1\n").expect("first write");
        atomic_write(&path, "a = 2\n").expect("second write");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "a = 2\n");
        assert!(!path.with_extension("toml.tmp").exists());
    }

    #[test]
    fn scaffold_template_parses_and_validates() {
        let text = default_config_toml();
        let config = crate::config::Config::parse(&text).expect("scaffold must parse");
        config.validate().expect("scaffold must validate");
        // Scaffold contract: zero providers configured (cycle 3: providers
        // start disabled), web UI on, published default socket.
        assert!(config.provider.alpaca.is_none());
        assert!(config.provider.alpaca_crypto.is_none());
        let web = config.web_ui.expect("web_ui section");
        assert!(web.enabled);
        assert_eq!(
            config.server.admin_socket,
            datamancer_client::default_control_socket()
                .expect("home/runtime dir exists in test env"),
        );
    }

    #[test]
    fn explicit_path_is_used_verbatim_and_never_scaffolded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let explicit = dir.path().join("missing.toml");
        let default = dir.path().join("default/config.toml");
        let resolved = resolve_in(Some(explicit.clone()), default).expect("resolve");
        assert_eq!(resolved, explicit);
        assert!(!explicit.exists(), "explicit paths are never scaffolded");
    }

    #[test]
    fn default_config_path_matches_documented_location() {
        let path = default_config_path().expect("home dir exists in test env");
        let s = path.to_string_lossy();
        #[cfg(target_os = "macos")]
        assert!(
            s.ends_with("Library/Application Support/datamancer/config.toml"),
            "documented macOS path drifted: {s}"
        );
        #[cfg(target_os = "linux")]
        assert!(s.ends_with("datamancer/config.toml"), "{s}");
        #[cfg(windows)]
        assert!(
            s.replace('\\', "/")
                .ends_with("datamancer/config/config.toml"),
            "documented Windows path drifted: {s}"
        );
    }

    #[test]
    fn missing_default_path_scaffolds_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let default = dir.path().join("nested/config.toml");
        let resolved = resolve_in(None, default.clone()).expect("resolve");
        assert_eq!(resolved, default);
        let first = std::fs::read_to_string(&default).expect("scaffolded file");
        crate::config::Config::parse(&first).expect("scaffold parses");

        // Second resolve must not overwrite an existing file.
        std::fs::write(&default, "[provider.alpaca]\naccount_type = \"live\"\n").unwrap();
        let resolved2 = resolve_in(None, default.clone()).expect("resolve again");
        assert_eq!(resolved2, default);
        let second = std::fs::read_to_string(&default).unwrap();
        assert!(
            second.contains("live"),
            "existing config must not be overwritten"
        );
    }
}
