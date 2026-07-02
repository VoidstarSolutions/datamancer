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
/// `<config_dir>/config.toml` for the `datamancerd` project
/// (macOS: `~/Library/Application Support/datamancerd/config.toml`;
/// Linux: `$XDG_CONFIG_HOME/datamancerd/config.toml`).
#[must_use]
pub fn default_config_path() -> Option<PathBuf> {
    directories::ProjectDirs::from("", "Voidstar", "datamancerd")
        .map(|dirs| dirs.config_dir().join("config.toml"))
}

/// The commented first-run scaffold. `config_dir` anchors the user-writable
/// admin socket (the compiled-in `/run/datamancerd` default needs root and
/// does not exist on macOS).
#[must_use]
pub fn default_config_toml(config_dir: &Path) -> String {
    let socket = config_dir.join("admin.sock");
    format!(
        r#"# datamancerd configuration.
#
# Generated on first run; edit by hand or through the web UI settings page
# (http://127.0.0.1:8080/config). UI saves rewrite this file and drop comments.
# Changes take effect on daemon restart.
#
# Credentials are NEVER read from this file: the Alpaca providers resolve
# API keys from the environment (see crates/datamancerd/README.md).

# At least one provider is required. `paper` selects the paper-trading
# credential pair from the environment; `live` the live pair.
[provider.alpaca]
account_type = "paper"

# Uncomment for the crypto provider (venues: "us", "us_kraken", "eu_kraken").
# [provider.alpaca_crypto]
# account_type = "paper"
# venue = "us"

# Historical read-through cache; required by cache-using persistence presets.
# [cache]
# backend = "surreal-embedded"
# path = "/path/to/cache"

# Live tap-log write-through; required by tap-writing persistence presets.
# [tap_log]
# backend = "surreal-embedded"
# path = "/path/to/taplog"

[server]
# UDS control socket (same-host operator surface; filesystem permissions are
# the access control).
admin_socket = "{socket}"

# Read-mostly operator UI + JSON API + the config settings page. Loopback only.
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
"#,
        socket = socket.display()
    )
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
        atomic_write(&default, &default_config_toml(&dir))?;
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
        let dir = tempfile::tempdir().expect("tempdir");
        let text = default_config_toml(dir.path());
        let config = crate::config::Config::parse(&text).expect("scaffold must parse");
        config.validate().expect("scaffold must validate");
        // Scaffold contract: paper alpaca provider, web UI on, user-writable socket.
        assert!(config.provider.alpaca.is_some());
        let web = config.web_ui.expect("web_ui section");
        assert!(web.enabled);
        assert_eq!(
            config.server.admin_socket,
            dir.path().join("admin.sock")
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
        assert!(second.contains("live"), "existing config must not be overwritten");
    }
}
