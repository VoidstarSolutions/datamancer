//! The web layer's handle to the daemon's config file.
//!
//! Holds the resolved config path and the exact [`Config`] the daemon booted
//! with. The daemon's runtime is immutable after boot (apply-on-restart), so
//! this handle only reads and rewrites the *file*; `restart_required` is
//! parsed-config inequality between the on-disk file and the boot config —
//! a save that restores the boot config clears the flag even though comments
//! were lost.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::Config;
use crate::error::{DaemonError, Result};

/// Cheap-`Clone` (Arc-backed) config-file handle for web handlers.
#[derive(Clone)]
pub struct ConfigState {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    boot: Config,
    restart_required: AtomicBool,
}

impl ConfigState {
    /// Build from the resolved config path and the boot-time config.
    #[must_use]
    pub fn new(path: PathBuf, boot: Config) -> Self {
        Self {
            inner: Arc::new(Inner {
                path,
                boot,
                restart_required: AtomicBool::new(false),
            }),
        }
    }

    /// The config file path.
    #[allow(dead_code)] // Public API for Task 7.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// The config the daemon booted with.
    #[allow(dead_code)] // Public API for Task 7.
    #[must_use]
    pub fn boot(&self) -> &Config {
        &self.inner.boot
    }

    /// Latest known restart-required flag (recomputed by `read_disk`/`write`).
    #[allow(dead_code)] // Public API for Task 7.
    #[must_use]
    pub fn restart_required(&self) -> bool {
        self.inner.restart_required.load(Ordering::Relaxed)
    }

    /// Read and parse the on-disk config (no validation — this is the display
    /// path and must show external hand-edits). Recomputes the restart flag.
    ///
    /// # Errors
    ///
    /// [`DaemonError::ConfigRead`] / [`DaemonError::ConfigParse`] on failure.
    #[allow(dead_code)] // Public API for Task 7.
    pub async fn read_disk(&self) -> Result<Config> {
        let text = tokio::fs::read_to_string(&self.inner.path)
            .await
            .map_err(|source| DaemonError::ConfigRead {
                path: self.inner.path.clone(),
                source,
            })?;
        let config = Config::parse(&text)?;
        self.store_flag(&config);
        Ok(config)
    }

    /// Validate and atomically write `config` to the file, then recompute the
    /// restart flag. Nothing is written (and the flag is unchanged) on failure.
    ///
    /// # Errors
    ///
    /// Propagates [`Config::save`] errors.
    #[allow(dead_code)] // Public API for Task 7.
    pub async fn write(&self, config: &Config) -> Result<()> {
        let config = config.clone();
        let path = self.inner.path.clone();
        // `save` is small-file blocking I/O; keep it off the shared runtime.
        let config_result = tokio::task::spawn_blocking(move || config.save(&path).map(|()| config))
            .await
            .map_err(|e| DaemonError::ConfigInvalid(format!("config write task failed: {e}")))?;
        let saved = config_result?;
        self.store_flag(&saved);
        Ok(())
    }

    fn store_flag(&self, on_disk: &Config) {
        self.inner
            .restart_required
            .store(*on_disk != self.inner.boot, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AccountTypeCfg;

    const MINIMAL: &str = "[provider.alpaca]\naccount_type = \"paper\"\n";

    fn boot_state(dir: &std::path::Path) -> (ConfigState, Config) {
        let path = dir.join("config.toml");
        let boot = Config::parse(MINIMAL).expect("parse");
        boot.save(&path).expect("seed file");
        (ConfigState::new(path, boot.clone()), boot)
    }

    #[tokio::test]
    async fn restart_required_tracks_disk_vs_boot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (state, boot) = boot_state(dir.path());
        assert!(!state.restart_required(), "boot state matches disk");

        // A changed config flips the flag on write.
        let mut changed = boot.clone();
        changed.session.resume_buffer_events = 42;
        state.write(&changed).await.expect("write");
        assert!(state.restart_required());
        assert_eq!(state.read_disk().await.expect("read"), changed);

        // Restoring the boot config clears it (parsed equality, not bytes).
        state.write(&boot).await.expect("restore");
        assert!(!state.restart_required());
    }

    #[tokio::test]
    async fn read_disk_reflects_external_edits() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (state, _boot) = boot_state(dir.path());
        // Hand-edit on disk behind the daemon's back.
        std::fs::write(
            state.path(),
            "[provider.alpaca]\naccount_type = \"live\"\n",
        )
        .expect("hand edit");
        let disk = state.read_disk().await.expect("read");
        assert_eq!(disk.provider.alpaca.expect("alpaca").account_type, AccountTypeCfg::Live);
        assert!(state.restart_required(), "external edit shows up");
    }

    #[tokio::test]
    async fn write_invalid_config_changes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (state, boot) = boot_state(dir.path());
        let invalid = Config::parse("[provider]\n").expect("parse");
        state.write(&invalid).await.expect_err("must reject");
        assert_eq!(state.read_disk().await.expect("read"), boot);
        assert!(!state.restart_required());
    }
}
