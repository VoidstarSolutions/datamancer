# datamancerd Config Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** datamancerd loads its config from the platform-native default location (scaffolding a commented default on first run), `--config` becomes an optional override, and the web UI gains a structured settings form that validates and atomically writes the config file (apply-on-restart, with a restart-required banner).

**Architecture:** `Config` gains `Serialize + PartialEq` so one schema drives file parsing, JSON for the UI, and TOML writes. A new `paths` module owns default-path resolution (`directories` crate) and first-run scaffolding. The web layer gains a `ConfigState` substate (config path + boot config + restart-required flag) alongside the existing snapshot `WebState`, combined in an `AppState` router state. `PUT /api/config` is the single mutating route (loopback + `Content-Type: application/json` + Origin/Host guards). The SSE stream is wrapped in an envelope `{snapshot, restart_required}` so the banner updates live.

**Tech Stack:** Rust (edition 2024), clap 4, toml 0.8, serde, axum 0.8, maud, `directories` (new dep), tempfile (dev).

**Spec:** `docs/superpowers/specs/2026-07-01-datamancerd-config-management-design.md`

## Global Constraints

- All work is in `crates/datamancerd` (binary crate). `#![forbid(unsafe_code)]` stays.
- Workspace lints: `clippy::pedantic = deny`. Every task's verification includes `cargo clippy -p datamancerd --all-targets --all-features -- -D warnings`.
- No hot-reload: the daemon's runtime config is immutable after boot.
- Credentials never appear in the config file (env vars only, via `oxidized_alpaca`).
- `restart_required` is **parsed-config equality** (`on-disk Config != boot Config`), not byte equality — a PUT that restores the boot config must clear the flag even though comments were lost.
- Stable error codes for the write path: `config` (validation/parse failure of a config), `bad_request` (malformed request body). Reuse the exact strings from `control.rs::codes`.
- The loopback-only bind invariant (`web::bind`) is unchanged.
- Run tests with `cargo test -p datamancerd` (the `#[ignore]`d e2e tests are excluded automatically and must not be modified).

---

### Task 1: `Serialize + PartialEq` on the config schema

**Files:**
- Modify: `crates/datamancerd/src/config.rs`

**Interfaces:**
- Produces: `Config` (and every section struct/enum in `config.rs`) implements `Serialize`, `PartialEq`, in addition to the existing derives. All `Option` fields carry `#[serde(skip_serializing_if = "Option::is_none")]` (TOML cannot represent `None`).
- Later tasks rely on: `toml::to_string_pretty(&config)` succeeding for any valid `Config`, and `config_a == config_b` comparison.

- [ ] **Step 1: Write the failing round-trip test**

Append to the `tests` module in `crates/datamancerd/src/config.rs`:

```rust
    const FULL: &str = r#"
[provider.alpaca]
account_type = "paper"

[provider.alpaca_crypto]
account_type = "live"
venue = "us_kraken"

[cache]
backend = "surreal-embedded"
path = "/tmp/dmc-cache"

[tap_log]
backend = "surreal-memory"

[session]
resume_buffer_events = 1024
adjustment = "split"

[server]
admin_socket = "/tmp/dmc/admin.sock"
service_prefix = "dmc"
shutdown_timeout_secs = 5

[diagnostics]
publish_interval_ms = 500
cache_catalog_interval_ms = 10000

[iceoryx2]
max_clients = 8

[web_ui]
enabled = true
bind = "127.0.0.1"
port = 8091

[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"
symbol = "BTC/USD"
kind = "trade"
scope = "live_backfill"
backfill_from = "2026-06-01T00:00:00Z"
persistence = "cached_with_tap"
always_on = true
"#;

    #[test]
    fn config_round_trips_through_toml() {
        let config = Config::parse(FULL).expect("parse");
        config.validate().expect("validate");
        let text = toml::to_string_pretty(&config).expect("serialize");
        let back = Config::parse(&text).expect("reparse");
        assert_eq!(config, back);
    }

    #[test]
    fn minimal_config_round_trips_without_none_fields() {
        // `None` options must be skipped, not serialized (TOML has no null).
        let config = Config::parse(MINIMAL).expect("parse");
        let text = toml::to_string_pretty(&config).expect("serialize");
        assert!(!text.contains("cache"), "absent [cache] must not serialize: {text}");
        let back = Config::parse(&text).expect("reparse");
        assert_eq!(config, back);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datamancerd config_round_trips -- --nocapture`
Expected: COMPILE ERROR — `Config` does not implement `Serialize`/`PartialEq`.

- [ ] **Step 3: Add the derives**

In `crates/datamancerd/src/config.rs`, for **every** type below, extend the derive list with `Serialize` and `PartialEq` where missing (several enums already have one or both — bring all to a consistent set):

- Structs (`Config`, `ProviderConfig`, `AlpacaSection`, `AlpacaCryptoSection`, `StorageConfig`, `SessionConfig`, `ServerConfig`, `DiagnosticsConfig`, `Iceoryx2Config`, `WebUiConfig`, `StartupSession`): `#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]` (keep `Default` where it exists today).
- Enums (`AccountTypeCfg`, `CryptoVenueCfg`, `AdjustmentCfg`, `StorageBackend`): add `Serialize` (keeping existing `rename_all` attributes — they apply to both directions). `AssetClassCfg`, `EventKindCfg`, `ScopeCfg`, `PersistenceCfg` already derive `Serialize`; add nothing there beyond confirming `PartialEq` (present).

Add `#[serde(skip_serializing_if = "Option::is_none")]` alongside the existing `#[serde(default)]` on every `Option` field:

- `Config::cache`, `Config::tap_log`, `Config::web_ui`
- `StorageConfig::path`
- `StartupSession::backfill_from`
- `WebUiConfig::assets_dir`

Example (pattern for all of them):

```rust
    /// Historical-cache backend (optional unless a session uses the cache).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<StorageConfig>,
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p datamancerd config_ && cargo clippy -p datamancerd --all-targets --all-features -- -D warnings`
Expected: all config tests PASS, clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancerd/src/config.rs
git commit -m "feat(datamancerd): Serialize + PartialEq config schema for round-tripping"
```

---

### Task 2: `paths::atomic_write` + `Config::to_toml` + `Config::save`

**Files:**
- Create: `crates/datamancerd/src/paths.rs`
- Modify: `crates/datamancerd/src/main.rs` (register `mod paths;`)
- Modify: `crates/datamancerd/src/config.rs`
- Modify: `crates/datamancerd/src/error.rs`

**Interfaces:**
- Consumes: `Config: Serialize + PartialEq` (Task 1).
- Produces:
  - `paths::atomic_write(path: &Path, contents: &str) -> std::io::Result<()>` — write `<path>.tmp` in the same directory, then rename over `path`.
  - `Config::to_toml(&self) -> Result<String>` — `toml::to_string_pretty`, mapping serializer errors to `DaemonError::ConfigInvalid`.
  - `Config::save(&self, path: &Path) -> Result<()>` — `validate()` → `to_toml()` → `atomic_write`. Nothing is written on any failure.

- [ ] **Step 1: Write the failing tests**

Create `crates/datamancerd/src/paths.rs` with a doc comment and a test module (implementation stubs come in Step 3; write the file with only the tests first is awkward in Rust, so write tests for `Config::save` in `config.rs` and for `atomic_write` in `paths.rs` together with minimal stubs that `todo!()`):

Append to `crates/datamancerd/src/config.rs` tests:

```rust
    #[test]
    fn save_writes_atomically_and_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let config = Config::parse(FULL).expect("parse");
        config.save(&path).expect("save");
        let loaded = Config::load(&path).expect("load");
        assert_eq!(config, loaded);
        // No temp droppings left behind.
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from("config.toml")]);
    }

    #[test]
    fn save_rejects_invalid_config_and_writes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        // No provider configured -> validation failure.
        let config = Config::parse("[provider]\n").expect("parse");
        let err = config.save(&path).expect_err("must reject");
        assert!(matches!(err, DaemonError::ConfigInvalid(_)));
        assert!(!path.exists(), "invalid config must not be written");
    }
```

`tempfile` is currently a dev-dependency — these are unit tests inside the crate, so it is available under `#[cfg(test)]`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancerd save_ -- --nocapture`
Expected: COMPILE ERROR — no method `save`.

- [ ] **Step 3: Implement**

Create `crates/datamancerd/src/paths.rs`:

```rust
//! Filesystem plumbing for the daemon's config file: the platform-native
//! default location, first-run scaffolding, and atomic writes.
//!
//! Writes are atomic within a filesystem: the new contents land in a sibling
//! temp file which is then renamed over the target, so a reader (including a
//! concurrently-restarting daemon) never observes a torn config.

use std::io::Write as _;
use std::path::Path;

/// Atomically replace `path` with `contents`: write `<path>.tmp` in the same
/// directory, fsync, then rename over `path`.
///
/// # Errors
///
/// Propagates I/O errors from create/write/sync/rename. On failure the target
/// file is untouched (the temp file is best-effort removed).
pub fn atomic_write(path: &Path, contents: &str) -> std::io::Result<()> {
    let tmp = path.with_extension("toml.tmp");
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
```

Register the module in `crates/datamancerd/src/main.rs` (alongside the existing `mod` items):

```rust
mod paths;
```

Add a serializer-error variant mapping in `crates/datamancerd/src/error.rs` — extend `DaemonError` (keep existing variants untouched):

```rust
    /// A config value could not be serialized to TOML.
    #[error("failed to serialize config: {0}")]
    ConfigSerialize(#[from] toml::ser::Error),
```

Add to `crates/datamancerd/src/config.rs` (inside `impl Config`, after `validate`):

```rust
    /// Serialize to pretty TOML.
    ///
    /// # Errors
    ///
    /// [`DaemonError::ConfigSerialize`] if serialization fails.
    pub fn to_toml(&self) -> Result<String> {
        Ok(toml::to_string_pretty(self)?)
    }

    /// Validate, serialize, and atomically write this config to `path`.
    /// Nothing is written when validation or serialization fails.
    ///
    /// # Errors
    ///
    /// [`DaemonError::ConfigInvalid`] on validation failure,
    /// [`DaemonError::ConfigSerialize`] on serialization failure, or an I/O
    /// error from the atomic write.
    pub fn save(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let text = self.to_toml()?;
        crate::paths::atomic_write(path, &text)?;
        Ok(())
    }
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancerd save_ && cargo clippy -p datamancerd --all-targets --all-features -- -D warnings`
Expected: PASS, clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancerd/src/paths.rs crates/datamancerd/src/config.rs crates/datamancerd/src/error.rs crates/datamancerd/src/main.rs
git commit -m "feat(datamancerd): Config::save with atomic TOML writes"
```

---

### Task 3: Default config location + first-run scaffolding

**Files:**
- Modify: `crates/datamancerd/Cargo.toml` (add `directories = "6"`)
- Modify: `crates/datamancerd/src/paths.rs`

**Interfaces:**
- Consumes: `paths::atomic_write` (Task 2).
- Produces:
  - `paths::default_config_path() -> Option<PathBuf>` — `ProjectDirs::from("", "Voidstar", "datamancerd")` → `config_dir().join("config.toml")`. `None` only when the platform has no home directory.
  - `paths::default_config_toml(config_dir: &Path) -> String` — the commented scaffold template (admin socket at `<config_dir>/admin.sock`).
  - `paths::resolve_config_path(explicit: Option<PathBuf>) -> crate::error::Result<PathBuf>` — explicit path used verbatim (missing file surfaces later as `ConfigRead`); default path scaffolds when missing. Internally delegates to `resolve_in(explicit, default) ` so tests never touch the real home directory.

- [ ] **Step 1: Write the failing tests**

Append to `crates/datamancerd/src/paths.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

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
        let config = Config::parse(&text).expect("scaffold must parse");
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
        Config::parse(&first).expect("scaffold parses");

        // Second resolve must not overwrite an existing file.
        std::fs::write(&default, "[provider.alpaca]\naccount_type = \"live\"\n").unwrap();
        let resolved2 = resolve_in(None, default.clone()).expect("resolve again");
        assert_eq!(resolved2, default);
        let second = std::fs::read_to_string(&default).unwrap();
        assert!(second.contains("live"), "existing config must not be overwritten");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancerd -- paths::`
Expected: COMPILE ERROR — `default_config_toml`, `resolve_in` not found.

- [ ] **Step 3: Implement**

Add to `crates/datamancerd/Cargo.toml` under `[dependencies]`:

```toml
directories = "6"
```

Add to `crates/datamancerd/src/paths.rs`:

```rust
use std::path::PathBuf;

use crate::error::{DaemonError, Result};

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
pub fn resolve_config_path(explicit: Option<PathBuf>) -> Result<PathBuf> {
    match explicit {
        Some(path) => Ok(path),
        None => {
            let default = default_config_path().ok_or_else(|| {
                DaemonError::ConfigInvalid(
                    "no home directory found to derive the default config path; pass --config"
                        .to_string(),
                )
            })?;
            resolve_in(None, default)
        }
    }
}

/// Testable core of [`resolve_config_path`]: `default` is injected so tests
/// never touch the real home directory.
fn resolve_in(explicit: Option<PathBuf>, default: PathBuf) -> Result<PathBuf> {
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
```

(Adjust the `use std::path::Path;` import at the top of the file to `use std::path::{Path, PathBuf};`.)

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancerd -- paths:: && cargo clippy -p datamancerd --all-targets --all-features -- -D warnings`
Expected: 4 tests PASS, clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancerd/Cargo.toml crates/datamancerd/src/paths.rs Cargo.lock
git commit -m "feat(datamancerd): platform-default config path with first-run scaffolding"
```

---

### Task 4: `--config` becomes optional

**Files:**
- Modify: `crates/datamancerd/src/main.rs`

**Interfaces:**
- Consumes: `paths::resolve_config_path` (Task 3).
- Produces: `Args::config: Option<PathBuf>`; `run()` resolves the path, loads, and passes **both** the `Config` and the resolved `PathBuf` forward. Until Task 10 rewires `Server::bootstrap`, only the `Config` is consumed; keep the resolved path in a local (`let config_path = ...;`) that Task 10 threads into `bootstrap`.

- [ ] **Step 1: Modify `Args` and `run`**

In `crates/datamancerd/src/main.rs`:

```rust
/// Command-line arguments.
#[derive(Debug, Parser)]
#[command(
    name = "datamancerd",
    about = "Standalone datamancer market-data server"
)]
struct Args {
    /// Path to the TOML config file. Defaults to the platform config
    /// directory (scaffolded with a commented default on first run).
    #[arg(long, short)]
    config: Option<std::path::PathBuf>,
}
```

and in `run()`:

```rust
async fn run() -> Result<()> {
    let args = Args::parse();
    let config_path = paths::resolve_config_path(args.config)?;
    tracing::info!(path = %config_path.display(), "loading config");
    let config = Config::load(&config_path)?;
    server::Server::bootstrap(config).await?.run().await
}
```

Remove the now-unused `use std::path::PathBuf;` at the top of `main.rs` (the type is referenced fully-qualified in `Args`).

- [ ] **Step 2: Verify behavior manually**

Run: `cargo run -p datamancerd -- --config /nonexistent/nope.toml 2>&1 | head -3`
Expected: `datamancerd failed` with `failed to read config file /nonexistent/nope.toml` (explicit path: no scaffolding).

Run: `cargo build -p datamancerd && cargo clippy -p datamancerd --all-targets --all-features -- -D warnings`
Expected: build + clippy clean. (Do **not** run the binary without `--config` on the dev machine — it would scaffold into the real config dir.)

- [ ] **Step 3: Run the full crate tests**

Run: `cargo test -p datamancerd`
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/datamancerd/src/main.rs
git commit -m "feat(datamancerd): make --config optional, defaulting to the platform config path"
```

---

### Task 5: `ConfigState` — the web layer's config handle

**Files:**
- Create: `crates/datamancerd/src/web/config_api.rs`
- Modify: `crates/datamancerd/src/web/mod.rs` (register `pub mod config_api;`)

**Interfaces:**
- Consumes: `Config: PartialEq` (Task 1), `Config::save` (Task 2).
- Produces (all in `crate::web::config_api`):
  - `ConfigState` — `Clone` (Arc-backed): `ConfigState::new(path: PathBuf, boot: Config) -> Self`.
  - `ConfigState::path(&self) -> &Path`, `ConfigState::boot(&self) -> &Config`.
  - `ConfigState::restart_required(&self) -> bool` (relaxed atomic read; updated by GET/PUT recomputation).
  - `async ConfigState::read_disk(&self) -> crate::error::Result<Config>` — `tokio::fs::read_to_string` + `Config::parse` (no validation — display path). Recomputes and stores the restart flag (`disk != boot`).
  - `async ConfigState::write(&self, config: &Config) -> crate::error::Result<()>` — `config.save(path)` then store flag (`config != boot`).

- [ ] **Step 1: Write the failing tests**

Create `crates/datamancerd/src/web/config_api.rs` starting with module docs, then the test module (implementation in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

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
        assert_eq!(disk.provider.alpaca.expect("alpaca").account_type, super::super::super::config::AccountTypeCfg::Live);
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
```

(Use `crate::config::AccountTypeCfg` directly instead of the `super` chain — write the import at the top of the test module: `use crate::config::AccountTypeCfg;` and assert `disk.provider.alpaca.expect("alpaca").account_type == AccountTypeCfg::Live`.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancerd config_api`
Expected: COMPILE ERROR — `ConfigState` not defined.

- [ ] **Step 3: Implement**

Fill in `crates/datamancerd/src/web/config_api.rs` above the tests:

```rust
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
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// The config the daemon booted with.
    #[must_use]
    pub fn boot(&self) -> &Config {
        &self.inner.boot
    }

    /// Latest known restart-required flag (recomputed by `read_disk`/`write`).
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
    pub async fn write(&self, config: &Config) -> Result<()> {
        let config = config.clone();
        let path = self.inner.path.clone();
        // `save` is small-file blocking I/O; keep it off the shared runtime.
        let saved = tokio::task::spawn_blocking(move || config.save(&path).map(|()| config))
            .await
            .map_err(|e| DaemonError::ConfigInvalid(format!("config write task failed: {e}")))??;
        self.store_flag(&saved);
        Ok(())
    }

    fn store_flag(&self, on_disk: &Config) {
        self.inner
            .restart_required
            .store(*on_disk != self.inner.boot, Ordering::Relaxed);
    }
}
```

Register in `crates/datamancerd/src/web/mod.rs` (with the other `pub mod` lines):

```rust
pub mod config_api;
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancerd config_api && cargo clippy -p datamancerd --all-targets --all-features -- -D warnings`
Expected: 3 tests PASS, clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancerd/src/web/config_api.rs crates/datamancerd/src/web/mod.rs
git commit -m "feat(datamancerd): ConfigState handle with restart-required tracking"
```

---

### Task 6: `AppState` — combined router state

**Files:**
- Modify: `crates/datamancerd/src/web/mod.rs`

**Interfaces:**
- Consumes: `WebState` (existing), `ConfigState` (Task 5).
- Produces:
  - `web::AppState { pub snapshots: WebState, pub config: ConfigState }`, `Clone`.
  - `impl FromRef<AppState> for WebState` and `impl FromRef<AppState> for ConfigState` (manual impls; no axum `macros` feature).
  - `router(state: AppState, assets_dir: Option<&Path>) -> Router` — signature change; existing handlers keep extracting `State<WebState>` unchanged (via `FromRef`).
  - Test helper `AppState::fixed_for_tests()` pattern: tests build `AppState { snapshots: WebState::fixed(..), config: ConfigState::new(temp_path, minimal_config) }`.

- [ ] **Step 1: Introduce `AppState` and update `router`**

In `crates/datamancerd/src/web/mod.rs`:

```rust
use axum::extract::FromRef;

pub use config_api::ConfigState;

/// Combined router state: snapshot reads plus the config-file handle.
#[derive(Clone)]
pub struct AppState {
    pub snapshots: WebState,
    pub config: ConfigState,
}

impl FromRef<AppState> for WebState {
    fn from_ref(state: &AppState) -> Self {
        state.snapshots.clone()
    }
}

impl FromRef<AppState> for ConfigState {
    fn from_ref(state: &AppState) -> Self {
        state.config.clone()
    }
}
```

Change `router`'s signature and state:

```rust
pub fn router(state: AppState, assets_dir: Option<&Path>) -> Router {
```

(the body is unchanged except the final `.with_state(state)` now receives `AppState`). Update `serve` the same way:

```rust
pub async fn serve(
    state: AppState,
    listener: tokio::net::TcpListener,
    ...
```

- [ ] **Step 2: Update the test helper in `web/mod.rs`**

Replace the `state()` helper in the `tests` module:

```rust
    fn state() -> AppState {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let boot = crate::config::Config::parse("[provider.alpaca]\naccount_type = \"paper\"\n")
            .expect("parse");
        boot.save(&path).expect("seed config");
        // Leak the tempdir so the file outlives the helper (test-only).
        std::mem::forget(dir);
        AppState {
            snapshots: WebState::fixed(testdata::snapshot(), testdata::snapshot()),
            config: ConfigState::new(path, boot),
        }
    }
```

- [ ] **Step 3: Run the web tests**

Run: `cargo test -p datamancerd web && cargo clippy -p datamancerd --all-targets --all-features -- -D warnings`
Expected: all existing web tests PASS unchanged (routes still GET-only at this point), clippy clean. `server.rs` does not compile against the new `serve` signature yet only if it is currently calling it — it is; update the call site minimally by constructing a placeholder `AppState` in `start_web`:

In `crates/datamancerd/src/server.rs::start_web`, the `state` local becomes:

```rust
        let state = crate::web::AppState {
            snapshots: refreshers.state.clone(),
            config: self.config_state.clone(),
        };
```

which requires the `Server` field added now (full wiring including `bootstrap`'s signature happens in Task 10 — add the field and thread it through `bootstrap` in this step if the build requires it; see Task 10 Step 1 for the exact code and reuse it here verbatim, checking off that part of Task 10 early).

- [ ] **Step 4: Commit**

```bash
git add crates/datamancerd/src/web/mod.rs crates/datamancerd/src/server.rs crates/datamancerd/src/main.rs
git commit -m "refactor(datamancerd): AppState router state combining snapshots + config handle"
```

---

### Task 7: `GET /api/config` + `PUT /api/config` with same-origin guards

**Files:**
- Modify: `crates/datamancerd/src/web/config_api.rs`
- Modify: `crates/datamancerd/src/web/mod.rs` (routes + read-only test update)

**Interfaces:**
- Consumes: `ConfigState` (Task 5), `AppState` (Task 6), stable codes `crate::control::codes::{CONFIG, BAD_REQUEST}` (existing).
- Produces:
  - `GET /api/config` → `200 {"config": <Config>, "restart_required": bool, "path": "<abs path>"}`; `500 {"code":"config","message":...}` when the on-disk file is unreadable/unparseable.
  - `PUT /api/config` → same success shape after a validated atomic write; `422 {"code":"config",...}` on validation failure (nothing written); `400 {"code":"bad_request",...}` on malformed JSON; `403 {"code":"bad_request",...}` on Origin/Host mismatch; `415` (axum `Json` rejection) on wrong content type.
  - `web::config_api::{get_config, put_config}` handler fns; `same_origin_ok(&HeaderMap) -> bool`.

- [ ] **Step 1: Write the failing handler tests**

Append to the tests in `crates/datamancerd/src/web/mod.rs` (they exercise the full router):

```rust
    async fn send_json(method: Method, uri: &str, body: &str, origin: Option<&str>) -> axum::response::Response {
        let app = router(state(), None);
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .header("host", "127.0.0.1:8080");
        if let Some(o) = origin {
            req = req.header("origin", o);
        }
        app.oneshot(req.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn config_get_returns_config_and_flag() {
        let resp = send(Method::GET, "/api/config").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let v: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(v["restart_required"], Value::Bool(false));
        assert!(v["config"]["provider"]["alpaca"].is_object());
        assert!(v["path"].as_str().unwrap().ends_with("config.toml"));
    }

    #[tokio::test]
    async fn config_put_writes_and_flags_restart() {
        let app = router(state(), None);
        let body = serde_json::json!({
            "provider": {"alpaca": {"account_type": "live"}},
            "session": {"resume_buffer_events": 128, "adjustment": "all"}
        })
        .to_string();
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/api/config")
                    .header("content-type", "application/json")
                    .header("host", "127.0.0.1:8080")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(v["restart_required"], Value::Bool(true));
        assert_eq!(v["config"]["provider"]["alpaca"]["account_type"], "live");
    }

    #[tokio::test]
    async fn config_put_invalid_writes_nothing() {
        // No provider at all -> validation failure with the stable `config` code.
        let resp = send_json(Method::PUT, "/api/config", r#"{"provider": {}}"#, None).await;
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let v: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(v["code"], "config");
    }

    #[tokio::test]
    async fn config_put_malformed_json_is_bad_request() {
        let resp = send_json(Method::PUT, "/api/config", "{not json", None).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let v: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(v["code"], "bad_request");
    }

    #[tokio::test]
    async fn config_put_cross_origin_is_rejected() {
        let body = r#"{"provider": {"alpaca": {"account_type": "paper"}}}"#;
        let resp = send_json(Method::PUT, "/api/config", body, Some("http://evil.example")).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn config_put_wrong_content_type_is_rejected() {
        let app = router(state(), None);
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/api/config")
                    .header("content-type", "text/plain")
                    .header("host", "127.0.0.1:8080")
                    .body(Body::from("x=1"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
```

Update the read-only invariant test `web_router_get_only`: `PUT /api/config` is now the **single allowed** mutating route; everything else stays rejected:

```rust
    #[tokio::test]
    async fn web_router_single_mutating_route() {
        for route in ROUTES {
            for method in [Method::POST, Method::PUT, Method::DELETE, Method::PATCH] {
                let resp = send(method.clone(), route).await;
                assert_eq!(
                    resp.status(),
                    StatusCode::METHOD_NOT_ALLOWED,
                    "{method} {route} must be rejected"
                );
            }
        }
        // /api/config: PUT is allowed (guarded), all other mutations rejected.
        for method in [Method::POST, Method::DELETE, Method::PATCH] {
            let resp = send(method.clone(), "/api/config").await;
            assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        }
    }
```

(Rename replaces the old `web_router_get_only` test; add `"/config"` and `"/api/config"` to `ROUTES`? **No** — keep `ROUTES` as the strictly-GET-only list and leave `/api/config` + `/config` out of it, covered by the explicit assertions above and by `config_get_returns_config_and_flag`.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancerd web::tests::config_`
Expected: FAIL — 404s (routes not registered).

- [ ] **Step 3: Implement handlers and routes**

Append to `crates/datamancerd/src/web/config_api.rs`:

```rust
use axum::Json;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::control::codes;

/// The `GET`/`PUT /api/config` success payload.
#[derive(Serialize)]
struct ConfigView {
    config: Config,
    restart_required: bool,
    path: String,
}

#[derive(Serialize)]
struct ConfigError {
    code: &'static str,
    message: String,
}

fn error_response(status: StatusCode, code: &'static str, message: String) -> Response {
    (status, Json(ConfigError { code, message })).into_response()
}

fn view(state: &ConfigState, config: Config) -> Response {
    let body = ConfigView {
        restart_required: state.restart_required(),
        path: state.path().display().to_string(),
        config,
    };
    (StatusCode::OK, Json(body)).into_response()
}

/// `GET /api/config` — the on-disk config (shows external hand-edits) plus the
/// restart-required flag and the file path.
pub(crate) async fn get_config(State(state): State<ConfigState>) -> Response {
    match state.read_disk().await {
        Ok(config) => view(&state, config),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            codes::CONFIG,
            e.to_string(),
        ),
    }
}

/// `PUT /api/config` — validate and atomically write a full config. The one
/// mutating route on the web surface; guarded by loopback bind (transport),
/// JSON content type (axum `Json` rejects others with 415, which also forces a
/// CORS preflight for cross-origin scripts), and a same-origin Origin/Host
/// check (blocks non-preflighted cross-site sends).
pub(crate) async fn put_config(
    State(state): State<ConfigState>,
    headers: HeaderMap,
    payload: Result<Json<Config>, JsonRejection>,
) -> Response {
    if !same_origin_ok(&headers) {
        return error_response(
            StatusCode::FORBIDDEN,
            codes::BAD_REQUEST,
            "cross-origin config writes are not allowed".to_string(),
        );
    }
    let Json(config) = match payload {
        Ok(json) => json,
        Err(JsonRejection::UnsupportedMediaType(r)) => return r.into_response(),
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, codes::BAD_REQUEST, e.to_string());
        }
    };
    match state.write(&config).await {
        Ok(()) => view(&state, config),
        Err(e @ (DaemonError::ConfigInvalid(_) | DaemonError::ConfigSerialize(_))) => {
            error_response(StatusCode::UNPROCESSABLE_ENTITY, codes::CONFIG, e.to_string())
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            codes::CONFIG,
            e.to_string(),
        ),
    }
}

/// `true` when `Origin` (if present) and `Host` (if present) are loopback.
/// The UI is same-origin on a loopback bind, so any non-loopback value means a
/// cross-site request that slipped past content-type preflighting.
fn same_origin_ok(headers: &HeaderMap) -> bool {
    fn loopback_host(hostport: &str) -> bool {
        // `[::1]:8080`, `[::1]`, `127.0.0.1:8080`, `localhost` forms.
        let host = hostport
            .strip_prefix('[')
            .and_then(|rest| rest.split(']').next())
            .map_or_else(
                || hostport.split(':').next().unwrap_or(""),
                |v6| v6,
            );
        host == "127.0.0.1" || host == "localhost" || host == "::1"
    }
    let origin_ok = headers
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .is_none_or(|origin| {
            origin
                .strip_prefix("http://")
                .is_some_and(loopback_host)
        });
    let host_ok = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .is_none_or(loopback_host);
    origin_ok && host_ok
}
```

**Note:** `codes::CONFIG` and `codes::BAD_REQUEST` must exist as `pub` consts in `crate::control::codes` — check `control.rs`; the explorer report lists `config` and `bad_request` among the stable codes. If the consts are `pub(crate)` or differently named, reference the actual const names; do not mint new strings.

`JsonRejection::UnsupportedMediaType` variant name: in axum 0.8 the rejection enum has variants `JsonDataError`, `JsonSyntaxError`, `MissingJsonContentType`, `BytesRejection`. Match `MissingJsonContentType` and return `StatusCode::UNSUPPORTED_MEDIA_TYPE` with the `bad_request` code body if the variant above does not exist:

```rust
        Err(JsonRejection::MissingJsonContentType(_)) => {
            return error_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                codes::BAD_REQUEST,
                "config writes require Content-Type: application/json".to_string(),
            );
        }
```

Register the routes in `crates/datamancerd/src/web/mod.rs` (`put` joins the `get` import from `axum::routing`):

```rust
use axum::routing::{get, put};
...
        .route("/api/config", get(config_api::get_config).put(config_api::put_config))
```

(One `.route` call with chained methods; drop the separate `put` import if unused.)

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancerd web && cargo clippy -p datamancerd --all-targets --all-features -- -D warnings`
Expected: all web tests PASS including the six new config tests and the renamed mutation-invariant test; clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancerd/src/web/config_api.rs crates/datamancerd/src/web/mod.rs
git commit -m "feat(datamancerd): GET/PUT /api/config with same-origin + content-type guards"
```

---

### Task 8: SSE envelope + restart banner on the operator page

**Files:**
- Modify: `crates/datamancerd/src/web/handlers.rs`
- Modify: `crates/datamancerd/src/web/ui.rs`

**Interfaces:**
- Consumes: `ConfigState::restart_required` (Task 5), `AppState` (Task 6).
- Produces: `GET /api/stream` events change shape from a bare `SystemSnapshot` to `{"snapshot": <SystemSnapshot>, "restart_required": bool}`. `live_json_stream(state: &WebState, config: ConfigState)` (signature change). The operator page shows a banner div toggled by the flag and a header link to `/config`.

- [ ] **Step 1: Update the SSE unit test to expect the envelope**

In `crates/datamancerd/src/web/handlers.rs` tests, `sse_stream_emits_initial_then_on_change` — build a `ConfigState` (same tempfile seeding pattern as `web/mod.rs::state()`), pass it to `live_json_stream`, and parse the envelope:

```rust
        let first: Value = serde_json::from_str(&first).unwrap();
        let first_snap: SystemSnapshot = serde_json::from_value(first["snapshot"].clone()).unwrap();
        assert_eq!(first_snap, testdata::snapshot());
        assert_eq!(first["restart_required"], Value::Bool(false));
```

(add `use serde_json::Value;` to the test imports; apply the same envelope unwrap to the `second` sample.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancerd sse_stream`
Expected: COMPILE ERROR (signature) or FAIL (shape).

- [ ] **Step 3: Implement the envelope**

In `crates/datamancerd/src/web/handlers.rs`:

```rust
use crate::web::config_api::ConfigState;

/// The SSE event payload: the live snapshot plus the config restart flag, so
/// the banner updates without a page reload.
#[derive(Serialize)]
struct StreamEvent<'a> {
    snapshot: &'a SystemSnapshot,
    restart_required: bool,
}

/// Build the underlying stream of serialized live-state envelopes. Emits the
/// current snapshot immediately, then again on every live-refresh publish.
pub(crate) fn live_json_stream(
    state: &WebState,
    config: ConfigState,
) -> impl Stream<Item = String> + use<> {
    let state = state.clone();
    WatchStream::new(state.live_version()).map(move |_version| {
        let snap = state.live_snapshot();
        let event = StreamEvent {
            snapshot: &snap,
            restart_required: config.restart_required(),
        };
        serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string())
    })
}

/// `GET /api/stream` — SSE of the live-state envelope, one event per refresh.
pub(crate) async fn stream(
    State(state): State<WebState>,
    State(config): State<ConfigState>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let events =
        live_json_stream(&state, config).map(|json| Ok(Event::default().data(json)));
    Sse::new(events).keep_alive(KeepAlive::default())
}
```

(Two `State` extractors are fine: both `WebState` and `ConfigState` are `FromRef<AppState>`.)

In `crates/datamancerd/src/web/ui.rs`:

- Header additions inside `header { ... }`:

```rust
                    p.status { "stream: " span #conn { "connecting…" } " · " a href="/config" { "settings" } }
                    div #banner hidden { "Configuration changed on disk — restart datamancerd to apply." }
```

- CSS addition to `PAGE_CSS`:

```
#banner { background: #b45309; color: #fff; padding: .4rem .6rem; border-radius: 4px; margin: .4rem 0; }
```

- `PAGE_JS` changes: the SSE `onmessage` unwraps the envelope and toggles the banner:

```
es.onmessage = (ev) => {
  try {
    const d = JSON.parse(ev.data);
    document.getElementById('banner').hidden = !d.restart_required;
    paint(d.snapshot);
  } catch (e) {}
};
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancerd web && cargo clippy -p datamancerd --all-targets --all-features -- -D warnings`
Expected: PASS (including the updated SSE test), clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancerd/src/web/handlers.rs crates/datamancerd/src/web/ui.rs
git commit -m "feat(datamancerd): SSE envelope with restart_required + operator-page banner"
```

---

### Task 9: The `/config` settings page

**Files:**
- Create: `crates/datamancerd/src/web/settings.rs`
- Modify: `crates/datamancerd/src/web/mod.rs` (register module + route)

**Interfaces:**
- Consumes: `GET /api/config`, `PUT /api/config` (Task 7) — the page is a server-rendered shell whose inline JS fetches the config, renders typed inputs per section, and PUTs the assembled JSON back.
- Produces: `GET /config` route returning HTML (maud). No new server-side state.

- [ ] **Step 1: Write the failing route test**

Append to `crates/datamancerd/src/web/mod.rs` tests:

```rust
    #[tokio::test]
    async fn settings_page_serves_form_shell() {
        let resp = send(Method::GET, "/config").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = String::from_utf8(body_bytes(resp).await).unwrap();
        assert!(body.contains("id=\"settings\""), "form container present");
        assert!(body.contains("/api/config"), "wired to the config API");
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancerd settings_page`
Expected: FAIL — 404.

- [ ] **Step 3: Implement the page**

Create `crates/datamancerd/src/web/settings.rs`:

```rust
//! The `/config` settings page: a server-rendered shell whose inline JS
//! fetches `GET /api/config`, renders a structured form (typed inputs per
//! section, repeatable startup-session rows), and submits the assembled
//! config back via `PUT /api/config`. Validation is entirely server-side —
//! the form never re-implements the `Config` schema rules; it renders what
//! the API returns and displays what the API rejects.
//!
//! Apply-on-restart: a successful save flips the restart banner; the daemon's
//! running config is unchanged until restart.

use maud::{DOCTYPE, Markup, html};

/// `GET /config` — the settings shell.
pub(crate) async fn page() -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "datamancerd — settings" }
                style { (CSS) }
            }
            body {
                header {
                    h1 { a href="/" { "datamancerd" } " / settings" }
                    p.sub { "edits the config file; changes apply on restart" }
                    div #banner hidden { "Configuration changed on disk — restart datamancerd to apply." }
                    div #error hidden {}
                }
                main {
                    div #settings { "loading…" }
                    p {
                        button #save disabled { "Save config" }
                        span #path.note {}
                    }
                }
                script { (maud::PreEscaped(JS)) }
            }
        }
    }
}

const CSS: &str = r"
:root { color-scheme: light dark; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
body { margin: 0; padding: 1rem 1.25rem; line-height: 1.5; max-width: 60rem; }
header h1 { margin: 0; font-size: 1.4rem; } header h1 a { color: inherit; }
.sub { margin: .15rem 0 1rem; opacity: .7; }
.note { font-size: .8rem; opacity: .6; margin-left: 1rem; }
#banner { background: #b45309; color: #fff; padding: .4rem .6rem; border-radius: 4px; margin: .4rem 0; }
#error { background: #b91c1c; color: #fff; padding: .4rem .6rem; border-radius: 4px; margin: .4rem 0; white-space: pre-wrap; }
fieldset { border: 1px solid rgba(127,127,127,.4); border-radius: 4px; margin: 0 0 1rem; }
legend { font-weight: 600; padding: 0 .4rem; }
label { display: inline-block; margin: .2rem 1rem .2rem 0; }
input[type=text], input[type=number] { font: inherit; width: 14rem; }
input[type=number] { width: 8rem; }
select { font: inherit; }
button { font: inherit; padding: .3rem .8rem; }
.session-row { border-bottom: 1px dashed rgba(127,127,127,.4); padding: .4rem 0; }
";

const JS: &str = r#"
const $ = (id) => document.getElementById(id);
let current = null;

const SEL = (name, options, value) =>
  `<select name="${name}">` + options.map(o => `<option value="${o}"${o===value?' selected':''}>${o}</option>`).join('') + `</select>`;
const TXT = (name, value, ph) => `<input type="text" name="${name}" value="${value ?? ''}" placeholder="${ph ?? ''}">`;
const NUM = (name, value) => `<input type="number" name="${name}" value="${value}">`;
const CHK = (name, value) => `<input type="checkbox" name="${name}"${value?' checked':''}>`;
const L = (text, control) => `<label>${text} ${control}</label>`;

const KINDS = ['trade','quote','bar_1s','bar_1m','bar_5m','bar_15m','bar_1h','bar_1d'];
const PERSIST = ['none','cached','cached_with_tap','read_only','refresh','tap_only'];

function sessionRow(s, i) {
  s = s || {provider:'alpaca-crypto', asset_class:'crypto', symbol:'', kind:'trade',
            scope:'live', persistence:'none', always_on:false};
  return `<div class="session-row" data-i="${i}">`
    + L('provider', TXT(`ss-provider-${i}`, s.provider))
    + L('asset_class', SEL(`ss-asset-${i}`, ['equity','crypto'], s.asset_class))
    + L('symbol', TXT(`ss-symbol-${i}`, s.symbol))
    + L('kind', SEL(`ss-kind-${i}`, KINDS, s.kind))
    + L('scope', SEL(`ss-scope-${i}`, ['live','live_backfill'], s.scope))
    + L('backfill_from', TXT(`ss-backfill-${i}`, s.backfill_from, 'RFC3339, for live_backfill'))
    + L('persistence', SEL(`ss-persist-${i}`, PERSIST, s.persistence))
    + L('always_on', CHK(`ss-always-${i}`, s.always_on))
    + `<button type="button" data-remove="${i}">remove</button></div>`;
}

function storageFields(key, cfg) {
  const on = cfg != null;
  return `<fieldset><legend>${key}</legend>`
    + L('enabled', CHK(`${key}-on`, on))
    + L('backend', SEL(`${key}-backend`, ['surreal-embedded','surreal-memory'], on ? cfg.backend : 'surreal-embedded'))
    + L('path', TXT(`${key}-path`, on ? cfg.path : ''))
    + `</fieldset>`;
}

function render(cfg) {
  const p = cfg.provider || {};
  const w = cfg.web_ui || {enabled:false, bind:'127.0.0.1', port:8080,
                           live_state_cadence_ms:1000, cache_catalog_cadence_ms:30000};
  $('settings').innerHTML =
    `<fieldset><legend>provider.alpaca (equities)</legend>`
      + L('enabled', CHK('alpaca-on', !!p.alpaca))
      + L('account_type', SEL('alpaca-account', ['paper','live'], p.alpaca?.account_type ?? 'paper'))
    + `</fieldset>`
    + `<fieldset><legend>provider.alpaca_crypto</legend>`
      + L('enabled', CHK('crypto-on', !!p.alpaca_crypto))
      + L('account_type', SEL('crypto-account', ['paper','live'], p.alpaca_crypto?.account_type ?? 'paper'))
      + L('venue', SEL('crypto-venue', ['us','us_kraken','eu_kraken'], p.alpaca_crypto?.venue ?? 'us'))
    + `</fieldset>`
    + storageFields('cache', cfg.cache)
    + storageFields('tap_log', cfg.tap_log)
    + `<fieldset><legend>session</legend>`
      + L('resume_buffer_events', NUM('sess-buffer', cfg.session.resume_buffer_events))
      + L('adjustment', SEL('sess-adjust', ['raw','split','dividend','spin_off','all'], cfg.session.adjustment))
    + `</fieldset>`
    + `<fieldset><legend>server</legend>`
      + L('admin_socket', TXT('srv-socket', cfg.server.admin_socket))
      + L('service_prefix', TXT('srv-prefix', cfg.server.service_prefix))
      + L('shutdown_timeout_secs', NUM('srv-timeout', cfg.server.shutdown_timeout_secs))
    + `</fieldset>`
    + `<fieldset><legend>diagnostics</legend>`
      + L('publish_interval_ms', NUM('diag-live', cfg.diagnostics.publish_interval_ms))
      + L('cache_catalog_interval_ms', NUM('diag-catalog', cfg.diagnostics.cache_catalog_interval_ms))
    + `</fieldset>`
    + `<fieldset><legend>iceoryx2</legend>`
      + L('max_clients', NUM('iox-clients', cfg.iceoryx2.max_clients))
    + `</fieldset>`
    + `<fieldset><legend>web_ui</legend>`
      + L('enabled', CHK('web-on', w.enabled))
      + L('bind', TXT('web-bind', w.bind))
      + L('port', NUM('web-port', w.port))
      + L('assets_dir', TXT('web-assets', w.assets_dir, 'optional'))
      + L('live_state_cadence_ms', NUM('web-live', w.live_state_cadence_ms))
      + L('cache_catalog_cadence_ms', NUM('web-catalog', w.cache_catalog_cadence_ms))
    + `</fieldset>`
    + `<fieldset><legend>startup sessions</legend><div id="sessions">`
      + (cfg.startup_session || []).map(sessionRow).join('')
    + `</div><button type="button" id="add-session">add session</button></fieldset>`;
  $('save').disabled = false;
}

const val = (n) => document.getElementsByName(n)[0].value;
const num = (n) => Number(val(n));
const chk = (n) => document.getElementsByName(n)[0].checked;
const opt = (v) => (v === '' ? undefined : v);

function collect() {
  const cfg = { provider: {} };
  if (chk('alpaca-on')) cfg.provider.alpaca = { account_type: val('alpaca-account') };
  if (chk('crypto-on')) cfg.provider.alpaca_crypto = { account_type: val('crypto-account'), venue: val('crypto-venue') };
  for (const key of ['cache','tap_log']) {
    if (chk(`${key}-on`)) cfg[key] = { backend: val(`${key}-backend`), path: opt(val(`${key}-path`)) };
  }
  cfg.session = { resume_buffer_events: num('sess-buffer'), adjustment: val('sess-adjust') };
  cfg.server = { admin_socket: val('srv-socket'), service_prefix: val('srv-prefix'), shutdown_timeout_secs: num('srv-timeout') };
  cfg.diagnostics = { publish_interval_ms: num('diag-live'), cache_catalog_interval_ms: num('diag-catalog') };
  cfg.iceoryx2 = { max_clients: num('iox-clients') };
  cfg.web_ui = { enabled: chk('web-on'), bind: val('web-bind'), port: num('web-port'),
                 assets_dir: opt(val('web-assets')),
                 live_state_cadence_ms: num('web-live'), cache_catalog_cadence_ms: num('web-catalog') };
  cfg.startup_session = [...document.querySelectorAll('.session-row')].map(row => {
    const i = row.dataset.i;
    return { provider: val(`ss-provider-${i}`), asset_class: val(`ss-asset-${i}`),
             symbol: val(`ss-symbol-${i}`), kind: val(`ss-kind-${i}`),
             scope: val(`ss-scope-${i}`), backfill_from: opt(val(`ss-backfill-${i}`)),
             persistence: val(`ss-persist-${i}`), always_on: chk(`ss-always-${i}`) };
  });
  return cfg;
}

function show(data) {
  current = data.config;
  $('banner').hidden = !data.restart_required;
  $('path').textContent = data.path;
  render(current);
}

function fail(msg) { const e = $('error'); e.hidden = false; e.textContent = msg; }

let nextI = 0;
document.addEventListener('click', async (ev) => {
  if (ev.target.id === 'add-session') {
    const div = document.createElement('div');
    div.innerHTML = sessionRow(null, `n${nextI++}`);
    $('sessions').appendChild(div.firstChild);
  } else if (ev.target.dataset.remove !== undefined) {
    ev.target.closest('.session-row').remove();
  } else if (ev.target.id === 'save') {
    $('error').hidden = true;
    const resp = await fetch('/api/config', {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(collect()),
    });
    const data = await resp.json().catch(() => null);
    if (resp.ok && data) { show(data); }
    else { fail(data ? `${data.code}: ${data.message}` : `save failed (${resp.status})`); }
  }
});

fetch('/api/config').then(r => r.json()).then(show).catch(e => fail(String(e)));
"#;
```

When rendering fresh session rows the JS uses string indices (`n0`, `n1`, …) — the `data-i` lookup keys are strings throughout, so mixed initial/added rows collect correctly.

Register in `crates/datamancerd/src/web/mod.rs`:

```rust
mod settings;
...
        .route("/config", get(settings::page))
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p datamancerd web && cargo clippy -p datamancerd --all-targets --all-features -- -D warnings`
Expected: PASS, clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancerd/src/web/settings.rs crates/datamancerd/src/web/mod.rs
git commit -m "feat(datamancerd): /config settings page with structured form"
```

---

### Task 10: Server wiring, docs, and end-to-end verification

**Files:**
- Modify: `crates/datamancerd/src/main.rs`
- Modify: `crates/datamancerd/src/server.rs`
- Modify: `crates/datamancerd/src/web/mod.rs` (module security docs)
- Modify: `crates/datamancerd/README.md`

**Interfaces:**
- Consumes: everything above.
- Produces: `Server::bootstrap(config: Config, config_path: PathBuf)`; the daemon threads a `ConfigState` into `start_web`.

- [ ] **Step 1: Thread the config path through the server** *(skip parts already done as Task 6 Step 3 required)*

`crates/datamancerd/src/server.rs`:

- Add fields to `Server` (following the existing `web` field's cfg pattern):

```rust
    /// The daemon's boot config + config-file path, handed to the web layer
    /// for the settings surface. Runtime config stays immutable after boot.
    #[cfg(feature = "web-ui")]
    config_state: crate::web::ConfigState,
```

- Change `bootstrap` to accept the path and clone the boot config before it is consumed:

```rust
    pub async fn bootstrap(config: Config, config_path: std::path::PathBuf) -> Result<Self> {
        #[cfg(feature = "web-ui")]
        let config_state = crate::web::ConfigState::new(config_path, config.clone());
        #[cfg(not(feature = "web-ui"))]
        let _ = config_path;
        ...
        Ok(Self {
            ...
            #[cfg(feature = "web-ui")]
            config_state,
            draining: false,
        })
    }
```

- In `start_web`, replace the `state` local (as noted in Task 6):

```rust
        let state = crate::web::AppState {
            snapshots: refreshers.state.clone(),
            config: self.config_state.clone(),
        };
```

`crates/datamancerd/src/main.rs` `run()` final line:

```rust
    server::Server::bootstrap(config, config_path).await?.run().await
}
```

- [ ] **Step 2: Update the security/docs surfaces**

- `crates/datamancerd/src/web/mod.rs` module docs: replace the "**`GET`-only**" bullet with:

```
//! - **One mutating route**: `PUT /api/config` (validated, atomic, loopback +
//!   same-origin + JSON-content-type guarded) writes the config *file*;
//!   apply-on-restart, the running daemon is never mutated. Everything else is
//!   `GET`-only (guarded by `web_router_single_mutating_route`).
```

- `crates/datamancerd/README.md`:
  - Config section: document the default path (macOS `~/Library/Application Support/datamancerd/config.toml`, Linux `~/.config/datamancerd/config.toml`), first-run scaffolding, `--config` as optional override (explicit missing path errors, never scaffolds).
  - Web UI / security section: document `GET /config`, `GET/PUT /api/config` (request/response shapes, error codes `config`/`bad_request`, guards), the restart-required banner semantics (parsed-equality vs boot config), the SSE envelope shape change (`{"snapshot": ..., "restart_required": ...}`), and that UI saves drop TOML comments.

- [ ] **Step 3: Full verification**

Run:
```bash
cargo test -p datamancerd
cargo clippy -p datamancerd --all-targets --all-features -- -D warnings
cargo build -p datamancerd --no-default-features   # web-ui off must still compile
cargo fmt --check
```
Expected: all PASS/clean.

Manual smoke (uses a scratch config, not the real default dir):

```bash
cd /private/tmp && mkdir -p dmc-smoke && cat > dmc-smoke/config.toml <<'EOF'
[provider.alpaca]
account_type = "paper"
[server]
admin_socket = "/private/tmp/dmc-smoke/admin.sock"
[web_ui]
enabled = true
port = 8099
EOF
cargo run -p datamancerd -- --config /private/tmp/dmc-smoke/config.toml &
sleep 3
curl -s http://127.0.0.1:8099/api/config | head -c 400   # config JSON + restart_required:false
curl -s -X PUT http://127.0.0.1:8099/api/config -H 'content-type: application/json' \
  -d '{"provider":{"alpaca":{"account_type":"live"}},"server":{"admin_socket":"/private/tmp/dmc-smoke/admin.sock"},"web_ui":{"enabled":true,"port":8099}}' | head -c 400
# expect restart_required:true; then confirm the file was rewritten:
cat /private/tmp/dmc-smoke/config.toml
kill %1
```

Expected: GET returns the config with `"restart_required":false`; PUT returns `"restart_required":true`; the file on disk shows `account_type = "live"`.

- [ ] **Step 4: Commit**

```bash
git add crates/datamancerd/src/main.rs crates/datamancerd/src/server.rs crates/datamancerd/src/web/mod.rs crates/datamancerd/README.md
git commit -m "feat(datamancerd): wire ConfigState through the server; document the config contract"
```
