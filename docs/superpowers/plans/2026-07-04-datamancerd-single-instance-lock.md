# datamancerd Single-Instance Lock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Guarantee at most one `datamancerd` per user on a host — regardless of which config it is given — by holding a global advisory file lock for the process lifetime, failing a second launch fast and clearly.

**Architecture:** A new `single_instance` module acquires an exclusive `flock` on a fixed, config-independent lockfile in the platform data directory and returns a guard whose live file descriptor keeps the lock held. `main.rs::run()` acquires the guard before any config load, DB open, or iceoryx2 node creation, holding it for the whole process. Because the lock authoritatively answers "is another daemon running," `Server::clear_stale_socket` is simplified to drop its indirect connect-based liveness check.

**Tech Stack:** Rust (edition 2024), `rustix` (safe `flock`, already in the compiled tree), `thiserror`, `tokio`.

## Global Constraints

- `#![forbid(unsafe_code)]` in `datamancerd` — the lock primitive must be a safe API (`rustix::fs::flock`), no `libc` FFI.
- Workspace lints: `clippy::pedantic = deny`. Code must pass `cargo clippy --all-targets -- -D warnings`.
- Member crate opts into workspace lints via `[lints] workspace = true` — no per-crate lint config changes.
- The lock path MUST be derived from the platform data dir (`paths::default_data_dir()`), never from the loaded config — this is what makes the guarantee global rather than per-config.
- The lockfile is never deleted on release (avoids the unlink/recreate race); the kernel releases the lock when the fd closes.
- New dependency line, exact: `rustix = { version = "1", features = ["fs"] }`.
- Lockfile basename, exact: `datamancerd.lock`.

---

### Task 1: `single_instance` module — the lock primitive

**Files:**
- Modify: `crates/datamancerd/Cargo.toml` (add `rustix` dependency)
- Modify: `crates/datamancerd/src/error.rs` (add `AlreadyRunning` variant)
- Create: `crates/datamancerd/src/single_instance.rs`
- Modify: `crates/datamancerd/src/main.rs:21-30` (register `mod single_instance;`)

**Interfaces:**
- Consumes: `crate::paths::default_data_dir() -> Option<PathBuf>`, `crate::error::{DaemonError, Result}`.
- Produces:
  - `DaemonError::AlreadyRunning { pid: Option<u32>, path: PathBuf }`
  - `single_instance::InstanceLock` with `pub fn acquire() -> Result<InstanceLock>` and private `fn acquire_at(path: &Path) -> Result<InstanceLock>`. The guard must be held (bound to a live variable) for as long as single-instance exclusion is required; dropping it releases the lock.

- [ ] **Step 1: Add the `rustix` dependency**

In `crates/datamancerd/Cargo.toml`, under `[dependencies]`, after the `directories = "6"` line, add:

```toml
# Safe flock(2) for the global single-instance lock. Already in the tree
# transitively; the `fs` feature exposes `rustix::fs::flock`.
rustix = { version = "1", features = ["fs"] }
```

- [ ] **Step 2: Add the `AlreadyRunning` error variant**

In `crates/datamancerd/src/error.rs`, add this variant to the `DaemonError` enum (e.g. after the `Io` variant, before the closing brace):

```rust
    /// Another `datamancerd` already holds the global single-instance lock.
    #[error(
        "another datamancerd is already running{}; single-instance lock held at {path}",
        pid.map_or_else(String::new, |p| format!(" (pid {p})"))
    )]
    AlreadyRunning { pid: Option<u32>, path: PathBuf },
```

(`PathBuf` is already imported at the top of `error.rs`.)

- [ ] **Step 3: Write the `single_instance` module with failing tests**

Create `crates/datamancerd/src/single_instance.rs`:

```rust
//! Global single-instance lock: at most one `datamancerd` per user on a host.
//!
//! Acquires an exclusive advisory `flock` on a fixed, config-independent
//! lockfile in the platform data directory and holds it for the whole process
//! lifetime. A second launch — regardless of which config it is given — fails
//! to acquire and reports the holding PID. The kernel releases the lock when
//! the process exits (cleanly or not), so a crash leaves at most a harmless
//! unlocked lockfile that the next start re-locks.

use std::fs::File;
use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Path, PathBuf};

use rustix::fs::{FlockOperation, flock};

use crate::error::{DaemonError, Result};
use crate::paths::default_data_dir;

/// Basename of the lockfile within the data directory.
const LOCK_FILE_NAME: &str = "datamancerd.lock";

/// Holds the process-wide single-instance lock. Keeping the `File` open keeps
/// the exclusive `flock` held; dropping it (or process exit) releases it.
#[derive(Debug)]
pub struct InstanceLock {
    // Never read: its sole job is to keep the fd — and thus the flock — alive
    // for the lifetime of this value.
    _file: File,
}

impl InstanceLock {
    /// Acquire the global lock at the fixed, config-independent path
    /// (`<data dir>/datamancerd.lock`).
    ///
    /// # Errors
    ///
    /// - [`DaemonError::ConfigInvalid`] if no home directory exists to derive
    ///   the data directory.
    /// - [`DaemonError::AlreadyRunning`] if another daemon holds the lock.
    /// - [`DaemonError::Io`] for other filesystem errors.
    pub fn acquire() -> Result<Self> {
        let dir = default_data_dir().ok_or_else(|| {
            DaemonError::ConfigInvalid(
                "no home directory found to derive the data directory for the \
                 single-instance lock"
                    .to_string(),
            )
        })?;
        Self::acquire_at(&dir.join(LOCK_FILE_NAME))
    }

    /// Testable core of [`acquire`]: the lock path is injected so tests never
    /// touch the real data directory.
    fn acquire_at(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        match flock(&file, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {}
            Err(e) if e == rustix::io::Errno::WOULDBLOCK || e == rustix::io::Errno::AGAIN => {
                let pid = read_pid(&mut file);
                return Err(DaemonError::AlreadyRunning {
                    pid,
                    path: path.to_path_buf(),
                });
            }
            Err(e) => return Err(std::io::Error::from(e).into()),
        }

        // Lock held. Record our PID for diagnostics only; the lock — not the
        // file body — is authoritative.
        file.set_len(0)?;
        file.seek(std::io::SeekFrom::Start(0))?;
        write!(file, "{}", std::process::id())?;
        file.flush()?;

        Ok(Self { _file: file })
    }
}

/// Best-effort read of the PID text a lock holder wrote. `None` if the file is
/// empty or unparseable — there is a brief window between another process
/// acquiring the lock and writing its PID.
fn read_pid(file: &mut File) -> Option<u32> {
    file.seek(std::io::SeekFrom::Start(0)).ok()?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).ok()?;
    buf.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_writes_pid_and_creates_parent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested/datamancerd.lock");
        let lock = InstanceLock::acquire_at(&path).expect("first acquire");
        assert!(path.exists(), "lockfile created under a fresh parent dir");
        let contents = std::fs::read_to_string(&path).expect("read lockfile");
        assert_eq!(
            contents.trim(),
            std::process::id().to_string(),
            "lockfile records our PID"
        );
        drop(lock);
    }

    #[test]
    fn second_acquire_fails_while_held() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("datamancerd.lock");
        let held = InstanceLock::acquire_at(&path).expect("first acquire");
        match InstanceLock::acquire_at(&path) {
            Err(DaemonError::AlreadyRunning { pid, path: reported }) => {
                assert_eq!(pid, Some(std::process::id()), "reports the holder PID");
                assert_eq!(reported, path, "reports the lock path");
            }
            other => panic!("expected AlreadyRunning, got {other:?}"),
        }
        drop(held);
    }

    #[test]
    fn reacquire_after_release_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("datamancerd.lock");
        let first = InstanceLock::acquire_at(&path).expect("first acquire");
        drop(first);
        let _second =
            InstanceLock::acquire_at(&path).expect("re-acquire after the first is released");
    }
}
```

- [ ] **Step 4: Register the module**

In `crates/datamancerd/src/main.rs`, add `mod single_instance;` to the module list (the block at lines 21-30, alphabetically after `mod shutdown;`):

```rust
mod config;
mod control;
mod error;
mod paths;
mod server;
mod shutdown;
mod single_instance;
#[cfg(feature = "web-ui")]
mod web;
#[cfg(feature = "ws")]
mod ws;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p datamancerd single_instance`
Expected: PASS — `acquire_writes_pid_and_creates_parent`, `second_acquire_fails_while_held`, `reacquire_after_release_succeeds` all green. (`main.rs` currently has no reference to `InstanceLock`; the `#[derive(Debug)]` and `pub fn acquire` may draw a `dead_code`/unused warning until Task 2 wires it in — the next step tolerates that, Task 2 removes it.)

- [ ] **Step 6: Verify clippy is clean for the new module**

Run: `cargo clippy -p datamancerd --all-targets -- -D warnings`
Expected: no errors from `single_instance.rs` or `error.rs`. If clippy flags `acquire` as unused (dead_code) because nothing calls it yet, that is resolved in Task 2; if it errors here, add `#[allow(dead_code)]` on `impl InstanceLock`'s `acquire` **only if** the build fails, and remove it in Task 2. Prefer proceeding to Task 2 in the same review batch so no allow is needed.

- [ ] **Step 7: Commit**

```bash
git add crates/datamancerd/Cargo.toml crates/datamancerd/src/error.rs \
        crates/datamancerd/src/single_instance.rs crates/datamancerd/src/main.rs
git commit -m "feat(datamancerd): add global single-instance lock primitive"
```

---

### Task 2: Acquire the lock in `main.rs::run()`

**Files:**
- Modify: `crates/datamancerd/src/main.rs:68-77` (the `run` function)

**Interfaces:**
- Consumes: `single_instance::InstanceLock::acquire() -> Result<InstanceLock>` (Task 1).
- Produces: no new public surface — the daemon now holds the lock for its whole lifetime; a second launch exits non-zero with the `AlreadyRunning` error.

- [ ] **Step 1: Acquire the guard before any shared resource**

Replace the body of `run()` in `crates/datamancerd/src/main.rs` (currently lines 68-77) with:

```rust
async fn run() -> Result<()> {
    let args = Args::parse();
    // Acquire the global single-instance lock before touching any shared
    // resource (config scaffold, tap-log/cache DBs, iceoryx2 node). Held for
    // the whole process; released by the kernel on exit. A second launch —
    // whatever config it is given — fails here.
    let _instance = single_instance::InstanceLock::acquire()?;
    let config_path = paths::resolve_config_path(args.config)?;
    tracing::info!(path = %config_path.display(), "loading config");
    let config = Config::load(&config_path)?;
    server::Server::bootstrap(config, config_path)
        .await?
        .run()
        .await
}
```

The `_instance` binding must stay named `_instance` (not `_`) so the guard lives until `run()` returns rather than dropping immediately.

- [ ] **Step 2: Verify the workspace builds and is clippy-clean**

Run: `cargo clippy -p datamancerd --all-targets -- -D warnings`
Expected: PASS — no `dead_code` warning for `acquire` now that `run()` calls it.

- [ ] **Step 3: Verify unit tests still pass**

Run: `cargo test -p datamancerd`
Expected: PASS (existing tests plus the three `single_instance` tests).

- [ ] **Step 4: Manually verify mutual exclusion end-to-end**

This wiring is on the binary entry point (not unit-testable). Verify by hand:

```bash
# Terminal A — start the daemon (adjust config path as needed):
cargo run -p datamancerd
# Terminal B — while A runs, a second launch must fail fast:
cargo run -p datamancerd; echo "exit=$?"
```

Expected: Terminal B logs an error like
`another datamancerd is already running (pid <A>); single-instance lock held at <data dir>/datamancerd.lock`
and prints `exit=1`. Stop Terminal A (Ctrl-C); a subsequent launch in B then starts normally.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancerd/src/main.rs
git commit -m "feat(datamancerd): hold single-instance lock for the process lifetime"
```

---

### Task 3: Simplify `clear_stale_socket`

**Files:**
- Modify: `crates/datamancerd/src/server.rs:442-476` (the `clear_stale_socket` method + its doc comment)

**Interfaces:**
- Consumes: nothing new. Relies on the invariant that the global lock (Task 2) is acquired before `Server::run` → `bind_socket` → `clear_stale_socket`, so no other daemon can be running.
- Produces: no signature change — `fn clear_stale_socket(&self) -> Result<()>` unchanged; only its body and doc comment change.

- [ ] **Step 1: Replace the method with the simplified version**

In `crates/datamancerd/src/server.rs`, replace the entire `clear_stale_socket` method (currently lines 442-476, from its doc comment through its closing brace) with:

```rust
    /// Remove a *stale* admin socket left by an unclean prior exit.
    ///
    /// The global single-instance lock (acquired before `run`) guarantees no
    /// other daemon is running by the time we reach here, so any socket at this
    /// path is necessarily stale and safe to remove. Still refuses to delete a
    /// path that exists and is *not* a socket, so a misconfiguration cannot
    /// clobber an arbitrary file.
    fn clear_stale_socket(&self) -> Result<()> {
        use std::os::unix::fs::FileTypeExt;
        let meta = match std::fs::symlink_metadata(&self.admin_socket) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if !meta.file_type().is_socket() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "admin_socket path {} exists and is not a socket; refusing to remove it",
                    self.admin_socket.display()
                ),
            )
            .into());
        }
        // A socket here is necessarily stale (the lock rules out a live daemon).
        std::fs::remove_file(&self.admin_socket)?;
        Ok(())
    }
```

This drops the `std::os::unix::net::UnixStream::connect(...).is_ok()` block (the old `AddrInUse` / "already in use" branch), which is now unreachable.

- [ ] **Step 2: Verify the workspace builds and is clippy-clean**

Run: `cargo clippy -p datamancerd --all-targets -- -D warnings`
Expected: PASS — no unused-import or dead-code warnings (the `UnixStream` reference is gone; `UnixStream`/`UnixListener` are still used elsewhere in `server.rs`).

- [ ] **Step 3: Verify the full test suite passes**

Run: `cargo test -p datamancerd`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/datamancerd/src/server.rs
git commit -m "refactor(datamancerd): drop connect-based stale-socket check now covered by the instance lock"
```

---

### Task 4: Document the single-instance lock

**Files:**
- Modify: `crates/datamancerd/README.md:36-41` (insert a subsection after the credentials paragraph under `## Running`)

**Interfaces:**
- Consumes: nothing. Documentation only.
- Produces: operator-facing docs for the lock path and failure behavior.

- [ ] **Step 1: Add the "Single instance" subsection**

In `crates/datamancerd/README.md`, immediately after the credentials paragraph that ends with `` `live` → `ALPACA_LIVE_API_KEY_ID`/`ALPACA_LIVE_API_SECRET_KEY`). `` (line 40) and before `### Config file location`, insert:

```markdown

### Single instance

Only one `datamancerd` runs per user on a host — **regardless of which config it
is given**. At startup, before loading config or opening any storage, the daemon
takes an exclusive advisory lock (`flock`) on a fixed, config-independent
lockfile:

- macOS: `~/Library/Application Support/datamancerd/datamancerd.lock`
- Linux: `~/.local/share/datamancerd/datamancerd.lock` (`$XDG_DATA_HOME` respected)

A second launch while one is running fails fast and exits non-zero with, e.g.:

```
another datamancerd is already running (pid 4321); single-instance lock held at \
<data dir>/datamancerd.lock
```

The lock is held for the whole process and released by the kernel on exit —
clean or not — so a crash leaves at most a harmless leftover lockfile that the
next start re-locks. The file's contents (the holder's PID) are diagnostic only;
the lock itself is authoritative. This is a **per-host, per-user** guarantee: it
does not coordinate across users or hosts.
```

- [ ] **Step 2: Verify the doc renders and links are intact**

Run: `git diff crates/datamancerd/README.md`
Expected: a clean insertion of the new `### Single instance` subsection between the credentials paragraph and `### Config file location`; no other sections disturbed.

- [ ] **Step 3: Commit**

```bash
git add crates/datamancerd/README.md
git commit -m "docs(datamancerd): document the single-instance lock"
```

---

## Self-Review

**Spec coverage:**
- Mechanism (process-lifetime `flock`, `InstanceLock`, `acquire`/`acquire_at`) → Task 1.
- Fixed config-independent lock path via `default_data_dir()` + `datamancerd.lock` → Task 1 (`acquire`).
- Acquire semantics (create/open, non-blocking exclusive, PID write, WOULDBLOCK→`AlreadyRunning`) → Task 1 Step 3.
- Release semantics (fd-close releases, no delete) → Task 1 (no unlink on drop) + README (Task 4).
- Wiring before shared resources → Task 2.
- `rustix` dep + `AlreadyRunning` error variant → Task 1 Steps 1-2.
- Simplify `clear_stale_socket` (drop connect check, keep non-socket guard + stale removal) → Task 3.
- Four unit tests via `acquire_at` → Task 1 covers acquire+PID, second-acquire-fails, reacquire-after-release, and missing-parent-dir (folded into `acquire_writes_pid_and_creates_parent`, which uses a `nested/` subpath).
- README operator docs → Task 4.

**Placeholder scan:** none — every code and command step is concrete.

**Type consistency:** `InstanceLock`, `acquire()`, `acquire_at(&Path)`, and `DaemonError::AlreadyRunning { pid: Option<u32>, path: PathBuf }` are named identically across Tasks 1-2 and the tests. `read_pid(&mut File) -> Option<u32>` is used only within Task 1.

Note: the spec lists four discrete tests; this plan folds "missing parent dir is created" into the first test (`acquire_writes_pid_and_creates_parent`) by locating the lockfile under a `nested/` subdir, so all four behaviors are exercised across three test functions.
