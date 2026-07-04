# datamancerd single-instance lock — design

**Date:** 2026-07-04
**Status:** approved, ready for implementation plan

## Problem

`datamancerd` must run as at most one instance per user on a host. Today the only
guard is `Server::clear_stale_socket` (`crates/datamancerd/src/server.rs`), which
is keyed on the `admin_socket` path and infers liveness by connecting to that
socket. This has three gaps:

1. **Path-scoped only.** Launching `datamancerd` twice with two config files that
   point at different `admin_socket` paths lets both run — colliding on the
   tap-log DB, cache DB, iceoryx2 service names, and the web-UI port.
2. **TOCTOU race.** `clear_stale_socket` → `UnixListener::bind` is not atomic; two
   daemons booting simultaneously can both pass the connect-check.
3. **Indirect liveness.** "A daemon is alive" is inferred from "something is
   listening on this socket," not from a real process/lock identity.

## Goal

At most one `datamancerd` per user on a host, **regardless of which config it is
given**. A second launch fails fast with a clear, actionable error. Scope chosen:
**per machine/user (global)** — the strongest guarantee, matching "only ever a
single instance."

## Mechanism — process-lifetime advisory `flock`

A new module `crates/datamancerd/src/single_instance.rs`:

```rust
pub struct InstanceLock {
    file: std::fs::File, // holding the fd keeps the exclusive flock held
}

impl InstanceLock {
    /// Acquire the global lock at the fixed, config-independent path.
    pub fn acquire() -> Result<Self>;

    /// Testable core: the lock path is injected so tests never touch the real
    /// home directory (mirrors `paths::resolve_in`).
    fn acquire_at(path: &std::path::Path) -> Result<Self>;
}
```

### Lock path — fixed and config-independent

This is what makes the guarantee *global* rather than per-config: the path is
derived from the platform data dir and never from the loaded config.

- `paths::default_data_dir()` + `datamancerd.lock`:
  - macOS: `~/Library/Application Support/datamancerd/datamancerd.lock`
  - Linux: `~/.local/share/datamancerd/datamancerd.lock` (`$XDG_DATA_HOME` respected)
- No home directory → `DaemonError::ConfigInvalid`-style error, mirroring
  `paths::resolve_config_path`'s "no home directory" handling. (A dedicated
  message is fine; it must tell the operator no default path could be derived.)
- The lockfile's parent directory is created (`create_dir_all`) before opening,
  matching how `resolve_in` scaffolds the config dir.

### Acquire semantics

1. Create/open the lockfile (read+write, create).
2. `rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive)`.
   - **Ok:** truncate the file and write `std::process::id()` as text
     (diagnostics only — the lock, not the file contents, is authoritative).
     Keep the `File` inside `InstanceLock`. The lock is held until the struct
     drops.
   - **`Errno::WOULDBLOCK`/`AGAIN`:** another daemon holds the lock. Read the PID
     text currently in the file (best-effort; may be empty or stale in a tiny
     window) and return `DaemonError::AlreadyRunning { pid: Option<u32>, path }`.
   - Other errno → surface as an I/O error.

### Release semantics

- On clean exit, crash, or kill, the kernel releases the flock when the fd
  closes. No explicit unlock is required.
- The lockfile is **not deleted** on drop. Deleting-then-relocking is a classic
  race (another process can open the old inode between unlink and recreate); a
  leftover *unlocked* file is harmless and simply re-locked on next start.

## Wiring — acquire before any shared resource

Add `rustix = { version = "1", features = ["fs"] }` as a direct dependency of
`datamancerd`. `rustix 1.1.4` is already present transitively and exposes a
**safe** `flock`, so this honors `#![forbid(unsafe_code)]` with no meaningful
compile-time cost and no heavyweight new dependency.

New error variant in `crates/datamancerd/src/error.rs`:

```rust
/// Another datamancerd already holds the global single-instance lock.
#[error("another datamancerd is already running{}; lock held at {path}",
        pid.map(|p| format!(" (pid {p})")).unwrap_or_default())]
AlreadyRunning { pid: Option<u32>, path: PathBuf },
```

In `crates/datamancerd/src/main.rs::run()`, acquire **before** `Config::load` and
`Server::bootstrap` (which open the tap-log/cache DBs and create the iceoryx2
node), and hold the guard for the whole process:

```rust
let args = Args::parse();
let config_path = paths::resolve_config_path(args.config)?;
let _instance = single_instance::InstanceLock::acquire()?; // held until run() returns
tracing::info!(path = %config_path.display(), "loading config");
let config = Config::load(&config_path)?;
server::Server::bootstrap(config, config_path).await?.run().await
// `_instance` drops here → kernel releases the lock
```

Register `mod single_instance;` in `main.rs`.

## Simplify `clear_stale_socket` (server.rs:445)

Because the global exclusive lock is acquired before `bind_socket`, no other
daemon can be running by the time we reach the socket. Therefore any socket file
at `admin_socket` is definitively stale.

- **Drop** the connect-based liveness check (the `UnixStream::connect(...).is_ok()`
  branch that returns `AddrInUse`/"already in use"); it is now unreachable.
- **Keep** the non-socket-file guard (refuse to remove a path that exists and is
  not a socket) and the stale-socket removal.

This removes the TOCTOU window in the old check and sources the "is another
daemon running" answer from the lock's real identity rather than from "is
something listening." Update the doc comment accordingly.

## Tests

New `#[cfg(test)]` module in `single_instance.rs`, all via `acquire_at` against a
`tempfile::tempdir()` path so the real home directory is never touched:

1. **First acquire succeeds and records our PID:** the lockfile exists and
   contains `std::process::id()`.
2. **Second acquire fails while the first is held:** `acquire_at` on the same
   path returns `DaemonError::AlreadyRunning`, with `pid` reporting the first
   holder's PID.
3. **Re-acquire after release:** dropping the first `InstanceLock`, a subsequent
   `acquire_at` on the same path succeeds.
4. **Missing parent dir is created:** `acquire_at` on a path under a
   not-yet-existing subdirectory creates the directory and succeeds.

Note on test scope: flock is per-open-file-description and advisory. Two
`acquire_at` calls in the same process each `open(2)` the file, giving distinct
descriptions, so the second correctly fails to acquire — the in-process test is
representative of the cross-process behavior.

## Docs

`crates/datamancerd/README.md` operator section: document the lockfile path (both
platforms), that the guarantee is **global / per-user and config-independent**,
the "already running" failure behavior (non-zero exit, error naming the holding
PID and lock path), and that a leftover lockfile after a crash is harmless.

## Non-goals

- Cross-user or cross-host mutual exclusion (flock is per-host, and the data dir
  is per-user).
- Windows support (datamancerd is already Unix-only: UDS admin socket, SIGTERM).
- Taking over / signaling the existing instance. A second launch reports and
  exits; it does not attempt to replace the running daemon.
