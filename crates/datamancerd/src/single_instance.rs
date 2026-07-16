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
use std::path::Path;

#[cfg(unix)]
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

        match try_lock_exclusive(&file) {
            LockTry::Acquired => {}
            LockTry::Contended => {
                let pid = read_pid(&mut file);
                return Err(DaemonError::AlreadyRunning {
                    pid,
                    path: path.to_path_buf(),
                });
            }
            LockTry::Failed(e) => return Err(e.into()),
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

/// Outcome of a non-blocking exclusive-lock attempt on the lockfile.
enum LockTry {
    Acquired,
    Contended,
    Failed(std::io::Error),
}

/// Take the exclusive lock without blocking. Unix: advisory `flock`. Windows:
/// std file locking (stable since Rust 1.89), which maps to `LockFileEx`. Both
/// are released by the kernel on process exit, and both scope per-user via the
/// per-user data-dir lockfile path.
#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> LockTry {
    match flock(file, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => LockTry::Acquired,
        Err(e) if e == rustix::io::Errno::WOULDBLOCK || e == rustix::io::Errno::AGAIN => {
            LockTry::Contended
        }
        Err(e) => LockTry::Failed(std::io::Error::from(e)),
    }
}

#[cfg(windows)]
fn try_lock_exclusive(file: &File) -> LockTry {
    match file.try_lock() {
        Ok(()) => LockTry::Acquired,
        Err(std::fs::TryLockError::WouldBlock) => LockTry::Contended,
        Err(std::fs::TryLockError::Error(e)) => LockTry::Failed(e),
    }
}

/// Best-effort read of the PID text a lock holder wrote. `None` if the file is
/// empty or unparseable — there is a brief window between another process
/// acquiring the lock and writing its PID. On Windows the holder's mandatory
/// lock additionally blocks this read, so a contender always sees `None` there
/// (the single-instance guarantee is unaffected; only the diagnostic PID is).
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
        // Read the PID back only after releasing: Windows locks are mandatory,
        // so another handle cannot read the locked range while it is held.
        drop(lock);
        let contents = std::fs::read_to_string(&path).expect("read lockfile");
        assert_eq!(
            contents.trim(),
            std::process::id().to_string(),
            "lockfile records our PID"
        );
    }

    #[test]
    fn second_acquire_fails_while_held() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("datamancerd.lock");
        let held = InstanceLock::acquire_at(&path).expect("first acquire");
        match InstanceLock::acquire_at(&path) {
            Err(DaemonError::AlreadyRunning {
                pid,
                path: reported,
            }) => {
                // Unix advisory locks let the contender read the holder's PID;
                // Windows mandatory locks block that read, so the PID is absent
                // there. The single-instance guarantee holds either way.
                #[cfg(unix)]
                assert_eq!(pid, Some(std::process::id()), "reports the holder PID");
                #[cfg(windows)]
                assert_eq!(pid, None, "mandatory lock blocks the contender's PID read");
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
