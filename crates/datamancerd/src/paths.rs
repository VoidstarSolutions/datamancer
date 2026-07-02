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
#[allow(dead_code)]
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
}
