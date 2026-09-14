//! Small helpers shared by several modules. Each one exists because the same
//! few lines had been written out in more than one place, slightly
//! differently each time.

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

/// Locks a `std` mutex, carrying on through poisoning.
///
/// A panic elsewhere while the lock was held leaves the data inside perfectly
/// usable for everything this gateway keeps behind a `std` mutex (maps of
/// hashes, counters, sets). Refusing to serve video over that would be a worse
/// outcome than the inconsistency poisoning guards against.
pub fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Formats a byte count for a person: `512 B`, `1.5 GB`.
///
/// Whole bytes never carry decimals ("0.0 B" says nothing "0 B" does not);
/// every larger unit gets `decimals` places. Binary units (1 KB = 1024 B),
/// matching what librqbit and every file manager on Linux report.
pub fn human_bytes(bytes: u64, decimals: usize) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.decimals$} {}", UNITS[unit])
}

/// Replaces `path` with `bytes` so that a reader only ever sees the old file
/// or the new one, never a half-written mix.
///
/// Writes a sibling temporary file and renames it over the target, which is a
/// single atomic step on the same filesystem. A plain write truncates first,
/// so a crash, a full disk or a concurrent reader in the middle of it leaves
/// an empty or partial file behind -- and for the files this is used for (the
/// advertised-hash list, the cached certificate) a partial file silently
/// resets state rather than failing loudly.
///
/// `private` creates the file owner-only from the first byte, so there is no
/// window in which a key is briefly world-readable.
///
/// Every call writes its own uniquely named temporary, so two concurrent
/// writes of one path cannot interleave inside a shared file: each rename is
/// whole, and the later one wins. Callers that need "the newest state wins"
/// rather than "the later rename wins" still have to order their writes.
pub async fn write_atomic(
    path: &Path,
    bytes: impl AsRef<[u8]> + Send + 'static,
    private: bool,
) -> std::io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || write_atomic_blocking(&path, bytes.as_ref(), private))
        .await
        .map_err(std::io::Error::other)?
}

fn write_atomic_blocking(path: &Path, bytes: &[u8], private: bool) -> std::io::Result<()> {
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty());
    if let Some(parent) = parent {
        std::fs::create_dir_all(parent)?;
    }
    let file_name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("atomic write target has no file name"))?;
    static NEXT_TMP: AtomicU64 = AtomicU64::new(0);
    let mut tmp_name = std::ffi::OsString::from(".");
    tmp_name.push(file_name);
    tmp_name.push(format!(
        ".{}.{}.tmp",
        std::process::id(),
        NEXT_TMP.fetch_add(1, Ordering::Relaxed)
    ));
    let tmp = path.with_file_name(tmp_name);

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;

    let result = options
        .open(&tmp)
        .and_then(|mut file| file.write_all(bytes))
        .and_then(|()| std::fs::rename(&tmp, path));
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_bytes_carry_no_decimals_and_larger_units_do() {
        assert_eq!(human_bytes(0, 1), "0 B");
        assert_eq!(human_bytes(512, 2), "512 B");
        assert_eq!(human_bytes(1024, 1), "1.0 KB");
        assert_eq!(human_bytes(5 * 1024 * 1024, 1), "5.0 MB");
        assert_eq!(human_bytes(2 * 1024 * 1024 * 1024, 2), "2.00 GB");
    }

    #[test]
    fn a_poisoned_lock_still_hands_back_its_data() {
        let mutex = std::sync::Arc::new(Mutex::new(7));
        let poisoner = std::sync::Arc::clone(&mutex);
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.lock().unwrap();
            panic!("poison the lock");
        })
        .join();
        assert!(mutex.is_poisoned());
        assert_eq!(*lock(&mutex), 7);
    }

    #[tokio::test]
    async fn an_atomic_write_replaces_the_file_and_leaves_no_temporary_behind() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/state.json");

        write_atomic(&path, b"first".to_vec(), false).await.unwrap();
        write_atomic(&path, b"second".to_vec(), false)
            .await
            .unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"second");
        let names: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from("state.json")]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_private_write_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.key");
        write_atomic(&path, b"key".to_vec(), true).await.unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
