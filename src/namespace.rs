use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, OwnedRwLockReadGuard, OwnedRwLockWriteGuard};

/// a held lock on a single path component - either read or write
pub enum PathGuard {
    Read(OwnedRwLockReadGuard<()>),
    Write(OwnedRwLockWriteGuard<()>),
}

/// per-path read/write locks for the GFS namespace.
/// allows concurrent operations on different files while serializing
/// operations that conflict (e.g. create vs delete on same directory).
pub struct NamespaceLock {
    locks: Mutex<HashMap<String, Arc<RwLock<()>>>>,
}

impl NamespaceLock {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
        }
    }

    /// get or create a lock for a given path
    async fn get_lock(&self, path: &str) -> Arc<RwLock<()>> {
        let mut locks = self.locks.lock().await;
        locks
            .entry(path.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    /// lock a path for a read operation:
    ///   read locks on all ancestors, read lock on the leaf
    pub async fn lock_read(&self, path: &str) -> Vec<PathGuard> {
        let components = path_components(path);
        let mut guards = Vec::with_capacity(components.len());

        for component in &components {
            let lock = self.get_lock(component).await;
            guards.push(PathGuard::Read(lock.read_owned().await));
        }

        guards
    }

    /// lock a path for a mutation:
    ///   read locks on all ancestors, write lock on the leaf
    pub async fn lock_mutate(&self, path: &str) -> Vec<PathGuard> {
        let components = path_components(path);
        if components.is_empty() {
            return Vec::new();
        }

        let mut guards = Vec::with_capacity(components.len());

        // read lock all ancestors
        for component in &components[..components.len() - 1] {
            let lock = self.get_lock(component).await;
            guards.push(PathGuard::Read(lock.read_owned().await));
        }

        // write lock the leaf
        let leaf = components.last().unwrap();
        let lock = self.get_lock(leaf).await;
        guards.push(PathGuard::Write(lock.write_owned().await));

        guards
    }

    /// lock a directory path with a write lock (for snapshot, delete dir).
    /// read locks on ancestors, write lock on the directory itself.
    pub async fn lock_dir_mutate(&self, path: &str) -> Vec<PathGuard> {
        // same as lock_mutate - write lock on the path itself
        self.lock_mutate(path).await
    }
}

/// split "/d1/d2/leaf" into ["/d1", "/d1/d2", "/d1/d2/leaf"]
fn path_components(path: &str) -> Vec<String> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return vec!["/".to_string()];
    }

    let parts: Vec<&str> = trimmed.split('/').collect();
    let mut components = Vec::with_capacity(parts.len());
    let mut current = String::new();

    for part in parts {
        current.push('/');
        current.push_str(part);
        components.push(current.clone());
    }

    components
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_components() {
        assert_eq!(path_components("/a/b/c"), vec!["/a", "/a/b", "/a/b/c"]);
        assert_eq!(path_components("/hello.txt"), vec!["/hello.txt"]);
        assert_eq!(path_components("/d1/file.txt"), vec!["/d1", "/d1/file.txt"]);
        assert_eq!(path_components("/"), vec!["/"]);
    }

    #[tokio::test]
    async fn test_concurrent_creates_different_files() {
        let ns = NamespaceLock::new();

        // both acquire read lock on parent, write lock on different files
        // should not block each other
        let _g1 = ns.lock_mutate("/home/a.txt").await;
        let _g2 = ns.lock_mutate("/home/b.txt").await;
        // if we got here, no deadlock
    }

    #[tokio::test]
    async fn test_read_doesnt_block_read() {
        let ns = NamespaceLock::new();
        let _g1 = ns.lock_read("/home/a.txt").await;
        let _g2 = ns.lock_read("/home/a.txt").await;
    }
}
