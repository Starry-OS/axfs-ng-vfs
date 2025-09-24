use alloc::{borrow::ToOwned, string::String, sync::Arc};
use core::{
    mem,
    ops::{Deref, DerefMut},
};

use hashbrown::HashMap;

use super::DirEntry;
use crate::{
    MetadataUpdate, Mountpoint, Mutex, MutexGuard, NodeOps, NodePermission, NodeType, VfsError,
    VfsResult,
    path::{DOT, DOTDOT, MAX_NAME_LEN, verify_entry_name},
};

/// A trait for a sink that can receive directory entries.
pub trait DirEntrySink {
    /// Accept a directory entry, returns `false` if the sink is full.
    ///
    /// `offset` is the offset of the next entry to be read.
    ///
    /// It's not recommended to operate on the node inside the `accept`
    /// function, since some filesystem may impose a lock while iterating the
    /// directory, and operating on the node may cause deadlock.
    fn accept(&mut self, name: &str, ino: u64, node_type: NodeType, offset: u64) -> bool;
}

impl<F: FnMut(&str, u64, NodeType, u64) -> bool> DirEntrySink for F {
    fn accept(&mut self, name: &str, ino: u64, node_type: NodeType, offset: u64) -> bool {
        self(name, ino, node_type, offset)
    }
}

type DirChildren = HashMap<String, DirEntry>;

pub trait DirNodeOps: NodeOps {
    /// Reads directory entries.
    ///
    /// Returns the number of entries read.
    ///
    /// Implementations should ensure that `.` and `..` are present in the
    /// result.
    fn read_dir(&self, offset: u64, sink: &mut dyn DirEntrySink) -> VfsResult<usize>;

    /// Lookups a directory entry by name.
    fn lookup(&self, name: &str) -> VfsResult<DirEntry>;

    /// Returns whether directory entries can be cached.
    ///
    /// Some filesystems (like '/proc') may not support caching directory
    /// entries, as they may change frequently or not be backed by persistent
    /// storage.
    ///
    /// If this returns `false`, the directory will not be cached in dentry and
    /// each call to [`DirNode::lookup`] will end up calling [`lookup`].
    /// Implementations should take care to handle cases where [`lookup`] is
    /// called multiple times for the same name.
    fn is_cacheable(&self) -> bool {
        true
    }

    /// Creates a directory entry.
    fn create(
        &self,
        name: &str,
        node_type: NodeType,
        permission: NodePermission,
    ) -> VfsResult<DirEntry>;

    /// Creates a link to a node.
    fn link(&self, name: &str, node: &DirEntry) -> VfsResult<DirEntry>;

    /// Unlinks a directory entry by name.
    ///
    /// If the entry is a non-empty directory, it should return `ENOTEMPTY`
    /// error.
    fn unlink(&self, name: &str) -> VfsResult<()>;

    /// Renames a directory entry, replacing the original entry (dst) if it
    /// already exists.
    ///
    /// If src and dst link to the same file, this should do nothing and return
    /// `Ok(())`.
    ///
    /// The caller should ensure:
    /// - If `src` is a directory, `dst` must not exist or be an empty
    ///   directory.
    /// - If `src` is not a directory, `dst` must not exist or not be a
    ///   directory.
    fn rename(&self, src_name: &str, dst_dir: &DirNode, dst_name: &str) -> VfsResult<()>;
}

/// Options for opening (or creating) a directory entry.
///
/// See [`DirNode::open_file`] for more details.
#[derive(Debug, Clone)]
pub struct OpenOptions {
    pub create: bool,
    pub create_new: bool,
    pub node_type: NodeType,
    pub permission: NodePermission,
    pub user: Option<(u32, u32)>, // (uid, gid)
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            create: false,
            create_new: false,
            node_type: NodeType::RegularFile,
            permission: NodePermission::default(),
            user: None,
        }
    }
}

pub struct DirNode {
    ops: Arc<dyn DirNodeOps>,
    cache: Mutex<DirChildren>,
    pub(crate) mountpoint: Mutex<Option<Arc<Mountpoint>>>,
}

impl Deref for DirNode {
    type Target = dyn NodeOps;

    fn deref(&self) -> &Self::Target {
        &*self.ops
    }
}

impl From<DirNode> for Arc<dyn NodeOps> {
    fn from(node: DirNode) -> Self {
        node.ops.clone()
    }
}

impl DirNode {
    pub fn new(ops: Arc<dyn DirNodeOps>) -> Self {
        Self {
            ops,
            cache: Mutex::default(),
            mountpoint: Mutex::default(),
        }
    }

    pub fn inner(&self) -> &Arc<dyn DirNodeOps> {
        &self.ops
    }

    pub fn downcast<T: DirNodeOps>(&self) -> VfsResult<Arc<T>> {
        self.ops
            .clone()
            .into_any()
            .downcast()
            .map_err(|_| VfsError::InvalidInput)
    }

    fn forget_entry(children: &mut DirChildren, name: &str) {
        if let Some(entry) = children.remove(name)
            && let Ok(dir) = entry.as_dir()
        {
            dir.forget();
        }
    }

    fn lookup_locked(&self, name: &str, children: &mut DirChildren) -> VfsResult<DirEntry> {
        use hashbrown::hash_map::Entry;
        match children.entry(name.to_owned()) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => {
                let node = self.ops.lookup(name)?;
                if self.ops.is_cacheable() {
                    e.insert(node.clone());
                }
                Ok(node)
            }
        }
    }

    /// Looks up a directory entry by name.
    pub fn lookup(&self, name: &str) -> VfsResult<DirEntry> {
        if name.len() > MAX_NAME_LEN {
            return Err(VfsError::NameTooLong);
        }
        // Fast path
        if self.ops.is_cacheable() {
            self.lookup_locked(name, &mut self.cache.lock())
        } else {
            self.ops.lookup(name)
        }
    }

    /// Looks up a directory entry by name in cache.
    pub fn lookup_cache(&self, name: &str) -> Option<DirEntry> {
        if self.ops.is_cacheable() {
            self.cache.lock().get(name).cloned()
        } else {
            None
        }
    }

    /// Inserts a directory entry into the cache.
    pub fn insert_cache(&self, name: String, entry: DirEntry) -> Option<DirEntry> {
        if self.ops.is_cacheable() {
            self.cache.lock().insert(name, entry)
        } else {
            None
        }
    }

    pub fn read_dir(&self, offset: u64, sink: &mut dyn DirEntrySink) -> VfsResult<usize> {
        self.ops.read_dir(offset, sink)
    }

    /// Creates a link to a node.
    pub fn link(&self, name: &str, node: &DirEntry) -> VfsResult<DirEntry> {
        verify_entry_name(name)?;

        self.ops.link(name, node).inspect(|entry| {
            self.cache.lock().insert(name.to_owned(), entry.clone());
        })
    }

    /// Unlinks a directory entry by name.
    pub fn unlink(&self, name: &str, is_dir: bool) -> VfsResult<()> {
        verify_entry_name(name)?;

        let mut children = self.cache.lock();
        let entry = self.lookup_locked(name, &mut children)?;
        match (entry.is_dir(), is_dir) {
            (true, false) => return Err(VfsError::IsADirectory),
            (false, true) => return Err(VfsError::NotADirectory),
            _ => {}
        }

        self.ops.unlink(name).inspect(|_| {
            Self::forget_entry(&mut children, name);
        })
    }

    /// Returns whether the directory contains children.
    pub fn has_children(&self) -> VfsResult<bool> {
        let mut has_children = false;
        self.read_dir(0, &mut |name: &str, _, _, _| {
            if name != DOT && name != DOTDOT {
                has_children = true;
                false
            } else {
                true
            }
        })?;
        Ok(has_children)
    }

    fn create_locked(
        &self,
        name: &str,
        node_type: NodeType,
        permission: NodePermission,
        children: &mut DirChildren,
    ) -> VfsResult<DirEntry> {
        let entry = self.ops.create(name, node_type, permission)?;
        children.insert(name.to_owned(), entry.clone());
        Ok(entry)
    }

    /// Creates a directory entry.
    pub fn create(
        &self,
        name: &str,
        node_type: NodeType,
        permission: NodePermission,
    ) -> VfsResult<DirEntry> {
        verify_entry_name(name)?;
        self.create_locked(name, node_type, permission, &mut self.cache.lock())
    }

    fn lock_both_cache<'a>(
        &'a self,
        other: &'a Self,
    ) -> (
        MutexGuard<'a, DirChildren>,
        Option<MutexGuard<'a, DirChildren>>,
    ) {
        let src_children = self.cache.lock();
        let dst_children = if core::ptr::eq(self, other) {
            None
        } else {
            Some(other.cache.lock())
        };
        (src_children, dst_children)
    }

    /// Renames a directory entry.
    pub fn rename(&self, src_name: &str, dst_dir: &Self, dst_name: &str) -> VfsResult<()> {
        verify_entry_name(src_name)?;
        verify_entry_name(dst_name)?;

        let (mut src_children, mut dst_children) = self.lock_both_cache(dst_dir);

        let src = self.lookup_locked(src_name, &mut src_children)?;
        if let Ok(dst) = dst_dir.lookup_locked(
            dst_name,
            dst_children
                .as_mut()
                .map_or_else(|| src_children.deref_mut(), DerefMut::deref_mut),
        ) {
            if src.node_type() == NodeType::Directory {
                if let Ok(dir) = dst.as_dir()
                    && dir.has_children()?
                {
                    return Err(VfsError::DirectoryNotEmpty);
                }
            } else if dst.node_type() == NodeType::Directory {
                return Err(VfsError::IsADirectory);
            }
        }
        drop(src_children);
        drop(dst_children);

        self.ops.rename(src_name, dst_dir, dst_name).inspect(|_| {
            let (mut src_children, mut dst_children) = self.lock_both_cache(dst_dir);
            Self::forget_entry(&mut src_children, src_name);
            Self::forget_entry(
                dst_children
                    .as_mut()
                    .map_or_else(|| src_children.deref_mut(), DerefMut::deref_mut),
                dst_name,
            );
        })
    }

    /// Opens (or creates) a file in the directory.
    pub fn open_file(&self, name: &str, options: &OpenOptions) -> VfsResult<DirEntry> {
        verify_entry_name(name)?;

        let mut children = self.cache.lock();
        match self.lookup_locked(name, &mut children) {
            Ok(val) => {
                if options.create_new {
                    return Err(VfsError::AlreadyExists);
                }
                return Ok(val);
            }
            Err(err) if err == VfsError::NotFound && options.create => {}
            Err(err) => return Err(err),
        }
        let entry =
            self.create_locked(name, options.node_type, options.permission, &mut children)?;
        if options.user.is_some() {
            entry.update_metadata(MetadataUpdate {
                owner: options.user,
                ..Default::default()
            })?;
        }
        Ok(entry)
    }

    pub fn mountpoint(&self) -> Option<Arc<Mountpoint>> {
        self.mountpoint.lock().clone()
    }

    pub fn is_mountpoint(&self) -> bool {
        self.mountpoint.lock().is_some()
    }

    /// Clears the cache of directory entries & user data, allowing them to be
    /// released.
    pub(crate) fn forget(&self) {
        for (_, child) in mem::take(self.cache.lock().deref_mut()) {
            if let Ok(dir) = child.as_dir() {
                dir.forget();
            }
        }
    }
}
