use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use mvcc_bplustree::index::version_info::{AtomicVersion, Version};
use mvcc_bplustree::locking::locking_strategy::{DEFAULT_OPTIMISTIC_ATTEMPTS, Level, LockingStrategy};
use mvcc_bplustree::utils::cc_cell::CCCell;
use crate::index::node::{Node, NodeGuard, NodeRef};
use crate::index::node_manager::{NodeManager, NodeSettings};
use crate::tools::un_cell::UnCell;

pub(crate) type Index = BPlusTree;

pub(crate) type SharedRoot = UnCell<NodeRef>;

pub struct BPlusTree {
    pub(crate) root: SharedRoot,
    pub(crate) locking_strategy: LockingStrategy,
    pub(crate) node_manager: NodeManager,
    pub(crate) version_manager: AtomicVersion,
    height: AtomicUsize,
}

impl Default for Index {
    fn default() -> Self {
        Index::new_multi_versioned()
    }
}

impl Into<SharedRoot> for Node {
    fn into(self) -> SharedRoot {
        UnCell::new(self.into())
    }
}

impl BPlusTree {
    pub(crate) const INIT_TREE_HEIGHT: Level = 1;
    pub(crate) const MAX_TREE_HEIGHT: Level = usize::MAX;
    pub(crate) const START_VERSION: Version = 0;

    fn make(node_manager: NodeManager, locking_strategy: LockingStrategy) -> Self {
        Self {
            root: node_manager.make_empty_root().into(),
            locking_strategy,
            node_manager,
            version_manager: AtomicVersion::new(Self::START_VERSION),
            height: AtomicUsize::new(Self::INIT_TREE_HEIGHT),
        }
    }

    pub fn new_with(locking_strategy: LockingStrategy) -> Self {
        Self::make(NodeManager::SingleVersion(NodeSettings::default()),
                   locking_strategy)
    }

    pub fn new_dolos() -> Self {
        Self::new_with(LockingStrategy::dolos(DEFAULT_OPTIMISTIC_ATTEMPTS))
    }

    pub fn new_single_version_with(node_settings: NodeSettings, locking_strategy: LockingStrategy) -> Self {
        Self::make(
            NodeManager::single_version_with(node_settings),
            locking_strategy)
    }

    pub fn new_multi_version_with(node_settings: NodeSettings, locking_strategy: LockingStrategy) -> Self {
        Self::make(
            NodeManager::multi_version_with(node_settings),
            locking_strategy)
    }

    pub fn new_single_versioned() -> Self {
        Self::make(
            NodeManager::SingleVersion(Default::default()),
            LockingStrategy::SingleWriter)
    }

    pub fn new_multi_versioned() -> Self {
        Self::make(
            NodeManager::MultiVersion(Default::default()),
            LockingStrategy::SingleWriter)
    }

    pub(crate) fn set_root_on_insert(&self, new_root: NodeRef) {
        let _ = self.root.replace(new_root);
        self.inc_height()
    }

    pub(crate) fn set_root_on_delete(&self, new_root: NodeRef) {
        let _ = self.root.replace(new_root);
        self.dec_height()
    }

    pub const fn locking_strategy(&self) -> &LockingStrategy {
        &self.locking_strategy
    }

    pub fn height(&self) -> usize {
        self.height.load(Relaxed)
    }

    fn inc_height(&self) {
        self.height.fetch_add(1, Relaxed);
    }

    fn dec_height(&self) {
        self.height.fetch_sub(1, Relaxed);
    }

    pub(crate) fn next_version(&self) -> Version {
        self.version_manager.fetch_add(1, Relaxed)
    }

    pub(crate) fn lock_reader(&self, node: &CCCell<Node>) -> NodeGuard {
        match self.locking_strategy {
            LockingStrategy::SingleWriter => node.borrow_free_static(),
            LockingStrategy::WriteCoupling => node.borrow_mut_exclusive_static(),
            LockingStrategy::Optimistic(..) => node.borrow_read_static(),
            _ => unreachable!("Sleepy joe hit me -> lock_reader on dolos not allowed")
        }
    }
}