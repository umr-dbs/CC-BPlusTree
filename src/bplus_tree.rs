use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use chronicle_db::tools::safe_cell::SafeCell;
use mvcc_bplustree::index::version_info::{AtomicVersion, Version};
use mvcc_bplustree::index::version_manager::VersionManager;
use mvcc_bplustree::locking::locking_strategy::{DEFAULT_OPTIMISTIC_ATTEMPTS, Level, LockingStrategy};
use crate::node_manager::{NodeManager, NodeSettings};
use crate::node::{NodeRef, Node, LeafLinks};

pub(crate) type Index = BPlusTree;

pub struct BPlusTree {
    pub(crate) root: SafeCell<NodeRef>,
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

impl Into<SafeCell<NodeRef>> for Node {
    fn into(self) -> SafeCell<NodeRef> {
        SafeCell::new(self.into())
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

    pub const fn locking_strategy(&self) -> &LockingStrategy {
        &self.locking_strategy
    }

    pub fn height(&self) -> usize {
        self.height.load(Relaxed)
    }

    pub fn inc_height(&self) {
        self.height.fetch_add(1, Relaxed);
    }

    pub(crate) fn next_version(&self) -> Version {
        self.version_manager.fetch_add(1, Relaxed)
    }
}