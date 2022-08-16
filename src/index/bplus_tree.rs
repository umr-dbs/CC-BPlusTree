use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, SeqCst};
use mvcc_bplustree::index::version_info::{AtomicVersion, Version};
use mvcc_bplustree::locking::locking_strategy::{Attempts, DEFAULT_OPTIMISTIC_ATTEMPTS, Level, LockingStrategy};
use mvcc_bplustree::utils::cc_cell::CCCell;
use crate::index::node::{Node, NodeGuard, NodeRef};
use crate::index::node_manager::{NodeManager, NodeSettings};
use crate::utils::un_cell::UnCell;
use crate::utils::vcc_cell::{VCCCell, VCCCellGuard};
// use serde::{Serialize, Deserialize};

pub(crate) type Index = BPlusTree;

pub(crate) type SharedRoot = NodeRef;

// #[derive(Serialize, Deserialize)]
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

    pub fn new_single_version_for(locking_strategy: LockingStrategy) -> Self {
        Self::make(NodeManager::SingleVersion(NodeSettings::default()),
                   locking_strategy)
    }

    pub fn new_multi_version_for(locking_strategy: LockingStrategy) -> Self {
        Self::make(NodeManager::MultiVersion(NodeSettings::default()),
                   locking_strategy)
    }

    pub fn new_dolos() -> Self {
        Self::new_single_version_for(LockingStrategy::dolos(DEFAULT_OPTIMISTIC_ATTEMPTS))
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

    // pub(crate) fn set_root_on_insert(&self, new_root: NodeRef) {
    //     let _ = self.root.replace(new_root);
    //     self.inc_height()
    // }
    //
    // pub(crate) fn set_root_on_delete(&self, new_root: NodeRef) {
    //     let _ = self.root.replace(new_root);
    //     self.dec_height()
    // }

    pub const fn locking_strategy(&self) -> &LockingStrategy {
        &self.locking_strategy
    }

    pub fn height(&self) -> Level {
        self.height.load(SeqCst)
    }

    pub(crate)fn inc_height(&self) {
        self.height.fetch_add(1, Relaxed);
    }

    fn dec_height(&self) {
        self.height.fetch_sub(1, Relaxed);
    }

    pub(crate) fn next_version(&self) -> Version {
        self.version_manager.fetch_add(1, Relaxed)
    }

    pub(crate) fn lock_reader(&self, node: &VCCCell<Node>) -> NodeGuard {
        match self.locking_strategy {
            LockingStrategy::SingleWriter => node.borrow_free_static(),
            LockingStrategy::WriteCoupling => node.borrow_read_static(),
            LockingStrategy::Optimistic(..) => node.borrow_read_static(),
            LockingStrategy::Dolos(..) => node.borrow_versioned_static(),
        }
    }
    #[inline]
    pub(crate) fn apply_for<E: Default>(&self, curr_level: Level, max_level: Level, attempt: Attempts, height: Level, block_cc: &VCCCell<E>) -> VCCCellGuard<'static, E> {
        match self.locking_strategy() {
            LockingStrategy::SingleWriter =>
                block_cc.borrow_free_static(),
            LockingStrategy::WriteCoupling =>
                block_cc.borrow_mut_static(),
            LockingStrategy::Optimistic(lock_level, attempts)
            if curr_level >= height || curr_level >= max_level || attempt >= *attempts || lock_level.is_lock(curr_level, height) =>
                block_cc.borrow_mut_static(),
            LockingStrategy::Dolos(lock_level, attempts)
            if curr_level >= height || curr_level >= max_level || attempt >= *attempts || lock_level.is_lock(curr_level, height) =>
                block_cc.borrow_mut_static(),
            LockingStrategy::Optimistic(..) =>
                block_cc.borrow_read_static(),
            LockingStrategy::Dolos(..) =>
                block_cc.borrow_versioned_static(),
        }
    }
}