use std::sync::atomic::Ordering::Relaxed;
use mvcc_bplustree::index::version_info::{AtomicVersion, Version};
use mvcc_bplustree::locking::locking_strategy::{Attempts, Level, LockingStrategy};
use crate::index::block::Block;
use crate::index::block_manager::BlockManager;
use crate::index::node::{Node, BlockGuard, BlockRef};
use crate::index::root::Root;
use crate::utils::vcc_cell::ConcurrentCell::{ConcurrencyControlCell, OptimisticCell};
// use serde::{Serialize, Deserialize};

pub(crate) type Index = BPlusTree;

// #[derive(Serialize, Deserialize)]
pub struct BPlusTree {
    pub(crate) root: Root,
    pub(crate) locking_strategy: LockingStrategy,
    pub(crate) block_manager: BlockManager,
    pub(crate) version_counter: AtomicVersion,
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

    pub(crate) fn set_new_root(&self, new_root: Block, new_height: Level, old_root_ptr: &mut Node) -> Option<BlockGuard> {
        match self.locking_strategy.is_dolos() {
            true => {
                let new_root
                    = new_root.into_cell_dolos();

                let new_root_guard = self.apply_for(
                    Self::INIT_TREE_HEIGHT,
                    Level::MIN,
                    Attempts::MAX,
                    Level::MIN,
                    new_root.clone());

                debug_assert!(new_root_guard.is_write_lock());

                let _ =
                    self.root.replace(new_root, new_height);

                Some(new_root_guard)
            },
            false => {
                *old_root_ptr = new_root.node_data;
                None
            }
        }
    }

    fn make(block_manager: BlockManager, locking_strategy: LockingStrategy) -> Self {
        let empty_node
            = block_manager.make_empty_root();

        Self {
            root: (empty_node, Self::INIT_TREE_HEIGHT, &locking_strategy).into(),
            version_counter: AtomicVersion::new(Self::START_VERSION),
            locking_strategy,
            block_manager,
        }
    }

    pub fn new_single_version_for(locking_strategy: LockingStrategy) -> Self {
        let mut block_manager
            = BlockManager::default();

        block_manager.is_multi_version = false;

        Self::make(block_manager, locking_strategy)
    }

    pub fn new_multi_version_for(locking_strategy: LockingStrategy) -> Self {
        let mut block_manager
            = BlockManager::default();

        block_manager.is_multi_version = true;

        Self::make(block_manager, locking_strategy)
    }

    pub fn new_single_versioned() -> Self {
        Self::new_single_version_for(LockingStrategy::SingleWriter)
    }

    pub fn new_multi_versioned() -> Self {
        Self::new_multi_version_for(LockingStrategy::SingleWriter)
    }

    pub const fn locking_strategy(&self) -> &LockingStrategy {
        &self.locking_strategy
    }

    pub fn height(&self) -> Level {
        self.root.height()
    }

    pub(crate) fn next_version(&self) -> Version {
        self.version_counter.fetch_add(1, Relaxed)
    }

    pub(crate) fn lock_reader(&self, node: &BlockRef) -> BlockGuard {
        match self.locking_strategy {
            LockingStrategy::SingleWriter => node.borrow_free_static(),
            LockingStrategy::WriteCoupling => node.borrow_mut_exclusive_static(),
            _ => node.borrow_read_static(),
        }
    }

    #[inline]
    pub(crate) fn apply_for(&self, curr_level: Level, max_level: Level, attempt: Attempts, height: Level, block_cc: BlockRef) -> BlockGuard {
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
                block_cc.borrow_free_static(),
        }
    }
}