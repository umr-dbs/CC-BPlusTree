use std::mem;
use std::sync::atomic::Ordering::Relaxed;
use mvcc_bplustree::index::version_info::{AtomicVersion, Version};
use mvcc_bplustree::locking::locking_strategy::{Attempts, Level, LockingStrategy};
use crate::block::block::Block;
use crate::block::block_manager::BlockManager;
use crate::index::node::{Node, BlockGuard, BlockRef};
use crate::index::root::Root;
use crate::utils::un_cell::UnCell;
use crate::utils::vcc_cell::ConcurrentCell::{ConcurrencyControlCell, OptimisticCell};
// use serde::{Serialize, Deserialize};

pub(crate) type Height = Level;
pub(crate) type LockLevel = Level;
pub(crate) type Index = BPlusTree;

// #[derive(Serialize, Deserialize)]
pub struct BPlusTree {
    pub(crate) root: UnCell<Root>,
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

    pub(crate) fn set_new_root<'a>(&self, current_root_guard: &mut BlockGuard<'a>, new_root: Block, new_height: Level){
        let _ = mem::replace(
            self.root.block.unsafe_borrow_mut_static(),
            new_root
        );

        self.root.get_mut().height = new_height;
    }

    fn make(block_manager: BlockManager, locking_strategy: LockingStrategy) -> Self {
        let empty_node
            = block_manager.make_empty_root();

        Self {
            root: UnCell::new(Root::new(
                empty_node.into_cell(locking_strategy.is_dolos()),
                Self::INIT_TREE_HEIGHT
            )),
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

    pub fn height(&self) -> Height {
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