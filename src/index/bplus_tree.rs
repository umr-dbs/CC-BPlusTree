use std::mem;
use std::sync::atomic::Ordering::Relaxed;
use mvcc_bplustree::index::version_info::{AtomicVersion, Version};
use mvcc_bplustree::locking::locking_strategy::{Attempts, Level, LockingStrategy};
use crate::block::block::{Block, BlockRef};
use crate::block::block_lock::BlockGuard;
use crate::block::block_manager::BlockManager;
use crate::index::cclocking_strategy::{CCLockingStrategy, LevelConstraints};
use crate::index::cclocking_strategy::LevelConstraints::OptimisticLimit;
use crate::index::root::Root;
use crate::utils::un_cell::UnCell;
// use serde::{Serialize, Deserialize};

pub(crate) type Height = Level;
pub(crate) type LockLevel = Level;
pub(crate) type Index = BPlusTree;

// #[derive(Serialize, Deserialize)]
pub struct BPlusTree {
    pub(crate) root: UnCell<Root>,
    pub(crate) locking_strategy: CCLockingStrategy,
    pub(crate) block_manager: BlockManager,
    pub(crate) version_counter: AtomicVersion,
}

impl Default for Index {
    fn default() -> Self {
        Index::new_multi_versioned()
    }
}

impl BPlusTree {
    pub(crate) const INIT_TREE_HEIGHT: Height = 1;
    pub(crate) const MAX_TREE_HEIGHT: Height = Height::MAX;
    pub(crate) const START_VERSION: Version = 0;

    pub(crate) fn set_new_root(&self, new_root: Block, new_height: Level) {
        let _ = mem::replace(
            self.root.block.unsafe_borrow_mut_static(),
            new_root,
        );

        self.root.get_mut().height = new_height;
    }

    pub fn make(block_manager: BlockManager, locking_strategy: CCLockingStrategy) -> Self {
        let empty_node
            = block_manager.make_empty_root();

        Self {
            root: UnCell::new(Root::new(
                empty_node.into_cell(locking_strategy.is_olc()),
                Self::INIT_TREE_HEIGHT,
            )),
            version_counter: AtomicVersion::new(Self::START_VERSION),
            locking_strategy,
            block_manager,
        }
    }

    pub fn new_single_version_for(locking_strategy: CCLockingStrategy) -> Self {
        let mut block_manager
            = BlockManager::default();

        block_manager.is_multi_version = false;

        Self::make(block_manager, locking_strategy)
    }

    pub fn new_multi_version_for(locking_strategy: CCLockingStrategy) -> Self {
        let mut block_manager
            = BlockManager::default();

        block_manager.is_multi_version = true;

        Self::make(block_manager, locking_strategy)
    }

    pub fn new_single_versioned() -> Self {
        Self::new_single_version_for(CCLockingStrategy::default())
    }

    pub fn new_multi_versioned() -> Self {
        Self::new_multi_version_for(CCLockingStrategy::default())
    }

    pub const fn locking_strategy(&self) -> &CCLockingStrategy {
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
            CCLockingStrategy::MonoWriter => node.borrow_free_static(),
            CCLockingStrategy::LockCoupling => node.borrow_mut_exclusive_static(),
            _ => node.borrow_read_static(),
        }
    }

    #[inline]
    pub(crate) fn apply_for(&self, curr_level: Level, max_level: Level, attempt: Attempts, height: Level, block_cc: BlockRef) -> BlockGuard {
        match self.locking_strategy() {
            CCLockingStrategy::MonoWriter =>
                block_cc.borrow_free_static(),
            CCLockingStrategy::LockCoupling =>
                block_cc.borrow_mut_exclusive_static(),
            CCLockingStrategy::RWLockCoupling(lock_level, attempts)
            if curr_level >= height || curr_level >= max_level || attempt >= *attempts || lock_level.is_lock(curr_level, height) =>
                block_cc.borrow_mut_static(),
            CCLockingStrategy::RWLockCoupling(..) =>
                block_cc.borrow_read_static(),
            CCLockingStrategy::OLC(LevelConstraints::None) =>
                block_cc.borrow_free_static(),
            CCLockingStrategy::OLC(OptimisticLimit { attempts, level })
            if curr_level >= height || curr_level >= max_level || attempt >= *attempts || level.is_lock(curr_level, height) =>
                block_cc.borrow_mut_static(),
            CCLockingStrategy::OLC(..) =>
                block_cc.borrow_free_static(),
        }
    }
}