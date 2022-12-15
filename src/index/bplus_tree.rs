use std::hash::Hash;
use std::{mem, ptr};
use TXDataModel::page_model::block::{Block, BlockGuard};
use TXDataModel::page_model::{Attempts, BlockRef, Height, Level, ObjectCount};
use TXDataModel::record_model::{AtomicVersion, Version};
use TXDataModel::utils::un_cell::UnCell;
use crate::block::block_manager::BlockManager;
use crate::index::root::Root;
use crate::locking::locking_strategy::{LevelConstraints, LockingStrategy};
// use serde::{Serialize, Deserialize};

pub type LockLevel = ObjectCount;
pub const INIT_TREE_HEIGHT: Height = 1;
pub const MAX_TREE_HEIGHT: Height = Height::MAX;
pub const START_VERSION: Version = 0;

// #[derive(Serialize, Deserialize)]
pub struct BPlusTree<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Default + Clone + Sync + 'static
> {
    pub(crate) root: UnCell<Root<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    pub(crate) locking_strategy: LockingStrategy,
    pub(crate) block_manager: BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>,
    // pub(crate) version_counter: AtomicVersion,
}

// impl<const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Ord + Copy + Hash + Sync + 'static,
//     Payload: Default + Clone + Sync + 'static
// > Drop for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {
//     fn drop(&mut self) {
//
//     }
// }

unsafe impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync
> Sync for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync
> Send for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync,
> Default for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        BPlusTree::new_single_versioned()
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub(crate) fn set_new_root(&self, new_root: Block<FAN_OUT, NUM_RECORDS, Key, Payload>, new_height: Height) {
        unsafe {
            ptr::write(self.root.block.unsafe_borrow_mut(), new_root);
        }
        // let _ = mem::replace(
        //     self.root.block.unsafe_borrow_mut(),
        //     new_root,
        // );

        self.root.get_mut().height = new_height;
    }

    pub fn make(block_manager: BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>, locking_strategy: LockingStrategy) -> Self {
        let empty_node
            = block_manager.make_empty_root();

        Self {
            root: UnCell::new(Root::new(
                empty_node.into_cell(locking_strategy.is_olc()),
                INIT_TREE_HEIGHT,
            )),
            // version_counter: AtomicVersion::new(START_VERSION),
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
        Self::make(BlockManager::new(true), locking_strategy)
    }

    #[inline(always)]
    pub fn new_single_versioned() -> Self {
        Self::new_single_version_for(LockingStrategy::default())
    }

    #[inline(always)]
    pub fn new_multi_versioned() -> Self {
        Self::new_multi_version_for(LockingStrategy::default())
    }

    #[inline(always)]
    pub const fn locking_strategy(&self) -> &LockingStrategy {
        &self.locking_strategy
    }

    #[inline(always)]
    pub fn height(&self) -> Height {
        self.root.height()
    }

    // #[inline(always)]
    // pub(crate) fn next_version(&self) -> Version {
    //     self.version_counter.fetch_add(1, Relaxed)
    // }

    #[inline(always)]
    pub(crate) fn lock_reader(&self, node: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
        -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        match self.locking_strategy {
            LockingStrategy::MonoWriter => node.borrow_free(),
            LockingStrategy::LockCoupling => node.borrow_mut_exclusive(),
            _ => node.borrow_read(),
        }
    }

    #[inline]
    pub(crate) fn apply_for(&self,
                            curr_level: Level,
                            max_level: Level,
                            attempt: Attempts,
                            height: Level,
                            block_cc: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>
    ) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        match self.locking_strategy() {
            LockingStrategy::MonoWriter =>
                block_cc.borrow_free(),
            LockingStrategy::LockCoupling =>
                block_cc.borrow_mut_exclusive(),
            LockingStrategy::RWLockCoupling(lock_level, attempts)
            if curr_level >= height || curr_level >= max_level || attempt >= *attempts || lock_level.is_lock(curr_level, height) =>
                block_cc.borrow_mut(),
            LockingStrategy::RWLockCoupling(..) =>
                block_cc.borrow_read(),
            LockingStrategy::OLC(LevelConstraints::Unlimited) =>
                block_cc.borrow_free(),
            LockingStrategy::OLC(LevelConstraints::OptimisticLimit { attempts, level })
            if curr_level >= height || curr_level >= max_level || attempt >= *attempts || level.is_lock(curr_level, height) =>
                block_cc.borrow_mut(),
            LockingStrategy::OLC(..) =>
                block_cc.borrow_free(),
        }
    }
}