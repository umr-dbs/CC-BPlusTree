use std::hash::Hash;
use std::mem;
use std::sync::atomic::Ordering::Relaxed;
use TXDataModel::page_model::block::{Block, BlockGuard};
use TXDataModel::page_model::{Attempts, BlockRef, Height, Level, ObjectCount};
use TXDataModel::record_model::{AtomicVersion, Version};
use TXDataModel::record_model::record_like::RecordLike;
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
    Payload: Default + Clone + Sync + 'static,
    Entry: RecordLike<Key, Payload> + Sync + 'static
> {
    pub(crate) root: UnCell<Root<FAN_OUT, NUM_RECORDS, Key, Payload, Entry>>,
    pub(crate) locking_strategy: LockingStrategy,
    pub(crate) block_manager: BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload, Entry>,
    pub(crate) version_counter: AtomicVersion,
}

unsafe impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync,
    Entry: RecordLike<Key, Payload> + Sync
> Sync for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload, Entry> {}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync,
    Entry: RecordLike<Key, Payload> + Sync
> Send for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload, Entry> {}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync,
    Entry: RecordLike<Key, Payload> + Sync
> Default for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload, Entry> {
    fn default() -> Self {
        BPlusTree::new_single_versioned()
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync,
    Entry: RecordLike<Key, Payload> + Sync
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload, Entry>
{
    #[inline(always)]
    pub(crate) fn set_new_root(&self, new_root: Block<FAN_OUT, NUM_RECORDS, Key, Payload, Entry>, new_height: Height) {
        let _ = mem::replace(
            self.root.block.unsafe_borrow_mut_static(),
            new_root,
        );

        self.root.get_mut().height = new_height;
    }

    pub fn make(block_manager: BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload, Entry>, locking_strategy: LockingStrategy) -> Self {
        let empty_node
            = block_manager.make_empty_root();

        Self {
            root: UnCell::new(Root::new(
                empty_node.into_cell(locking_strategy.is_olc()),
                INIT_TREE_HEIGHT,
            )),
            version_counter: AtomicVersion::new(START_VERSION),
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

    #[inline(always)]
    pub(crate) fn next_version(&self) -> Version {
        self.version_counter.fetch_add(1, Relaxed)
    }

    #[inline(always)]
    pub(crate) fn lock_reader(&self, node: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload, Entry>)
        -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload, Entry>
    {
        match self.locking_strategy {
            LockingStrategy::MonoWriter => node.borrow_free_static(),
            LockingStrategy::LockCoupling => node.borrow_mut_exclusive_static(),
            _ => node.borrow_read_static(),
        }
    }

    #[inline]
    pub(crate) fn apply_for(&self,
                            curr_level: Level,
                            max_level: Level,
                            attempt: Attempts,
                            height: Level,
                            block_cc: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload, Entry>
    ) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload, Entry>
    {
        match self.locking_strategy() {
            LockingStrategy::MonoWriter =>
                block_cc.borrow_free_static(),
            LockingStrategy::LockCoupling =>
                block_cc.borrow_mut_exclusive_static(),
            LockingStrategy::RWLockCoupling(lock_level, attempts)
            if curr_level >= height || curr_level >= max_level || attempt >= *attempts || lock_level.is_lock(curr_level, height) =>
                block_cc.borrow_mut_static(),
            LockingStrategy::RWLockCoupling(..) =>
                block_cc.borrow_read_static(),
            LockingStrategy::OLC(LevelConstraints::Unlimited) =>
                block_cc.borrow_free_static(),
            LockingStrategy::OLC(LevelConstraints::OptimisticLimit { attempts, level })
            if curr_level >= height || curr_level >= max_level || attempt >= *attempts || level.is_lock(curr_level, height) =>
                block_cc.borrow_mut_static(),
            LockingStrategy::OLC(..) =>
                block_cc.borrow_free_static(),
        }
    }
}