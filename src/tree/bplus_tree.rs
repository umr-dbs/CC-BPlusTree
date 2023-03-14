use std::hash::Hash;
use std::mem;
use crate::block::block_manager::BlockManager;
use crate::tree::root::Root;
use crate::locking::locking_strategy::{OLCVariant, LockingStrategy};
use crate::page_model::{Attempts, BlockRef, Height, Level, ObjectCount};
use crate::block::block::{Block, BlockGuard};
use crate::test::{dec_key, inc_key};
use crate::utils::un_cell::UnCell;

pub type LockLevel = ObjectCount;
pub const INIT_TREE_HEIGHT: Height = 1;
pub const MAX_TREE_HEIGHT: Height = Height::MAX;

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
    pub(crate) min_key: Key,
    pub(crate) max_key: Key,
    pub(crate) inc_key: fn(Key) -> Key,
    pub(crate) dec_key: fn(Key) -> Key,
}


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
    Payload: Default + Clone + Sync,
> Default for BPlusTree<FAN_OUT, NUM_RECORDS, u64, Payload> {
    fn default() -> Self {
        BPlusTree::new(
            u64::MIN,
            u64::MAX,
            inc_key,
            dec_key)
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
        self.root.get_mut().height = new_height;

        mem::drop(mem::replace(
            self.root.block.unsafe_borrow_mut(),
            new_root,
        ));
    }

    fn make(block_manager: BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>,
                locking_strategy: LockingStrategy,
                min_key: Key,
                max_key: Key,
                inc_key: fn(Key) -> Key,
                dec_key: fn(Key) -> Key) -> Self
    {
        let empty_node
            = block_manager.make_empty_root();

        Self {
            root: UnCell::new(Root::new(
                empty_node.into_cell(locking_strategy.is_olc()),
                INIT_TREE_HEIGHT,
            )),
            locking_strategy,
            block_manager,
            min_key,
            max_key,
            inc_key,
            dec_key
        }
    }

    pub fn new_with(locking_strategy: LockingStrategy,
                    min_key: Key,
                    max_key: Key,
                    inc_key: fn(Key) -> Key,
                    dec_key: fn(Key) -> Key) -> Self {
        let mut block_manager
            = BlockManager::default();

        block_manager.is_multi_version = false;

        Self::make(block_manager, locking_strategy, min_key, max_key, inc_key, dec_key)
    }

    #[inline(always)]
    pub fn new(min_key: Key, max_key: Key, inc_key: fn(Key) -> Key, dec_key: fn(Key) -> Key) -> Self {
        Self::new_with(LockingStrategy::default(), min_key, max_key, inc_key, dec_key)
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
    pub(crate) fn lock_reader(&self, node: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
        -> BlockGuard<'static, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        match self.locking_strategy {
            LockingStrategy::MonoWriter => node.borrow_free(),
            LockingStrategy::LockCoupling => node.borrow_mut_exclusive(),
            _ => node.borrow_read(),
        }
    }

    #[inline(always)]
    pub(crate) fn lock_reader_olc(&self,
                                  node: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
                                  curr_level: Level,
                                  attempt: Attempts,
                                  height: Height,)
                                  -> BlockGuard<'static, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        match self.locking_strategy() {
            LockingStrategy::MonoWriter => node.borrow_free(),
            LockingStrategy::LockCoupling => node.borrow_mut_exclusive(),
            LockingStrategy::OLC(OLCVariant::Pinned { attempts, level })
            if attempt >= *attempts || level.is_lock(curr_level, height) =>
                node.borrow_pin(),
            LockingStrategy::OLC(OLCVariant::Bounded { attempts, level })
            if attempt >= *attempts  || level.is_lock(curr_level, height) =>
                node.borrow_pin(),
            _ => node.borrow_read(),
        }
    }

    #[inline]
    pub(crate) fn apply_for_ref(&self,
                            curr_level: Level,
                            max_level: Level,
                            attempt: Attempts,
                            height: Level,
                            block_cc: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>
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
            LockingStrategy::OLC(OLCVariant::Free) =>
                block_cc.borrow_free(),
            LockingStrategy::OLC(OLCVariant::Bounded { attempts, level })
            if curr_level >= height || curr_level >= max_level || attempt >= *attempts || level.is_lock(curr_level, height) =>
                block_cc.borrow_mut(),
            LockingStrategy::OLC(OLCVariant::Pinned { attempts, level })
            if curr_level >= height || curr_level >= max_level || attempt >= *attempts || level.is_lock(curr_level, height) =>
                block_cc.borrow_pin(),
            LockingStrategy::OLC(..) =>
                block_cc.borrow_free(),
        }
    }
}