use std::fmt::{Display, Formatter};
use std::hash::Hash;
use TXDataModel::page_model::{BlockID, BlockRef, Height};
use TXDataModel::record_model::record_like::RecordLike;

pub const LEVEL_ROOT: Height = 1;

#[derive(Clone, Default)]
pub(crate) struct Root<const KEY_SIZE: usize,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
    Entry: RecordLike<Key, Payload>>
{
    pub(crate) block: BlockRef<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry>,
    pub(crate) height: Height
}

unsafe impl<const KEY_SIZE: usize,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
    Entry: RecordLike<Key, Payload>
> Send for Root<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry> { }

impl<const KEY_SIZE: usize,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
    Entry: RecordLike<Key, Payload>
> Display for Root<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Root(height={})", self.height())
    }
}

unsafe impl<const KEY_SIZE: usize,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
    Entry: RecordLike<Key, Payload>
> Sync for Root<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry> { }

impl<const KEY_SIZE: usize,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
    Entry: RecordLike<Key, Payload>
> Into<Root<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry>> for (BlockRef<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry>, Height) {
    #[inline(always)]
    fn into(self) -> Root<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry> {
        Root::new(self.0, self.1)
    }
}

impl<const KEY_SIZE: usize,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
    Entry: RecordLike<Key, Payload>
> Root<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry> {
    #[inline(always)]
    pub(crate) fn new(block: BlockRef<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry>, height: Height) -> Self {
        Self {
            block,
            height
        }
    }

    #[inline(always)]
    pub(crate) fn block(&self) -> BlockRef<KEY_SIZE, FAN_OUT, NUM_RECORDS, Key, Payload, Entry> {
        self.block.clone()
    }

    #[inline(always)]
    pub(crate) const fn height(&self) -> Height {
        self.height
    }

    #[inline(always)]
    pub(crate) fn block_id(&self) -> BlockID {
        self.block.unsafe_borrow().block_id()
    }
}