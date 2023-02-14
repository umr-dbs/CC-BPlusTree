use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::Ordering;
use crate::page_model::{AtomicBlockID, BlockID, BlockRef, ObjectCount};
use crate::page_model::block::Block;
use crate::page_model::internal_page::InternalPage;
use crate::page_model::leaf_page::LeafPage;
use crate::page_model::node::Node;
// use crate::index::settings::BlockSettings;

/// Default starting numerical value for a valid BlockID.
pub const START_BLOCK_ID: BlockID = BlockID::MIN;

pub const _1KB: usize   = 1024;
pub const _2KB: usize   = 2 * _1KB;
pub const _4KB: usize   = 4 * _1KB;
pub const _8KB: usize   = 8 * _1KB;
pub const _16KB: usize  = 16 * _1KB;
pub const _32KB: usize  = 32 * _1KB;

pub enum BlockSize {
    _1KB,
    _2KB,
    _4KB,
    _8KB,
    _16KB,
    _32KB
}

impl Display for BlockSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} kb", match self {
            BlockSize::_1KB => "1",
            BlockSize::_2KB => "2",
            BlockSize::_4KB => "4",
            BlockSize::_8KB => "8",
            BlockSize::_16KB => "16",
            BlockSize::_32KB => "32",
        })
    }
}

pub const fn fan_out(bsz: BlockSize) -> usize {
    ((match bsz {
        BlockSize::_1KB => _1KB,
        BlockSize::_2KB => _2KB,
        BlockSize::_4KB => _4KB,
        BlockSize::_8KB => _8KB,
        BlockSize::_16KB => _16KB,
        BlockSize::_32KB => _32KB,
    }) - 2) / (8 + 8)
}

pub const fn num_records(bsz: BlockSize) -> usize {
    (match bsz {
        BlockSize::_1KB => _1KB,
        BlockSize::_2KB => _2KB,
        BlockSize::_4KB => _4KB,
        BlockSize::_8KB => _8KB,
        BlockSize::_16KB => _16KB,
        BlockSize::_32KB => _32KB,
    }) / 8 / 2
}

pub const fn bsz_alignment_min<Key, Payload>() -> usize
where Key: Default + Ord + Copy + Hash,
      Payload: Default + Clone
{
    mem::size_of::<BlockID>() +
        mem::size_of::<BlockRef<0, 0, Key, Payload>>() + // ptr alignment size
        mem::align_of::<Block<0,0,Key, Payload>>() + // alignment for block
        mem::size_of::<ObjectCount>()
}

pub const fn bsz_alignment<Key, Payload>() -> usize
where Key: Default + Ord + Copy + Hash,
      Payload: Default + Clone
{
    bsz_alignment_min::<Key, Payload>()+4000 //+ // extra sized counter for num records in leaf blocks
    // 16 // + // (wc + sc) per block ref
    // mem::size_of::<BlockGuard<0,0, Key, Payload>>()
}

pub struct BlockManager<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
> {
    block_id_counter: AtomicBlockID,
    pub(crate) is_multi_version: bool,
    _marker: PhantomData<(Key, Payload)>
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> Clone for BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn clone(&self) -> Self {
        Self {
            block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
            is_multi_version: self.is_multi_version,
            _marker: PhantomData,
        }
    }
}

/// Default implementation for BlockManager with default BlockSettings.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
> Default for BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        BlockManager::new(false)
    }
}

/// Main functionality implementation for BlockManager.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Generates and returns a new atomic (unique across callers) BlockID.
    #[inline(always)]
    pub(crate) fn next_block_id(&self) -> BlockID {
        self.block_id_counter.fetch_add(1, Ordering::Relaxed)
    }

    #[inline(always)]
    pub const fn allocation_leaf(&self) -> usize {
        NUM_RECORDS
    }

    #[inline(always)]
    pub const fn allocation_directory(&self) -> usize {
        FAN_OUT - 1
    }

    /// Main Constructor requiring supplied BlockSettings.
    #[inline(always)]
    pub(crate) fn new(is_multi_version: bool) -> Self {
        Self {
            block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
            is_multi_version,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn make_empty_root(&self) -> Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.new_empty_leaf()
    }

    #[inline(always)]
    pub(crate) fn new_empty_leaf(&self) -> Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
        // if self.is_multi_version {
        //     self.new_empty_leaf_multi_version_block()
        // }
        // else {
            self.new_empty_leaf_single_version_block()
        // }
    }

    /// Crafts a new aligned Index-Block.
    #[inline(always)]
    pub(crate) fn new_empty_index_block(&self) -> Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
        Block {
            block_id: self.next_block_id(),
            node_data: Node::Index(InternalPage::new())
        }
    }

    /// Crafts a new aligned Leaf-Block.
    #[inline(always)]
    pub(crate) fn new_empty_leaf_single_version_block(&self) -> Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
        Block {
            block_id: self.next_block_id(),
            node_data: Node::Leaf(LeafPage::new())
        }
    }

    // /// Crafts a new aligned Multi-Version-Leaf-Block.
    // pub(crate) fn new_empty_leaf_multi_version_block(&self) -> Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
    //     Block {
    //         block_id: self.next_block_id(),
    //         node_data: Node::MultiVersionLeaf(LeafPage::new())
    //     }
    // }
}