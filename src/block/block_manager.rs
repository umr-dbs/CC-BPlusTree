use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use TXDataModel::page_model::{AtomicBlockID, BlockID};
use TXDataModel::page_model::block::Block;
use TXDataModel::page_model::internal_page::InternalPage;
use TXDataModel::page_model::leaf_page::LeafPage;
use TXDataModel::page_model::node::Node;
// use crate::index::settings::BlockSettings;

/// Default starting numerical value for a valid BlockID.
pub const START_BLOCK_ID: BlockID = BlockID::MIN;

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