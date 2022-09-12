use std::sync::atomic::Ordering;
use mvcc_bplustree::block::block::{AtomicBlockID, BlockID};
use crate::index::aligned_page::{IndexPage, RecordListsPage, RecordsPage};
use crate::index::block::Block;
use crate::index::node::Node;

pub(crate) const DEFAULT_ALLOCATION_LEAF: usize = 10;
pub(crate) const DEFAULT_ALLOCATION_INDEX: usize = 10;

pub(crate) struct BlockManager {
    block_id_counter: AtomicBlockID,
    leaf_allocation: usize,
    index_allocation: usize,
    pub(crate) is_multi_version: bool
}

/// Default implementation for BlockManager with default BlockSettings.
impl Default for BlockManager {
    fn default() -> Self {
        BlockManager::new(
            DEFAULT_ALLOCATION_LEAF,
            DEFAULT_ALLOCATION_INDEX,
            false)
    }
}

impl Block {
    fn new(block_id: BlockID, node_data: Node) -> Self {
        Self {
            block_id,
            node_data
        }
    }
}

/// Main functionality implementation for BlockManager.
impl BlockManager {
    /// Default starting numerical value for a valid BlockID.
    const START_BLOCK_ID: BlockID = BlockID::MIN;

    /// Generates and returns a new atomic (unique across callers) BlockID.
    pub(crate) fn next_block_id(&self) -> BlockID {
        self.block_id_counter.fetch_add(1, Ordering::Relaxed)
    }

    pub const fn allocation_leaf(&self) -> usize {
        self.leaf_allocation
    }

    pub const fn allocation_directory(&self) -> usize {
        self.index_allocation
    }

    /// Main Constructor requiring supplied BlockSettings.
    pub(crate) fn new(leaf_allocation: usize, index_allocation: usize, is_multi_version: bool) -> Self {
        Self {
            block_id_counter: AtomicBlockID::new(Self::START_BLOCK_ID),
            leaf_allocation,
            index_allocation,
            is_multi_version
        }
    }

    pub(crate) fn make_empty_root(&self) -> Block {
        self.new_empty_leaf()
    }

    pub(crate) fn new_empty_leaf(&self) -> Block {
        if self.is_multi_version {
            self.new_empty_leaf_multi_version_block()
        }
        else {
            self.new_empty_leaf_single_version_block()
        }
    }

    /// Crafts a new aligned Index-Block.
    pub(crate) fn new_empty_index_block(&self) -> Block {
        let keys_vec
            = Vec::with_capacity(self.allocation_directory());

        let children_vec
            = Vec::with_capacity(self.allocation_directory() + 1);

        debug_assert!(keys_vec.capacity() == self.index_allocation);
        debug_assert!(children_vec.capacity() == self.index_allocation + 1);

        Block::new(
            self.next_block_id(),
            Node::Index(IndexPage::from(keys_vec, children_vec)))
    }

    /// Crafts a new aligned Leaf-Block.
    pub(crate) fn new_empty_leaf_single_version_block(&self) -> Block {
        Block::new(
            self.next_block_id(),
            Node::Leaf(RecordsPage::new(self.leaf_allocation)))
    }

    /// Crafts a new aligned Multi-Version-Leaf-Block.
    pub(crate) fn new_empty_leaf_multi_version_block(&self) -> Block {
        Block::new(
            self.next_block_id(),
            Node::MultiVersionLeaf(RecordListsPage::new(self.leaf_allocation)))
    }
}