use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use mvcc_bplustree::block::block::BlockID;
use mvcc_bplustree::utils::cc_cell::CCCell;
use crate::index::node::{BlockRef, Node};
use crate::utils::vcc_cell::ConcurrentCell::{ConcurrencyControlCell, OptimisticCell};
use crate::utils::vcc_cell::OptCell;

#[derive(Default)]
pub(crate) struct Block {
    pub(crate) block_id: BlockID,
    pub(crate) node_data: Node
}

impl Block {
    pub const fn block_id(&self) -> BlockID  {
        self.block_id
    }

    pub(crate) fn into_cell(self, optimistic: bool) -> BlockRef {
        if optimistic {
            self.into_cell_dolos()
        }
        else {
            self.into_cell_cc()
        }
    }

    #[inline(always)]
    pub(crate) fn into_cell_dolos(self) -> BlockRef {
        OptimisticCell(Arc::new(OptCell::new(self)))
    }

    #[inline(always)]
    pub(crate) fn into_cell_cc(self) -> BlockRef {
        ConcurrencyControlCell(Arc::new(CCCell::new(self)))
    }
}

impl Deref for Block {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.node_data
    }
}

impl DerefMut for Block {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node_data
    }
}


impl AsRef<Node> for Block {
    fn as_ref(&self) -> &Node {
        &self.node_data
    }
}

impl AsMut<Node> for Block {
    fn as_mut(&mut self) -> &mut Node {
        &mut self.node_data
    }
}