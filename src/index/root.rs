use std::sync::Arc;
use mvcc_bplustree::block::block::BlockID;
use mvcc_bplustree::locking::locking_strategy::{Level, LockingStrategy};
use mvcc_bplustree::utils::cc_cell::CCCell;
use crate::index::block::Block;
use crate::index::node::BlockRef;
use crate::utils::un_cell::UnCell;
use crate::utils::vcc_cell::{ConcurrentCell, OptCell};

pub(crate) struct Root {
    block: UnCell<(BlockRef, Level)>,
}

impl Into<Root> for (BlockRef, Level) {
    fn into(self) -> Root {
        Root::new(self.0, self.1)
    }
}

impl Into<Root> for (Block, Level, &LockingStrategy) {
    fn into(self) -> Root {
        if self.2.is_dolos() {
            (ConcurrentCell::OptimisticCell(Arc::new(OptCell::new(self.0))), self.1)
                .into()
        }
        else {
            (ConcurrentCell::ConcurrencyControlCell(Arc::new(CCCell::new(self.0))), self.1)
                .into()
        }
    }
}

impl Root {
    pub(crate) fn new(block: BlockRef, height: Level) -> Self {
        Self {
            block: UnCell::new((block, height)),
        }
    }

    pub(crate) fn block(&self) -> BlockRef {
        self.block.get().0.clone()
    }

    pub(crate) fn replace(&self, new_block: BlockRef, height: Level) -> BlockRef {
        self.block.replace((new_block, height)).0
    }

    pub(crate) fn height(&self) -> Level {
        self.block.get().1
    }

    pub(crate) fn block_id(&self) -> BlockID {
        self.block.get().0.unsafe_borrow().block_id()
    }
}