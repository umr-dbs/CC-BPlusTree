use mvcc_bplustree::block::block::BlockID;
use crate::bplus_tree::Height;
use crate::index::node::BlockRef;

#[derive(Clone, Default)]
pub(crate) struct Root {
    pub(crate) block: BlockRef,
    pub(crate) height: Height
}

unsafe impl Send for Root { }
unsafe impl Sync for Root { }

impl Into<Root> for (BlockRef, Height) {
    fn into(self) -> Root {
        Root::new(self.0, self.1)
    }
}


impl Root {
    pub(crate) fn new(block: BlockRef, height: Height) -> Self {
        Self {
            block,
            height
        }
    }

    pub(crate) fn block(&self) -> BlockRef {
        self.block.clone()
    }

    pub(crate) fn height(&self) -> Height {
        self.height
    }

    pub(crate) fn block_id(&self) -> BlockID {
        self.block.unsafe_borrow().block_id()
    }
}