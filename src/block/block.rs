use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::ptr::{addr_of, addr_of_mut};
use crate::page_model::{BlockID, BlockRef};
use crate::page_model::leaf_page::LeafPage;
use crate::page_model::node::Node;
use crate::record_model::Version;
use crate::utils::smart_cell::{SmartFlavor, SmartGuard};

// #[repr(align(4096))]
#[repr(C, packed)]
pub struct Block<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
> {
    pub block_id: BlockID,
    pub node_data: Node<FAN_OUT, NUM_RECORDS, Key, Payload>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> Default for Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn default() -> Self {
        Block {
            block_id: 0,
            node_data: Node::Leaf(LeafPage::new()),
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub const fn block_id(&self) -> BlockID {
        self.block_id
    }

    #[inline(always)]
    pub fn into_cell(self, optimistic: bool) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        if optimistic {
            self.into_cell_dolos()
        } else {
            self.into_cell_cc()
        }
    }

    #[inline(always)]
    pub fn into_cell_dolos(self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.into_olc()
        // OptimisticCell(Arc::new(OptCell::new(self)))
    }

    #[inline(always)]
    pub fn into_cell_cc(self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.into_cc()
        // ConcurrencyControlCell(Arc::new(CCCell::new(self)))
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> Deref for Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
    type Target = Node<FAN_OUT, NUM_RECORDS, Key, Payload>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe {
            &*addr_of!(self.node_data) as &Self::Target
        }
        // &self.node_data
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> DerefMut for Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            &mut *addr_of_mut!(self.node_data) as &mut Self::Target
        }
        // &mut self.node_data
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> AsRef<Node<FAN_OUT, NUM_RECORDS, Key, Payload>> for Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    fn as_ref(&self) -> &Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe {
            &*addr_of!(self.node_data) as _
        }
        // &self.node_data
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
> AsMut<Node<FAN_OUT, NUM_RECORDS, Key, Payload>> for Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    fn as_mut(&mut self) -> &mut Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe {
            &mut *addr_of_mut!(self.node_data) as _
        }
        // &mut self.node_data
    }
}

pub type BlockGuard<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> = SmartGuard<'a, Block<FAN_OUT, NUM_RECORDS, Key, Payload>>;

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    // #[inline(always)]
    // pub unsafe fn cell_version_olc(&self) -> Version {
    //     match self {
    //         BlockGuard::OLCWriter(Some((.., latch))) => *latch,
    //         BlockGuard::OLCReader(Some((cell, ..))) =>
    //             if let SmartFlavor::OLCCell(opt) = cell.0.as_ref() {
    //                 opt.load_version()
    //             } else {
    //                 Version::MIN
    //             },
    //         _ => Version::MIN
    //     }
    // }

    // #[inline(always)]
    // pub unsafe fn read_cell_version_as_reader(&self) -> Version {
    //     let mut attempts = 0;
    //
    //     loop {
    //         if let SmartGuard::OLCReader(Some((cell, ..))) = self {
    //             if let SmartFlavor::OLCCell(opt) = cell.as_ref() {
    //                 match opt.read_lock() {
    //                     (false, ..) => {
    //                         sched_yield(attempts);
    //                         attempts += 1;
    //                     }
    //                     (true, read) => break read
    //                 }
    //             }
    //         }
    //     }
    // }
}

// pub type BlockGuardResult<
//     'a,
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Ord + Copy + Hash,
//     Payload: Default + Clone
// > = GuardDerefResult<'a, Block<FAN_OUT, NUM_RECORDS, Key, Payload>>;
