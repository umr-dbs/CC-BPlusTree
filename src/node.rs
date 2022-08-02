use std::collections::LinkedList;
use std::sync::Arc;
use mvcc_bplustree::index::record::Record;
use mvcc_bplustree::utils::cc_cell_rrw_new::{CCCellGuard, CCCellRRWOPT};

// CCCellRRWOPT is the specialized cell, implementing different access ways in the MVCCBPlusTree, the doc:
/*  // 1. Write via RwLock write lock.
    // 2. Read via RwLock read lock.
    // 3. Write Exclusive via Mutex lock.
    // 4. Lock-free read via unsafe borrow.
    // 5. Lock-free write via unsafe borrow mut.
 */

pub(crate) type NodeRef = Arc<CCCellRRWOPT<Node>>;
pub(crate) type NodeGuard<'a> = CCCellGuard<'a, Node>;

pub(crate) enum Node {
    Index(Vec<u64>, Vec<NodeRef>),
    Leaf(Vec<Record>, LeafLinks),
    MultiVersionLeaf(Vec<LinkedList<Record>>, LeafLinks)
}


impl Into<NodeRef> for Node {
    fn into(self) -> NodeRef {
        Arc::new(CCCellRRWOPT::new(self))
    }
}

impl Node {
    pub(crate) fn is_overflow(&self, allocation: usize) -> bool {
        // debug_assert!(allocation >= self.len());

        self.len() >= allocation
    }
    
    pub const fn is_leaf(&self) -> bool {
        match self {
            Node::Index(..) => false,
            _ => true
        }
    }

    pub const fn is_directory(&self) -> bool {
        !self.is_leaf()
    }

    pub fn as_records_mut(&mut self) -> &mut Vec<Record> {
        match self {
            Node::Leaf(records, _) => records,
            _ => unreachable!("Sleepy joe hit me -> expected a vec of records!")
        }
    }

    pub fn as_records_versioned_mut(&mut self) -> &mut Vec<LinkedList<Record>> {
        match self {
            Node::MultiVersionLeaf(records, ..) => records,
            _ => unreachable!("Sleepy joe hit me -> expected a vec of version lists of records!")
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Node::Index(keys, _) => keys.len(),
            Node::Leaf(records, _) => records.len(),
            Node::MultiVersionLeaf(record_lists, _) => record_lists.len()
        }
    }
}

pub(crate) struct LeafLinks {
    left: Option<NodeRef>,
    right: Option<NodeRef>
}

impl Default for LeafLinks {
    fn default() -> Self {
        Self {
            left: None,
            right: None
        }
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::Leaf(vec![], LeafLinks::default())
    }
}